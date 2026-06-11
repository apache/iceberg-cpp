/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "iceberg/data/deletion_vector_writer.h"

#include <format>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "iceberg/file_io.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/partition_spec.h"
#include "iceberg/puffin/file_metadata.h"
#include "iceberg/puffin/puffin_writer.h"
#include "iceberg/util/content_file_util.h"
#include "iceberg/util/macros.h"
#include "iceberg/version.h"

namespace iceberg {

namespace {
constexpr std::string_view kReferencedDataFile = "referenced-data-file";
constexpr std::string_view kCardinality = "cardinality";
}  // namespace

class DeletionVectorWriter::Impl {
 public:
  explicit Impl(DeletionVectorWriterOptions options) : options_(std::move(options)) {}

  // Accumulated positions and metadata for a single referenced data file.
  struct Deletes {
    PositionDeleteIndex positions;
    std::shared_ptr<PartitionSpec> spec;
    PartitionValues partition;
  };

  Deletes& DeletesFor(std::string_view referenced_data_file,
                      std::shared_ptr<PartitionSpec> spec, PartitionValues partition) {
    auto [it, inserted] = deletes_by_path_.try_emplace(std::string(referenced_data_file));
    if (inserted) {
      it->second.spec = std::move(spec);
      it->second.partition = std::move(partition);
    }
    return it->second;
  }

  Status Delete(std::string_view referenced_data_file, int64_t pos,
                std::shared_ptr<PartitionSpec> spec, PartitionValues partition) {
    ICEBERG_CHECK(!closed_, "Cannot delete after the writer is closed");
    ICEBERG_PRECHECK(!referenced_data_file.empty(),
                     "Deletion vector requires a non-empty referenced data file");
    ICEBERG_PRECHECK(pos >= 0 && pos <= RoaringPositionBitmap::kMaxPosition,
                     "Deletion vector position out of range [0, {}]: {}",
                     RoaringPositionBitmap::kMaxPosition, pos);
    DeletesFor(referenced_data_file, std::move(spec), std::move(partition))
        .positions.Delete(pos);
    return {};
  }

  Status Delete(std::string_view referenced_data_file,
                const PositionDeleteIndex& positions, std::shared_ptr<PartitionSpec> spec,
                PartitionValues partition) {
    ICEBERG_CHECK(!closed_, "Cannot delete after the writer is closed");
    ICEBERG_PRECHECK(!referenced_data_file.empty(),
                     "Deletion vector requires a non-empty referenced data file");
    DeletesFor(referenced_data_file, std::move(spec), std::move(partition))
        .positions.Merge(positions);
    return {};
  }

  Status Close() {
    if (closed_) {
      return {};
    }

    // No deletes: skip creating an orphan Puffin file that no metadata
    // references.
    if (deletes_by_path_.empty()) {
      closed_ = true;
      return {};
    }

    // Merge previously written deletes and collect the file-scoped delete files
    // they came from so callers can remove them from table state.
    for (auto& [path, deletes] : deletes_by_path_) {
      ICEBERG_ASSIGN_OR_RAISE(auto previous, options_.load_previous_deletes(path));
      if (previous == nullptr) {
        continue;
      }
      deletes.positions.Merge(*previous);
      for (const auto& delete_file : previous->delete_files()) {
        ICEBERG_ASSIGN_OR_RAISE(bool file_scoped,
                                ContentFileUtil::IsFileScoped(*delete_file));
        if (file_scoped) {
          result_.rewritten_delete_files.push_back(delete_file);
        }
      }
    }

    auto properties = options_.properties;
    if (const std::string created_by(puffin::StandardPuffinProperties::kCreatedBy);
        !properties.contains(created_by)) {
      properties.emplace(created_by,
                         std::format("iceberg-cpp/{}.{}.{}", ICEBERG_VERSION_MAJOR,
                                     ICEBERG_VERSION_MINOR, ICEBERG_VERSION_PATCH));
    }

    ICEBERG_ASSIGN_OR_RAISE(auto output_file, options_.io->NewOutputFile(options_.path));
    ICEBERG_ASSIGN_OR_RAISE(
        auto writer,
        puffin::PuffinWriter::Make(std::move(output_file), std::move(properties)));

    struct Entry {
      std::string referenced_data_file;
      std::shared_ptr<PartitionSpec> spec;
      PartitionValues partition;
      int64_t offset;
      int64_t length;
      int64_t cardinality;
    };
    std::vector<Entry> entries;
    entries.reserve(deletes_by_path_.size());

    for (auto& [path, deletes] : deletes_by_path_) {
      const int64_t cardinality = deletes.positions.Cardinality();
      ICEBERG_ASSIGN_OR_RAISE(auto data, deletes.positions.Serialize());

      puffin::Blob blob{
          .type = std::string(puffin::StandardBlobTypes::kDeletionVectorV1),
          .input_fields = {MetadataColumns::kFilePositionColumnId},
          // Snapshot ID and sequence number are inherited; the spec requires -1.
          .snapshot_id = -1,
          .sequence_number = -1,
          .data = std::move(data),
          .requested_compression = puffin::PuffinCompressionCodec::kNone,
      };
      blob.properties.emplace(std::string(kReferencedDataFile), path);
      blob.properties.emplace(std::string(kCardinality), std::format("{}", cardinality));

      ICEBERG_ASSIGN_OR_RAISE(auto blob_metadata, writer->Write(blob));
      entries.push_back(Entry{
          .referenced_data_file = path,
          .spec = deletes.spec,
          .partition = deletes.partition,
          .offset = blob_metadata.offset,
          .length = blob_metadata.length,
          .cardinality = cardinality,
      });
    }

    ICEBERG_RETURN_UNEXPECTED(writer->Finish());
    ICEBERG_ASSIGN_OR_RAISE(const int64_t file_size, writer->FileSize());

    for (auto& entry : entries) {
      result_.referenced_data_files.push_back(entry.referenced_data_file);
      result_.delete_files.push_back(std::make_shared<DataFile>(DataFile{
          .content = DataFile::Content::kPositionDeletes,
          .file_path = options_.path,
          .file_format = FileFormatType::kPuffin,
          .partition = std::move(entry.partition),
          .record_count = entry.cardinality,
          .file_size_in_bytes = file_size,
          .referenced_data_file = std::move(entry.referenced_data_file),
          .content_offset = entry.offset,
          .content_size_in_bytes = entry.length,
          .partition_spec_id =
              entry.spec ? std::make_optional(entry.spec->spec_id()) : std::nullopt,
      }));
    }

    closed_ = true;
    return {};
  }

  Result<DeleteWriteResult> Metadata() {
    ICEBERG_CHECK(closed_, "Cannot get metadata before closing the writer");
    return result_;
  }

 private:
  DeletionVectorWriterOptions options_;
  // Ordered by referenced data file path for deterministic blob layout.
  std::map<std::string, Deletes> deletes_by_path_;
  DeleteWriteResult result_;
  bool closed_ = false;
};

DeletionVectorWriter::DeletionVectorWriter(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

DeletionVectorWriter::~DeletionVectorWriter() = default;

Result<std::unique_ptr<DeletionVectorWriter>> DeletionVectorWriter::Make(
    DeletionVectorWriterOptions options) {
  ICEBERG_PRECHECK(options.io != nullptr, "DeletionVectorWriter requires a FileIO");
  ICEBERG_PRECHECK(!options.path.empty(), "DeletionVectorWriter requires a path");
  ICEBERG_PRECHECK(
      static_cast<bool>(options.load_previous_deletes),
      "DeletionVectorWriter requires a load_previous_deletes callback (return "
      "nullptr when a data file has no existing deletes)");
  return std::unique_ptr<DeletionVectorWriter>(
      new DeletionVectorWriter(std::make_unique<Impl>(std::move(options))));
}

Status DeletionVectorWriter::Delete(std::string_view referenced_data_file, int64_t pos,
                                    std::shared_ptr<PartitionSpec> spec,
                                    PartitionValues partition) {
  return impl_->Delete(referenced_data_file, pos, std::move(spec), std::move(partition));
}

Status DeletionVectorWriter::Delete(std::string_view referenced_data_file,
                                    const PositionDeleteIndex& positions,
                                    std::shared_ptr<PartitionSpec> spec,
                                    PartitionValues partition) {
  return impl_->Delete(referenced_data_file, positions, std::move(spec),
                       std::move(partition));
}

Status DeletionVectorWriter::Close() { return impl_->Close(); }

Result<DeleteWriteResult> DeletionVectorWriter::Metadata() { return impl_->Metadata(); }

}  // namespace iceberg
