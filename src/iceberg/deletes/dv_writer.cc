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

#include "iceberg/deletes/dv_writer.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "iceberg/deletes/dv_util_internal.h"
#include "iceberg/deletes/dv_writer_internal.h"
#include "iceberg/deletes/position_delete_index.h"
#include "iceberg/file_format.h"
#include "iceberg/file_io.h"  // IWYU pragma: keep
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/partition_spec.h"
#include "iceberg/puffin/file_metadata.h"
#include "iceberg/puffin/puffin_writer.h"
#include "iceberg/result.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/util/content_file_util.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/string_util.h"
#include "iceberg/version.h"

namespace iceberg {

class DVWriter::Impl {
 public:
  Impl(DVWriterOptions options, size_t max_serialized_length)
      : options_(std::move(options)), max_serialized_length_(max_serialized_length) {}

  // Accumulated positions and metadata for a single referenced data file.
  struct Deletes {
    PositionDeleteIndex positions;
    std::shared_ptr<PartitionSpec> spec;
    PartitionValues partition;
  };

  Deletes& DeletesFor(std::string_view referenced_data_file,
                      const std::shared_ptr<PartitionSpec>& spec,
                      const PartitionValues& partition) {
    auto it = deletes_by_path_.lower_bound(referenced_data_file);
    if (it == deletes_by_path_.end() ||
        deletes_by_path_.key_comp()(referenced_data_file, it->first)) {
      it =
          deletes_by_path_.emplace_hint(it, std::string(referenced_data_file), Deletes{});
      it->second.spec = spec;
      it->second.partition = partition;
    }
    return it->second;
  }

  Status Delete(std::string_view referenced_data_file, int64_t pos,
                const std::shared_ptr<PartitionSpec>& spec,
                const PartitionValues& partition) {
    ICEBERG_CHECK(!closed_, "Cannot delete after writer is closed");
    ICEBERG_PRECHECK(!referenced_data_file.empty(),
                     "Deletion vector requires a non-empty referenced data file");
    ICEBERG_PRECHECK(spec != nullptr, "Deletion vector requires a partition spec");
    ICEBERG_PRECHECK(pos >= 0, "Deletion vector position must be non-negative: {}", pos);
    DeletesFor(referenced_data_file, spec, partition).positions.Delete(pos);
    return {};
  }

  Status Delete(std::string_view referenced_data_file,
                const PositionDeleteIndex& positions,
                const std::shared_ptr<PartitionSpec>& spec,
                const PartitionValues& partition) {
    ICEBERG_CHECK(!closed_, "Cannot delete after writer is closed");
    ICEBERG_PRECHECK(!referenced_data_file.empty(),
                     "Deletion vector requires a non-empty referenced data file");
    ICEBERG_PRECHECK(spec != nullptr, "Deletion vector requires a partition spec");
    DeletesFor(referenced_data_file, spec, partition).positions.Merge(positions);
    return {};
  }

  Status Close() {
    if (closed_) {
      return {};
    }

    if (deletes_by_path_.empty()) {
      closed_ = true;
      return {};
    }

    for (auto& [path, deletes] : deletes_by_path_) {
      ICEBERG_RETURN_UNEXPECTED(LoadPreviousDeletes(path, deletes));
    }

    for (auto& [_, deletes] : deletes_by_path_) {
      ICEBERG_RETURN_UNEXPECTED(
          deletes.positions.ValidateSerializedSize(max_serialized_length_));
    }

    ICEBERG_ASSIGN_OR_RAISE(auto output_file, options_.io->NewOutputFile(options_.path));
    const std::string output_path(options_.path);
    ICEBERG_ASSIGN_OR_RAISE(
        auto writer, puffin::PuffinWriter::Make(
                         std::move(output_file),
                         {{std::string(puffin::StandardPuffinProperties::kCreatedBy),
                           ICEBERG_FULL_VERSION_STRING}}));

    for (auto& [path, deletes] : deletes_by_path_) {
      ICEBERG_RETURN_UNEXPECTED(Write(*writer, path, deletes));
    }

    ICEBERG_RETURN_UNEXPECTED(writer->Finish());
    ICEBERG_ASSIGN_OR_RAISE(const int64_t file_size, writer->FileSize());

    for (const auto& [path, _] : deletes_by_path_) {
      result_.referenced_data_files.push_back(path);
      ICEBERG_ASSIGN_OR_RAISE(auto data_file, CreateDV(output_path, file_size, path));
      result_.data_files.push_back(std::move(data_file));
    }

    closed_ = true;
    return {};
  }

  Result<DeleteWriteResult> Metadata() {
    ICEBERG_CHECK(closed_, "Cannot get metadata before closing the writer");
    return result_;
  }

 private:
  Status LoadPreviousDeletes(std::string_view path, Deletes& deletes) {
    ICEBERG_ASSIGN_OR_RAISE(auto previous, options_.load_previous_deletes(path));
    if (!previous.has_value()) {
      return {};
    }

    deletes.positions.Merge(*previous);
    for (const auto& delete_file : previous->delete_files()) {
      ICEBERG_ASSIGN_OR_RAISE(bool file_scoped,
                              ContentFileUtil::IsFileScoped(*delete_file));
      if (file_scoped) {
        result_.rewritten_delete_files.push_back(delete_file);
      }
    }
    return {};
  }

  Status Write(puffin::PuffinWriter& writer, std::string_view path, Deletes& deletes) {
    ICEBERG_ASSIGN_OR_RAISE(auto blob_metadata,
                            DVUtil::WriteDVBlob(writer, path, deletes.positions));
    blobs_by_path_.insert_or_assign(std::string(path), std::move(blob_metadata));
    return {};
  }

  Result<std::shared_ptr<DataFile>> CreateDV(const std::string& path, int64_t size,
                                             std::string_view referenced_data_file) {
    auto deletes = deletes_by_path_.find(referenced_data_file);
    ICEBERG_CHECK(deletes != deletes_by_path_.end(), "Missing deletion vector for {}",
                  referenced_data_file);
    auto blob_metadata = blobs_by_path_.find(referenced_data_file);
    ICEBERG_CHECK(blob_metadata != blobs_by_path_.end(),
                  "Missing deletion vector blob for {}", referenced_data_file);

    return std::make_shared<DataFile>(DataFile{
        .content = DataFile::Content::kPositionDeletes,
        .file_path = path,
        .file_format = FileFormatType::kPuffin,
        .partition = deletes->second.partition,
        .record_count = deletes->second.positions.Cardinality(),
        .file_size_in_bytes = size,
        // TODO(gangwu): support encryption key metadata
        .referenced_data_file = std::string(referenced_data_file),
        .content_offset = blob_metadata->second.offset,
        .content_size_in_bytes = blob_metadata->second.length,
        .partition_spec_id = deletes->second.spec
                                 ? std::make_optional(deletes->second.spec->spec_id())
                                 : std::nullopt,
    });
  }

  DVWriterOptions options_;
  std::map<std::string, Deletes, StringLess> deletes_by_path_;
  std::map<std::string, puffin::BlobMetadata, StringLess> blobs_by_path_;
  DeleteWriteResult result_;
  bool closed_ = false;
  size_t max_serialized_length_;
};

DVWriter::DVWriter(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

DVWriter::~DVWriter() = default;

Result<std::unique_ptr<DVWriter>> DVWriter::Make(DVWriterOptions options) {
  return internal::DVWriterFactory::Make(
      std::move(options), static_cast<size_t>(std::numeric_limits<int32_t>::max()));
}

Result<std::unique_ptr<DVWriter>> internal::DVWriterFactory::Make(
    DVWriterOptions options, size_t max_serialized_length) {
  ICEBERG_PRECHECK(!options.path.empty(), "DVWriter requires an output path");
  ICEBERG_PRECHECK(options.io != nullptr, "DVWriter requires a FileIO");
  ICEBERG_PRECHECK(options.load_previous_deletes != nullptr,
                   "DVWriter requires a load_previous_deletes callback");
  return std::unique_ptr<DVWriter>(new DVWriter(
      std::make_unique<DVWriter::Impl>(std::move(options), max_serialized_length)));
}

Status DVWriter::Delete(std::string_view referenced_data_file, int64_t pos,
                        const std::shared_ptr<PartitionSpec>& spec,
                        const PartitionValues& partition) {
  return impl_->Delete(referenced_data_file, pos, spec, partition);
}

Status DVWriter::Delete(std::string_view referenced_data_file,
                        const PositionDeleteIndex& positions,
                        const std::shared_ptr<PartitionSpec>& spec,
                        const PartitionValues& partition) {
  return impl_->Delete(referenced_data_file, positions, spec, partition);
}

Status DVWriter::Close() { return impl_->Close(); }

Result<DeleteWriteResult> DVWriter::Metadata() { return impl_->Metadata(); }

}  // namespace iceberg
