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

#include "iceberg/deletes/roaring_position_bitmap.h"
#include "iceberg/file_io.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/partition_spec.h"
#include "iceberg/puffin/deletion_vector.h"
#include "iceberg/puffin/file_metadata.h"
#include "iceberg/puffin/puffin_writer.h"
#include "iceberg/util/macros.h"
#include "iceberg/version.h"

namespace iceberg {

class DeletionVectorWriter::Impl {
 public:
  explicit Impl(DeletionVectorWriterOptions options) : options_(std::move(options)) {}

  Status Delete(std::string_view referenced_data_file, int64_t pos) {
    ICEBERG_CHECK(!closed_, "Cannot delete after the writer is closed");
    ICEBERG_PRECHECK(!referenced_data_file.empty(),
                     "Deletion vector requires a non-empty referenced data file");
    ICEBERG_PRECHECK(pos >= 0 && pos <= RoaringPositionBitmap::kMaxPosition,
                     "Invalid deletion vector position: {}", pos);
    bitmaps_[std::string(referenced_data_file)].Add(pos);
    return {};
  }

  Status Close() {
    if (closed_) {
      return {};
    }

    // No deletes: skip creating an orphan Puffin file that no metadata
    // references.
    if (bitmaps_.empty()) {
      closed_ = true;
      return {};
    }

    auto properties = options_.properties;
    properties.try_emplace(std::string(puffin::StandardPuffinProperties::kCreatedBy),
                           std::format("iceberg-cpp/{}.{}.{}", ICEBERG_VERSION_MAJOR,
                                       ICEBERG_VERSION_MINOR, ICEBERG_VERSION_PATCH));

    ICEBERG_ASSIGN_OR_RAISE(auto output_file, options_.io->NewOutputFile(options_.path));
    ICEBERG_ASSIGN_OR_RAISE(
        auto writer,
        puffin::PuffinWriter::Make(std::move(output_file), std::move(properties)));

    // One blob per referenced data file, in deterministic (sorted) order.
    struct Entry {
      std::string referenced_data_file;
      int64_t offset;
      int64_t length;
      int64_t cardinality;
    };
    std::vector<Entry> entries;
    entries.reserve(bitmaps_.size());
    for (auto& [referenced_data_file, bitmap] : bitmaps_) {
      bitmap.Optimize();  // run-length encode before serializing
      ICEBERG_ASSIGN_OR_RAISE(
          auto blob, puffin::MakeDeletionVectorBlob(bitmap, referenced_data_file));
      ICEBERG_ASSIGN_OR_RAISE(auto blob_metadata, writer->Write(blob));
      entries.push_back(Entry{
          .referenced_data_file = referenced_data_file,
          .offset = blob_metadata.offset,
          .length = blob_metadata.length,
          .cardinality = static_cast<int64_t>(bitmap.Cardinality()),
      });
    }

    ICEBERG_RETURN_UNEXPECTED(writer->Finish());
    ICEBERG_ASSIGN_OR_RAISE(file_size_, writer->FileSize());

    const auto spec_id =
        options_.spec ? std::make_optional(options_.spec->spec_id()) : std::nullopt;

    for (auto& entry : entries) {
      data_files_.push_back(std::make_shared<DataFile>(DataFile{
          .content = DataFile::Content::kPositionDeletes,
          .file_path = options_.path,
          .file_format = FileFormatType::kPuffin,
          .partition = options_.partition,
          .record_count = entry.cardinality,
          .file_size_in_bytes = file_size_,
          .referenced_data_file = std::move(entry.referenced_data_file),
          .content_offset = entry.offset,
          .content_size_in_bytes = entry.length,
          .partition_spec_id = spec_id,
      }));
    }

    closed_ = true;
    return {};
  }

  Result<FileWriter::WriteResult> Metadata() {
    ICEBERG_CHECK(closed_, "Cannot get metadata before closing the writer");
    FileWriter::WriteResult result;
    result.data_files = data_files_;
    return result;
  }

 private:
  DeletionVectorWriterOptions options_;
  // Ordered by referenced data file path for deterministic blob layout.
  std::map<std::string, RoaringPositionBitmap> bitmaps_;
  std::vector<std::shared_ptr<DataFile>> data_files_;
  int64_t file_size_ = -1;
  bool closed_ = false;
};

DeletionVectorWriter::DeletionVectorWriter(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

DeletionVectorWriter::~DeletionVectorWriter() = default;

Result<std::unique_ptr<DeletionVectorWriter>> DeletionVectorWriter::Make(
    DeletionVectorWriterOptions options) {
  ICEBERG_PRECHECK(options.io != nullptr, "DeletionVectorWriter requires a FileIO");
  ICEBERG_PRECHECK(!options.path.empty(), "DeletionVectorWriter requires a path");
  return std::unique_ptr<DeletionVectorWriter>(
      new DeletionVectorWriter(std::make_unique<Impl>(std::move(options))));
}

Status DeletionVectorWriter::Delete(std::string_view referenced_data_file, int64_t pos) {
  return impl_->Delete(referenced_data_file, pos);
}

Status DeletionVectorWriter::Close() { return impl_->Close(); }

Result<FileWriter::WriteResult> DeletionVectorWriter::Metadata() {
  return impl_->Metadata();
}

}  // namespace iceberg
