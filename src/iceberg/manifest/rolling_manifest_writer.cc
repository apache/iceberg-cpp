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

#include "iceberg/manifest/rolling_manifest_writer.h"

#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/result.h"
#include "iceberg/util/macros.h"

namespace iceberg {

RollingManifestWriter::RollingManifestWriter(
    ManifestWriterFactory manifest_writer_factory, int64_t target_file_size_in_bytes)
    : manifest_writer_factory_(std::move(manifest_writer_factory)),
      target_file_size_in_bytes_(target_file_size_in_bytes) {}

RollingManifestWriter::~RollingManifestWriter() {
  // Ensure we close the current writer if not already closed
  std::ignore = Close();
}

Status RollingManifestWriter::WriteAddedEntry(
    std::shared_ptr<DataFile> file, std::optional<int64_t> data_sequence_number) {
  ICEBERG_ASSIGN_OR_RAISE(auto* writer, CurrentWriter());
  ICEBERG_RETURN_UNEXPECTED(
      writer->WriteAddedEntry(std::move(file), data_sequence_number));
  current_file_rows_++;
  return {};
}

Status RollingManifestWriter::WriteExistingEntry(
    std::shared_ptr<DataFile> file, int64_t file_snapshot_id,
    int64_t data_sequence_number, std::optional<int64_t> file_sequence_number) {
  ICEBERG_ASSIGN_OR_RAISE(auto* writer, CurrentWriter());
  ICEBERG_RETURN_UNEXPECTED(writer->WriteExistingEntry(
      std::move(file), file_snapshot_id, data_sequence_number, file_sequence_number));
  current_file_rows_++;
  return {};
}

Status RollingManifestWriter::WriteDeletedEntry(
    std::shared_ptr<DataFile> file, int64_t data_sequence_number,
    std::optional<int64_t> file_sequence_number) {
  ICEBERG_ASSIGN_OR_RAISE(auto* writer, CurrentWriter());
  ICEBERG_RETURN_UNEXPECTED(writer->WriteDeletedEntry(
      std::move(file), data_sequence_number, file_sequence_number));
  current_file_rows_++;
  return {};
}

Status RollingManifestWriter::Close() {
  if (!closed_) {
    ICEBERG_RETURN_UNEXPECTED(CloseCurrentWriter());
    closed_ = true;
  }
  return {};
}

Result<std::vector<ManifestFile>> RollingManifestWriter::ToManifestFiles() const {
  if (!closed_) {
    return Invalid("Cannot get ManifestFile list from unclosed writer");
  }
  return manifest_files_;
}

Result<ManifestWriter*> RollingManifestWriter::CurrentWriter() {
  if (current_writer_ == nullptr) {
    ICEBERG_ASSIGN_OR_RAISE(current_writer_, manifest_writer_factory_());
  } else if (ShouldRollToNewFile()) {
    ICEBERG_RETURN_UNEXPECTED(CloseCurrentWriter());
    ICEBERG_ASSIGN_OR_RAISE(current_writer_, manifest_writer_factory_());
  }

  return current_writer_.get();
}

bool RollingManifestWriter::ShouldRollToNewFile() const {
  if (current_writer_ == nullptr) {
    return false;
  }
  // Roll when row count is a multiple of the divisor and file size >= target
  if (current_file_rows_ % kRowsDivisor == 0) {
    auto length_result = current_writer_->length();
    if (length_result.has_value()) {
      return length_result.value() >= target_file_size_in_bytes_;
    }
    // TODO(anyone): If we can't get the length, don't roll for now, revisit this later.
  }
  return false;
}

Status RollingManifestWriter::CloseCurrentWriter() {
  if (current_writer_ != nullptr) {
    ICEBERG_RETURN_UNEXPECTED(current_writer_->Close());
    ICEBERG_ASSIGN_OR_RAISE(auto manifest_file, current_writer_->ToManifestFile());
    manifest_files_.push_back(std::move(manifest_file));
    current_writer_.reset();
    current_file_rows_ = 0;
  }
  return {};
}

}  // namespace iceberg
