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

#include "iceberg/v3_metadata.h"

#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_list.h"
#include "iceberg/schema.h"
#include "iceberg/util/macros.h"

namespace iceberg {

Status ManifestEntryAdapterV3::Init() {
  static std::unordered_set<int32_t> compatible_fields{
      0,   1,   2,   3,   4,   134, 100, 101, 102, 103, 104, 108, 109,
      110, 137, 125, 128, 131, 132, 135, 140, 142, 143, 144, 145,
  };
  // TODO(xiao.dong) schema to json
  metadata_["schema"] = "{}";
  // TODO(xiao.dong) partition spec to json
  metadata_["partition-spec"] = "{}";
  if (partition_spec_ != nullptr) {
    metadata_["partition-spec-id"] = std::to_string(partition_spec_->spec_id());
  }
  metadata_["format-version"] = "3";
  metadata_["content"] = "data";
  return InitSchema(compatible_fields);
}

Status ManifestEntryAdapterV3::Append(const iceberg::ManifestEntry& entry) {
  return AppendInternal(entry);
}

Result<std::optional<int64_t>> ManifestEntryAdapterV3::GetWrappedSequenceNumber(
    const iceberg::ManifestEntry& entry) {
  if (!entry.sequence_number.has_value()) {
    // if the entry's data sequence number is null,
    // then it will inherit the sequence number of the current commit.
    // to validate that this is correct, check that the snapshot id is either null (will
    // also be inherited) or that it matches the id of the current commit.
    if (entry.snapshot_id.has_value() && entry.snapshot_id.value() != snapshot_id_) {
      return InvalidManifest(
          "Found unassigned sequence number for an entry from snapshot: {}",
          entry.snapshot_id.value());
    }

    // inheritance should work only for ADDED entries
    if (entry.status != ManifestStatus::kAdded) {
      return InvalidManifest(
          "Only entries with status ADDED can have null sequence number");
    }

    return std::nullopt;
  }
  return entry.sequence_number;
}

Result<std::optional<std::string>> ManifestEntryAdapterV3::GetWrappedReferenceDataFile(
    const std::shared_ptr<DataFile>& file) {
  if (file->content == DataFile::Content::kPositionDeletes) {
    return file->referenced_data_file;
  }
  return std::nullopt;
}

Result<std::optional<int64_t>> ManifestEntryAdapterV3::GetWrappedFirstRowId(
    const std::shared_ptr<DataFile>& file) {
  if (file->content == DataFile::Content::kData) {
    return file->first_row_id;
  }
  return std::nullopt;
}

Result<std::optional<int64_t>> ManifestEntryAdapterV3::GetWrappedContentOffset(
    const std::shared_ptr<DataFile>& file) {
  if (file->content == DataFile::Content::kPositionDeletes) {
    return file->content_offset;
  }
  return std::nullopt;
}

Result<std::optional<int64_t>> ManifestEntryAdapterV3::GetWrappedContentSizeInBytes(
    const std::shared_ptr<DataFile>& file) {
  if (file->content == DataFile::Content::kPositionDeletes) {
    return file->content_size_in_bytes;
  }
  return std::nullopt;
}

Status ManifestFileAdapterV3::Init() {
  static std::unordered_set<int32_t> compatible_fields{
      500, 501, 502, 517, 515, 516, 503, 504, 505, 506, 512, 513, 514, 507, 519, 520,
  };
  metadata_["snapshot-id"] = std::to_string(snapshot_id_);
  metadata_["parent-snapshot-id"] = parent_snapshot_id_.has_value()
                                        ? std::to_string(parent_snapshot_id_.value())
                                        : "null";
  metadata_["sequence-number"] = std::to_string(sequence_number_);
  metadata_["first-row-id"] =
      next_row_id_.has_value() ? std::to_string(next_row_id_.value()) : "null";
  metadata_["format-version"] = "3";
  return InitSchema(compatible_fields);
}

Status ManifestFileAdapterV3::Append(const iceberg::ManifestFile& file) {
  auto status = AppendInternal(file);
  ICEBERG_RETURN_UNEXPECTED(status);
  if (WrappedFirstRowId(file) && next_row_id_.has_value()) {
    next_row_id_ = next_row_id_.value() + file.existing_rows_count.value_or(0) +
                   file.added_rows_count.value_or(0);
  }
  return status;
}

Result<int64_t> ManifestFileAdapterV3::GetWrappedSequenceNumber(
    const iceberg::ManifestFile& file) {
  if (file.sequence_number == TableMetadata::kInvalidSequenceNumber) {
    if (snapshot_id_ != file.added_snapshot_id) {
      return InvalidManifestList(
          "Found unassigned sequence number for a manifest from snapshot: %s",
          file.added_snapshot_id);
    }
    return sequence_number_;
  }
  return file.sequence_number;
}

Result<int64_t> ManifestFileAdapterV3::GetWrappedMinSequenceNumber(
    const iceberg::ManifestFile& file) {
  if (file.min_sequence_number == TableMetadata::kInvalidSequenceNumber) {
    if (snapshot_id_ != file.added_snapshot_id) {
      return InvalidManifestList(
          "Found unassigned sequence number for a manifest from snapshot: %s",
          file.added_snapshot_id);
    }
    return sequence_number_;
  }
  return file.min_sequence_number;
}

Result<std::optional<int64_t>> ManifestFileAdapterV3::GetWrappedFirstRowId(
    const iceberg::ManifestFile& file) {
  if (WrappedFirstRowId(file)) {
    return next_row_id_;
  } else if (file.content != ManifestFile::Content::kData) {
    return std::nullopt;
  } else {
    if (!file.first_row_id.has_value()) {
      return InvalidManifestList("Found unassigned first-row-id for file:{}",
                                 file.manifest_path);
    }
    return file.first_row_id.value();
  }
}

bool ManifestFileAdapterV3::WrappedFirstRowId(const iceberg::ManifestFile& file) {
  return file.content == ManifestFile::Content::kData && !file.first_row_id.has_value();
}

}  // namespace iceberg
