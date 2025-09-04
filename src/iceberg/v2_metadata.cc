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

#include "iceberg/v2_metadata.h"

#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_list.h"
#include "iceberg/schema.h"

namespace iceberg {

Status ManifestEntryAdapterV2::Init() {
  static std::unordered_set<int32_t> compatible_fields{
      0,   1,   2,   3,   4,   134, 100, 101, 102, 103, 104,
      108, 109, 110, 137, 125, 128, 131, 132, 135, 140, 143,
  };
  // TODO(xiao.dong) schema to json
  metadata_["schema"] = "{}";
  // TODO(xiao.dong) partition spec to json
  metadata_["partition-spec"] = "{}";
  if (partition_spec_ != nullptr) {
    metadata_["partition-spec-id"] = std::to_string(partition_spec_->spec_id());
  }
  metadata_["format-version"] = "2";
  metadata_["content"] = "data";
  return InitSchema(compatible_fields);
}

Status ManifestEntryAdapterV2::Append(const iceberg::ManifestEntry& entry) {
  return AppendInternal(entry);
}

Result<std::optional<int64_t>> ManifestEntryAdapterV2::GetWrappedSequenceNumber(
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

Result<std::optional<std::string>> ManifestEntryAdapterV2::GetWrappedReferenceDataFile(
    const std::shared_ptr<DataFile>& file) {
  if (file->content == DataFile::Content::kPositionDeletes) {
    return file->referenced_data_file;
  }
  return std::nullopt;
}

Status ManifestFileAdapterV2::Init() {
  static std::unordered_set<int32_t> compatible_fields{
      500, 501, 502, 517, 515, 516, 503, 504, 505, 506, 512, 513, 514, 507, 519,
  };
  metadata_["snapshot-id"] = std::to_string(snapshot_id_);
  metadata_["parent-snapshot-id"] = parent_snapshot_id_.has_value()
                                        ? std::to_string(parent_snapshot_id_.value())
                                        : "null";
  metadata_["sequence-number"] = std::to_string(sequence_number_);
  metadata_["format-version"] = "2";
  return InitSchema(compatible_fields);
}

Status ManifestFileAdapterV2::Append(const iceberg::ManifestFile& file) {
  return AppendInternal(file);
}

Result<int64_t> ManifestFileAdapterV2::GetWrappedSequenceNumber(
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

Result<int64_t> ManifestFileAdapterV2::GetWrappedMinSequenceNumber(
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

}  // namespace iceberg
