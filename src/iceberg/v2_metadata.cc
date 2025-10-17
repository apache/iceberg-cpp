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

#include "iceberg/json_internal.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_list.h"
#include "iceberg/schema.h"
#include "iceberg/util/macros.h"

namespace iceberg {

Status ManifestEntryAdapterV2::Init() {
  static std::unordered_set<int32_t> kManifestEntryFieldIds{
      ManifestEntry::kStatus.field_id(),
      ManifestEntry::kSnapshotId.field_id(),
      ManifestEntry::kSequenceNumber.field_id(),
      ManifestEntry::kFileSequenceNumber.field_id(),
      ManifestEntry::kDataFileFieldId,
      DataFile::kContent.field_id(),
      DataFile::kFilePath.field_id(),
      DataFile::kFileFormat.field_id(),
      DataFile::kPartitionFieldId,
      DataFile::kRecordCount.field_id(),
      DataFile::kFileSize.field_id(),
      DataFile::kColumnSizes.field_id(),
      DataFile::kValueCounts.field_id(),
      DataFile::kNullValueCounts.field_id(),
      DataFile::kNanValueCounts.field_id(),
      DataFile::kLowerBounds.field_id(),
      DataFile::kUpperBounds.field_id(),
      DataFile::kKeyMetadata.field_id(),
      DataFile::kSplitOffsets.field_id(),
      DataFile::kEqualityIds.field_id(),
      DataFile::kSortOrderId.field_id(),
      DataFile::kReferencedDataFile.field_id(),
  };
  ICEBERG_RETURN_UNEXPECTED(InitSchema(kManifestEntryFieldIds));
  ICEBERG_ASSIGN_OR_RAISE(metadata_["schema"], ToJsonString(*manifest_schema_))
  if (partition_spec_ != nullptr) {
    ICEBERG_ASSIGN_OR_RAISE(metadata_["partition-spec"], ToJsonString(*partition_spec_));
    metadata_["partition-spec-id"] = std::to_string(partition_spec_->spec_id());
  }
  metadata_["format-version"] = "2";
  metadata_["content"] = "data";
  return {};
}

Status ManifestEntryAdapterV2::Append(const ManifestEntry& entry) {
  return AppendInternal(entry);
}

Result<std::optional<int64_t>> ManifestEntryAdapterV2::GetSequenceNumber(
    const ManifestEntry& entry) const {
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

Result<std::optional<std::string>> ManifestEntryAdapterV2::GetReferenceDataFile(
    const DataFile& file) const {
  if (file.content == DataFile::Content::kPositionDeletes) {
    return file.referenced_data_file;
  }
  return std::nullopt;
}

Status ManifestFileAdapterV2::Init() {
  static std::unordered_set<int32_t> kManifestFileFieldIds{
      ManifestFile::kManifestPath.field_id(),
      ManifestFile::kManifestLength.field_id(),
      ManifestFile::kPartitionSpecId.field_id(),
      ManifestFile::kContent.field_id(),
      ManifestFile::kSequenceNumber.field_id(),
      ManifestFile::kMinSequenceNumber.field_id(),
      ManifestFile::kAddedSnapshotId.field_id(),
      ManifestFile::kAddedFilesCount.field_id(),
      ManifestFile::kExistingFilesCount.field_id(),
      ManifestFile::kDeletedFilesCount.field_id(),
      ManifestFile::kAddedRowsCount.field_id(),
      ManifestFile::kExistingRowsCount.field_id(),
      ManifestFile::kDeletedRowsCount.field_id(),
      ManifestFile::kPartitions.field_id(),
      ManifestFile::kKeyMetadata.field_id(),
  };
  metadata_["snapshot-id"] = std::to_string(snapshot_id_);
  metadata_["parent-snapshot-id"] = parent_snapshot_id_.has_value()
                                        ? std::to_string(parent_snapshot_id_.value())
                                        : "null";
  metadata_["sequence-number"] = std::to_string(sequence_number_);
  metadata_["format-version"] = "2";
  return InitSchema(kManifestFileFieldIds);
}

Status ManifestFileAdapterV2::Append(const ManifestFile& file) {
  return AppendInternal(file);
}

Result<int64_t> ManifestFileAdapterV2::GetSequenceNumber(const ManifestFile& file) const {
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

Result<int64_t> ManifestFileAdapterV2::GetMinSequenceNumber(
    const ManifestFile& file) const {
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
