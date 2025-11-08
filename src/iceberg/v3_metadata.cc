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

#include "iceberg/json_internal.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_list.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
#include "iceberg/util/macros.h"

namespace iceberg {

ManifestEntryAdapterV3::ManifestEntryAdapterV3(
    std::optional<int64_t> snapshot_id, std::optional<int64_t> first_row_id,
    std::shared_ptr<PartitionSpec> partition_spec, ManifestContent content)
    : ManifestEntryAdapter(std::move(partition_spec), content),
      snapshot_id_(snapshot_id),
      first_row_id_(first_row_id) {}

std::shared_ptr<Schema> ManifestEntryAdapterV3::EntrySchema(
    std::shared_ptr<StructType> partition_type) {
  return WrapFileSchema(DataFileType(std::move(partition_type)));
}

std::shared_ptr<Schema> ManifestEntryAdapterV3::WrapFileSchema(
    std::shared_ptr<StructType> file_schema) {
  return std::make_shared<Schema>(std::vector<SchemaField>{
      ManifestEntry::kStatus,
      ManifestEntry::kSnapshotId,
      ManifestEntry::kSequenceNumber,
      ManifestEntry::kFileSequenceNumber,
      SchemaField::MakeRequired(ManifestEntry::kDataFileFieldId,
                                ManifestEntry::kDataFileField, std::move(file_schema)),
  });
}

std::shared_ptr<StructType> ManifestEntryAdapterV3::DataFileType(
    std::shared_ptr<StructType> partition_type) {
  return std::make_shared<StructType>(std::vector<SchemaField>{
      DataFile::kContent.AsRequired(),
      DataFile::kFilePath,
      DataFile::kFileFormat,
      SchemaField::MakeRequired(DataFile::kPartitionFieldId, DataFile::kPartitionField,
                                std::move(partition_type), DataFile::kPartitionDoc),
      DataFile::kRecordCount,
      DataFile::kFileSize,
      DataFile::kColumnSizes,
      DataFile::kValueCounts,
      DataFile::kNullValueCounts,
      DataFile::kNanValueCounts,
      DataFile::kLowerBounds,
      DataFile::kUpperBounds,
      DataFile::kKeyMetadata,
      DataFile::kSplitOffsets,
      DataFile::kEqualityIds,
      DataFile::kSortOrderId,
      DataFile::kFirstRowId,
      DataFile::kReferencedDataFile,
      DataFile::kContentOffset,
      DataFile::kContentSize,
  });
}

Status ManifestEntryAdapterV3::Init() {
  // ICEBERG_ASSIGN_OR_RAISE(metadata_["schema"], ToJsonString(*manifest_schema_))
  ICEBERG_ASSIGN_OR_RAISE(metadata_["partition-spec"], ToJsonString(*partition_spec_));
  metadata_["partition-spec-id"] = std::to_string(partition_spec_->spec_id());
  metadata_["format-version"] = "3";
  metadata_["content"] = content_ == ManifestContent::kData ? "data" : "delete";

  ICEBERG_ASSIGN_OR_RAISE(auto partition_type, partition_spec_->PartitionType());
  if (!partition_type) {
    partition_type = std::make_shared<StructType>(std::vector<SchemaField>{});
  }
  manifest_schema_ = EntrySchema(std::move(partition_type));
  return ToArrowSchema(*manifest_schema_, &schema_);
}

Status ManifestEntryAdapterV3::Append(const ManifestEntry& entry) {
  return AppendInternal(entry);
}

Result<std::optional<int64_t>> ManifestEntryAdapterV3::GetSequenceNumber(
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

Result<std::optional<std::string>> ManifestEntryAdapterV3::GetReferenceDataFile(
    const DataFile& file) const {
  if (file.content == DataFile::Content::kPositionDeletes) {
    return file.referenced_data_file;
  }
  return std::nullopt;
}

Result<std::optional<int64_t>> ManifestEntryAdapterV3::GetFirstRowId(
    const DataFile& file) const {
  if (file.content == DataFile::Content::kData) {
    return file.first_row_id;
  }
  return std::nullopt;
}

Result<std::optional<int64_t>> ManifestEntryAdapterV3::GetContentOffset(
    const DataFile& file) const {
  if (file.content == DataFile::Content::kPositionDeletes) {
    return file.content_offset;
  }
  return std::nullopt;
}

Result<std::optional<int64_t>> ManifestEntryAdapterV3::GetContentSizeInBytes(
    const DataFile& file) const {
  if (file.content == DataFile::Content::kPositionDeletes) {
    return file.content_size_in_bytes;
  }
  return std::nullopt;
}

const std::shared_ptr<Schema> ManifestFileAdapterV3::kManifestListSchema =
    std::make_shared<Schema>(std::vector<SchemaField>{
        ManifestFile::kManifestPath,
        ManifestFile::kManifestLength,
        ManifestFile::kPartitionSpecId,
        ManifestFile::kContent.AsRequired(),
        ManifestFile::kSequenceNumber.AsRequired(),
        ManifestFile::kMinSequenceNumber.AsRequired(),
        ManifestFile::kAddedSnapshotId,
        ManifestFile::kAddedFilesCount.AsRequired(),
        ManifestFile::kExistingFilesCount.AsRequired(),
        ManifestFile::kDeletedFilesCount.AsRequired(),
        ManifestFile::kAddedRowsCount.AsRequired(),
        ManifestFile::kExistingRowsCount.AsRequired(),
        ManifestFile::kDeletedRowsCount.AsRequired(),
        ManifestFile::kPartitions,
        ManifestFile::kKeyMetadata,
        ManifestFile::kFirstRowId,
    });
Status ManifestFileAdapterV3::Init() {
  metadata_["snapshot-id"] = std::to_string(snapshot_id_);
  metadata_["parent-snapshot-id"] = parent_snapshot_id_.has_value()
                                        ? std::to_string(parent_snapshot_id_.value())
                                        : "null";
  metadata_["sequence-number"] = std::to_string(sequence_number_);
  metadata_["first-row-id"] =
      next_row_id_.has_value() ? std::to_string(next_row_id_.value()) : "null";
  metadata_["format-version"] = "3";

  manifest_list_schema_ = kManifestListSchema;
  return ToArrowSchema(*manifest_list_schema_, &schema_);
}

Status ManifestFileAdapterV3::Append(const ManifestFile& file) {
  ICEBERG_RETURN_UNEXPECTED(AppendInternal(file));
  if (WrapFirstRowId(file) && next_row_id_.has_value()) {
    next_row_id_ = next_row_id_.value() + file.existing_rows_count.value_or(0) +
                   file.added_rows_count.value_or(0);
  }
  return {};
}

Result<int64_t> ManifestFileAdapterV3::GetSequenceNumber(const ManifestFile& file) const {
  if (file.sequence_number == TableMetadata::kInvalidSequenceNumber) {
    // if the sequence number is being assigned here, then the manifest must be created by
    // the current operation. to validate this, check that the snapshot id matches the
    // current commit
    if (snapshot_id_ != file.added_snapshot_id) {
      return InvalidManifestList(
          "Found unassigned sequence number for a manifest from snapshot: {}",
          file.added_snapshot_id);
    }
    return sequence_number_;
  }
  return file.sequence_number;
}

Result<int64_t> ManifestFileAdapterV3::GetMinSequenceNumber(
    const ManifestFile& file) const {
  if (file.min_sequence_number == TableMetadata::kInvalidSequenceNumber) {
    // same sanity check as above
    if (snapshot_id_ != file.added_snapshot_id) {
      return InvalidManifestList(
          "Found unassigned sequence number for a manifest from snapshot: {}",
          file.added_snapshot_id);
    }
    // if the min sequence number is not determined, then there was no assigned sequence
    // number for any file written to the wrapped manifest. replace the unassigned
    // sequence number with the one for this commit
    return sequence_number_;
  }
  return file.min_sequence_number;
}

Result<std::optional<int64_t>> ManifestFileAdapterV3::GetFirstRowId(
    const ManifestFile& file) const {
  if (WrapFirstRowId(file)) {
    // if first-row-id is assigned, ensure that it is valid
    if (!next_row_id_.has_value()) {
      // TODO(gangwu): add ToString for ManifestFile
      return InvalidManifestList("Found invalid first-row-id assignment: {}",
                                 file.manifest_path);
    }
    return next_row_id_;
  } else if (file.content != ManifestFile::Content::kData) {
    return std::nullopt;
  } else {
    if (!file.first_row_id.has_value()) {
      return InvalidManifestList("Found unassigned first-row-id for file: {}",
                                 file.manifest_path);
    }
    return file.first_row_id;
  }
}

bool ManifestFileAdapterV3::WrapFirstRowId(const ManifestFile& file) const {
  return file.content == ManifestFile::Content::kData && !file.first_row_id.has_value();
}

}  // namespace iceberg
