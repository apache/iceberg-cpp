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

#pragma once

/// \file iceberg/manifest/manifest_list.h

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "iceberg/iceberg_export.h"
#include "iceberg/partition_spec.h"
#include "iceberg/result.h"
#include "iceberg/schema_field.h"
#include "iceberg/table_metadata.h"
#include "iceberg/type.h"

namespace iceberg {

/// \brief Field summary for partition field in the spec.
///
/// Each field of this corresponds to a field in the manifest file's partition spec.
struct ICEBERG_EXPORT PartitionFieldSummary {
  /// Field id: 509
  /// Whether the manifest contains at least one partition with a null value for the field
  bool contains_null = true;
  /// Field id: 518
  /// Whether the manifest contains at least one partition with a NaN value for the field
  std::optional<bool> contains_nan;
  /// Field id: 510
  /// Lower bound for the non-null, non-NaN values in the partition field, or null if all
  /// values are null or NaN
  std::optional<std::vector<uint8_t>> lower_bound;
  /// Field id: 511
  /// Upper bound for the non-null, non-NaN values in the partition field, or null if all
  /// values are null or NaN
  std::optional<std::vector<uint8_t>> upper_bound;

  inline static const SchemaField kContainsNull =
      SchemaField::MakeRequired(509, "contains_null", iceberg::boolean(),
                                "True if any file has a null partition value");
  inline static const SchemaField kContainsNaN =
      SchemaField::MakeOptional(518, "contains_nan", iceberg::boolean(),
                                "True if any file has a nan partition value");
  inline static const SchemaField kLowerBound = SchemaField::MakeOptional(
      510, "lower_bound", iceberg::binary(), "Partition lower bound for all files");
  inline static const SchemaField kUpperBound = SchemaField::MakeOptional(
      511, "upper_bound", iceberg::binary(), "Partition upper bound for all files");

  bool operator==(const PartitionFieldSummary& other) const = default;

  static const std::shared_ptr<StructType>& Type();
};

/// \brief The type of files tracked by the manifest, either data or delete files; 0 for
/// all v1 manifests
enum class ManifestContent {
  /// The manifest content is data.
  kData = 0,
  /// The manifest content is deletes.
  kDeletes = 1,
};

/// \brief Entry in a manifest list.
struct ICEBERG_EXPORT ManifestFile {
  /// Field id: 500
  /// Location of the manifest file
  std::string manifest_path;
  /// Field id: 501
  /// Length of the manifest file in bytes
  int64_t manifest_length = 0;
  /// Field id: 502
  /// ID of a partition spec used to write the manifest; must be listed in table metadata
  /// partition-specs
  int32_t partition_spec_id = PartitionSpec::kInitialSpecId;
  /// Field id: 517
  /// The type of files tracked by the manifest, either data or delete files; 0 for all v1
  /// manifests
  ManifestContent content = ManifestContent::kData;
  /// Field id: 515
  /// The sequence number when the manifest was added to the table; use 0 when reading v1
  /// manifest lists
  int64_t sequence_number = TableMetadata::kInitialSequenceNumber;
  /// Field id: 516
  /// The minimum data sequence number of all live data or delete files in the manifest;
  /// use 0 when reading v1 manifest lists
  int64_t min_sequence_number = TableMetadata::kInitialSequenceNumber;
  /// Field id: 503
  /// ID of the snapshot where the manifest file was added
  int64_t added_snapshot_id = -1;  // Snapshot::kInvalidSnapshotId
  /// Field id: 504
  /// Number of entries in the manifest that have status ADDED (1), when null this is
  /// assumed to be non-zero
  std::optional<int32_t> added_files_count;
  /// Field id: 505
  /// Number of entries in the manifest that have status EXISTING (0), when null this is
  /// assumed to be non-zero
  std::optional<int32_t> existing_files_count;
  /// Field id: 506
  /// Number of entries in the manifest that have status DELETED (2), when null this is
  /// assumed to be non-zero
  std::optional<int32_t> deleted_files_count;
  /// Field id: 512
  /// Number of rows in all of files in the manifest that have status ADDED, when null
  /// this is assumed to be non-zero
  std::optional<int64_t> added_rows_count;
  /// Field id: 513
  /// Number of rows in all of files in the manifest that have status EXISTING, when null
  /// this is assumed to be non-zero
  std::optional<int64_t> existing_rows_count;
  /// Field id: 514
  /// Number of rows in all of files in the manifest that have status DELETED, when null
  /// this is assumed to be non-zero
  std::optional<int64_t> deleted_rows_count;
  /// Field id: 507
  /// Element field id: 508
  /// A list of field summaries for each partition field in the spec. Each field in the
  /// list corresponds to a field in the manifest file's partition spec.
  std::vector<PartitionFieldSummary> partitions;
  /// Field id: 519
  /// Implementation-specific key metadata for encryption
  std::vector<uint8_t> key_metadata;
  /// Field id: 520
  /// The starting _row_id to assign to rows added by ADDED data files
  std::optional<int64_t> first_row_id;

  /// \brief Checks if this manifest file contains entries with ADDED status.
  bool has_added_files() const { return added_files_count.value_or(1) > 0; }

  /// \brief Checks if this manifest file contains entries with EXISTING status.
  bool has_existing_files() const { return existing_files_count.value_or(1) > 0; }

  /// \brief Checks if this manifest file contains entries with DELETED status
  bool has_deleted_files() const { return deleted_files_count.value_or(1) > 0; }

  static constexpr int32_t kManifestPathFieldId = 500;
  inline static const SchemaField kManifestPath = SchemaField::MakeRequired(
      kManifestPathFieldId, "manifest_path", string(), "Location URI with FS scheme");

  static constexpr int32_t kManifestLengthFieldId = 501;
  inline static const SchemaField kManifestLength = SchemaField::MakeRequired(
      kManifestLengthFieldId, "manifest_length", int64(), "Total file size in bytes");

  static constexpr int32_t kPartitionSpecIdFieldId = 502;
  inline static const SchemaField kPartitionSpecId = SchemaField::MakeRequired(
      kPartitionSpecIdFieldId, "partition_spec_id", int32(), "Spec ID used to write");

  static constexpr int32_t kContentFieldId = 517;
  inline static const SchemaField kContent = SchemaField::MakeOptional(
      kContentFieldId, "content", int32(), "Contents of the manifest: 0=data, 1=deletes");

  static constexpr int32_t kSequenceNumberFieldId = 515;
  inline static const SchemaField kSequenceNumber =
      SchemaField::MakeOptional(kSequenceNumberFieldId, "sequence_number", int64(),
                                "Sequence number when the manifest was added");

  static constexpr int32_t kMinSequenceNumberFieldId = 516;
  inline static const SchemaField kMinSequenceNumber =
      SchemaField::MakeOptional(kMinSequenceNumberFieldId, "min_sequence_number", int64(),
                                "Lowest sequence number in the manifest");

  static constexpr int32_t kAddedSnapshotIdFieldId = 503;
  inline static const SchemaField kAddedSnapshotId =
      SchemaField::MakeRequired(kAddedSnapshotIdFieldId, "added_snapshot_id", int64(),
                                "Snapshot ID that added the manifest");

  static constexpr int32_t kAddedFilesCountFieldId = 504;
  inline static const SchemaField kAddedFilesCount = SchemaField::MakeOptional(
      kAddedFilesCountFieldId, "added_files_count", int32(), "Added entry count");

  static constexpr int32_t kExistingFilesCountFieldId = 505;
  inline static const SchemaField kExistingFilesCount =
      SchemaField::MakeOptional(kExistingFilesCountFieldId, "existing_files_count",
                                int32(), "Existing entry count");

  static constexpr int32_t kDeletedFilesCountFieldId = 506;
  inline static const SchemaField kDeletedFilesCount = SchemaField::MakeOptional(
      kDeletedFilesCountFieldId, "deleted_files_count", int32(), "Deleted entry count");

  static constexpr int32_t kAddedRowsCountFieldId = 512;
  inline static const SchemaField kAddedRowsCount = SchemaField::MakeOptional(
      kAddedRowsCountFieldId, "added_rows_count", int64(), "Added rows count");

  static constexpr int32_t kExistingRowsCountFieldId = 513;
  inline static const SchemaField kExistingRowsCount = SchemaField::MakeOptional(
      kExistingRowsCountFieldId, "existing_rows_count", int64(), "Existing rows count");

  static constexpr int32_t kDeletedRowsCountFieldId = 514;
  inline static const SchemaField kDeletedRowsCount = SchemaField::MakeOptional(
      kDeletedRowsCountFieldId, "deleted_rows_count", int64(), "Deleted rows count");

  static constexpr int32_t kPartitionSummaryFieldId = 507;
  inline static const SchemaField kPartitions = SchemaField::MakeOptional(
      kPartitionSummaryFieldId, "partitions",
      list(SchemaField::MakeRequired(508, std::string(ListType::kElementName),
                                     struct_({
                                         PartitionFieldSummary::kContainsNull,
                                         PartitionFieldSummary::kContainsNaN,
                                         PartitionFieldSummary::kLowerBound,
                                         PartitionFieldSummary::kUpperBound,
                                     }))),
      "Summary for each partition");

  static constexpr int32_t kKeyMetadataFieldId = 519;
  inline static const SchemaField kKeyMetadata = SchemaField::MakeOptional(
      kKeyMetadataFieldId, "key_metadata", binary(), "Encryption key metadata blob");

  static constexpr int32_t kFirstRowIdFieldId = 520;
  inline static const SchemaField kFirstRowId = SchemaField::MakeOptional(
      kFirstRowIdFieldId, "first_row_id", int64(),
      "Starting row ID to assign to new rows in ADDED data files");

  bool operator==(const ManifestFile& other) const = default;

  static const std::shared_ptr<StructType>& Type();
};

/// Snapshots are embedded in table metadata, but the list of manifests for a snapshot are
/// stored in a separate manifest list file.
///
/// A new manifest list is written for each attempt to commit a snapshot because the list
/// of manifests always changes to produce a new snapshot. When a manifest list is
/// written, the (optimistic) sequence number of the snapshot is written for all new
/// manifest files tracked by the list.
///
/// A manifest list includes summary metadata that can be used to avoid scanning all of
/// the manifests in a snapshot when planning a table scan. This includes the number of
/// added, existing, and deleted files, and a summary of values for each field of the
/// partition spec used to write the manifest.
struct ICEBERG_EXPORT ManifestList {
  /// Entries in a manifest list.
  std::vector<ManifestFile> entries;
};

/// \brief Get the relative manifest content type name
ICEBERG_EXPORT inline constexpr std::string_view ToString(ManifestContent type) noexcept {
  switch (type) {
    case ManifestContent::kData:
      return "data";
    case ManifestContent::kDeletes:
      return "deletes";
  }
  std::unreachable();
}

/// \brief Get the relative manifest content type from name
ICEBERG_EXPORT inline constexpr Result<ManifestContent> ManifestContentFromString(
    std::string_view str) noexcept {
  if (str == "data") return ManifestContent::kData;
  if (str == "deletes") return ManifestContent::kDeletes;
  return InvalidArgument("Invalid manifest content type: {}", str);
}

}  // namespace iceberg
