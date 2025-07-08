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

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "iceberg/expression/literal.h"
#include "iceberg/file_format.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/schema_field.h"
#include "iceberg/type.h"

namespace iceberg {

enum class ManifestStatus {
  kExisting = 0,
  kAdded = 1,
  kDeleted = 2,
};

/// \brief Get the relative manifest status type from int
ICEBERG_EXPORT constexpr Result<ManifestStatus> ManifestStatusFromInt(
    int status) noexcept {
  switch (status) {
    case 0:
      return ManifestStatus::kExisting;
    case 1:
      return ManifestStatus::kAdded;
    case 2:
      return ManifestStatus::kDeleted;
    default:
      return InvalidArgument("Invalid manifest status: {}", status);
  }
}

/// \brief DataFile carries data file path, partition tuple, metrics, ...
struct ICEBERG_EXPORT DataFile {
  /// \brief Content of a data file
  enum class Content {
    kData = 0,
    kPositionDeletes = 1,
    kEqualityDeletes = 2,
  };

  /// Field id: 134
  /// Type of content stored by the data file: data, equality deletes, or position
  /// deletes (all v1 files are data files)
  Content content;
  /// Field id: 100
  /// Full URI for the file with FS scheme
  std::string file_path;
  /// Field id: 101
  /// File format type, avro, orc, parquet, or puffin
  FileFormatType file_format;
  /// Field id: 102
  /// Partition data tuple, schema based on the partition spec output using partition
  /// field ids
  std::vector<Literal> partition;
  /// Field id: 103
  /// Number of records in this file, or the cardinality of a deletion vector
  int64_t record_count = 0;
  /// Field id: 104
  /// Total file size in bytes
  int64_t file_size_in_bytes = 0;
  /// Field id: 108
  /// Key field id: 117
  /// Value field id: 118
  /// Map from column id to the total size on disk of all regions that store the column.
  /// Does not include bytes necessary to read other columns, like footers. Leave null for
  /// row-oriented formats (Avro)
  std::map<int32_t, int64_t> column_sizes;
  /// Field id: 109
  /// Key field id: 119
  /// Value field id: 120
  /// Map from column id to number of values in the column (including null and NaN values)
  std::map<int32_t, int64_t> value_counts;
  /// Field id: 110
  /// Key field id: 121
  /// Value field id: 122
  /// Map from column id to number of null values in the column
  std::map<int32_t, int64_t> null_value_counts;
  /// Field id: 137
  /// Key field id: 138
  /// Value field id: 139
  /// Map from column id to number of NaN values in the column
  std::map<int32_t, int64_t> nan_value_counts;
  /// Field id: 125
  /// Key field id: 126
  /// Value field id: 127
  /// Map from column id to lower bound in the column serialized as binary.
  /// Each value must be less than or equal to all non-null, non-NaN values in the column
  /// for the file.
  std::map<int32_t, std::vector<uint8_t>> lower_bounds;
  /// Field id: 128
  /// Key field id: 129
  /// Value field id: 130
  /// Map from column id to upper bound in the column serialized as binary.
  /// Each value must be greater than or equal to all non-null, non-NaN values in the
  /// column for the file.
  std::map<int32_t, std::vector<uint8_t>> upper_bounds;
  /// Field id: 131
  /// Implementation-specific key metadata for encryption
  std::vector<uint8_t> key_metadata;
  /// Field id: 132
  /// Element Field id: 133
  /// Split offsets for the data file. For example, all row group offsets in a Parquet
  /// file. Must be sorted ascending.
  std::vector<int64_t> split_offsets;
  /// Field id: 135
  /// Element Field id: 136
  /// Field ids used to determine row equality in equality delete files. Required when
  /// content=2 and should be null otherwise. Fields with ids listed in this column must
  /// be present in the delete file.
  std::vector<int32_t> equality_ids;
  /// Field id: 140
  /// ID representing sort order for this file
  ///
  /// If sort order ID is missing or unknown, then the order is assumed to be unsorted.
  /// Only data files and equality delete files should be written with a non-null order
  /// id. Position deletes are required to be sorted by file and position, not a table
  /// order, and should set sort order id to null. Readers must ignore sort order id for
  /// position delete files.
  std::optional<int32_t> sort_order_id;
  /// This field is not included in spec, so it is not serialized into the manifest file.
  /// It is just store in memory representation used in process.
  int32_t partition_spec_id;
  /// Field id: 142
  /// The _row_id for the first row in the data file.
  ///
  /// Reference:
  /// - [First Row ID
  /// Inheritance](https://github.com/apache/iceberg/blob/main/format/spec.md#first-row-id-inheritance)
  std::optional<int64_t> first_row_id;
  /// Field id: 143
  /// Fully qualified location (URI with FS scheme) of a data file that all deletes
  /// reference.
  ///
  /// Position delete metadata can use referenced_data_file when all deletes tracked by
  /// the entry are in a single data file. Setting the referenced file is required for
  /// deletion vectors.
  std::optional<std::string> referenced_data_file;
  /// Field id: 144
  /// The offset in the file where the content starts.
  ///
  /// The content_offset and content_size_in_bytes fields are used to reference a specific
  /// blob for direct access to a deletion vector. For deletion vectors, these values are
  /// required and must exactly match the offset and length stored in the Puffin footer
  /// for the deletion vector blob.
  std::optional<int64_t> content_offset;
  /// Field id: 145
  /// The length of a referenced content stored in the file; required if content_offset is
  /// present
  std::optional<int64_t> content_size_in_bytes;

  inline static const SchemaField kContent = SchemaField::MakeRequired(
      134, "content", iceberg::int32(),
      "Contents of the file: 0=data, 1=position deletes, 2=equality deletes");
  inline static const SchemaField kFilePath = SchemaField::MakeRequired(
      100, "file_path", iceberg::string(), "Location URI with FS scheme");
  inline static const SchemaField kFileFormat = SchemaField::MakeRequired(
      101, "file_format", iceberg::int32(), "File format name: avro, orc, or parquet");
  inline static const SchemaField kRecordCount = SchemaField::MakeRequired(
      103, "record_count", iceberg::int64(), "Number of records in the file");
  inline static const SchemaField kFileSize = SchemaField::MakeRequired(
      104, "file_size_in_bytes", iceberg::int64(), "Total file size in bytes");
  inline static const SchemaField kColumnSizes = SchemaField::MakeOptional(
      108, "column_sizes",
      std::make_shared<MapType>(
          SchemaField::MakeRequired(117, std::string(MapType::kKeyName),
                                    iceberg::int32()),
          SchemaField::MakeRequired(118, std::string(MapType::kValueName),
                                    iceberg::int64())),
      "Map of column id to total size on disk");
  inline static const SchemaField kValueCounts = SchemaField::MakeOptional(
      109, "value_counts",
      std::make_shared<MapType>(
          SchemaField::MakeRequired(119, std::string(MapType::kKeyName),
                                    iceberg::int32()),
          SchemaField::MakeRequired(120, std::string(MapType::kValueName),
                                    iceberg::int64())),
      "Map of column id to total count, including null and NaN");
  inline static const SchemaField kNullValueCounts = SchemaField::MakeOptional(
      110, "null_value_counts",
      std::make_shared<MapType>(
          SchemaField::MakeRequired(121, std::string(MapType::kKeyName),
                                    iceberg::int32()),
          SchemaField::MakeRequired(122, std::string(MapType::kValueName),
                                    iceberg::int64())),
      "Map of column id to null value count");
  inline static const SchemaField kNanValueCounts = SchemaField::MakeOptional(
      137, "nan_value_counts",
      std::make_shared<MapType>(
          SchemaField::MakeRequired(138, std::string(MapType::kKeyName),
                                    iceberg::int32()),
          SchemaField::MakeRequired(139, std::string(MapType::kValueName),
                                    iceberg::int64())),
      "Map of column id to number of NaN values in the column");
  inline static const SchemaField kLowerBounds = SchemaField::MakeOptional(
      125, "lower_bounds",
      std::make_shared<MapType>(
          SchemaField::MakeRequired(126, std::string(MapType::kKeyName),
                                    iceberg::int32()),
          SchemaField::MakeRequired(127, std::string(MapType::kValueName),
                                    iceberg::binary())),
      "Map of column id to lower bound");
  inline static const SchemaField kUpperBounds = SchemaField::MakeOptional(
      128, "upper_bounds",
      std::make_shared<MapType>(
          SchemaField::MakeRequired(129, std::string(MapType::kKeyName),
                                    iceberg::int32()),
          SchemaField::MakeRequired(130, std::string(MapType::kValueName),
                                    iceberg::binary())),
      "Map of column id to upper bound");
  inline static const SchemaField kKeyMetadata = SchemaField::MakeOptional(
      131, "key_metadata", iceberg::binary(), "Encryption key metadata blob");
  inline static const SchemaField kSplitOffsets = SchemaField::MakeOptional(
      132, "split_offsets",
      std::make_shared<ListType>(SchemaField::MakeRequired(
          133, std::string(ListType::kElementName), iceberg::int64())),
      "Splittable offsets");
  inline static const SchemaField kEqualityIds = SchemaField::MakeOptional(
      135, "equality_ids",
      std::make_shared<ListType>(SchemaField::MakeRequired(
          136, std::string(ListType::kElementName), iceberg::int32())),
      "Equality comparison field IDs");
  inline static const SchemaField kSortOrderId =
      SchemaField::MakeOptional(140, "sort_order_id", iceberg::int32(), "Sort order ID");
  inline static const SchemaField kFirstRowId = SchemaField::MakeOptional(
      142, "first_row_id", iceberg::int64(), "Starting row ID to assign to new rows");
  inline static const SchemaField kReferencedDataFile = SchemaField::MakeOptional(
      143, "referenced_data_file", iceberg::string(),
      "Fully qualified location (URI with FS scheme) of a data file that all deletes "
      "reference");
  inline static const SchemaField kContentOffset =
      SchemaField::MakeOptional(144, "content_offset", iceberg::int64(),
                                "The offset in the file where the content starts");
  inline static const SchemaField kContentSize =
      SchemaField::MakeOptional(145, "content_size_in_bytes", iceberg::int64(),
                                "The length of referenced content stored in the file");

  static std::shared_ptr<StructType> Type(std::shared_ptr<StructType> partition_type);
};

/// \brief A manifest is an immutable Avro file that lists data files or delete files,
/// along with each file's partition data tuple, metrics, and tracking information.

/// \brief The schema of a manifest file
struct ICEBERG_EXPORT ManifestEntry {
  /// Field id: 0
  /// Used to track additions and deletions. Deletes are informational only and not used
  /// in scans.
  ManifestStatus status;
  /// Field id: 1
  /// Snapshot id where the file was added, or deleted if status is 2. Inherited when
  /// null.
  std::optional<int64_t> snapshot_id;
  /// Field id: 3
  /// Data sequence number of the file. Inherited when null and status is 1 (added).
  std::optional<int64_t> sequence_number;
  /// Field id: 4
  /// File sequence number indicating when the file was added. Inherited when null and
  /// status is 1 (added).
  std::optional<int64_t> file_sequence_number;
  /// Field id: 2
  /// File path, partition tuple, metrics, ...
  std::shared_ptr<DataFile> data_file;

  inline static const SchemaField kStatus =
      SchemaField::MakeRequired(0, "status", iceberg::int32());
  inline static const SchemaField kSnapshotId =
      SchemaField::MakeOptional(1, "snapshot_id", iceberg::int64());
  inline static const SchemaField kSequenceNumber =
      SchemaField::MakeOptional(3, "sequence_number", iceberg::int64());
  inline static const SchemaField kFileSequenceNumber =
      SchemaField::MakeOptional(4, "file_sequence_number", iceberg::int64());

  static std::shared_ptr<StructType> TypeFromPartitionType(
      std::shared_ptr<StructType> partition_type);
  static std::shared_ptr<StructType> TypeFromDataFileType(
      std::shared_ptr<StructType> datafile_type);
};

/// \brief Get the relative data file content type from int
ICEBERG_EXPORT constexpr Result<DataFile::Content> DataFileContentFromInt(
    int content) noexcept {
  switch (content) {
    case 0:
      return DataFile::Content::kData;
    case 1:
      return DataFile::Content::kPositionDeletes;
    case 2:
      return DataFile::Content::kEqualityDeletes;
    default:
      return InvalidArgument("Invalid data file content: {}", content);
  }
}

}  // namespace iceberg
