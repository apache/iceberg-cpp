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

#include <any>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "iceberg/file_format.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

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

enum class DataFileContent {
  kData = 0,
  kPositionDeletes = 1,
  kEqualityDeletes = 2,
};

/// \brief Get the relative data file content type from int
ICEBERG_EXPORT constexpr Result<DataFileContent> DataFileContentFromInt(
    int content) noexcept {
  switch (content) {
    case 0:
      return DataFileContent::kData;
    case 1:
      return DataFileContent::kPositionDeletes;
    case 2:
      return DataFileContent::kEqualityDeletes;
    default:
      return InvalidArgument("Invalid data file content: {}", content);
  }
}

/// \brief DataFile carries data file path, partition tuple, metrics, ...
struct ICEBERG_EXPORT DataFile {
  /// Field id: 134
  /// Type of content stored by the data file: data, equality deletes, or position
  /// deletes (all v1 files are data files)
  DataFileContent content;
  /// Field id: 100
  /// Full URI for the file with FS scheme
  std::string file_path;
  /// Field id: 101
  /// File format type, avro, orc, parquet, or puffin
  FileFormatType file_format;
  /// Field id: 102
  /// Partition data tuple, schema based on the partition spec output using partition
  /// field ids for the struct field ids
  /// TODO(zhjwpku): use StructLike to represent partition data tuple
  std::map<std::string, std::any> partition;
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
  std::unordered_map<int32_t, int64_t> column_sizes;
  /// Field id: 109
  /// Key field id: 119
  /// Value field id: 120
  /// Map from column id to number of values in the column (including null and NaN values)
  std::unordered_map<int32_t, int64_t> value_counts;
  /// Field id: 110
  /// Key field id: 121
  /// Value field id: 122
  /// Map from column id to number of null values in the column
  std::unordered_map<int32_t, int64_t> null_value_counts;
  /// Field id: 137
  /// Key field id: 138
  /// Value field id: 139
  /// Map from column id to number of NaN values in the column
  std::unordered_map<int32_t, int64_t> nan_value_counts;
  /// Field id: 125
  /// Key field id: 126
  /// Value field id: 127
  /// Map from column id to lower bound in the column serialized as binary.
  /// Each value must be less than or equal to all non-null, non-NaN values in the column
  /// for the file.
  ///
  /// Reference:
  /// - [Binary single-value
  /// serialization](https://iceberg.apache.org/spec/#binary-single-value-serialization)
  std::unordered_map<int32_t, std::vector<uint8_t>> lower_bounds;
  /// Field id: 128
  /// Key field id: 129
  /// Value field id: 130
  /// Map from column id to upper bound in the column serialized as binary.
  /// Each value must be greater than or equal to all non-null, non-Nan values in the
  /// column for the file.
  ///
  /// Reference:
  /// - [Binary single-value
  /// serialization](https://iceberg.apache.org/spec/#binary-single-value-serialization)
  std::unordered_map<int32_t, std::vector<uint8_t>> upper_bounds;
  /// Field id: 131
  /// Implementation-specific key metadata for encryption
  std::optional<std::vector<uint8_t>> key_metadata;
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

  static const SchemaField CONTENT;
  static const SchemaField FILE_PATH;
  static const SchemaField FILE_FORMAT;
  static const SchemaField RECORD_COUNT;
  static const SchemaField FILE_SIZE;
  static const SchemaField COLUMN_SIZES;
  static const SchemaField VALUE_COUNTS;
  static const SchemaField NULL_VALUE_COUNTS;
  static const SchemaField NAN_VALUE_COUNTS;
  static const SchemaField LOWER_BOUNDS;
  static const SchemaField UPPER_BOUNDS;
  static const SchemaField KEY_METADATA;
  static const SchemaField SPLIT_OFFSETS;
  static const SchemaField EQUALITY_IDS;
  static const SchemaField SORT_ORDER_ID;
  static const SchemaField FIRST_ROW_ID;
  static const SchemaField REFERENCED_DATA_FILE;
  static const SchemaField CONTENT_OFFSET;
  static const SchemaField CONTENT_SIZE;

  static StructType GetType(StructType partition_type);
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
  DataFile data_file;

  static const SchemaField STATUS;
  static const SchemaField SNAPSHOT_ID;
  static const SchemaField SEQUENCE_NUMBER;
  static const SchemaField FILE_SEQUENCE_NUMBER;

  static Schema GetSchema(StructType partition_type);
  static Schema GetSchemaFromDataFileType(StructType datafile_type);
};

}  // namespace iceberg
