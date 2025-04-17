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

/// \file iceberg/table_metadata.h
/// Table metadata for Iceberg tables.

#include <chrono>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief A time point in milliseconds
using TimePointMs =
    std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>;

/// \brief Returns a TimePointMs from a Unix timestamp in milliseconds
ICEBERG_EXPORT Result<TimePointMs> TimePointMsFromUnixMs(int64_t unix_ms);

/// \brief Returns a Unix timestamp in milliseconds from a TimePointMs
ICEBERG_EXPORT int64_t UnixMsFromTimePointMs(const TimePointMs& time_point_ms);

/// \brief Represents a snapshot log entry
struct ICEBERG_EXPORT SnapshotLogEntry {
  /// The timestamp in milliseconds of the change
  TimePointMs timestamp_ms;
  /// ID of the snapshot
  int64_t snapshot_id;

  friend bool operator==(const SnapshotLogEntry& lhs, const SnapshotLogEntry& rhs) {
    return lhs.timestamp_ms == rhs.timestamp_ms && lhs.snapshot_id == rhs.snapshot_id;
  }

  friend bool operator!=(const SnapshotLogEntry& lhs, const SnapshotLogEntry& rhs) {
    return !(lhs == rhs);
  }
};

/// \brief Represents a metadata log entry
struct ICEBERG_EXPORT MetadataLogEntry {
  /// The timestamp in milliseconds of the change
  TimePointMs timestamp_ms;
  /// Metadata file location
  std::string metadata_file;

  friend bool operator==(const MetadataLogEntry& lhs, const MetadataLogEntry& rhs) {
    return lhs.timestamp_ms == rhs.timestamp_ms && lhs.metadata_file == rhs.metadata_file;
  }

  friend bool operator!=(const MetadataLogEntry& lhs, const MetadataLogEntry& rhs) {
    return !(lhs == rhs);
  }
};

/// \brief Represents the metadata for an Iceberg table
///
/// Note that it only contains table metadata from the spec.  Compared to the Java
/// implementation, missing pieces including: 1) Map<Integer,
/// Schema|PartitionSpec|SortOrder> 2) List<MetadataUpdate> 3) Map<Long, Snapshot> 4)
/// Map<String, SnapshotRef>
struct ICEBERG_EXPORT TableMetadata {
  static constexpr int8_t kDefaultTableFormatVersion = 2;
  static constexpr int8_t kSupportedTableFormatVersion = 3;
  static constexpr int8_t kMinFormatVersionRowLineage = 3;
  static constexpr int32_t kInitialSpecId = 0;
  static constexpr int32_t kInitialSortOrderId = 1;
  static constexpr int32_t kInitialSchemaId = 0;
  static constexpr int64_t kInitialRowId = 0;
  static constexpr int64_t kInitialSequenceNumber = 0;
  static constexpr int64_t kInvalidSequenceNumber = -1;
  static constexpr int64_t kInvalidSnapshotId = -1;

  /// An integer version number for the format
  int8_t format_version;
  /// A UUID that identifies the table
  std::string table_uuid;
  /// The table's base location
  std::string location;
  /// The table's highest assigned sequence number
  int64_t last_sequence_number;
  /// Timestamp in milliseconds from the unix epoch when the table was last updated.
  TimePointMs last_updated_ms;
  /// The highest assigned column ID for the table
  int32_t last_column_id;
  /// A list of schemas
  std::vector<std::shared_ptr<Schema>> schemas;
  /// ID of the table's current schema
  int32_t current_schema_id;
  /// A list of partition specs
  std::vector<std::shared_ptr<PartitionSpec>> partition_specs;
  /// ID of the current partition spec that writers should use by default
  int32_t default_spec_id;
  /// The highest assigned partition field ID across all partition specs for the table
  int32_t last_partition_id;
  /// A string to string map of table properties
  std::unordered_map<std::string, std::string> properties;
  /// ID of the current table snapshot
  int64_t current_snapshot_id;
  /// A list of valid snapshots
  std::vector<std::shared_ptr<Snapshot>> snapshots;
  /// A list of timestamp and snapshot ID pairs that encodes changes to the current
  /// snapshot for the table
  std::vector<SnapshotLogEntry> snapshot_log;
  /// A list of timestamp and metadata file location pairs that encodes changes to the
  /// previous metadata files for the table
  std::vector<MetadataLogEntry> metadata_log;
  /// A list of sort orders
  std::vector<std::shared_ptr<SortOrder>> sort_orders;
  /// Default sort order id of the table
  int32_t default_sort_order_id;
  /// A map of snapshot references
  std::unordered_map<std::string, std::shared_ptr<SnapshotRef>> refs;
  /// A list of table statistics
  std::vector<std::shared_ptr<struct StatisticsFile>> statistics;
  /// A list of partition statistics
  std::vector<std::shared_ptr<struct PartitionStatisticsFile>> partition_statistics;
  /// A `long` higher than all assigned row IDs
  int64_t next_row_id;
};

/// \brief Returns a string representation of a SnapshotLogEntry
ICEBERG_EXPORT std::string ToString(const SnapshotLogEntry& entry);

/// \brief Returns a string representation of a MetadataLogEntry
ICEBERG_EXPORT std::string ToString(const MetadataLogEntry& entry);

}  // namespace iceberg
