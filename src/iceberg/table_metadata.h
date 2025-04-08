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
#include "iceberg/type_fwd.h"
#include "iceberg/util/formattable.h"

namespace iceberg {

using TimePointMs =
    std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>;

/// \brief Represents a snapshot log entry
struct ICEBERG_EXPORT SnapshotLogEntry : public util::Formattable {
  /// The timestamp in milliseconds of the change
  TimePointMs timestamp_ms;
  /// ID of the snapshot
  int64_t snapshot_id;

  std::string ToString() const override;
};

/// \brief Represents a metadata log entry
struct ICEBERG_EXPORT MetadataLogEntry : public util::Formattable {
  /// The timestamp in milliseconds of the change
  TimePointMs timestamp_ms;
  /// Metadata file location
  std::string file;

  std::string ToString() const override;
};

/// \brief Represents the metadata for an Iceberg table
///
/// Note that it only contains table metadata from the spec.  Compared to the Java
/// implementation, missing pieces including: 1) Map<Integer,
/// Schema|PartitionSpec|SortOrder> 2) List<MetadataUpdate> 3) Map<Long, Snapshot> 4)
/// Map<String, SnapshotRef>
///
/// TODO(wgtmac): Implement Equals and ToString once SortOrder and Snapshot are
/// implemented.
struct ICEBERG_EXPORT TableMetadata {
  /// An integer version number for the format
  int8_t format_version;
  /// A UUID that identifies the table
  std::string table_uuid;
  /// The table's base location
  std::string location;
  /// The table's highest assigned sequence number
  int64_t last_sequence_number;
  /// Timestamp in milliseconds from the unix epoch when the table was last updated.
  int64_t last_updated_ms;
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
  std::unordered_map<std::string, std::string> refs;
  /// A list of table statistics
  std::vector<std::shared_ptr<struct StatisticsFile>> statistics;
  /// A list of partition statistics
  std::vector<std::shared_ptr<struct PartitionStatisticsFile>> partition_statistics;
  /// A `long` higher than all assigned row IDs
  int64_t next_row_id;
};

}  // namespace iceberg
