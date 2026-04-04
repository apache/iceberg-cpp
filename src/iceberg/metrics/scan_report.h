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

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "iceberg/constants.h"
#include "iceberg/expression/expression.h"
#include "iceberg/iceberg_export.h"

namespace iceberg {

/// \brief Duration type for scan metrics reporting (nanosecond precision).
using DurationNs = std::chrono::nanoseconds;

/// \brief Metrics collected during the planning and execution of a table scan.
///
/// Embedded in ScanReport and populated by DataTableScan after PlanFiles()
/// completes. Mirrors the fields in Java's ScanMetricsResult.
struct ICEBERG_EXPORT ScanMetrics {
  /// \brief Duration spent planning the scan (manifest evaluation, file skipping).
  DurationNs total_planning_duration{0};
  /// \brief Number of data files included in the scan result.
  int64_t result_data_files = 0;
  /// \brief Number of delete files included in the scan result.
  int64_t result_delete_files = 0;
  /// \brief Number of data manifests whose files were read (not skipped).
  int64_t scanned_data_manifests = 0;
  /// \brief Number of delete manifests whose files were read (not skipped).
  int64_t scanned_delete_manifests = 0;
  /// \brief Total number of data manifests in the snapshot.
  int64_t total_data_manifests = 0;
  /// \brief Total number of delete manifests in the snapshot.
  int64_t total_delete_manifests = 0;
  /// \brief Total byte size of all result data files.
  int64_t total_file_size_in_bytes = 0;
  /// \brief Total byte size of all result delete files.
  int64_t total_delete_file_size_in_bytes = 0;
  /// \brief Number of data manifests skipped by partition/stats pruning.
  int64_t skipped_data_manifests = 0;
  /// \brief Number of delete manifests skipped by partition/stats pruning.
  int64_t skipped_delete_manifests = 0;
  /// \brief Number of individual data files skipped by stats pruning.
  int64_t skipped_data_files = 0;
  /// \brief Number of individual delete files skipped by stats pruning.
  int64_t skipped_delete_files = 0;
  /// \brief Number of indexed delete files (positional or DV) in the result.
  int64_t indexed_delete_files = 0;
  /// \brief Number of equality delete files in the result.
  int64_t equality_delete_files = 0;
  /// \brief Number of positional delete files in the result.
  int64_t positional_delete_files = 0;
  /// \brief Number of deletion vectors in the result.
  int64_t dvs = 0;
};

/// \brief Report generated after a table scan operation.
///
/// Contains metrics about the planning and execution of a table scan,
/// including information about manifests and data files processed.
struct ICEBERG_EXPORT ScanReport {
  /// \brief The fully qualified name of the table that was scanned.
  std::string table_name;
  /// \brief Snapshot ID that was scanned, if available.
  int64_t snapshot_id = kInvalidSnapshotId;
  /// \brief Filter expression used in the scan, if any.
  std::shared_ptr<Expression> filter;
  /// \brief Schema ID.
  int32_t schema_id = kInvalidSchemaId;
  /// \brief Projected field IDs from the scan schema.
  std::vector<int32_t> projected_field_ids;
  /// \brief Projected field names from the scan schema.
  std::vector<std::string> projected_field_names;
  /// \brief Metrics collected during the scan operation.
  ScanMetrics scan_metrics;
  /// \brief Additional key-value metadata.
  std::unordered_map<std::string, std::string> metadata;
};

}  // namespace iceberg
