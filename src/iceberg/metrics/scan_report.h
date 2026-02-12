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
#include "iceberg/metrics/counter.h"
#include "iceberg/metrics/metrics_context.h"
#include "iceberg/metrics/timer.h"

namespace iceberg {

/// \brief Duration type for scan metrics reporting (nanosecond precision).
using DurationNs = std::chrono::nanoseconds;

/// \brief Serializable snapshot of a single Counter measurement.
///
/// Used by the JSON serde layer to encode unit information alongside the value.
/// Mirrors org.apache.iceberg.metrics.CounterResult.
struct ICEBERG_EXPORT CounterResult {
  CounterUnit unit = CounterUnit::kCount;
  int64_t value = 0;
};

/// \brief Serializable snapshot of a single Timer measurement.
///
/// Used by the JSON serde layer to encode unit and count alongside the duration.
/// Mirrors org.apache.iceberg.metrics.TimerResult.
struct ICEBERG_EXPORT TimerResult {
  int64_t count = 0;
  std::chrono::nanoseconds total_duration{0};
};

/// \brief Immutable snapshot of scan metrics for use in ScanReport.
///
/// Populated by ScanMetrics::ToResult() after a scan completes.
/// Mirrors org.apache.iceberg.metrics.ScanMetricsResult.
struct ICEBERG_EXPORT ScanMetricsResult {
  /// \brief Total planning duration (count of recordings + accumulated nanoseconds).
  TimerResult total_planning_duration;
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

/// \brief Live scan metrics collected during a table scan operation.
///
/// Holds named Counter and Timer instances obtained from a MetricsContext.
/// Call Of() at the start of a scan to obtain an instrumented instance, then
/// increment counters and start/stop the planning timer as the scan proceeds.
/// Call ToResult() at the end to obtain the serialisable ScanMetricsResult.
///
/// Mirrors org.apache.iceberg.metrics.ScanMetrics.
class ICEBERG_EXPORT ScanMetrics {
 public:
  /// \brief Create a ScanMetrics instance backed by the given MetricsContext.
  static ScanMetrics Of(MetricsContext& context);

  /// \brief Create a ScanMetrics instance with all-noop counters and timer.
  static ScanMetrics Noop();

  /// \brief Snapshot current counter/timer values into a ScanMetricsResult.
  ScanMetricsResult ToResult() const;

  std::shared_ptr<Timer> total_planning_duration;
  std::shared_ptr<Counter> result_data_files;
  std::shared_ptr<Counter> result_delete_files;
  std::shared_ptr<Counter> scanned_data_manifests;
  std::shared_ptr<Counter> scanned_delete_manifests;
  std::shared_ptr<Counter> total_data_manifests;
  std::shared_ptr<Counter> total_delete_manifests;
  std::shared_ptr<Counter> total_file_size_in_bytes;
  std::shared_ptr<Counter> total_delete_file_size_in_bytes;
  std::shared_ptr<Counter> skipped_data_manifests;
  std::shared_ptr<Counter> skipped_delete_manifests;
  std::shared_ptr<Counter> skipped_data_files;
  std::shared_ptr<Counter> skipped_delete_files;
  std::shared_ptr<Counter> indexed_delete_files;
  std::shared_ptr<Counter> equality_delete_files;
  std::shared_ptr<Counter> positional_delete_files;
  std::shared_ptr<Counter> dvs;
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
  ScanMetricsResult scan_metrics;
  /// \brief Additional key-value metadata.
  std::unordered_map<std::string, std::string> metadata;
};

}  // namespace iceberg
