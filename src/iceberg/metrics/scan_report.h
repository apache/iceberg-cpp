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

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "iceberg/constants.h"
#include "iceberg/expression/expression.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/metrics/metrics_context.h"
#include "iceberg/metrics/metrics_types.h"
#include "iceberg/metrics/timer.h"

namespace iceberg {

// Forward declaration: ScanMetrics is defined later in this header.
class ScanMetrics;

/// \brief Immutable snapshot of scan metrics for use in ScanReport.
///
/// Populated by ScanMetrics::ToResult() after a scan completes.
struct ICEBERG_EXPORT ScanMetricsResult {
  /// \brief Total planning duration (count of recordings + accumulated nanoseconds).
  std::optional<TimerResult> total_planning_duration;
  /// \brief Number of data files included in the scan result.
  std::optional<CounterResult> result_data_files;
  /// \brief Number of delete files included in the scan result.
  std::optional<CounterResult> result_delete_files;
  /// \brief Number of data manifests whose files were read (not skipped).
  std::optional<CounterResult> scanned_data_manifests;
  /// \brief Number of delete manifests whose files were read (not skipped).
  std::optional<CounterResult> scanned_delete_manifests;
  /// \brief Total number of data manifests in the snapshot.
  std::optional<CounterResult> total_data_manifests;
  /// \brief Total number of delete manifests in the snapshot.
  std::optional<CounterResult> total_delete_manifests;
  /// \brief Total byte size of all result data files.
  std::optional<CounterResult> total_file_size_in_bytes;
  /// \brief Total byte size of all result delete files.
  std::optional<CounterResult> total_delete_file_size_in_bytes;
  /// \brief Number of data manifests skipped by partition/stats pruning.
  std::optional<CounterResult> skipped_data_manifests;
  /// \brief Number of delete manifests skipped by partition/stats pruning.
  std::optional<CounterResult> skipped_delete_manifests;
  /// \brief Number of individual data files skipped by stats pruning.
  std::optional<CounterResult> skipped_data_files;
  /// \brief Number of individual delete files skipped by stats pruning.
  std::optional<CounterResult> skipped_delete_files;
  /// \brief Number of indexed delete files (positional or DV) in the result.
  std::optional<CounterResult> indexed_delete_files;
  /// \brief Number of equality delete files in the result.
  std::optional<CounterResult> equality_delete_files;
  /// \brief Number of positional delete files in the result.
  std::optional<CounterResult> positional_delete_files;
  /// \brief Number of deletion vectors in the result.
  std::optional<CounterResult> dvs;

  bool operator==(const ScanMetricsResult&) const = default;

  /// \brief Build a ScanMetricsResult from live scan metrics.
  static ScanMetricsResult From(const ScanMetrics& scan_metrics);
};

/// \brief Live scan metrics collected during a table scan operation.
///
/// Holds named Counter and Timer instances obtained from a MetricsContext.
/// Call Make() at the start of a scan to obtain an instrumented instance, then
/// increment counters and start/stop the planning timer as the scan proceeds.
/// Call ToResult() at the end to obtain the serialisable ScanMetricsResult.
class ICEBERG_EXPORT ScanMetrics {
 public:
  /// \brief Create a ScanMetrics instance backed by the given MetricsContext.
  static std::unique_ptr<ScanMetrics> Make(MetricsContext& context);

  /// \brief Create a ScanMetrics instance with all-noop counters and timer.
  static std::unique_ptr<ScanMetrics> Noop();

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

 private:
  ScanMetrics() = default;
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
