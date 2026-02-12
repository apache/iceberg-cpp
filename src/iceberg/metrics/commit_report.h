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
#include <memory>
#include <string>
#include <unordered_map>

#include "iceberg/constants.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/metrics/counter.h"
#include "iceberg/metrics/metrics_context.h"
#include "iceberg/metrics/scan_report.h"  // TimerResult
#include "iceberg/metrics/timer.h"

namespace iceberg {

/// \brief Immutable snapshot of commit metrics for use in CommitReport.
///
/// Populated by CommitMetrics::ToResult() (for total_duration and attempts) and
/// by parsing snapshot summary fields (for file/record counts).
/// Mirrors org.apache.iceberg.metrics.CommitMetricsResult.
struct ICEBERG_EXPORT CommitMetricsResult {
  /// \brief Total wall-clock duration of the commit attempt (count + nanoseconds).
  TimerResult total_duration;
  /// \brief Number of commit attempts (1 on success without retries).
  int64_t attempts = 0;
  /// \brief Number of data files added in this commit.
  int64_t added_data_files = 0;
  /// \brief Number of data files removed in this commit.
  int64_t removed_data_files = 0;
  /// \brief Total live data files after this commit.
  int64_t total_data_files = 0;
  /// \brief Number of delete files added in this commit.
  int64_t added_delete_files = 0;
  /// \brief Equality delete files added.
  int64_t added_equality_delete_files = 0;
  /// \brief Positional delete files added.
  int64_t added_positional_delete_files = 0;
  /// \brief Deletion vectors added.
  int64_t added_dvs = 0;
  /// \brief Positional delete files removed.
  int64_t removed_positional_delete_files = 0;
  /// \brief Deletion vectors removed.
  int64_t removed_dvs = 0;
  /// \brief Equality delete files removed.
  int64_t removed_equality_delete_files = 0;
  /// \brief Number of delete files removed in this commit.
  int64_t removed_delete_files = 0;
  /// \brief Total live delete files after this commit.
  int64_t total_delete_files = 0;
  /// \brief Number of records added in this commit.
  int64_t added_records = 0;
  /// \brief Number of records removed in this commit.
  int64_t removed_records = 0;
  /// \brief Total live records after this commit.
  int64_t total_records = 0;
  /// \brief Total byte size of files added.
  int64_t added_files_size_bytes = 0;
  /// \brief Total byte size of files removed.
  int64_t removed_files_size_bytes = 0;
  /// \brief Total byte size of all live files after this commit.
  int64_t total_files_size_bytes = 0;
  /// \brief Positional delete records added.
  int64_t added_positional_deletes = 0;
  /// \brief Positional delete records removed.
  int64_t removed_positional_deletes = 0;
  /// \brief Total positional delete records after this commit.
  int64_t total_positional_deletes = 0;
  /// \brief Equality delete records added.
  int64_t added_equality_deletes = 0;
  /// \brief Equality delete records removed.
  int64_t removed_equality_deletes = 0;
  /// \brief Total equality delete records after this commit.
  int64_t total_equality_deletes = 0;
  /// \brief Manifest files kept unchanged in this commit.
  int64_t kept_manifest_count = 0;
  /// \brief Manifest files created in this commit.
  int64_t created_manifest_count = 0;
  /// \brief Manifest files replaced in this commit.
  int64_t replaced_manifest_count = 0;
  /// \brief Manifest entries processed in this commit.
  int64_t processed_manifest_entries_count = 0;
};

/// \brief Live commit metrics collected during a table commit operation.
///
/// Tracks the overall commit duration and retry count. File/record counts come
/// from the snapshot summary after the commit succeeds and are stored separately
/// in CommitMetricsResult.
///
/// Mirrors org.apache.iceberg.metrics.CommitMetrics.
class ICEBERG_EXPORT CommitMetrics {
 public:
  /// \brief Create a CommitMetrics instance backed by the given MetricsContext.
  static CommitMetrics Of(MetricsContext& context);

  /// \brief Create a CommitMetrics instance with all-noop timer and counter.
  static CommitMetrics Noop();

  /// \brief Snapshot timer and counter values into the corresponding fields of result.
  ///
  /// Only total_duration and attempts are written; the caller is responsible for
  /// populating the remaining snapshot-summary fields.
  void PopulateResult(CommitMetricsResult& result) const;

  /// \brief Timer measuring total wall-clock time of the commit call.
  std::shared_ptr<Timer> total_duration;

  /// \brief Counter for the number of commit attempts (including retries).
  std::shared_ptr<Counter> attempts;
};

/// \brief Report generated after a commit operation.
///
/// Contains metrics about the changes made in a commit, including
/// files added/removed and retry information.
struct ICEBERG_EXPORT CommitReport {
  /// \brief The fully qualified name of the table that was modified.
  std::string table_name;
  /// \brief The snapshot ID created by this commit.
  int64_t snapshot_id = kInvalidSnapshotId;
  /// \brief The sequence number assigned to this commit.
  int64_t sequence_number = kInvalidSequenceNumber;
  /// \brief The operation that was performed (append, overwrite, delete, etc.).
  std::string operation;
  /// \brief Metrics collected during the commit operation.
  CommitMetricsResult commit_metrics;
  /// \brief Additional key-value metadata.
  std::unordered_map<std::string, std::string> metadata;
};

}  // namespace iceberg
