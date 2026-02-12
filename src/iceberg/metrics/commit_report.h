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
#include <string>
#include <unordered_map>

#include "iceberg/constants.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/metrics/metrics_context.h"
#include "iceberg/metrics/metrics_types.h"
#include "iceberg/metrics/timer.h"

namespace iceberg {

// Forward declaration: CommitMetricsResult is defined later in this header.
struct CommitMetricsResult;

/// \brief Live commit metrics collected during a table commit operation.
///
/// Tracks the overall commit duration and retry count. File/record counts come
/// from the snapshot summary after the commit succeeds and are stored separately
/// in CommitMetricsResult.
class ICEBERG_EXPORT CommitMetrics {
 public:
  /// \brief Create a CommitMetrics instance backed by the given MetricsContext.
  static std::unique_ptr<CommitMetrics> Of(MetricsContext& context);

  /// \brief Create a CommitMetrics instance with all-noop timer and counter.
  static std::unique_ptr<CommitMetrics> Noop();

  /// \brief Snapshot timer and counter values into the corresponding fields of result.
  ///
  /// Only total_duration and attempts are written; the caller is responsible for
  /// populating the remaining snapshot-summary fields.
  void PopulateResult(CommitMetricsResult& result) const;

  /// \brief Timer measuring total wall-clock time of the commit call.
  std::shared_ptr<Timer> total_duration;

  /// \brief Counter for the number of commit attempts (including retries).
  std::shared_ptr<Counter> attempts;

 private:
  CommitMetrics() = default;
};

/// \brief Immutable snapshot of commit metrics for use in CommitReport.
struct ICEBERG_EXPORT CommitMetricsResult {
  /// \brief Total wall-clock duration of the commit attempt.
  TimerResult total_duration;
  /// \brief Number of commit attempts (1 on success without retries).
  CounterResult attempts;
  /// \brief Number of data files added in this commit.
  CounterResult added_data_files;
  /// \brief Number of data files removed in this commit.
  CounterResult removed_data_files;
  /// \brief Total live data files after this commit.
  CounterResult total_data_files;
  /// \brief Number of delete files added in this commit.
  CounterResult added_delete_files;
  /// \brief Equality delete files added.
  CounterResult added_equality_delete_files;
  /// \brief Positional delete files added.
  CounterResult added_positional_delete_files;
  /// \brief Deletion vectors added.
  CounterResult added_dvs;
  /// \brief Positional delete files removed.
  CounterResult removed_positional_delete_files;
  /// \brief Deletion vectors removed.
  CounterResult removed_dvs;
  /// \brief Equality delete files removed.
  CounterResult removed_equality_delete_files;
  /// \brief Number of delete files removed in this commit.
  CounterResult removed_delete_files;
  /// \brief Total live delete files after this commit.
  CounterResult total_delete_files;
  /// \brief Number of records added in this commit.
  CounterResult added_records;
  /// \brief Number of records removed in this commit.
  CounterResult removed_records;
  /// \brief Total live records after this commit.
  CounterResult total_records;
  /// \brief Total byte size of files added.
  CounterResult added_files_size_bytes;
  /// \brief Total byte size of files removed.
  CounterResult removed_files_size_bytes;
  /// \brief Total byte size of all live files after this commit.
  CounterResult total_files_size_bytes;
  /// \brief Positional delete records added.
  CounterResult added_positional_deletes;
  /// \brief Positional delete records removed.
  CounterResult removed_positional_deletes;
  /// \brief Total positional delete records after this commit.
  CounterResult total_positional_deletes;
  /// \brief Equality delete records added.
  CounterResult added_equality_deletes;
  /// \brief Equality delete records removed.
  CounterResult removed_equality_deletes;
  /// \brief Total equality delete records after this commit.
  CounterResult total_equality_deletes;
  /// \brief Manifest files kept unchanged in this commit.
  CounterResult kept_manifest_count;
  /// \brief Manifest files created in this commit.
  CounterResult created_manifest_count;
  /// \brief Manifest files replaced in this commit.
  CounterResult replaced_manifest_count;
  /// \brief Manifest entries processed in this commit.
  CounterResult processed_manifest_entries_count;

  bool operator==(const CommitMetricsResult&) const = default;

  /// \brief Build a CommitMetricsResult from live metrics and a snapshot summary map.
  ///
  /// Combines timer/retry measurements from \p live_metrics with records parsed
  /// from \p snapshot_summary.  Missing or unparseable summary keys default to 0.
  static CommitMetricsResult From(
      const CommitMetrics& live_metrics,
      const std::unordered_map<std::string, std::string>& snapshot_summary);
};

/// \brief Report generated after a commit operation.
///
/// Contains metrics about the changes made in a commit.
struct ICEBERG_EXPORT CommitReport {
  /// \brief The fully qualified name of the table that was modified.
  std::string table_name;
  /// \brief The snapshot ID created by this commit.
  int64_t snapshot_id = kInvalidSnapshotId;
  /// \brief The sequence number assigned to this commit.
  int64_t sequence_number = kInvalidSequenceNumber;
  /// \brief The operation that was performed (write, delete, etc.).
  std::string operation;
  /// \brief Metrics collected during the commit operation.
  CommitMetricsResult commit_metrics;
  /// \brief Additional key-value metadata.
  std::unordered_map<std::string, std::string> metadata;
};

}  // namespace iceberg
