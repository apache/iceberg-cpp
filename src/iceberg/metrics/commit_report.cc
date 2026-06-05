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

#include "iceberg/metrics/commit_report.h"

#include "iceberg/snapshot.h"
#include "iceberg/util/string_util.h"

namespace iceberg {

std::unique_ptr<CommitMetrics> CommitMetrics::Make(MetricsContext& context) {
  auto m = std::unique_ptr<CommitMetrics>(new CommitMetrics());
  m->total_duration = context.GetTimer("total-duration", TimerUnit::kNanoseconds);
  m->attempts = context.GetCounter("attempts");
  return m;
}

std::unique_ptr<CommitMetrics> CommitMetrics::Noop() {
  return CommitMetrics::Make(*MetricsContext::Noop());
}

CommitMetricsResult CommitMetrics::ToResult() const {
  CommitMetricsResult result;
  if (total_duration && !total_duration->IsNoop()) {
    result.total_duration =
        TimerResult{.unit = std::string(total_duration->Unit()),
                    .count = total_duration->Count(),
                    .total_duration = total_duration->TotalDuration()};
  }
  if (attempts && !attempts->IsNoop()) {
    result.attempts = CounterResult{.unit = attempts->unit(), .value = attempts->value()};
  }
  return result;
}

CommitMetricsResult CommitMetricsResult::From(
    const CommitMetrics& live_metrics,
    const std::unordered_map<std::string, std::string>& snapshot_summary) {
  auto result = live_metrics.ToResult();

  auto count_field =
      [&snapshot_summary](const std::string& key) -> std::optional<CounterResult> {
    auto it = snapshot_summary.find(key);
    if (it == snapshot_summary.end()) return std::nullopt;
    auto parsed = StringUtils::ParseNumber<int64_t>(it->second);
    if (!parsed.has_value()) return std::nullopt;
    return CounterResult{.unit = CounterUnit::kCount, .value = parsed.value()};
  };
  auto bytes_field =
      [&snapshot_summary](const std::string& key) -> std::optional<CounterResult> {
    auto it = snapshot_summary.find(key);
    if (it == snapshot_summary.end()) return std::nullopt;
    auto parsed = StringUtils::ParseNumber<int64_t>(it->second);
    if (!parsed.has_value()) return std::nullopt;
    return CounterResult{.unit = CounterUnit::kBytes, .value = parsed.value()};
  };

  result.added_data_files = count_field(SnapshotSummaryFields::kAddedDataFiles);
  result.removed_data_files = count_field(SnapshotSummaryFields::kDeletedDataFiles);
  result.total_data_files = count_field(SnapshotSummaryFields::kTotalDataFiles);
  result.added_delete_files = count_field(SnapshotSummaryFields::kAddedDeleteFiles);
  result.added_equality_delete_files =
      count_field(SnapshotSummaryFields::kAddedEqDeleteFiles);
  result.added_positional_delete_files =
      count_field(SnapshotSummaryFields::kAddedPosDeleteFiles);
  result.added_dvs = count_field(SnapshotSummaryFields::kAddedDVs);
  result.removed_delete_files = count_field(SnapshotSummaryFields::kRemovedDeleteFiles);
  result.removed_positional_delete_files =
      count_field(SnapshotSummaryFields::kRemovedPosDeleteFiles);
  result.removed_dvs = count_field(SnapshotSummaryFields::kRemovedDVs);
  result.removed_equality_delete_files =
      count_field(SnapshotSummaryFields::kRemovedEqDeleteFiles);
  result.total_delete_files = count_field(SnapshotSummaryFields::kTotalDeleteFiles);
  result.added_records = count_field(SnapshotSummaryFields::kAddedRecords);
  result.removed_records = count_field(SnapshotSummaryFields::kDeletedRecords);
  result.total_records = count_field(SnapshotSummaryFields::kTotalRecords);
  result.added_files_size_bytes = bytes_field(SnapshotSummaryFields::kAddedFileSize);
  result.removed_files_size_bytes = bytes_field(SnapshotSummaryFields::kRemovedFileSize);
  result.total_files_size_bytes = bytes_field(SnapshotSummaryFields::kTotalFileSize);
  result.added_positional_deletes = count_field(SnapshotSummaryFields::kAddedPosDeletes);
  result.removed_positional_deletes =
      count_field(SnapshotSummaryFields::kRemovedPosDeletes);
  result.total_positional_deletes = count_field(SnapshotSummaryFields::kTotalPosDeletes);
  result.added_equality_deletes = count_field(SnapshotSummaryFields::kAddedEqDeletes);
  result.removed_equality_deletes = count_field(SnapshotSummaryFields::kRemovedEqDeletes);
  result.total_equality_deletes = count_field(SnapshotSummaryFields::kTotalEqDeletes);
  result.kept_manifest_count = count_field(SnapshotSummaryFields::kManifestsKept);
  result.created_manifest_count = count_field(SnapshotSummaryFields::kManifestsCreated);
  result.replaced_manifest_count = count_field(SnapshotSummaryFields::kManifestsReplaced);
  result.processed_manifest_entries_count =
      count_field(SnapshotSummaryFields::kEntriesProcessed);
  return result;
}

}  // namespace iceberg
