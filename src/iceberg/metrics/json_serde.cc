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

#include "iceberg/metrics/json_serde.h"

#include <nlohmann/json.hpp>

#include "iceberg/expression/json_serde_internal.h"
#include "iceberg/util/json_util_internal.h"

namespace iceberg {

namespace {

// JSON key constants (kebab-case, matching Iceberg spec)
constexpr std::string_view kTableName = "table-name";
constexpr std::string_view kSnapshotId = "snapshot-id";
constexpr std::string_view kFilter = "filter";
constexpr std::string_view kSchemaId = "schema-id";
constexpr std::string_view kProjectedFieldIds = "projected-field-ids";
constexpr std::string_view kProjectedFieldNames = "projected-field-names";
constexpr std::string_view kScanMetrics = "scan-metrics";
constexpr std::string_view kMetadata = "metadata";
constexpr std::string_view kSequenceNumber = "sequence-number";
constexpr std::string_view kOperation = "operation";
constexpr std::string_view kCommitMetrics = "commit-metrics";

// CounterResult / TimerResult keys
constexpr std::string_view kUnit = "unit";
constexpr std::string_view kValue = "value";
constexpr std::string_view kCount = "count";
constexpr std::string_view kTotalDuration = "total-duration";

// ScanMetricsResult keys
constexpr std::string_view kTotalPlanningDuration = "total-planning-duration";
constexpr std::string_view kResultDataFiles = "result-data-files";
constexpr std::string_view kResultDeleteFiles = "result-delete-files";
constexpr std::string_view kScannedDataManifests = "scanned-data-manifests";
constexpr std::string_view kScannedDeleteManifests = "scanned-delete-manifests";
constexpr std::string_view kTotalDataManifests = "total-data-manifests";
constexpr std::string_view kTotalDeleteManifests = "total-delete-manifests";
constexpr std::string_view kTotalFileSizeInBytes = "total-file-size-in-bytes";
constexpr std::string_view kTotalDeleteFileSizeInBytes =
    "total-delete-file-size-in-bytes";
constexpr std::string_view kSkippedDataManifests = "skipped-data-manifests";
constexpr std::string_view kSkippedDeleteManifests = "skipped-delete-manifests";
constexpr std::string_view kSkippedDataFiles = "skipped-data-files";
constexpr std::string_view kSkippedDeleteFiles = "skipped-delete-files";
constexpr std::string_view kIndexedDeleteFiles = "indexed-delete-files";
constexpr std::string_view kEqualityDeleteFiles = "equality-delete-files";
constexpr std::string_view kPositionalDeleteFiles = "positional-delete-files";
constexpr std::string_view kDvs = "dvs";

// CommitMetricsResult keys
constexpr std::string_view kAttempts = "attempts";
constexpr std::string_view kAddedDataFiles = "added-data-files";
constexpr std::string_view kRemovedDataFiles = "removed-data-files";
constexpr std::string_view kTotalDataFiles = "total-data-files";
constexpr std::string_view kAddedDeleteFiles = "added-delete-files";
constexpr std::string_view kAddedEqualityDeleteFiles = "added-equality-delete-files";
constexpr std::string_view kAddedPositionalDeleteFiles = "added-positional-delete-files";
constexpr std::string_view kAddedDvs = "added-dvs";
constexpr std::string_view kRemovedPositionalDeleteFiles =
    "removed-positional-delete-files";
constexpr std::string_view kRemovedDvs = "removed-dvs";
constexpr std::string_view kRemovedEqualityDeleteFiles = "removed-equality-delete-files";
constexpr std::string_view kRemovedDeleteFiles = "removed-delete-files";
constexpr std::string_view kTotalDeleteFiles = "total-delete-files";
constexpr std::string_view kAddedRecords = "added-records";
constexpr std::string_view kRemovedRecords = "removed-records";
constexpr std::string_view kTotalRecords = "total-records";
constexpr std::string_view kAddedFilesSizeBytes = "added-files-size-bytes";
constexpr std::string_view kRemovedFilesSizeBytes = "removed-files-size-bytes";
constexpr std::string_view kTotalFilesSizeBytes = "total-files-size-bytes";
constexpr std::string_view kAddedPositionalDeletes = "added-positional-deletes";
constexpr std::string_view kRemovedPositionalDeletes = "removed-positional-deletes";
constexpr std::string_view kTotalPositionalDeletes = "total-positional-deletes";
constexpr std::string_view kAddedEqualityDeletes = "added-equality-deletes";
constexpr std::string_view kRemovedEqualityDeletes = "removed-equality-deletes";
constexpr std::string_view kTotalEqualityDeletes = "total-equality-deletes";
constexpr std::string_view kKeptManifestCount = "kept-manifest-count";
constexpr std::string_view kCreatedManifestCount = "created-manifest-count";
constexpr std::string_view kReplacedManifestCount = "replaced-manifest-count";
constexpr std::string_view kProcessedManifestEntriesCount =
    "processed-manifest-entries-count";

// Helper: emit a counter field only when value != 0
void SetCounterField(nlohmann::json& json, std::string_view key, int64_t value,
                     CounterUnit unit) {
  if (value == 0) return;
  json[key] = ToJson(CounterResult{.unit = unit, .value = value});
}

// Helper: parse optional int64 counter field (returns 0 if absent)
int64_t ParseCounterValue(const nlohmann::json& json, std::string_view key) {
  auto it = json.find(key);
  if (it == json.end() || it->is_null()) return 0;
  auto result = GetJsonValue<int64_t>(*it, kValue);
  return result.has_value() ? result.value() : 0;
}

// Helper: parse optional timer field; absent/null yields TimerResult{}, malformed
// propagates.
Result<TimerResult> ParseTimerResult(const nlohmann::json& json, std::string_view key) {
  auto it = json.find(key);
  if (it == json.end() || it->is_null()) return TimerResult{};
  return TimerResultFromJson(*it);
}

}  // namespace

// ---------------------------------------------------------------------------
// CounterResult
// ---------------------------------------------------------------------------

nlohmann::json ToJson(const CounterResult& counter) {
  return {{kUnit, ToString(counter.unit)}, {kValue, counter.value}};
}

Result<CounterResult> CounterResultFromJson(const nlohmann::json& json) {
  ICEBERG_ASSIGN_OR_RAISE(auto value, GetJsonValue<int64_t>(json, kValue));
  CounterResult result;
  result.value = value;
  if (auto it = json.find(kUnit); it != json.end() && it->is_string()) {
    result.unit = CounterUnitFromString(it->get<std::string>());
  }
  return result;
}

// ---------------------------------------------------------------------------
// TimerResult
// ---------------------------------------------------------------------------

nlohmann::json ToJson(const TimerResult& timer) {
  return {{kUnit, "nanoseconds"},
          {kCount, timer.count},
          {kTotalDuration, timer.total_duration.count()}};
}

Result<TimerResult> TimerResultFromJson(const nlohmann::json& json) {
  ICEBERG_ASSIGN_OR_RAISE(auto count, GetJsonValue<int64_t>(json, kCount));
  ICEBERG_ASSIGN_OR_RAISE(auto total, GetJsonValue<int64_t>(json, kTotalDuration));
  return TimerResult{.count = count, .total_duration = std::chrono::nanoseconds{total}};
}

// ---------------------------------------------------------------------------
// ScanMetricsResult
// ---------------------------------------------------------------------------

nlohmann::json ToJson(const ScanMetricsResult& m) {
  nlohmann::json json = nlohmann::json::object();
  if (m.total_planning_duration.count > 0) {
    json[std::string(kTotalPlanningDuration)] = ToJson(m.total_planning_duration);
  }
  // Counter fields (count unit unless noted)
  SetCounterField(json, std::string(kResultDataFiles), m.result_data_files,
                  CounterUnit::kCount);
  SetCounterField(json, std::string(kResultDeleteFiles), m.result_delete_files,
                  CounterUnit::kCount);
  SetCounterField(json, std::string(kScannedDataManifests), m.scanned_data_manifests,
                  CounterUnit::kCount);
  SetCounterField(json, std::string(kScannedDeleteManifests), m.scanned_delete_manifests,
                  CounterUnit::kCount);
  SetCounterField(json, std::string(kTotalDataManifests), m.total_data_manifests,
                  CounterUnit::kCount);
  SetCounterField(json, std::string(kTotalDeleteManifests), m.total_delete_manifests,
                  CounterUnit::kCount);
  SetCounterField(json, std::string(kTotalFileSizeInBytes), m.total_file_size_in_bytes,
                  CounterUnit::kBytes);
  SetCounterField(json, std::string(kTotalDeleteFileSizeInBytes),
                  m.total_delete_file_size_in_bytes, CounterUnit::kBytes);
  SetCounterField(json, std::string(kSkippedDataManifests), m.skipped_data_manifests,
                  CounterUnit::kCount);
  SetCounterField(json, std::string(kSkippedDeleteManifests), m.skipped_delete_manifests,
                  CounterUnit::kCount);
  SetCounterField(json, std::string(kSkippedDataFiles), m.skipped_data_files,
                  CounterUnit::kCount);
  SetCounterField(json, std::string(kSkippedDeleteFiles), m.skipped_delete_files,
                  CounterUnit::kCount);
  SetCounterField(json, std::string(kIndexedDeleteFiles), m.indexed_delete_files,
                  CounterUnit::kCount);
  SetCounterField(json, std::string(kEqualityDeleteFiles), m.equality_delete_files,
                  CounterUnit::kCount);
  SetCounterField(json, std::string(kPositionalDeleteFiles), m.positional_delete_files,
                  CounterUnit::kCount);
  SetCounterField(json, std::string(kDvs), m.dvs, CounterUnit::kCount);
  return json;
}

Result<ScanMetricsResult> ScanMetricsResultFromJson(const nlohmann::json& json) {
  ScanMetricsResult m;
  ICEBERG_ASSIGN_OR_RAISE(m.total_planning_duration,
                          ParseTimerResult(json, std::string(kTotalPlanningDuration)));
  m.result_data_files = ParseCounterValue(json, std::string(kResultDataFiles));
  m.result_delete_files = ParseCounterValue(json, std::string(kResultDeleteFiles));
  m.scanned_data_manifests = ParseCounterValue(json, std::string(kScannedDataManifests));
  m.scanned_delete_manifests =
      ParseCounterValue(json, std::string(kScannedDeleteManifests));
  m.total_data_manifests = ParseCounterValue(json, std::string(kTotalDataManifests));
  m.total_delete_manifests = ParseCounterValue(json, std::string(kTotalDeleteManifests));
  m.total_file_size_in_bytes =
      ParseCounterValue(json, std::string(kTotalFileSizeInBytes));
  m.total_delete_file_size_in_bytes =
      ParseCounterValue(json, std::string(kTotalDeleteFileSizeInBytes));
  m.skipped_data_manifests = ParseCounterValue(json, std::string(kSkippedDataManifests));
  m.skipped_delete_manifests =
      ParseCounterValue(json, std::string(kSkippedDeleteManifests));
  m.skipped_data_files = ParseCounterValue(json, std::string(kSkippedDataFiles));
  m.skipped_delete_files = ParseCounterValue(json, std::string(kSkippedDeleteFiles));
  m.indexed_delete_files = ParseCounterValue(json, std::string(kIndexedDeleteFiles));
  m.equality_delete_files = ParseCounterValue(json, std::string(kEqualityDeleteFiles));
  m.positional_delete_files =
      ParseCounterValue(json, std::string(kPositionalDeleteFiles));
  m.dvs = ParseCounterValue(json, std::string(kDvs));
  return m;
}

// ---------------------------------------------------------------------------
// CommitMetricsResult
// ---------------------------------------------------------------------------

nlohmann::json ToJson(const CommitMetricsResult& m) {
  nlohmann::json json = nlohmann::json::object();
  if (m.total_duration.count > 0) {
    json[std::string(kTotalDuration)] = ToJson(m.total_duration);
  }
  SetCounterField(json, std::string(kAttempts), m.attempts, CounterUnit::kCount);
  SetCounterField(json, std::string(kAddedDataFiles), m.added_data_files,
                  CounterUnit::kCount);
  SetCounterField(json, std::string(kRemovedDataFiles), m.removed_data_files,
                  CounterUnit::kCount);
  SetCounterField(json, std::string(kTotalDataFiles), m.total_data_files,
                  CounterUnit::kCount);
  SetCounterField(json, std::string(kAddedDeleteFiles), m.added_delete_files,
                  CounterUnit::kCount);
  SetCounterField(json, std::string(kAddedEqualityDeleteFiles),
                  m.added_equality_delete_files, CounterUnit::kCount);
  SetCounterField(json, std::string(kAddedPositionalDeleteFiles),
                  m.added_positional_delete_files, CounterUnit::kCount);
  SetCounterField(json, std::string(kAddedDvs), m.added_dvs, CounterUnit::kCount);
  SetCounterField(json, std::string(kRemovedPositionalDeleteFiles),
                  m.removed_positional_delete_files, CounterUnit::kCount);
  SetCounterField(json, std::string(kRemovedDvs), m.removed_dvs, CounterUnit::kCount);
  SetCounterField(json, std::string(kRemovedEqualityDeleteFiles),
                  m.removed_equality_delete_files, CounterUnit::kCount);
  SetCounterField(json, std::string(kRemovedDeleteFiles), m.removed_delete_files,
                  CounterUnit::kCount);
  SetCounterField(json, std::string(kTotalDeleteFiles), m.total_delete_files,
                  CounterUnit::kCount);
  SetCounterField(json, std::string(kAddedRecords), m.added_records, CounterUnit::kCount);
  SetCounterField(json, std::string(kRemovedRecords), m.removed_records,
                  CounterUnit::kCount);
  SetCounterField(json, std::string(kTotalRecords), m.total_records, CounterUnit::kCount);
  SetCounterField(json, std::string(kAddedFilesSizeBytes), m.added_files_size_bytes,
                  CounterUnit::kBytes);
  SetCounterField(json, std::string(kRemovedFilesSizeBytes), m.removed_files_size_bytes,
                  CounterUnit::kBytes);
  SetCounterField(json, std::string(kTotalFilesSizeBytes), m.total_files_size_bytes,
                  CounterUnit::kBytes);
  SetCounterField(json, std::string(kAddedPositionalDeletes), m.added_positional_deletes,
                  CounterUnit::kCount);
  SetCounterField(json, std::string(kRemovedPositionalDeletes),
                  m.removed_positional_deletes, CounterUnit::kCount);
  SetCounterField(json, std::string(kTotalPositionalDeletes), m.total_positional_deletes,
                  CounterUnit::kCount);
  SetCounterField(json, std::string(kAddedEqualityDeletes), m.added_equality_deletes,
                  CounterUnit::kCount);
  SetCounterField(json, std::string(kRemovedEqualityDeletes), m.removed_equality_deletes,
                  CounterUnit::kCount);
  SetCounterField(json, std::string(kTotalEqualityDeletes), m.total_equality_deletes,
                  CounterUnit::kCount);
  SetCounterField(json, std::string(kKeptManifestCount), m.kept_manifest_count,
                  CounterUnit::kCount);
  SetCounterField(json, std::string(kCreatedManifestCount), m.created_manifest_count,
                  CounterUnit::kCount);
  SetCounterField(json, std::string(kReplacedManifestCount), m.replaced_manifest_count,
                  CounterUnit::kCount);
  SetCounterField(json, std::string(kProcessedManifestEntriesCount),
                  m.processed_manifest_entries_count, CounterUnit::kCount);
  return json;
}

Result<CommitMetricsResult> CommitMetricsResultFromJson(const nlohmann::json& json) {
  CommitMetricsResult m;
  ICEBERG_ASSIGN_OR_RAISE(m.total_duration,
                          ParseTimerResult(json, std::string(kTotalDuration)));
  m.attempts = ParseCounterValue(json, std::string(kAttempts));
  m.added_data_files = ParseCounterValue(json, std::string(kAddedDataFiles));
  m.removed_data_files = ParseCounterValue(json, std::string(kRemovedDataFiles));
  m.total_data_files = ParseCounterValue(json, std::string(kTotalDataFiles));
  m.added_delete_files = ParseCounterValue(json, std::string(kAddedDeleteFiles));
  m.added_equality_delete_files =
      ParseCounterValue(json, std::string(kAddedEqualityDeleteFiles));
  m.added_positional_delete_files =
      ParseCounterValue(json, std::string(kAddedPositionalDeleteFiles));
  m.added_dvs = ParseCounterValue(json, std::string(kAddedDvs));
  m.removed_positional_delete_files =
      ParseCounterValue(json, std::string(kRemovedPositionalDeleteFiles));
  m.removed_dvs = ParseCounterValue(json, std::string(kRemovedDvs));
  m.removed_equality_delete_files =
      ParseCounterValue(json, std::string(kRemovedEqualityDeleteFiles));
  m.removed_delete_files = ParseCounterValue(json, std::string(kRemovedDeleteFiles));
  m.total_delete_files = ParseCounterValue(json, std::string(kTotalDeleteFiles));
  m.added_records = ParseCounterValue(json, std::string(kAddedRecords));
  m.removed_records = ParseCounterValue(json, std::string(kRemovedRecords));
  m.total_records = ParseCounterValue(json, std::string(kTotalRecords));
  m.added_files_size_bytes = ParseCounterValue(json, std::string(kAddedFilesSizeBytes));
  m.removed_files_size_bytes =
      ParseCounterValue(json, std::string(kRemovedFilesSizeBytes));
  m.total_files_size_bytes = ParseCounterValue(json, std::string(kTotalFilesSizeBytes));
  m.added_positional_deletes =
      ParseCounterValue(json, std::string(kAddedPositionalDeletes));
  m.removed_positional_deletes =
      ParseCounterValue(json, std::string(kRemovedPositionalDeletes));
  m.total_positional_deletes =
      ParseCounterValue(json, std::string(kTotalPositionalDeletes));
  m.added_equality_deletes = ParseCounterValue(json, std::string(kAddedEqualityDeletes));
  m.removed_equality_deletes =
      ParseCounterValue(json, std::string(kRemovedEqualityDeletes));
  m.total_equality_deletes = ParseCounterValue(json, std::string(kTotalEqualityDeletes));
  m.kept_manifest_count = ParseCounterValue(json, std::string(kKeptManifestCount));
  m.created_manifest_count = ParseCounterValue(json, std::string(kCreatedManifestCount));
  m.replaced_manifest_count =
      ParseCounterValue(json, std::string(kReplacedManifestCount));
  m.processed_manifest_entries_count =
      ParseCounterValue(json, std::string(kProcessedManifestEntriesCount));
  return m;
}

// ---------------------------------------------------------------------------
// ScanReport
// ---------------------------------------------------------------------------

Result<nlohmann::json> ToJson(const ScanReport& report) {
  nlohmann::json json;
  json[kTableName] = report.table_name;
  json[kSnapshotId] = report.snapshot_id;
  if (report.filter) {
    ICEBERG_ASSIGN_OR_RAISE(auto filter_json, ToJson(*report.filter));
    json[kFilter] = std::move(filter_json);
  }
  json[kSchemaId] = report.schema_id;
  SetContainerField(json, kProjectedFieldIds, report.projected_field_ids);
  SetContainerField(json, kProjectedFieldNames, report.projected_field_names);
  json[kScanMetrics] = ToJson(report.scan_metrics);
  if (!report.metadata.empty()) {
    json[kMetadata] = report.metadata;
  }
  return json;
}

Result<ScanReport> ScanReportFromJson(const nlohmann::json& json) {
  ScanReport report;
  ICEBERG_ASSIGN_OR_RAISE(report.table_name, GetJsonValue<std::string>(json, kTableName));
  ICEBERG_ASSIGN_OR_RAISE(report.snapshot_id, GetJsonValue<int64_t>(json, kSnapshotId));
  if (auto it = json.find(kFilter); it != json.end() && !it->is_null()) {
    ICEBERG_ASSIGN_OR_RAISE(report.filter, ExpressionFromJson(*it));
  }
  if (auto it = json.find(kSchemaId); it != json.end()) {
    report.schema_id = it->get<int32_t>();
  }
  if (auto it = json.find(kProjectedFieldIds); it != json.end()) {
    report.projected_field_ids = it->get<std::vector<int32_t>>();
  }
  if (auto it = json.find(kProjectedFieldNames); it != json.end()) {
    report.projected_field_names = it->get<std::vector<std::string>>();
  }
  if (auto it = json.find(kScanMetrics); it != json.end() && !it->is_null()) {
    ICEBERG_ASSIGN_OR_RAISE(report.scan_metrics, ScanMetricsResultFromJson(*it));
  }
  if (auto it = json.find(kMetadata); it != json.end() && it->is_object()) {
    report.metadata = it->get<std::unordered_map<std::string, std::string>>();
  }
  return report;
}

// ---------------------------------------------------------------------------
// CommitReport
// ---------------------------------------------------------------------------

nlohmann::json ToJson(const CommitReport& report) {
  nlohmann::json json;
  json[kTableName] = report.table_name;
  json[kSnapshotId] = report.snapshot_id;
  json[kSequenceNumber] = report.sequence_number;
  SetOptionalStringField(json, kOperation, report.operation);
  json[kCommitMetrics] = ToJson(report.commit_metrics);
  if (!report.metadata.empty()) {
    json[kMetadata] = report.metadata;
  }
  return json;
}

Result<CommitReport> CommitReportFromJson(const nlohmann::json& json) {
  CommitReport report;
  ICEBERG_ASSIGN_OR_RAISE(report.table_name, GetJsonValue<std::string>(json, kTableName));
  ICEBERG_ASSIGN_OR_RAISE(report.snapshot_id, GetJsonValue<int64_t>(json, kSnapshotId));
  ICEBERG_ASSIGN_OR_RAISE(report.sequence_number,
                          GetJsonValue<int64_t>(json, kSequenceNumber));
  if (auto it = json.find(kOperation); it != json.end() && it->is_string()) {
    report.operation = it->get<std::string>();
  }
  if (auto it = json.find(kCommitMetrics); it != json.end() && !it->is_null()) {
    ICEBERG_ASSIGN_OR_RAISE(report.commit_metrics, CommitMetricsResultFromJson(*it));
  }
  if (auto it = json.find(kMetadata); it != json.end() && it->is_object()) {
    report.metadata = it->get<std::unordered_map<std::string, std::string>>();
  }
  return report;
}

}  // namespace iceberg
