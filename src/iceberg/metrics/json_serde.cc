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

#include <nlohmann/json.hpp>

#include "iceberg/expression/json_serde_internal.h"
#include "iceberg/metrics/json_serde_internal.h"
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

// Helper: emit a CounterResult field only when its value != 0
void SetCounterField(nlohmann::json& json, std::string_view key,
                     const CounterResult& counter) {
  if (counter.value == 0) return;
  json[key] = ToJson(counter);
}

// Helper: parse optional CounterResult; absent/null yields CounterResult{}, malformed
// propagates.
Result<CounterResult> ParseCounterResult(const nlohmann::json& json,
                                         std::string_view key) {
  auto it = json.find(key);
  if (it == json.end() || it->is_null()) return CounterResult{};
  return CounterResultFromJson(*it);
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
  return {{kUnit, timer.unit},
          {kCount, timer.count},
          {kTotalDuration, timer.total_duration.count()}};
}

Result<TimerResult> TimerResultFromJson(const nlohmann::json& json) {
  ICEBERG_ASSIGN_OR_RAISE(auto count, GetJsonValue<int64_t>(json, kCount));
  ICEBERG_ASSIGN_OR_RAISE(auto total, GetJsonValue<int64_t>(json, kTotalDuration));
  TimerResult result{.count = count, .total_duration = std::chrono::nanoseconds{total}};
  if (auto it = json.find(kUnit); it != json.end() && it->is_string()) {
    result.unit = it->get<std::string>();
  }
  return result;
}

// ---------------------------------------------------------------------------
// ScanMetricsResult
// ---------------------------------------------------------------------------

nlohmann::json ToJson(const ScanMetricsResult& m) {
  nlohmann::json json = nlohmann::json::object();
  if (m.total_planning_duration.count > 0) {
    json[std::string(kTotalPlanningDuration)] = ToJson(m.total_planning_duration);
  }
  SetCounterField(json, kResultDataFiles, m.result_data_files);
  SetCounterField(json, kResultDeleteFiles, m.result_delete_files);
  SetCounterField(json, kScannedDataManifests, m.scanned_data_manifests);
  SetCounterField(json, kScannedDeleteManifests, m.scanned_delete_manifests);
  SetCounterField(json, kTotalDataManifests, m.total_data_manifests);
  SetCounterField(json, kTotalDeleteManifests, m.total_delete_manifests);
  SetCounterField(json, kTotalFileSizeInBytes, m.total_file_size_in_bytes);
  SetCounterField(json, kTotalDeleteFileSizeInBytes, m.total_delete_file_size_in_bytes);
  SetCounterField(json, kSkippedDataManifests, m.skipped_data_manifests);
  SetCounterField(json, kSkippedDeleteManifests, m.skipped_delete_manifests);
  SetCounterField(json, kSkippedDataFiles, m.skipped_data_files);
  SetCounterField(json, kSkippedDeleteFiles, m.skipped_delete_files);
  SetCounterField(json, kIndexedDeleteFiles, m.indexed_delete_files);
  SetCounterField(json, kEqualityDeleteFiles, m.equality_delete_files);
  SetCounterField(json, kPositionalDeleteFiles, m.positional_delete_files);
  SetCounterField(json, kDvs, m.dvs);
  return json;
}

Result<ScanMetricsResult> ScanMetricsResultFromJson(const nlohmann::json& json) {
  ScanMetricsResult m;
  ICEBERG_ASSIGN_OR_RAISE(m.total_planning_duration,
                          ParseTimerResult(json, kTotalPlanningDuration));
  ICEBERG_ASSIGN_OR_RAISE(m.result_data_files,
                          ParseCounterResult(json, kResultDataFiles));
  ICEBERG_ASSIGN_OR_RAISE(m.result_delete_files,
                          ParseCounterResult(json, kResultDeleteFiles));
  ICEBERG_ASSIGN_OR_RAISE(m.scanned_data_manifests,
                          ParseCounterResult(json, kScannedDataManifests));
  ICEBERG_ASSIGN_OR_RAISE(m.scanned_delete_manifests,
                          ParseCounterResult(json, kScannedDeleteManifests));
  ICEBERG_ASSIGN_OR_RAISE(m.total_data_manifests,
                          ParseCounterResult(json, kTotalDataManifests));
  ICEBERG_ASSIGN_OR_RAISE(m.total_delete_manifests,
                          ParseCounterResult(json, kTotalDeleteManifests));
  ICEBERG_ASSIGN_OR_RAISE(m.total_file_size_in_bytes,
                          ParseCounterResult(json, kTotalFileSizeInBytes));
  ICEBERG_ASSIGN_OR_RAISE(m.total_delete_file_size_in_bytes,
                          ParseCounterResult(json, kTotalDeleteFileSizeInBytes));
  ICEBERG_ASSIGN_OR_RAISE(m.skipped_data_manifests,
                          ParseCounterResult(json, kSkippedDataManifests));
  ICEBERG_ASSIGN_OR_RAISE(m.skipped_delete_manifests,
                          ParseCounterResult(json, kSkippedDeleteManifests));
  ICEBERG_ASSIGN_OR_RAISE(m.skipped_data_files,
                          ParseCounterResult(json, kSkippedDataFiles));
  ICEBERG_ASSIGN_OR_RAISE(m.skipped_delete_files,
                          ParseCounterResult(json, kSkippedDeleteFiles));
  ICEBERG_ASSIGN_OR_RAISE(m.indexed_delete_files,
                          ParseCounterResult(json, kIndexedDeleteFiles));
  ICEBERG_ASSIGN_OR_RAISE(m.equality_delete_files,
                          ParseCounterResult(json, kEqualityDeleteFiles));
  ICEBERG_ASSIGN_OR_RAISE(m.positional_delete_files,
                          ParseCounterResult(json, kPositionalDeleteFiles));
  ICEBERG_ASSIGN_OR_RAISE(m.dvs, ParseCounterResult(json, kDvs));
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
  SetCounterField(json, kAttempts, m.attempts);
  SetCounterField(json, kAddedDataFiles, m.added_data_files);
  SetCounterField(json, kRemovedDataFiles, m.removed_data_files);
  SetCounterField(json, kTotalDataFiles, m.total_data_files);
  SetCounterField(json, kAddedDeleteFiles, m.added_delete_files);
  SetCounterField(json, kAddedEqualityDeleteFiles, m.added_equality_delete_files);
  SetCounterField(json, kAddedPositionalDeleteFiles, m.added_positional_delete_files);
  SetCounterField(json, kAddedDvs, m.added_dvs);
  SetCounterField(json, kRemovedPositionalDeleteFiles, m.removed_positional_delete_files);
  SetCounterField(json, kRemovedDvs, m.removed_dvs);
  SetCounterField(json, kRemovedEqualityDeleteFiles, m.removed_equality_delete_files);
  SetCounterField(json, kRemovedDeleteFiles, m.removed_delete_files);
  SetCounterField(json, kTotalDeleteFiles, m.total_delete_files);
  SetCounterField(json, kAddedRecords, m.added_records);
  SetCounterField(json, kRemovedRecords, m.removed_records);
  SetCounterField(json, kTotalRecords, m.total_records);
  SetCounterField(json, kAddedFilesSizeBytes, m.added_files_size_bytes);
  SetCounterField(json, kRemovedFilesSizeBytes, m.removed_files_size_bytes);
  SetCounterField(json, kTotalFilesSizeBytes, m.total_files_size_bytes);
  SetCounterField(json, kAddedPositionalDeletes, m.added_positional_deletes);
  SetCounterField(json, kRemovedPositionalDeletes, m.removed_positional_deletes);
  SetCounterField(json, kTotalPositionalDeletes, m.total_positional_deletes);
  SetCounterField(json, kAddedEqualityDeletes, m.added_equality_deletes);
  SetCounterField(json, kRemovedEqualityDeletes, m.removed_equality_deletes);
  SetCounterField(json, kTotalEqualityDeletes, m.total_equality_deletes);
  SetCounterField(json, kKeptManifestCount, m.kept_manifest_count);
  SetCounterField(json, kCreatedManifestCount, m.created_manifest_count);
  SetCounterField(json, kReplacedManifestCount, m.replaced_manifest_count);
  SetCounterField(json, kProcessedManifestEntriesCount,
                  m.processed_manifest_entries_count);
  return json;
}

Result<CommitMetricsResult> CommitMetricsResultFromJson(const nlohmann::json& json) {
  CommitMetricsResult m;
  ICEBERG_ASSIGN_OR_RAISE(m.total_duration, ParseTimerResult(json, kTotalDuration));
  ICEBERG_ASSIGN_OR_RAISE(m.attempts, ParseCounterResult(json, kAttempts));
  ICEBERG_ASSIGN_OR_RAISE(m.added_data_files, ParseCounterResult(json, kAddedDataFiles));
  ICEBERG_ASSIGN_OR_RAISE(m.removed_data_files,
                          ParseCounterResult(json, kRemovedDataFiles));
  ICEBERG_ASSIGN_OR_RAISE(m.total_data_files, ParseCounterResult(json, kTotalDataFiles));
  ICEBERG_ASSIGN_OR_RAISE(m.added_delete_files,
                          ParseCounterResult(json, kAddedDeleteFiles));
  ICEBERG_ASSIGN_OR_RAISE(m.added_equality_delete_files,
                          ParseCounterResult(json, kAddedEqualityDeleteFiles));
  ICEBERG_ASSIGN_OR_RAISE(m.added_positional_delete_files,
                          ParseCounterResult(json, kAddedPositionalDeleteFiles));
  ICEBERG_ASSIGN_OR_RAISE(m.added_dvs, ParseCounterResult(json, kAddedDvs));
  ICEBERG_ASSIGN_OR_RAISE(m.removed_positional_delete_files,
                          ParseCounterResult(json, kRemovedPositionalDeleteFiles));
  ICEBERG_ASSIGN_OR_RAISE(m.removed_dvs, ParseCounterResult(json, kRemovedDvs));
  ICEBERG_ASSIGN_OR_RAISE(m.removed_equality_delete_files,
                          ParseCounterResult(json, kRemovedEqualityDeleteFiles));
  ICEBERG_ASSIGN_OR_RAISE(m.removed_delete_files,
                          ParseCounterResult(json, kRemovedDeleteFiles));
  ICEBERG_ASSIGN_OR_RAISE(m.total_delete_files,
                          ParseCounterResult(json, kTotalDeleteFiles));
  ICEBERG_ASSIGN_OR_RAISE(m.added_records, ParseCounterResult(json, kAddedRecords));
  ICEBERG_ASSIGN_OR_RAISE(m.removed_records, ParseCounterResult(json, kRemovedRecords));
  ICEBERG_ASSIGN_OR_RAISE(m.total_records, ParseCounterResult(json, kTotalRecords));
  ICEBERG_ASSIGN_OR_RAISE(m.added_files_size_bytes,
                          ParseCounterResult(json, kAddedFilesSizeBytes));
  ICEBERG_ASSIGN_OR_RAISE(m.removed_files_size_bytes,
                          ParseCounterResult(json, kRemovedFilesSizeBytes));
  ICEBERG_ASSIGN_OR_RAISE(m.total_files_size_bytes,
                          ParseCounterResult(json, kTotalFilesSizeBytes));
  ICEBERG_ASSIGN_OR_RAISE(m.added_positional_deletes,
                          ParseCounterResult(json, kAddedPositionalDeletes));
  ICEBERG_ASSIGN_OR_RAISE(m.removed_positional_deletes,
                          ParseCounterResult(json, kRemovedPositionalDeletes));
  ICEBERG_ASSIGN_OR_RAISE(m.total_positional_deletes,
                          ParseCounterResult(json, kTotalPositionalDeletes));
  ICEBERG_ASSIGN_OR_RAISE(m.added_equality_deletes,
                          ParseCounterResult(json, kAddedEqualityDeletes));
  ICEBERG_ASSIGN_OR_RAISE(m.removed_equality_deletes,
                          ParseCounterResult(json, kRemovedEqualityDeletes));
  ICEBERG_ASSIGN_OR_RAISE(m.total_equality_deletes,
                          ParseCounterResult(json, kTotalEqualityDeletes));
  ICEBERG_ASSIGN_OR_RAISE(m.kept_manifest_count,
                          ParseCounterResult(json, kKeptManifestCount));
  ICEBERG_ASSIGN_OR_RAISE(m.created_manifest_count,
                          ParseCounterResult(json, kCreatedManifestCount));
  ICEBERG_ASSIGN_OR_RAISE(m.replaced_manifest_count,
                          ParseCounterResult(json, kReplacedManifestCount));
  ICEBERG_ASSIGN_OR_RAISE(m.processed_manifest_entries_count,
                          ParseCounterResult(json, kProcessedManifestEntriesCount));
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
