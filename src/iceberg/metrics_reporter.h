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
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "iceberg/iceberg_export.h"

namespace iceberg {

/// \brief Duration type for metrics reporting in milliseconds.
using DurationMs = std::chrono::milliseconds;

/// \brief Report generated after a table scan operation.
///
/// Contains metrics about the planning and execution of a table scan,
/// including information about manifests and data files processed.
struct ICEBERG_EXPORT ScanReport {
  /// \brief The fully qualified name of the table that was scanned.
  std::string table_name;

  /// \brief Snapshot ID that was scanned, if available.
  int64_t snapshot_id = -1;

  /// \brief Filter expression used in the scan, if any.
  std::string filter;

  /// \brief Schema ID.
  int32_t schema_id = -1;

  /// \brief Total duration of the entire scan operation.
  DurationMs total_duration{0};

  /// \brief Duration spent planning the scan.
  DurationMs total_planning_duration{0};

  /// \brief Number of data files in the scan result.
  int64_t result_data_files = 0;

  /// \brief Number of delete files in the scan result.
  int64_t result_delete_files = 0;

  /// \brief Total number of data manifests.
  int64_t total_data_manifests = 0;

  /// \brief Number of data manifests that were skipped.
  int64_t skipped_data_files = 0;

  /// \brief Number of data manifests that were skipped.
  int64_t skipped_delete_files = 0;

  /// \brief Number of data manifests that were scanned.
  int64_t scanned_data_manifests = 0;

  /// \brief Number of data manifests that were skipped due to filtering.
  int64_t skipped_data_manifests = 0;

  /// \brief Total number of delete manifests.
  int64_t total_delete_manifests = 0;

  /// \brief Number of delete manifests that were scanned.
  int64_t scanned_delete_manifests = 0;

  /// \brief Number of delete manifests that were skipped.
  int64_t skipped_delete_manifests = 0;

  /// \brief Projected field IDs from the scan schema.
  std::vector<int32_t> projected_field_ids;
  /// \brief Projected field names from the scan schema.
  std::vector<std::string> projected_field_names;
  /// \brief Total size in bytes of all result data files.
  int64_t total_file_size_in_bytes = 0;
  /// \brief Total size in bytes of all result delete files.
  int64_t total_delete_file_size_in_bytes = 0;
  /// \brief Number of indexed delete files.
  int64_t indexed_delete_files = 0;
  /// \brief Number of equality delete files in the scan result.
  int64_t equality_delete_files = 0;
  /// \brief Number of positional delete files in the scan result.
  int64_t positional_delete_files = 0;
  /// \brief Number of deletion vectors in the scan result.
  int64_t dvs = 0;
  /// \brief Additional key-value metadata.
  std::unordered_map<std::string, std::string> metadata;
};

/// \brief Report generated after a commit operation.
///
/// Contains metrics about the changes made in a commit, including
/// files added/removed and retry information.
struct ICEBERG_EXPORT CommitReport {
  /// \brief The fully qualified name of the table that was modified.
  std::string table_name;

  /// \brief The snapshot ID created by this commit.
  int64_t snapshot_id = -1;

  /// \brief The sequence number assigned to this commit.
  int64_t sequence_number = -1;

  /// \brief The operation that was performed (append, overwrite, delete, etc.).
  std::string operation;

  /// \brief Number of commit attempts (1 = success on first try).
  int32_t attempts = 1;

  /// \brief Number of data files added in this commit.
  int64_t added_data_files = 0;

  /// \brief Number of data files removed in this commit.
  int64_t removed_data_files = 0;

  /// \brief Total number of data files after this commit.
  int64_t total_data_files = 0;

  /// \brief Number of delete files added in this commit.
  int64_t added_delete_files = 0;

  /// \brief Number of delete files removed in this commit.
  int64_t removed_delete_files = 0;

  /// \brief Total number of delete files after this commit.
  int64_t total_delete_files = 0;

  /// \brief Number of records added in this commit.
  int64_t added_records = 0;

  /// \brief Number of records removed in this commit.
  int64_t removed_records = 0;

  /// \brief Size in bytes of files added.
  int64_t added_files_size = 0;

  /// \brief Size in bytes of files removed.
  int64_t removed_files_size = 0;

  /// \brief Total duration of the commit operation.
  DurationMs total_duration{0};
  /// \brief Total records after this commit.
  int64_t total_records = 0;
  /// \brief Total file size in bytes after this commit.
  int64_t total_files_size = 0;
  /// \brief Equality delete files added.
  int64_t added_equality_delete_files = 0;
  /// \brief Equality delete files removed.
  int64_t removed_equality_delete_files = 0;
  /// \brief Positional delete files added.
  int64_t added_positional_delete_files = 0;
  /// \brief Positional delete files removed.
  int64_t removed_positional_delete_files = 0;
  /// \brief Position delete records added.
  int64_t added_positional_deletes = 0;
  /// \brief Position delete records removed.
  int64_t removed_positional_deletes = 0;
  /// \brief Total position delete records.
  int64_t total_positional_deletes = 0;
  /// \brief Equality delete records added.
  int64_t added_equality_deletes = 0;
  /// \brief Equality delete records removed.
  int64_t removed_equality_deletes = 0;
  /// \brief Total equality delete records.
  int64_t total_equality_deletes = 0;
  /// \brief Deletion vectors added.
  int64_t added_dvs = 0;
  /// \brief Deletion vectors removed.
  int64_t removed_dvs = 0;
  /// \brief Manifests created in this commit.
  int64_t manifests_created = 0;
  /// \brief Manifests replaced in this commit.
  int64_t manifests_replaced = 0;
  /// \brief Manifests kept in this commit.
  int64_t manifests_kept = 0;
  /// \brief Manifest entries processed.
  int64_t manifest_entries_processed = 0;
  /// \brief Additional key-value metadata.
  std::unordered_map<std::string, std::string> metadata;
};

/// \brief The type of a metrics report.
enum class MetricsReportType {
  kScanReport,
  kCommitReport,
};

/// \brief Get the string representation of a metrics report type.
ICEBERG_EXPORT constexpr std::string_view ToString(MetricsReportType type) noexcept {
  switch (type) {
    case MetricsReportType::kScanReport:
      return "scan";
    case MetricsReportType::kCommitReport:
      return "commit";
  }
  std::unreachable();
}

/// \brief A metrics report, which can be either a ScanReport or CommitReport.
///
/// This variant type allows handling both report types uniformly through
/// the MetricsReporter interface.
using MetricsReport = std::variant<ScanReport, CommitReport>;

/// \brief Get the type of a metrics report.
///
/// \param report The metrics report to get the type of.
/// \return The type of the metrics report.
ICEBERG_EXPORT inline MetricsReportType GetReportType(const MetricsReport& report) {
  return std::visit(
      [](const auto& r) -> MetricsReportType {
        using T = std::decay_t<decltype(r)>;
        if constexpr (std::is_same_v<T, ScanReport>) {
          return MetricsReportType::kScanReport;
        } else {
          return MetricsReportType::kCommitReport;
        }
      },
      report);
}

/// \brief Interface for reporting metrics from Iceberg operations.
///
/// Implementations of this interface can be used to collect and report
/// metrics about scan and commit operations. Common implementations include
/// logging reporters, metrics collectors, and the noop reporter for testing.
class ICEBERG_EXPORT MetricsReporter {
 public:
  virtual ~MetricsReporter() = default;

  /// \brief Report a metrics report.
  ///
  /// Implementations should handle the report according to their purpose
  /// (e.g., logging, sending to a metrics service, etc.).
  ///
  /// \param report The metrics report to process.
  virtual void Report(const MetricsReport& report) = 0;
};

}  // namespace iceberg
