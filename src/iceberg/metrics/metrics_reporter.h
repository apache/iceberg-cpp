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

#include <string_view>
#include <variant>

#include "iceberg/iceberg_export.h"
#include "iceberg/metrics/commit_report.h"
#include "iceberg/metrics/scan_report.h"

namespace iceberg {

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
