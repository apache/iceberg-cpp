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

/// \file iceberg/metrics/json_serde.h
/// \brief JSON serialization and deserialization for metrics report types.

#include <nlohmann/json_fwd.hpp>

#include "iceberg/iceberg_export.h"
#include "iceberg/metrics/commit_report.h"
#include "iceberg/metrics/scan_report.h"
#include "iceberg/result.h"

namespace iceberg {

ICEBERG_EXPORT nlohmann::json ToJson(const CounterResult& counter);
ICEBERG_EXPORT Result<CounterResult> CounterResultFromJson(const nlohmann::json& json);

ICEBERG_EXPORT nlohmann::json ToJson(const TimerResult& timer);
ICEBERG_EXPORT Result<TimerResult> TimerResultFromJson(const nlohmann::json& json);

ICEBERG_EXPORT nlohmann::json ToJson(const ScanMetricsResult& metrics);
ICEBERG_EXPORT Result<ScanMetricsResult> ScanMetricsResultFromJson(
    const nlohmann::json& json);

ICEBERG_EXPORT nlohmann::json ToJson(const CommitMetricsResult& metrics);
ICEBERG_EXPORT Result<CommitMetricsResult> CommitMetricsResultFromJson(
    const nlohmann::json& json);

/// \brief Serialize a ScanReport to JSON.
///
/// Returns Result because ScanReport.filter is an Expression whose serialization
/// is fallible.  Returns an error if the filter cannot be serialized.
ICEBERG_EXPORT Result<nlohmann::json> ToJson(const ScanReport& report);
ICEBERG_EXPORT Result<ScanReport> ScanReportFromJson(const nlohmann::json& json);

/// \brief Serialize a CommitReport to JSON.
///
/// Returns nlohmann::json directly (not Result) because CommitReport contains
/// no Expression fields and serialization cannot fail.
ICEBERG_EXPORT nlohmann::json ToJson(const CommitReport& report);
ICEBERG_EXPORT Result<CommitReport> CommitReportFromJson(const nlohmann::json& json);

}  // namespace iceberg
