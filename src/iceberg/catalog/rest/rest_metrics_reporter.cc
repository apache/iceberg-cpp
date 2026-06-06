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

#include "iceberg/catalog/rest/rest_metrics_reporter.h"

#include <cstdio>
#include <utility>

#include <nlohmann/json.hpp>

#include "iceberg/catalog/rest/auth/auth_session.h"
#include "iceberg/catalog/rest/error_handlers.h"
#include "iceberg/catalog/rest/http_client.h"
#include "iceberg/metrics/json_serde_internal.h"
#include "iceberg/metrics/metrics_reporter.h"

namespace iceberg::rest {

namespace {

constexpr std::string_view kReportType = "report-type";
constexpr std::string_view kScanReportType = "scan-report";
constexpr std::string_view kCommitReportType = "commit-report";

}  // namespace

RestMetricsReporter::RestMetricsReporter(std::shared_ptr<HttpClient> client,
                                         std::string metrics_endpoint,
                                         std::shared_ptr<auth::AuthSession> session)
    : client_(std::move(client)),
      metrics_endpoint_(std::move(metrics_endpoint)),
      session_(std::move(session)) {}

Status RestMetricsReporter::Report(const MetricsReport& report) {
  try {
    // Serialize the report variant to JSON.
    Result<nlohmann::json> json_result = std::visit(
        [](const auto& r) -> Result<nlohmann::json> { return ToJson(r); }, report);
    if (!json_result) {
      return {};
    }

    // Inject "report-type" required by the REST spec (not included in core ToJson).
    auto& json = json_result.value();
    json[kReportType] =
        std::holds_alternative<ScanReport>(report) ? kScanReportType : kCommitReportType;

    // POST to the metrics endpoint; suppress errors to match Java fire-and-forget
    // behavior.
    std::ignore = client_->Post(metrics_endpoint_, json.dump(), /*headers=*/{},
                                *DefaultErrorHandler::Instance(), *session_);
  } catch (const std::exception&) {
    return {};
  }
  return {};
}

}  // namespace iceberg::rest
