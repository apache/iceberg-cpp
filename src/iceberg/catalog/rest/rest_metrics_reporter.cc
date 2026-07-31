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

#include <utility>

#include <nlohmann/json.hpp>

#include "iceberg/catalog/rest/auth/auth_session.h"
#include "iceberg/catalog/rest/rest_metrics_reporter_internal.h"
#include "iceberg/metrics/json_serde_internal.h"
#include "iceberg/metrics/metrics_reporter.h"
#include "iceberg/util/executor.h"

namespace iceberg::rest {

namespace {

constexpr std::string_view kReportType = "report-type";
constexpr std::string_view kScanReportType = "scan-report";
constexpr std::string_view kCommitReportType = "commit-report";

}  // namespace

RestMetricsReporter::RestMetricsReporter(std::string metrics_endpoint,
                                         std::shared_ptr<auth::AuthSession> session,
                                         Executor* executor, RestMetricsPost post)
    : metrics_endpoint_(std::move(metrics_endpoint)),
      session_(std::move(session)),
      executor_(executor),
      post_(std::move(post)) {}

Result<std::string> RestMetricsReporter::BuildRequestBody(const MetricsReport& report) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto json,
      std::visit([](const auto& r) -> Result<nlohmann::json> { return ToJson(r); },
                 report));

  json[kReportType] =
      std::holds_alternative<ScanReport>(report) ? kScanReportType : kCommitReportType;
  return json.dump();
}

Status RestMetricsReporter::Report(const MetricsReport& report) {
  try {
    auto body_result = BuildRequestBody(report);
    if (!body_result || !session_ || !post_) {
      return {};
    }

    ExecutorTask task([post = post_, endpoint = metrics_endpoint_,
                       body = std::move(*body_result), session = session_]() mutable {
      try {
        post(endpoint, body, *session);
      } catch (...) {
      }
    });
    if (executor_) {
      std::ignore = executor_->Submit(std::move(task));
    } else {
      std::move(task)();
    }
  } catch (...) {
  }
  return {};
}

}  // namespace iceberg::rest
