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

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/catalog/rest/auth/auth_session.h"
#include "iceberg/catalog/rest/error_handlers.h"
#include "iceberg/catalog/rest/http_client.h"
#include "iceberg/metrics/commit_report.h"
#include "iceberg/metrics/metrics_reporter.h"
#include "iceberg/metrics/scan_report.h"
#include "iceberg/result.h"
#include "iceberg/test/matchers.h"

namespace iceberg::rest {

namespace {

// A minimal HttpClientBase test double: RestMetricsReporter only ever calls Post(), so
// only that method needs to be mockable (see the migration comment on HttpClientBase).
class MockHttpClient : public HttpClientBase {
 public:
  MOCK_METHOD(Result<HttpResponse>, Post,
              (const std::string& path, const std::string& body,
               (const std::unordered_map<std::string, std::string>& headers),
               const ErrorHandler& error_handler, auth::AuthSession& session),
              (override));
};

}  // namespace

class RestMetricsReporterTest : public ::testing::Test {
 protected:
  void SetUp() override {
    client_ = std::make_shared<HttpClient>();
    session_ = auth::AuthSession::MakeDefault({});
  }

  std::shared_ptr<HttpClient> client_;
  std::shared_ptr<auth::AuthSession> session_;
};

namespace {

// A Scan/Commit report test case: the report to send plus what a correct request for
// it must contain. Parameterizing over this collapses what would otherwise be 3
// near-identical Scan/Commit test-body pairs into 3 bodies total.
struct ReportTestCase {
  std::string name;  // instantiation test-name suffix, e.g. "ScanReport"
  MetricsReport report;
  std::string expected_report_type;
  std::vector<std::pair<std::string, nlohmann::json>> expected_fields;
};

ReportTestCase MakeScanReportCase() {
  ScanReport report;
  report.table_name = "ns.tbl";
  report.snapshot_id = 42;
  report.schema_id = 0;
  return {"ScanReport",
          report,
          "scan-report",
          {{"table-name", "ns.tbl"}, {"snapshot-id", 42}}};
}

ReportTestCase MakeCommitReportCase() {
  CommitReport report;
  report.table_name = "ns.tbl";
  report.snapshot_id = 99;
  report.sequence_number = 1;
  report.operation = "append";
  return {"CommitReport",
          report,
          "commit-report",
          {{"table-name", "ns.tbl"},
           {"snapshot-id", 99},
           {"sequence-number", 1},
           {"operation", "append"}}};
}

}  // namespace

class RestMetricsReporterPayloadTest
    : public RestMetricsReporterTest,
      public ::testing::WithParamInterface<ReportTestCase> {};

// Report() must return OK even when the HTTP call fails (connection refused).
// This validates the fire-and-forget error-suppression contract matching Java behavior.
TEST_P(RestMetricsReporterPayloadTest, ReportSuppressesHttpErrors) {
  RestMetricsReporter reporter(client_, "http://localhost:0/v1/ns/tables/tbl/metrics",
                               session_);
  EXPECT_THAT(reporter.Report(GetParam().report), IsOk());
}

// Verify that Report() actually calls Post() with the configured metrics endpoint and
// the correct serialized body, including the `report-type` field required by the REST
// metrics spec.
TEST_P(RestMetricsReporterPayloadTest, ReportPostsToConfiguredEndpoint) {
  const auto& test_case = GetParam();
  auto mock_client = std::make_shared<MockHttpClient>();
  const std::string endpoint = "http://mock-host/v1/ns/tables/tbl/metrics";

  std::string captured_path;
  std::string captured_body;
  EXPECT_CALL(*mock_client,
              Post(::testing::_, ::testing::_, ::testing::_, ::testing::_, ::testing::_))
      .WillOnce([&](const std::string& path, const std::string& body,
                    const std::unordered_map<std::string, std::string>&,
                    const ErrorHandler&, auth::AuthSession&) -> Result<HttpResponse> {
        captured_path = path;
        captured_body = body;
        return Result<HttpResponse>(HttpResponse{});
      });

  RestMetricsReporter reporter(mock_client, endpoint, session_);
  EXPECT_THAT(reporter.Report(test_case.report), IsOk());

  EXPECT_EQ(captured_path, endpoint);
  auto json = nlohmann::json::parse(captured_body);
  EXPECT_EQ(json.at("report-type"), test_case.expected_report_type);
  for (const auto& [key, value] : test_case.expected_fields) {
    EXPECT_EQ(json.at(key), value);
  }
}

INSTANTIATE_TEST_SUITE_P(ScanAndCommit, RestMetricsReporterPayloadTest,
                         ::testing::Values(MakeScanReportCase(), MakeCommitReportCase()),
                         [](const ::testing::TestParamInfo<ReportTestCase>& info) {
                           return info.param.name;
                         });

}  // namespace iceberg::rest
