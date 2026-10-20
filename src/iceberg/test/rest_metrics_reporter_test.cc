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

#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/catalog/rest/auth/auth_session.h"
#include "iceberg/catalog/rest/rest_metrics_reporter_internal.h"
#include "iceberg/metrics/commit_report.h"
#include "iceberg/metrics/metrics_reporter.h"
#include "iceberg/metrics/scan_report.h"
#include "iceberg/result.h"
#include "iceberg/test/executor.h"
#include "iceberg/test/matchers.h"
#include "iceberg/util/executor.h"

namespace iceberg::rest {

namespace {

// A Submit action that keeps the task so the test decides when it runs.
auto CaptureTask(std::optional<ExecutorTask>& slot) {
  return [&slot](ExecutorTask task) -> Status {
    slot.emplace(std::move(task));
    return {};
  };
}

}  // namespace

class RestMetricsReporterTest : public ::testing::Test {
 protected:
  void SetUp() override { session_ = auth::AuthSession::MakeDefault({}); }

  std::shared_ptr<auth::AuthSession> session_;
};

namespace {

struct ReportTestCase {
  std::string name;
  MetricsReport report;
  std::string expected_report_type;
  std::vector<std::pair<std::string, nlohmann::json>> expected_fields;
};

ScanReport MakeScanReport() {
  ScanReport report;
  report.table_name = "ns.tbl";
  report.snapshot_id = 42;
  report.schema_id = 0;
  return report;
}

ReportTestCase MakeScanReportCase() {
  return {.name = "ScanReport",
          .report = MakeScanReport(),
          .expected_report_type = "scan-report",
          .expected_fields = {{"table-name", "ns.tbl"}, {"snapshot-id", 42}}};
}

ReportTestCase MakeCommitReportCase() {
  CommitReport report;
  report.table_name = "ns.tbl";
  report.snapshot_id = 99;
  report.sequence_number = 1;
  report.operation = "append";
  return {.name = "CommitReport",
          .report = report,
          .expected_report_type = "commit-report",
          .expected_fields = {{"table-name", "ns.tbl"},
                              {"snapshot-id", 99},
                              {"sequence-number", 1},
                              {"operation", "append"}}};
}

}  // namespace

class RestMetricsReporterPayloadTest
    : public RestMetricsReporterTest,
      public ::testing::WithParamInterface<ReportTestCase> {};

TEST_F(RestMetricsReporterTest, SuppressesHttpErrors) {
  test::MockExecutor executor;
  std::optional<ExecutorTask> task;
  EXPECT_CALL(executor, Submit).WillOnce(CaptureTask(task));
  RestMetricsReporter reporter(
      "http://localhost:0/v1/ns/tables/tbl/metrics", session_, &executor,
      [](const std::string&, const std::string&, auth::AuthSession&) {
        throw std::runtime_error("HTTP failure");
      });
  MetricsReport report = MakeScanReport();
  EXPECT_THAT(reporter.Report(report), IsOk());
  ASSERT_TRUE(task.has_value());
  EXPECT_NO_THROW(std::move(*task)());
}

TEST_P(RestMetricsReporterPayloadTest, PostsSerializedPayloadToConfiguredEndpoint) {
  const auto& test_case = GetParam();
  test::MockExecutor executor;
  std::optional<ExecutorTask> task;
  EXPECT_CALL(executor, Submit).WillOnce(CaptureTask(task));
  const std::string endpoint = "http://mock-host/v1/ns/tables/tbl/metrics";

  std::string captured_path;
  std::string captured_body;
  RestMetricsReporter reporter(
      endpoint, session_, &executor,
      [&](const std::string& path, const std::string& body, auth::AuthSession&) {
        captured_path = path;
        captured_body = body;
      });

  EXPECT_THAT(reporter.Report(test_case.report), IsOk());
  EXPECT_TRUE(captured_path.empty());
  ASSERT_TRUE(task.has_value());
  std::move (*task)();

  EXPECT_EQ(captured_path, endpoint);
  auto json = nlohmann::json::parse(captured_body);
  EXPECT_EQ(json.at("report-type"), test_case.expected_report_type);
  for (const auto& [key, value] : test_case.expected_fields) {
    EXPECT_EQ(json.at(key), value);
  }
}

TEST_F(RestMetricsReporterTest, PostsSynchronouslyWithoutExecutor) {
  bool posted = false;
  RestMetricsReporter reporter(
      "http://mock-host/v1/ns/tables/tbl/metrics", session_, nullptr,
      [&](const std::string&, const std::string&, auth::AuthSession&) { posted = true; });

  MetricsReport report = MakeScanReport();
  EXPECT_THAT(reporter.Report(report), IsOk());
  EXPECT_TRUE(posted);
}

TEST_F(RestMetricsReporterTest, SuppressesExecutorRejection) {
  test::ThreadExecutor executor(IOError("executor rejected task"));
  RestMetricsReporter reporter(
      "http://mock-host/v1/ns/tables/tbl/metrics", session_, &executor,
      [](const std::string&, const std::string&, auth::AuthSession&) {});

  MetricsReport report = MakeScanReport();
  EXPECT_THAT(reporter.Report(report), IsOk());
}

INSTANTIATE_TEST_SUITE_P(ScanAndCommit, RestMetricsReporterPayloadTest,
                         ::testing::Values(MakeScanReportCase(), MakeCommitReportCase()),
                         [](const ::testing::TestParamInfo<ReportTestCase>& info) {
                           return info.param.name;
                         });

}  // namespace iceberg::rest
