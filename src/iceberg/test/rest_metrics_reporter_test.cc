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

#include <gtest/gtest.h>

#include "iceberg/catalog/rest/auth/auth_session.h"
#include "iceberg/catalog/rest/http_client.h"
#include "iceberg/metrics/commit_report.h"
#include "iceberg/metrics/metrics_reporter.h"
#include "iceberg/metrics/scan_report.h"
#include "iceberg/test/matchers.h"

namespace iceberg::rest {

class RestMetricsReporterTest : public ::testing::Test {
 protected:
  void SetUp() override {
    client_ = std::make_shared<HttpClient>();
    session_ = auth::AuthSession::MakeDefault({});
  }

  std::shared_ptr<HttpClient> client_;
  std::shared_ptr<auth::AuthSession> session_;
};

// Report() must return OK even when the HTTP call fails (connection refused).
// This validates the fire-and-forget error-suppression contract matching Java behavior.
TEST_F(RestMetricsReporterTest, ReportSuppressesHttpErrorsForScanReport) {
  RestMetricsReporter reporter(client_, "http://localhost:0/v1/ns/tables/tbl/metrics",
                               session_);

  ScanReport report;
  report.table_name = "ns.tbl";
  report.snapshot_id = 42;
  report.schema_id = 0;
  // Leave filter/metrics as default; serialization should still succeed.

  EXPECT_THAT(reporter.Report(report), IsOk());
}

TEST_F(RestMetricsReporterTest, ReportSuppressesHttpErrorsForCommitReport) {
  RestMetricsReporter reporter(client_, "http://localhost:0/v1/ns/tables/tbl/metrics",
                               session_);

  CommitReport report;
  report.table_name = "ns.tbl";
  report.snapshot_id = 99;
  report.sequence_number = 1;
  report.operation = "append";

  EXPECT_THAT(reporter.Report(report), IsOk());
}

}  // namespace iceberg::rest
