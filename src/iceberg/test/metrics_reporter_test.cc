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

#include "iceberg/metrics/metrics_reporter.h"

#include <chrono>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/metrics/metrics_reporters.h"

namespace iceberg {

class CollectingMetricsReporter : public MetricsReporter {
 public:
  static Result<std::unique_ptr<MetricsReporter>> Make(
      [[maybe_unused]] const std::unordered_map<std::string, std::string>& properties) {
    return std::make_unique<CollectingMetricsReporter>();
  }

  void Report(const MetricsReport& report) override { reports_.push_back(report); }

  const std::vector<MetricsReport>& reports() const { return reports_; }

 private:
  std::vector<MetricsReport> reports_;
};

TEST(CustomMetricsReporterTest, RegisterAndLoad) {
  // Register custom reporter
  MetricsReporters::Register("collecting",
                             [](const std::unordered_map<std::string, std::string>& props)
                                 -> Result<std::unique_ptr<MetricsReporter>> {
                               return CollectingMetricsReporter::Make(props);
                             });

  // Load the custom reporter
  std::unordered_map<std::string, std::string> properties = {
      {std::string(kMetricsReporterImpl), "collecting"}};
  auto result = MetricsReporters::Load(properties);

  ASSERT_TRUE(result.has_value());
  ASSERT_NE(result.value(), nullptr);

  // Report and verify
  auto* reporter = dynamic_cast<CollectingMetricsReporter*>(result.value().get());
  ASSERT_NE(reporter, nullptr);

  ScanReport scan_report{.table_name = "test.table"};
  reporter->Report(scan_report);

  EXPECT_EQ(reporter->reports().size(), 1);
  EXPECT_EQ(GetReportType(reporter->reports()[0]), MetricsReportType::kScanReport);
}

struct ReporterRegistrationParam {
  std::string test_name;
  std::string register_name;
  std::string load_name;
  bool expect_success;
};

class ReporterRegistrationTest
    : public ::testing::TestWithParam<ReporterRegistrationParam> {};

TEST_P(ReporterRegistrationTest, LoadsRegisteredReporter) {
  const auto& param = GetParam();
  MetricsReporters::Register(param.register_name,
                             [](const std::unordered_map<std::string, std::string>&)
                                 -> Result<std::unique_ptr<MetricsReporter>> {
                               return std::make_unique<CollectingMetricsReporter>();
                             });

  std::unordered_map<std::string, std::string> props = {
      {std::string(kMetricsReporterImpl), param.load_name}};
  auto result = MetricsReporters::Load(props);
  EXPECT_EQ(result.has_value(), param.expect_success);
}

INSTANTIATE_TEST_SUITE_P(
    MetricsReporterRegistration, ReporterRegistrationTest,
    ::testing::Values(ReporterRegistrationParam{.test_name = "ExactMatch",
                                                .register_name = "custom1",
                                                .load_name = "custom1",
                                                .expect_success = true},
                      ReporterRegistrationParam{.test_name = "UpperToLower",
                                                .register_name = "UPPER1",
                                                .load_name = "upper1",
                                                .expect_success = true},
                      ReporterRegistrationParam{.test_name = "UnregisteredType",
                                                .register_name = "registered1",
                                                .load_name = "nonexistent1",
                                                .expect_success = false}),
    [](const auto& info) { return info.param.test_name; });

struct VariantDispatchParam {
  std::string test_name;
  MetricsReport report;
  MetricsReportType expected_type;
};

class VariantDispatchTest : public ::testing::TestWithParam<VariantDispatchParam> {};

TEST_P(VariantDispatchTest, CorrectTypeDispatch) {
  const auto& param = GetParam();
  EXPECT_EQ(GetReportType(param.report), param.expected_type);
}

INSTANTIATE_TEST_SUITE_P(
    MetricsReportVariant, VariantDispatchTest,
    ::testing::Values(
        VariantDispatchParam{.test_name = "ScanReportDefault",
                             .report = ScanReport{},
                             .expected_type = MetricsReportType::kScanReport},
        VariantDispatchParam{.test_name = "CommitReportDefault",
                             .report = CommitReport{},
                             .expected_type = MetricsReportType::kCommitReport}),
    [](const auto& info) { return info.param.test_name; });

struct CollectorParam {
  std::string test_name;
  MetricsReport report;
  MetricsReportType expected_type;
  std::string expected_table_name;
};

class CollectorTest : public ::testing::TestWithParam<CollectorParam> {};

TEST_P(CollectorTest, CollectsAndPreservesReport) {
  const auto& param = GetParam();
  CollectingMetricsReporter reporter;
  reporter.Report(param.report);

  ASSERT_EQ(reporter.reports().size(), 1);
  EXPECT_EQ(GetReportType(reporter.reports()[0]), param.expected_type);

  std::visit([&](const auto& r) { EXPECT_EQ(r.table_name, param.expected_table_name); },
             reporter.reports()[0]);
}

INSTANTIATE_TEST_SUITE_P(
    MetricsCollector, CollectorTest,
    ::testing::Values(
        CollectorParam{
            .test_name = "ScanWithFields",
            .report = ScanReport{.table_name = "db.t1",
                                 .snapshot_id = 1,
                                 .scan_metrics = {.total_file_size_in_bytes = 99999}},
            .expected_type = MetricsReportType::kScanReport,
            .expected_table_name = "db.t1"},
        CollectorParam{.test_name = "CommitWithFields",
                       .report = CommitReport{.table_name = "db.t2",
                                              .snapshot_id = 2,
                                              .operation = "append"},
                       .expected_type = MetricsReportType::kCommitReport,
                       .expected_table_name = "db.t2"}),
    [](const auto& info) { return info.param.test_name; });

// ---------------------------------------------------------------------------
// CompositeMetricsReporter / MetricsReporters::Combine tests
// ---------------------------------------------------------------------------

class ThrowingMetricsReporter : public MetricsReporter {
 public:
  void Report([[maybe_unused]] const MetricsReport& report) override {
    throw std::runtime_error("reporter failure");
  }
};

TEST(CombineTest, FlattenNestedComposite) {
  auto a = std::make_shared<CollectingMetricsReporter>();
  auto b = std::make_shared<CollectingMetricsReporter>();
  auto c = std::make_shared<CollectingMetricsReporter>();

  auto ab = MetricsReporters::Combine(a, b);
  auto abc = MetricsReporters::Combine(ab, c);

  // Result must be a flat composite — not a composite-of-composites.
  auto* composite = dynamic_cast<CompositeMetricsReporter*>(abc.get());
  ASSERT_NE(composite, nullptr);
  EXPECT_EQ(composite->Reporters().size(), 3u);
  for (const auto& r : composite->Reporters()) {
    EXPECT_EQ(dynamic_cast<CompositeMetricsReporter*>(r.get()), nullptr);
  }

  abc->Report(CommitReport{.table_name = "db.t2"});
  EXPECT_EQ(a->reports().size(), 1u);
  EXPECT_EQ(b->reports().size(), 1u);
  EXPECT_EQ(c->reports().size(), 1u);
}

TEST(CombineTest, DeduplicateByIdentity) {
  auto a = std::make_shared<CollectingMetricsReporter>();
  auto b = std::make_shared<CollectingMetricsReporter>();

  // ab already contains a and b; combining with b again must not add b twice.
  auto ab = MetricsReporters::Combine(a, b);
  auto result = MetricsReporters::Combine(ab, b);

  auto* composite = dynamic_cast<CompositeMetricsReporter*>(result.get());
  ASSERT_NE(composite, nullptr);
  EXPECT_EQ(composite->Reporters().size(), 2u);

  result->Report(ScanReport{});
  EXPECT_EQ(a->reports().size(), 1u);
  EXPECT_EQ(b->reports().size(), 1u);  // delivered once, not twice
}

TEST(CombineTest, ExceptionInOneReporterDoesNotBlockOthers) {
  auto throwing = std::make_shared<ThrowingMetricsReporter>();
  auto collecting = std::make_shared<CollectingMetricsReporter>();
  auto combined = MetricsReporters::Combine(throwing, collecting);

  EXPECT_NO_THROW(combined->Report(ScanReport{}));
  EXPECT_EQ(collecting->reports().size(), 1u);
}

}  // namespace iceberg
