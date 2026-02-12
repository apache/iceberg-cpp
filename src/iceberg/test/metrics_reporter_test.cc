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

#include "iceberg/metrics_reporter.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/metrics_reporters.h"

namespace iceberg {

class CollectingMetricsReporter : public MetricsReporter {
 public:
  static Result<std::unique_ptr<MetricsReporter>> Make(
      [[maybe_unused]] std::string_view name,
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
  MetricsReporters::Register(
      "collecting",
      [](std::string_view name, const std::unordered_map<std::string, std::string>& props)
          -> Result<std::unique_ptr<MetricsReporter>> {
        return CollectingMetricsReporter::Make(name, props);
      });

  // Load the custom reporter
  std::unordered_map<std::string, std::string> properties = {
      {std::string(kMetricsReporterType), "collecting"}};
  auto result = MetricsReporters::Load("test", properties);

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

TEST(CustomMetricsReporterTest, RegisterCaseInsensitive) {
  // Register with uppercase
  MetricsReporters::Register(
      "UPPERCASE",
      [](std::string_view, const std::unordered_map<std::string, std::string>&)
          -> Result<std::unique_ptr<MetricsReporter>> {
        return std::make_unique<CollectingMetricsReporter>();
      });

  // Load with lowercase
  std::unordered_map<std::string, std::string> properties = {
      {std::string(kMetricsReporterType), "uppercase"}};
  auto result = MetricsReporters::Load("test", properties);

  ASSERT_TRUE(result.has_value());
  ASSERT_NE(result.value(), nullptr);
}

}  // namespace iceberg
