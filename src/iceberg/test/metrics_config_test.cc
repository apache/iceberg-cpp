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

#include "iceberg/metrics_config.h"

#include <memory>
#include <unordered_map>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/table_properties.h"
#include "iceberg/test/matchers.h"

namespace iceberg {

class MetricsConfigTest : public ::testing::Test {
 protected:
  void SetUp() override {
    SchemaField field1(1, "col1", std::make_shared<LongType>(), false);
    SchemaField field2(2, "col2", std::make_shared<StringType>(), true);
    SchemaField field3(3, "col3", std::make_shared<DoubleType>(), false);
    schema_ =
        std::make_unique<Schema>(std::vector<SchemaField>{field1, field2, field3}, 100);
  }

  std::unique_ptr<Schema> schema_;
};

TEST_F(MetricsConfigTest, ValidateColumnReferences) {
  {
    // Empty updates should be valid
    std::unordered_map<std::string, std::string> updates;

    auto result = MetricsConfig::VerifyReferencedColumns(updates, *schema_);
    EXPECT_THAT(result, IsOk()) << "Validation should pass for empty updates";
  }

  {
    // No column references
    std::unordered_map<std::string, std::string> updates;
    updates["write.format.default"] = "parquet";
    updates["write.target-file-size-bytes"] = "524288000";

    auto result = MetricsConfig::VerifyReferencedColumns(updates, *schema_);
    EXPECT_THAT(result, IsOk())
        << "Validation should pass when no column references exist";
  }

  {
    // Valid column reference
    std::unordered_map<std::string, std::string> updates;
    updates[std::string(TableProperties::kMetricModeColumnConfPrefix) + "col1"] =
        "counts";
    updates[std::string(TableProperties::kMetricModeColumnConfPrefix) + "col2"] = "full";
    updates["some.other.property"] = "value";

    auto result = MetricsConfig::VerifyReferencedColumns(updates, *schema_);
    EXPECT_THAT(result, IsOk()) << "Validation should pass for valid column references";
  }

  {
    // Invalid column reference
    std::unordered_map<std::string, std::string> updates;
    updates[std::string(TableProperties::kMetricModeColumnConfPrefix) + "nonexistent"] =
        "counts";

    auto result = MetricsConfig::VerifyReferencedColumns(updates, *schema_);
    EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed))
        << "Validation should fail for invalid column references";
  }

  {
    // Mixed valid and invalid column references
    std::unordered_map<std::string, std::string> updates;
    updates[std::string(TableProperties::kMetricModeColumnConfPrefix) + "col1"] =
        "counts";
    updates[std::string(TableProperties::kMetricModeColumnConfPrefix) + "nonexistent"] =
        "full";

    auto result = MetricsConfig::VerifyReferencedColumns(updates, *schema_);
    EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed))
        << "Validation should fail when any column reference is invalid";
  }
}

}  // namespace iceberg
