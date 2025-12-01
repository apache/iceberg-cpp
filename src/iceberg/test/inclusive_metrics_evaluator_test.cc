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

#include "iceberg/expression/inclusive_metrics_evaluator.h"

#include <gtest/gtest.h>

#include "iceberg/expression/binder.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/schema.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"

namespace iceberg {

namespace {
static const bool ROWS_MIGHT_MATCH = true;
static const bool ROWS_CANNOT_MATCH = false;
}  // namespace
using TestVariant = std::variant<bool, int32_t, int64_t, double, std::string>;

class InclusiveMetricsEvaluatorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    schema_ = std::make_shared<Schema>(
        std::vector<SchemaField>{
            SchemaField::MakeRequired(1, "id", int64()),
            SchemaField::MakeOptional(2, "name", string()),
            SchemaField::MakeRequired(3, "age", int32()),
            SchemaField::MakeOptional(4, "salary", float64()),
            SchemaField::MakeRequired(5, "active", boolean()),
            SchemaField::MakeRequired(6, "date", string()),
        },
        /*schema_id=*/0);
  }

  Result<std::shared_ptr<Expression>> Bind(const std::shared_ptr<Expression>& expr,
                                           bool case_sensitive = true) {
    return Binder::Bind(*schema_, expr, case_sensitive);
  }

  std::shared_ptr<DataFile> PrepareDataFile(
      const std::string& partition, int64_t record_count, int64_t file_size_in_bytes,
      const std::map<std::string, TestVariant>& lower_bounds,
      const std::map<std::string, TestVariant>& upper_bounds,
      const std::map<int32_t, int64_t>& value_counts = {},
      const std::map<int32_t, int64_t>& null_counts = {},
      const std::map<int32_t, int64_t>& nan_counts = {}) {
    auto parse_bound = [&](const std::map<std::string, TestVariant>& bounds,
                           std::map<int32_t, std::vector<uint8_t>>& bound_values) {
      for (const auto& [key, value] : bounds) {
        if (key == "id") {
          bound_values[1] = Literal::Long(std::get<int64_t>(value)).Serialize().value();
        } else if (key == "name") {
          bound_values[2] =
              Literal::String(std::get<std::string>(value)).Serialize().value();
        } else if (key == "age") {
          bound_values[3] = Literal::Int(std::get<int32_t>(value)).Serialize().value();
        } else if (key == "salary") {
          bound_values[4] = Literal::Double(std::get<double>(value)).Serialize().value();
        } else if (key == "active") {
          bound_values[5] = Literal::Boolean(std::get<bool>(value)).Serialize().value();
        }
      }
    };

    auto data_file = std::make_shared<DataFile>();
    data_file->file_path = "test_path";
    data_file->file_format = FileFormatType::kParquet;
    data_file->partition.AddValue(Literal::String(partition));
    data_file->record_count = record_count;
    data_file->file_size_in_bytes = file_size_in_bytes;
    data_file->column_sizes = {};
    data_file->value_counts = value_counts;
    data_file->null_value_counts = null_counts;
    data_file->nan_value_counts = nan_counts;
    data_file->split_offsets = {1};
    data_file->sort_order_id = 0;
    parse_bound(upper_bounds, data_file->upper_bounds);
    parse_bound(lower_bounds, data_file->lower_bounds);
    return data_file;
  }

  void TestCase(const std::shared_ptr<Expression>& unbound, bool expected_result) {
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           InclusiveMetricsEvaluator::Make(unbound, *schema_, true));
    auto file = PrepareDataFile("20251128", 10, 1024, {{"id", static_cast<int64_t>(100)}},
                                {{"id", static_cast<int64_t>(200)}});
    auto result = evaluator->Eval(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), expected_result) << unbound->ToString();
  }

  void TestStringCase(const std::shared_ptr<Expression>& unbound, bool expected_result) {
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           InclusiveMetricsEvaluator::Make(unbound, *schema_, true));
    auto file = PrepareDataFile("20251128", 10, 1024, {{"name", "123"}},
                                {{"name", "456"}}, {{2, 10}}, {{2, 0}});
    auto result = evaluator->Eval(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), expected_result) << unbound->ToString();
  }

  std::shared_ptr<Schema> schema_;
};

TEST_F(InclusiveMetricsEvaluatorTest, CaseSensitiveTest) {
  {
    auto unbound = Expressions::Equal("id", Literal::Long(300));
    auto evaluator = InclusiveMetricsEvaluator::Make(unbound, *schema_, true);
    ASSERT_TRUE(evaluator.has_value());
  }
  {
    auto unbound = Expressions::Equal("ID", Literal::Long(300));
    auto evaluator = InclusiveMetricsEvaluator::Make(unbound, *schema_, true);
    ASSERT_FALSE(evaluator.has_value());
    ASSERT_EQ(evaluator.error().kind, ErrorKind::kInvalidExpression);
  }
  {
    auto unbound = Expressions::Equal("ID", Literal::Long(300));
    auto evaluator = InclusiveMetricsEvaluator::Make(unbound, *schema_, false);
    ASSERT_TRUE(evaluator.has_value());
  }
}

TEST_F(InclusiveMetricsEvaluatorTest, IsNullTest) {
  {
    auto unbound = Expressions::IsNull("name");
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           InclusiveMetricsEvaluator::Make(unbound, *schema_, true));
    auto file = PrepareDataFile("20251128", 10, 1024, {{"name", "1"}}, {{"name", "2"}},
                                {{2, 10}}, {{2, 5}}, {});
    auto result = evaluator->Eval(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), ROWS_MIGHT_MATCH) << unbound->ToString();
  }
  {
    auto unbound = Expressions::IsNull("name");
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           InclusiveMetricsEvaluator::Make(unbound, *schema_, true));
    auto file = PrepareDataFile("20251128", 10, 1024, {{"name", "1"}}, {{"name", "2"}},
                                {{2, 10}}, {{2, 0}}, {});
    auto result = evaluator->Eval(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), ROWS_CANNOT_MATCH) << unbound->ToString();
  }
}

TEST_F(InclusiveMetricsEvaluatorTest, NotNullTest) {
  {
    auto unbound = Expressions::NotNull("name");
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           InclusiveMetricsEvaluator::Make(unbound, *schema_, true));
    auto file = PrepareDataFile("20251128", 10, 1024, {{"name", "1"}}, {{"name", "2"}},
                                {{2, 10}}, {{2, 5}}, {});
    auto result = evaluator->Eval(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), ROWS_MIGHT_MATCH) << unbound->ToString();
  }
  {
    auto unbound = Expressions::NotNull("name");
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           InclusiveMetricsEvaluator::Make(unbound, *schema_, true));
    auto file = PrepareDataFile("20251128", 10, 1024, {{"name", "1"}}, {{"name", "2"}},
                                {{2, 10}}, {{2, 10}}, {});
    auto result = evaluator->Eval(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), ROWS_CANNOT_MATCH) << unbound->ToString();
  }
}

TEST_F(InclusiveMetricsEvaluatorTest, IsNanTest) {
  {
    auto unbound = Expressions::IsNaN("salary");
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           InclusiveMetricsEvaluator::Make(unbound, *schema_, true));
    auto file = PrepareDataFile("20251128", 10, 1024, {{"salary", 1.0}},
                                {{"salary", 2.0}}, {{4, 10}}, {{4, 5}}, {{4, 5}});
    auto result = evaluator->Eval(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), ROWS_MIGHT_MATCH) << unbound->ToString();
  }
  {
    auto unbound = Expressions::IsNaN("salary");
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           InclusiveMetricsEvaluator::Make(unbound, *schema_, true));
    auto file = PrepareDataFile("20251128", 10, 1024, {{"salary", 1.0}},
                                {{"salary", 2.0}}, {{4, 10}}, {{4, 10}}, {{4, 5}});
    auto result = evaluator->Eval(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), ROWS_CANNOT_MATCH) << unbound->ToString();
  }
  {
    auto unbound = Expressions::IsNaN("salary");
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           InclusiveMetricsEvaluator::Make(unbound, *schema_, true));
    auto file = PrepareDataFile("20251128", 10, 1024, {{"salary", 1.0}},
                                {{"salary", 2.0}}, {{4, 10}}, {{4, 5}}, {{4, 0}});
    auto result = evaluator->Eval(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), ROWS_CANNOT_MATCH) << unbound->ToString();
  }
}

TEST_F(InclusiveMetricsEvaluatorTest, NotNanTest) {
  {
    auto unbound = Expressions::NotNaN("salary");
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           InclusiveMetricsEvaluator::Make(unbound, *schema_, true));
    auto file = PrepareDataFile("20251128", 10, 1024, {{"salary", 1.0}},
                                {{"salary", 2.0}}, {{4, 10}}, {}, {{4, 5}});
    auto result = evaluator->Eval(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), ROWS_MIGHT_MATCH) << unbound->ToString();
  }
  {
    auto unbound = Expressions::NotNaN("salary");
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           InclusiveMetricsEvaluator::Make(unbound, *schema_, true));
    auto file = PrepareDataFile("20251128", 10, 1024, {{"salary", 1.0}},
                                {{"salary", 2.0}}, {{4, 10}}, {}, {{4, 10}});
    auto result = evaluator->Eval(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), ROWS_CANNOT_MATCH) << unbound->ToString();
  }
}

TEST_F(InclusiveMetricsEvaluatorTest, LTTest) {
  TestCase(Expressions::LessThan("id", Literal::Long(300)), ROWS_MIGHT_MATCH);
  TestCase(Expressions::LessThan("id", Literal::Long(150)), ROWS_MIGHT_MATCH);
  TestCase(Expressions::LessThan("id", Literal::Long(100)), ROWS_CANNOT_MATCH);
  TestCase(Expressions::LessThan("id", Literal::Long(200)), ROWS_MIGHT_MATCH);
  TestCase(Expressions::LessThan("id", Literal::Long(99)), ROWS_CANNOT_MATCH);
}

TEST_F(InclusiveMetricsEvaluatorTest, LTEQTest) {
  TestCase(Expressions::LessThanOrEqual("id", Literal::Long(300)), ROWS_MIGHT_MATCH);
  TestCase(Expressions::LessThanOrEqual("id", Literal::Long(150)), ROWS_MIGHT_MATCH);
  TestCase(Expressions::LessThanOrEqual("id", Literal::Long(100)), ROWS_MIGHT_MATCH);
  TestCase(Expressions::LessThanOrEqual("id", Literal::Long(200)), ROWS_MIGHT_MATCH);
  TestCase(Expressions::LessThanOrEqual("id", Literal::Long(99)), ROWS_CANNOT_MATCH);
}

TEST_F(InclusiveMetricsEvaluatorTest, GTTest) {
  TestCase(Expressions::GreaterThan("id", Literal::Long(300)), ROWS_CANNOT_MATCH);
  TestCase(Expressions::GreaterThan("id", Literal::Long(150)), ROWS_MIGHT_MATCH);
  TestCase(Expressions::GreaterThan("id", Literal::Long(100)), ROWS_MIGHT_MATCH);
  TestCase(Expressions::GreaterThan("id", Literal::Long(200)), ROWS_CANNOT_MATCH);
  TestCase(Expressions::GreaterThan("id", Literal::Long(99)), ROWS_MIGHT_MATCH);
}

TEST_F(InclusiveMetricsEvaluatorTest, GTEQTest) {
  TestCase(Expressions::GreaterThanOrEqual("id", Literal::Long(300)), ROWS_CANNOT_MATCH);
  TestCase(Expressions::GreaterThanOrEqual("id", Literal::Long(150)), ROWS_MIGHT_MATCH);
  TestCase(Expressions::GreaterThanOrEqual("id", Literal::Long(100)), ROWS_MIGHT_MATCH);
  TestCase(Expressions::GreaterThanOrEqual("id", Literal::Long(200)), ROWS_MIGHT_MATCH);
  TestCase(Expressions::GreaterThanOrEqual("id", Literal::Long(99)), ROWS_MIGHT_MATCH);
}

TEST_F(InclusiveMetricsEvaluatorTest, EQTest) {
  TestCase(Expressions::Equal("id", Literal::Long(300)), ROWS_CANNOT_MATCH);
  TestCase(Expressions::Equal("id", Literal::Long(150)), ROWS_MIGHT_MATCH);
  TestCase(Expressions::Equal("id", Literal::Long(100)), ROWS_MIGHT_MATCH);
  TestCase(Expressions::Equal("id", Literal::Long(200)), ROWS_MIGHT_MATCH);
}

TEST_F(InclusiveMetricsEvaluatorTest, NotEqTest) {
  TestCase(Expressions::NotEqual("id", Literal::Long(300)), ROWS_MIGHT_MATCH);
  TestCase(Expressions::NotEqual("id", Literal::Long(150)), ROWS_MIGHT_MATCH);
  TestCase(Expressions::NotEqual("id", Literal::Long(100)), ROWS_MIGHT_MATCH);
  TestCase(Expressions::NotEqual("id", Literal::Long(200)), ROWS_MIGHT_MATCH);
}

TEST_F(InclusiveMetricsEvaluatorTest, InTest) {
  TestCase(Expressions::In("id",
                           {
                               Literal::Long(300),
                               Literal::Long(400),
                               Literal::Long(500),
                           }),
           ROWS_CANNOT_MATCH);
  TestCase(Expressions::In("id",
                           {
                               Literal::Long(150),
                               Literal::Long(300),
                           }),
           ROWS_MIGHT_MATCH);
  TestCase(Expressions::In("id", {Literal::Long(100)}), ROWS_MIGHT_MATCH);
  TestCase(Expressions::In("id", {Literal::Long(200)}), ROWS_MIGHT_MATCH);
  TestCase(Expressions::In("id",
                           {
                               Literal::Long(99),
                               Literal::Long(201),
                           }),
           ROWS_CANNOT_MATCH);
}

TEST_F(InclusiveMetricsEvaluatorTest, NotInTest) {
  TestCase(Expressions::NotIn("id",
                              {
                                  Literal::Long(300),
                                  Literal::Long(400),
                                  Literal::Long(500),
                              }),
           ROWS_MIGHT_MATCH);
  TestCase(Expressions::NotIn("id",
                              {
                                  Literal::Long(150),
                                  Literal::Long(300),
                              }),
           ROWS_MIGHT_MATCH);
  TestCase(Expressions::NotIn("id",
                              {
                                  Literal::Long(100),
                                  Literal::Long(200),
                              }),
           ROWS_MIGHT_MATCH);
  TestCase(Expressions::NotIn("id",
                              {
                                  Literal::Long(99),
                                  Literal::Long(201),
                              }),
           ROWS_MIGHT_MATCH);
}

TEST_F(InclusiveMetricsEvaluatorTest, StartsWithTest) {
  TestStringCase(Expressions::StartsWith("name", "1"), ROWS_MIGHT_MATCH);
  TestStringCase(Expressions::StartsWith("name", "4"), ROWS_MIGHT_MATCH);
  TestStringCase(Expressions::StartsWith("name", "12"), ROWS_MIGHT_MATCH);
  TestStringCase(Expressions::StartsWith("name", "45"), ROWS_MIGHT_MATCH);
  TestStringCase(Expressions::StartsWith("name", "123"), ROWS_MIGHT_MATCH);
  TestStringCase(Expressions::StartsWith("name", "456"), ROWS_MIGHT_MATCH);
  TestStringCase(Expressions::StartsWith("name", "1234"), ROWS_MIGHT_MATCH);
  TestStringCase(Expressions::StartsWith("name", "4567"), ROWS_CANNOT_MATCH);
  TestStringCase(Expressions::StartsWith("name", "78"), ROWS_CANNOT_MATCH);
  TestStringCase(Expressions::StartsWith("name", "7"), ROWS_CANNOT_MATCH);
  TestStringCase(Expressions::StartsWith("name", "A"), ROWS_CANNOT_MATCH);
}

TEST_F(InclusiveMetricsEvaluatorTest, NotStartsWithTest) {
  TestStringCase(Expressions::NotStartsWith("name", "1"), ROWS_MIGHT_MATCH);
  TestStringCase(Expressions::NotStartsWith("name", "4"), ROWS_MIGHT_MATCH);
  TestStringCase(Expressions::NotStartsWith("name", "12"), ROWS_MIGHT_MATCH);
  TestStringCase(Expressions::NotStartsWith("name", "45"), ROWS_MIGHT_MATCH);
  TestStringCase(Expressions::NotStartsWith("name", "123"), ROWS_MIGHT_MATCH);
  TestStringCase(Expressions::NotStartsWith("name", "456"), ROWS_MIGHT_MATCH);
  TestStringCase(Expressions::NotStartsWith("name", "1234"), ROWS_MIGHT_MATCH);
  TestStringCase(Expressions::NotStartsWith("name", "4567"), ROWS_MIGHT_MATCH);
  TestStringCase(Expressions::NotStartsWith("name", "78"), ROWS_MIGHT_MATCH);
  TestStringCase(Expressions::NotStartsWith("name", "7"), ROWS_MIGHT_MATCH);
  TestStringCase(Expressions::NotStartsWith("name", "A"), ROWS_MIGHT_MATCH);

  auto test_case = [&](const std::string& prefix, bool expected_result) {
    auto unbound = Expressions::NotStartsWith("name", prefix);
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           InclusiveMetricsEvaluator::Make(unbound, *schema_, true));
    auto file = PrepareDataFile("20251128", 10, 1024, {{"name", "123"}},
                                {{"name", "123"}}, {{2, 10}}, {{2, 0}});
    auto result = evaluator->Eval(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), expected_result) << unbound->ToString();
  };
  test_case("12", ROWS_CANNOT_MATCH);
  test_case("123", ROWS_CANNOT_MATCH);
  test_case("1234", ROWS_MIGHT_MATCH);
}

}  // namespace iceberg
