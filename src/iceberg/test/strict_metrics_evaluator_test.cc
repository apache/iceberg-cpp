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

#include "iceberg/expression/strict_metrics_evaluator.h"

#include <limits>

#include <gtest/gtest.h>

#include "iceberg/expression/binder.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/schema.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"

namespace iceberg {

namespace {
constexpr bool kRowsMustMatch = true;
constexpr bool kRowsMightNotMatch = false;
}  // namespace
using TestVariant = std::variant<bool, int32_t, int64_t, double, std::string>;

class StrictMetricsEvaluatorTest : public ::testing::Test {
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
                           StrictMetricsEvaluator::Make(unbound, schema_, true));
    auto file = PrepareDataFile(/*partition=*/"20251128", /*record_count=*/10,
                                /*file_size_in_bytes=*/1024,
                                /*lower_bounds=*/{{"id", static_cast<int64_t>(100)}},
                                /*upper_bounds=*/{{"id", static_cast<int64_t>(200)}},
                                /*value_counts=*/{{1, 10}}, /*null_counts=*/{{1, 0}});
    auto result = evaluator->Evaluate(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), expected_result) << unbound->ToString();
  }

  void TestStringCase(const std::shared_ptr<Expression>& unbound, bool expected_result) {
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           StrictMetricsEvaluator::Make(unbound, schema_, true));
    auto file = PrepareDataFile(/*partition=*/"20251128", /*record_count=*/10,
                                /*file_size_in_bytes=*/1024,
                                /*lower_bounds=*/{{"name", "123"}}, {{"name", "456"}},
                                /*value_counts=*/{{2, 10}}, /*null_counts=*/{{2, 0}});
    auto result = evaluator->Evaluate(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), expected_result) << unbound->ToString();
  }

  std::shared_ptr<Schema> schema_;
};

TEST_F(StrictMetricsEvaluatorTest, CaseSensitiveTest) {
  {
    auto unbound = Expressions::Equal("id", Literal::Long(300));
    auto evaluator = StrictMetricsEvaluator::Make(unbound, schema_, true);
    ASSERT_TRUE(evaluator.has_value());
  }
  {
    auto unbound = Expressions::Equal("ID", Literal::Long(300));
    auto evaluator = StrictMetricsEvaluator::Make(unbound, schema_, true);
    ASSERT_FALSE(evaluator.has_value());
    ASSERT_EQ(evaluator.error().kind, ErrorKind::kInvalidExpression);
  }
  {
    auto unbound = Expressions::Equal("ID", Literal::Long(300));
    auto evaluator = StrictMetricsEvaluator::Make(unbound, schema_, false);
    ASSERT_TRUE(evaluator.has_value());
  }
}

TEST_F(StrictMetricsEvaluatorTest, IsNullTest) {
  {
    auto unbound = Expressions::IsNull("name");
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           StrictMetricsEvaluator::Make(unbound, schema_, true));
    auto file = PrepareDataFile("20251128", 10, 1024, {{"name", "1"}}, {{"name", "2"}},
                                {{2, 10}}, {{2, 5}}, {});
    auto result = evaluator->Evaluate(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), kRowsMightNotMatch) << unbound->ToString();
  }
  {
    auto unbound = Expressions::IsNull("name");
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           StrictMetricsEvaluator::Make(unbound, schema_, true));
    auto file = PrepareDataFile("20251128", 10, 1024, {{"name", "1"}}, {{"name", "2"}},
                                {{2, 10}}, {{2, 10}}, {});
    auto result = evaluator->Evaluate(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), kRowsMustMatch) << unbound->ToString();
  }
}

TEST_F(StrictMetricsEvaluatorTest, NotNullTest) {
  {
    auto unbound = Expressions::NotNull("name");
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           StrictMetricsEvaluator::Make(unbound, schema_, true));
    auto file = PrepareDataFile("20251128", 10, 1024, {{"name", "1"}}, {{"name", "2"}},
                                {{2, 10}}, {{2, 5}}, {});
    auto result = evaluator->Evaluate(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), kRowsMightNotMatch) << unbound->ToString();
  }
  {
    auto unbound = Expressions::NotNull("name");
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           StrictMetricsEvaluator::Make(unbound, schema_, true));
    auto file = PrepareDataFile("20251128", 10, 1024, {{"name", "1"}}, {{"name", "2"}},
                                {{2, 10}}, {{2, 0}}, {});
    auto result = evaluator->Evaluate(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), kRowsMustMatch) << unbound->ToString();
  }
}

TEST_F(StrictMetricsEvaluatorTest, IsNanTest) {
  {
    auto unbound = Expressions::IsNaN("salary");
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           StrictMetricsEvaluator::Make(unbound, schema_, true));
    auto file = PrepareDataFile("20251128", 10, 1024, {{"salary", 1.0}},
                                {{"salary", 2.0}}, {{4, 10}}, {{4, 5}}, {{4, 5}});
    auto result = evaluator->Evaluate(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), kRowsMightNotMatch) << unbound->ToString();
  }
  {
    auto unbound = Expressions::IsNaN("salary");
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           StrictMetricsEvaluator::Make(unbound, schema_, true));
    auto file = PrepareDataFile("20251128", 10, 1024, {{"salary", 1.0}},
                                {{"salary", 2.0}}, {{4, 10}}, {{4, 10}}, {{4, 5}});
    auto result = evaluator->Evaluate(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), kRowsMightNotMatch) << unbound->ToString();
  }
  {
    auto unbound = Expressions::IsNaN("salary");
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           StrictMetricsEvaluator::Make(unbound, schema_, true));
    auto file = PrepareDataFile("20251128", 10, 1024, {{"salary", 1.0}},
                                {{"salary", 2.0}}, {{4, 10}}, {{4, 5}}, {{4, 10}});
    auto result = evaluator->Evaluate(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), kRowsMustMatch) << unbound->ToString();
  }
}

TEST_F(StrictMetricsEvaluatorTest, NotNanTest) {
  {
    auto unbound = Expressions::NotNaN("salary");
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           StrictMetricsEvaluator::Make(unbound, schema_, true));
    auto file = PrepareDataFile("20251128", 10, 1024, {{"salary", 1.0}},
                                {{"salary", 2.0}}, {{4, 10}}, {}, {{4, 5}});
    auto result = evaluator->Evaluate(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), kRowsMightNotMatch) << unbound->ToString();
  }
  {
    auto unbound = Expressions::NotNaN("salary");
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           StrictMetricsEvaluator::Make(unbound, schema_, true));
    auto file = PrepareDataFile("20251128", 10, 1024, {{"salary", 1.0}},
                                {{"salary", 2.0}}, {{4, 10}}, {}, {{4, 0}});
    auto result = evaluator->Evaluate(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), kRowsMustMatch) << unbound->ToString();
  }
  {
    auto unbound = Expressions::NotNaN("salary");
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           StrictMetricsEvaluator::Make(unbound, schema_, true));
    auto file = PrepareDataFile("20251128", 10, 1024, {{"salary", 1.0}},
                                {{"salary", 2.0}}, {{4, 10}}, {{4, 10}}, {});
    auto result = evaluator->Evaluate(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), kRowsMustMatch) << unbound->ToString();
  }
}

TEST_F(StrictMetricsEvaluatorTest, LTTest) {
  TestCase(Expressions::LessThan("id", Literal::Long(300)), kRowsMustMatch);
  TestCase(Expressions::LessThan("id", Literal::Long(150)), kRowsMightNotMatch);
  TestCase(Expressions::LessThan("id", Literal::Long(100)), kRowsMightNotMatch);
  TestCase(Expressions::LessThan("id", Literal::Long(200)), kRowsMightNotMatch);
  TestCase(Expressions::LessThan("id", Literal::Long(99)), kRowsMightNotMatch);
}

TEST_F(StrictMetricsEvaluatorTest, LTEQTest) {
  TestCase(Expressions::LessThanOrEqual("id", Literal::Long(300)), kRowsMustMatch);
  TestCase(Expressions::LessThanOrEqual("id", Literal::Long(150)), kRowsMightNotMatch);
  TestCase(Expressions::LessThanOrEqual("id", Literal::Long(100)), kRowsMightNotMatch);
  TestCase(Expressions::LessThanOrEqual("id", Literal::Long(200)), kRowsMustMatch);
  TestCase(Expressions::LessThanOrEqual("id", Literal::Long(99)), kRowsMightNotMatch);
}

TEST_F(StrictMetricsEvaluatorTest, GTTest) {
  TestCase(Expressions::GreaterThan("id", Literal::Long(300)), kRowsMightNotMatch);
  TestCase(Expressions::GreaterThan("id", Literal::Long(150)), kRowsMightNotMatch);
  TestCase(Expressions::GreaterThan("id", Literal::Long(100)), kRowsMightNotMatch);
  TestCase(Expressions::GreaterThan("id", Literal::Long(200)), kRowsMightNotMatch);
  TestCase(Expressions::GreaterThan("id", Literal::Long(99)), kRowsMustMatch);
}

TEST_F(StrictMetricsEvaluatorTest, GTEQTest) {
  TestCase(Expressions::GreaterThanOrEqual("id", Literal::Long(300)), kRowsMightNotMatch);
  TestCase(Expressions::GreaterThanOrEqual("id", Literal::Long(150)), kRowsMightNotMatch);
  TestCase(Expressions::GreaterThanOrEqual("id", Literal::Long(100)), kRowsMustMatch);
  TestCase(Expressions::GreaterThanOrEqual("id", Literal::Long(200)), kRowsMightNotMatch);
  TestCase(Expressions::GreaterThanOrEqual("id", Literal::Long(99)), kRowsMustMatch);
}

TEST_F(StrictMetricsEvaluatorTest, EQTest) {
  TestCase(Expressions::Equal("id", Literal::Long(300)), kRowsMightNotMatch);
  TestCase(Expressions::Equal("id", Literal::Long(150)), kRowsMightNotMatch);
  TestCase(Expressions::Equal("id", Literal::Long(100)), kRowsMightNotMatch);
  TestCase(Expressions::Equal("id", Literal::Long(200)), kRowsMightNotMatch);

  auto test_case = [&](const std::shared_ptr<Expression>& unbound, bool expected_result) {
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           StrictMetricsEvaluator::Make(unbound, schema_, true));
    auto file = PrepareDataFile(/*partition=*/"20251128", /*record_count=*/10,
                                /*file_size_in_bytes=*/1024,
                                /*lower_bounds=*/{{"id", static_cast<int64_t>(100)}},
                                /*upper_bounds=*/{{"id", static_cast<int64_t>(100)}},
                                /*value_counts=*/{{1, 10}}, /*null_counts=*/{{1, 0}});
    auto result = evaluator->Evaluate(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), expected_result) << unbound->ToString();
  };
  test_case(Expressions::Equal("id", Literal::Long(100)), kRowsMustMatch);
  test_case(Expressions::Equal("id", Literal::Long(200)), kRowsMightNotMatch);
}

TEST_F(StrictMetricsEvaluatorTest, NotEqTest) {
  TestCase(Expressions::NotEqual("id", Literal::Long(300)), kRowsMustMatch);
  TestCase(Expressions::NotEqual("id", Literal::Long(150)), kRowsMightNotMatch);
  TestCase(Expressions::NotEqual("id", Literal::Long(100)), kRowsMightNotMatch);
  TestCase(Expressions::NotEqual("id", Literal::Long(200)), kRowsMightNotMatch);
  TestCase(Expressions::NotEqual("id", Literal::Long(99)), kRowsMustMatch);
}

TEST_F(StrictMetricsEvaluatorTest, InTest) {
  TestCase(Expressions::In("id",
                           {
                               Literal::Long(100),
                               Literal::Long(200),
                               Literal::Long(300),
                               Literal::Long(400),
                               Literal::Long(500),
                           }),
           kRowsMightNotMatch);

  auto test_case = [&](const std::shared_ptr<Expression>& unbound, bool expected_result) {
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           StrictMetricsEvaluator::Make(unbound, schema_, true));
    auto file = PrepareDataFile(/*partition=*/"20251128", /*record_count=*/10,
                                /*file_size_in_bytes=*/1024,
                                /*lower_bounds=*/{{"id", static_cast<int64_t>(100)}},
                                /*upper_bounds=*/{{"id", static_cast<int64_t>(100)}},
                                /*value_counts=*/{{1, 10}}, /*null_counts=*/{{1, 0}});
    auto result = evaluator->Evaluate(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), expected_result) << unbound->ToString();
  };
  test_case(Expressions::In("id", {Literal::Long(100), Literal::Long(200)}),
            kRowsMustMatch);
  test_case(Expressions::In("id", {Literal::Long(200), Literal::Long(300)}),
            kRowsMightNotMatch);
}

TEST_F(StrictMetricsEvaluatorTest, NotInTest) {
  TestCase(Expressions::NotIn("id",
                              {
                                  Literal::Long(88),
                                  Literal::Long(99),
                              }),
           kRowsMustMatch);
  TestCase(Expressions::NotIn("id",
                              {
                                  Literal::Long(288),
                                  Literal::Long(299),
                              }),
           kRowsMustMatch);
  TestCase(Expressions::NotIn("id",
                              {
                                  Literal::Long(88),
                                  Literal::Long(288),
                                  Literal::Long(299),
                              }),
           kRowsMustMatch);
  TestCase(Expressions::NotIn("id",
                              {
                                  Literal::Long(88),
                                  Literal::Long(100),
                              }),
           kRowsMightNotMatch);
  TestCase(Expressions::NotIn("id",
                              {
                                  Literal::Long(88),
                                  Literal::Long(101),
                              }),
           kRowsMightNotMatch);
  TestCase(Expressions::NotIn("id",
                              {
                                  Literal::Long(100),
                                  Literal::Long(101),
                              }),
           kRowsMightNotMatch);
}

TEST_F(StrictMetricsEvaluatorTest, StartsWithTest) {
  // always true
  TestStringCase(Expressions::StartsWith("name", "1"), kRowsMightNotMatch);
}

TEST_F(StrictMetricsEvaluatorTest, NotStartsWithTest) {
  TestStringCase(Expressions::NotStartsWith("name", "1"), kRowsMightNotMatch);
}

class StrictMetricsEvaluatorMigratedTest : public StrictMetricsEvaluatorTest {
 protected:
  static constexpr int64_t kIntMinValue = 30;
  static constexpr int64_t kIntMaxValue = 79;
  static constexpr int64_t kAlwaysFive = 5;

  void SetUp() override {
    schema_ = std::make_shared<Schema>(
        std::vector<SchemaField>{
            SchemaField::MakeRequired(1, "id", int64()),
            SchemaField::MakeOptional(2, "no_stats", int64()),
            SchemaField::MakeRequired(3, "required", string()),
            SchemaField::MakeOptional(4, "all_nulls", string()),
            SchemaField::MakeOptional(5, "some_nulls", string()),
            SchemaField::MakeOptional(6, "no_nulls", string()),
            SchemaField::MakeRequired(7, "always_5", int64()),
            SchemaField::MakeOptional(8, "all_nans", float64()),
            SchemaField::MakeOptional(9, "some_nans", float32()),
            SchemaField::MakeOptional(10, "no_nans", float32()),
            SchemaField::MakeOptional(11, "all_nulls_double", float64()),
            SchemaField::MakeOptional(12, "all_nans_v1_stats", float32()),
            SchemaField::MakeOptional(13, "nan_and_null_only", float64()),
            SchemaField::MakeOptional(14, "no_nan_stats", float64()),
            SchemaField::MakeOptional(
                15, "struct",
                std::make_shared<StructType>(std::vector<SchemaField>{
                    SchemaField::MakeOptional(16, "nested_col_no_stats", int64()),
                    SchemaField::MakeOptional(17, "nested_col_with_stats", int64())})),
        },
        /*schema_id=*/0);

    file_ = MakePrimaryFile();
    file_with_bounds_ = MakeSomeNullsFile();
    file_with_equal_bounds_ = MakeSomeNullsEqualBoundsFile();
  }

  std::shared_ptr<DataFile> MakePrimaryFile() {
    auto data_file = std::make_shared<DataFile>();
    data_file->file_path = "file.avro";
    data_file->file_format = FileFormatType::kParquet;
    data_file->record_count = 50;
    data_file->value_counts = {
        {4, 50L},  {5, 50L},  {6, 50L},  {8, 50L},  {9, 50L},  {10, 50L},
        {11, 50L}, {12, 50L}, {13, 50L}, {14, 50L}, {17, 50L},
    };
    data_file->null_value_counts = {
        {4, 50L}, {5, 10L}, {6, 0L}, {11, 50L}, {12, 0L}, {13, 1L}, {17, 0L},
    };
    data_file->nan_value_counts = {
        {8, 50L},
        {9, 10L},
        {10, 0L},
    };
    const float float_nan = std::numeric_limits<float>::quiet_NaN();
    const double double_nan = std::numeric_limits<double>::quiet_NaN();
    data_file->lower_bounds = {
        {1, Literal::Long(kIntMinValue).Serialize().value()},
        {7, Literal::Long(kAlwaysFive).Serialize().value()},
        {12, Literal::Float(float_nan).Serialize().value()},
        {13, Literal::Double(double_nan).Serialize().value()},
        {17, Literal::Long(kIntMinValue).Serialize().value()},
    };
    data_file->upper_bounds = {
        {1, Literal::Long(kIntMaxValue).Serialize().value()},
        {7, Literal::Long(kAlwaysFive).Serialize().value()},
        {12, Literal::Float(float_nan).Serialize().value()},
        {13, Literal::Double(double_nan).Serialize().value()},
        {17, Literal::Long(kIntMaxValue).Serialize().value()},
    };
    return data_file;
  }

  std::shared_ptr<DataFile> MakeSomeNullsFile() {
    auto data_file = std::make_shared<DataFile>();
    data_file->file_path = "file_2.avro";
    data_file->file_format = FileFormatType::kParquet;
    data_file->record_count = 50;
    data_file->value_counts = {
        {4, 50L},
        {5, 50L},
        {6, 50L},
        {8, 50L},
    };
    data_file->null_value_counts = {
        {4, 50L},
        {5, 10L},
        {6, 0L},
    };
    data_file->lower_bounds = {
        {5, Literal::String("bbb").Serialize().value()},
    };
    data_file->upper_bounds = {
        {5, Literal::String("eee").Serialize().value()},
    };
    return data_file;
  }

  std::shared_ptr<DataFile> MakeSomeNullsEqualBoundsFile() {
    auto data_file = std::make_shared<DataFile>();
    data_file->file_path = "file_3.avro";
    data_file->file_format = FileFormatType::kParquet;
    data_file->record_count = 50;
    data_file->value_counts = {
        {4, 50L},
        {5, 50L},
        {6, 50L},
    };
    data_file->null_value_counts = {
        {4, 50L},
        {5, 10L},
        {6, 0L},
    };
    data_file->lower_bounds = {
        {5, Literal::String("bbb").Serialize().value()},
    };
    data_file->upper_bounds = {
        {5, Literal::String("bbb").Serialize().value()},
    };
    return data_file;
  }

  std::shared_ptr<DataFile> MakeMissingStatsFile() {
    auto data_file = std::make_shared<DataFile>();
    data_file->file_path = "missing.parquet";
    data_file->file_format = FileFormatType::kParquet;
    data_file->record_count = 50;
    return data_file;
  }

  std::shared_ptr<DataFile> MakeZeroRecordFile() {
    auto data_file = std::make_shared<DataFile>();
    data_file->file_path = "zero.parquet";
    data_file->file_format = FileFormatType::kParquet;
    data_file->record_count = 0;
    return data_file;
  }

  void ExpectShouldRead(const std::shared_ptr<Expression>& expr, bool expected,
                        std::shared_ptr<DataFile> file = nullptr,
                        bool case_sensitive = true) {
    auto target = file ? file : file_;
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           StrictMetricsEvaluator::Make(expr, schema_, case_sensitive));
    auto eval_result = evaluator->Evaluate(*target);
    ASSERT_TRUE(eval_result.has_value());
    ASSERT_EQ(eval_result.value(), expected) << expr->ToString();
  }

  std::shared_ptr<Schema> schema_;
  std::shared_ptr<DataFile> file_;
  std::shared_ptr<DataFile> file_with_bounds_;
  std::shared_ptr<DataFile> file_with_equal_bounds_;
};

TEST_F(StrictMetricsEvaluatorMigratedTest, AllNulls) {
  ExpectShouldRead(Expressions::NotNull("all_nulls"), false);
  ExpectShouldRead(Expressions::NotNull("some_nulls"), false);
  ExpectShouldRead(Expressions::NotNull("no_nulls"), true);
  ExpectShouldRead(Expressions::NotEqual("all_nulls", Literal::String("a")), true);
}

TEST_F(StrictMetricsEvaluatorMigratedTest, NoNulls) {
  ExpectShouldRead(Expressions::IsNull("all_nulls"), true);
  ExpectShouldRead(Expressions::IsNull("some_nulls"), false);
  ExpectShouldRead(Expressions::IsNull("no_nulls"), false);
}

TEST_F(StrictMetricsEvaluatorMigratedTest, SomeNulls) {
  ExpectShouldRead(Expressions::LessThan("some_nulls", Literal::String("ggg")), false,
                   file_with_bounds_);
  ExpectShouldRead(Expressions::LessThanOrEqual("some_nulls", Literal::String("eee")),
                   false, file_with_bounds_);
  ExpectShouldRead(Expressions::GreaterThan("some_nulls", Literal::String("aaa")), false,
                   file_with_bounds_);
  ExpectShouldRead(Expressions::GreaterThanOrEqual("some_nulls", Literal::String("bbb")),
                   false, file_with_bounds_);
  ExpectShouldRead(Expressions::Equal("some_nulls", Literal::String("bbb")), false,
                   file_with_equal_bounds_);
}

TEST_F(StrictMetricsEvaluatorMigratedTest, IsNaN) {
  ExpectShouldRead(Expressions::IsNaN("all_nans"), true);
  ExpectShouldRead(Expressions::IsNaN("some_nans"), false);
  ExpectShouldRead(Expressions::IsNaN("no_nans"), false);
  ExpectShouldRead(Expressions::IsNaN("all_nulls_double"), false);
  ExpectShouldRead(Expressions::IsNaN("no_nan_stats"), false);
  ExpectShouldRead(Expressions::IsNaN("all_nans_v1_stats"), false);
  ExpectShouldRead(Expressions::IsNaN("nan_and_null_only"), false);
}

TEST_F(StrictMetricsEvaluatorMigratedTest, NotNaN) {
  ExpectShouldRead(Expressions::NotNaN("all_nans"), false);
  ExpectShouldRead(Expressions::NotNaN("some_nans"), false);
  ExpectShouldRead(Expressions::NotNaN("no_nans"), true);
  ExpectShouldRead(Expressions::NotNaN("all_nulls_double"), true);
  ExpectShouldRead(Expressions::NotNaN("no_nan_stats"), false);
  ExpectShouldRead(Expressions::NotNaN("all_nans_v1_stats"), false);
  ExpectShouldRead(Expressions::NotNaN("nan_and_null_only"), false);
}

TEST_F(StrictMetricsEvaluatorMigratedTest, RequiredColumn) {
  ExpectShouldRead(Expressions::NotNull("required"), true);
  ExpectShouldRead(Expressions::IsNull("required"), false);
}

TEST_F(StrictMetricsEvaluatorMigratedTest, MissingColumn) {
  auto expr = Expressions::LessThan("missing", Literal::Long(5));
  auto evaluator = StrictMetricsEvaluator::Make(expr, schema_, true);
  ASSERT_FALSE(evaluator.has_value());
  EXPECT_TRUE(evaluator.error().message.contains("Cannot find field 'missing'"))
      << evaluator.error().message;
}

TEST_F(StrictMetricsEvaluatorMigratedTest, MissingStats) {
  auto missing_stats = MakeMissingStatsFile();
  std::vector<std::shared_ptr<Expression>> expressions = {
      Expressions::LessThan("no_stats", Literal::Long(5)),
      Expressions::LessThanOrEqual("no_stats", Literal::Long(30)),
      Expressions::Equal("no_stats", Literal::Long(70)),
      Expressions::GreaterThan("no_stats", Literal::Long(78)),
      Expressions::GreaterThanOrEqual("no_stats", Literal::Long(90)),
      Expressions::NotEqual("no_stats", Literal::Long(101)),
      Expressions::IsNull("no_stats"),
      Expressions::NotNull("no_stats"),
      Expressions::IsNaN("all_nans"),
      Expressions::NotNaN("all_nans"),
  };
  for (const auto& expr : expressions) {
    ExpectShouldRead(expr, false, missing_stats);
  }
}

TEST_F(StrictMetricsEvaluatorMigratedTest, ZeroRecordFile) {
  auto zero_record_file = MakeZeroRecordFile();
  std::vector<std::shared_ptr<Expression>> expressions = {
      Expressions::LessThan("id", Literal::Long(5)),
      Expressions::LessThanOrEqual("id", Literal::Long(30)),
      Expressions::Equal("id", Literal::Long(70)),
      Expressions::GreaterThan("id", Literal::Long(78)),
      Expressions::GreaterThanOrEqual("id", Literal::Long(90)),
      Expressions::NotEqual("id", Literal::Long(101)),
      Expressions::IsNull("some_nulls"),
      Expressions::NotNull("some_nulls"),
      Expressions::IsNaN("all_nans"),
      Expressions::NotNaN("all_nans"),
  };
  for (const auto& expr : expressions) {
    ExpectShouldRead(expr, true, zero_record_file);
  }
}

TEST_F(StrictMetricsEvaluatorMigratedTest, Not) {
  ExpectShouldRead(
      Expressions::Not(Expressions::LessThan("id", Literal::Long(kIntMinValue - 25))),
      true);
  ExpectShouldRead(
      Expressions::Not(Expressions::GreaterThan("id", Literal::Long(kIntMinValue - 25))),
      false);
}

TEST_F(StrictMetricsEvaluatorMigratedTest, And) {
  ExpectShouldRead(
      Expressions::And(Expressions::GreaterThan("id", Literal::Long(kIntMinValue - 25)),
                       Expressions::LessThanOrEqual("id", Literal::Long(kIntMinValue))),
      false);
  ExpectShouldRead(
      Expressions::And(
          Expressions::LessThan("id", Literal::Long(kIntMinValue - 25)),
          Expressions::GreaterThanOrEqual("id", Literal::Long(kIntMinValue - 30))),
      false);
  ExpectShouldRead(
      Expressions::And(
          Expressions::LessThan("id", Literal::Long(kIntMaxValue + 6)),
          Expressions::GreaterThanOrEqual("id", Literal::Long(kIntMinValue - 30))),
      true);
}

TEST_F(StrictMetricsEvaluatorMigratedTest, Or) {
  ExpectShouldRead(
      Expressions::Or(
          Expressions::LessThan("id", Literal::Long(kIntMinValue - 25)),
          Expressions::GreaterThanOrEqual("id", Literal::Long(kIntMaxValue + 1))),
      false);
  ExpectShouldRead(
      Expressions::Or(
          Expressions::LessThan("id", Literal::Long(kIntMinValue - 25)),
          Expressions::GreaterThanOrEqual("id", Literal::Long(kIntMaxValue - 19))),
      false);
  ExpectShouldRead(
      Expressions::Or(Expressions::LessThan("id", Literal::Long(kIntMinValue - 25)),
                      Expressions::GreaterThanOrEqual("id", Literal::Long(kIntMinValue))),
      true);
}

TEST_F(StrictMetricsEvaluatorMigratedTest, IntegerLt) {
  ExpectShouldRead(Expressions::LessThan("id", Literal::Long(kIntMinValue)), false);
  ExpectShouldRead(Expressions::LessThan("id", Literal::Long(kIntMinValue + 1)), false);
  ExpectShouldRead(Expressions::LessThan("id", Literal::Long(kIntMaxValue)), false);
  ExpectShouldRead(Expressions::LessThan("id", Literal::Long(kIntMaxValue + 1)), true);
}

TEST_F(StrictMetricsEvaluatorMigratedTest, IntegerLtEq) {
  ExpectShouldRead(Expressions::LessThanOrEqual("id", Literal::Long(kIntMinValue - 1)),
                   false);
  ExpectShouldRead(Expressions::LessThanOrEqual("id", Literal::Long(kIntMinValue)),
                   false);
  ExpectShouldRead(Expressions::LessThanOrEqual("id", Literal::Long(kIntMaxValue)), true);
  ExpectShouldRead(Expressions::LessThanOrEqual("id", Literal::Long(kIntMaxValue + 1)),
                   true);
}

TEST_F(StrictMetricsEvaluatorMigratedTest, IntegerGt) {
  ExpectShouldRead(Expressions::GreaterThan("id", Literal::Long(kIntMaxValue)), false);
  ExpectShouldRead(Expressions::GreaterThan("id", Literal::Long(kIntMaxValue - 1)),
                   false);
  ExpectShouldRead(Expressions::GreaterThan("id", Literal::Long(kIntMinValue)), false);
  ExpectShouldRead(Expressions::GreaterThan("id", Literal::Long(kIntMinValue - 1)), true);
}

TEST_F(StrictMetricsEvaluatorMigratedTest, IntegerGtEq) {
  ExpectShouldRead(Expressions::GreaterThanOrEqual("id", Literal::Long(kIntMaxValue + 1)),
                   false);
  ExpectShouldRead(Expressions::GreaterThanOrEqual("id", Literal::Long(kIntMaxValue)),
                   false);
  ExpectShouldRead(Expressions::GreaterThanOrEqual("id", Literal::Long(kIntMinValue + 1)),
                   false);
  ExpectShouldRead(Expressions::GreaterThanOrEqual("id", Literal::Long(kIntMinValue)),
                   true);
}

TEST_F(StrictMetricsEvaluatorMigratedTest, IntegerEq) {
  ExpectShouldRead(Expressions::Equal("id", Literal::Long(kIntMinValue - 25)), false);
  ExpectShouldRead(Expressions::Equal("id", Literal::Long(kIntMinValue)), false);
  ExpectShouldRead(Expressions::Equal("id", Literal::Long(kIntMaxValue - 4)), false);
  ExpectShouldRead(Expressions::Equal("id", Literal::Long(kIntMaxValue)), false);
  ExpectShouldRead(Expressions::Equal("id", Literal::Long(kIntMaxValue + 1)), false);
  ExpectShouldRead(Expressions::Equal("always_5", Literal::Long(kIntMinValue - 25)),
                   true);
}

TEST_F(StrictMetricsEvaluatorMigratedTest, IntegerNotEq) {
  ExpectShouldRead(Expressions::NotEqual("id", Literal::Long(kIntMinValue - 25)), true);
  ExpectShouldRead(Expressions::NotEqual("id", Literal::Long(kIntMinValue - 1)), true);
  ExpectShouldRead(Expressions::NotEqual("id", Literal::Long(kIntMinValue)), false);
  ExpectShouldRead(Expressions::NotEqual("id", Literal::Long(kIntMaxValue - 4)), false);
  ExpectShouldRead(Expressions::NotEqual("id", Literal::Long(kIntMaxValue)), false);
  ExpectShouldRead(Expressions::NotEqual("id", Literal::Long(kIntMaxValue + 1)), true);
  ExpectShouldRead(Expressions::NotEqual("id", Literal::Long(kIntMaxValue + 6)), true);
}

TEST_F(StrictMetricsEvaluatorMigratedTest, IntegerNotEqRewritten) {
  ExpectShouldRead(
      Expressions::Not(Expressions::Equal("id", Literal::Long(kIntMinValue - 25))), true);
  ExpectShouldRead(
      Expressions::Not(Expressions::Equal("id", Literal::Long(kIntMinValue - 1))), true);
  ExpectShouldRead(
      Expressions::Not(Expressions::Equal("id", Literal::Long(kIntMinValue))), false);
  ExpectShouldRead(
      Expressions::Not(Expressions::Equal("id", Literal::Long(kIntMaxValue - 4))), false);
  ExpectShouldRead(
      Expressions::Not(Expressions::Equal("id", Literal::Long(kIntMaxValue))), false);
  ExpectShouldRead(
      Expressions::Not(Expressions::Equal("id", Literal::Long(kIntMaxValue + 1))), true);
  ExpectShouldRead(
      Expressions::Not(Expressions::Equal("id", Literal::Long(kIntMaxValue + 6))), true);
}

TEST_F(StrictMetricsEvaluatorMigratedTest, IntegerIn) {
  ExpectShouldRead(Expressions::In("id", {Literal::Long(kIntMinValue - 25),
                                          Literal::Long(kIntMinValue - 24)}),
                   false);
  ExpectShouldRead(Expressions::In("id", {Literal::Long(kIntMinValue - 1),
                                          Literal::Long(kIntMinValue)}),
                   false);
  ExpectShouldRead(Expressions::In("id", {Literal::Long(kIntMaxValue - 4),
                                          Literal::Long(kIntMaxValue - 3)}),
                   false);
  ExpectShouldRead(Expressions::In("id", {Literal::Long(kIntMaxValue),
                                          Literal::Long(kIntMaxValue + 1)}),
                   false);
  ExpectShouldRead(Expressions::In("id", {Literal::Long(kIntMaxValue + 1),
                                          Literal::Long(kIntMaxValue + 2)}),
                   false);
  ExpectShouldRead(Expressions::In("always_5", {Literal::Long(5), Literal::Long(6)}),
                   true);
  ExpectShouldRead(
      Expressions::In("all_nulls", {Literal::String("abc"), Literal::String("def")}),
      false);
  ExpectShouldRead(
      Expressions::In("some_nulls", {Literal::String("abc"), Literal::String("def")}),
      false, file_with_equal_bounds_);
  ExpectShouldRead(
      Expressions::In("no_nulls", {Literal::String("abc"), Literal::String("def")}),
      false);
}

TEST_F(StrictMetricsEvaluatorMigratedTest, IntegerNotIn) {
  ExpectShouldRead(Expressions::NotIn("id", {Literal::Long(kIntMinValue - 25),
                                             Literal::Long(kIntMinValue - 24)}),
                   true);
  ExpectShouldRead(Expressions::NotIn("id", {Literal::Long(kIntMinValue - 1),
                                             Literal::Long(kIntMinValue)}),
                   false);
  ExpectShouldRead(Expressions::NotIn("id", {Literal::Long(kIntMaxValue - 4),
                                             Literal::Long(kIntMaxValue - 3)}),
                   false);
  ExpectShouldRead(Expressions::NotIn("id", {Literal::Long(kIntMaxValue),
                                             Literal::Long(kIntMaxValue + 1)}),
                   false);
  ExpectShouldRead(Expressions::NotIn("id", {Literal::Long(kIntMaxValue + 1),
                                             Literal::Long(kIntMaxValue + 2)}),
                   true);
  ExpectShouldRead(Expressions::NotIn("always_5", {Literal::Long(5), Literal::Long(6)}),
                   false);
  ExpectShouldRead(
      Expressions::NotIn("all_nulls", {Literal::String("abc"), Literal::String("def")}),
      true);
  ExpectShouldRead(
      Expressions::NotIn("some_nulls", {Literal::String("abc"), Literal::String("def")}),
      true, file_with_equal_bounds_);
  ExpectShouldRead(
      Expressions::NotIn("no_nulls", {Literal::String("abc"), Literal::String("def")}),
      false);
}

TEST_F(StrictMetricsEvaluatorMigratedTest, EvaluateOnNestedColumnWithoutStats) {
  ExpectShouldRead(Expressions::GreaterThanOrEqual("struct.nested_col_no_stats",
                                                   Literal::Long(kIntMinValue)),
                   false);
  ExpectShouldRead(Expressions::LessThanOrEqual("struct.nested_col_no_stats",
                                                Literal::Long(kIntMaxValue)),
                   false);
  ExpectShouldRead(Expressions::IsNull("struct.nested_col_no_stats"), false);
  ExpectShouldRead(Expressions::NotNull("struct.nested_col_no_stats"), false);
}

TEST_F(StrictMetricsEvaluatorMigratedTest, EvaluateOnNestedColumnWithStats) {
  ExpectShouldRead(Expressions::GreaterThanOrEqual("struct.nested_col_with_stats",
                                                   Literal::Long(kIntMinValue)),
                   false);
  ExpectShouldRead(Expressions::LessThanOrEqual("struct.nested_col_with_stats",
                                                Literal::Long(kIntMaxValue)),
                   false);
  ExpectShouldRead(Expressions::IsNull("struct.nested_col_with_stats"), false);
  ExpectShouldRead(Expressions::NotNull("struct.nested_col_with_stats"), false);
}

}  // namespace iceberg
