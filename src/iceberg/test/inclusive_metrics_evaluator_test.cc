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
#include "iceberg/util/truncate_util.h"

namespace iceberg {

namespace {
constexpr bool kRowsMightMatch = true;
constexpr bool kRowCannotMatch = false;
constexpr int64_t kIntMinValue = 30;
constexpr int64_t kIntMaxValue = 79;
constexpr float kFloatNan = std::numeric_limits<float>::quiet_NaN();
constexpr double kDoubleNan = std::numeric_limits<double>::quiet_NaN();
}  // namespace
using TestVariant = std::variant<bool, int32_t, int64_t, double, std::string>;

class InclusiveMetricsEvaluatorTest : public ::testing::Test {
 protected:
  Result<std::shared_ptr<Expression>> Bind(const std::shared_ptr<Expression>& expr,
                                           bool case_sensitive = true) {
    return Binder::Bind(*schema_, expr, case_sensitive);
  }

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
        0);
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
    auto result = evaluator->Evaluate(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), expected_result) << unbound->ToString();
  }

  void TestStringCase(const std::shared_ptr<Expression>& unbound, bool expected_result) {
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           InclusiveMetricsEvaluator::Make(unbound, *schema_, true));
    auto file = PrepareDataFile("20251128", 10, 1024, {{"name", "123"}},
                                {{"name", "456"}}, {{2, 10}}, {{2, 0}});
    auto result = evaluator->Evaluate(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), expected_result) << unbound->ToString();
  }

 protected:
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
    auto result = evaluator->Evaluate(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), kRowsMightMatch) << unbound->ToString();
  }
  {
    auto unbound = Expressions::IsNull("name");
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           InclusiveMetricsEvaluator::Make(unbound, *schema_, true));
    auto file = PrepareDataFile("20251128", 10, 1024, {{"name", "1"}}, {{"name", "2"}},
                                {{2, 10}}, {{2, 0}}, {});
    auto result = evaluator->Evaluate(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), kRowCannotMatch) << unbound->ToString();
  }
}

TEST_F(InclusiveMetricsEvaluatorTest, NotNullTest) {
  {
    auto unbound = Expressions::NotNull("name");
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           InclusiveMetricsEvaluator::Make(unbound, *schema_, true));
    auto file = PrepareDataFile("20251128", 10, 1024, {{"name", "1"}}, {{"name", "2"}},
                                {{2, 10}}, {{2, 5}}, {});
    auto result = evaluator->Evaluate(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), kRowsMightMatch) << unbound->ToString();
  }
  {
    auto unbound = Expressions::NotNull("name");
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           InclusiveMetricsEvaluator::Make(unbound, *schema_, true));
    auto file = PrepareDataFile("20251128", 10, 1024, {{"name", "1"}}, {{"name", "2"}},
                                {{2, 10}}, {{2, 10}}, {});
    auto result = evaluator->Evaluate(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), kRowCannotMatch) << unbound->ToString();
  }
}

TEST_F(InclusiveMetricsEvaluatorTest, IsNanTest) {
  {
    auto unbound = Expressions::IsNaN("salary");
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           InclusiveMetricsEvaluator::Make(unbound, *schema_, true));
    auto file = PrepareDataFile("20251128", 10, 1024, {{"salary", 1.0}},
                                {{"salary", 2.0}}, {{4, 10}}, {{4, 5}}, {{4, 5}});
    auto result = evaluator->Evaluate(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), kRowsMightMatch) << unbound->ToString();
  }
  {
    auto unbound = Expressions::IsNaN("salary");
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           InclusiveMetricsEvaluator::Make(unbound, *schema_, true));
    auto file = PrepareDataFile("20251128", 10, 1024, {{"salary", 1.0}},
                                {{"salary", 2.0}}, {{4, 10}}, {{4, 10}}, {{4, 5}});
    auto result = evaluator->Evaluate(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), kRowCannotMatch) << unbound->ToString();
  }
  {
    auto unbound = Expressions::IsNaN("salary");
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           InclusiveMetricsEvaluator::Make(unbound, *schema_, true));
    auto file = PrepareDataFile("20251128", 10, 1024, {{"salary", 1.0}},
                                {{"salary", 2.0}}, {{4, 10}}, {{4, 5}}, {{4, 0}});
    auto result = evaluator->Evaluate(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), kRowCannotMatch) << unbound->ToString();
  }
}

TEST_F(InclusiveMetricsEvaluatorTest, NotNanTest) {
  {
    auto unbound = Expressions::NotNaN("salary");
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           InclusiveMetricsEvaluator::Make(unbound, *schema_, true));
    auto file = PrepareDataFile("20251128", 10, 1024, {{"salary", 1.0}},
                                {{"salary", 2.0}}, {{4, 10}}, {}, {{4, 5}});
    auto result = evaluator->Evaluate(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), kRowsMightMatch) << unbound->ToString();
  }
  {
    auto unbound = Expressions::NotNaN("salary");
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           InclusiveMetricsEvaluator::Make(unbound, *schema_, true));
    auto file = PrepareDataFile("20251128", 10, 1024, {{"salary", 1.0}},
                                {{"salary", 2.0}}, {{4, 10}}, {}, {{4, 10}});
    auto result = evaluator->Evaluate(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), kRowCannotMatch) << unbound->ToString();
  }
}

TEST_F(InclusiveMetricsEvaluatorTest, LTTest) {
  TestCase(Expressions::LessThan("id", Literal::Long(300)), kRowsMightMatch);
  TestCase(Expressions::LessThan("id", Literal::Long(150)), kRowsMightMatch);
  TestCase(Expressions::LessThan("id", Literal::Long(100)), kRowCannotMatch);
  TestCase(Expressions::LessThan("id", Literal::Long(200)), kRowsMightMatch);
  TestCase(Expressions::LessThan("id", Literal::Long(99)), kRowCannotMatch);
}

TEST_F(InclusiveMetricsEvaluatorTest, LTEQTest) {
  TestCase(Expressions::LessThanOrEqual("id", Literal::Long(300)), kRowsMightMatch);
  TestCase(Expressions::LessThanOrEqual("id", Literal::Long(150)), kRowsMightMatch);
  TestCase(Expressions::LessThanOrEqual("id", Literal::Long(100)), kRowsMightMatch);
  TestCase(Expressions::LessThanOrEqual("id", Literal::Long(200)), kRowsMightMatch);
  TestCase(Expressions::LessThanOrEqual("id", Literal::Long(99)), kRowCannotMatch);
}

TEST_F(InclusiveMetricsEvaluatorTest, GTTest) {
  TestCase(Expressions::GreaterThan("id", Literal::Long(300)), kRowCannotMatch);
  TestCase(Expressions::GreaterThan("id", Literal::Long(150)), kRowsMightMatch);
  TestCase(Expressions::GreaterThan("id", Literal::Long(100)), kRowsMightMatch);
  TestCase(Expressions::GreaterThan("id", Literal::Long(200)), kRowCannotMatch);
  TestCase(Expressions::GreaterThan("id", Literal::Long(99)), kRowsMightMatch);
}

TEST_F(InclusiveMetricsEvaluatorTest, GTEQTest) {
  TestCase(Expressions::GreaterThanOrEqual("id", Literal::Long(300)), kRowCannotMatch);
  TestCase(Expressions::GreaterThanOrEqual("id", Literal::Long(150)), kRowsMightMatch);
  TestCase(Expressions::GreaterThanOrEqual("id", Literal::Long(100)), kRowsMightMatch);
  TestCase(Expressions::GreaterThanOrEqual("id", Literal::Long(200)), kRowsMightMatch);
  TestCase(Expressions::GreaterThanOrEqual("id", Literal::Long(99)), kRowsMightMatch);
}

TEST_F(InclusiveMetricsEvaluatorTest, EQTest) {
  TestCase(Expressions::Equal("id", Literal::Long(300)), kRowCannotMatch);
  TestCase(Expressions::Equal("id", Literal::Long(150)), kRowsMightMatch);
  TestCase(Expressions::Equal("id", Literal::Long(100)), kRowsMightMatch);
  TestCase(Expressions::Equal("id", Literal::Long(200)), kRowsMightMatch);
}

TEST_F(InclusiveMetricsEvaluatorTest, NotEqTest) {
  TestCase(Expressions::NotEqual("id", Literal::Long(300)), kRowsMightMatch);
  TestCase(Expressions::NotEqual("id", Literal::Long(150)), kRowsMightMatch);
  TestCase(Expressions::NotEqual("id", Literal::Long(100)), kRowsMightMatch);
  TestCase(Expressions::NotEqual("id", Literal::Long(200)), kRowsMightMatch);
}

TEST_F(InclusiveMetricsEvaluatorTest, InTest) {
  TestCase(Expressions::In("id",
                           {
                               Literal::Long(300),
                               Literal::Long(400),
                               Literal::Long(500),
                           }),
           kRowCannotMatch);
  TestCase(Expressions::In("id",
                           {
                               Literal::Long(150),
                               Literal::Long(300),
                           }),
           kRowsMightMatch);
  TestCase(Expressions::In("id", {Literal::Long(100)}), kRowsMightMatch);
  TestCase(Expressions::In("id", {Literal::Long(200)}), kRowsMightMatch);
  TestCase(Expressions::In("id",
                           {
                               Literal::Long(99),
                               Literal::Long(201),
                           }),
           kRowCannotMatch);
}

TEST_F(InclusiveMetricsEvaluatorTest, NotInTest) {
  TestCase(Expressions::NotIn("id",
                              {
                                  Literal::Long(300),
                                  Literal::Long(400),
                                  Literal::Long(500),
                              }),
           kRowsMightMatch);
  TestCase(Expressions::NotIn("id",
                              {
                                  Literal::Long(150),
                                  Literal::Long(300),
                              }),
           kRowsMightMatch);
  TestCase(Expressions::NotIn("id",
                              {
                                  Literal::Long(100),
                                  Literal::Long(200),
                              }),
           kRowsMightMatch);
  TestCase(Expressions::NotIn("id",
                              {
                                  Literal::Long(99),
                                  Literal::Long(201),
                              }),
           kRowsMightMatch);
}

TEST_F(InclusiveMetricsEvaluatorTest, StartsWithTest) {
  TestStringCase(Expressions::StartsWith("name", "1"), kRowsMightMatch);
  TestStringCase(Expressions::StartsWith("name", "4"), kRowsMightMatch);
  TestStringCase(Expressions::StartsWith("name", "12"), kRowsMightMatch);
  TestStringCase(Expressions::StartsWith("name", "45"), kRowsMightMatch);
  TestStringCase(Expressions::StartsWith("name", "123"), kRowsMightMatch);
  TestStringCase(Expressions::StartsWith("name", "456"), kRowsMightMatch);
  TestStringCase(Expressions::StartsWith("name", "1234"), kRowsMightMatch);
  TestStringCase(Expressions::StartsWith("name", "4567"), kRowCannotMatch);
  TestStringCase(Expressions::StartsWith("name", "78"), kRowCannotMatch);
  TestStringCase(Expressions::StartsWith("name", "7"), kRowCannotMatch);
  TestStringCase(Expressions::StartsWith("name", "A"), kRowCannotMatch);
}

TEST_F(InclusiveMetricsEvaluatorTest, NotStartsWithTest) {
  TestStringCase(Expressions::NotStartsWith("name", "1"), kRowsMightMatch);
  TestStringCase(Expressions::NotStartsWith("name", "4"), kRowsMightMatch);
  TestStringCase(Expressions::NotStartsWith("name", "12"), kRowsMightMatch);
  TestStringCase(Expressions::NotStartsWith("name", "45"), kRowsMightMatch);
  TestStringCase(Expressions::NotStartsWith("name", "123"), kRowsMightMatch);
  TestStringCase(Expressions::NotStartsWith("name", "456"), kRowsMightMatch);
  TestStringCase(Expressions::NotStartsWith("name", "1234"), kRowsMightMatch);
  TestStringCase(Expressions::NotStartsWith("name", "4567"), kRowsMightMatch);
  TestStringCase(Expressions::NotStartsWith("name", "78"), kRowsMightMatch);
  TestStringCase(Expressions::NotStartsWith("name", "7"), kRowsMightMatch);
  TestStringCase(Expressions::NotStartsWith("name", "A"), kRowsMightMatch);

  auto RunTest = [&](const std::string& prefix, bool expected_result) {
    auto unbound = Expressions::NotStartsWith("name", prefix);
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           InclusiveMetricsEvaluator::Make(unbound, *schema_, true));
    auto file = PrepareDataFile("20251128", 10, 1024, {{"name", "123"}},
                                {{"name", "123"}}, {{2, 10}}, {{2, 0}});
    auto result = evaluator->Evaluate(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), expected_result) << unbound->ToString();
  };
  RunTest("12", kRowCannotMatch);
  RunTest("123", kRowCannotMatch);
  RunTest("1234", kRowsMightMatch);
}

class InclusiveMetricsEvaluatorMigratedTest : public InclusiveMetricsEvaluatorTest {
 protected:
  void SetUp() override {
    schema_ = std::make_shared<Schema>(
        std::vector<SchemaField>{
            SchemaField::MakeRequired(1, "id", int64()),
            SchemaField::MakeOptional(2, "no_stats", int64()),
            SchemaField::MakeRequired(3, "required", string()),
            SchemaField::MakeOptional(4, "all_nulls", string()),
            SchemaField::MakeOptional(5, "some_nulls", string()),
            SchemaField::MakeOptional(6, "no_nulls", string()),
            SchemaField::MakeOptional(7, "all_nans", float64()),
            SchemaField::MakeOptional(8, "some_nans", float32()),
            SchemaField::MakeOptional(9, "no_nans", float32()),
            SchemaField::MakeOptional(10, "all_nulls_double", float64()),
            SchemaField::MakeOptional(11, "all_nans_v1_stats", float32()),
            SchemaField::MakeOptional(12, "nan_and_null_only", float64()),
            SchemaField::MakeOptional(13, "no_nan_stats", float64()),
            SchemaField::MakeOptional(14, "some_empty", string()),
        },
        /*schema_id=*/0);
    file1_ = PrepareDataFile1();
    file2_ = PrepareDataFile2();
    file3_ = PrepareDataFile3();
    file4_ = PrepareDataFile4();
    file5_ = PrepareDataFile5();
  }

  std::shared_ptr<DataFile> PrepareDataFile1() {
    auto data_file = std::make_shared<DataFile>();
    data_file->file_path = "test_path1";
    data_file->file_format = FileFormatType::kParquet;
    data_file->record_count = 50;
    data_file->value_counts = {
        {4, 50L},  {5, 50L},  {6, 50L},  {7, 50L},  {8, 50L},  {9, 50L},
        {10, 50L}, {11, 50L}, {12, 50L}, {13, 50L}, {14, 50L},
    };
    data_file->null_value_counts = {
        {4, 50L}, {5, 10L}, {6, 0L}, {10, 50L}, {11, 0L}, {12, 1L}, {14, 0L},
    };
    data_file->nan_value_counts = {
        {7, 50L},
        {8, 10L},
        {9, 0L},
    };
    data_file->lower_bounds = {
        {1, Literal::Long(kIntMinValue).Serialize().value()},
        {11, Literal::Float(kFloatNan).Serialize().value()},
        {12, Literal::Double(kDoubleNan).Serialize().value()},
        {14, Literal::String("").Serialize().value()},
    };
    data_file->upper_bounds = {
        {1, Literal::Long(kIntMaxValue).Serialize().value()},
        {11, Literal::Float(kFloatNan).Serialize().value()},
        {12, Literal::Double(kDoubleNan).Serialize().value()},
        {14, Literal::String("房东整租霍营小区二层两居室").Serialize().value()},
    };
    return data_file;
  }

  std::shared_ptr<DataFile> PrepareDataFile2() {
    auto data_file = std::make_shared<DataFile>();
    data_file->file_path = "test_path2";
    data_file->file_format = FileFormatType::kParquet;
    data_file->record_count = 50;
    data_file->value_counts = {
        {3, 50L},
    };
    data_file->null_value_counts = {
        {3, 0L},
    };
    data_file->nan_value_counts = {};
    data_file->lower_bounds = {
        {3, Literal::String("aa").Serialize().value()},
    };
    data_file->upper_bounds = {
        {3, Literal::String("dC").Serialize().value()},
    };
    return data_file;
  }

  std::shared_ptr<DataFile> PrepareDataFile3() {
    auto data_file = std::make_shared<DataFile>();
    data_file->file_path = "test_path3";
    data_file->file_format = FileFormatType::kParquet;
    data_file->record_count = 50;
    data_file->value_counts = {
        {3, 50L},
    };
    data_file->null_value_counts = {
        {3, 0L},
    };
    data_file->nan_value_counts = {};
    data_file->lower_bounds = {
        {3, Literal::String("1str1").Serialize().value()},
    };
    data_file->upper_bounds = {
        {3, Literal::String("3str3").Serialize().value()},
    };
    return data_file;
  }

  std::shared_ptr<DataFile> PrepareDataFile4() {
    auto data_file = std::make_shared<DataFile>();
    data_file->file_path = "test_path4";
    data_file->file_format = FileFormatType::kParquet;
    data_file->record_count = 50;
    data_file->value_counts = {
        {3, 50L},
    };
    data_file->null_value_counts = {
        {3, 0L},
    };
    data_file->nan_value_counts = {};
    data_file->lower_bounds = {
        {3, Literal::String("abc").Serialize().value()},
    };
    data_file->upper_bounds = {
        {3, Literal::String("イロハニホヘト").Serialize().value()},
    };
    return data_file;
  }

  std::shared_ptr<DataFile> PrepareDataFile5() {
    auto data_file = std::make_shared<DataFile>();
    data_file->file_path = "test_path5";
    data_file->file_format = FileFormatType::kParquet;
    data_file->record_count = 50;
    data_file->value_counts = {
        {3, 50L},
    };
    data_file->null_value_counts = {
        {3, 0L},
    };
    data_file->nan_value_counts = {};
    data_file->lower_bounds = {
        {3, Literal::String("abc").Serialize().value()},
    };
    data_file->upper_bounds = {
        {3, Literal::String("abcdefghi").Serialize().value()},
    };
    return data_file;
  }

  void RunTest(const std::shared_ptr<Expression>& expr, bool expected_result,
               const std::shared_ptr<DataFile>& file, bool case_sensitive = true) {
    ICEBERG_UNWRAP_OR_FAIL(
        auto evaluator, InclusiveMetricsEvaluator::Make(expr, *schema_, case_sensitive));
    auto result = evaluator->Evaluate(*file);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), expected_result) << expr->ToString();
  };

  std::shared_ptr<DataFile> file1_;
  std::shared_ptr<DataFile> file2_;
  std::shared_ptr<DataFile> file3_;
  std::shared_ptr<DataFile> file4_;
  std::shared_ptr<DataFile> file5_;
};

TEST_F(InclusiveMetricsEvaluatorMigratedTest, CaseSensitiveTest) {
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

TEST_F(InclusiveMetricsEvaluatorMigratedTest, AllNullsTest) {
  RunTest(Expressions::NotNull("all_nulls"), kRowCannotMatch, file1_);
  RunTest(Expressions::LessThan("all_nulls", Literal::String("a")), kRowCannotMatch,
          file1_);
  RunTest(Expressions::LessThanOrEqual("all_nulls", Literal::String("a")),
          kRowCannotMatch, file1_);
  RunTest(Expressions::GreaterThan("all_nulls", Literal::String("a")), kRowCannotMatch,
          file1_);
  RunTest(Expressions::GreaterThanOrEqual("all_nulls", Literal::String("a")),
          kRowCannotMatch, file1_);
  RunTest(Expressions::Equal("all_nulls", Literal::String("a")), kRowCannotMatch, file1_);
  RunTest(Expressions::StartsWith("all_nulls", "a"), kRowCannotMatch, file1_);
  RunTest(Expressions::NotStartsWith("all_nulls", "a"), kRowsMightMatch, file1_);
  RunTest(Expressions::NotNull("some_nulls"), kRowsMightMatch, file1_);
  RunTest(Expressions::NotNull("no_nulls"), kRowsMightMatch, file1_);
}

TEST_F(InclusiveMetricsEvaluatorMigratedTest, NoNullsTest) {
  RunTest(Expressions::IsNull("all_nulls"), kRowsMightMatch, file1_);
  RunTest(Expressions::IsNull("some_nulls"), kRowsMightMatch, file1_);
  RunTest(Expressions::IsNull("no_nulls"), kRowCannotMatch, file1_);
}

TEST_F(InclusiveMetricsEvaluatorMigratedTest, IsNaNTest) {
  RunTest(Expressions::IsNaN("all_nans"), kRowsMightMatch, file1_);
  RunTest(Expressions::IsNaN("some_nans"), kRowsMightMatch, file1_);
  RunTest(Expressions::IsNaN("no_nans"), kRowCannotMatch, file1_);
  RunTest(Expressions::IsNaN("all_nulls_double"), kRowCannotMatch, file1_);
  RunTest(Expressions::IsNaN("no_nan_stats"), kRowsMightMatch, file1_);
  RunTest(Expressions::IsNaN("all_nans_v1_stats"), kRowsMightMatch, file1_);
  RunTest(Expressions::IsNaN("nan_and_null_only"), kRowsMightMatch, file1_);
}

TEST_F(InclusiveMetricsEvaluatorMigratedTest, NotNaNTest) {
  RunTest(Expressions::NotNaN("all_nans"), kRowCannotMatch, file1_);
  RunTest(Expressions::NotNaN("some_nans"), kRowsMightMatch, file1_);
  RunTest(Expressions::NotNaN("no_nans"), kRowsMightMatch, file1_);
  RunTest(Expressions::NotNaN("all_nulls_double"), kRowsMightMatch, file1_);
  RunTest(Expressions::NotNaN("no_nan_stats"), kRowsMightMatch, file1_);
  RunTest(Expressions::NotNaN("all_nans_v1_stats"), kRowsMightMatch, file1_);
  RunTest(Expressions::NotNaN("nan_and_null_only"), kRowsMightMatch, file1_);
}

TEST_F(InclusiveMetricsEvaluatorMigratedTest, RequiredColumnTest) {
  RunTest(Expressions::NotNull("required"), kRowsMightMatch, file1_);
  RunTest(Expressions::IsNull("required"), kRowCannotMatch, file1_);
}

TEST_F(InclusiveMetricsEvaluatorMigratedTest, MissingColumnTest) {
  auto expr = Expressions::LessThan("missing", Literal::Long(5));
  auto result = InclusiveMetricsEvaluator::Make(expr, *schema_, true);
  ASSERT_FALSE(result.has_value()) << result.error().message;
  ASSERT_TRUE(result.error().message.contains("Cannot find field 'missing' in struct"))
      << result.error().message;
}

TEST_F(InclusiveMetricsEvaluatorMigratedTest, MissingStatsTest) {
  auto data_file = std::make_shared<DataFile>();
  data_file->file_path = "test_path";
  data_file->file_format = FileFormatType::kParquet;
  data_file->record_count = 50;

  RunTest(Expressions::LessThan("no_stats", Literal::Long(5)), kRowsMightMatch,
          data_file);
  RunTest(Expressions::LessThanOrEqual("no_stats", Literal::Long(30)), kRowsMightMatch,
          data_file);
  RunTest(Expressions::Equal("no_stats", Literal::Long(70)), kRowsMightMatch, data_file);
  RunTest(Expressions::GreaterThan("no_stats", Literal::Long(78)), kRowsMightMatch,
          data_file);
  RunTest(Expressions::GreaterThanOrEqual("no_stats", Literal::Long(90)), kRowsMightMatch,
          data_file);
  RunTest(Expressions::NotEqual("no_stats", Literal::Long(101)), kRowsMightMatch,
          data_file);
  RunTest(Expressions::IsNull("no_stats"), kRowsMightMatch, data_file);
  RunTest(Expressions::NotNull("no_stats"), kRowsMightMatch, data_file);
  RunTest(Expressions::IsNaN("some_nans"), kRowsMightMatch, data_file);
  RunTest(Expressions::NotNaN("some_nans"), kRowsMightMatch, data_file);
}

TEST_F(InclusiveMetricsEvaluatorMigratedTest, ZeroRecordFileTest) {
  auto data_file = std::make_shared<DataFile>();
  data_file->file_path = "test_path";
  data_file->file_format = FileFormatType::kParquet;
  data_file->record_count = 0;

  RunTest(Expressions::LessThan("no_stats", Literal::Long(5)), kRowCannotMatch,
          data_file);
  RunTest(Expressions::LessThanOrEqual("no_stats", Literal::Long(30)), kRowCannotMatch,
          data_file);
  RunTest(Expressions::Equal("no_stats", Literal::Long(70)), kRowCannotMatch, data_file);
  RunTest(Expressions::GreaterThan("no_stats", Literal::Long(78)), kRowCannotMatch,
          data_file);
  RunTest(Expressions::GreaterThanOrEqual("no_stats", Literal::Long(90)), kRowCannotMatch,
          data_file);
  RunTest(Expressions::NotEqual("no_stats", Literal::Long(101)), kRowCannotMatch,
          data_file);
  RunTest(Expressions::IsNull("some_nulls"), kRowCannotMatch, data_file);
  RunTest(Expressions::NotNull("some_nulls"), kRowCannotMatch, data_file);
  RunTest(Expressions::IsNaN("some_nans"), kRowCannotMatch, data_file);
  RunTest(Expressions::NotNaN("some_nans"), kRowCannotMatch, data_file);
}

TEST_F(InclusiveMetricsEvaluatorMigratedTest, NotTest) {
  RunTest(Expressions::Not(Expressions::LessThan("id", Literal::Long(kIntMinValue - 25))),
          kRowsMightMatch, file1_);
  RunTest(
      Expressions::Not(Expressions::GreaterThan("id", Literal::Long(kIntMinValue - 25))),
      kRowCannotMatch, file1_);
}

TEST_F(InclusiveMetricsEvaluatorMigratedTest, AndTest) {
  RunTest(Expressions::And(
              Expressions::LessThan("id", Literal::Long(kIntMinValue - 25)),
              Expressions::GreaterThanOrEqual("id", Literal::Long(kIntMinValue - 30))),
          kRowCannotMatch, file1_);
  RunTest(Expressions::And(
              Expressions::LessThan("id", Literal::Long(kIntMinValue - 25)),
              Expressions::GreaterThanOrEqual("id", Literal::Long(kIntMaxValue + 1))),
          kRowCannotMatch, file1_);
  RunTest(
      Expressions::And(Expressions::GreaterThan("id", Literal::Long(kIntMinValue - 25)),
                       Expressions::LessThanOrEqual("id", Literal::Long(kIntMaxValue))),
      kRowsMightMatch, file1_);
}

TEST_F(InclusiveMetricsEvaluatorMigratedTest, OrTest) {
  RunTest(Expressions::Or(
              Expressions::LessThan("id", Literal::Long(kIntMinValue - 25)),
              Expressions::GreaterThanOrEqual("id", Literal::Long(kIntMaxValue + 1))),
          kRowCannotMatch, file1_);
  RunTest(Expressions::Or(
              Expressions::LessThan("id", Literal::Long(kIntMinValue - 25)),
              Expressions::GreaterThanOrEqual("id", Literal::Long(kIntMaxValue - 19))),
          kRowsMightMatch, file1_);
}

TEST_F(InclusiveMetricsEvaluatorMigratedTest, IntegerLtTest) {
  RunTest(Expressions::LessThan("id", Literal::Long(kIntMinValue - 25)), kRowCannotMatch,
          file1_);
  RunTest(Expressions::LessThan("id", Literal::Long(kIntMinValue)), kRowCannotMatch,
          file1_);
  RunTest(Expressions::LessThan("id", Literal::Long(kIntMinValue + 1)), kRowsMightMatch,
          file1_);
  RunTest(Expressions::LessThan("id", Literal::Long(kIntMaxValue)), kRowsMightMatch,
          file1_);
}

TEST_F(InclusiveMetricsEvaluatorMigratedTest, IntegerLtEqTest) {
  RunTest(Expressions::LessThanOrEqual("id", Literal::Long(kIntMinValue - 25)),
          kRowCannotMatch, file1_);
  RunTest(Expressions::LessThanOrEqual("id", Literal::Long(kIntMinValue - 1)),
          kRowCannotMatch, file1_);
  RunTest(Expressions::LessThanOrEqual("id", Literal::Long(kIntMinValue)),
          kRowsMightMatch, file1_);
  RunTest(Expressions::LessThanOrEqual("id", Literal::Long(kIntMaxValue)),
          kRowsMightMatch, file1_);
}

TEST_F(InclusiveMetricsEvaluatorMigratedTest, IntegerGtTest) {
  RunTest(Expressions::GreaterThan("id", Literal::Long(kIntMaxValue + 6)),
          kRowCannotMatch, file1_);
  RunTest(Expressions::GreaterThan("id", Literal::Long(kIntMaxValue)), kRowCannotMatch,
          file1_);
  RunTest(Expressions::GreaterThan("id", Literal::Long(kIntMaxValue - 1)),
          kRowsMightMatch, file1_);
  RunTest(Expressions::GreaterThan("id", Literal::Long(kIntMaxValue - 4)),
          kRowsMightMatch, file1_);
}

TEST_F(InclusiveMetricsEvaluatorMigratedTest, IntegerGtEqTest) {
  RunTest(Expressions::GreaterThanOrEqual("id", Literal::Long(kIntMaxValue + 6)),
          kRowCannotMatch, file1_);
  RunTest(Expressions::GreaterThanOrEqual("id", Literal::Long(kIntMaxValue + 1)),
          kRowCannotMatch, file1_);
  RunTest(Expressions::GreaterThanOrEqual("id", Literal::Long(kIntMaxValue)),
          kRowsMightMatch, file1_);
  RunTest(Expressions::GreaterThanOrEqual("id", Literal::Long(kIntMaxValue - 4)),
          kRowsMightMatch, file1_);
}

TEST_F(InclusiveMetricsEvaluatorMigratedTest, IntegerEqTest) {
  RunTest(Expressions::Equal("id", Literal::Long(kIntMinValue - 25)), kRowCannotMatch,
          file1_);
  RunTest(Expressions::Equal("id", Literal::Long(kIntMinValue - 1)), kRowCannotMatch,
          file1_);
  RunTest(Expressions::Equal("id", Literal::Long(kIntMinValue)), kRowsMightMatch, file1_);
  RunTest(Expressions::Equal("id", Literal::Long(kIntMaxValue - 4)), kRowsMightMatch,
          file1_);
  RunTest(Expressions::Equal("id", Literal::Long(kIntMaxValue)), kRowsMightMatch, file1_);
  RunTest(Expressions::Equal("id", Literal::Long(kIntMaxValue + 1)), kRowCannotMatch,
          file1_);
  RunTest(Expressions::Equal("id", Literal::Long(kIntMaxValue + 6)), kRowCannotMatch,
          file1_);
}

TEST_F(InclusiveMetricsEvaluatorMigratedTest, IntegerNotEqTest) {
  RunTest(Expressions::NotEqual("id", Literal::Long(kIntMinValue - 25)), kRowsMightMatch,
          file1_);
  RunTest(Expressions::NotEqual("id", Literal::Long(kIntMinValue - 1)), kRowsMightMatch,
          file1_);
  RunTest(Expressions::NotEqual("id", Literal::Long(kIntMinValue)), kRowsMightMatch,
          file1_);
  RunTest(Expressions::NotEqual("id", Literal::Long(kIntMaxValue - 4)), kRowsMightMatch,
          file1_);
  RunTest(Expressions::NotEqual("id", Literal::Long(kIntMaxValue)), kRowsMightMatch,
          file1_);
  RunTest(Expressions::NotEqual("id", Literal::Long(kIntMaxValue + 1)), kRowsMightMatch,
          file1_);
  RunTest(Expressions::NotEqual("id", Literal::Long(kIntMaxValue + 6)), kRowsMightMatch,
          file1_);
}

TEST_F(InclusiveMetricsEvaluatorMigratedTest, IntegerNotEqRewrittenTest) {
  RunTest(Expressions::Not(Expressions::Equal("id", Literal::Long(kIntMinValue - 25))),
          kRowsMightMatch, file1_);
  RunTest(Expressions::Not(Expressions::Equal("id", Literal::Long(kIntMinValue - 1))),
          kRowsMightMatch, file1_);
  RunTest(Expressions::Not(Expressions::Equal("id", Literal::Long(kIntMinValue))),
          kRowsMightMatch, file1_);
  RunTest(Expressions::Not(Expressions::Equal("id", Literal::Long(kIntMaxValue - 4))),
          kRowsMightMatch, file1_);
  RunTest(Expressions::Not(Expressions::Equal("id", Literal::Long(kIntMaxValue))),
          kRowsMightMatch, file1_);
  RunTest(Expressions::Not(Expressions::Equal("id", Literal::Long(kIntMaxValue + 1))),
          kRowsMightMatch, file1_);
  RunTest(Expressions::Not(Expressions::Equal("id", Literal::Long(kIntMaxValue + 6))),
          kRowsMightMatch, file1_);
}

TEST_F(InclusiveMetricsEvaluatorMigratedTest, CaseInsensitiveIntegerNotEqRewrittenTest) {
  RunTest(Expressions::Not(Expressions::Equal("ID", Literal::Long(kIntMinValue - 25))),
          kRowsMightMatch, file1_, false);
  RunTest(Expressions::Not(Expressions::Equal("ID", Literal::Long(kIntMinValue - 1))),
          kRowsMightMatch, file1_, false);
  RunTest(Expressions::Not(Expressions::Equal("ID", Literal::Long(kIntMinValue))),
          kRowsMightMatch, file1_, false);
  RunTest(Expressions::Not(Expressions::Equal("ID", Literal::Long(kIntMaxValue - 4))),
          kRowsMightMatch, file1_, false);
  RunTest(Expressions::Not(Expressions::Equal("ID", Literal::Long(kIntMaxValue))),
          kRowsMightMatch, file1_, false);
  RunTest(Expressions::Not(Expressions::Equal("ID", Literal::Long(kIntMaxValue + 1))),
          kRowsMightMatch, file1_, false);
  RunTest(Expressions::Not(Expressions::Equal("ID", Literal::Long(kIntMaxValue + 6))),
          kRowsMightMatch, file1_, false);
}

TEST_F(InclusiveMetricsEvaluatorMigratedTest, CaseSensitiveIntegerNotEqRewrittenTest) {
  auto expr = Expressions::Not(Expressions::Equal("ID", Literal::Long(5)));
  auto result = InclusiveMetricsEvaluator::Make(expr, *schema_, true);
  ASSERT_FALSE(result.has_value()) << result.error().message;
  ASSERT_TRUE(result.error().message.contains("Cannot find field 'ID' in struct"))
      << result.error().message;
}

TEST_F(InclusiveMetricsEvaluatorMigratedTest, StringStartsWithTest) {
  RunTest(Expressions::StartsWith("required", "a"), kRowsMightMatch, file1_);
  RunTest(Expressions::StartsWith("required", "a"), kRowsMightMatch, file2_);
  RunTest(Expressions::StartsWith("required", "aa"), kRowsMightMatch, file2_);
  RunTest(Expressions::StartsWith("required", "aaa"), kRowsMightMatch, file2_);
  RunTest(Expressions::StartsWith("required", "1s"), kRowsMightMatch, file3_);
  RunTest(Expressions::StartsWith("required", "1str1x"), kRowsMightMatch, file3_);
  RunTest(Expressions::StartsWith("required", "ff"), kRowsMightMatch, file4_);

  RunTest(Expressions::StartsWith("required", "aB"), kRowCannotMatch, file2_);
  RunTest(Expressions::StartsWith("required", "dWX"), kRowCannotMatch, file2_);

  RunTest(Expressions::StartsWith("required", "5"), kRowCannotMatch, file3_);
  RunTest(Expressions::StartsWith("required", "3str3x"), kRowCannotMatch, file3_);
  RunTest(Expressions::StartsWith("some_empty", "房东整租霍"), kRowsMightMatch, file1_);

  RunTest(Expressions::StartsWith("all_nulls", ""), kRowCannotMatch, file1_);
  auto above_max = TruncateUtils::TruncateLiteral(Literal::String("イロハニホヘト"), 4)
                       .value()
                       .ToString();
  RunTest(Expressions::StartsWith("required", above_max), kRowCannotMatch, file4_);
}

TEST_F(InclusiveMetricsEvaluatorMigratedTest, StringNotStartsWithTest) {
  RunTest(Expressions::NotStartsWith("required", "a"), kRowsMightMatch, file1_);
  RunTest(Expressions::NotStartsWith("required", "a"), kRowsMightMatch, file2_);
  RunTest(Expressions::NotStartsWith("required", "aa"), kRowsMightMatch, file2_);
  RunTest(Expressions::NotStartsWith("required", "aaa"), kRowsMightMatch, file2_);
  RunTest(Expressions::NotStartsWith("required", "1s"), kRowsMightMatch, file3_);
  RunTest(Expressions::NotStartsWith("required", "1str1x"), kRowsMightMatch, file3_);
  RunTest(Expressions::NotStartsWith("required", "ff"), kRowsMightMatch, file4_);

  RunTest(Expressions::NotStartsWith("required", "aB"), kRowsMightMatch, file2_);
  RunTest(Expressions::NotStartsWith("required", "dWX"), kRowsMightMatch, file2_);

  RunTest(Expressions::NotStartsWith("required", "5"), kRowsMightMatch, file3_);
  RunTest(Expressions::NotStartsWith("required", "3str3x"), kRowsMightMatch, file3_);

  auto above_max = TruncateUtils::TruncateLiteral(Literal::String("イロハニホヘト"), 4)
                       .value()
                       .ToString();
  RunTest(Expressions::NotStartsWith("required", above_max), kRowsMightMatch, file4_);

  RunTest(Expressions::NotStartsWith("required", "abc"), kRowCannotMatch, file5_);
  RunTest(Expressions::NotStartsWith("required", "abcd"), kRowsMightMatch, file5_);
}

TEST_F(InclusiveMetricsEvaluatorMigratedTest, IntegerInTest) {
  RunTest(Expressions::In(
              "id", {Literal::Long(kIntMinValue - 25), Literal::Long(kIntMinValue - 24)}),
          kRowCannotMatch, file1_);
  RunTest(Expressions::In(
              "id", {Literal::Long(kIntMinValue - 2), Literal::Long(kIntMinValue - 1)}),
          kRowCannotMatch, file1_);
  RunTest(Expressions::In("id",
                          {Literal::Long(kIntMinValue - 1), Literal::Long(kIntMinValue)}),
          kRowsMightMatch, file1_);
  RunTest(Expressions::In(
              "id", {Literal::Long(kIntMaxValue - 4), Literal::Long(kIntMaxValue - 3)}),
          kRowsMightMatch, file1_);
  RunTest(Expressions::In("id",
                          {Literal::Long(kIntMaxValue), Literal::Long(kIntMaxValue + 1)}),
          kRowsMightMatch, file1_);
  RunTest(Expressions::In(
              "id", {Literal::Long(kIntMaxValue + 1), Literal::Long(kIntMaxValue + 2)}),
          kRowCannotMatch, file1_);
  RunTest(Expressions::In(
              "id", {Literal::Long(kIntMaxValue + 6), Literal::Long(kIntMaxValue + 7)}),
          kRowCannotMatch, file1_);

  RunTest(Expressions::In("all_nulls", {Literal::String("abc"), Literal::String("def")}),
          kRowCannotMatch, file1_);
  RunTest(Expressions::In("some_nulls", {Literal::String("abc"), Literal::String("def")}),
          kRowsMightMatch, file1_);
  RunTest(Expressions::In("no_nulls", {Literal::String("abc"), Literal::String("def")}),
          kRowsMightMatch, file1_);

  std::vector<Literal> ids;
  for (int i = -400; i <= 0; i++) {
    ids.emplace_back(Literal::Long(i));
  }
  RunTest(Expressions::In("id", ids), kRowsMightMatch, file1_);
}

TEST_F(InclusiveMetricsEvaluatorMigratedTest, IntegerNotInTest) {
  RunTest(Expressions::NotIn(
              "id", {Literal::Long(kIntMinValue - 25), Literal::Long(kIntMinValue - 24)}),
          kRowsMightMatch, file1_);
  RunTest(Expressions::NotIn(
              "id", {Literal::Long(kIntMinValue - 2), Literal::Long(kIntMinValue - 1)}),
          kRowsMightMatch, file1_);
  RunTest(Expressions::NotIn(
              "id", {Literal::Long(kIntMinValue - 1), Literal::Long(kIntMinValue)}),
          kRowsMightMatch, file1_);
  RunTest(Expressions::NotIn(
              "id", {Literal::Long(kIntMaxValue - 4), Literal::Long(kIntMaxValue - 3)}),
          kRowsMightMatch, file1_);
  RunTest(Expressions::NotIn(
              "id", {Literal::Long(kIntMaxValue), Literal::Long(kIntMaxValue + 1)}),
          kRowsMightMatch, file1_);
  RunTest(Expressions::NotIn(
              "id", {Literal::Long(kIntMaxValue + 1), Literal::Long(kIntMaxValue + 2)}),
          kRowsMightMatch, file1_);
  RunTest(Expressions::NotIn(
              "id", {Literal::Long(kIntMaxValue + 6), Literal::Long(kIntMaxValue + 7)}),
          kRowsMightMatch, file1_);

  RunTest(
      Expressions::NotIn("all_nulls", {Literal::String("abc"), Literal::String("def")}),
      kRowsMightMatch, file1_);
  RunTest(
      Expressions::NotIn("some_nulls", {Literal::String("abc"), Literal::String("def")}),
      kRowsMightMatch, file1_);
  RunTest(
      Expressions::NotIn("no_nulls", {Literal::String("abc"), Literal::String("def")}),
      kRowsMightMatch, file1_);

  std::vector<Literal> ids;
  for (int i = -400; i <= 0; i++) {
    ids.emplace_back(Literal::Long(i));
  }
  RunTest(Expressions::NotIn("id", ids), kRowsMightMatch, file1_);
}

}  // namespace iceberg
