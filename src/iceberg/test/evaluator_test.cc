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

#include "iceberg/expression/evaluator.h"

#include <cstddef>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include <arrow/array.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/c/bridge.h>
#include <arrow/json/from_string.h>
#include <arrow/testing/gtest_util.h>
#include <arrow/type.h>
#include <gtest/gtest.h>

#include "iceberg/arrow_c_data_guard_internal.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/expression/literal.h"
#include "iceberg/result.h"
#include "iceberg/row/arrow_array_wrapper.h"
#include "iceberg/schema.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"

namespace iceberg {

class EvaluatorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    schema_ = std::make_unique<Schema>(std::vector<SchemaField>{
        SchemaField::MakeRequired(13, "x", int32()),
        SchemaField::MakeRequired(14, "y", float64()),
        SchemaField::MakeOptional(15, "z", int32()),
        SchemaField::MakeOptional(
            16, "s1",
            std::make_shared<StructType>(
                std::vector<SchemaField>{SchemaField::MakeRequired(
                    17, "s2",
                    std::make_shared<StructType>(
                        std::vector<SchemaField>{SchemaField::MakeRequired(
                            18, "s3",
                            std::make_shared<StructType>(
                                std::vector<SchemaField>{SchemaField::MakeRequired(
                                    19, "s4",
                                    std::make_shared<StructType>(std::vector<SchemaField>{
                                        SchemaField::MakeRequired(20, "i",
                                                                  int32())}))}))}))})),
        SchemaField::MakeOptional(
            21, "s5",
            std::make_shared<StructType>(
                std::vector<SchemaField>{SchemaField::MakeRequired(
                    22, "s6",
                    std::make_shared<StructType>(std::vector<SchemaField>{
                        SchemaField::MakeRequired(23, "f", float32())}))}))});

    arrow_data_type_ = ::arrow::struct_({
        ::arrow::field("x", ::arrow::int32(), /*nullable=*/false),
        ::arrow::field("y", ::arrow::float64(), /*nullable=*/false),
        ::arrow::field("z", ::arrow::int32(), /*nullable=*/true),
        ::arrow::field("s1",
                       ::arrow::struct_({::arrow::field(
                           "s2",
                           ::arrow::struct_({::arrow::field(
                               "s3",
                               ::arrow::struct_({::arrow::field(
                                   "s4",
                                   ::arrow::struct_({::arrow::field("i", ::arrow::int32(),
                                                                    /*nullable=*/false)}),
                                   /*nullable=*/false)}),
                               /*nullable=*/false)}),
                           /*nullable=*/false)}),
                       /*nullable=*/true),
        ::arrow::field("s5",
                       ::arrow::struct_({::arrow::field(
                           "s6",
                           ::arrow::struct_({::arrow::field("f", ::arrow::float32(),
                                                            /*nullable=*/false)}),
                           /*nullable=*/false)}),
                       /*nullable=*/true),
    });

    ASSERT_TRUE(::arrow::ExportType(*arrow_data_type_, &arrow_c_schema_).ok());
  }

  void TearDown() override {
    if (arrow_c_schema_.release != nullptr) {
      ArrowSchemaRelease(&arrow_c_schema_);
    }
  }

  void TestData(const std::string& json_data, Evaluator& evaluator,
                bool expected_result) {
    auto arrow_array =
        ::arrow::json::ArrayFromJSONString(arrow_data_type_, json_data).ValueOrDie();
    ASSERT_EQ(arrow_array->length(), 1)
        << "Expected 1 row, got " << arrow_array->length();

    ArrowArray arrow_c_array;
    internal::ArrowArrayGuard array_guard(&arrow_c_array);
    ASSERT_TRUE(::arrow::ExportArray(*arrow_array, &arrow_c_array).ok());

    ICEBERG_UNWRAP_OR_FAIL(auto struct_like,
                           ArrowArrayStructLike::Make(arrow_c_schema_, arrow_c_array, 0));
    ICEBERG_UNWRAP_OR_FAIL(auto result, evaluator.Eval(*struct_like));
    ASSERT_EQ(result, expected_result);
  }

  std::unique_ptr<Schema> schema_;
  std::shared_ptr<::arrow::DataType> arrow_data_type_;
  ArrowSchema arrow_c_schema_;
};

TEST_F(EvaluatorTest, LessThan) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto evaluator,
      Evaluator::Make(*schema_, Expressions::LessThan("x", Literal::Int(7))));

  // 7 < 7 => false
  TestData(R"([{"x": 7, "y": 8.0, "z": null, "s1": null, "s5": null}])", *evaluator,
           false);

  // 6 < 7 => true
  TestData(R"([{"x": 6, "y": 8.0, "z": null, "s1": null, "s5": null}])", *evaluator,
           true);

  ICEBERG_UNWRAP_OR_FAIL(
      auto struct_evaluator,
      Evaluator::Make(*schema_, Expressions::LessThan("s1.s2.s3.s4.i", Literal::Int(7))));

  // 7 < 7 => false
  TestData(
      R"([{"x": 7, "y": 8.0, "z": null, "s1": {"s2": {"s3": {"s4": {"i": 7}}}}, "s5": null}])",
      *struct_evaluator, false);

  // 6 < 7 => true
  TestData(
      R"([{"x": 6, "y": 8.0, "z": null, "s1": {"s2": {"s3": {"s4": {"i": 6}}}}, "s5": null}])",
      *struct_evaluator, true);
}

TEST_F(EvaluatorTest, LessThanOrEqual) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto evaluator,
      Evaluator::Make(*schema_, Expressions::LessThanOrEqual("x", Literal::Int(7))));

  // 7 <= 7 => true
  TestData(R"([{"x": 7, "y": 8.0, "z": null, "s1": null, "s5": null}])", *evaluator,
           true);

  // 6 <= 7 => true
  TestData(R"([{"x": 6, "y": 8.0, "z": null, "s1": null, "s5": null}])", *evaluator,
           true);

  // 8 <= 7 => false
  TestData(R"([{"x": 8, "y": 8.0, "z": null, "s1": null, "s5": null}])", *evaluator,
           false);

  ICEBERG_UNWRAP_OR_FAIL(
      auto struct_evaluator,
      Evaluator::Make(*schema_,
                      Expressions::LessThanOrEqual("s1.s2.s3.s4.i", Literal::Int(7))));

  // 7 <= 7 => true
  TestData(
      R"([{"x": 7, "y": 8.0, "z": null, "s1": {"s2": {"s3": {"s4": {"i": 7}}}}, "s5": null}])",
      *struct_evaluator, true);

  // 6 <= 7 => true
  TestData(
      R"([{"x": 6, "y": 8.0, "z": null, "s1": {"s2": {"s3": {"s4": {"i": 6}}}}, "s5": null}])",
      *struct_evaluator, true);

  // 8 <= 7 => false
  TestData(
      R"([{"x": 6, "y": 8.0, "z": null, "s1": {"s2": {"s3": {"s4": {"i": 8}}}}, "s5": null}])",
      *struct_evaluator, false);
}

TEST_F(EvaluatorTest, GreaterThan) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto evaluator,
      Evaluator::Make(*schema_, Expressions::GreaterThan("x", Literal::Int(7))));

  // 7 > 7 => false
  TestData(R"([{"x": 7, "y": 8.0, "z": null, "s1": null, "s5": null}])", *evaluator,
           false);

  // 6 > 7 => false
  TestData(R"([{"x": 6, "y": 8.0, "z": null, "s1": null, "s5": null}])", *evaluator,
           false);

  // 8 > 7 => true
  TestData(R"([{"x": 8, "y": 8.0, "z": null, "s1": null, "s5": null}])", *evaluator,
           true);

  ICEBERG_UNWRAP_OR_FAIL(
      auto struct_evaluator,
      Evaluator::Make(*schema_,
                      Expressions::GreaterThan("s1.s2.s3.s4.i", Literal::Int(7))));

  // 7 > 7 => false
  TestData(
      R"([{"x": 7, "y": 8.0, "z": null, "s1": {"s2": {"s3": {"s4": {"i": 7}}}}, "s5": null}])",
      *struct_evaluator, false);

  // 6 > 7 => false
  TestData(
      R"([{"x": 7, "y": 8.0, "z": null, "s1": {"s2": {"s3": {"s4": {"i": 6}}}}, "s5": null}])",
      *struct_evaluator, false);

  // 8 > 7 => true
  TestData(
      R"([{"x": 7, "y": 8.0, "z": null, "s1": {"s2": {"s3": {"s4": {"i": 8}}}}, "s5": null}])",
      *struct_evaluator, true);
}

TEST_F(EvaluatorTest, GreaterThanOrEqual) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto evaluator,
      Evaluator::Make(*schema_, Expressions::GreaterThanOrEqual("x", Literal::Int(7))));

  // 7 >= 7 => true
  TestData(R"([{"x": 7, "y": 8.0, "z": null, "s1": null, "s5": null}])", *evaluator,
           true);

  // 6 >= 7 => false
  TestData(R"([{"x": 6, "y": 8.0, "z": null, "s1": null, "s5": null}])", *evaluator,
           false);

  // 8 >= 7 => true
  TestData(R"([{"x": 8, "y": 8.0, "z": null, "s1": null, "s5": null}])", *evaluator,
           true);

  ICEBERG_UNWRAP_OR_FAIL(
      auto struct_evaluator,
      Evaluator::Make(*schema_,
                      Expressions::GreaterThanOrEqual("s1.s2.s3.s4.i", Literal::Int(7))));

  // 7 >= 7 => true
  TestData(
      R"([{"x": 7, "y": 8.0, "z": null, "s1": {"s2": {"s3": {"s4": {"i": 7}}}}, "s5": null}])",
      *struct_evaluator, true);

  // 6 >= 7 => false
  TestData(
      R"([{"x": 7, "y": 8.0, "z": null, "s1": {"s2": {"s3": {"s4": {"i": 6}}}}, "s5": null}])",
      *struct_evaluator, false);

  // 8 >= 7 => true
  TestData(
      R"([{"x": 7, "y": 8.0, "z": null, "s1": {"s2": {"s3": {"s4": {"i": 8}}}}, "s5": null}])",
      *struct_evaluator, true);
}

TEST_F(EvaluatorTest, Equal) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto evaluator,
      Evaluator::Make(*schema_, Expressions::Equal("x", Literal::Int(7))));

  // 7 == 7 => true
  TestData(R"([{"x": 7, "y": 8.0, "z": null, "s1": null, "s5": null}])", *evaluator,
           true);

  // 6 == 7 => false
  TestData(R"([{"x": 6, "y": 8.0, "z": null, "s1": null, "s5": null}])", *evaluator,
           false);

  ICEBERG_UNWRAP_OR_FAIL(
      auto struct_evaluator,
      Evaluator::Make(*schema_, Expressions::Equal("s1.s2.s3.s4.i", Literal::Int(7))));

  // 7 == 7 => true
  TestData(
      R"([{"x": 7, "y": 8.0, "z": null, "s1": {"s2": {"s3": {"s4": {"i": 7}}}}, "s5": null}])",
      *struct_evaluator, true);

  // 6 == 7 => false
  TestData(
      R"([{"x": 6, "y": 8.0, "z": null, "s1": {"s2": {"s3": {"s4": {"i": 6}}}}, "s5": null}])",
      *struct_evaluator, false);
}

TEST_F(EvaluatorTest, NotEqual) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto evaluator,
      Evaluator::Make(*schema_, Expressions::NotEqual("x", Literal::Int(7))));

  // 7 != 7 => false
  TestData(R"([{"x": 7, "y": 8.0, "z": null, "s1": null, "s5": null}])", *evaluator,
           false);

  // 6 != 7 => true
  TestData(R"([{"x": 6, "y": 8.0, "z": null, "s1": null, "s5": null}])", *evaluator,
           true);

  ICEBERG_UNWRAP_OR_FAIL(
      auto struct_evaluator,
      Evaluator::Make(*schema_, Expressions::NotEqual("s1.s2.s3.s4.i", Literal::Int(7))));

  // 7 != 7 => false
  TestData(
      R"([{"x": 7, "y": 8.0, "z": null, "s1": {"s2": {"s3": {"s4": {"i": 7}}}}, "s5": null}])",
      *struct_evaluator, false);

  // 6 != 7 => true
  TestData(
      R"([{"x": 6, "y": 8.0, "z": null, "s1": {"s2": {"s3": {"s4": {"i": 6}}}}, "s5": null}])",
      *struct_evaluator, true);
}

TEST_F(EvaluatorTest, StartsWith) {
  auto string_schema = std::make_unique<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(24, "s", string())});
  ICEBERG_UNWRAP_OR_FAIL(
      auto evaluator,
      Evaluator::Make(*string_schema, Expressions::StartsWith("s", "abc")));

  auto arrow_string_type = ::arrow::struct_({::arrow::field("s", ::arrow::utf8())});
  auto arrow_string_array = ::arrow::json::ArrayFromJSONString(arrow_string_type, R"([
        {"s": "abc"},
        {"s": "xabc"},
        {"s": "Abc"},
        {"s": "a"},
        {"s": "abcd"},
        {"s": null}
      ])")
                                .ValueOrDie();

  ArrowSchema c_schema;
  ArrowArray c_array;
  internal::ArrowSchemaGuard schema_guard(&c_schema);
  internal::ArrowArrayGuard array_guard(&c_array);
  ASSERT_TRUE(::arrow::ExportType(*arrow_string_type, &c_schema).ok());
  ASSERT_TRUE(::arrow::ExportArray(*arrow_string_array, &c_array).ok());
  ICEBERG_UNWRAP_OR_FAIL(auto struct_like,
                         ArrowArrayStructLike::Make(c_schema, c_array, /*row_index=*/0));

  // abc startsWith abc => true
  ASSERT_THAT(struct_like->Reset(0), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto result, evaluator->Eval(*struct_like));
  EXPECT_TRUE(result);

  // xabc startsWith abc => false
  ASSERT_THAT(struct_like->Reset(1), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(result, evaluator->Eval(*struct_like));
  EXPECT_FALSE(result);

  // Abc startsWith abc => false
  ASSERT_THAT(struct_like->Reset(2), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(result, evaluator->Eval(*struct_like));
  EXPECT_FALSE(result);

  // a startsWith abc => false
  ASSERT_THAT(struct_like->Reset(3), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(result, evaluator->Eval(*struct_like));
  EXPECT_FALSE(result);

  // abcd startsWith abc => true
  ASSERT_THAT(struct_like->Reset(4), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(result, evaluator->Eval(*struct_like));
  EXPECT_TRUE(result);

  // null startsWith abc => false
  ASSERT_THAT(struct_like->Reset(5), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(result, evaluator->Eval(*struct_like));
  EXPECT_FALSE(result);
}

TEST_F(EvaluatorTest, NotStartsWith) {
  auto string_schema = std::make_unique<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(24, "s", string())});
  ICEBERG_UNWRAP_OR_FAIL(
      auto evaluator,
      Evaluator::Make(*string_schema, Expressions::NotStartsWith("s", "abc")));

  auto arrow_string_type = ::arrow::struct_({::arrow::field("s", ::arrow::utf8())});
  auto arrow_string_array = ::arrow::json::ArrayFromJSONString(arrow_string_type, R"([
        {"s": "abc"},
        {"s": "xabc"},
        {"s": "Abc"},
        {"s": "a"},
        {"s": "abcde"},
        {"s": "Abcde"}
      ])")
                                .ValueOrDie();

  ArrowSchema c_schema;
  ArrowArray c_array;
  internal::ArrowSchemaGuard schema_guard(&c_schema);
  internal::ArrowArrayGuard array_guard(&c_array);
  ASSERT_TRUE(::arrow::ExportType(*arrow_string_type, &c_schema).ok());
  ASSERT_TRUE(::arrow::ExportArray(*arrow_string_array, &c_array).ok());

  ICEBERG_UNWRAP_OR_FAIL(auto struct_like,
                         ArrowArrayStructLike::Make(c_schema, c_array, /*row_index=*/0));

  // abc notStartsWith abc => false
  ASSERT_THAT(struct_like->Reset(0), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto result, evaluator->Eval(*struct_like));
  EXPECT_FALSE(result);

  // xabc notStartsWith abc => true
  ASSERT_THAT(struct_like->Reset(1), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(result, evaluator->Eval(*struct_like));
  EXPECT_TRUE(result);

  // Abc notStartsWith abc => true
  ASSERT_THAT(struct_like->Reset(2), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(result, evaluator->Eval(*struct_like));
  EXPECT_TRUE(result);

  // a notStartsWith abc => true
  ASSERT_THAT(struct_like->Reset(3), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(result, evaluator->Eval(*struct_like));
  EXPECT_TRUE(result);

  // abcde notStartsWith abc => false
  ASSERT_THAT(struct_like->Reset(4), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(result, evaluator->Eval(*struct_like));
  EXPECT_FALSE(result);

  // Abcde notStartsWith abc => true
  ASSERT_THAT(struct_like->Reset(5), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(result, evaluator->Eval(*struct_like));
  EXPECT_TRUE(result);
}

TEST_F(EvaluatorTest, AlwaysTrue) {
  ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                         Evaluator::Make(*schema_, Expressions::AlwaysTrue()));

  TestData(R"([{"x": 7, "y": 8.0, "z": null, "s1": null, "s5": null}])", *evaluator,
           true);
}

TEST_F(EvaluatorTest, AlwaysFalse) {
  ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                         Evaluator::Make(*schema_, Expressions::AlwaysFalse()));

  TestData(R"([{"x": 7, "y": 8.0, "z": null, "s1": null, "s5": null}])", *evaluator,
           false);
}

TEST_F(EvaluatorTest, IsNull) {
  ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                         Evaluator::Make(*schema_, Expressions::IsNull("z")));

  // null is null => true
  TestData(R"([{"x": 1, "y": 2.0, "z": null, "s1": null, "s5": null}])", *evaluator,
           true);

  // 3 is not null => false
  TestData(R"([{"x": 1, "y": 2.0, "z": 3, "s1": null, "s5": null}])", *evaluator, false);

  ICEBERG_UNWRAP_OR_FAIL(auto struct_evaluator,
                         Evaluator::Make(*schema_, Expressions::IsNull("s1.s2.s3.s4.i")));

  // 3 is not null => false
  TestData(
      R"([{"x": 1, "y": 2.0, "z": 3, "s1": {"s2": {"s3": {"s4": {"i": 3}}}}, "s5": null}])",
      *struct_evaluator, false);
}

TEST_F(EvaluatorTest, NotNull) {
  ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                         Evaluator::Make(*schema_, Expressions::NotNull("z")));

  // null is null => false
  TestData(R"([{"x": 1, "y": 2.0, "z": null, "s1": null, "s5": null}])", *evaluator,
           false);

  // 3 is not null => true
  TestData(R"([{"x": 1, "y": 2.0, "z": 3, "s1": null, "s5": null}])", *evaluator, true);

  ICEBERG_UNWRAP_OR_FAIL(
      auto struct_evaluator,
      Evaluator::Make(*schema_, Expressions::NotNull("s1.s2.s3.s4.i")));

  // 3 is not null => true
  TestData(
      R"([{"x": 1, "y": 2.0, "z": 3, "s1": {"s2": {"s3": {"s4": {"i": 3}}}}, "s5": null}])",
      *struct_evaluator, true);
}

TEST_F(EvaluatorTest, IsNaN) {
  auto double_schema = std::make_unique<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(25, "d", float64())});
  ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                         Evaluator::Make(*double_schema, Expressions::IsNaN("d")));

  auto arrow_double_type = ::arrow::struct_({::arrow::field("d", ::arrow::float64())});

  // Build array with NaN and regular values
  ::arrow::DoubleBuilder builder;
  ASSERT_TRUE(builder.Append(std::numeric_limits<double>::quiet_NaN()).ok());
  ASSERT_TRUE(builder.Append(2.0).ok());
  ASSERT_TRUE(builder.Append(std::numeric_limits<double>::infinity()).ok());
  auto double_array = builder.Finish().ValueOrDie();

  auto struct_array =
      ::arrow::StructArray::Make({double_array}, {arrow_double_type->field(0)})
          .ValueOrDie();

  ArrowSchema c_schema;
  ArrowArray c_array;
  internal::ArrowSchemaGuard schema_guard(&c_schema);
  internal::ArrowArrayGuard array_guard(&c_array);
  ASSERT_TRUE(::arrow::ExportType(*arrow_double_type, &c_schema).ok());
  ASSERT_TRUE(::arrow::ExportArray(*struct_array, &c_array).ok());

  ICEBERG_UNWRAP_OR_FAIL(auto struct_like,
                         ArrowArrayStructLike::Make(c_schema, c_array, /*row_index=*/0));

  // NaN is NaN => true
  ASSERT_THAT(struct_like->Reset(0), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto result, evaluator->Eval(*struct_like));
  EXPECT_TRUE(result);

  // 2.0 is not NaN => false
  ASSERT_THAT(struct_like->Reset(1), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(result, evaluator->Eval(*struct_like));
  EXPECT_FALSE(result);

  // Infinity is not NaN => false
  ASSERT_THAT(struct_like->Reset(2), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(result, evaluator->Eval(*struct_like));
  EXPECT_FALSE(result);
}

TEST_F(EvaluatorTest, NotNaN) {
  auto double_schema = std::make_unique<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(25, "d", float64())});
  ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                         Evaluator::Make(*double_schema, Expressions::NotNaN("d")));

  auto arrow_double_type = ::arrow::struct_({::arrow::field("d", ::arrow::float64())});

  // Build array with NaN and regular values
  ::arrow::DoubleBuilder builder;
  ASSERT_TRUE(builder.Append(std::numeric_limits<double>::quiet_NaN()).ok());
  ASSERT_TRUE(builder.Append(2.0).ok());
  ASSERT_TRUE(builder.Append(std::numeric_limits<double>::infinity()).ok());
  auto double_array = builder.Finish().ValueOrDie();

  auto struct_array =
      ::arrow::StructArray::Make({double_array}, {arrow_double_type->field(0)})
          .ValueOrDie();

  ArrowSchema c_schema;
  ArrowArray c_array;
  internal::ArrowSchemaGuard schema_guard(&c_schema);
  internal::ArrowArrayGuard array_guard(&c_array);
  ASSERT_TRUE(::arrow::ExportType(*arrow_double_type, &c_schema).ok());
  ASSERT_TRUE(::arrow::ExportArray(*struct_array, &c_array).ok());

  ICEBERG_UNWRAP_OR_FAIL(auto struct_like,
                         ArrowArrayStructLike::Make(c_schema, c_array, /*row_index=*/0));

  // NaN is NaN => false
  ASSERT_THAT(struct_like->Reset(0), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto result, evaluator->Eval(*struct_like));
  EXPECT_FALSE(result);

  // 2.0 is not NaN => true
  ASSERT_THAT(struct_like->Reset(1), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(result, evaluator->Eval(*struct_like));
  EXPECT_TRUE(result);

  // Infinity is not NaN => true
  ASSERT_THAT(struct_like->Reset(2), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(result, evaluator->Eval(*struct_like));
  EXPECT_TRUE(result);
}

TEST_F(EvaluatorTest, And) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto evaluator,
      Evaluator::Make(*schema_, Expressions::And(Expressions::Equal("x", Literal::Int(7)),
                                                 Expressions::NotNull("z"))));

  // 7, 3 => true
  TestData(R"([{"x": 7, "y": 0.0, "z": 3, "s1": null, "s5": null}])", *evaluator, true);

  // 8, 3 => false
  TestData(R"([{"x": 8, "y": 0.0, "z": 3, "s1": null, "s5": null}])", *evaluator, false);

  // 7, null => false
  TestData(R"([{"x": 7, "y": 0.0, "z": null, "s1": null, "s5": null}])", *evaluator,
           false);

  // 8, null => false
  TestData(R"([{"x": 8, "y": 0.0, "z": null, "s1": null, "s5": null}])", *evaluator,
           false);

  ICEBERG_UNWRAP_OR_FAIL(
      auto struct_evaluator,
      Evaluator::Make(
          *schema_, Expressions::And(Expressions::Equal("s1.s2.s3.s4.i", Literal::Int(7)),
                                     Expressions::NotNull("s1.s2.s3.s4.i"))));

  // 7, 7 => true
  TestData(
      R"([{"x": 7, "y": 0.0, "z": 3, "s1": {"s2": {"s3": {"s4": {"i": 7}}}}, "s5": null}])",
      *struct_evaluator, true);

  // 8, 8 => false
  TestData(
      R"([{"x": 8, "y": 0.0, "z": 3, "s1": {"s2": {"s3": {"s4": {"i": 8}}}}, "s5": null}])",
      *struct_evaluator, false);

  // 8, 8 => false (different x value)
  TestData(
      R"([{"x": 8, "y": 0.0, "z": null, "s1": {"s2": {"s3": {"s4": {"i": 8}}}}, "s5": null}])",
      *struct_evaluator, false);
}

TEST_F(EvaluatorTest, Or) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto evaluator,
      Evaluator::Make(*schema_, Expressions::Or(Expressions::Equal("x", Literal::Int(7)),
                                                Expressions::NotNull("z"))));

  // 7, 3 => true
  TestData(R"([{"x": 7, "y": 0.0, "z": 3, "s1": null, "s5": null}])", *evaluator, true);

  // 8, 3 => true
  TestData(R"([{"x": 8, "y": 0.0, "z": 3, "s1": null, "s5": null}])", *evaluator, true);

  // 7, null => true
  TestData(R"([{"x": 7, "y": 0.0, "z": null, "s1": null, "s5": null}])", *evaluator,
           true);

  // 8, null => false
  TestData(R"([{"x": 8, "y": 0.0, "z": null, "s1": null, "s5": null}])", *evaluator,
           false);

  ICEBERG_UNWRAP_OR_FAIL(
      auto struct_evaluator,
      Evaluator::Make(
          *schema_, Expressions::Or(Expressions::Equal("s1.s2.s3.s4.i", Literal::Int(7)),
                                    Expressions::NotNull("s1.s2.s3.s4.i"))));

  // 7, 7 => true
  TestData(
      R"([{"x": 7, "y": 0.0, "z": 3, "s1": {"s2": {"s3": {"s4": {"i": 7}}}}, "s5": null}])",
      *struct_evaluator, true);

  // 8, 8 => true
  TestData(
      R"([{"x": 8, "y": 0.0, "z": 3, "s1": {"s2": {"s3": {"s4": {"i": 8}}}}, "s5": null}])",
      *struct_evaluator, true);

  // 7, notnull => true
  TestData(
      R"([{"x": 7, "y": 0.0, "z": null, "s1": {"s2": {"s3": {"s4": {"i": 7}}}}, "s5": null}])",
      *struct_evaluator, true);
}

TEST_F(EvaluatorTest, Not) {
  ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                         Evaluator::Make(*schema_, Expressions::Not(Expressions::Equal(
                                                       "x", Literal::Int(7)))));

  // not(7 == 7) => false
  TestData(R"([{"x": 7, "y": 0.0, "z": null, "s1": null, "s5": null}])", *evaluator,
           false);

  // not(8 == 7) => true
  TestData(R"([{"x": 8, "y": 0.0, "z": null, "s1": null, "s5": null}])", *evaluator,
           true);

  ICEBERG_UNWRAP_OR_FAIL(
      auto struct_evaluator,
      Evaluator::Make(*schema_, Expressions::Not(Expressions::Equal("s1.s2.s3.s4.i",
                                                                    Literal::Int(7)))));

  // not(7 == 7) => false
  TestData(
      R"([{"x": 7, "y": null, "z": null, "s1": {"s2": {"s3": {"s4": {"i": 7}}}}, "s5": null}])",
      *struct_evaluator, false);

  // not(8 == 7) => true
  TestData(
      R"([{"x": 8, "y": null, "z": null, "s1": {"s2": {"s3": {"s4": {"i": 8}}}}, "s5": null}])",
      *struct_evaluator, true);
}

TEST_F(EvaluatorTest, CaseInsensitiveNot) {
  // Use case-insensitive binding (false)
  ICEBERG_UNWRAP_OR_FAIL(
      auto evaluator,
      Evaluator::Make(*schema_,
                      Expressions::Not(Expressions::Equal("X", Literal::Int(7))),
                      /*case_sensitive=*/false));

  // not(7 == 7) => false
  TestData(R"([{"x": 7, "y": 0.0, "z": null, "s1": null, "s5": null}])", *evaluator,
           false);

  // not(8 == 7) => true
  TestData(R"([{"x": 8, "y": 0.0, "z": null, "s1": null, "s5": null}])", *evaluator,
           true);

  // Test with nested struct
  ICEBERG_UNWRAP_OR_FAIL(auto struct_evaluator,
                         Evaluator::Make(*schema_,
                                         Expressions::Not(Expressions::Equal(
                                             "s1.s2.s3.s4.i", Literal::Int(7))),
                                         /*case_sensitive=*/false));

  // not(7 == 7) => false
  TestData(
      R"([{"x": 7, "y": null, "z": null, "s1": {"s2": {"s3": {"s4": {"i": 7}}}}, "s5": null}])",
      *struct_evaluator, false);

  // not(8 == 7) => true
  TestData(
      R"([{"x": 8, "y": null, "z": null, "s1": {"s2": {"s3": {"s4": {"i": 8}}}}, "s5": null}])",
      *struct_evaluator, true);
}

TEST_F(EvaluatorTest, CaseSensitiveNot) {
  // Should fail to bind with case-sensitive matching
  auto result = Evaluator::Make(
      *schema_, Expressions::Not(Expressions::Equal("X", Literal::Int(7))),
      /*case_sensitive=*/true);
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidExpression));
  EXPECT_THAT(result, HasErrorMessage("Cannot find field 'X'"));
}

TEST_F(EvaluatorTest, In) {
  ASSERT_EQ(Expressions::In("s", {Literal::Int(7), Literal::Int(8), Literal::Int(9)})
                ->literals()
                .size(),
            size_t{3});
  ASSERT_EQ(Expressions::In("s", {Literal::Int(7), Literal::Double(8.1),
                                  Literal::Long(std::numeric_limits<int64_t>::max())})
                ->literals()
                .size(),
            size_t{3});
  ASSERT_EQ(Expressions::In("s", {Literal::String("abc"), Literal::String("abd"),
                                  Literal::String("abc")})
                ->literals()
                .size(),
            size_t{3});
  ASSERT_EQ(Expressions::In("s", {Literal::Int(5)})->literals().size(), size_t{1});
  ASSERT_EQ(Expressions::In("s", {Literal::Int(5), Literal::Int(5)})->literals().size(),
            size_t{2});

  ICEBERG_UNWRAP_OR_FAIL(
      auto evaluator,
      Evaluator::Make(
          *schema_,
          Expressions::In("x", {Literal::Int(7), Literal::Int(8),
                                Literal::Long(std::numeric_limits<int64_t>::max())})));

  // 7 in [7, 8] => true
  TestData(R"([{"x": 7, "y": 8.0, "z": null, "s1": null, "s5": null}])", *evaluator,
           true);

  // 9 in [7, 8] => false
  TestData(R"([{"x": 9, "y": 8.0, "z": null, "s1": null, "s5": null}])", *evaluator,
           false);

  // Test with integer field
  ICEBERG_UNWRAP_OR_FAIL(
      auto integer_evaluator,
      Evaluator::Make(
          *schema_,
          Expressions::In("x", {Literal::Long(std::numeric_limits<int64_t>::max()),
                                Literal::Int(std::numeric_limits<int32_t>::max()),
                                Literal::Long(std::numeric_limits<int64_t>::min())})));

  // Integer.MAX_VALUE in [Integer.MAX_VALUE] => true
  TestData(R"([{"x": 2147483647, "y": 8.0, "z": null, "s1": null, "s5": null}])",
           *integer_evaluator, true);

  // 6 in [Integer.MAX_VALUE]  => false
  TestData(R"([{"x": 6, "y": 6.8, "z": null, "s1": null, "s5": null}])",
           *integer_evaluator, false);

  // Test with double field
  ICEBERG_UNWRAP_OR_FAIL(
      auto double_evaluator,
      Evaluator::Make(*schema_, Expressions::In("y", {Literal::Int(7), Literal::Int(8),
                                                      Literal::Double(9.1)})));

  // 7.0 in [7, 8, 9.1] => true
  TestData(R"([{"x": 0, "y": 7.0, "z": null, "s1": null, "s5": null}])",
           *double_evaluator, true);

  // 9.1 in [7, 8, 9.1] => true
  TestData(R"([{"x": 7, "y": 9.1, "z": null, "s1": null, "s5": null}])",
           *double_evaluator, true);

  // 6.8 in [7, 8, 9.1] => false
  TestData(R"([{"x": 6, "y": 6.8, "z": null, "s1": null, "s5": null}])",
           *double_evaluator, false);

  // Test with nested struct
  ICEBERG_UNWRAP_OR_FAIL(
      auto struct_evaluator,
      Evaluator::Make(*schema_,
                      Expressions::In("s1.s2.s3.s4.i", {Literal::Int(7), Literal::Int(8),
                                                        Literal::Int(9)})));

  // 7 in [7, 8, 9] => true
  TestData(
      R"([{"x": 7, "y": 8.0, "z": null, "s1": {"s2": {"s3": {"s4": {"i": 7}}}}, "s5": null}])",
      *struct_evaluator, true);

  // 6 in [7, 8, 9] => false
  TestData(
      R"([{"x": 6, "y": 8.0, "z": null, "s1": {"s2": {"s3": {"s4": {"i": 6}}}}, "s5": null}])",
      *struct_evaluator, false);
}

TEST_F(EvaluatorTest, NotIn) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto evaluator,
      Evaluator::Make(
          *schema_,
          Expressions::NotIn("x", {Literal::Int(7), Literal::Int(8),
                                   Literal::Long(std::numeric_limits<int64_t>::max())})));

  // 7 not in [7, 8] => false
  TestData(R"([{"x": 7, "y": 8.0, "z": null, "s1": null, "s5": null}])", *evaluator,
           false);

  // 9 not in [7, 8] => true
  TestData(R"([{"x": 9, "y": 8.0, "z": null, "s1": null, "s5": null}])", *evaluator,
           true);

  // Test with double field
  ICEBERG_UNWRAP_OR_FAIL(
      auto double_evaluator,
      Evaluator::Make(*schema_, Expressions::NotIn("y", {Literal::Int(7), Literal::Int(8),
                                                         Literal::Double(9.1)})));

  // 7.0 not in [7, 8, 9.1] => false
  TestData(R"([{"x": 0, "y": 7.0, "z": null, "s1": null, "s5": null}])",
           *double_evaluator, false);

  // 9.1 not in [7, 8, 9.1] => false
  TestData(R"([{"x": 7, "y": 9.1, "z": null, "s1": null, "s5": null}])",
           *double_evaluator, false);

  // 6.8 not in [7, 8, 9.1] => true
  TestData(R"([{"x": 6, "y": 6.8, "z": null, "s1": null, "s5": null}])",
           *double_evaluator, true);

  // Test with nested struct
  ICEBERG_UNWRAP_OR_FAIL(
      auto struct_evaluator,
      Evaluator::Make(
          *schema_, Expressions::NotIn("s1.s2.s3.s4.i", {Literal::Int(7), Literal::Int(8),
                                                         Literal::Int(9)})));

  // 7 not in [7, 8, 9] => false
  TestData(
      R"([{"x": 7, "y": 8.0, "z": null, "s1": {"s2": {"s3": {"s4": {"i": 7}}}}, "s5": null}])",
      *struct_evaluator, false);

  // 6 not in [7, 8, 9] => true
  TestData(
      R"([{"x": 6, "y": 8.0, "z": null, "s1": {"s2": {"s3": {"s4": {"i": 6}}}}, "s5": null}])",
      *struct_evaluator, true);
}

TEST_F(EvaluatorTest, InExceptions) {
  {
    auto result = Evaluator::Make(
        *schema_,
        Expressions::In("x", {Literal::Int(7), Literal::Int(8), Literal::Null(int32())}),
        /*case_sensitive=*/false);
    EXPECT_THAT(result, IsError(ErrorKind::kInvalidExpression));
    EXPECT_THAT(result,
                HasErrorMessage("Invalid value for conversion to type int: null (int)"));
  }

  {
    auto result = Evaluator::Make(
        *schema_,
        Expressions::In("x", {Literal::Int(7), Literal::Int(8), Literal::Double(9.1)}),
        /*case_sensitive=*/false);
    EXPECT_THAT(result, IsError(ErrorKind::kNotSupported));
    EXPECT_THAT(result, HasErrorMessage("Cast from Double to int is not supported"));
  }

  {
    auto result = UnboundPredicateImpl<BoundReference>::Make(Expression::Operation::kIn,
                                                             Expressions::Ref("x"), {});
    EXPECT_THAT(result, IsError(ErrorKind::kInvalidExpression));
    EXPECT_THAT(result, HasErrorMessage("Cannot create IN predicate without a value"));
  }
}

}  // namespace iceberg
