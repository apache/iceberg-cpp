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

#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <format>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <arrow/array.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/c/bridge.h>
#include <arrow/json/from_string.h>
#include <arrow/testing/gtest_util.h>
#include <arrow/type.h>
#include <gtest/gtest.h>

#include "iceberg/arrow_c_data_guard_internal.h"
#include "iceberg/expression/binder.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/expression/literal.h"
#include "iceberg/expression/predicate.h"
#include "iceberg/expression/rewrite_not.h"
#include "iceberg/expression/term.h"
#include "iceberg/result.h"
#include "iceberg/row/arrow_array_wrapper.h"
#include "iceberg/row/struct_like.h"
#include "iceberg/schema.h"
#include "iceberg/test/matchers.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"
#include "iceberg/util/uuid.h"

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
    ICEBERG_UNWRAP_OR_FAIL(auto result, evaluator.Evaluate(*struct_like));
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

TEST_F(EvaluatorTest, OutOfRangeComparisonsPreserveNulls) {
  using Op = Expression::Operation;
  const int64_t above = int64_t{std::numeric_limits<int32_t>::max()} + 1;
  const int64_t below = int64_t{std::numeric_limits<int32_t>::min()} - 1;
  const std::string present =
      R"([{"x": 1, "y": 0.0, "z": 1, "s1": {"s2": {"s3": {"s4": {"i": 1}}}}}])";
  const std::string nulls = R"([{"x": 1, "y": 0.0, "z": null, "s1": null}])";
  for (auto op : {Op::kLt, Op::kLtEq, Op::kGt, Op::kGtEq, Op::kNotEq}) {
    for (const std::string name : {"x", "z", "s1.s2.s3.s4.i"}) {
      const int64_t bound = op == Op::kGt || op == Op::kGtEq ? below : above;
      auto predicate = Expressions::Predicate(op, name, Literal::Long(bound));
      SCOPED_TRACE(predicate->ToString());
      ICEBERG_UNWRAP_OR_FAIL(auto evaluator, Evaluator::Make(*schema_, predicate));
      TestData(present, *evaluator, true);
      // A required leaf under an optional parent can still be null. NotEq keeps
      // its existing semantics: null is unequal to the out-of-range literal.
      TestData(nulls, *evaluator, name == "x" || op == Op::kNotEq);
    }
  }
}

TEST_F(EvaluatorTest, NullPredicatesOnStructsRespectParentValidity) {
  for (const std::string name : {"s1", "s1.s2", "s1.s2.s3.s4"}) {
    SCOPED_TRACE(name);
    ICEBERG_UNWRAP_OR_FAIL(auto is_null,
                           Evaluator::Make(*schema_, Expressions::IsNull(name)));
    ICEBERG_UNWRAP_OR_FAIL(auto not_null,
                           Evaluator::Make(*schema_, Expressions::NotNull(name)));
    const std::string absent = R"([{"x": 1, "y": 0.0, "s1": null}])";
    const std::string present =
        R"([{"x": 1, "y": 0.0, "s1": {"s2": {"s3": {"s4": {"i": 1}}}}}])";
    TestData(absent, *is_null, true);
    TestData(absent, *not_null, false);
    TestData(present, *is_null, false);
    TestData(present, *not_null, true);
  }
}

TEST_F(EvaluatorTest, NullPredicatesEvaluateTransformResults) {
  auto term = Expressions::Transform("x", Transform::Void());
  ICEBERG_UNWRAP_OR_FAIL(
      auto is_null, Evaluator::Make(*schema_, Expressions::IsNull<BoundTransform>(term)));
  ICEBERG_UNWRAP_OR_FAIL(
      auto not_null,
      Evaluator::Make(*schema_, Expressions::NotNull<BoundTransform>(term)));
  // The source is non-null but the transform result is null.
  TestData(R"([{"x": 1, "y": 0.0}])", *is_null, true);
  TestData(R"([{"x": 1, "y": 0.0}])", *not_null, false);
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
      std::vector<SchemaField>{SchemaField::MakeOptional(24, "s", string())});
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
  ICEBERG_UNWRAP_OR_FAIL(auto result, evaluator->Evaluate(*struct_like));
  EXPECT_TRUE(result);

  // xabc startsWith abc => false
  ASSERT_THAT(struct_like->Reset(1), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(result, evaluator->Evaluate(*struct_like));
  EXPECT_FALSE(result);

  // Abc startsWith abc => false
  ASSERT_THAT(struct_like->Reset(2), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(result, evaluator->Evaluate(*struct_like));
  EXPECT_FALSE(result);

  // a startsWith abc => false
  ASSERT_THAT(struct_like->Reset(3), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(result, evaluator->Evaluate(*struct_like));
  EXPECT_FALSE(result);

  // abcd startsWith abc => true
  ASSERT_THAT(struct_like->Reset(4), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(result, evaluator->Evaluate(*struct_like));
  EXPECT_TRUE(result);

  // null startsWith abc => false
  ASSERT_THAT(struct_like->Reset(5), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(result, evaluator->Evaluate(*struct_like));
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
  ICEBERG_UNWRAP_OR_FAIL(auto result, evaluator->Evaluate(*struct_like));
  EXPECT_FALSE(result);

  // xabc notStartsWith abc => true
  ASSERT_THAT(struct_like->Reset(1), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(result, evaluator->Evaluate(*struct_like));
  EXPECT_TRUE(result);

  // Abc notStartsWith abc => true
  ASSERT_THAT(struct_like->Reset(2), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(result, evaluator->Evaluate(*struct_like));
  EXPECT_TRUE(result);

  // a notStartsWith abc => true
  ASSERT_THAT(struct_like->Reset(3), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(result, evaluator->Evaluate(*struct_like));
  EXPECT_TRUE(result);

  // abcde notStartsWith abc => false
  ASSERT_THAT(struct_like->Reset(4), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(result, evaluator->Evaluate(*struct_like));
  EXPECT_FALSE(result);

  // Abcde notStartsWith abc => true
  ASSERT_THAT(struct_like->Reset(5), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(result, evaluator->Evaluate(*struct_like));
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
  ICEBERG_UNWRAP_OR_FAIL(auto result, evaluator->Evaluate(*struct_like));
  EXPECT_TRUE(result);

  // 2.0 is not NaN => false
  ASSERT_THAT(struct_like->Reset(1), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(result, evaluator->Evaluate(*struct_like));
  EXPECT_FALSE(result);

  // Infinity is not NaN => false
  ASSERT_THAT(struct_like->Reset(2), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(result, evaluator->Evaluate(*struct_like));
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
  ICEBERG_UNWRAP_OR_FAIL(auto result, evaluator->Evaluate(*struct_like));
  EXPECT_FALSE(result);

  // 2.0 is not NaN => true
  ASSERT_THAT(struct_like->Reset(1), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(result, evaluator->Evaluate(*struct_like));
  EXPECT_TRUE(result);

  // Infinity is not NaN => true
  ASSERT_THAT(struct_like->Reset(2), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(result, evaluator->Evaluate(*struct_like));
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

TEST_F(EvaluatorTest, NotRangeComparisonsIncludeNull) {
  struct Case {
    std::shared_ptr<Expression> predicate;
    bool below;
    bool equal;
    bool above;
  };
  const std::vector<Case> cases = {
      {.predicate = Expressions::LessThan("z", Literal::Int(7)),
       .below = false,
       .equal = true,
       .above = true},
      {.predicate = Expressions::LessThanOrEqual("z", Literal::Int(7)),
       .below = false,
       .equal = false,
       .above = true},
      {.predicate = Expressions::GreaterThan("z", Literal::Int(7)),
       .below = true,
       .equal = true,
       .above = false},
      {.predicate = Expressions::GreaterThanOrEqual("z", Literal::Int(7)),
       .below = true,
       .equal = false,
       .above = false}};
  for (const auto& test : cases) {
    SCOPED_TRACE(test.predicate->ToString());
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           Evaluator::Make(*schema_, Expressions::Not(test.predicate)));
    TestData(R"([{"x": 0, "y": 0.0, "z": null}])", *evaluator, true);
    TestData(R"([{"x": 0, "y": 0.0, "z": 6}])", *evaluator, test.below);
    TestData(R"([{"x": 0, "y": 0.0, "z": 7}])", *evaluator, test.equal);
    TestData(R"([{"x": 0, "y": 0.0, "z": 8}])", *evaluator, test.above);
  }
}

TEST_F(EvaluatorTest, NegatedRangePreservesNullableAncestors) {
  for (const auto& constant :
       {Literal::Int(7), Literal::Long(std::numeric_limits<int64_t>::max())}) {
    auto expr = Expressions::Not(Expressions::LessThan("s1.s2.s3.s4.i", constant));
    ICEBERG_UNWRAP_OR_FAIL(auto original, Evaluator::Make(*schema_, expr));
    ICEBERG_UNWRAP_OR_FAIL(auto rewritten, RewriteNot::Visit(expr));
    ICEBERG_UNWRAP_OR_FAIL(auto normalized, Evaluator::Make(*schema_, rewritten));
    for (auto* evaluator : {original.get(), normalized.get()}) {
      TestData(R"([{"x": 0, "y": 0.0, "s1": null}])", *evaluator, true);
      TestData(R"([{"x": 0, "y": 0.0, "s1": {"s2": {"s3": {"s4": {"i": 6}}}}}])",
               *evaluator, false);
      TestData(R"([{"x": 0, "y": 0.0, "s1": {"s2": {"s3": {"s4": {"i": 7}}}}}])",
               *evaluator, constant == Literal::Int(7));
    }
  }
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

namespace {

// A row of raw scalars, one per top-level field.
class ScalarRow : public StructLike {
 public:
  explicit ScalarRow(std::vector<Scalar> values) : values_(std::move(values)) {}
  Result<Scalar> GetField(size_t pos) const override { return values_.at(pos); }
  size_t num_fields() const override { return values_.size(); }

 private:
  std::vector<Scalar> values_;
};

// One column under test: its type, row values, and literals to compare with.
struct ColumnCase {
  std::shared_ptr<PrimitiveType> type;
  std::vector<Scalar> values;
  std::vector<Literal> literals;
};

std::string Bytes(std::initializer_list<int> bytes) {
  std::string result;
  for (int byte : bytes) {
    result.push_back(static_cast<char>(byte));
  }
  return result;
}

std::vector<uint8_t> ByteVector(std::string_view bytes) {
  return {bytes.begin(), bytes.end()};
}

}  // namespace

// The evaluator compares raw scalars for reference predicates. It must match the
// literal-based semantics of BoundPredicate::Test for every type, operation, and
// edge value, including nulls, NaNs, signed zeros, and unsigned byte order.
TEST(EvaluatorRawComparisonTest, MatchesLiteralSemantics) {
  const float nan_f = std::numeric_limits<float>::quiet_NaN();
  const double nan_d = std::numeric_limits<double>::quiet_NaN();
  const double inf = std::numeric_limits<double>::infinity();
  const std::string high = Bytes({0xff, 0x01});
  const std::string low = Bytes({0x01, 0xff});
  static const std::array<std::string, 7> kStrings = {"",  "a",  "ab", "abc",
                                                      "b", high, low};
  auto uuid_a = Uuid::FromString("123e4567-e89b-12d3-a456-426614174000").value();
  auto uuid_b = Uuid::FromString("f23e4567-e89b-12d3-a456-426614174000").value();
  static std::string uuid_a_bytes;
  static std::string uuid_b_bytes;
  uuid_a_bytes.assign(reinterpret_cast<const char*>(uuid_a.bytes().data()), 16);
  uuid_b_bytes.assign(reinterpret_cast<const char*>(uuid_b.bytes().data()), 16);

  std::vector<ColumnCase> cases;
  cases.push_back({.type = boolean(),
                   .values = {std::monostate{}, false, true},
                   .literals = {Literal::Boolean(false), Literal::Boolean(true)}});
  cases.push_back({.type = int32(),
                   .values = {std::monostate{}, int32_t{-5}, int32_t{0}, int32_t{7},
                              std::numeric_limits<int32_t>::max()},
                   .literals = {Literal::Int(0), Literal::Int(7), Literal::Int(-100)}});
  cases.push_back({.type = date(),
                   .values = {std::monostate{}, int32_t{18000}, int32_t{19000}},
                   .literals = {Literal::Date(18500), Literal::Date(18000)}});
  cases.push_back(
      {.type = int64(),
       .values = {std::monostate{}, int64_t{-1}, int64_t{0}, int64_t{1} << 40},
       .literals = {Literal::Long(0), Literal::Long(int64_t{1} << 40)}});
  cases.push_back({.type = timestamp(),
                   .values = {std::monostate{}, int64_t{1000}, int64_t{2000}},
                   .literals = {Literal::Timestamp(1500), Literal::Timestamp(2000)}});
  cases.push_back({.type = timestamp_tz(),
                   .values = {std::monostate{}, int64_t{1000}},
                   .literals = {Literal::TimestampTz(1000)}});
  cases.push_back({.type = time(),
                   .values = {std::monostate{}, int64_t{5}},
                   .literals = {Literal::Time(6)}});
  cases.push_back(
      {.type = float32(),
       .values = {std::monostate{}, -0.0f, 0.0f, 1.5f, nan_f, -nan_f,
                  std::numeric_limits<float>::infinity()},
       .literals = {Literal::Float(0.0f), Literal::Float(-0.0f), Literal::Float(1.5f)}});
  cases.push_back({.type = float64(),
                   .values = {std::monostate{}, -0.0, 0.0, -inf, inf, nan_d, 2.25},
                   .literals = {Literal::Double(0.0), Literal::Double(-0.0),
                                Literal::Double(2.25), Literal::Double(inf)}});
  cases.push_back({.type = decimal(9, 2),
                   .values = {std::monostate{}, Decimal(int128_t{-150}),
                              Decimal(int128_t{0}), Decimal(int128_t{12345})},
                   .literals = {Literal::Decimal(0, 9, 2), Literal::Decimal(12345, 9, 2),
                                Literal::Decimal(-151, 9, 2)}});
  std::vector<Scalar> strings{std::monostate{}};
  for (const auto& value : kStrings) {
    strings.emplace_back(std::string_view(value));
  }
  cases.push_back(
      {.type = string(),
       .values = strings,
       .literals = {Literal::String(""), Literal::String("a"), Literal::String("ab"),
                    Literal::String(high), Literal::String("b")}});
  cases.push_back({.type = binary(),
                   .values = strings,
                   .literals = {Literal::Binary({}), Literal::Binary(ByteVector("ab")),
                                Literal::Binary(ByteVector(high)),
                                Literal::Binary(ByteVector(low))}});
  static const std::array<std::string, 3> kFixed = {"abc", "abd",
                                                    Bytes({0xff, 0x00, 0x00})};
  std::vector<Scalar> fixed_values{std::monostate{}};
  for (const auto& value : kFixed) {
    fixed_values.emplace_back(std::string_view(value));
  }
  cases.push_back({.type = fixed(3),
                   .values = fixed_values,
                   .literals = {Literal::Fixed(ByteVector("abd")),
                                Literal::Fixed(ByteVector(Bytes({0xff, 0x00, 0x00})))}});
  cases.push_back({.type = uuid(),
                   .values = {std::monostate{}, std::string_view(uuid_a_bytes),
                              std::string_view(uuid_b_bytes)},
                   .literals = {Literal::UUID(uuid_a), Literal::UUID(uuid_b)}});

  using Op = Expression::Operation;
  const std::array literal_ops = {Op::kLt,         Op::kLtEq,         Op::kGt,
                                  Op::kGtEq,       Op::kEq,           Op::kNotEq,
                                  Op::kStartsWith, Op::kNotStartsWith};
  const std::array unary_ops = {Op::kIsNull, Op::kNotNull, Op::kIsNan, Op::kNotNan};

  int checked = 0;
  for (const auto& column : cases) {
    Schema schema({SchemaField::MakeOptional(1, "c", column.type)});
    std::vector<std::shared_ptr<Expression>> predicates;
    for (Op op : unary_ops) {
      predicates.push_back(Expressions::Predicate(op, "c"));
    }
    for (const auto& literal : column.literals) {
      for (Op op : literal_ops) {
        predicates.push_back(Expressions::Predicate(op, "c", literal));
      }
    }
    for (const auto& unbound : predicates) {
      auto bound = Binder::Bind(schema, unbound, /*case_sensitive=*/true);
      auto evaluator = Evaluator::Make(schema, unbound);
      if (!bound.has_value()) {
        // Invalid for this type, e.g. starts_with on an int.
        EXPECT_FALSE(evaluator.has_value()) << unbound->ToString();
        continue;
      }
      ASSERT_TRUE(evaluator.has_value()) << evaluator.error().message;
      for (const auto& value : column.values) {
        ScalarRow row({value});
        SCOPED_TRACE(
            std::format("{} on {}", unbound->ToString(), column.type->ToString()));
        Result<bool> expected = false;
        if ((*bound)->op() == Op::kTrue || (*bound)->op() == Op::kFalse) {
          expected = (*bound)->op() == Op::kTrue;
        } else {
          const auto& predicate = std::dynamic_pointer_cast<BoundPredicate>(*bound);
          ASSERT_NE(predicate, nullptr) << (*bound)->ToString();
          auto literal = predicate->term()->Evaluate(row);
          ASSERT_TRUE(literal.has_value()) << literal.error().message;
          expected = predicate->Test(*literal);
        }
        auto actual = (*evaluator)->Evaluate(row);
        ASSERT_EQ(expected.has_value(), actual.has_value());
        if (expected.has_value()) {
          EXPECT_EQ(*expected, *actual)
              << "row value index in " << column.type->ToString();
        }
        ++checked;
      }
    }
  }
  EXPECT_GT(checked, 1000);
}

TEST(EvaluatorRawComparisonTest, RejectsUnaryOperationOnLiteralPredicate) {
  Schema schema({SchemaField::MakeOptional(1, "id", int32())});
  auto expr = Expressions::Predicate(Expression::Operation::kIsNull, "id",
                                     std::vector<Literal>{Literal::Int(1)});
  ICEBERG_UNWRAP_OR_FAIL(auto evaluator, Evaluator::Make(schema, expr));
  EXPECT_THAT(evaluator->Evaluate(ScalarRow({int32_t{1}})),
              IsError(ErrorKind::kInvalidExpression));
}

TEST(EvaluatorRawComparisonTest, NanPredicatesRejectMismatchedFloatTypes) {
  Schema schema({SchemaField::MakeOptional(1, "f", float32()),
                 SchemaField::MakeOptional(2, "d", float64())});
  ScalarRow row({std::numeric_limits<double>::quiet_NaN(),
                 std::numeric_limits<float>::quiet_NaN()});
  for (auto op : {Expression::Operation::kIsNan, Expression::Operation::kNotNan}) {
    for (const auto* field : {"f", "d"}) {
      auto expr = Expressions::Predicate(op, field);
      SCOPED_TRACE(expr->ToString());
      ICEBERG_UNWRAP_OR_FAIL(auto evaluator, Evaluator::Make(schema, expr));
      EXPECT_THROW(static_cast<void>(evaluator->Evaluate(row)), std::bad_variant_access);
    }
  }
}

TEST(EvaluatorRawComparisonTest, CompoundExpressionsKeepErrorsFromBothSides) {
  Schema schema({SchemaField::MakeRequired(1, "id", int32()),
                 SchemaField::MakeOptional(2, "name", string())});
  // A required field that is null is an error. The left side is false, but the
  // right side is still evaluated, as before.
  auto expr = Expressions::And(Expressions::Equal("name", Literal::String("x")),
                               Expressions::GreaterThan("id", Literal::Int(0)));
  ICEBERG_UNWRAP_OR_FAIL(auto evaluator, Evaluator::Make(schema, expr));
  ScalarRow bad_row({std::monostate{}, std::string_view("y")});
  EXPECT_FALSE(evaluator->Evaluate(bad_row).has_value());
  ScalarRow good_row({int32_t{1}, std::string_view("x")});
  EXPECT_THAT(evaluator->Evaluate(good_row), HasValue(::testing::Eq(true)));
}

TEST(EvaluatorRawComparisonTest, TransformsAndSetsUseLiteralEvaluation) {
  Schema schema({SchemaField::MakeOptional(1, "id", int32())});
  auto in = Expressions::In("id", {Literal::Int(1), Literal::Int(3)});
  ICEBERG_UNWRAP_OR_FAIL(auto in_evaluator, Evaluator::Make(schema, in));
  EXPECT_THAT(in_evaluator->Evaluate(ScalarRow({int32_t{3}})),
              HasValue(::testing::Eq(true)));
  EXPECT_THAT(in_evaluator->Evaluate(ScalarRow({int32_t{2}})),
              HasValue(::testing::Eq(false)));
  EXPECT_THAT(in_evaluator->Evaluate(ScalarRow({std::monostate{}})),
              HasValue(::testing::Eq(false)));

  auto bucket = Expressions::Equal(
      std::shared_ptr<UnboundTerm<BoundTransform>>(Expressions::Bucket("id", 4)),
      Literal::Int(3));
  ICEBERG_UNWRAP_OR_FAIL(auto bucket_evaluator, Evaluator::Make(schema, bucket));
  // bucket[4](34) is 3 and bucket[4](0) is 0.
  EXPECT_THAT(bucket_evaluator->Evaluate(ScalarRow({int32_t{34}})),
              HasValue(::testing::Eq(true)));
  EXPECT_THAT(bucket_evaluator->Evaluate(ScalarRow({int32_t{0}})),
              HasValue(::testing::Eq(false)));
}

}  // namespace iceberg
