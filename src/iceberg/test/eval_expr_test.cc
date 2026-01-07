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

#include <arrow/c/bridge.h>
#include <arrow/json/from_string.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <gtest/gtest.h>

#include "iceberg/arrow_c_data.h"
#include "iceberg/arrow_c_data_guard_internal.h"
#include "iceberg/expression/expression.h"
#include "iceberg/expression/literal.h"
#include "iceberg/expression/term.h"
#include "iceberg/row/arrow_array_wrapper.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
#include "iceberg/test/matchers.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"

namespace iceberg {

class BoundExpressionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    schema_ = std::make_unique<Schema>(std::vector<SchemaField>{
        SchemaField::MakeOptional(1, "id", int32()),
        SchemaField::MakeOptional(2, "name", string()),
        SchemaField::MakeRequired(3, "timestamp_field", timestamp()),
        SchemaField::MakeRequired(4, "string_field", string())});

    arrow_data_type_ = ::arrow::struct_(
        {::arrow::field("id", ::arrow::int32()), ::arrow::field("name", ::arrow::utf8()),
         ::arrow::field("timestamp_field", ::arrow::timestamp(::arrow::TimeUnit::MICRO)),
         ::arrow::field("string_field", ::arrow::utf8())});

    arrow_array_ = ::arrow::json::ArrayFromJSONString(arrow_data_type_, R"([
          {"id": 1, "name": "Alice", "timestamp_field": 1609459200000000, "string_field": "hello_world"},
          {"id": 2, "name": null, "timestamp_field": 1609459200000000, "string_field": "hello_world"}
        ])")
                       .ValueOrDie();

    ASSERT_TRUE(::arrow::ExportType(*arrow_data_type_, &arrow_c_schema_).ok());
    ASSERT_TRUE(::arrow::ExportArray(*arrow_array_, &arrow_c_array_).ok());
  }

  void TearDown() override {
    if (arrow_c_schema_.release != nullptr) {
      ArrowSchemaRelease(&arrow_c_schema_);
    }
    if (arrow_c_array_.release != nullptr) {
      ArrowArrayRelease(&arrow_c_array_);
    }
  }

  std::unique_ptr<Schema> schema_;
  std::shared_ptr<::arrow::DataType> arrow_data_type_;
  std::shared_ptr<::arrow::Array> arrow_array_;
  ArrowSchema arrow_c_schema_;
  ArrowArray arrow_c_array_;
};

TEST_F(BoundExpressionTest, EvaluateBoundReference) {
  ICEBERG_UNWRAP_OR_FAIL(auto id_ref, NamedReference::Make("id"));
  ICEBERG_UNWRAP_OR_FAIL(auto id_bound_ref,
                         id_ref->Bind(*schema_, /*case_sensitive=*/true));

  ICEBERG_UNWRAP_OR_FAIL(auto name_ref, NamedReference::Make("name"));
  ICEBERG_UNWRAP_OR_FAIL(auto name_bound_ref,
                         name_ref->Bind(*schema_, /*case_sensitive=*/true));

  struct TestCase {
    size_t row_index;
    Literal expected_id;
    Literal expected_name;
  };

  for (const auto& test_case : std::vector<TestCase>{
           {.row_index = 0,
            .expected_id = Literal::Int(1),
            .expected_name = Literal::String("Alice")},
           {.row_index = 1,
            .expected_id = Literal::Int(2),
            .expected_name = Literal::Null(string())},
       }) {
    ICEBERG_UNWRAP_OR_FAIL(
        auto struct_like,
        ArrowArrayStructLike::Make(arrow_c_schema_, arrow_c_array_, test_case.row_index));

    ICEBERG_UNWRAP_OR_FAIL(auto id_literal, id_bound_ref->Evaluate(*struct_like));
    EXPECT_EQ(id_literal, test_case.expected_id);

    ICEBERG_UNWRAP_OR_FAIL(auto name_literal, name_bound_ref->Evaluate(*struct_like));
    if (test_case.expected_name.IsNull()) {
      EXPECT_TRUE(name_literal.IsNull());
    } else {
      EXPECT_EQ(name_literal, test_case.expected_name);
    }
  }
}

TEST_F(BoundExpressionTest, IdentityTransform) {
  ICEBERG_UNWRAP_OR_FAIL(auto name_ref, NamedReference::Make("name"));
  ICEBERG_UNWRAP_OR_FAIL(
      auto name_transform,
      UnboundTransform::Make(std::move(name_ref), Transform::Identity()));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_transform,
                         name_transform->Bind(*schema_, /*case_sensitive=*/true));

  struct TestCase {
    size_t row_index;
    Literal expected_name;
  };

  for (const auto& test_case : std::vector<TestCase>{
           {.row_index = 0, .expected_name = Literal::String("Alice")},
           {.row_index = 1, .expected_name = Literal::Null(string())},
       }) {
    ICEBERG_UNWRAP_OR_FAIL(
        auto struct_like,
        ArrowArrayStructLike::Make(arrow_c_schema_, arrow_c_array_, test_case.row_index));
    ICEBERG_UNWRAP_OR_FAIL(auto result, bound_transform->Evaluate(*struct_like));
    if (test_case.expected_name.IsNull()) {
      EXPECT_TRUE(result.IsNull());
    } else {
      EXPECT_EQ(result, test_case.expected_name);
    }
  }
}

TEST_F(BoundExpressionTest, YearTransform) {
  // Create and bind year transform
  ICEBERG_UNWRAP_OR_FAIL(auto timestamp_ref, NamedReference::Make("timestamp_field"));
  ICEBERG_UNWRAP_OR_FAIL(
      auto unbound_transform,
      UnboundTransform::Make(std::move(timestamp_ref), Transform::Year()));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_transform,
                         unbound_transform->Bind(*schema_, /*case_sensitive=*/true));

  // Test data: 2021-01-01 00:00:00 UTC = 1609459200000000 microseconds
  ICEBERG_UNWRAP_OR_FAIL(auto struct_like,
                         ArrowArrayStructLike::Make(arrow_c_schema_, arrow_c_array_, 0));

  // Evaluate (2021)
  ICEBERG_UNWRAP_OR_FAIL(auto result, bound_transform->Evaluate(*struct_like));
  EXPECT_FALSE(result.IsNull());
  EXPECT_EQ(std::get<int32_t>(result.value()), 2021 - 1970);  // Year value
}

TEST_F(BoundExpressionTest, MonthTransform) {
  // Create and bind month transform
  ICEBERG_UNWRAP_OR_FAIL(auto timestamp_ref, NamedReference::Make("timestamp_field"));
  ICEBERG_UNWRAP_OR_FAIL(
      auto unbound_transform,
      UnboundTransform::Make(std::move(timestamp_ref), Transform::Month()));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_transform,
                         unbound_transform->Bind(*schema_, /*case_sensitive=*/true));

  // Test data: 2021-01-01
  ICEBERG_UNWRAP_OR_FAIL(auto struct_like,
                         ArrowArrayStructLike::Make(arrow_c_schema_, arrow_c_array_, 0));

  // Evaluate (2021-01)
  ICEBERG_UNWRAP_OR_FAIL(auto result, bound_transform->Evaluate(*struct_like));
  EXPECT_FALSE(result.IsNull());
  EXPECT_EQ(std::get<int32_t>(result.value()), 612);  // Months since 1970-01
}

TEST_F(BoundExpressionTest, DayTransform) {
  // Create and bind day transform
  ICEBERG_UNWRAP_OR_FAIL(auto timestamp_ref, NamedReference::Make("timestamp_field"));
  ICEBERG_UNWRAP_OR_FAIL(
      auto unbound_transform,
      UnboundTransform::Make(std::move(timestamp_ref), Transform::Day()));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_transform,
                         unbound_transform->Bind(*schema_, /*case_sensitive=*/true));

  // Test data: 2021-01-01
  ICEBERG_UNWRAP_OR_FAIL(auto struct_like,
                         ArrowArrayStructLike::Make(arrow_c_schema_, arrow_c_array_, 0));

  // Evaluate
  ICEBERG_UNWRAP_OR_FAIL(auto result, bound_transform->Evaluate(*struct_like));
  EXPECT_FALSE(result.IsNull());
  EXPECT_EQ(std::get<int32_t>(result.value()), 18628);  // Days since 1970-01-01
}

TEST_F(BoundExpressionTest, BucketTransform) {
  // Create and bind bucket[4] transform
  ICEBERG_UNWRAP_OR_FAIL(auto string_ref, NamedReference::Make("string_field"));
  ICEBERG_UNWRAP_OR_FAIL(
      auto unbound_transform,
      UnboundTransform::Make(std::move(string_ref), Transform::Bucket(4)));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_transform,
                         unbound_transform->Bind(*schema_, /*case_sensitive=*/true));

  // Test data: "hello_world"
  ICEBERG_UNWRAP_OR_FAIL(auto struct_like,
                         ArrowArrayStructLike::Make(arrow_c_schema_, arrow_c_array_, 0));

  // Evaluate - verify result is in range [0, 3]
  ICEBERG_UNWRAP_OR_FAIL(auto result, bound_transform->Evaluate(*struct_like));
  EXPECT_FALSE(result.IsNull());
  auto bucket_value = std::get<int32_t>(result.value());
  EXPECT_GE(bucket_value, 0);
  EXPECT_LT(bucket_value, 4);
}

TEST_F(BoundExpressionTest, TruncateTransform) {
  // Create and bind truncate[5] transform
  ICEBERG_UNWRAP_OR_FAIL(auto string_ref, NamedReference::Make("string_field"));
  ICEBERG_UNWRAP_OR_FAIL(
      auto unbound_transform,
      UnboundTransform::Make(std::move(string_ref), Transform::Truncate(5)));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_transform,
                         unbound_transform->Bind(*schema_, /*case_sensitive=*/true));

  // Test data: "hello_world"
  ICEBERG_UNWRAP_OR_FAIL(auto struct_like,
                         ArrowArrayStructLike::Make(arrow_c_schema_, arrow_c_array_, 0));

  // Evaluate - "hello_world" truncated to 5 chars = "hello"
  ICEBERG_UNWRAP_OR_FAIL(auto result, bound_transform->Evaluate(*struct_like));
  EXPECT_FALSE(result.IsNull());
  EXPECT_EQ(std::get<std::string>(result.value()), "hello");
}

}  // namespace iceberg
