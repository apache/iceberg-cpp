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

#include <memory>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/expression/expression.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/expression/json_serde_internal.h"
#include "iceberg/expression/literal.h"
#include "iceberg/expression/predicate.h"
#include "iceberg/expression/term.h"
#include "iceberg/test/matchers.h"
#include "iceberg/transform.h"

namespace iceberg {

// Test boolean constant expressions
TEST(ExpressionJsonTest, CheckBooleanExpression) {
  auto checkBoolean = [](std::shared_ptr<Expression> expr, bool value) {
    auto json = ToJson(*expr);
    EXPECT_TRUE(json.is_boolean());
    EXPECT_EQ(json.get<bool>(), value);

    auto result = ExpressionFromJson(json);
    ASSERT_THAT(result, IsOk());
    if (value) {
      EXPECT_EQ(result.value()->op(), Expression::Operation::kTrue);
    } else {
      EXPECT_EQ(result.value()->op(), Expression::Operation::kFalse);
    }
  };
  checkBoolean(True::Instance(), true);
  checkBoolean(False::Instance(), false);
}

TEST(ExpressionJsonTest, OperationTypeTests) {
  EXPECT_EQ(OperationTypeFromJson("true"), Expression::Operation::kTrue);
  EXPECT_EQ("true", ToJson(Expression::Operation::kTrue));
  EXPECT_TRUE(IsSetOperation(Expression::Operation::kIn));
  EXPECT_FALSE(IsSetOperation(Expression::Operation::kTrue));

  EXPECT_TRUE(IsUnaryOperation(Expression::Operation::kIsNull));
  EXPECT_FALSE(IsUnaryOperation(Expression::Operation::kTrue));
}

TEST(ExpressionJsonTest, NameReferenceRoundTrip) {
  ICEBERG_UNWRAP_OR_FAIL(auto ref, NamedReference::Make("col_name"));
  auto json = ToJson(*ref);
  EXPECT_EQ(json.get<std::string>(), "col_name");

  ICEBERG_UNWRAP_OR_FAIL(auto parsed, NamedReferenceFromJson(json));
  EXPECT_EQ(parsed->name(), "col_name");
}

TEST(ExpressionJsonTest, UnboundTransfromRoundTrip) {
  ICEBERG_UNWRAP_OR_FAIL(auto ref, NamedReference::Make("ts"));
  auto transform = Transform::Day();
  ICEBERG_UNWRAP_OR_FAIL(auto unbound, UnboundTransform::Make(std::move(ref), transform));

  auto json = ToJson(*unbound);
  EXPECT_EQ(json["type"], "transform");
  EXPECT_EQ(json["transform"], "day");
  EXPECT_EQ(json["term"], "ts");

  ICEBERG_UNWRAP_OR_FAIL(auto parsed, UnboundTransformFromJson(json));
  EXPECT_EQ(parsed->reference()->name(), unbound->reference()->name());
  EXPECT_EQ(parsed->transform()->transform_type(),
            unbound->transform()->transform_type());
  EXPECT_EQ(parsed->transform()->ToString(), unbound->transform()->ToString());
}

TEST(ExpressionJsonTest, BucketTransform) {
  ICEBERG_UNWRAP_OR_FAIL(auto ref, NamedReference::Make("id"));
  ICEBERG_UNWRAP_OR_FAIL(auto unbound,
                         UnboundTransform::Make(std::move(ref), Transform::Bucket(16)));

  auto json = ToJson(*unbound);
  EXPECT_EQ(json["type"], "transform");
  EXPECT_EQ(json["transform"], "bucket[16]");
  EXPECT_EQ(json["term"], "id");

  ICEBERG_UNWRAP_OR_FAIL(auto parsed, UnboundTransformFromJson(json));
  EXPECT_EQ(parsed->transform()->transform_type(),
            unbound->transform()->transform_type());
  EXPECT_EQ(parsed->transform()->ToString(), unbound->transform()->ToString());
}

TEST(ExpressionJsonTest, InvalidInput) {
  EXPECT_THAT(UnboundTransformFromJson(nlohmann::json::object()),
              IsError(ErrorKind::kJsonParseError));
  EXPECT_THAT(UnboundTransformFromJson(nlohmann::json{{"type", "other"}}),
              IsError(ErrorKind::kJsonParseError));
}

}  // namespace iceberg
