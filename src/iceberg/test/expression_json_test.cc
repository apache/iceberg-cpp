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

}  // namespace iceberg
