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
#include "iceberg/expression/json_internal.h"
#include "iceberg/expression/literal.h"
#include "iceberg/expression/predicate.h"
#include "iceberg/expression/term.h"
#include "iceberg/test/matchers.h"

namespace iceberg {

class ExpressionJsonTest : public ::testing::Test {
 protected:
  // Helper to test round-trip serialization
  // Uses string comparison since expressions may have different internal identity
  // but the same semantic meaning (i.e., ToString() output matches)
  void TestRoundTrip(const Expression& expr) {
    auto json = ToJson(expr);
    auto result = ExpressionFromJson(json);
    ASSERT_THAT(result, IsOk()) << "Failed to parse JSON: " << json.dump();
    EXPECT_EQ(expr.ToString(), result.value()->ToString())
        << "Round-trip failed.\nJSON: " << json.dump();
  }
};

// Test boolean constant expressions
TEST_F(ExpressionJsonTest, TrueExpression) {
  auto expr = True::Instance();
  auto json = ToJson(*expr);

  // True should serialize as JSON boolean true
  EXPECT_TRUE(json.is_boolean());
  EXPECT_TRUE(json.get<bool>());

  // Parse back
  auto result = ExpressionFromJson(json);
  ASSERT_THAT(result, IsOk());
  EXPECT_EQ(result.value()->op(), Expression::Operation::kTrue);
}

TEST_F(ExpressionJsonTest, FalseExpression) {
  auto expr = False::Instance();
  auto json = ToJson(*expr);

  // False should serialize as JSON boolean false
  EXPECT_TRUE(json.is_boolean());
  EXPECT_FALSE(json.get<bool>());

  // Parse back
  auto result = ExpressionFromJson(json);
  ASSERT_THAT(result, IsOk());
  EXPECT_EQ(result.value()->op(), Expression::Operation::kFalse);
}

}  // namespace iceberg
