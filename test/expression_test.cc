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

#include "iceberg/expression/expression.h"

#include <memory>

#include <gtest/gtest.h>

namespace iceberg {

TEST(TrueFalseTest, Basic) {
  // Test negation of False returns True
  auto false_instance = False::Instance();
  auto negated = false_instance->Negate();

  EXPECT_TRUE(negated.has_value());

  // Check that negated expression is True
  auto true_expr = negated.value();
  EXPECT_EQ(true_expr->op(), Expression::Operation::kTrue);

  EXPECT_EQ(true_expr->ToString(), "true");

  // Test negation of True returns false
  auto true_instance = True::Instance();
  negated = true_instance->Negate();

  EXPECT_TRUE(negated.has_value());

  // Check that negated expression is False
  auto false_expr = negated.value();
  EXPECT_EQ(false_expr->op(), Expression::Operation::kFalse);

  EXPECT_EQ(false_expr->ToString(), "false");
}

TEST(ANDTest, Basic) {
  // Create two True expressions
  auto true_expr1 = True::Instance();
  auto true_expr2 = True::Instance();

  // Create an AND expression
  auto and_expr = std::make_shared<And>(true_expr1, true_expr2);

  EXPECT_EQ(and_expr->op(), Expression::Operation::kAnd);
  EXPECT_EQ(and_expr->ToString(), "(true and true)");
  EXPECT_EQ(and_expr->left()->op(), Expression::Operation::kTrue);
  EXPECT_EQ(and_expr->right()->op(), Expression::Operation::kTrue);
}
}  // namespace iceberg
