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

#include <gtest/gtest.h>

#include "iceberg/expressions/and.h"
#include "iceberg/expressions/false.h"
#include "iceberg/expressions/true.h"

namespace iceberg {

TEST(TrueFalseTest, Basi) {
  // Test negation of False returns True
  const False& false_instance = False::instance();
  auto negated = false_instance.Negate();

  EXPECT_TRUE(negated.has_value());

  // Check that negated expression is True
  std::shared_ptr<Expression> true_expr = negated.value();
  EXPECT_EQ(true_expr->Op(), Expression::Operation::kTrue);

  EXPECT_EQ(true_expr->ToString(), "true");

  // Test negation of True returns false
  const True& true_instance = True::instance();
  negated = true_instance.Negate();

  EXPECT_TRUE(negated.has_value());

  // Check that negated expression is True
  std::shared_ptr<Expression> false_expr = negated.value();
  EXPECT_EQ(false_expr->Op(), Expression::Operation::kFalse);

  EXPECT_EQ(false_expr->ToString(), "false");
}

TEST(ANDTest, Basic) {
  // Create two True expressions
  auto true_expr1 = True::shared_instance();
  auto true_expr2 = True::shared_instance();

  // Create an AND expression
  auto and_expr = std::make_shared<And>(true_expr1, true_expr2);

  EXPECT_EQ(and_expr->Op(), Expression::Operation::kAnd);
  EXPECT_EQ(and_expr->ToString(), "(true and true)");
  EXPECT_EQ(and_expr->left()->Op(), Expression::Operation::kTrue);
  EXPECT_EQ(and_expr->right()->Op(), Expression::Operation::kTrue);
}
}  // namespace iceberg
