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

  // Check that negated expression is True
  EXPECT_EQ(negated->op(), Expression::Operation::kTrue);
  EXPECT_EQ(negated->ToString(), "true");

  // Test negation of True returns false
  auto true_instance = True::Instance();
  negated = true_instance->Negate();

  // Check that negated expression is False
  EXPECT_EQ(negated->op(), Expression::Operation::kFalse);
  EXPECT_EQ(negated->ToString(), "false");
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

TEST(ORTest, Basic) {
  // Create True and False expressions
  auto true_expr = True::Instance();
  auto false_expr = False::Instance();

  // Create an OR expression
  auto or_expr = std::make_shared<Or>(true_expr, false_expr);

  EXPECT_EQ(or_expr->op(), Expression::Operation::kOr);
  EXPECT_EQ(or_expr->ToString(), "(true or false)");
  EXPECT_EQ(or_expr->left()->op(), Expression::Operation::kTrue);
  EXPECT_EQ(or_expr->right()->op(), Expression::Operation::kFalse);
}

TEST(ORTest, Negation) {
  // Test De Morgan's law: not(A or B) = (not A) and (not B)
  auto true_expr = True::Instance();
  auto false_expr = False::Instance();

  auto or_expr = std::make_shared<Or>(true_expr, false_expr);
  auto negated_or = or_expr->Negate();

  // Should become AND expression
  EXPECT_EQ(negated_or->op(), Expression::Operation::kAnd);
  EXPECT_EQ(negated_or->ToString(), "(false and true)");
}

TEST(ORTest, Equals) {
  auto true_expr = True::Instance();
  auto false_expr = False::Instance();

  // Test basic equality
  auto or_expr1 = std::make_shared<Or>(true_expr, false_expr);
  auto or_expr2 = std::make_shared<Or>(true_expr, false_expr);
  EXPECT_TRUE(or_expr1->Equals(*or_expr2));

  // Test commutativity: (A or B) equals (B or A)
  auto or_expr3 = std::make_shared<Or>(false_expr, true_expr);
  EXPECT_TRUE(or_expr1->Equals(*or_expr3));

  // Test inequality with different expressions
  auto or_expr4 = std::make_shared<Or>(true_expr, true_expr);
  EXPECT_FALSE(or_expr1->Equals(*or_expr4));

  // Test inequality with different operation types
  auto and_expr = std::make_shared<And>(true_expr, false_expr);
  EXPECT_FALSE(or_expr1->Equals(*and_expr));
}

TEST(ANDTest, Negation) {
  // Test De Morgan's law: not(A and B) = (not A) or (not B)
  auto true_expr = True::Instance();
  auto false_expr = False::Instance();

  auto and_expr = std::make_shared<And>(true_expr, false_expr);
  auto negated_and = and_expr->Negate();

  // Should become OR expression
  EXPECT_EQ(negated_and->op(), Expression::Operation::kOr);
  EXPECT_EQ(negated_and->ToString(), "(false or true)");
}

TEST(ANDTest, Equals) {
  auto true_expr = True::Instance();
  auto false_expr = False::Instance();

  // Test basic equality
  auto and_expr1 = std::make_shared<And>(true_expr, false_expr);
  auto and_expr2 = std::make_shared<And>(true_expr, false_expr);
  EXPECT_TRUE(and_expr1->Equals(*and_expr2));

  // Test commutativity: (A and B) equals (B and A)
  auto and_expr3 = std::make_shared<And>(false_expr, true_expr);
  EXPECT_TRUE(and_expr1->Equals(*and_expr3));

  // Test inequality with different expressions
  auto and_expr4 = std::make_shared<And>(true_expr, true_expr);
  EXPECT_FALSE(and_expr1->Equals(*and_expr4));

  // Test inequality with different operation types
  auto or_expr = std::make_shared<Or>(true_expr, false_expr);
  EXPECT_FALSE(and_expr1->Equals(*or_expr));
}

TEST(ExpressionTest, BaseClassNegateThrowsException) {
  // Create a mock expression that doesn't override Negate()
  class MockExpression : public Expression {
   public:
    Operation op() const override { return Operation::kTrue; }
    // Deliberately not overriding Negate() to test base class behavior
  };

  auto mock_expr = std::make_shared<MockExpression>();

  // Should throw IcebergError when calling Negate() on base class
  EXPECT_THROW(mock_expr->Negate(), IcebergError);
}
}  // namespace iceberg
