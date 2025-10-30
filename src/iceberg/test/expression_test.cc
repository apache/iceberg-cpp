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

#include "iceberg/test/matchers.h"

namespace iceberg {

TEST(TrueFalseTest, Basic) {
  // Test negation of False returns True
  auto false_instance = False::Instance();
  auto negated_result = false_instance->Negate();
  ASSERT_THAT(negated_result, IsOk());
  auto negated = negated_result.value();

  // Check that negated expression is True
  EXPECT_EQ(negated->op(), Expression::Operation::kTrue);
  EXPECT_EQ(negated->ToString(), "true");

  // Test negation of True returns false
  auto true_instance = True::Instance();
  negated_result = true_instance->Negate();
  ASSERT_THAT(negated_result, IsOk());
  negated = negated_result.value();

  // Check that negated expression is False
  EXPECT_EQ(negated->op(), Expression::Operation::kFalse);
  EXPECT_EQ(negated->ToString(), "false");
}

TEST(ANDTest, Basic) {
  // Create two True expressions
  auto true_expr1 = True::Instance();
  auto true_expr2 = True::Instance();

  // Create an AND expression
  auto and_expr_result = And::Make(true_expr1, true_expr2);
  ASSERT_THAT(and_expr_result, IsOk());
  auto and_expr = std::shared_ptr<Expression>(std::move(and_expr_result.value()));

  EXPECT_EQ(and_expr->op(), Expression::Operation::kAnd);
  EXPECT_EQ(and_expr->ToString(), "(true and true)");
  auto& and_ref = static_cast<const And&>(*and_expr);
  EXPECT_EQ(and_ref.left()->op(), Expression::Operation::kTrue);
  EXPECT_EQ(and_ref.right()->op(), Expression::Operation::kTrue);
}

TEST(ORTest, Basic) {
  // Create True and False expressions
  auto true_expr = True::Instance();
  auto false_expr = False::Instance();

  // Create an OR expression
  auto or_expr_result = Or::Make(true_expr, false_expr);
  ASSERT_THAT(or_expr_result, IsOk());
  auto or_expr = std::shared_ptr<Expression>(std::move(or_expr_result.value()));

  EXPECT_EQ(or_expr->op(), Expression::Operation::kOr);
  EXPECT_EQ(or_expr->ToString(), "(true or false)");
  auto& or_ref = static_cast<const Or&>(*or_expr);
  EXPECT_EQ(or_ref.left()->op(), Expression::Operation::kTrue);
  EXPECT_EQ(or_ref.right()->op(), Expression::Operation::kFalse);
}

TEST(ORTest, Negation) {
  // Test De Morgan's law: not(A or B) = (not A) and (not B)
  auto true_expr = True::Instance();
  auto false_expr = False::Instance();

  auto or_expr_result = Or::Make(true_expr, false_expr);
  ASSERT_THAT(or_expr_result, IsOk());
  auto or_expr = std::shared_ptr<Expression>(std::move(or_expr_result.value()));
  auto negated_or_result = or_expr->Negate();
  ASSERT_THAT(negated_or_result, IsOk());
  auto negated_or = negated_or_result.value();

  // Should become AND expression
  EXPECT_EQ(negated_or->op(), Expression::Operation::kAnd);
  EXPECT_EQ(negated_or->ToString(), "(false and true)");
}

TEST(ORTest, Equals) {
  auto true_expr = True::Instance();
  auto false_expr = False::Instance();

  // Test basic equality
  auto or_expr1_result = Or::Make(true_expr, false_expr);
  ASSERT_THAT(or_expr1_result, IsOk());
  auto or_expr1 = std::shared_ptr<Expression>(std::move(or_expr1_result.value()));

  auto or_expr2_result = Or::Make(true_expr, false_expr);
  ASSERT_THAT(or_expr2_result, IsOk());
  auto or_expr2 = std::shared_ptr<Expression>(std::move(or_expr2_result.value()));
  EXPECT_TRUE(or_expr1->Equals(*or_expr2));

  // Test commutativity: (A or B) equals (B or A)
  auto or_expr3_result = Or::Make(false_expr, true_expr);
  ASSERT_THAT(or_expr3_result, IsOk());
  auto or_expr3 = std::shared_ptr<Expression>(std::move(or_expr3_result.value()));
  EXPECT_TRUE(or_expr1->Equals(*or_expr3));

  // Test inequality with different expressions
  auto or_expr4_result = Or::Make(true_expr, true_expr);
  ASSERT_THAT(or_expr4_result, IsOk());
  auto or_expr4 = std::shared_ptr<Expression>(std::move(or_expr4_result.value()));
  EXPECT_FALSE(or_expr1->Equals(*or_expr4));

  // Test inequality with different operation types
  auto and_expr_result = And::Make(true_expr, false_expr);
  ASSERT_THAT(and_expr_result, IsOk());
  auto and_expr = std::shared_ptr<Expression>(std::move(and_expr_result.value()));
  EXPECT_FALSE(or_expr1->Equals(*and_expr));
}

TEST(ANDTest, Negation) {
  // Test De Morgan's law: not(A and B) = (not A) or (not B)
  auto true_expr = True::Instance();
  auto false_expr = False::Instance();

  auto and_expr_result = And::Make(true_expr, false_expr);
  ASSERT_THAT(and_expr_result, IsOk());
  auto and_expr = std::shared_ptr<Expression>(std::move(and_expr_result.value()));
  auto negated_and_result = and_expr->Negate();
  ASSERT_THAT(negated_and_result, IsOk());
  auto negated_and = negated_and_result.value();

  // Should become OR expression
  EXPECT_EQ(negated_and->op(), Expression::Operation::kOr);
  EXPECT_EQ(negated_and->ToString(), "(false or true)");
}

TEST(ANDTest, Equals) {
  auto true_expr = True::Instance();
  auto false_expr = False::Instance();

  // Test basic equality
  auto and_expr1_result = And::Make(true_expr, false_expr);
  ASSERT_THAT(and_expr1_result, IsOk());
  auto and_expr1 = std::shared_ptr<Expression>(std::move(and_expr1_result.value()));

  auto and_expr2_result = And::Make(true_expr, false_expr);
  ASSERT_THAT(and_expr2_result, IsOk());
  auto and_expr2 = std::shared_ptr<Expression>(std::move(and_expr2_result.value()));
  EXPECT_TRUE(and_expr1->Equals(*and_expr2));

  // Test commutativity: (A and B) equals (B and A)
  auto and_expr3_result = And::Make(false_expr, true_expr);
  ASSERT_THAT(and_expr3_result, IsOk());
  auto and_expr3 = std::shared_ptr<Expression>(std::move(and_expr3_result.value()));
  EXPECT_TRUE(and_expr1->Equals(*and_expr3));

  // Test inequality with different expressions
  auto and_expr4_result = And::Make(true_expr, true_expr);
  ASSERT_THAT(and_expr4_result, IsOk());
  auto and_expr4 = std::shared_ptr<Expression>(std::move(and_expr4_result.value()));
  EXPECT_FALSE(and_expr1->Equals(*and_expr4));

  // Test inequality with different operation types
  auto or_expr_result = Or::Make(true_expr, false_expr);
  ASSERT_THAT(or_expr_result, IsOk());
  auto or_expr = std::shared_ptr<Expression>(std::move(or_expr_result.value()));
  EXPECT_FALSE(and_expr1->Equals(*or_expr));
}

TEST(ExpressionTest, BaseClassNegateErrorOut) {
  // Create a mock expression that doesn't override Negate()
  class MockExpression : public Expression {
   public:
    Operation op() const override { return Operation::kTrue; }
    // Deliberately not overriding Negate() to test base class behavior
  };

  auto mock_expr = std::make_shared<MockExpression>();

  // Should return NotSupported error when calling Negate() on base class
  auto negate_result = mock_expr->Negate();
  EXPECT_THAT(negate_result, IsError(ErrorKind::kNotSupported));
}

TEST(NotTest, Basic) {
  auto true_expr = True::Instance();
  auto not_expr_result = Not::Make(true_expr);
  ASSERT_THAT(not_expr_result, IsOk());
  auto not_expr = std::shared_ptr<Expression>(std::move(not_expr_result.value()));

  EXPECT_EQ(not_expr->op(), Expression::Operation::kNot);
  EXPECT_EQ(not_expr->ToString(), "not(true)");
  auto& not_ref = static_cast<const Not&>(*not_expr);
  EXPECT_EQ(not_ref.child()->op(), Expression::Operation::kTrue);
}

TEST(NotTest, Negation) {
  // Test that not(not(x)) = x
  auto true_expr = True::Instance();
  auto not_expr_result = Not::Make(true_expr);
  ASSERT_THAT(not_expr_result, IsOk());
  auto not_expr = std::shared_ptr<Expression>(std::move(not_expr_result.value()));

  auto negated_result = not_expr->Negate();
  ASSERT_THAT(negated_result, IsOk());
  auto negated = negated_result.value();

  // Should return the original true expression
  EXPECT_EQ(negated->op(), Expression::Operation::kTrue);
}

TEST(NotTest, Equals) {
  auto true_expr = True::Instance();
  auto false_expr = False::Instance();

  // Test basic equality
  auto not_expr1_result = Not::Make(true_expr);
  ASSERT_THAT(not_expr1_result, IsOk());
  auto not_expr1 = std::shared_ptr<Expression>(std::move(not_expr1_result.value()));

  auto not_expr2_result = Not::Make(true_expr);
  ASSERT_THAT(not_expr2_result, IsOk());
  auto not_expr2 = std::shared_ptr<Expression>(std::move(not_expr2_result.value()));
  EXPECT_TRUE(not_expr1->Equals(*not_expr2));

  // Test inequality with different child expressions
  auto not_expr3_result = Not::Make(false_expr);
  ASSERT_THAT(not_expr3_result, IsOk());
  auto not_expr3 = std::shared_ptr<Expression>(std::move(not_expr3_result.value()));
  EXPECT_FALSE(not_expr1->Equals(*not_expr3));

  // Test inequality with different operation types
  auto and_expr_result = And::Make(true_expr, false_expr);
  ASSERT_THAT(and_expr_result, IsOk());
  auto and_expr = std::shared_ptr<Expression>(std::move(and_expr_result.value()));
  EXPECT_FALSE(not_expr1->Equals(*and_expr));
}

}  // namespace iceberg
