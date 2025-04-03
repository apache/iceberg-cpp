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

#include "iceberg/expression.h"

#include <memory>

#include <gtest/gtest.h>

using iceberg::Error;
using iceberg::ErrorKind;
using iceberg::Expression;

// A concrete implementation of Expression for testing
class TestExpression : public Expression {
 public:
  explicit TestExpression(Operation op) : operation_(op) {}

  Operation Op() const override { return operation_; }

  iceberg::expected<std::shared_ptr<Expression>, Error> Negate() const override {
    auto negated_op = Expression::Negate(operation_);
    if (!negated_op) {
      return iceberg::unexpected<Error>(negated_op.error());
    }
    return std::make_shared<TestExpression>(negated_op.value());
  }

  bool IsEquivalentTo(const Expression& other) const override {
    if (auto* test_expr = dynamic_cast<const TestExpression*>(&other)) {
      return operation_ == test_expr->operation_;
    }
    return false;
  }

 private:
  Operation operation_;
};

TEST(ExpressionTest, FromString) {
  // Test valid operations
  auto true_op = Expression::FromString("true");
  ASSERT_TRUE(true_op);
  EXPECT_EQ(true_op.value(), Expression::Operation::kTrue);

  auto false_op = Expression::FromString("false");
  ASSERT_TRUE(false_op);
  EXPECT_EQ(false_op.value(), Expression::Operation::kFalse);

  auto lt_op = Expression::FromString("lt");
  ASSERT_TRUE(lt_op);
  EXPECT_EQ(lt_op.value(), Expression::Operation::kLt);

  // Test case insensitivity
  auto and_op = Expression::FromString("AND");
  ASSERT_TRUE(and_op);
  EXPECT_EQ(and_op.value(), Expression::Operation::kAnd);

  // Test invalid operation
  auto invalid_op = Expression::FromString("invalid_operation");
  ASSERT_FALSE(invalid_op);
  EXPECT_EQ(invalid_op.error().kind, ErrorKind::kInvalidOperatorType);
  EXPECT_EQ(invalid_op.error().message, "Unknown operation type: invalid_operation");
}

TEST(ExpressionTest, NegateOperation) {
  // Test valid negations
  auto true_negated = Expression::Negate(Expression::Operation::kTrue);
  ASSERT_TRUE(true_negated);
  EXPECT_EQ(true_negated.value(), Expression::Operation::kFalse);

  auto lt_negated = Expression::Negate(Expression::Operation::kLt);
  ASSERT_TRUE(lt_negated);
  EXPECT_EQ(lt_negated.value(), Expression::Operation::kGtEq);

  auto is_null_negated = Expression::Negate(Expression::Operation::kIsNull);
  ASSERT_TRUE(is_null_negated);
  EXPECT_EQ(is_null_negated.value(), Expression::Operation::kNotNull);

  // Test invalid negation
  auto not_negated = Expression::Negate(Expression::Operation::kNot);
  ASSERT_FALSE(not_negated);
  EXPECT_EQ(not_negated.error().kind, ErrorKind::kInvalidOperatorType);
  EXPECT_EQ(not_negated.error().message, "No negation defined for operation");
}

TEST(ExpressionTest, FlipLR) {
  // Test valid flips
  auto lt_flipped = Expression::FlipLR(Expression::Operation::kLt);
  ASSERT_TRUE(lt_flipped);
  EXPECT_EQ(lt_flipped.value(), Expression::Operation::kGt);

  auto eq_flipped = Expression::FlipLR(Expression::Operation::kEq);
  ASSERT_TRUE(eq_flipped);
  EXPECT_EQ(eq_flipped.value(), Expression::Operation::kEq);

  // Test invalid flip
  auto in_flipped = Expression::FlipLR(Expression::Operation::kIn);
  ASSERT_FALSE(in_flipped);
  EXPECT_EQ(in_flipped.error().kind, ErrorKind::kInvalidOperatorType);
  EXPECT_EQ(in_flipped.error().message, "No left-right flip for operation");
}

TEST(ExpressionTest, TestExpressionNegate) {
  // Test negatable expression
  auto expr = std::make_shared<TestExpression>(Expression::Operation::kLt);
  auto negated = expr->Negate();
  ASSERT_TRUE(negated);
  EXPECT_EQ(negated.value()->Op(), Expression::Operation::kGtEq);

  // Test equality between original and double-negated
  auto double_negated = negated.value()->Negate();
  ASSERT_TRUE(double_negated);
  EXPECT_TRUE(expr->IsEquivalentTo(*double_negated.value()));

  // Test non-negatable expression
  auto non_negatable = std::make_shared<TestExpression>(Expression::Operation::kNot);
  auto negated_result = non_negatable->Negate();
  ASSERT_FALSE(negated_result);
  EXPECT_EQ(negated_result.error().kind, ErrorKind::kInvalidOperatorType);
  EXPECT_EQ(negated_result.error().message, "No negation defined for operation");
}

TEST(ExpressionTest, IsEquivalentTo) {
  auto expr1 = std::make_shared<TestExpression>(Expression::Operation::kEq);
  auto expr2 = std::make_shared<TestExpression>(Expression::Operation::kEq);
  auto expr3 = std::make_shared<TestExpression>(Expression::Operation::kNotEq);

  // Same operation should be equivalent
  EXPECT_TRUE(expr1->IsEquivalentTo(*expr2));

  // Different operations should not be equivalent
  EXPECT_FALSE(expr1->IsEquivalentTo(*expr3));

  // Test double negation equivalence
  auto negated = expr1->Negate();
  ASSERT_TRUE(negated);
  auto double_negated = negated.value()->Negate();
  ASSERT_TRUE(double_negated);
  EXPECT_TRUE(expr1->IsEquivalentTo(*double_negated.value()));
}