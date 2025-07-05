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
  auto false_instance = Predicate::AlwaysFalse();
  auto negated = false_instance->Negate();

  // Check that negated expression is True
  EXPECT_EQ(negated->op(), Operation::kTrue);
  EXPECT_EQ(negated->ToString(), "true");

  // Test negation of True returns false
  auto true_instance = Predicate::AlwaysTrue();
  negated = true_instance->Negate();

  // Check that negated expression is False
  EXPECT_EQ(negated->op(), Operation::kFalse);
  EXPECT_EQ(negated->ToString(), "false");
}

TEST(ANDTest, Basic) {
  // Create two True expressions
  auto true_expr1 = Predicate::AlwaysTrue();
  auto true_expr2 = Predicate::AlwaysTrue();

  // Create an AND expression
  auto and_expr = Predicate::And(true_expr1, true_expr2);

  EXPECT_EQ(and_expr->op(), Operation::kAnd);
  EXPECT_EQ(and_expr->ToString(), "(true and true)");
}

TEST(ORTest, Basic) {
  // Create True and False expressions
  auto true_expr = Predicate::AlwaysTrue();
  auto false_expr = Predicate::AlwaysFalse();

  // Create an OR expression
  auto or_expr = Predicate::Or(true_expr, false_expr);

  EXPECT_EQ(or_expr->op(), Operation::kOr);
  EXPECT_EQ(or_expr->ToString(), "(true or false)");
}

TEST(ORTest, Negation) {
  // Test De Morgan's law: not(A or B) = (not A) and (not B)
  auto true_expr = Predicate::AlwaysTrue();
  auto false_expr = Predicate::AlwaysFalse();

  auto or_expr = Predicate::Or(true_expr, false_expr);
  auto negated_or = or_expr->Negate();

  // Should become AND expression
  EXPECT_EQ(negated_or->op(), Operation::kAnd);
  EXPECT_EQ(negated_or->ToString(), "(false and true)");
}

TEST(ORTest, Equals) {
  auto true_expr = Predicate::AlwaysTrue();
  auto false_expr = Predicate::AlwaysFalse();

  // Test basic equality
  auto or_expr1 = Predicate::Or(true_expr, false_expr);
  auto or_expr2 = Predicate::Or(true_expr, false_expr);
  EXPECT_TRUE(or_expr1->Equals(*or_expr2));

  // Test commutativity: (A or B) equals (B or A)
  auto or_expr3 = Predicate::Or(false_expr, true_expr);
  EXPECT_TRUE(or_expr1->Equals(*or_expr3));

  // Test inequality with different expressions
  auto or_expr4 = Predicate::Or(true_expr, true_expr);
  EXPECT_FALSE(or_expr1->Equals(*or_expr4));

  // Test inequality with different operation types
  auto and_expr = Predicate::And(true_expr, false_expr);
  EXPECT_FALSE(or_expr1->Equals(*and_expr));
}

TEST(ANDTest, Negation) {
  // Test De Morgan's law: not(A and B) = (not A) or (not B)
  auto true_expr = Predicate::AlwaysTrue();
  auto false_expr = Predicate::AlwaysFalse();

  auto and_expr = Predicate::And(true_expr, false_expr);
  auto negated_and = and_expr->Negate();

  // Should become OR expression
  EXPECT_EQ(negated_and->op(), Operation::kOr);
  EXPECT_EQ(negated_and->ToString(), "(false or true)");
}

TEST(ANDTest, Equals) {
  auto true_expr = Predicate::AlwaysTrue();
  auto false_expr = Predicate::AlwaysFalse();

  // Test basic equality
  auto and_expr1 = Predicate::And(true_expr, false_expr);
  auto and_expr2 = Predicate::And(true_expr, false_expr);
  EXPECT_TRUE(and_expr1->Equals(*and_expr2));

  // Test commutativity: (A and B) equals (B and A)
  auto and_expr3 = Predicate::And(false_expr, true_expr);
  EXPECT_TRUE(and_expr1->Equals(*and_expr3));

  // Test inequality with different expressions
  auto and_expr4 = Predicate::And(true_expr, true_expr);
  EXPECT_FALSE(and_expr1->Equals(*and_expr4));

  // Test inequality with different operation types
  auto or_expr = Predicate::Or(true_expr, false_expr);
  EXPECT_FALSE(and_expr1->Equals(*or_expr));
}

TEST(PredicateFactoryTest, FactoryMethods) {
  // Test that factory methods work correctly
  auto true_pred = Predicate::AlwaysTrue();
  auto false_pred = Predicate::AlwaysFalse();

  EXPECT_EQ(true_pred->op(), Operation::kTrue);
  EXPECT_EQ(false_pred->op(), Operation::kFalse);

  // Test that multiple calls return equivalent instances
  auto true_pred2 = Predicate::AlwaysTrue();
  auto false_pred2 = Predicate::AlwaysFalse();

  EXPECT_TRUE(true_pred->Equals(*true_pred2));
  EXPECT_TRUE(false_pred->Equals(*false_pred2));

  // Test compound expressions
  auto and_pred = Predicate::And(true_pred, false_pred);
  auto or_pred = Predicate::Or(true_pred, false_pred);

  EXPECT_EQ(and_pred->op(), Operation::kAnd);
  EXPECT_EQ(or_pred->op(), Operation::kOr);

  // Test nested expressions
  auto nested_and = Predicate::And(and_pred, or_pred);
  EXPECT_EQ(nested_and->op(), Operation::kAnd);
  EXPECT_EQ(nested_and->ToString(), "((true and false) and (true or false))");
}

}  // namespace iceberg
