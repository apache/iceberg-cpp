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

#include <gtest/gtest.h>

#include "iceberg/expression/binder.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/expression/rewrite_not.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"

namespace iceberg {

class ExpressionVisitorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    schema_ = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64()),
                                 SchemaField::MakeOptional(2, "name", string()),
                                 SchemaField::MakeRequired(3, "age", int32()),
                                 SchemaField::MakeOptional(4, "salary", float64()),
                                 SchemaField::MakeRequired(5, "active", boolean())},
        /*schema_id=*/0);
  }

  Result<std::shared_ptr<Expression>> Bind(const std::shared_ptr<Expression>& expr,
                                           bool case_sensitive = true) {
    return Binder::Bind(*schema_, expr, case_sensitive);
  }

  std::shared_ptr<Schema> schema_;
};

class BinderTest : public ExpressionVisitorTest {};

TEST_F(BinderTest, UnaryPredicates) {
  // Test IsNull
  auto unbound_is_null = Expressions::IsNull("name");
  ICEBERG_UNWRAP_OR_FAIL(auto bound_is_null, Bind(unbound_is_null));
  EXPECT_EQ(bound_is_null->op(), Expression::Operation::kIsNull);
  EXPECT_TRUE(bound_is_null->is_bound_predicate());
  EXPECT_EQ(bound_is_null->ToString(), "is_null(ref(id=2, type=string))");

  // Test NotNull
  auto unbound_not_null = Expressions::NotNull("name");
  ICEBERG_UNWRAP_OR_FAIL(auto bound_not_null, Bind(unbound_not_null));
  EXPECT_EQ(bound_not_null->op(), Expression::Operation::kNotNull);
  EXPECT_TRUE(bound_not_null->is_bound_predicate());
  EXPECT_EQ(bound_not_null->ToString(), "not_null(ref(id=2, type=string))");

  // Test IsNaN
  auto unbound_is_nan = Expressions::IsNaN("salary");
  ICEBERG_UNWRAP_OR_FAIL(auto bound_is_nan, Bind(unbound_is_nan));
  EXPECT_EQ(bound_is_nan->op(), Expression::Operation::kIsNan);
  EXPECT_TRUE(bound_is_nan->is_bound_predicate());
  EXPECT_EQ(bound_is_nan->ToString(), "is_nan(ref(id=4, type=double))");

  // Test NotNaN
  auto unbound_not_nan = Expressions::NotNaN("salary");
  ICEBERG_UNWRAP_OR_FAIL(auto bound_not_nan, Bind(unbound_not_nan));
  EXPECT_EQ(bound_not_nan->op(), Expression::Operation::kNotNan);
  EXPECT_TRUE(bound_not_nan->is_bound_predicate());
  EXPECT_EQ(bound_not_nan->ToString(), "not_nan(ref(id=4, type=double))");
}

TEST_F(BinderTest, ComparisonPredicates) {
  // Test LessThan
  auto unbound_lt = Expressions::LessThan("age", Literal::Int(30));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_lt, Bind(unbound_lt));
  EXPECT_EQ(bound_lt->op(), Expression::Operation::kLt);
  EXPECT_TRUE(bound_lt->is_bound_predicate());
  EXPECT_EQ(bound_lt->ToString(), "ref(id=3, type=int) < 30");

  // Test LessThanOrEqual
  auto unbound_lte = Expressions::LessThanOrEqual("age", Literal::Int(30));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_lte, Bind(unbound_lte));
  EXPECT_EQ(bound_lte->op(), Expression::Operation::kLtEq);
  EXPECT_TRUE(bound_lte->is_bound_predicate());
  EXPECT_EQ(bound_lte->ToString(), "ref(id=3, type=int) <= 30");

  // Test GreaterThan
  auto unbound_gt = Expressions::GreaterThan("salary", Literal::Double(50000.0));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_gt, Bind(unbound_gt));
  EXPECT_EQ(bound_gt->op(), Expression::Operation::kGt);
  EXPECT_TRUE(bound_gt->is_bound_predicate());
  EXPECT_EQ(bound_gt->ToString(), "ref(id=4, type=double) > 50000.000000");

  // Test GreaterThanOrEqual
  auto unbound_gte = Expressions::GreaterThanOrEqual("salary", Literal::Double(50000.0));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_gte, Bind(unbound_gte));
  EXPECT_EQ(bound_gte->op(), Expression::Operation::kGtEq);
  EXPECT_TRUE(bound_gte->is_bound_predicate());
  EXPECT_EQ(bound_gte->ToString(), "ref(id=4, type=double) >= 50000.000000");

  // Test Equal
  auto unbound_eq = Expressions::Equal("name", Literal::String("Alice"));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_eq, Bind(unbound_eq));
  EXPECT_EQ(bound_eq->op(), Expression::Operation::kEq);
  EXPECT_TRUE(bound_eq->is_bound_predicate());
  EXPECT_EQ(bound_eq->ToString(), "ref(id=2, type=string) == \"Alice\"");

  // Test NotEqual
  auto unbound_neq = Expressions::NotEqual("name", Literal::String("Bob"));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_neq, Bind(unbound_neq));
  EXPECT_EQ(bound_neq->op(), Expression::Operation::kNotEq);
  EXPECT_TRUE(bound_neq->is_bound_predicate());
  EXPECT_EQ(bound_neq->ToString(), "ref(id=2, type=string) != \"Bob\"");
}

TEST_F(BinderTest, StringPredicates) {
  // Test StartsWith
  auto unbound_starts = Expressions::StartsWith("name", "Al");
  ICEBERG_UNWRAP_OR_FAIL(auto bound_starts, Bind(unbound_starts));
  EXPECT_EQ(bound_starts->op(), Expression::Operation::kStartsWith);
  EXPECT_TRUE(bound_starts->is_bound_predicate());
  EXPECT_EQ(bound_starts->ToString(), "ref(id=2, type=string) startsWith \"\"Al\"\"");

  // Test NotStartsWith
  auto unbound_not_starts = Expressions::NotStartsWith("name", "Bo");
  ICEBERG_UNWRAP_OR_FAIL(auto bound_not_starts, Bind(unbound_not_starts));
  EXPECT_EQ(bound_not_starts->op(), Expression::Operation::kNotStartsWith);
  EXPECT_TRUE(bound_not_starts->is_bound_predicate());
  EXPECT_EQ(bound_not_starts->ToString(),
            "ref(id=2, type=string) notStartsWith \"\"Bo\"\"");
}

TEST_F(BinderTest, SetPredicates) {
  // Test In
  auto unbound_in =
      Expressions::In("age", {Literal::Int(25), Literal::Int(30), Literal::Int(35)});
  ICEBERG_UNWRAP_OR_FAIL(auto bound_in, Bind(unbound_in));
  EXPECT_EQ(bound_in->op(), Expression::Operation::kIn);
  EXPECT_TRUE(bound_in->is_bound_predicate());
  EXPECT_THAT(bound_in->ToString(), testing::HasSubstr("ref(id=3, type=int) in ("));

  // Test NotIn
  auto unbound_not_in = Expressions::NotIn("age", {Literal::Int(40), Literal::Int(45)});
  ICEBERG_UNWRAP_OR_FAIL(auto bound_not_in, Bind(unbound_not_in));
  EXPECT_EQ(bound_not_in->op(), Expression::Operation::kNotIn);
  EXPECT_TRUE(bound_not_in->is_bound_predicate());
  EXPECT_THAT(bound_not_in->ToString(),
              testing::HasSubstr("ref(id=3, type=int) not in ("));
}

TEST_F(BinderTest, Constants) {
  // Test AlwaysTrue
  auto true_expr = Expressions::AlwaysTrue();
  ICEBERG_UNWRAP_OR_FAIL(auto bound_true, Bind(true_expr));
  EXPECT_EQ(bound_true->op(), Expression::Operation::kTrue);

  // Test AlwaysFalse
  auto false_expr = Expressions::AlwaysFalse();
  ICEBERG_UNWRAP_OR_FAIL(auto bound_false, Bind(false_expr));
  EXPECT_EQ(bound_false->op(), Expression::Operation::kFalse);
}

TEST_F(BinderTest, AndExpression) {
  auto pred1 = Expressions::Equal("name", Literal::String("Alice"));
  auto pred2 = Expressions::GreaterThan("age", Literal::Int(25));
  auto unbound_and = Expressions::And(pred1, pred2);

  ICEBERG_UNWRAP_OR_FAIL(auto bound_and, Bind(unbound_and));
  EXPECT_EQ(bound_and->op(), Expression::Operation::kAnd);
  EXPECT_EQ(bound_and->ToString(),
            "(ref(id=2, type=string) == \"Alice\" and ref(id=3, type=int) > 25)");

  // Verify both children are bound
  auto result = IsBoundVisitor::IsBound(bound_and);
  ASSERT_THAT(result, IsOk());
  EXPECT_TRUE(result.value());
}

TEST_F(BinderTest, OrExpression) {
  auto pred1 = Expressions::IsNull("name");
  auto pred2 = Expressions::LessThan("salary", Literal::Double(30000.0));
  auto unbound_or = Expressions::Or(pred1, pred2);

  ICEBERG_UNWRAP_OR_FAIL(auto bound_or, Bind(unbound_or));
  EXPECT_EQ(bound_or->op(), Expression::Operation::kOr);
  EXPECT_EQ(bound_or->ToString(),
            "(is_null(ref(id=2, type=string)) or ref(id=4, type=double) < 30000.000000)");

  // Verify both children are bound
  auto result = IsBoundVisitor::IsBound(bound_or);
  ASSERT_THAT(result, IsOk());
  EXPECT_TRUE(result.value());
}

TEST_F(BinderTest, NotExpression) {
  auto pred = Expressions::Equal("active", Literal::Boolean(true));
  auto unbound_not = Expressions::Not(pred);

  ICEBERG_UNWRAP_OR_FAIL(auto bound_not, Bind(unbound_not));
  EXPECT_EQ(bound_not->op(), Expression::Operation::kNot);
  EXPECT_EQ(bound_not->ToString(), "not(ref(id=5, type=boolean) == true)");

  // Verify child is bound
  auto result = IsBoundVisitor::IsBound(bound_not);
  ASSERT_THAT(result, IsOk());
  EXPECT_TRUE(result.value());
}

TEST_F(BinderTest, ComplexNestedExpression) {
  // (name = 'Alice' AND age > 25) OR (salary < 30000 AND active = true)
  auto pred1 = Expressions::Equal("name", Literal::String("Alice"));
  auto pred2 = Expressions::GreaterThan("age", Literal::Int(25));
  auto pred3 = Expressions::LessThan("salary", Literal::Double(30000.0));
  auto pred4 = Expressions::Equal("active", Literal::Boolean(true));

  auto and1 = Expressions::And(pred1, pred2);
  auto and2 = Expressions::And(pred3, pred4);
  auto complex_or = Expressions::Or(and1, and2);

  ICEBERG_UNWRAP_OR_FAIL(auto bound_complex, Bind(complex_or));
  EXPECT_EQ(bound_complex->op(), Expression::Operation::kOr);

  // Verify entire tree is bound
  auto result = IsBoundVisitor::IsBound(bound_complex);
  ASSERT_THAT(result, IsOk());
  EXPECT_TRUE(result.value());
}

TEST_F(BinderTest, CaseSensitive) {
  // Create predicate with exact field name
  auto pred_exact = Expressions::Equal("name", Literal::String("Alice"));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_exact, Bind(pred_exact, true));
  EXPECT_EQ(bound_exact->op(), Expression::Operation::kEq);
  EXPECT_TRUE(bound_exact->is_bound_predicate());

  // Create predicate with different case - should fail with case-sensitive binding
  auto pred_wrong_case = Expressions::Equal("NAME", Literal::String("Alice"));
  auto result_case_sensitive = Bind(pred_wrong_case, true);
  EXPECT_THAT(result_case_sensitive, HasErrorMessage("NAME"));
}

TEST_F(BinderTest, CaseInsensitive) {
  // Create predicate with different case
  auto pred_upper = Expressions::Equal("NAME", Literal::String("Alice"));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_upper, Bind(pred_upper, false));
  EXPECT_EQ(bound_upper->op(), Expression::Operation::kEq);
  EXPECT_TRUE(bound_upper->is_bound_predicate());

  // Create predicate with mixed case
  auto pred_mixed = Expressions::Equal("NaMe", Literal::String("Bob"));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_mixed, Bind(pred_mixed, false));
  EXPECT_EQ(bound_mixed->op(), Expression::Operation::kEq);
  EXPECT_TRUE(bound_mixed->is_bound_predicate());
}

TEST_F(BinderTest, ErrorFieldNotFound) {
  // Try to bind with non-existent field
  auto pred_nonexistent =
      Expressions::Equal("nonexistent_field", Literal::String("value"));
  auto result = Bind(pred_nonexistent);
  EXPECT_THAT(result, HasErrorMessage("Cannot find field 'nonexistent_field'"));
}

TEST_F(BinderTest, ErrorAlreadyBound) {
  // First bind the predicate
  auto unbound_pred = Expressions::Equal("name", Literal::String("Alice"));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_pred, Bind(unbound_pred));

  // Try to bind it again - should fail
  auto result = Bind(bound_pred);
  EXPECT_THAT(result, HasErrorMessage("already bound"));
}

TEST_F(BinderTest, ErrorNestedUnboundField) {
  // Create complex expression with one invalid field
  auto pred1 = Expressions::Equal("name", Literal::String("Alice"));
  auto pred2 = Expressions::Equal("invalid_field", Literal::String("value"));
  auto complex_and = Expressions::And(pred1, pred2);

  auto result = Bind(complex_and);
  EXPECT_THAT(result, HasErrorMessage("invalid_field"));
}

class IsBoundVisitorTest : public ExpressionVisitorTest {};

TEST_F(IsBoundVisitorTest, Constants) {
  // True and False should error out
  auto true_expr = Expressions::AlwaysTrue();
  auto result_true = IsBoundVisitor::IsBound(true_expr);
  EXPECT_THAT(result_true, IsError(ErrorKind::kInvalidExpression));

  auto false_expr = Expressions::AlwaysFalse();
  auto result_false = IsBoundVisitor::IsBound(false_expr);
  EXPECT_THAT(result_false, IsError(ErrorKind::kInvalidExpression));
}

TEST_F(IsBoundVisitorTest, UnboundPredicate) {
  // Unbound predicates should return false
  auto unbound_pred = Expressions::Equal("name", Literal::String("Alice"));
  ICEBERG_UNWRAP_OR_FAIL(auto is_bound, IsBoundVisitor::IsBound(unbound_pred));
  EXPECT_FALSE(is_bound);
}

TEST_F(IsBoundVisitorTest, BoundPredicate) {
  // Bound predicates should return true
  auto unbound_pred = Expressions::Equal("name", Literal::String("Alice"));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_pred, Bind(unbound_pred));
  ICEBERG_UNWRAP_OR_FAIL(auto is_bound, IsBoundVisitor::IsBound(bound_pred));
  EXPECT_TRUE(is_bound);
}

TEST_F(IsBoundVisitorTest, AndWithBoundChildren) {
  // AND with all bound children should return true
  auto pred1 = Expressions::Equal("name", Literal::String("Alice"));
  auto pred2 = Expressions::GreaterThan("age", Literal::Int(25));
  auto unbound_and = Expressions::And(pred1, pred2);
  ICEBERG_UNWRAP_OR_FAIL(auto bound_and, Bind(unbound_and));

  ICEBERG_UNWRAP_OR_FAIL(auto is_bound, IsBoundVisitor::IsBound(bound_and));
  EXPECT_TRUE(is_bound);
}

TEST_F(IsBoundVisitorTest, AndWithUnboundChild) {
  // AND with any unbound child should return false
  auto bound_pred = Expressions::Equal("name", Literal::String("Alice"));
  ICEBERG_UNWRAP_OR_FAIL(auto pred1, Bind(bound_pred));
  auto pred2 = Expressions::Equal("age", Literal::Int(25));  // unbound
  auto mixed_and = Expressions::And(pred1, pred2);

  ICEBERG_UNWRAP_OR_FAIL(auto is_bound, IsBoundVisitor::IsBound(mixed_and));
  EXPECT_FALSE(is_bound);
}

TEST_F(IsBoundVisitorTest, OrWithBoundChildren) {
  // OR with all bound children should return true
  auto pred1 = Expressions::IsNull("name");
  auto pred2 = Expressions::LessThan("salary", Literal::Double(30000.0));
  auto unbound_or = Expressions::Or(pred1, pred2);
  ICEBERG_UNWRAP_OR_FAIL(auto bound_or, Bind(unbound_or));

  ICEBERG_UNWRAP_OR_FAIL(auto is_bound, IsBoundVisitor::IsBound(bound_or));
  EXPECT_TRUE(is_bound);
}

TEST_F(IsBoundVisitorTest, OrWithUnboundChild) {
  // OR with any unbound child should return false
  auto pred1 = Expressions::IsNull("name");  // unbound
  auto bound_pred2 = Expressions::Equal("age", Literal::Int(25));
  ICEBERG_UNWRAP_OR_FAIL(auto pred2, Bind(bound_pred2));
  auto mixed_or = Expressions::Or(pred1, pred2);

  ICEBERG_UNWRAP_OR_FAIL(auto is_bound, IsBoundVisitor::IsBound(mixed_or));
  EXPECT_FALSE(is_bound);
}

TEST_F(IsBoundVisitorTest, NotWithBoundChild) {
  // NOT with bound child should return true
  auto unbound_pred = Expressions::Equal("active", Literal::Boolean(true));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_pred, Bind(unbound_pred));
  auto not_expr = Expressions::Not(bound_pred);

  ICEBERG_UNWRAP_OR_FAIL(auto is_bound, IsBoundVisitor::IsBound(not_expr));
  EXPECT_TRUE(is_bound);
}

TEST_F(IsBoundVisitorTest, NotWithUnboundChild) {
  // NOT with unbound child should return false
  auto unbound_pred = Expressions::Equal("active", Literal::Boolean(true));
  auto not_expr = Expressions::Not(unbound_pred);

  ICEBERG_UNWRAP_OR_FAIL(auto is_bound, IsBoundVisitor::IsBound(not_expr));
  EXPECT_FALSE(is_bound);
}

TEST_F(IsBoundVisitorTest, ComplexExpression) {
  // Complex expression: all bound should return true
  auto pred1 = Expressions::Equal("name", Literal::String("Alice"));
  auto pred2 = Expressions::GreaterThan("age", Literal::Int(25));
  auto pred3 = Expressions::LessThan("salary", Literal::Double(30000.0));
  auto and_expr = Expressions::And(pred1, pred2);
  auto complex_or = Expressions::Or(and_expr, pred3);
  ICEBERG_UNWRAP_OR_FAIL(auto bound_complex, Bind(complex_or));

  ICEBERG_UNWRAP_OR_FAIL(auto is_bound, IsBoundVisitor::IsBound(bound_complex));
  EXPECT_TRUE(is_bound);

  // Complex expression: one unbound should return false
  auto unbound_pred = Expressions::Equal("name", Literal::String("Alice"));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_pred2, Bind(pred2));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_pred3, Bind(pred3));
  auto mixed_and = Expressions::And(unbound_pred, bound_pred2);
  auto mixed_complex = Expressions::Or(mixed_and, bound_pred3);

  ICEBERG_UNWRAP_OR_FAIL(auto is_bound_mixed, IsBoundVisitor::IsBound(mixed_complex));
  EXPECT_FALSE(is_bound_mixed);
}

class RewriteNotTest : public ExpressionVisitorTest {};

TEST_F(RewriteNotTest, Constants) {
  // True remains True
  auto true_expr = Expressions::AlwaysTrue();
  ICEBERG_UNWRAP_OR_FAIL(auto rewritten_true, RewriteNot::Visit(true_expr));
  EXPECT_EQ(rewritten_true->op(), Expression::Operation::kTrue);
  EXPECT_TRUE(rewritten_true->Equals(*True::Instance()));

  // False remains False
  auto false_expr = Expressions::AlwaysFalse();
  ICEBERG_UNWRAP_OR_FAIL(auto rewritten_false, RewriteNot::Visit(false_expr));
  EXPECT_EQ(rewritten_false->op(), Expression::Operation::kFalse);
  EXPECT_TRUE(rewritten_false->Equals(*False::Instance()));
}

TEST_F(RewriteNotTest, Predicates) {
  // Bound predicates pass through unchanged
  auto unbound_pred = Expressions::Equal("name", Literal::String("Alice"));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_pred, Bind(unbound_pred));
  ICEBERG_UNWRAP_OR_FAIL(auto rewritten, RewriteNot::Visit(bound_pred));
  EXPECT_EQ(rewritten->op(), Expression::Operation::kEq);
  EXPECT_TRUE(rewritten->is_bound_predicate());

  // Unbound predicates pass through unchanged
  auto unbound_pred2 = Expressions::IsNull("salary");
  ICEBERG_UNWRAP_OR_FAIL(auto rewritten_unbound, RewriteNot::Visit(unbound_pred2));
  EXPECT_EQ(rewritten_unbound->op(), Expression::Operation::kIsNull);
  EXPECT_TRUE(rewritten_unbound->is_unbound_predicate());
}

TEST_F(RewriteNotTest, NotExpression) {
  // NOT(predicate) should be rewritten to negated predicate
  auto unbound_pred = Expressions::Equal("name", Literal::String("Alice"));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_pred, Bind(unbound_pred));
  auto not_expr = Expressions::Not(bound_pred);

  ICEBERG_UNWRAP_OR_FAIL(auto rewritten, RewriteNot::Visit(not_expr));
  // Equal should be negated to NotEqual
  EXPECT_EQ(rewritten->op(), Expression::Operation::kNotEq);
  EXPECT_TRUE(rewritten->is_bound_predicate());
  EXPECT_EQ(rewritten->ToString(), "ref(id=2, type=string) != \"Alice\"");
}

TEST_F(RewriteNotTest, DoubleNegation) {
  // NOT(NOT(predicate)) should be rewritten back to predicate
  auto unbound_pred = Expressions::Equal("age", Literal::Int(25));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_pred, Bind(unbound_pred));
  auto not_expr = Expressions::Not(bound_pred);
  auto double_not = Expressions::Not(not_expr);

  ICEBERG_UNWRAP_OR_FAIL(auto rewritten, RewriteNot::Visit(double_not));
  // Should be back to Equal
  EXPECT_EQ(rewritten->op(), Expression::Operation::kEq);
  EXPECT_TRUE(rewritten->is_bound_predicate());
  EXPECT_EQ(rewritten->ToString(), "ref(id=3, type=int) == 25");
}

TEST_F(RewriteNotTest, AndExpression) {
  // AND expressions pass through (children are processed)
  auto pred1 = Expressions::Equal("name", Literal::String("Alice"));
  auto pred2 = Expressions::GreaterThan("age", Literal::Int(25));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_pred1, Bind(pred1));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_pred2, Bind(pred2));
  auto and_expr = Expressions::And(bound_pred1, bound_pred2);

  ICEBERG_UNWRAP_OR_FAIL(auto rewritten, RewriteNot::Visit(and_expr));
  EXPECT_EQ(rewritten->op(), Expression::Operation::kAnd);
}

TEST_F(RewriteNotTest, OrExpression) {
  // OR expressions pass through (children are processed)
  auto pred1 = Expressions::IsNull("name");
  auto pred2 = Expressions::LessThan("salary", Literal::Double(30000.0));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_pred1, Bind(pred1));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_pred2, Bind(pred2));
  auto or_expr = Expressions::Or(bound_pred1, bound_pred2);

  ICEBERG_UNWRAP_OR_FAIL(auto rewritten, RewriteNot::Visit(or_expr));
  EXPECT_EQ(rewritten->op(), Expression::Operation::kOr);
}

TEST_F(RewriteNotTest, ComplexExpression) {
  // Complex: NOT(pred1 AND NOT(pred2))
  auto pred1 = Expressions::Equal("name", Literal::String("Alice"));
  auto pred2 = Expressions::GreaterThan("age", Literal::Int(25));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_pred1, Bind(pred1));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_pred2, Bind(pred2));

  auto not_pred2 = Expressions::Not(bound_pred2);
  auto and_expr = Expressions::And(bound_pred1, not_pred2);
  auto not_and = Expressions::Not(and_expr);

  ICEBERG_UNWRAP_OR_FAIL(auto rewritten, RewriteNot::Visit(not_and));
  // The outer NOT should push down via negation
  // NOT(pred1 AND NOT(pred2)) becomes NOT(pred1) OR pred2
  EXPECT_EQ(rewritten->op(), Expression::Operation::kOr);
}

class ReferenceVisitorTest : public ExpressionVisitorTest {};

TEST_F(ReferenceVisitorTest, Constants) {
  // Constants should have no referenced fields
  auto true_expr = Expressions::AlwaysTrue();
  ICEBERG_UNWRAP_OR_FAIL(auto refs_true,
                         ReferenceVisitor::GetReferencedFieldIds(true_expr));
  EXPECT_TRUE(refs_true.empty());

  auto false_expr = Expressions::AlwaysFalse();
  ICEBERG_UNWRAP_OR_FAIL(auto refs_false,
                         ReferenceVisitor::GetReferencedFieldIds(false_expr));
  EXPECT_TRUE(refs_false.empty());
}

TEST_F(ReferenceVisitorTest, UnboundPredicate) {
  auto unbound_pred = Expressions::Equal("name", Literal::String("Alice"));
  auto result = ReferenceVisitor::GetReferencedFieldIds(unbound_pred);
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidExpression));
  EXPECT_THAT(result,
              HasErrorMessage("Cannot get referenced field IDs from unbound predicate"));
}

TEST_F(ReferenceVisitorTest, BoundPredicate) {
  // Bound predicate should return the field ID
  auto unbound_pred = Expressions::Equal("name", Literal::String("Alice"));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_pred, Bind(unbound_pred));

  ICEBERG_UNWRAP_OR_FAIL(auto refs, ReferenceVisitor::GetReferencedFieldIds(bound_pred));
  EXPECT_EQ(refs.size(), 1);
  EXPECT_EQ(refs.count(2), 1);  // name field has id=2
}

TEST_F(ReferenceVisitorTest, MultiplePredicates) {
  // Test various predicates with different fields
  auto pred_age = Expressions::GreaterThan("age", Literal::Int(25));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_age, Bind(pred_age));
  ICEBERG_UNWRAP_OR_FAIL(auto refs_age,
                         ReferenceVisitor::GetReferencedFieldIds(bound_age));
  EXPECT_EQ(refs_age.size(), 1);
  EXPECT_EQ(refs_age.count(3), 1);  // age field has id=3

  auto pred_salary = Expressions::LessThan("salary", Literal::Double(50000.0));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_salary, Bind(pred_salary));
  ICEBERG_UNWRAP_OR_FAIL(auto refs_salary,
                         ReferenceVisitor::GetReferencedFieldIds(bound_salary));
  EXPECT_EQ(refs_salary.size(), 1);
  EXPECT_EQ(refs_salary.count(4), 1);  // salary field has id=4
}

TEST_F(ReferenceVisitorTest, UnaryPredicates) {
  // Test unary predicates
  auto pred_is_null = Expressions::IsNull("name");
  ICEBERG_UNWRAP_OR_FAIL(auto bound_is_null, Bind(pred_is_null));
  ICEBERG_UNWRAP_OR_FAIL(auto refs,
                         ReferenceVisitor::GetReferencedFieldIds(bound_is_null));
  EXPECT_EQ(refs.size(), 1);
  EXPECT_EQ(refs.count(2), 1);

  auto pred_is_nan = Expressions::IsNaN("salary");
  ICEBERG_UNWRAP_OR_FAIL(auto bound_is_nan, Bind(pred_is_nan));
  ICEBERG_UNWRAP_OR_FAIL(auto refs_nan,
                         ReferenceVisitor::GetReferencedFieldIds(bound_is_nan));
  EXPECT_EQ(refs_nan.size(), 1);
  EXPECT_EQ(refs_nan.count(4), 1);
}

TEST_F(ReferenceVisitorTest, AndExpression) {
  // AND expression should return union of field IDs from both sides
  auto pred1 = Expressions::Equal("name", Literal::String("Alice"));
  auto pred2 = Expressions::GreaterThan("age", Literal::Int(25));
  auto and_expr = Expressions::And(pred1, pred2);

  ICEBERG_UNWRAP_OR_FAIL(auto bound_and, Bind(and_expr));
  ICEBERG_UNWRAP_OR_FAIL(auto refs, ReferenceVisitor::GetReferencedFieldIds(bound_and));

  EXPECT_EQ(refs.size(), 2);
  EXPECT_EQ(refs.count(2), 1);  // name field
  EXPECT_EQ(refs.count(3), 1);  // age field
}

TEST_F(ReferenceVisitorTest, OrExpression) {
  // OR expression should return union of field IDs from both sides
  auto pred1 = Expressions::IsNull("salary");
  auto pred2 = Expressions::Equal("active", Literal::Boolean(true));
  auto or_expr = Expressions::Or(pred1, pred2);

  ICEBERG_UNWRAP_OR_FAIL(auto bound_or, Bind(or_expr));
  ICEBERG_UNWRAP_OR_FAIL(auto refs, ReferenceVisitor::GetReferencedFieldIds(bound_or));

  EXPECT_EQ(refs.size(), 2);
  EXPECT_EQ(refs.count(4), 1);  // salary field
  EXPECT_EQ(refs.count(5), 1);  // active field
}

TEST_F(ReferenceVisitorTest, NotExpression) {
  // NOT expression should return field IDs from its child
  auto pred = Expressions::Equal("name", Literal::String("Alice"));
  auto not_expr = Expressions::Not(pred);

  ICEBERG_UNWRAP_OR_FAIL(auto bound_not, Bind(not_expr));
  ICEBERG_UNWRAP_OR_FAIL(auto refs, ReferenceVisitor::GetReferencedFieldIds(bound_not));

  EXPECT_EQ(refs.size(), 1);
  EXPECT_EQ(refs.count(2), 1);  // name field
}

TEST_F(ReferenceVisitorTest, ComplexNestedExpression) {
  // (name = 'Alice' AND age > 25) OR (salary < 30000 AND active = true)
  // Should reference fields: name(2), age(3), salary(4), active(5)
  auto pred1 = Expressions::Equal("name", Literal::String("Alice"));
  auto pred2 = Expressions::GreaterThan("age", Literal::Int(25));
  auto pred3 = Expressions::LessThan("salary", Literal::Double(30000.0));
  auto pred4 = Expressions::Equal("active", Literal::Boolean(true));

  auto and1 = Expressions::And(pred1, pred2);
  auto and2 = Expressions::And(pred3, pred4);
  auto complex_or = Expressions::Or(and1, and2);

  ICEBERG_UNWRAP_OR_FAIL(auto bound_complex, Bind(complex_or));
  ICEBERG_UNWRAP_OR_FAIL(auto refs,
                         ReferenceVisitor::GetReferencedFieldIds(bound_complex));

  EXPECT_EQ(refs.size(), 4);
  EXPECT_EQ(refs.count(2), 1);  // name field
  EXPECT_EQ(refs.count(3), 1);  // age field
  EXPECT_EQ(refs.count(4), 1);  // salary field
  EXPECT_EQ(refs.count(5), 1);  // active field
}

TEST_F(ReferenceVisitorTest, DuplicateFieldReferences) {
  // Multiple predicates referencing the same field
  // age > 25 AND age < 50
  auto pred1 = Expressions::GreaterThan("age", Literal::Int(25));
  auto pred2 = Expressions::LessThan("age", Literal::Int(50));
  auto and_expr = Expressions::And(pred1, pred2);

  ICEBERG_UNWRAP_OR_FAIL(auto bound_and, Bind(and_expr));
  ICEBERG_UNWRAP_OR_FAIL(auto refs, ReferenceVisitor::GetReferencedFieldIds(bound_and));

  // Should only contain the field ID once (set semantics)
  EXPECT_EQ(refs.size(), 1);
  EXPECT_EQ(refs.count(3), 1);  // age field
}

TEST_F(ReferenceVisitorTest, SetPredicates) {
  // Test In predicate
  auto pred_in =
      Expressions::In("age", {Literal::Int(25), Literal::Int(30), Literal::Int(35)});
  ICEBERG_UNWRAP_OR_FAIL(auto bound_in, Bind(pred_in));
  ICEBERG_UNWRAP_OR_FAIL(auto refs_in, ReferenceVisitor::GetReferencedFieldIds(bound_in));

  EXPECT_EQ(refs_in.size(), 1);
  EXPECT_EQ(refs_in.count(3), 1);  // age field

  // Test NotIn predicate
  auto pred_not_in =
      Expressions::NotIn("name", {Literal::String("Alice"), Literal::String("Bob")});
  ICEBERG_UNWRAP_OR_FAIL(auto bound_not_in, Bind(pred_not_in));
  ICEBERG_UNWRAP_OR_FAIL(auto refs_not_in,
                         ReferenceVisitor::GetReferencedFieldIds(bound_not_in));

  EXPECT_EQ(refs_not_in.size(), 1);
  EXPECT_EQ(refs_not_in.count(2), 1);  // name field
}

TEST_F(ReferenceVisitorTest, MixedBoundAndUnbound) {
  auto bound_pred = Expressions::Equal("name", Literal::String("Alice"));
  ICEBERG_UNWRAP_OR_FAIL(auto pred1, Bind(bound_pred));
  auto unbound_pred = Expressions::GreaterThan("age", Literal::Int(25));
  auto mixed_and = Expressions::And(pred1, unbound_pred);

  auto result = ReferenceVisitor::GetReferencedFieldIds(mixed_and);
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidExpression));
  EXPECT_THAT(result,
              HasErrorMessage("Cannot get referenced field IDs from unbound predicate"));
}

TEST_F(ReferenceVisitorTest, AllFields) {
  // Create expression referencing all fields in the schema
  auto pred1 = Expressions::NotNull("id");
  auto pred2 = Expressions::Equal("name", Literal::String("Test"));
  auto pred3 = Expressions::GreaterThan("age", Literal::Int(0));
  auto pred4 = Expressions::LessThan("salary", Literal::Double(100000.0));
  auto pred5 = Expressions::Equal("active", Literal::Boolean(true));

  auto and1 = Expressions::And(pred1, pred2);
  auto and2 = Expressions::And(pred3, pred4);
  auto and3 = Expressions::And(and1, and2);
  auto all_fields = Expressions::And(and3, pred5);

  ICEBERG_UNWRAP_OR_FAIL(auto bound_all, Bind(all_fields));
  ICEBERG_UNWRAP_OR_FAIL(auto refs, ReferenceVisitor::GetReferencedFieldIds(bound_all));

  // Should reference all 5 fields
  EXPECT_EQ(refs.size(), 4);
  EXPECT_EQ(refs.count(1), 0);  // id field is optimized out
  EXPECT_EQ(refs.count(2), 1);  // name field
  EXPECT_EQ(refs.count(3), 1);  // age field
  EXPECT_EQ(refs.count(4), 1);  // salary field
  EXPECT_EQ(refs.count(5), 1);  // active field
}

}  // namespace iceberg
