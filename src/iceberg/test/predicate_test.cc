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

#include "iceberg/expression/predicate.h"

#include <limits>
#include <memory>

#include "iceberg/expression/expressions.h"
#include "iceberg/schema.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"

namespace iceberg {

class PredicateTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create a simple test schema with various field types
    schema_ = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64()),
                                 SchemaField::MakeOptional(2, "name", string()),
                                 SchemaField::MakeRequired(3, "age", int32()),
                                 SchemaField::MakeOptional(4, "salary", float64()),
                                 SchemaField::MakeRequired(5, "active", boolean())},
        /*schema_id=*/0);
  }

  std::shared_ptr<Schema> schema_;
};

TEST_F(PredicateTest, LogicalOperationsAndOr) {
  auto true_expr = Expressions::AlwaysTrue();
  auto false_expr = Expressions::AlwaysFalse();
  auto pred1 = Expressions::Equal("age", Literal::Int(25));
  auto pred2 = Expressions::Equal("name", Literal::String("test"));

  // Test AND operations
  auto and_true_true = Expressions::And(true_expr, true_expr);
  EXPECT_EQ(and_true_true->op(), Expression::Operation::kTrue);

  auto and_true_pred = Expressions::And(true_expr, pred1);
  EXPECT_EQ(and_true_pred->op(), Expression::Operation::kEq);

  auto and_pred_true = Expressions::And(pred1, true_expr);
  EXPECT_EQ(and_pred_true->op(), Expression::Operation::kEq);

  auto and_false_pred = Expressions::And(false_expr, pred1);
  EXPECT_EQ(and_false_pred->op(), Expression::Operation::kFalse);

  auto and_pred_false = Expressions::And(pred1, false_expr);
  EXPECT_EQ(and_pred_false->op(), Expression::Operation::kFalse);

  // Test OR operations
  auto or_false_false = Expressions::Or(false_expr, false_expr);
  EXPECT_EQ(or_false_false->op(), Expression::Operation::kFalse);

  auto or_false_pred = Expressions::Or(false_expr, pred1);
  EXPECT_EQ(or_false_pred->op(), Expression::Operation::kEq);

  auto or_pred_false = Expressions::Or(pred1, false_expr);
  EXPECT_EQ(or_pred_false->op(), Expression::Operation::kEq);

  auto or_true_pred = Expressions::Or(true_expr, pred1);
  EXPECT_EQ(or_true_pred->op(), Expression::Operation::kTrue);

  auto or_pred_true = Expressions::Or(pred1, true_expr);
  EXPECT_EQ(or_pred_true->op(), Expression::Operation::kTrue);
}

TEST_F(PredicateTest, ConstantExpressions) {
  auto always_true = Expressions::AlwaysTrue();
  auto always_false = Expressions::AlwaysFalse();

  EXPECT_EQ(always_true->op(), Expression::Operation::kTrue);
  EXPECT_EQ(always_false->op(), Expression::Operation::kFalse);
}

TEST_F(PredicateTest, UnaryPredicateFactory) {
  auto is_null_name = Expressions::IsNull("name");
  EXPECT_EQ(is_null_name->op(), Expression::Operation::kIsNull);
  EXPECT_EQ(is_null_name->reference()->name(), "name");

  auto not_null_name = Expressions::NotNull("active");
  EXPECT_EQ(not_null_name->op(), Expression::Operation::kNotNull);
  EXPECT_EQ(not_null_name->reference()->name(), "active");

  auto is_nan_name = Expressions::IsNaN("salary");
  EXPECT_EQ(is_nan_name->op(), Expression::Operation::kIsNan);
  EXPECT_EQ(is_nan_name->reference()->name(), "salary");

  auto not_nan_name = Expressions::NotNaN("salary");
  EXPECT_EQ(not_nan_name->op(), Expression::Operation::kNotNan);
  EXPECT_EQ(not_nan_name->reference()->name(), "salary");
}

TEST_F(PredicateTest, ComparisonPredicateFactory) {
  auto lt_name = Expressions::LessThan("age", Literal::Int(30));
  EXPECT_EQ(lt_name->op(), Expression::Operation::kLt);
  EXPECT_EQ(lt_name->reference()->name(), "age");

  auto lte_name = Expressions::LessThanOrEqual("salary", Literal::Double(50000.0));
  EXPECT_EQ(lte_name->op(), Expression::Operation::kLtEq);
  EXPECT_EQ(lte_name->reference()->name(), "salary");

  auto gt_name = Expressions::GreaterThan("id", Literal::Long(1000));
  EXPECT_EQ(gt_name->op(), Expression::Operation::kGt);
  EXPECT_EQ(gt_name->reference()->name(), "id");

  auto gte_name = Expressions::GreaterThanOrEqual("age", Literal::Int(18));
  EXPECT_EQ(gte_name->op(), Expression::Operation::kGtEq);
  EXPECT_EQ(gte_name->reference()->name(), "age");

  auto eq_name = Expressions::Equal("name", Literal::String("test"));
  EXPECT_EQ(eq_name->op(), Expression::Operation::kEq);
  EXPECT_EQ(eq_name->reference()->name(), "name");

  auto neq_name = Expressions::NotEqual("active", Literal::Boolean(false));
  EXPECT_EQ(neq_name->op(), Expression::Operation::kNotEq);
  EXPECT_EQ(neq_name->reference()->name(), "active");
}

TEST_F(PredicateTest, StringPredicateFactory) {
  auto starts_name = Expressions::StartsWith("name", "John");
  EXPECT_EQ(starts_name->op(), Expression::Operation::kStartsWith);
  EXPECT_EQ(starts_name->reference()->name(), "name");

  auto not_starts_name = Expressions::NotStartsWith("name", "Jane");
  EXPECT_EQ(not_starts_name->op(), Expression::Operation::kNotStartsWith);
  EXPECT_EQ(not_starts_name->reference()->name(), "name");
}

TEST_F(PredicateTest, SetPredicateFactory) {
  std::vector<Literal> values = {Literal::Int(10), Literal::Int(20), Literal::Int(30)};
  std::initializer_list<Literal> init_values = {Literal::String("a"),
                                                Literal::String("b")};

  auto in_name_vec = Expressions::In("age", values);
  EXPECT_EQ(in_name_vec->op(), Expression::Operation::kIn);
  EXPECT_EQ(in_name_vec->reference()->name(), "age");

  auto in_name_init = Expressions::In("name", init_values);
  EXPECT_EQ(in_name_init->op(), Expression::Operation::kIn);
  EXPECT_EQ(in_name_init->reference()->name(), "name");

  auto not_in_name_vec = Expressions::NotIn("age", values);
  EXPECT_EQ(not_in_name_vec->op(), Expression::Operation::kNotIn);
  EXPECT_EQ(not_in_name_vec->reference()->name(), "age");

  auto not_in_name_init = Expressions::NotIn("name", init_values);
  EXPECT_EQ(not_in_name_init->op(), Expression::Operation::kNotIn);
  EXPECT_EQ(not_in_name_init->reference()->name(), "name");
}

TEST_F(PredicateTest, GenericPredicateFactory) {
  auto pred_single =
      Expressions::Predicate(Expression::Operation::kEq, "age", Literal::Int(25));
  EXPECT_EQ(pred_single->op(), Expression::Operation::kEq);
  EXPECT_EQ(pred_single->reference()->name(), "age");

  std::vector<Literal> values = {Literal::Int(10), Literal::Int(20)};
  auto pred_multi = Expressions::Predicate(Expression::Operation::kIn, "age", values);
  EXPECT_EQ(pred_multi->op(), Expression::Operation::kIn);
  EXPECT_EQ(pred_multi->reference()->name(), "age");

  auto pred_unary = Expressions::Predicate(Expression::Operation::kIsNull, "name");
  EXPECT_EQ(pred_unary->op(), Expression::Operation::kIsNull);
  EXPECT_EQ(pred_unary->reference()->name(), "name");
}

TEST_F(PredicateTest, TransformFactory) {
  auto bucket_transform = Expressions::Bucket("id", 10);
  EXPECT_NE(bucket_transform, nullptr);
  EXPECT_EQ(bucket_transform->reference()->name(), "id");

  auto year_transform = Expressions::Year("timestamp_field");
  EXPECT_NE(year_transform, nullptr);
  EXPECT_EQ(year_transform->reference()->name(), "timestamp_field");

  auto month_transform = Expressions::Month("timestamp_field");
  EXPECT_NE(month_transform, nullptr);
  EXPECT_EQ(month_transform->reference()->name(), "timestamp_field");

  auto day_transform = Expressions::Day("timestamp_field");
  EXPECT_NE(day_transform, nullptr);
  EXPECT_EQ(day_transform->reference()->name(), "timestamp_field");

  auto hour_transform = Expressions::Hour("timestamp_field");
  EXPECT_NE(hour_transform, nullptr);
  EXPECT_EQ(hour_transform->reference()->name(), "timestamp_field");

  auto truncate_transform = Expressions::Truncate("string_field", 5);
  EXPECT_NE(truncate_transform, nullptr);
  EXPECT_EQ(truncate_transform->reference()->name(), "string_field");
}

TEST_F(PredicateTest, ReferenceFactory) {
  auto ref = Expressions::Ref("test_field");
  EXPECT_EQ(ref->name(), "test_field");
  EXPECT_EQ(ref->ToString(), "ref(name=\"test_field\")");
}

TEST_F(PredicateTest, NamedReferenceBasics) {
  auto ref = std::make_shared<NamedReference>("id");
  EXPECT_EQ(ref->name(), "id");
  EXPECT_EQ(ref->ToString(), "ref(name=\"id\")");
  EXPECT_EQ(ref->reference(), ref);
}

TEST_F(PredicateTest, NamedReferenceBind) {
  auto ref = std::make_shared<NamedReference>("id");
  auto bound_result = ref->Bind(*schema_, /*case_sensitive=*/true);
  ASSERT_THAT(bound_result, IsOk());

  auto bound_ref = bound_result.value();
  EXPECT_EQ(bound_ref->name(), "id");
  EXPECT_EQ(bound_ref->field().field_id(), 1);
  EXPECT_EQ(bound_ref->type()->type_id(), TypeId::kLong);
  EXPECT_FALSE(bound_ref->MayProduceNull());
}

TEST_F(PredicateTest, NamedReferenceBindNonExistentField) {
  auto ref = std::make_shared<NamedReference>("non_existent_field");
  auto bound_result = ref->Bind(*schema_, /*case_sensitive=*/true);
  EXPECT_THAT(bound_result, IsError(ErrorKind::kInvalidExpression));
}

TEST_F(PredicateTest, BoundReferenceEquality) {
  auto ref1 = std::make_shared<NamedReference>("id");
  auto ref2 = std::make_shared<NamedReference>("id");
  auto ref3 = std::make_shared<NamedReference>("name");

  auto bound1 = ref1->Bind(*schema_, true).value();
  auto bound2 = ref2->Bind(*schema_, true).value();
  auto bound3 = ref3->Bind(*schema_, true).value();

  // Same field should be equal
  EXPECT_TRUE(bound1->Equals(*bound2));
  EXPECT_TRUE(bound2->Equals(*bound1));

  // Different fields should not be equal
  EXPECT_FALSE(bound1->Equals(*bound3));
  EXPECT_FALSE(bound3->Equals(*bound1));
}

TEST_F(PredicateTest, UnboundPredicateCreation) {
  auto is_null_pred = Expressions::IsNull("name");
  EXPECT_EQ(is_null_pred->op(), Expression::Operation::kIsNull);
  EXPECT_EQ(is_null_pred->reference()->name(), "name");

  auto not_null_pred = Expressions::NotNull("name");
  EXPECT_EQ(not_null_pred->op(), Expression::Operation::kNotNull);

  auto equal_pred = Expressions::Equal("age", Literal::Int(25));
  EXPECT_EQ(equal_pred->op(), Expression::Operation::kEq);

  auto greater_than_pred = Expressions::GreaterThan("salary", Literal::Double(50000.0));
  EXPECT_EQ(greater_than_pred->op(), Expression::Operation::kGt);
}

TEST_F(PredicateTest, UnboundPredicateToString) {
  auto equal_pred = Expressions::Equal("age", Literal::Int(25));
  EXPECT_EQ(equal_pred->ToString(), "ref(name=\"age\") == 25");

  auto is_null_pred = Expressions::IsNull("name");
  EXPECT_EQ(is_null_pred->ToString(), "is_null(ref(name=\"name\"))");

  auto in_pred = Expressions::In("age", {Literal::Int(10), Literal::Int(20)});
  EXPECT_EQ(in_pred->ToString(), "ref(name=\"age\") in [10, 20]");

  auto starts_with_pred = Expressions::StartsWith("name", "John");
  EXPECT_EQ(starts_with_pred->ToString(), "ref(name=\"name\") startsWith \"John\"");
}

TEST_F(PredicateTest, UnboundPredicateNegate) {
  auto equal_pred = Expressions::Equal("age", Literal::Int(25));
  auto negated_result = equal_pred->Negate();
  ASSERT_THAT(negated_result, IsOk());

  auto negated_pred = negated_result.value();
  EXPECT_EQ(negated_pred->op(), Expression::Operation::kNotEq);

  auto is_null_pred = Expressions::IsNull("name");
  auto negated_null_result = is_null_pred->Negate();
  ASSERT_THAT(negated_null_result, IsOk());

  auto negated_null_pred = negated_null_result.value();
  EXPECT_EQ(negated_null_pred->op(), Expression::Operation::kNotNull);

  auto in_pred = Expressions::In("age", {Literal::Int(10), Literal::Int(20)});
  auto negated_in_result = in_pred->Negate();
  ASSERT_THAT(negated_in_result, IsOk());

  auto negated_in_pred = negated_in_result.value();
  EXPECT_EQ(negated_in_pred->op(), Expression::Operation::kNotIn);
}

TEST_F(PredicateTest, UnboundPredicateBindUnary) {
  auto is_null_pred = Expressions::IsNull("name");
  auto bound_result = is_null_pred->Bind(*schema_, /*case_sensitive=*/true);
  ASSERT_THAT(bound_result, IsOk());

  auto bound_pred = bound_result.value();
  EXPECT_EQ(bound_pred->op(), Expression::Operation::kIsNull);

  // Test NOT NULL on non-nullable field - should return AlwaysTrue
  auto not_null_required = Expressions::NotNull("age");  // age is required
  auto bound_not_null_result = not_null_required->Bind(*schema_, /*case_sensitive=*/true);
  ASSERT_THAT(bound_not_null_result, IsOk());

  auto bound_not_null = bound_not_null_result.value();
  EXPECT_EQ(bound_not_null->op(), Expression::Operation::kTrue);

  // Test IS NULL on non-nullable field - should return AlwaysFalse
  auto is_null_required = Expressions::IsNull("age");  // age is required
  auto bound_is_null_result = is_null_required->Bind(*schema_, /*case_sensitive=*/true);
  ASSERT_THAT(bound_is_null_result, IsOk());

  auto bound_is_null = bound_is_null_result.value();
  EXPECT_EQ(bound_is_null->op(), Expression::Operation::kFalse);
}

TEST_F(PredicateTest, UnboundPredicateBindLiteral) {
  auto equal_pred = Expressions::Equal("age", Literal::Int(25));
  auto bound_result = equal_pred->Bind(*schema_, /*case_sensitive=*/true);
  ASSERT_THAT(bound_result, IsOk());

  auto bound_pred = bound_result.value();
  EXPECT_EQ(bound_pred->op(), Expression::Operation::kEq);

  // Test binding with type conversion
  auto equal_long_pred =
      Expressions::Equal("id", Literal::Int(123));  // int to long conversion
  auto bound_long_result = equal_long_pred->Bind(*schema_, /*case_sensitive=*/true);
  ASSERT_THAT(bound_long_result, IsOk());

  auto bound_long_pred = bound_long_result.value();
  EXPECT_EQ(bound_long_pred->op(), Expression::Operation::kEq);
}

TEST_F(PredicateTest, UnboundPredicateBindIn) {
  // Test IN operation with single value (should become equality)
  auto in_single = Expressions::In("age", {Literal::Int(25)});
  auto bound_single_result = in_single->Bind(*schema_, /*case_sensitive=*/true);
  ASSERT_THAT(bound_single_result, IsOk());

  auto bound_single = bound_single_result.value();
  EXPECT_EQ(bound_single->op(), Expression::Operation::kEq);

  // Test NOT IN operation with single value (should become inequality)
  auto not_in_single = Expressions::NotIn("age", {Literal::Int(25)});
  auto bound_not_single_result = not_in_single->Bind(*schema_, /*case_sensitive=*/true);
  ASSERT_THAT(bound_not_single_result, IsOk());

  auto bound_not_single = bound_not_single_result.value();
  EXPECT_EQ(bound_not_single->op(), Expression::Operation::kNotEq);

  // Test IN operation with multiple values (should stay as IN)
  auto in_multi = Expressions::In("age", {Literal::Int(25), Literal::Int(30)});
  auto bound_multi_result = in_multi->Bind(*schema_, true);
  ASSERT_THAT(bound_multi_result, IsOk());

  auto bound_multi = bound_multi_result.value();
  EXPECT_EQ(bound_multi->op(), Expression::Operation::kIn);
}

TEST_F(PredicateTest, FloatingPointNaNPredicates) {
  auto is_nan_float = Expressions::IsNaN("salary");  // salary is float64
  auto bound_nan_result = is_nan_float->Bind(*schema_, /*case_sensitive=*/true);
  ASSERT_THAT(bound_nan_result, IsOk());

  auto bound_nan = bound_nan_result.value();
  EXPECT_EQ(bound_nan->op(), Expression::Operation::kIsNan);

  auto is_nan_int = Expressions::IsNaN("age");  // age is int32
  auto bound_nan_int_result = is_nan_int->Bind(*schema_, /*case_sensitive=*/true);
  EXPECT_THAT(bound_nan_int_result, IsError(ErrorKind::kInvalidExpression));
}

TEST_F(PredicateTest, StringStartsWithPredicates) {
  auto starts_with = Expressions::StartsWith("name", "John");  // name is string
  auto bound_starts_result = starts_with->Bind(*schema_, /*case_sensitive=*/true);
  ASSERT_THAT(bound_starts_result, IsOk());

  auto bound_starts = bound_starts_result.value();
  EXPECT_EQ(bound_starts->op(), Expression::Operation::kStartsWith);

  auto starts_with_int = Expressions::StartsWith("age", "test");  // age is int32
  auto bound_starts_int_result = starts_with_int->Bind(*schema_, /*case_sensitive=*/true);
  EXPECT_THAT(bound_starts_int_result, IsError(ErrorKind::kInvalidExpression));
}

TEST_F(PredicateTest, LiteralConversionEdgeCases) {
  auto large_value_lt =
      Expressions::LessThan("age", Literal::Long(std::numeric_limits<int64_t>::max()));
  auto bound_large_result = large_value_lt->Bind(*schema_, /*case_sensitive=*/true);
  ASSERT_THAT(bound_large_result, IsOk());

  auto bound_large = bound_large_result.value();
  EXPECT_EQ(bound_large->op(), Expression::Operation::kTrue);
}

TEST_F(PredicateTest, ComplexExpressionCombinations) {
  auto eq_pred = Expressions::Equal("age", Literal::Int(25));
  auto null_pred = Expressions::IsNull("name");
  auto in_pred =
      Expressions::In("id", {Literal::Long(1), Literal::Long(2), Literal::Long(3)});

  // Test AND combinations
  auto and_eq_null = Expressions::And(eq_pred, null_pred);
  EXPECT_EQ(and_eq_null->op(), Expression::Operation::kAnd);

  auto and_eq_in = Expressions::And(eq_pred, in_pred);
  EXPECT_EQ(and_eq_in->op(), Expression::Operation::kAnd);

  // Test OR combinations
  auto or_null_in = Expressions::Or(null_pred, in_pred);
  EXPECT_EQ(or_null_in->op(), Expression::Operation::kOr);

  // Test nested combinations
  auto nested = Expressions::And(and_eq_null, or_null_in);
  EXPECT_EQ(nested->op(), Expression::Operation::kAnd);
}

TEST_F(PredicateTest, BoundUnaryPredicateNegate) {
  auto is_null_pred = Expressions::IsNull("name");
  auto bound_null = is_null_pred->Bind(*schema_, /*case_sensitive=*/true).value();

  auto negated_result = bound_null->Negate();
  ASSERT_THAT(negated_result, IsOk());
  auto negated = negated_result.value();
  EXPECT_EQ(negated->op(), Expression::Operation::kNotNull);

  // Double negation should return the original predicate
  auto double_neg_result = negated->Negate();
  ASSERT_THAT(double_neg_result, IsOk());
  auto double_neg = double_neg_result.value();
  EXPECT_EQ(double_neg->op(), Expression::Operation::kIsNull);
}

TEST_F(PredicateTest, BoundUnaryPredicateEquals) {
  auto is_null_name1 = Expressions::IsNull("name");
  auto is_null_name2 = Expressions::IsNull("name");
  auto is_null_age = Expressions::IsNull("age");
  auto not_null_name = Expressions::NotNull("name");

  auto bound_null1 = is_null_name1->Bind(*schema_, true).value();
  auto bound_null2 = is_null_name2->Bind(*schema_, true).value();
  auto bound_null_age = is_null_age->Bind(*schema_, true).value();
  auto bound_not_null = not_null_name->Bind(*schema_, true).value();

  // Same predicate should be equal
  EXPECT_TRUE(bound_null1->Equals(*bound_null2));
  EXPECT_TRUE(bound_null2->Equals(*bound_null1));

  // Different fields should not be equal
  EXPECT_FALSE(bound_null1->Equals(*bound_null_age));

  // Different operations should not be equal
  EXPECT_FALSE(bound_null1->Equals(*bound_not_null));
}

TEST_F(PredicateTest, BoundLiteralPredicateNegate) {
  auto eq_pred = Expressions::Equal("age", Literal::Int(25));
  auto bound_eq = eq_pred->Bind(*schema_, true).value();

  auto negated_result = bound_eq->Negate();
  ASSERT_THAT(negated_result, IsOk());

  auto negated = negated_result.value();
  EXPECT_EQ(negated->op(), Expression::Operation::kNotEq);

  // Test less than negation
  auto lt_pred = Expressions::LessThan("age", Literal::Int(30));
  auto bound_lt = lt_pred->Bind(*schema_, true).value();
  auto neg_lt_result = bound_lt->Negate();
  ASSERT_THAT(neg_lt_result, IsOk());
  EXPECT_EQ(neg_lt_result.value()->op(), Expression::Operation::kGtEq);
}

TEST_F(PredicateTest, BoundLiteralPredicateEquals) {
  auto eq1 = Expressions::Equal("age", Literal::Int(25));
  auto eq2 = Expressions::Equal("age", Literal::Int(25));
  auto eq3 = Expressions::Equal("age", Literal::Int(30));
  auto neq = Expressions::NotEqual("age", Literal::Int(25));

  auto bound_eq1 = eq1->Bind(*schema_, true).value();
  auto bound_eq2 = eq2->Bind(*schema_, true).value();
  auto bound_eq3 = eq3->Bind(*schema_, true).value();
  auto bound_neq = neq->Bind(*schema_, true).value();

  // Same predicate should be equal
  EXPECT_TRUE(bound_eq1->Equals(*bound_eq2));

  // Different literal values should not be equal
  EXPECT_FALSE(bound_eq1->Equals(*bound_eq3));

  // Different operations should not be equal
  EXPECT_FALSE(bound_eq1->Equals(*bound_neq));
}

TEST_F(PredicateTest, BoundLiteralPredicateIntegerEquivalence) {
  // Test that < 6 is equivalent to <= 5
  auto lt_6 = Expressions::LessThan("age", Literal::Int(6));
  auto lte_5 = Expressions::LessThanOrEqual("age", Literal::Int(5));
  auto bound_lt = lt_6->Bind(*schema_, true).value();
  auto bound_lte = lte_5->Bind(*schema_, true).value();
  EXPECT_TRUE(bound_lt->Equals(*bound_lte));
  EXPECT_TRUE(bound_lte->Equals(*bound_lt));

  // Test that > 5 is equivalent to >= 6
  auto gt_5 = Expressions::GreaterThan("age", Literal::Int(5));
  auto gte_6 = Expressions::GreaterThanOrEqual("age", Literal::Int(6));
  auto bound_gt = gt_5->Bind(*schema_, true).value();
  auto bound_gte = gte_6->Bind(*schema_, true).value();
  EXPECT_TRUE(bound_gt->Equals(*bound_gte));
  EXPECT_TRUE(bound_gte->Equals(*bound_gt));

  // Test that < 6 is not equivalent to <= 6
  auto lte_6 = Expressions::LessThanOrEqual("age", Literal::Int(6));
  auto bound_lte_6 = lte_6->Bind(*schema_, true).value();
  EXPECT_FALSE(bound_lt->Equals(*bound_lte_6));
}

TEST_F(PredicateTest, BoundSetPredicateToString) {
  auto in_pred =
      Expressions::In("age", {Literal::Int(10), Literal::Int(20), Literal::Int(30)});
  auto bound_in = in_pred->Bind(*schema_, true).value();

  auto str = bound_in->ToString();
  // The set order might vary, but should contain the key elements
  // BoundReference uses field_id in ToString, so check for id=3 (age field)
  EXPECT_TRUE(str.find("id=3") != std::string::npos);
  EXPECT_TRUE(str.find("in") != std::string::npos);

  auto not_in_pred =
      Expressions::NotIn("name", {Literal::String("a"), Literal::String("b")});
  auto bound_not_in = not_in_pred->Bind(*schema_, true).value();

  auto not_in_str = bound_not_in->ToString();
  // Check for id=2 (name field)
  EXPECT_TRUE(not_in_str.find("id=2") != std::string::npos);
  EXPECT_TRUE(not_in_str.find("not in") != std::string::npos);
}

TEST_F(PredicateTest, BoundSetPredicateNegate) {
  auto in_pred = Expressions::In("age", {Literal::Int(10), Literal::Int(20)});
  auto bound_in = in_pred->Bind(*schema_, true).value();

  auto negated_result = bound_in->Negate();
  ASSERT_THAT(negated_result, IsOk());

  auto negated = negated_result.value();
  EXPECT_EQ(negated->op(), Expression::Operation::kNotIn);

  // Test double negation
  auto double_neg_result = negated->Negate();
  ASSERT_THAT(double_neg_result, IsOk());
  EXPECT_EQ(double_neg_result.value()->op(), Expression::Operation::kIn);
}

TEST_F(PredicateTest, BoundSetPredicateEquals) {
  auto in1 = Expressions::In("age", {Literal::Int(10), Literal::Int(20)});
  auto in2 =
      Expressions::In("age", {Literal::Int(20), Literal::Int(10)});  // Different order
  auto in3 =
      Expressions::In("age", {Literal::Int(10), Literal::Int(30)});  // Different values

  auto bound_in1 = in1->Bind(*schema_, /*case_sensitive=*/true).value();
  auto bound_in2 = in2->Bind(*schema_, /*case_sensitive=*/true).value();
  auto bound_in3 = in3->Bind(*schema_, /*case_sensitive=*/true).value();

  // Same values in different order should be equal (unordered_set)
  EXPECT_TRUE(bound_in1->Equals(*bound_in2));
  EXPECT_TRUE(bound_in2->Equals(*bound_in1));

  // Different values should not be equal
  EXPECT_FALSE(bound_in1->Equals(*bound_in3));
}

namespace {

std::shared_ptr<BoundPredicate> AssertAndCastToBoundPredicate(
    std::shared_ptr<Expression> expr) {
  auto bound_pred = std::dynamic_pointer_cast<BoundPredicate>(expr);
  EXPECT_NE(bound_pred, nullptr) << "Expected a BoundPredicate, got " << expr->ToString();
  return bound_pred;
}

}  // namespace

TEST_F(PredicateTest, BoundUnaryPredicateTestIsNull) {
  ICEBERG_ASSIGN_OR_THROW(auto is_null_pred, Expressions::IsNull("name")->Bind(
                                                 *schema_, /*case_sensitive=*/true));
  auto bound_pred = AssertAndCastToBoundPredicate(is_null_pred);
  EXPECT_THAT(bound_pred->Test(Literal::Null(string())), HasValue(testing::Eq(true)));
  EXPECT_THAT(bound_pred->Test(Literal::String("test")), HasValue(testing::Eq(false)));
}

TEST_F(PredicateTest, BoundUnaryPredicateTestNotNull) {
  ICEBERG_ASSIGN_OR_THROW(auto not_null_pred, Expressions::NotNull("name")->Bind(
                                                  *schema_, /*case_sensitive=*/true));
  auto bound_pred = AssertAndCastToBoundPredicate(not_null_pred);
  EXPECT_THAT(bound_pred->Test(Literal::String("test")), HasValue(testing::Eq(true)));
  EXPECT_THAT(bound_pred->Test(Literal::Null(string())), HasValue(testing::Eq(false)));
}

TEST_F(PredicateTest, BoundUnaryPredicateTestIsNaN) {
  ICEBERG_ASSIGN_OR_THROW(auto is_nan_pred, Expressions::IsNaN("salary")->Bind(
                                                *schema_, /*case_sensitive=*/true));
  auto bound_pred = AssertAndCastToBoundPredicate(is_nan_pred);

  // Test with NaN values
  EXPECT_THAT(bound_pred->Test(Literal::Float(std::numeric_limits<float>::quiet_NaN())),
              HasValue(testing::Eq(true)));
  EXPECT_THAT(bound_pred->Test(Literal::Double(std::numeric_limits<double>::quiet_NaN())),
              HasValue(testing::Eq(true)));

  // Test with regular values
  EXPECT_THAT(bound_pred->Test(Literal::Float(3.14f)), HasValue(testing::Eq(false)));
  EXPECT_THAT(bound_pred->Test(Literal::Double(2.718)), HasValue(testing::Eq(false)));

  // Test with infinity
  EXPECT_THAT(bound_pred->Test(Literal::Float(std::numeric_limits<float>::infinity())),
              HasValue(testing::Eq(false)));
}

TEST_F(PredicateTest, BoundUnaryPredicateTestNotNaN) {
  ICEBERG_ASSIGN_OR_THROW(auto not_nan_pred, Expressions::NotNaN("salary")->Bind(
                                                 *schema_, /*case_sensitive=*/true));
  auto bound_pred = AssertAndCastToBoundPredicate(not_nan_pred);

  // Test with regular values
  EXPECT_THAT(bound_pred->Test(Literal::Double(100.5)), HasValue(testing::Eq(true)));

  // Test with NaN
  EXPECT_THAT(bound_pred->Test(Literal::Double(std::numeric_limits<double>::quiet_NaN())),
              HasValue(testing::Eq(false)));

  // Test with infinity (should be true as infinity is not NaN)
  EXPECT_THAT(bound_pred->Test(Literal::Double(std::numeric_limits<double>::infinity())),
              HasValue(testing::Eq(true)));
}

TEST_F(PredicateTest, BoundLiteralPredicateTestComparison) {
  // Test less than
  ICEBERG_ASSIGN_OR_THROW(auto lt_pred, Expressions::LessThan("age", Literal::Int(30))
                                            ->Bind(*schema_, /*case_sensitive=*/true));
  auto bound_lt = AssertAndCastToBoundPredicate(lt_pred);
  EXPECT_THAT(bound_lt->Test(Literal::Int(20)), HasValue(testing::Eq(true)));
  EXPECT_THAT(bound_lt->Test(Literal::Int(30)), HasValue(testing::Eq(false)));
  EXPECT_THAT(bound_lt->Test(Literal::Int(40)), HasValue(testing::Eq(false)));

  // Test less than or equal
  ICEBERG_ASSIGN_OR_THROW(auto lte_pred,
                          Expressions::LessThanOrEqual("age", Literal::Int(30))
                              ->Bind(*schema_, /*case_sensitive=*/true));
  auto bound_lte = AssertAndCastToBoundPredicate(lte_pred);
  EXPECT_THAT(bound_lte->Test(Literal::Int(20)), HasValue(testing::Eq(true)));
  EXPECT_THAT(bound_lte->Test(Literal::Int(30)), HasValue(testing::Eq(true)));
  EXPECT_THAT(bound_lte->Test(Literal::Int(40)), HasValue(testing::Eq(false)));

  // Test greater than
  ICEBERG_ASSIGN_OR_THROW(auto gt_pred, Expressions::GreaterThan("age", Literal::Int(30))
                                            ->Bind(*schema_, /*case_sensitive=*/true));
  auto bound_gt = AssertAndCastToBoundPredicate(gt_pred);
  EXPECT_THAT(bound_gt->Test(Literal::Int(20)), HasValue(testing::Eq(false)));
  EXPECT_THAT(bound_gt->Test(Literal::Int(30)), HasValue(testing::Eq(false)));
  EXPECT_THAT(bound_gt->Test(Literal::Int(40)), HasValue(testing::Eq(true)));

  // Test greater than or equal
  ICEBERG_ASSIGN_OR_THROW(auto gte_pred,
                          Expressions::GreaterThanOrEqual("age", Literal::Int(30))
                              ->Bind(*schema_, /*case_sensitive=*/true));
  auto bound_gte = AssertAndCastToBoundPredicate(gte_pred);
  EXPECT_THAT(bound_gte->Test(Literal::Int(20)), HasValue(testing::Eq(false)));
  EXPECT_THAT(bound_gte->Test(Literal::Int(30)), HasValue(testing::Eq(true)));
  EXPECT_THAT(bound_gte->Test(Literal::Int(40)), HasValue(testing::Eq(true)));
}

TEST_F(PredicateTest, BoundLiteralPredicateTestEquality) {
  // Test equal
  ICEBERG_ASSIGN_OR_THROW(auto eq_pred, Expressions::Equal("age", Literal::Int(25))
                                            ->Bind(*schema_, /*case_sensitive=*/true));
  auto bound_eq = AssertAndCastToBoundPredicate(eq_pred);
  EXPECT_THAT(bound_eq->Test(Literal::Int(25)), HasValue(testing::Eq(true)));
  EXPECT_THAT(bound_eq->Test(Literal::Int(26)), HasValue(testing::Eq(false)));
  EXPECT_THAT(bound_eq->Test(Literal::Int(24)), HasValue(testing::Eq(false)));

  // Test not equal
  ICEBERG_ASSIGN_OR_THROW(auto neq_pred, Expressions::NotEqual("age", Literal::Int(25))
                                             ->Bind(*schema_, /*case_sensitive=*/true));
  auto bound_neq = AssertAndCastToBoundPredicate(neq_pred);
  EXPECT_THAT(bound_neq->Test(Literal::Int(25)), HasValue(testing::Eq(false)));
  EXPECT_THAT(bound_neq->Test(Literal::Int(26)), HasValue(testing::Eq(true)));
  EXPECT_THAT(bound_neq->Test(Literal::Int(24)), HasValue(testing::Eq(true)));
}

TEST_F(PredicateTest, BoundLiteralPredicateTestWithDifferentTypes) {
  // Test with double
  ICEBERG_ASSIGN_OR_THROW(auto gt_pred,
                          Expressions::GreaterThan("salary", Literal::Double(50000.0))
                              ->Bind(*schema_, /*case_sensitive=*/true));
  auto bound_double = AssertAndCastToBoundPredicate(gt_pred);
  EXPECT_THAT(bound_double->Test(Literal::Double(60000.0)), HasValue(testing::Eq(true)));
  EXPECT_THAT(bound_double->Test(Literal::Double(40000.0)), HasValue(testing::Eq(false)));
  EXPECT_THAT(bound_double->Test(Literal::Double(50000.0)), HasValue(testing::Eq(false)));

  // Test with string
  ICEBERG_ASSIGN_OR_THROW(auto str_eq_pred,
                          Expressions::Equal("name", Literal::String("Alice"))
                              ->Bind(*schema_, /*case_sensitive=*/true));
  auto bound_string = AssertAndCastToBoundPredicate(str_eq_pred);
  EXPECT_THAT(bound_string->Test(Literal::String("Alice")), HasValue(testing::Eq(true)));
  EXPECT_THAT(bound_string->Test(Literal::String("Bob")), HasValue(testing::Eq(false)));
  EXPECT_THAT(bound_string->Test(Literal::String("alice")),
              HasValue(testing::Eq(false)));  // Case sensitive

  // Test with boolean
  ICEBERG_ASSIGN_OR_THROW(auto bool_eq_pred,
                          Expressions::Equal("active", Literal::Boolean(true))
                              ->Bind(*schema_, /*case_sensitive=*/true));
  auto bound_bool = AssertAndCastToBoundPredicate(bool_eq_pred);
  EXPECT_THAT(bound_bool->Test(Literal::Boolean(true)), HasValue(testing::Eq(true)));
  EXPECT_THAT(bound_bool->Test(Literal::Boolean(false)), HasValue(testing::Eq(false)));
}

TEST_F(PredicateTest, BoundLiteralPredicateTestStartsWith) {
  ICEBERG_ASSIGN_OR_THROW(
      auto starts_with_pred,
      Expressions::StartsWith("name", "Jo")->Bind(*schema_, /*case_sensitive=*/true));
  auto bound_pred = AssertAndCastToBoundPredicate(starts_with_pred);

  // Test strings that start with "Jo"
  EXPECT_THAT(bound_pred->Test(Literal::String("John")), HasValue(testing::Eq(true)));
  EXPECT_THAT(bound_pred->Test(Literal::String("Joe")), HasValue(testing::Eq(true)));
  EXPECT_THAT(bound_pred->Test(Literal::String("Jo")), HasValue(testing::Eq(true)));

  // Test strings that don't start with "Jo"
  EXPECT_THAT(bound_pred->Test(Literal::String("Alice")), HasValue(testing::Eq(false)));
  EXPECT_THAT(bound_pred->Test(Literal::String("Bob")), HasValue(testing::Eq(false)));
  EXPECT_THAT(bound_pred->Test(Literal::String("")), HasValue(testing::Eq(false)));

  // Test empty prefix
  ICEBERG_ASSIGN_OR_THROW(
      auto empty_prefix_pred,
      Expressions::StartsWith("name", "")->Bind(*schema_, /*case_sensitive=*/true));
  auto bound_empty = AssertAndCastToBoundPredicate(empty_prefix_pred);

  // All strings should start with empty prefix
  EXPECT_THAT(bound_empty->Test(Literal::String("test")), HasValue(testing::Eq(true)));
  EXPECT_THAT(bound_empty->Test(Literal::String("")), HasValue(testing::Eq(true)));
}

TEST_F(PredicateTest, BoundLiteralPredicateTestNotStartsWith) {
  ICEBERG_ASSIGN_OR_THROW(
      auto not_starts_with_pred,
      Expressions::NotStartsWith("name", "Jo")->Bind(*schema_, /*case_sensitive=*/true));
  auto bound_pred = AssertAndCastToBoundPredicate(not_starts_with_pred);

  // Test strings that don't start with "Jo"
  EXPECT_THAT(bound_pred->Test(Literal::String("Alice")), HasValue(testing::Eq(true)));
  EXPECT_THAT(bound_pred->Test(Literal::String("Bob")), HasValue(testing::Eq(true)));
  EXPECT_THAT(bound_pred->Test(Literal::String("")), HasValue(testing::Eq(true)));

  // Test strings that start with "Jo"
  EXPECT_THAT(bound_pred->Test(Literal::String("John")), HasValue(testing::Eq(false)));
  EXPECT_THAT(bound_pred->Test(Literal::String("Joe")), HasValue(testing::Eq(false)));
  EXPECT_THAT(bound_pred->Test(Literal::String("Jo")), HasValue(testing::Eq(false)));
}

TEST_F(PredicateTest, BoundSetPredicateTestIn) {
  ICEBERG_ASSIGN_OR_THROW(
      auto in_pred,
      Expressions::In("age", {Literal::Int(10), Literal::Int(20), Literal::Int(30)})
          ->Bind(*schema_, /*case_sensitive=*/true));
  auto bound_pred = AssertAndCastToBoundPredicate(in_pred);

  // Test values in the set
  EXPECT_THAT(bound_pred->Test(Literal::Int(10)), HasValue(testing::Eq(true)));
  EXPECT_THAT(bound_pred->Test(Literal::Int(20)), HasValue(testing::Eq(true)));
  EXPECT_THAT(bound_pred->Test(Literal::Int(30)), HasValue(testing::Eq(true)));

  // Test values not in the set
  EXPECT_THAT(bound_pred->Test(Literal::Int(15)), HasValue(testing::Eq(false)));
  EXPECT_THAT(bound_pred->Test(Literal::Int(40)), HasValue(testing::Eq(false)));
  EXPECT_THAT(bound_pred->Test(Literal::Int(0)), HasValue(testing::Eq(false)));
}

TEST_F(PredicateTest, BoundSetPredicateTestNotIn) {
  ICEBERG_ASSIGN_OR_THROW(
      auto not_in_pred,
      Expressions::NotIn("age", {Literal::Int(10), Literal::Int(20), Literal::Int(30)})
          ->Bind(*schema_, /*case_sensitive=*/true));
  auto bound_pred = AssertAndCastToBoundPredicate(not_in_pred);

  // Test values not in the set
  EXPECT_THAT(bound_pred->Test(Literal::Int(15)), HasValue(testing::Eq(true)));
  EXPECT_THAT(bound_pred->Test(Literal::Int(40)), HasValue(testing::Eq(true)));
  EXPECT_THAT(bound_pred->Test(Literal::Int(0)), HasValue(testing::Eq(true)));

  // Test values in the set
  EXPECT_THAT(bound_pred->Test(Literal::Int(10)), HasValue(testing::Eq(false)));
  EXPECT_THAT(bound_pred->Test(Literal::Int(20)), HasValue(testing::Eq(false)));
  EXPECT_THAT(bound_pred->Test(Literal::Int(30)), HasValue(testing::Eq(false)));
}

TEST_F(PredicateTest, BoundSetPredicateTestWithStrings) {
  ICEBERG_ASSIGN_OR_THROW(
      auto in_pred,
      Expressions::In("name", {Literal::String("Alice"), Literal::String("Bob"),
                               Literal::String("Charlie")})
          ->Bind(*schema_, /*case_sensitive=*/true));
  auto bound_pred = AssertAndCastToBoundPredicate(in_pred);

  // Test strings in the set
  EXPECT_THAT(bound_pred->Test(Literal::String("Alice")), HasValue(testing::Eq(true)));
  EXPECT_THAT(bound_pred->Test(Literal::String("Bob")), HasValue(testing::Eq(true)));
  EXPECT_THAT(bound_pred->Test(Literal::String("Charlie")), HasValue(testing::Eq(true)));

  // Test strings not in the set
  EXPECT_THAT(bound_pred->Test(Literal::String("David")), HasValue(testing::Eq(false)));
  EXPECT_THAT(bound_pred->Test(Literal::String("alice")),
              HasValue(testing::Eq(false)));  // Case sensitive
  EXPECT_THAT(bound_pred->Test(Literal::String("")), HasValue(testing::Eq(false)));
}

TEST_F(PredicateTest, BoundSetPredicateTestWithLongs) {
  ICEBERG_ASSIGN_OR_THROW(auto in_pred,
                          Expressions::In("id", {Literal::Long(100L), Literal::Long(200L),
                                                 Literal::Long(300L)})
                              ->Bind(*schema_, /*case_sensitive=*/true));
  auto bound_pred = AssertAndCastToBoundPredicate(in_pred);

  // Test longs in the set
  EXPECT_THAT(bound_pred->Test(Literal::Long(100L)), HasValue(testing::Eq(true)));
  EXPECT_THAT(bound_pred->Test(Literal::Long(200L)), HasValue(testing::Eq(true)));
  EXPECT_THAT(bound_pred->Test(Literal::Long(300L)), HasValue(testing::Eq(true)));

  // Test longs not in the set
  EXPECT_THAT(bound_pred->Test(Literal::Long(150L)), HasValue(testing::Eq(false)));
  EXPECT_THAT(bound_pred->Test(Literal::Long(400L)), HasValue(testing::Eq(false)));
}

TEST_F(PredicateTest, BoundSetPredicateTestSingleLiteral) {
  ICEBERG_ASSIGN_OR_THROW(auto in_pred, Expressions::In("age", {Literal::Int(42)})
                                            ->Bind(*schema_, /*case_sensitive=*/true));

  // Single element IN becomes Equal
  EXPECT_EQ(in_pred->op(), Expression::Operation::kEq);
  auto bound_literal = AssertAndCastToBoundPredicate(in_pred);
  EXPECT_THAT(bound_literal->Test(Literal::Int(42)), HasValue(testing::Eq(true)));
  EXPECT_THAT(bound_literal->Test(Literal::Int(41)), HasValue(testing::Eq(false)));
}

}  // namespace iceberg
