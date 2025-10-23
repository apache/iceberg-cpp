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

#include "iceberg/expression/expressions.h"
#include "iceberg/schema.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"

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

}  // namespace iceberg
