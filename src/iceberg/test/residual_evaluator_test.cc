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

#include "iceberg/expression/residual_evaluator.h"

#include <cmath>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/expression/expressions.h"
#include "iceberg/expression/literal.h"
#include "iceberg/expression/predicate.h"
#include "iceberg/partition_field.h"
#include "iceberg/partition_spec.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/schema.h"
#include "iceberg/test/matchers.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"

namespace iceberg {

class ResidualEvaluatorTest : public ::testing::Test {
 protected:
  void SetUp() override {}

  // Helper function to assert residual operation
  void AssertResidualOp(const std::shared_ptr<PartitionSpec>& spec,
                        const std::shared_ptr<Schema>& schema,
                        const std::shared_ptr<Expression>& pred,
                        const Literal& partition_value,
                        Expression::Operation expected_op) {
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           ResidualEvaluator::Make(pred, *spec, *schema, true));
    PartitionValues partition_data(partition_value);
    ICEBERG_UNWRAP_OR_FAIL(auto residual, evaluator->ResidualFor(partition_data));
    EXPECT_EQ(residual->op(), expected_op);
  }

  // Helper function to assert residual is the same as original predicate
  void AssertResidualPredicate(const std::shared_ptr<PartitionSpec>& spec,
                               const std::shared_ptr<Schema>& schema,
                               const std::shared_ptr<Expression>& pred,
                               const Literal& partition_value) {
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                           ResidualEvaluator::Make(pred, *spec, *schema, true));
    PartitionValues partition_data(partition_value);
    ICEBERG_UNWRAP_OR_FAIL(auto residual, evaluator->ResidualFor(partition_data));
    ASSERT_TRUE(residual->is_unbound_predicate());
    auto unbound_residual = std::dynamic_pointer_cast<UnboundPredicate>(residual);
    ASSERT_NE(unbound_residual, nullptr);
    auto unbound_original = std::dynamic_pointer_cast<UnboundPredicate>(pred);
    ASSERT_NE(unbound_original, nullptr);
    EXPECT_EQ(unbound_residual->op(), unbound_original->op());
    EXPECT_EQ(unbound_residual->reference()->name(),
              unbound_original->reference()->name());
    // Check literal value
    auto residual_impl =
        std::dynamic_pointer_cast<UnboundPredicateImpl<BoundReference>>(unbound_residual);
    auto original_impl =
        std::dynamic_pointer_cast<UnboundPredicateImpl<BoundReference>>(unbound_original);
    ASSERT_NE(residual_impl, nullptr);
    ASSERT_NE(original_impl, nullptr);
    ASSERT_EQ(residual_impl->literals().size(), original_impl->literals().size());
    if (!residual_impl->literals().empty()) {
      EXPECT_EQ(residual_impl->literals()[0].value(),
                original_impl->literals()[0].value());
    }
  }
};

TEST_F(ResidualEvaluatorTest, IdentityTransformResiduals) {
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(50, "dateint", int32()),
                               SchemaField::MakeOptional(51, "hour", int32())});

  auto identity_transform = Transform::Identity();
  PartitionField pt_field(50, 1000, "dateint", identity_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec_unique,
                         PartitionSpec::Make(*schema, 0, {pt_field}, false));
  auto spec = std::shared_ptr<PartitionSpec>(spec_unique.release());

  // Create expression: (dateint < 20170815 AND dateint > 20170801) OR
  //                     (dateint == 20170815 AND hour < 12) OR
  //                     (dateint == 20170801 AND hour > 11)
  auto expr = Expressions::Or(
      Expressions::Or(
          Expressions::And(Expressions::LessThan("dateint", Literal::Int(20170815)),
                           Expressions::GreaterThan("dateint", Literal::Int(20170801))),
          Expressions::And(Expressions::Equal("dateint", Literal::Int(20170815)),
                           Expressions::LessThan("hour", Literal::Int(12)))),
      Expressions::And(Expressions::Equal("dateint", Literal::Int(20170801)),
                       Expressions::GreaterThan("hour", Literal::Int(11))));

  ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                         ResidualEvaluator::Make(expr, *spec, *schema, true));

  // Equal to the upper date bound
  PartitionValues partition_data1(Literal::Int(20170815));
  ICEBERG_UNWRAP_OR_FAIL(auto residual1, evaluator->ResidualFor(partition_data1));
  ASSERT_TRUE(residual1->is_unbound_predicate());
  auto unbound1 = std::dynamic_pointer_cast<UnboundPredicate>(residual1);
  ASSERT_NE(unbound1, nullptr);
  EXPECT_EQ(unbound1->op(), Expression::Operation::kLt);
  EXPECT_EQ(unbound1->reference()->name(), "hour");
  // Access literal through literals() span
  auto unbound1_impl =
      std::dynamic_pointer_cast<UnboundPredicateImpl<BoundReference>>(unbound1);
  ASSERT_NE(unbound1_impl, nullptr);
  ASSERT_EQ(unbound1_impl->literals().size(), 1);
  EXPECT_EQ(unbound1_impl->literals()[0].value(), Literal::Int(12).value());

  // Equal to the lower date bound
  PartitionValues partition_data2(Literal::Int(20170801));
  ICEBERG_UNWRAP_OR_FAIL(auto residual2, evaluator->ResidualFor(partition_data2));
  ASSERT_TRUE(residual2->is_unbound_predicate());
  auto unbound2 = std::dynamic_pointer_cast<UnboundPredicate>(residual2);
  ASSERT_NE(unbound2, nullptr);
  EXPECT_EQ(unbound2->op(), Expression::Operation::kGt);
  EXPECT_EQ(unbound2->reference()->name(), "hour");
  // Access literal through literals() span
  auto unbound2_impl =
      std::dynamic_pointer_cast<UnboundPredicateImpl<BoundReference>>(unbound2);
  ASSERT_NE(unbound2_impl, nullptr);
  ASSERT_EQ(unbound2_impl->literals().size(), 1);
  EXPECT_EQ(unbound2_impl->literals()[0].value(), Literal::Int(11).value());

  // Inside the date range
  PartitionValues partition_data3(Literal::Int(20170812));
  ICEBERG_UNWRAP_OR_FAIL(auto residual3, evaluator->ResidualFor(partition_data3));
  EXPECT_EQ(residual3->op(), Expression::Operation::kTrue);

  // Outside the date range
  PartitionValues partition_data4(Literal::Int(20170817));
  ICEBERG_UNWRAP_OR_FAIL(auto residual4, evaluator->ResidualFor(partition_data4));
  EXPECT_EQ(residual4->op(), Expression::Operation::kFalse);
}

TEST_F(ResidualEvaluatorTest, CaseInsensitiveIdentityTransformResiduals) {
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(50, "dateint", int32()),
                               SchemaField::MakeOptional(51, "hour", int32())});

  auto identity_transform = Transform::Identity();
  PartitionField pt_field(50, 1000, "dateint", identity_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec_unique,
                         PartitionSpec::Make(*schema, 0, {pt_field}, false));
  auto spec = std::shared_ptr<PartitionSpec>(spec_unique.release());

  // Create expression with mixed case field names
  auto expr = Expressions::Or(
      Expressions::Or(
          Expressions::And(Expressions::LessThan("DATEINT", Literal::Int(20170815)),
                           Expressions::GreaterThan("dateint", Literal::Int(20170801))),
          Expressions::And(Expressions::Equal("dateint", Literal::Int(20170815)),
                           Expressions::LessThan("HOUR", Literal::Int(12)))),
      Expressions::And(Expressions::Equal("DateInt", Literal::Int(20170801)),
                       Expressions::GreaterThan("hOUr", Literal::Int(11))));

  ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                         ResidualEvaluator::Make(expr, *spec, *schema, false));

  // Equal to the upper date bound
  PartitionValues partition_data1(Literal::Int(20170815));
  ICEBERG_UNWRAP_OR_FAIL(auto residual1, evaluator->ResidualFor(partition_data1));
  ASSERT_TRUE(residual1->is_unbound_predicate());
  auto unbound1 = std::dynamic_pointer_cast<UnboundPredicate>(residual1);
  ASSERT_NE(unbound1, nullptr);
  EXPECT_EQ(unbound1->op(), Expression::Operation::kLt);
  EXPECT_EQ(unbound1->reference()->name(), "HOUR");
  // Access literal through literals() span
  auto unbound1_impl =
      std::dynamic_pointer_cast<UnboundPredicateImpl<BoundReference>>(unbound1);
  ASSERT_NE(unbound1_impl, nullptr);
  ASSERT_EQ(unbound1_impl->literals().size(), 1);
  EXPECT_EQ(unbound1_impl->literals()[0].value(), Literal::Int(12).value());

  // Equal to the lower date bound
  PartitionValues partition_data2(Literal::Int(20170801));
  ICEBERG_UNWRAP_OR_FAIL(auto residual2, evaluator->ResidualFor(partition_data2));
  ASSERT_TRUE(residual2->is_unbound_predicate());
  auto unbound2 = std::dynamic_pointer_cast<UnboundPredicate>(residual2);
  ASSERT_NE(unbound2, nullptr);
  EXPECT_EQ(unbound2->op(), Expression::Operation::kGt);
  EXPECT_EQ(unbound2->reference()->name(), "hOUr");
  // Access literal through literals() span
  auto unbound2_impl =
      std::dynamic_pointer_cast<UnboundPredicateImpl<BoundReference>>(unbound2);
  ASSERT_NE(unbound2_impl, nullptr);
  ASSERT_EQ(unbound2_impl->literals().size(), 1);
  EXPECT_EQ(unbound2_impl->literals()[0].value(), Literal::Int(11).value());

  // Inside the date range
  PartitionValues partition_data3(Literal::Int(20170812));
  ICEBERG_UNWRAP_OR_FAIL(auto residual3, evaluator->ResidualFor(partition_data3));
  EXPECT_EQ(residual3->op(), Expression::Operation::kTrue);

  // Outside the date range
  PartitionValues partition_data4(Literal::Int(20170817));
  ICEBERG_UNWRAP_OR_FAIL(auto residual4, evaluator->ResidualFor(partition_data4));
  EXPECT_EQ(residual4->op(), Expression::Operation::kFalse);
}

TEST_F(ResidualEvaluatorTest, UnpartitionedResiduals) {
  std::vector<std::shared_ptr<Expression>> expressions = {
      Expressions::AlwaysTrue(),
      Expressions::AlwaysFalse(),
      Expressions::LessThan("a", Literal::Int(5)),
      Expressions::GreaterThanOrEqual("b", Literal::Int(16)),
      Expressions::NotNull("c"),
      Expressions::IsNull("d"),
      Expressions::In("e", {Literal::Int(1), Literal::Int(2), Literal::Int(3)}),
      Expressions::NotIn("f", {Literal::Int(1), Literal::Int(2), Literal::Int(3)}),
      Expressions::NotNaN("g"),
      Expressions::IsNaN("h"),
      Expressions::StartsWith("data", "abcd"),
      Expressions::NotStartsWith("data", "abcd")};

  PartitionValues empty_partition;

  for (const auto& expr : expressions) {
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator, ResidualEvaluator::Unpartitioned(expr));
    ICEBERG_UNWRAP_OR_FAIL(auto residual, evaluator->ResidualFor(empty_partition));
    // For unpartitioned tables, residual should be the original expression
    EXPECT_EQ(residual->op(), expr->op());
  }
}

TEST_F(ResidualEvaluatorTest, In) {
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(50, "dateint", int32()),
                               SchemaField::MakeOptional(51, "hour", int32())});

  auto identity_transform = Transform::Identity();
  PartitionField pt_field(50, 1000, "dateint", identity_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec_unique,
                         PartitionSpec::Make(*schema, 0, {pt_field}, false));
  auto spec = std::shared_ptr<PartitionSpec>(spec_unique.release());

  auto expr = Expressions::In("dateint", {Literal::Int(20170815), Literal::Int(20170816),
                                          Literal::Int(20170817)});

  ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                         ResidualEvaluator::Make(expr, *spec, *schema, true));

  PartitionValues partition_data1(Literal::Int(20170815));
  ICEBERG_UNWRAP_OR_FAIL(auto residual1, evaluator->ResidualFor(partition_data1));
  EXPECT_EQ(residual1->op(), Expression::Operation::kTrue);

  PartitionValues partition_data2(Literal::Int(20180815));
  ICEBERG_UNWRAP_OR_FAIL(auto residual2, evaluator->ResidualFor(partition_data2));
  EXPECT_EQ(residual2->op(), Expression::Operation::kFalse);
}

TEST_F(ResidualEvaluatorTest, NotIn) {
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(50, "dateint", int32()),
                               SchemaField::MakeOptional(51, "hour", int32())});

  auto identity_transform = Transform::Identity();
  PartitionField pt_field(50, 1000, "dateint", identity_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec_unique,
                         PartitionSpec::Make(*schema, 0, {pt_field}, false));
  auto spec = std::shared_ptr<PartitionSpec>(spec_unique.release());

  auto expr = Expressions::NotIn(
      "dateint",
      {Literal::Int(20170815), Literal::Int(20170816), Literal::Int(20170817)});

  ICEBERG_UNWRAP_OR_FAIL(auto evaluator,
                         ResidualEvaluator::Make(expr, *spec, *schema, true));

  PartitionValues partition_data1(Literal::Int(20180815));
  ICEBERG_UNWRAP_OR_FAIL(auto residual1, evaluator->ResidualFor(partition_data1));
  EXPECT_EQ(residual1->op(), Expression::Operation::kTrue);

  PartitionValues partition_data2(Literal::Int(20170815));
  ICEBERG_UNWRAP_OR_FAIL(auto residual2, evaluator->ResidualFor(partition_data2));
  EXPECT_EQ(residual2->op(), Expression::Operation::kFalse);
}

TEST_F(ResidualEvaluatorTest, IsNaN) {
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(50, "double", float64()),
                               SchemaField::MakeOptional(51, "float", float32())});

  // Test double field
  auto identity_transform = Transform::Identity();
  PartitionField pt_field_double(50, 1000, "double", identity_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec_double_unique,
                         PartitionSpec::Make(*schema, 0, {pt_field_double}, false));
  auto spec_double = std::shared_ptr<PartitionSpec>(spec_double_unique.release());

  auto expr_double = Expressions::IsNaN("double");
  ICEBERG_UNWRAP_OR_FAIL(
      auto evaluator_double,
      ResidualEvaluator::Make(expr_double, *spec_double, *schema, true));

  PartitionValues partition_data_nan_double(Literal::Double(std::nan("")));
  ICEBERG_UNWRAP_OR_FAIL(auto residual_nan_double,
                         evaluator_double->ResidualFor(partition_data_nan_double));
  EXPECT_EQ(residual_nan_double->op(), Expression::Operation::kTrue);

  PartitionValues partition_data_double(Literal::Double(2.0));
  ICEBERG_UNWRAP_OR_FAIL(auto residual_double,
                         evaluator_double->ResidualFor(partition_data_double));
  EXPECT_EQ(residual_double->op(), Expression::Operation::kFalse);

  // Test float field
  PartitionField pt_field_float(51, 1001, "float", identity_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec_float_unique,
                         PartitionSpec::Make(*schema, 0, {pt_field_float}, false));
  auto spec_float = std::shared_ptr<PartitionSpec>(spec_float_unique.release());

  auto expr_float = Expressions::IsNaN("float");
  ICEBERG_UNWRAP_OR_FAIL(auto evaluator_float,
                         ResidualEvaluator::Make(expr_float, *spec_float, *schema, true));

  PartitionValues partition_data_nan_float(Literal::Float(std::nanf("")));
  ICEBERG_UNWRAP_OR_FAIL(auto residual_nan_float,
                         evaluator_float->ResidualFor(partition_data_nan_float));
  EXPECT_EQ(residual_nan_float->op(), Expression::Operation::kTrue);

  PartitionValues partition_data_float(Literal::Float(3.0f));
  ICEBERG_UNWRAP_OR_FAIL(auto residual_float,
                         evaluator_float->ResidualFor(partition_data_float));
  EXPECT_EQ(residual_float->op(), Expression::Operation::kFalse);
}

TEST_F(ResidualEvaluatorTest, NotNaN) {
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(50, "double", float64()),
                               SchemaField::MakeOptional(51, "float", float32())});

  // Test double field
  auto identity_transform = Transform::Identity();
  PartitionField pt_field_double(50, 1000, "double", identity_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec_double_unique,
                         PartitionSpec::Make(*schema, 0, {pt_field_double}, false));
  auto spec_double = std::shared_ptr<PartitionSpec>(spec_double_unique.release());

  auto expr_double = Expressions::NotNaN("double");
  ICEBERG_UNWRAP_OR_FAIL(
      auto evaluator_double,
      ResidualEvaluator::Make(expr_double, *spec_double, *schema, true));

  PartitionValues partition_data_nan_double(Literal::Double(std::nan("")));
  ICEBERG_UNWRAP_OR_FAIL(auto residual_nan_double,
                         evaluator_double->ResidualFor(partition_data_nan_double));
  EXPECT_EQ(residual_nan_double->op(), Expression::Operation::kFalse);

  PartitionValues partition_data_double(Literal::Double(2.0));
  ICEBERG_UNWRAP_OR_FAIL(auto residual_double,
                         evaluator_double->ResidualFor(partition_data_double));
  EXPECT_EQ(residual_double->op(), Expression::Operation::kTrue);

  // Test float field
  PartitionField pt_field_float(51, 1001, "float", identity_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec_float_unique,
                         PartitionSpec::Make(*schema, 0, {pt_field_float}, false));
  auto spec_float = std::shared_ptr<PartitionSpec>(spec_float_unique.release());

  auto expr_float = Expressions::NotNaN("float");
  ICEBERG_UNWRAP_OR_FAIL(auto evaluator_float,
                         ResidualEvaluator::Make(expr_float, *spec_float, *schema, true));

  PartitionValues partition_data_nan_float(Literal::Float(std::nanf("")));
  ICEBERG_UNWRAP_OR_FAIL(auto residual_nan_float,
                         evaluator_float->ResidualFor(partition_data_nan_float));
  EXPECT_EQ(residual_nan_float->op(), Expression::Operation::kFalse);

  PartitionValues partition_data_float(Literal::Float(3.0f));
  ICEBERG_UNWRAP_OR_FAIL(auto residual_float,
                         evaluator_float->ResidualFor(partition_data_float));
  EXPECT_EQ(residual_float->op(), Expression::Operation::kTrue);
}

TEST_F(ResidualEvaluatorTest, IntegerTruncateTransformResiduals) {
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(50, "value", int32())});

  // Valid partitions would be 0, 10, 20...90, 100 etc.
  auto truncate_transform = Transform::Truncate(10);
  PartitionField pt_field(50, 1000, "__value", truncate_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec_unique,
                         PartitionSpec::Make(*schema, 0, {pt_field}, false));
  auto spec = std::shared_ptr<PartitionSpec>(spec_unique.release());

  // Less than lower bound
  AssertResidualOp(spec, schema, Expressions::LessThan("value", Literal::Int(100)),
                   Literal::Int(110), Expression::Operation::kFalse);
  AssertResidualOp(spec, schema, Expressions::LessThan("value", Literal::Int(100)),
                   Literal::Int(100), Expression::Operation::kFalse);
  AssertResidualOp(spec, schema, Expressions::LessThan("value", Literal::Int(100)),
                   Literal::Int(90), Expression::Operation::kTrue);

  // Less than upper bound
  AssertResidualOp(spec, schema, Expressions::LessThan("value", Literal::Int(99)),
                   Literal::Int(100), Expression::Operation::kFalse);
  AssertResidualPredicate(spec, schema, Expressions::LessThan("value", Literal::Int(99)),
                          Literal::Int(90));
  AssertResidualOp(spec, schema, Expressions::LessThan("value", Literal::Int(99)),
                   Literal::Int(80), Expression::Operation::kTrue);

  // Less than equals lower bound
  AssertResidualOp(spec, schema, Expressions::LessThanOrEqual("value", Literal::Int(100)),
                   Literal::Int(110), Expression::Operation::kFalse);
  AssertResidualPredicate(spec, schema,
                          Expressions::LessThanOrEqual("value", Literal::Int(100)),
                          Literal::Int(100));
  AssertResidualOp(spec, schema, Expressions::LessThanOrEqual("value", Literal::Int(100)),
                   Literal::Int(90), Expression::Operation::kTrue);

  // Less than equals upper bound
  AssertResidualOp(spec, schema, Expressions::LessThanOrEqual("value", Literal::Int(99)),
                   Literal::Int(100), Expression::Operation::kFalse);
  AssertResidualOp(spec, schema, Expressions::LessThanOrEqual("value", Literal::Int(99)),
                   Literal::Int(90), Expression::Operation::kTrue);
  AssertResidualOp(spec, schema, Expressions::LessThanOrEqual("value", Literal::Int(99)),
                   Literal::Int(80), Expression::Operation::kTrue);

  // Greater than lower bound
  AssertResidualOp(spec, schema, Expressions::GreaterThan("value", Literal::Int(100)),
                   Literal::Int(110), Expression::Operation::kTrue);
  AssertResidualPredicate(spec, schema,
                          Expressions::GreaterThan("value", Literal::Int(100)),
                          Literal::Int(100));
  AssertResidualOp(spec, schema, Expressions::GreaterThan("value", Literal::Int(100)),
                   Literal::Int(90), Expression::Operation::kFalse);

  // Greater than upper bound
  AssertResidualOp(spec, schema, Expressions::GreaterThan("value", Literal::Int(99)),
                   Literal::Int(100), Expression::Operation::kTrue);
  AssertResidualOp(spec, schema, Expressions::GreaterThan("value", Literal::Int(99)),
                   Literal::Int(90), Expression::Operation::kFalse);
  AssertResidualOp(spec, schema, Expressions::GreaterThan("value", Literal::Int(99)),
                   Literal::Int(80), Expression::Operation::kFalse);

  // Greater than equals lower bound
  AssertResidualOp(spec, schema,
                   Expressions::GreaterThanOrEqual("value", Literal::Int(100)),
                   Literal::Int(110), Expression::Operation::kTrue);
  AssertResidualOp(spec, schema,
                   Expressions::GreaterThanOrEqual("value", Literal::Int(100)),
                   Literal::Int(100), Expression::Operation::kTrue);
  AssertResidualOp(spec, schema,
                   Expressions::GreaterThanOrEqual("value", Literal::Int(100)),
                   Literal::Int(90), Expression::Operation::kFalse);

  // Greater than equals upper bound
  AssertResidualOp(spec, schema,
                   Expressions::GreaterThanOrEqual("value", Literal::Int(99)),
                   Literal::Int(100), Expression::Operation::kTrue);
  AssertResidualPredicate(spec, schema,
                          Expressions::GreaterThanOrEqual("value", Literal::Int(99)),
                          Literal::Int(90));
  AssertResidualOp(spec, schema,
                   Expressions::GreaterThanOrEqual("value", Literal::Int(99)),
                   Literal::Int(80), Expression::Operation::kFalse);

  // Equal lower bound
  AssertResidualOp(spec, schema, Expressions::Equal("value", Literal::Int(100)),
                   Literal::Int(110), Expression::Operation::kFalse);
  AssertResidualPredicate(spec, schema, Expressions::Equal("value", Literal::Int(100)),
                          Literal::Int(100));
  AssertResidualOp(spec, schema, Expressions::Equal("value", Literal::Int(100)),
                   Literal::Int(90), Expression::Operation::kFalse);

  // Equal upper bound
  AssertResidualOp(spec, schema, Expressions::Equal("value", Literal::Int(99)),
                   Literal::Int(100), Expression::Operation::kFalse);
  AssertResidualPredicate(spec, schema, Expressions::Equal("value", Literal::Int(99)),
                          Literal::Int(90));
  AssertResidualOp(spec, schema, Expressions::Equal("value", Literal::Int(99)),
                   Literal::Int(80), Expression::Operation::kFalse);

  // Not equal lower bound
  AssertResidualOp(spec, schema, Expressions::NotEqual("value", Literal::Int(100)),
                   Literal::Int(110), Expression::Operation::kTrue);
  AssertResidualPredicate(spec, schema, Expressions::NotEqual("value", Literal::Int(100)),
                          Literal::Int(100));
  AssertResidualOp(spec, schema, Expressions::NotEqual("value", Literal::Int(100)),
                   Literal::Int(90), Expression::Operation::kTrue);

  // Not equal upper bound
  AssertResidualOp(spec, schema, Expressions::NotEqual("value", Literal::Int(99)),
                   Literal::Int(100), Expression::Operation::kTrue);
  AssertResidualPredicate(spec, schema, Expressions::NotEqual("value", Literal::Int(99)),
                          Literal::Int(90));
  AssertResidualOp(spec, schema, Expressions::NotEqual("value", Literal::Int(99)),
                   Literal::Int(80), Expression::Operation::kTrue);
}

TEST_F(ResidualEvaluatorTest, StringTruncateTransformResiduals) {
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(50, "value", string())});

  // Valid partitions would be two letter strings for eg: ab, bc etc
  auto truncate_transform = Transform::Truncate(2);
  PartitionField pt_field(50, 1000, "__value", truncate_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec_unique,
                         PartitionSpec::Make(*schema, 0, {pt_field}, false));
  auto spec = std::shared_ptr<PartitionSpec>(spec_unique.release());

  // Less than
  AssertResidualOp(spec, schema, Expressions::LessThan("value", Literal::String("bcd")),
                   Literal::String("ab"), Expression::Operation::kTrue);
  AssertResidualPredicate(spec, schema,
                          Expressions::LessThan("value", Literal::String("bcd")),
                          Literal::String("bc"));
  AssertResidualOp(spec, schema, Expressions::LessThan("value", Literal::String("bcd")),
                   Literal::String("cd"), Expression::Operation::kFalse);

  // Less than equals
  AssertResidualOp(spec, schema,
                   Expressions::LessThanOrEqual("value", Literal::String("bcd")),
                   Literal::String("ab"), Expression::Operation::kTrue);
  AssertResidualPredicate(spec, schema,
                          Expressions::LessThanOrEqual("value", Literal::String("bcd")),
                          Literal::String("bc"));
  AssertResidualOp(spec, schema,
                   Expressions::LessThanOrEqual("value", Literal::String("bcd")),
                   Literal::String("cd"), Expression::Operation::kFalse);

  // Greater than
  AssertResidualOp(spec, schema,
                   Expressions::GreaterThan("value", Literal::String("bcd")),
                   Literal::String("ab"), Expression::Operation::kFalse);
  AssertResidualPredicate(spec, schema,
                          Expressions::GreaterThan("value", Literal::String("bcd")),
                          Literal::String("bc"));
  AssertResidualOp(spec, schema,
                   Expressions::GreaterThan("value", Literal::String("bcd")),
                   Literal::String("cd"), Expression::Operation::kTrue);

  // Greater than equals
  AssertResidualOp(spec, schema,
                   Expressions::GreaterThanOrEqual("value", Literal::String("bcd")),
                   Literal::String("ab"), Expression::Operation::kFalse);
  AssertResidualPredicate(
      spec, schema, Expressions::GreaterThanOrEqual("value", Literal::String("bcd")),
      Literal::String("bc"));
  AssertResidualOp(spec, schema,
                   Expressions::GreaterThanOrEqual("value", Literal::String("bcd")),
                   Literal::String("cd"), Expression::Operation::kTrue);

  // Equal
  AssertResidualOp(spec, schema, Expressions::Equal("value", Literal::String("bcd")),
                   Literal::String("ab"), Expression::Operation::kFalse);
  AssertResidualPredicate(spec, schema,
                          Expressions::Equal("value", Literal::String("bcd")),
                          Literal::String("bc"));
  AssertResidualOp(spec, schema, Expressions::Equal("value", Literal::String("bcd")),
                   Literal::String("cd"), Expression::Operation::kFalse);

  // Not equal
  AssertResidualOp(spec, schema, Expressions::NotEqual("value", Literal::String("bcd")),
                   Literal::String("ab"), Expression::Operation::kTrue);
  AssertResidualPredicate(spec, schema,
                          Expressions::NotEqual("value", Literal::String("bcd")),
                          Literal::String("bc"));
  AssertResidualOp(spec, schema, Expressions::NotEqual("value", Literal::String("bcd")),
                   Literal::String("cd"), Expression::Operation::kTrue);

  // Starts with
  AssertResidualOp(spec, schema, Expressions::StartsWith("value", "bcd"),
                   Literal::String("ab"), Expression::Operation::kFalse);
  AssertResidualPredicate(spec, schema, Expressions::StartsWith("value", "bcd"),
                          Literal::String("bc"));
  AssertResidualOp(spec, schema, Expressions::StartsWith("value", "bcd"),
                   Literal::String("cd"), Expression::Operation::kFalse);
  AssertResidualPredicate(spec, schema, Expressions::StartsWith("value", "bcd"),
                          Literal::String("bcdd"));

  // Not starts with
  AssertResidualOp(spec, schema, Expressions::NotStartsWith("value", "bcd"),
                   Literal::String("ab"), Expression::Operation::kTrue);
  AssertResidualPredicate(spec, schema, Expressions::NotStartsWith("value", "bcd"),
                          Literal::String("bc"));
  AssertResidualOp(spec, schema, Expressions::NotStartsWith("value", "bcd"),
                   Literal::String("cd"), Expression::Operation::kTrue);
  AssertResidualPredicate(spec, schema, Expressions::NotStartsWith("value", "bcd"),
                          Literal::String("bcd"));
  AssertResidualPredicate(spec, schema, Expressions::NotStartsWith("value", "bcd"),
                          Literal::String("bcdd"));
}

}  // namespace iceberg
