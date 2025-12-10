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

#include "iceberg/expression/projections.h"

#include <algorithm>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/expression/expressions.h"
#include "iceberg/expression/predicate.h"
#include "iceberg/partition_field.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/temporal_test_helper.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"

namespace iceberg {

class ProjectionsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create a simple test schema with various field types
    schema_ = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeOptional(16, "id", int64())},
        /*schema_id=*/0);
  }

  std::shared_ptr<Schema> schema_;
};

// Helper function to extract UnboundPredicate from expression
std::shared_ptr<UnboundPredicate> ExtractUnboundPredicate(
    const std::shared_ptr<Expression>& expr) {
  if (expr->is_unbound_predicate()) {
    return std::dynamic_pointer_cast<UnboundPredicate>(expr);
  }
  return nullptr;
}

// Helper function to extract BoundPredicate from expression
std::shared_ptr<BoundPredicate> ExtractBoundPredicate(
    const std::shared_ptr<Expression>& expr) {
  if (expr->is_bound_predicate()) {
    return std::dynamic_pointer_cast<BoundPredicate>(expr);
  }
  return nullptr;
}

// Helper function to assert projection operation
void AssertProjectionOperation(const std::shared_ptr<Expression>& projection,
                               Expression::Operation expected_op) {
  ASSERT_NE(projection, nullptr);
  EXPECT_EQ(projection->op(), expected_op);
}

// Helper function to assert projection value for True/False
void AssertProjectionValue(const std::shared_ptr<Expression>& projection,
                           Expression::Operation expected_op) {
  ASSERT_NE(projection, nullptr);
  EXPECT_EQ(projection->op(), expected_op);
}

TEST_F(ProjectionsTest, IdentityProjectionInclusive) {
  auto identity_transform = Transform::Identity();
  PartitionField pt_field(16, 1000, "id", identity_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec, PartitionSpec::Make(0, {pt_field}));

  std::vector<std::shared_ptr<UnboundPredicate>> predicates = {
      Expressions::NotNull("id"),
      Expressions::IsNull("id"),
      Expressions::LessThan("id", Literal::Long(100)),
      Expressions::LessThanOrEqual("id", Literal::Long(101)),
      Expressions::GreaterThan("id", Literal::Long(102)),
      Expressions::GreaterThanOrEqual("id", Literal::Long(103)),
      Expressions::Equal("id", Literal::Long(104)),
      Expressions::NotEqual("id", Literal::Long(105)),
  };

  for (const auto& predicate : predicates) {
    // Bind the predicate first
    ICEBERG_UNWRAP_OR_FAIL(auto bound_pred, predicate->Bind(*schema_, true));
    auto bound = ExtractBoundPredicate(bound_pred);
    ASSERT_NE(bound, nullptr);

    // Project the bound predicate
    auto evaluator = Projections::Inclusive(*spec, *schema_, true);
    ICEBERG_UNWRAP_OR_FAIL(auto projected_expr, evaluator->Project(bound_pred));

    // Check that we got a predicate back
    auto projected = ExtractUnboundPredicate(projected_expr);
    ASSERT_NE(projected, nullptr);

    // Check that the operation matches
    EXPECT_EQ(projected->op(), bound->op());

    // Check that the field name matches
    EXPECT_EQ(projected->reference()->name(), "id");

    if (bound->kind() == BoundPredicate::Kind::kLiteral) {
      const auto& literal_predicate =
          internal::checked_pointer_cast<UnboundPredicateImpl<BoundReference>>(projected);
      const auto& bound_literal_predicate =
          internal::checked_pointer_cast<BoundLiteralPredicate>(bound);
      EXPECT_EQ(literal_predicate->literals().front(),
                bound_literal_predicate->literal());
    }
  }
}

TEST_F(ProjectionsTest, IdentityProjectionStrict) {
  auto identity_transform = Transform::Identity();
  PartitionField pt_field(16, 1000, "id", identity_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec, PartitionSpec::Make(0, {pt_field}));

  std::vector<std::shared_ptr<UnboundPredicate>> predicates = {
      Expressions::NotNull("id"),
      Expressions::IsNull("id"),
      Expressions::LessThan("id", Literal::Long(100)),
      Expressions::LessThanOrEqual("id", Literal::Long(101)),
      Expressions::GreaterThan("id", Literal::Long(102)),
      Expressions::GreaterThanOrEqual("id", Literal::Long(103)),
      Expressions::Equal("id", Literal::Long(104)),
      Expressions::NotEqual("id", Literal::Long(105)),
  };

  for (const auto& predicate : predicates) {
    // Bind the predicate first
    ICEBERG_UNWRAP_OR_FAIL(auto bound_pred, predicate->Bind(*schema_, true));
    auto bound = ExtractBoundPredicate(bound_pred);
    ASSERT_NE(bound, nullptr);

    // Project the bound predicate
    auto evaluator = Projections::Strict(*spec, *schema_, true);
    ICEBERG_UNWRAP_OR_FAIL(auto projected_expr, evaluator->Project(bound_pred));

    // Check that we got a predicate back
    auto projected = ExtractUnboundPredicate(projected_expr);
    ASSERT_NE(projected, nullptr);

    // Check that the operation matches
    EXPECT_EQ(projected->op(), bound->op());

    // Check that the field name matches
    EXPECT_EQ(projected->reference()->name(), "id");

    if (bound->kind() == BoundPredicate::Kind::kLiteral) {
      const auto& literal_predicate =
          internal::checked_pointer_cast<UnboundPredicateImpl<BoundReference>>(projected);
      const auto& bound_literal_predicate =
          internal::checked_pointer_cast<BoundLiteralPredicate>(bound);
      EXPECT_EQ(literal_predicate->literals().front(),
                bound_literal_predicate->literal());
    }
  }
}

TEST_F(ProjectionsTest, CaseInsensitiveIdentityProjection) {
  auto identity_transform = Transform::Identity();
  PartitionField pt_field(16, 1000, "id", identity_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec, PartitionSpec::Make(0, {pt_field}));

  std::vector<std::shared_ptr<UnboundPredicate>> predicates = {
      Expressions::NotNull("ID"),
      Expressions::IsNull("ID"),
      Expressions::LessThan("ID", Literal::Long(100)),
      Expressions::LessThanOrEqual("ID", Literal::Long(101)),
      Expressions::GreaterThan("ID", Literal::Long(102)),
      Expressions::GreaterThanOrEqual("ID", Literal::Long(103)),
      Expressions::Equal("ID", Literal::Long(104)),
      Expressions::NotEqual("ID", Literal::Long(105)),
  };

  for (const auto& predicate : predicates) {
    // Bind the predicate first (case insensitive)
    ICEBERG_UNWRAP_OR_FAIL(auto bound_pred, predicate->Bind(*schema_, false));
    auto bound = ExtractBoundPredicate(bound_pred);
    ASSERT_NE(bound, nullptr);

    // Project the bound predicate (case insensitive)
    auto evaluator = Projections::Inclusive(*spec, *schema_, false);
    ICEBERG_UNWRAP_OR_FAIL(auto projected_expr, evaluator->Project(bound_pred));

    // Check that we got a predicate back
    auto projected = ExtractUnboundPredicate(projected_expr);
    ASSERT_NE(projected, nullptr);

    // Check that the operation matches
    EXPECT_EQ(projected->op(), bound->op());

    // Check that the field name matches
    EXPECT_EQ(projected->reference()->name(), "id");

    if (bound->kind() == BoundPredicate::Kind::kLiteral) {
      const auto& literal_predicate =
          internal::checked_pointer_cast<UnboundPredicateImpl<BoundReference>>(projected);
      const auto& bound_literal_predicate =
          internal::checked_pointer_cast<BoundLiteralPredicate>(bound);
      EXPECT_EQ(literal_predicate->literals().front(),
                bound_literal_predicate->literal());
    }
  }
}

TEST_F(ProjectionsTest, CaseSensitiveIdentityProjectionFailure) {
  auto identity_transform = Transform::Identity();
  PartitionField pt_field(16, 1000, "id", identity_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec, PartitionSpec::Make(0, {pt_field}));

  auto predicate = Expressions::NotNull("ID");
  // Binding should fail with case sensitive
  auto bound_result = predicate->Bind(*schema_, true);
  EXPECT_THAT(bound_result, IsError(ErrorKind::kInvalidExpression));
}

// Bucketing projection tests
class BucketingProjectionTest : public ::testing::Test {
 protected:
  void AssertProjectionStrict(const PartitionSpec& spec, const Schema& schema,
                              const std::shared_ptr<Expression>& filter,
                              Expression::Operation expected_op,
                              const std::string& expected_literal) {
    auto evaluator = Projections::Strict(spec, schema, true);
    ICEBERG_UNWRAP_OR_FAIL(auto projection, evaluator->Project(filter));
    AssertProjectionOperation(projection, expected_op);

    if (expected_op != Expression::Operation::kFalse) {
      auto predicate = ExtractUnboundPredicate(projection);
      ASSERT_NE(predicate, nullptr);
      if (predicate->op() == Expression::Operation::kNotIn) {
        // For NOT_IN, check literals
        const auto& literal_predicate =
            internal::checked_pointer_cast<UnboundPredicateImpl<BoundReference>>(
                predicate);
        auto literals = literal_predicate->literals();
        std::vector<std::string> values;
        for (const auto& lit : literals) {
          values.push_back(std::to_string(std::get<int32_t>(lit.value())));
        }
        std::ranges::sort(values);
        std::string actual = "[";
        for (size_t i = 0; i < values.size(); ++i) {
          if (i > 0) actual += ", ";
          actual += values[i];
        }
        actual += "]";
        EXPECT_EQ(actual, expected_literal);
      } else {
        // For other operations, check single literal
        const auto& literal_predicate =
            internal::checked_pointer_cast<UnboundPredicateImpl<BoundReference>>(
                predicate);
        auto literal = literal_predicate->literals().front();
        std::string output = std::to_string(std::get<int32_t>(literal.value()));
        EXPECT_EQ(output, expected_literal);
      }
    }
  }

  void AssertProjectionStrictValue(const PartitionSpec& spec, const Schema& schema,
                                   const std::shared_ptr<Expression>& filter,
                                   Expression::Operation expected_op) {
    auto evaluator = Projections::Strict(spec, schema, true);
    ICEBERG_UNWRAP_OR_FAIL(auto projection, evaluator->Project(filter));
    AssertProjectionValue(projection, expected_op);
  }

  void AssertProjectionInclusive(const PartitionSpec& spec, const Schema& schema,
                                 const std::shared_ptr<Expression>& filter,
                                 Expression::Operation expected_op,
                                 const std::string& expected_literal) {
    auto evaluator = Projections::Inclusive(spec, schema, true);
    ICEBERG_UNWRAP_OR_FAIL(auto projection, evaluator->Project(filter));
    AssertProjectionOperation(projection, expected_op);

    if (expected_op != Expression::Operation::kTrue) {
      auto predicate = ExtractUnboundPredicate(projection);
      ASSERT_NE(predicate, nullptr);
      if (predicate->op() == Expression::Operation::kIn) {
        // For IN, check literals
        const auto& literal_predicate =
            internal::checked_pointer_cast<UnboundPredicateImpl<BoundReference>>(
                predicate);
        auto literals = literal_predicate->literals();
        std::vector<std::string> values;
        for (const auto& lit : literals) {
          values.push_back(std::to_string(std::get<int32_t>(lit.value())));
        }
        std::ranges::sort(values);
        std::string actual = "[";
        for (size_t i = 0; i < values.size(); ++i) {
          if (i > 0) actual += ", ";
          actual += values[i];
        }
        actual += "]";
        EXPECT_EQ(actual, expected_literal);
      } else {
        // For other operations, check single literal
        const auto& literal_predicate =
            internal::checked_pointer_cast<UnboundPredicateImpl<BoundReference>>(
                predicate);
        auto literal = literal_predicate->literals().front();
        std::string output = std::to_string(std::get<int32_t>(literal.value()));
        EXPECT_EQ(output, expected_literal);
      }
    }
  }

  void AssertProjectionInclusiveValue(const PartitionSpec& spec, const Schema& schema,
                                      const std::shared_ptr<Expression>& filter,
                                      Expression::Operation expected_op) {
    auto evaluator = Projections::Inclusive(spec, schema, true);
    ICEBERG_UNWRAP_OR_FAIL(auto projection, evaluator->Project(filter));
    AssertProjectionValue(projection, expected_op);
  }
};

TEST_F(BucketingProjectionTest, BucketIntegerStrict) {
  int32_t value = 100;
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(1, "value", int32())}, 0);
  auto bucket_transform = Transform::Bucket(10);
  PartitionField pt_field(1, 1000, "value_bucket", bucket_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec, PartitionSpec::Make(0, {pt_field}));

  // Bind predicates first
  auto not_equal_pred = Expressions::NotEqual("value", Literal::Int(value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_not_equal, not_equal_pred->Bind(*schema, true));

  auto equal_pred = Expressions::Equal("value", Literal::Int(value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_equal, equal_pred->Bind(*schema, true));

  auto less_than_pred = Expressions::LessThan("value", Literal::Int(value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_less_than, less_than_pred->Bind(*schema, true));

  auto less_equal_pred = Expressions::LessThanOrEqual("value", Literal::Int(value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_less_equal, less_equal_pred->Bind(*schema, true));

  auto greater_than_pred = Expressions::GreaterThan("value", Literal::Int(value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_greater_than, greater_than_pred->Bind(*schema, true));

  auto greater_equal_pred = Expressions::GreaterThanOrEqual("value", Literal::Int(value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_greater_equal,
                         greater_equal_pred->Bind(*schema, true));

  // The bucket number of 100 with 10 buckets is 6
  AssertProjectionStrict(*spec, *schema, bound_not_equal, Expression::Operation::kNotEq,
                         "6");
  AssertProjectionStrictValue(*spec, *schema, bound_equal, Expression::Operation::kFalse);
  AssertProjectionStrictValue(*spec, *schema, bound_less_than,
                              Expression::Operation::kFalse);
  AssertProjectionStrictValue(*spec, *schema, bound_less_equal,
                              Expression::Operation::kFalse);
  AssertProjectionStrictValue(*spec, *schema, bound_greater_than,
                              Expression::Operation::kFalse);
  AssertProjectionStrictValue(*spec, *schema, bound_greater_equal,
                              Expression::Operation::kFalse);
}

TEST_F(BucketingProjectionTest, BucketIntegerInclusive) {
  int32_t value = 100;
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(1, "value", int32())}, 0);
  auto bucket_transform = Transform::Bucket(10);
  PartitionField pt_field(1, 1000, "value_bucket", bucket_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec, PartitionSpec::Make(0, {pt_field}));

  // Bind predicates first
  auto equal_pred = Expressions::Equal("value", Literal::Int(value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_equal, equal_pred->Bind(*schema, true));

  auto not_equal_pred = Expressions::NotEqual("value", Literal::Int(value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_not_equal, not_equal_pred->Bind(*schema, true));

  auto less_than_pred = Expressions::LessThan("value", Literal::Int(value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_less_than, less_than_pred->Bind(*schema, true));

  auto less_equal_pred = Expressions::LessThanOrEqual("value", Literal::Int(value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_less_equal, less_equal_pred->Bind(*schema, true));

  auto greater_than_pred = Expressions::GreaterThan("value", Literal::Int(value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_greater_than, greater_than_pred->Bind(*schema, true));

  auto greater_equal_pred = Expressions::GreaterThanOrEqual("value", Literal::Int(value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_greater_equal,
                         greater_equal_pred->Bind(*schema, true));

  // The bucket number of 100 with 10 buckets is 6
  AssertProjectionInclusive(*spec, *schema, bound_equal, Expression::Operation::kEq, "6");
  AssertProjectionInclusiveValue(*spec, *schema, bound_not_equal,
                                 Expression::Operation::kTrue);
  AssertProjectionInclusiveValue(*spec, *schema, bound_less_than,
                                 Expression::Operation::kTrue);
  AssertProjectionInclusiveValue(*spec, *schema, bound_less_equal,
                                 Expression::Operation::kTrue);
  AssertProjectionInclusiveValue(*spec, *schema, bound_greater_than,
                                 Expression::Operation::kTrue);
  AssertProjectionInclusiveValue(*spec, *schema, bound_greater_equal,
                                 Expression::Operation::kTrue);
}

TEST_F(BucketingProjectionTest, BucketLongStrict) {
  int64_t value = 100L;
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(1, "value", int64())}, 0);
  auto bucket_transform = Transform::Bucket(10);
  PartitionField pt_field(1, 1000, "value_bucket", bucket_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec, PartitionSpec::Make(0, {pt_field}));

  auto not_equal_pred = Expressions::NotEqual("value", Literal::Long(value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_not_equal, not_equal_pred->Bind(*schema, true));

  auto equal_pred = Expressions::Equal("value", Literal::Long(value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_equal, equal_pred->Bind(*schema, true));

  // The bucket number of 100 with 10 buckets is 6
  AssertProjectionStrict(*spec, *schema, bound_not_equal, Expression::Operation::kNotEq,
                         "6");
  AssertProjectionStrictValue(*spec, *schema, bound_equal, Expression::Operation::kFalse);
}

TEST_F(BucketingProjectionTest, BucketLongInclusive) {
  int64_t value = 100L;
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(1, "value", int64())}, 0);
  auto bucket_transform = Transform::Bucket(10);
  PartitionField pt_field(1, 1000, "value_bucket", bucket_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec, PartitionSpec::Make(0, {pt_field}));

  auto equal_pred = Expressions::Equal("value", Literal::Long(value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_equal, equal_pred->Bind(*schema, true));

  auto not_equal_pred = Expressions::NotEqual("value", Literal::Long(value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_not_equal, not_equal_pred->Bind(*schema, true));

  // The bucket number of 100 with 10 buckets is 6
  AssertProjectionInclusive(*spec, *schema, bound_equal, Expression::Operation::kEq, "6");
  AssertProjectionInclusiveValue(*spec, *schema, bound_not_equal,
                                 Expression::Operation::kTrue);
}

TEST_F(BucketingProjectionTest, BucketStringStrict) {
  std::string value = "abcdefg";
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(1, "value", string())}, 0);
  auto bucket_transform = Transform::Bucket(10);
  PartitionField pt_field(1, 1000, "value_bucket", bucket_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec, PartitionSpec::Make(0, {pt_field}));

  auto not_equal_pred = Expressions::NotEqual("value", Literal::String(value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_not_equal, not_equal_pred->Bind(*schema, true));

  auto equal_pred = Expressions::Equal("value", Literal::String(value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_equal, equal_pred->Bind(*schema, true));

  // The bucket number of "abcdefg" with 10 buckets is 4
  AssertProjectionStrict(*spec, *schema, bound_not_equal, Expression::Operation::kNotEq,
                         "4");
  AssertProjectionStrictValue(*spec, *schema, bound_equal, Expression::Operation::kFalse);
}

TEST_F(BucketingProjectionTest, BucketStringInclusive) {
  std::string value = "abcdefg";
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(1, "value", string())}, 0);
  auto bucket_transform = Transform::Bucket(10);
  PartitionField pt_field(1, 1000, "value_bucket", bucket_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec, PartitionSpec::Make(0, {pt_field}));

  auto equal_pred = Expressions::Equal("value", Literal::String(value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_equal, equal_pred->Bind(*schema, true));

  auto not_equal_pred = Expressions::NotEqual("value", Literal::String(value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_not_equal, not_equal_pred->Bind(*schema, true));

  // The bucket number of "abcdefg" with 10 buckets is 4
  AssertProjectionInclusive(*spec, *schema, bound_equal, Expression::Operation::kEq, "4");
  AssertProjectionInclusiveValue(*spec, *schema, bound_not_equal,
                                 Expression::Operation::kTrue);
}

// Date projection tests
class DateProjectionTest : public ::testing::Test {
 protected:
  void AssertProjectionStrict(const PartitionSpec& spec, const Schema& schema,
                              const std::shared_ptr<Expression>& filter,
                              Expression::Operation expected_op) {
    auto evaluator = Projections::Strict(spec, schema, true);
    ICEBERG_UNWRAP_OR_FAIL(auto projection, evaluator->Project(filter));
    AssertProjectionOperation(projection, expected_op);
  }

  void AssertProjectionInclusive(const PartitionSpec& spec, const Schema& schema,
                                 const std::shared_ptr<Expression>& filter,
                                 Expression::Operation expected_op) {
    auto evaluator = Projections::Inclusive(spec, schema, true);
    ICEBERG_UNWRAP_OR_FAIL(auto projection, evaluator->Project(filter));
    AssertProjectionOperation(projection, expected_op);
  }
};

TEST_F(DateProjectionTest, DayStrict) {
  int32_t date_value =
      TemporalTestHelper::CreateDate({.year = 2017, .month = 1, .day = 1});
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(1, "date", date())}, 0);
  auto day_transform = Transform::Day();
  PartitionField pt_field(1, 1000, "date_day", day_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec, PartitionSpec::Make(0, {pt_field}));

  auto less_than_pred = Expressions::LessThan("date", Literal::Date(date_value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_less_than, less_than_pred->Bind(*schema, true));

  auto less_equal_pred = Expressions::LessThanOrEqual("date", Literal::Date(date_value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_less_equal, less_equal_pred->Bind(*schema, true));

  auto greater_than_pred = Expressions::GreaterThan("date", Literal::Date(date_value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_greater_than, greater_than_pred->Bind(*schema, true));

  auto greater_equal_pred =
      Expressions::GreaterThanOrEqual("date", Literal::Date(date_value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_greater_equal,
                         greater_equal_pred->Bind(*schema, true));

  auto equal_pred = Expressions::Equal("date", Literal::Date(date_value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_equal, equal_pred->Bind(*schema, true));

  AssertProjectionStrict(*spec, *schema, bound_less_than, Expression::Operation::kLt);
  AssertProjectionStrict(*spec, *schema, bound_less_equal, Expression::Operation::kLt);
  AssertProjectionStrict(*spec, *schema, bound_greater_than, Expression::Operation::kGt);
  AssertProjectionStrict(*spec, *schema, bound_greater_equal, Expression::Operation::kGt);
  AssertProjectionStrict(*spec, *schema, bound_equal, Expression::Operation::kFalse);
}

TEST_F(DateProjectionTest, DayInclusive) {
  int32_t date_value =
      TemporalTestHelper::CreateDate({.year = 2017, .month = 1, .day = 1});
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(1, "date", date())}, 0);
  auto day_transform = Transform::Day();
  PartitionField pt_field(1, 1000, "date_day", day_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec, PartitionSpec::Make(0, {pt_field}));

  auto less_than_pred = Expressions::LessThan("date", Literal::Date(date_value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_less_than, less_than_pred->Bind(*schema, true));

  auto less_equal_pred = Expressions::LessThanOrEqual("date", Literal::Date(date_value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_less_equal, less_equal_pred->Bind(*schema, true));

  auto greater_than_pred = Expressions::GreaterThan("date", Literal::Date(date_value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_greater_than, greater_than_pred->Bind(*schema, true));

  auto greater_equal_pred =
      Expressions::GreaterThanOrEqual("date", Literal::Date(date_value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_greater_equal,
                         greater_equal_pred->Bind(*schema, true));

  auto equal_pred = Expressions::Equal("date", Literal::Date(date_value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_equal, equal_pred->Bind(*schema, true));

  AssertProjectionInclusive(*spec, *schema, bound_less_than,
                            Expression::Operation::kLtEq);
  AssertProjectionInclusive(*spec, *schema, bound_less_equal,
                            Expression::Operation::kLtEq);
  AssertProjectionInclusive(*spec, *schema, bound_greater_than,
                            Expression::Operation::kGtEq);
  AssertProjectionInclusive(*spec, *schema, bound_greater_equal,
                            Expression::Operation::kGtEq);
  AssertProjectionInclusive(*spec, *schema, bound_equal, Expression::Operation::kEq);
}

TEST_F(DateProjectionTest, MonthStrict) {
  int32_t date_value =
      TemporalTestHelper::CreateDate({.year = 2017, .month = 1, .day = 1});
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(1, "date", date())}, 0);
  auto month_transform = Transform::Month();
  PartitionField pt_field(1, 1000, "date_month", month_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec, PartitionSpec::Make(0, {pt_field}));

  auto less_than_pred = Expressions::LessThan("date", Literal::Date(date_value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_less_than, less_than_pred->Bind(*schema, true));

  auto equal_pred = Expressions::Equal("date", Literal::Date(date_value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_equal, equal_pred->Bind(*schema, true));

  AssertProjectionStrict(*spec, *schema, bound_less_than, Expression::Operation::kLt);
  AssertProjectionStrict(*spec, *schema, bound_equal, Expression::Operation::kFalse);
}

TEST_F(DateProjectionTest, MonthInclusive) {
  int32_t date_value =
      TemporalTestHelper::CreateDate({.year = 2017, .month = 1, .day = 1});
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(1, "date", date())}, 0);
  auto month_transform = Transform::Month();
  PartitionField pt_field(1, 1000, "date_month", month_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec, PartitionSpec::Make(0, {pt_field}));

  auto less_than_pred = Expressions::LessThan("date", Literal::Date(date_value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_less_than, less_than_pred->Bind(*schema, true));

  auto equal_pred = Expressions::Equal("date", Literal::Date(date_value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_equal, equal_pred->Bind(*schema, true));

  AssertProjectionInclusive(*spec, *schema, bound_less_than,
                            Expression::Operation::kLtEq);
  AssertProjectionInclusive(*spec, *schema, bound_equal, Expression::Operation::kEq);
}

TEST_F(DateProjectionTest, YearStrict) {
  int32_t date_value =
      TemporalTestHelper::CreateDate({.year = 2017, .month = 1, .day = 1});
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(1, "date", date())}, 0);
  auto year_transform = Transform::Year();
  PartitionField pt_field(1, 1000, "date_year", year_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec, PartitionSpec::Make(0, {pt_field}));

  auto less_than_pred = Expressions::LessThan("date", Literal::Date(date_value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_less_than, less_than_pred->Bind(*schema, true));

  auto equal_pred = Expressions::Equal("date", Literal::Date(date_value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_equal, equal_pred->Bind(*schema, true));

  AssertProjectionStrict(*spec, *schema, bound_less_than, Expression::Operation::kLt);
  AssertProjectionStrict(*spec, *schema, bound_equal, Expression::Operation::kFalse);
}

TEST_F(DateProjectionTest, YearInclusive) {
  int32_t date_value =
      TemporalTestHelper::CreateDate({.year = 2017, .month = 1, .day = 1});
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(1, "date", date())}, 0);
  auto year_transform = Transform::Year();
  PartitionField pt_field(1, 1000, "date_year", year_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec, PartitionSpec::Make(0, {pt_field}));

  auto less_than_pred = Expressions::LessThan("date", Literal::Date(date_value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_less_than, less_than_pred->Bind(*schema, true));

  auto equal_pred = Expressions::Equal("date", Literal::Date(date_value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_equal, equal_pred->Bind(*schema, true));

  AssertProjectionInclusive(*spec, *schema, bound_less_than,
                            Expression::Operation::kLtEq);
  AssertProjectionInclusive(*spec, *schema, bound_equal, Expression::Operation::kEq);
}

// Timestamp projection tests
class TimestampProjectionTest : public ::testing::Test {
 protected:
  void AssertProjectionStrict(const PartitionSpec& spec, const Schema& schema,
                              const std::shared_ptr<Expression>& filter,
                              Expression::Operation expected_op) {
    auto evaluator = Projections::Strict(spec, schema, true);
    ICEBERG_UNWRAP_OR_FAIL(auto projection, evaluator->Project(filter));
    AssertProjectionOperation(projection, expected_op);
  }

  void AssertProjectionInclusive(const PartitionSpec& spec, const Schema& schema,
                                 const std::shared_ptr<Expression>& filter,
                                 Expression::Operation expected_op) {
    auto evaluator = Projections::Inclusive(spec, schema, true);
    ICEBERG_UNWRAP_OR_FAIL(auto projection, evaluator->Project(filter));
    AssertProjectionOperation(projection, expected_op);
  }
};

TEST_F(TimestampProjectionTest, DayStrict) {
  int64_t ts_value = TemporalTestHelper::CreateTimestamp({.year = 2017,
                                                          .month = 12,
                                                          .day = 1,
                                                          .hour = 0,
                                                          .minute = 0,
                                                          .second = 0,
                                                          .microsecond = 0});
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(1, "timestamp", timestamp())},
      0);
  auto day_transform = Transform::Day();
  PartitionField pt_field(1, 1000, "timestamp_day", day_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec, PartitionSpec::Make(0, {pt_field}));

  auto less_than_pred = Expressions::LessThan("timestamp", Literal::Timestamp(ts_value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_less_than, less_than_pred->Bind(*schema, true));

  auto equal_pred = Expressions::Equal("timestamp", Literal::Timestamp(ts_value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_equal, equal_pred->Bind(*schema, true));

  AssertProjectionStrict(*spec, *schema, bound_less_than, Expression::Operation::kLt);
  AssertProjectionStrict(*spec, *schema, bound_equal, Expression::Operation::kFalse);
}

TEST_F(TimestampProjectionTest, DayInclusive) {
  int64_t ts_value = TemporalTestHelper::CreateTimestamp({.year = 2017,
                                                          .month = 12,
                                                          .day = 1,
                                                          .hour = 0,
                                                          .minute = 0,
                                                          .second = 0,
                                                          .microsecond = 0});
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(1, "timestamp", timestamp())},
      0);
  auto day_transform = Transform::Day();
  PartitionField pt_field(1, 1000, "timestamp_day", day_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec, PartitionSpec::Make(0, {pt_field}));

  auto less_than_pred = Expressions::LessThan("timestamp", Literal::Timestamp(ts_value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_less_than, less_than_pred->Bind(*schema, true));

  auto equal_pred = Expressions::Equal("timestamp", Literal::Timestamp(ts_value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_equal, equal_pred->Bind(*schema, true));

  AssertProjectionInclusive(*spec, *schema, bound_less_than,
                            Expression::Operation::kLtEq);
  AssertProjectionInclusive(*spec, *schema, bound_equal, Expression::Operation::kEq);
}

TEST_F(TimestampProjectionTest, MonthStrict) {
  int64_t ts_value = TemporalTestHelper::CreateTimestamp({.year = 2017,
                                                          .month = 12,
                                                          .day = 1,
                                                          .hour = 0,
                                                          .minute = 0,
                                                          .second = 0,
                                                          .microsecond = 0});
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(1, "timestamp", timestamp())},
      0);
  auto month_transform = Transform::Month();
  PartitionField pt_field(1, 1000, "timestamp_month", month_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec, PartitionSpec::Make(0, {pt_field}));

  auto less_than_pred = Expressions::LessThan("timestamp", Literal::Timestamp(ts_value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_less_than, less_than_pred->Bind(*schema, true));

  auto equal_pred = Expressions::Equal("timestamp", Literal::Timestamp(ts_value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_equal, equal_pred->Bind(*schema, true));

  AssertProjectionStrict(*spec, *schema, bound_less_than, Expression::Operation::kLt);
  AssertProjectionStrict(*spec, *schema, bound_equal, Expression::Operation::kFalse);
}

TEST_F(TimestampProjectionTest, MonthInclusive) {
  int64_t ts_value = TemporalTestHelper::CreateTimestamp({.year = 2017,
                                                          .month = 12,
                                                          .day = 1,
                                                          .hour = 0,
                                                          .minute = 0,
                                                          .second = 0,
                                                          .microsecond = 0});
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(1, "timestamp", timestamp())},
      0);
  auto month_transform = Transform::Month();
  PartitionField pt_field(1, 1000, "timestamp_month", month_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec, PartitionSpec::Make(0, {pt_field}));

  auto less_than_pred = Expressions::LessThan("timestamp", Literal::Timestamp(ts_value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_less_than, less_than_pred->Bind(*schema, true));

  auto equal_pred = Expressions::Equal("timestamp", Literal::Timestamp(ts_value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_equal, equal_pred->Bind(*schema, true));

  AssertProjectionInclusive(*spec, *schema, bound_less_than,
                            Expression::Operation::kLtEq);
  AssertProjectionInclusive(*spec, *schema, bound_equal, Expression::Operation::kEq);
}

TEST_F(TimestampProjectionTest, YearStrict) {
  int64_t ts_value = TemporalTestHelper::CreateTimestamp({.year = 2017,
                                                          .month = 1,
                                                          .day = 1,
                                                          .hour = 0,
                                                          .minute = 0,
                                                          .second = 0,
                                                          .microsecond = 0});
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(1, "timestamp", timestamp())},
      0);
  auto year_transform = Transform::Year();
  PartitionField pt_field(1, 1000, "timestamp_year", year_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec, PartitionSpec::Make(0, {pt_field}));

  auto less_than_pred = Expressions::LessThan("timestamp", Literal::Timestamp(ts_value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_less_than, less_than_pred->Bind(*schema, true));

  auto equal_pred = Expressions::Equal("timestamp", Literal::Timestamp(ts_value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_equal, equal_pred->Bind(*schema, true));

  AssertProjectionStrict(*spec, *schema, bound_less_than, Expression::Operation::kLt);
  AssertProjectionStrict(*spec, *schema, bound_equal, Expression::Operation::kFalse);
}

TEST_F(TimestampProjectionTest, YearInclusive) {
  int64_t ts_value = TemporalTestHelper::CreateTimestamp({.year = 2017,
                                                          .month = 1,
                                                          .day = 1,
                                                          .hour = 0,
                                                          .minute = 0,
                                                          .second = 0,
                                                          .microsecond = 0});
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(1, "timestamp", timestamp())},
      0);
  auto year_transform = Transform::Year();
  PartitionField pt_field(1, 1000, "timestamp_year", year_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec, PartitionSpec::Make(0, {pt_field}));

  auto less_than_pred = Expressions::LessThan("timestamp", Literal::Timestamp(ts_value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_less_than, less_than_pred->Bind(*schema, true));

  auto equal_pred = Expressions::Equal("timestamp", Literal::Timestamp(ts_value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_equal, equal_pred->Bind(*schema, true));

  AssertProjectionInclusive(*spec, *schema, bound_less_than,
                            Expression::Operation::kLtEq);
  AssertProjectionInclusive(*spec, *schema, bound_equal, Expression::Operation::kEq);
}

TEST_F(TimestampProjectionTest, HourStrict) {
  int64_t ts_value = TemporalTestHelper::CreateTimestamp({.year = 2017,
                                                          .month = 12,
                                                          .day = 1,
                                                          .hour = 10,
                                                          .minute = 0,
                                                          .second = 0,
                                                          .microsecond = 0});
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(1, "timestamp", timestamp())},
      0);
  auto hour_transform = Transform::Hour();
  PartitionField pt_field(1, 1000, "timestamp_hour", hour_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec, PartitionSpec::Make(0, {pt_field}));

  auto less_than_pred = Expressions::LessThan("timestamp", Literal::Timestamp(ts_value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_less_than, less_than_pred->Bind(*schema, true));

  auto equal_pred = Expressions::Equal("timestamp", Literal::Timestamp(ts_value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_equal, equal_pred->Bind(*schema, true));

  AssertProjectionStrict(*spec, *schema, bound_less_than, Expression::Operation::kLt);
  AssertProjectionStrict(*spec, *schema, bound_equal, Expression::Operation::kFalse);
}

TEST_F(TimestampProjectionTest, HourInclusive) {
  int64_t ts_value = TemporalTestHelper::CreateTimestamp({.year = 2017,
                                                          .month = 12,
                                                          .day = 1,
                                                          .hour = 10,
                                                          .minute = 0,
                                                          .second = 0,
                                                          .microsecond = 0});
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(1, "timestamp", timestamp())},
      0);
  auto hour_transform = Transform::Hour();
  PartitionField pt_field(1, 1000, "timestamp_hour", hour_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec, PartitionSpec::Make(0, {pt_field}));

  auto less_than_pred = Expressions::LessThan("timestamp", Literal::Timestamp(ts_value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_less_than, less_than_pred->Bind(*schema, true));

  auto equal_pred = Expressions::Equal("timestamp", Literal::Timestamp(ts_value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_equal, equal_pred->Bind(*schema, true));

  AssertProjectionInclusive(*spec, *schema, bound_less_than,
                            Expression::Operation::kLtEq);
  AssertProjectionInclusive(*spec, *schema, bound_equal, Expression::Operation::kEq);
}

// Truncate projection tests
class TruncateProjectionTest : public ::testing::Test {
 protected:
  void AssertProjectionStrict(const PartitionSpec& spec, const Schema& schema,
                              const std::shared_ptr<Expression>& filter,
                              Expression::Operation expected_op) {
    auto evaluator = Projections::Strict(spec, schema, true);
    ICEBERG_UNWRAP_OR_FAIL(auto projection, evaluator->Project(filter));
    AssertProjectionOperation(projection, expected_op);
  }

  void AssertProjectionInclusive(const PartitionSpec& spec, const Schema& schema,
                                 const std::shared_ptr<Expression>& filter,
                                 Expression::Operation expected_op) {
    auto evaluator = Projections::Inclusive(spec, schema, true);
    ICEBERG_UNWRAP_OR_FAIL(auto projection, evaluator->Project(filter));
    AssertProjectionOperation(projection, expected_op);
  }
};

TEST_F(TruncateProjectionTest, IntegerStrict) {
  int32_t value = 100;
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(1, "value", int32())}, 0);
  auto truncate_transform = Transform::Truncate(10);
  PartitionField pt_field(1, 1000, "value_trunc", truncate_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec, PartitionSpec::Make(0, {pt_field}));

  auto less_than_pred = Expressions::LessThan("value", Literal::Int(value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_less_than, less_than_pred->Bind(*schema, true));

  auto equal_pred = Expressions::Equal("value", Literal::Int(value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_equal, equal_pred->Bind(*schema, true));

  AssertProjectionStrict(*spec, *schema, bound_less_than, Expression::Operation::kLt);
  AssertProjectionStrict(*spec, *schema, bound_equal, Expression::Operation::kFalse);
}

TEST_F(TruncateProjectionTest, IntegerInclusive) {
  int32_t value = 100;
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(1, "value", int32())}, 0);
  auto truncate_transform = Transform::Truncate(10);
  PartitionField pt_field(1, 1000, "value_trunc", truncate_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec, PartitionSpec::Make(0, {pt_field}));

  auto less_than_pred = Expressions::LessThan("value", Literal::Int(value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_less_than, less_than_pred->Bind(*schema, true));

  auto equal_pred = Expressions::Equal("value", Literal::Int(value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_equal, equal_pred->Bind(*schema, true));

  AssertProjectionInclusive(*spec, *schema, bound_less_than,
                            Expression::Operation::kLtEq);
  AssertProjectionInclusive(*spec, *schema, bound_equal, Expression::Operation::kEq);
}

TEST_F(TruncateProjectionTest, LongStrict) {
  int64_t value = 100L;
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(1, "value", int64())}, 0);
  auto truncate_transform = Transform::Truncate(10);
  PartitionField pt_field(1, 1000, "value_trunc", truncate_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec, PartitionSpec::Make(0, {pt_field}));

  auto less_than_pred = Expressions::LessThan("value", Literal::Long(value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_less_than, less_than_pred->Bind(*schema, true));

  auto equal_pred = Expressions::Equal("value", Literal::Long(value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_equal, equal_pred->Bind(*schema, true));

  AssertProjectionStrict(*spec, *schema, bound_less_than, Expression::Operation::kLt);
  AssertProjectionStrict(*spec, *schema, bound_equal, Expression::Operation::kFalse);
}

TEST_F(TruncateProjectionTest, LongInclusive) {
  int64_t value = 100L;
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(1, "value", int64())}, 0);
  auto truncate_transform = Transform::Truncate(10);
  PartitionField pt_field(1, 1000, "value_trunc", truncate_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec, PartitionSpec::Make(0, {pt_field}));

  auto less_than_pred = Expressions::LessThan("value", Literal::Long(value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_less_than, less_than_pred->Bind(*schema, true));

  auto equal_pred = Expressions::Equal("value", Literal::Long(value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_equal, equal_pred->Bind(*schema, true));

  AssertProjectionInclusive(*spec, *schema, bound_less_than,
                            Expression::Operation::kLtEq);
  AssertProjectionInclusive(*spec, *schema, bound_equal, Expression::Operation::kEq);
}

TEST_F(TruncateProjectionTest, StringStrict) {
  std::string value = "abcdefg";
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(1, "value", string())}, 0);
  auto truncate_transform = Transform::Truncate(5);
  PartitionField pt_field(1, 1000, "value_trunc", truncate_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec, PartitionSpec::Make(0, {pt_field}));

  auto less_than_pred = Expressions::LessThan("value", Literal::String(value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_less_than, less_than_pred->Bind(*schema, true));

  auto equal_pred = Expressions::Equal("value", Literal::String(value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_equal, equal_pred->Bind(*schema, true));

  AssertProjectionStrict(*spec, *schema, bound_less_than, Expression::Operation::kLt);
  AssertProjectionStrict(*spec, *schema, bound_equal, Expression::Operation::kFalse);
}

TEST_F(TruncateProjectionTest, StringInclusive) {
  std::string value = "abcdefg";
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(1, "value", string())}, 0);
  auto truncate_transform = Transform::Truncate(5);
  PartitionField pt_field(1, 1000, "value_trunc", truncate_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec, PartitionSpec::Make(0, {pt_field}));

  auto less_than_pred = Expressions::LessThan("value", Literal::String(value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_less_than, less_than_pred->Bind(*schema, true));

  auto equal_pred = Expressions::Equal("value", Literal::String(value));
  ICEBERG_UNWRAP_OR_FAIL(auto bound_equal, equal_pred->Bind(*schema, true));

  AssertProjectionInclusive(*spec, *schema, bound_less_than,
                            Expression::Operation::kLtEq);
  AssertProjectionInclusive(*spec, *schema, bound_equal, Expression::Operation::kEq);
}

// Complex expression tests
TEST_F(ProjectionsTest, ComplexExpressionWithOr) {
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{
          SchemaField::MakeRequired(1, "id", int64()),
          SchemaField::MakeOptional(2, "data", string()),
          SchemaField::MakeRequired(3, "hour", int32()),
          SchemaField::MakeRequired(4, "dateint", int32()),
      },
      0);

  auto identity_transform = Transform::Identity();
  PartitionField pt_field(4, 1000, "dateint", identity_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec, PartitionSpec::Make(*schema, 0, {pt_field}, false));

  // Create filter: dateint = 20180416 OR ((dateint = 20180415 AND hour >= 20) OR
  // (dateint = 20180417 AND hour <= 4))
  auto dateint_eq1 = Expressions::Equal("dateint", Literal::Int(20180416));
  auto dateint_eq2 = Expressions::Equal("dateint", Literal::Int(20180415));
  auto hour_ge = Expressions::GreaterThanOrEqual("hour", Literal::Int(20));
  auto dateint_eq3 = Expressions::Equal("dateint", Literal::Int(20180417));
  auto hour_le = Expressions::LessThanOrEqual("hour", Literal::Int(4));

  auto and1 = Expressions::And(dateint_eq2, hour_ge);
  auto and2 = Expressions::And(dateint_eq3, hour_le);
  auto or1 = Expressions::Or(and1, and2);
  auto filter = Expressions::Or(dateint_eq1, or1);

  // Project
  auto evaluator = Projections::Inclusive(*spec, *schema, true);
  ICEBERG_UNWRAP_OR_FAIL(auto projection, evaluator->Project(filter));

  // The projection should be an OR expression
  // Non-partition predicates (hour) are removed, and AND expressions simplify
  // Expected: dateint = 20180416 OR (dateint = 20180415 OR dateint = 20180417)
  EXPECT_EQ(projection->op(), Expression::Operation::kOr);

  auto or_expr = internal::checked_pointer_cast<Or>(projection);

  // Left side: dateint = 20180416
  auto dateint1_expr =
      std::dynamic_pointer_cast<UnboundPredicateImpl<BoundReference>>(or_expr->left());
  EXPECT_EQ(dateint1_expr->reference()->name(), "dateint");
  EXPECT_EQ(dateint1_expr->op(), Expression::Operation::kEq);
  EXPECT_EQ(dateint1_expr->literals().front(), Literal::Int(20180416));

  // Right side: OR of the two dateint predicates (AND expressions simplified)
  auto or1_expr = internal::checked_pointer_cast<Or>(or_expr->right());
  EXPECT_EQ(or1_expr->op(), Expression::Operation::kOr);

  // Left of inner OR: dateint = 20180415 (simplified from AND with hour >= 20)
  auto dateint2_expr =
      std::dynamic_pointer_cast<UnboundPredicateImpl<BoundReference>>(or1_expr->left());
  EXPECT_EQ(dateint2_expr->reference()->name(), "dateint");
  EXPECT_EQ(dateint2_expr->op(), Expression::Operation::kEq);
  EXPECT_EQ(dateint2_expr->literals().front(), Literal::Int(20180415));

  // Right of inner OR: dateint = 20180417 (simplified from AND with hour <= 4)
  auto dateint3_expr =
      std::dynamic_pointer_cast<UnboundPredicateImpl<BoundReference>>(or1_expr->right());
  EXPECT_EQ(dateint3_expr->reference()->name(), "dateint");
  EXPECT_EQ(dateint3_expr->op(), Expression::Operation::kEq);
  EXPECT_EQ(dateint3_expr->literals().front(), Literal::Int(20180417));
}

}  // namespace iceberg
