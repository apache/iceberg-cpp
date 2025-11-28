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

#include "iceberg/expression/aggregate.h"

#include <gtest/gtest.h>

#include "iceberg/expression/binder.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/row/struct_like.h"
#include "iceberg/schema.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"

namespace iceberg {

namespace {

/// XXX: `Scalar` carries view semantics, so it is unsafe to use std::string_view variant.
class VectorStructLike : public StructLike {
 public:
  explicit VectorStructLike(std::vector<Scalar> fields) : fields_(std::move(fields)) {}

  Result<Scalar> GetField(size_t pos) const override {
    if (pos >= fields_.size()) {
      return InvalidArgument("Position {} out of range", pos);
    }
    return fields_[pos];
  }

  size_t num_fields() const override { return fields_.size(); }

 private:
  std::vector<Scalar> fields_;
};

std::shared_ptr<BoundAggregate> BindAggregate(const Schema& schema,
                                              const std::shared_ptr<Expression>& expr) {
  auto result = Binder::Bind(schema, expr, /*case_sensitive=*/true);
  EXPECT_TRUE(result.has_value())
      << "Failed to bind aggregate: " << result.error().message;
  auto bound = std::dynamic_pointer_cast<BoundAggregate>(std::move(result).value());
  EXPECT_NE(bound, nullptr);
  return bound;
}

}  // namespace

TEST(AggregateTest, CountVariants) {
  Schema schema({SchemaField::MakeOptional(1, "id", int32()),
                 SchemaField::MakeOptional(2, "value", int32())});

  auto count_expr = Expressions::Count("id");
  auto count_bound = BindAggregate(schema, count_expr);
  auto count_evaluator = AggregateEvaluator::Make(count_bound).value();

  auto count_null_expr = Expressions::CountNull("value");
  auto count_null_bound = BindAggregate(schema, count_null_expr);
  auto count_null_evaluator = AggregateEvaluator::Make(count_null_bound).value();

  auto count_star_expr = Expressions::CountStar();
  auto count_star_bound = BindAggregate(schema, count_star_expr);
  auto count_star_evaluator = AggregateEvaluator::Make(count_star_bound).value();

  std::vector<VectorStructLike> rows{
      VectorStructLike({Scalar{int32_t{1}}, Scalar{int32_t{10}}}),
      VectorStructLike({Scalar{int32_t{2}}, Scalar{std::monostate{}}}),
      VectorStructLike({Scalar{std::monostate{}}, Scalar{int32_t{30}}})};

  for (const auto& row : rows) {
    ASSERT_TRUE(count_evaluator->Update(row).has_value());
    ASSERT_TRUE(count_null_evaluator->Update(row).has_value());
    ASSERT_TRUE(count_star_evaluator->Update(row).has_value());
  }

  ICEBERG_UNWRAP_OR_FAIL(auto count_result, count_evaluator->GetResult());
  EXPECT_EQ(std::get<int64_t>(count_result.value()), 2);

  ICEBERG_UNWRAP_OR_FAIL(auto count_null_result, count_null_evaluator->GetResult());
  EXPECT_EQ(std::get<int64_t>(count_null_result.value()), 1);

  ICEBERG_UNWRAP_OR_FAIL(auto count_star_result, count_star_evaluator->GetResult());
  EXPECT_EQ(std::get<int64_t>(count_star_result.value()), 3);
}

TEST(AggregateTest, MaxMinAggregates) {
  Schema schema({SchemaField::MakeOptional(1, "value", int32())});

  auto max_expr = Expressions::Max("value");
  auto min_expr = Expressions::Min("value");

  auto max_bound = BindAggregate(schema, max_expr);
  auto min_bound = BindAggregate(schema, min_expr);

  auto max_eval = AggregateEvaluator::Make(max_bound).value();
  auto min_eval = AggregateEvaluator::Make(min_bound).value();

  std::vector<VectorStructLike> rows{VectorStructLike({Scalar{int32_t{5}}}),
                                     VectorStructLike({Scalar{std::monostate{}}}),
                                     VectorStructLike({Scalar{int32_t{2}}}),
                                     VectorStructLike({Scalar{int32_t{12}}})};

  for (const auto& row : rows) {
    ASSERT_TRUE(max_eval->Update(row).has_value());
    ASSERT_TRUE(min_eval->Update(row).has_value());
  }

  ICEBERG_UNWRAP_OR_FAIL(auto max_result, max_eval->GetResult());
  EXPECT_EQ(std::get<int32_t>(max_result.value()), 12);

  ICEBERG_UNWRAP_OR_FAIL(auto min_result, min_eval->GetResult());
  EXPECT_EQ(std::get<int32_t>(min_result.value()), 2);
}

TEST(AggregateTest, UnboundAggregateCreationAndBinding) {
  Schema schema({SchemaField::MakeOptional(1, "id", int32()),
                 SchemaField::MakeOptional(2, "value", int32())});

  auto count = Expressions::Count("id");
  auto count_null = Expressions::CountNull("id");
  auto count_star = Expressions::CountStar();
  auto max = Expressions::Max("value");
  auto min = Expressions::Min("value");

  EXPECT_EQ(count->ToString(), "count(ref(name=\"id\"))");
  EXPECT_EQ(count_null->ToString(), "count_if(ref(name=\"id\") is null)");
  EXPECT_EQ(count_star->ToString(), "count(*)");
  EXPECT_EQ(max->ToString(), "max(ref(name=\"value\"))");
  EXPECT_EQ(min->ToString(), "min(ref(name=\"value\"))");

  // Bind succeeds for existing columns
  EXPECT_TRUE(Binder::Bind(schema, count, /*case_sensitive=*/true).has_value());
  EXPECT_TRUE(Binder::Bind(schema, count_null, /*case_sensitive=*/true).has_value());
  EXPECT_TRUE(Binder::Bind(schema, count_star, /*case_sensitive=*/true).has_value());
  EXPECT_TRUE(Binder::Bind(schema, max, /*case_sensitive=*/true).has_value());
  EXPECT_TRUE(Binder::Bind(schema, min, /*case_sensitive=*/true).has_value());

  // Binding fails when the reference is missing
  auto missing_count = Expressions::Count("missing");
  auto missing_bind = Binder::Bind(schema, missing_count, /*case_sensitive=*/true);
  EXPECT_THAT(missing_bind, IsError(ErrorKind::kInvalidExpression));

  // Creating a value aggregate with null term should fail
  auto invalid_unbound =
      UnboundAggregateImpl<BoundReference>::Make(Expression::Operation::kMax, nullptr);
  EXPECT_THAT(invalid_unbound, IsError(ErrorKind::kInvalidExpression));
}

TEST(AggregateTest, BoundAggregateEvaluateDirectly) {
  Schema schema({SchemaField::MakeOptional(1, "id", int32()),
                 SchemaField::MakeOptional(2, "value", int32())});

  auto count_bound = BindAggregate(schema, Expressions::Count("id"));
  auto count_null_bound = BindAggregate(schema, Expressions::CountNull("value"));
  auto max_bound = BindAggregate(schema, Expressions::Max("value"));

  std::vector<VectorStructLike> rows{
      VectorStructLike({Scalar{int32_t{1}}, Scalar{int32_t{10}}}),
      VectorStructLike({Scalar{int32_t{2}}, Scalar{std::monostate{}}}),
      VectorStructLike({Scalar{std::monostate{}}, Scalar{int32_t{30}}}),
      VectorStructLike({Scalar{int32_t{3}}, Scalar{int32_t{2}}})};

  int64_t count_sum = 0;
  int64_t count_null_sum = 0;
  int32_t max_val = std::numeric_limits<int32_t>::min();
  for (const auto& row : rows) {
    ICEBERG_UNWRAP_OR_FAIL(auto c, count_bound->Evaluate(row));
    count_sum += std::get<int64_t>(c.value());

    ICEBERG_UNWRAP_OR_FAIL(auto cn, count_null_bound->Evaluate(row));
    count_null_sum += std::get<int64_t>(cn.value());

    ICEBERG_UNWRAP_OR_FAIL(auto mv, max_bound->Evaluate(row));
    if (!mv.IsNull()) {
      max_val = std::max(max_val, std::get<int32_t>(mv.value()));
    }
  }

  EXPECT_EQ(count_sum, 3);
  EXPECT_EQ(count_null_sum, 1);
  EXPECT_EQ(max_val, 30);
}

TEST(AggregateTest, MultipleAggregatesInEvaluator) {
  Schema schema({SchemaField::MakeOptional(1, "id", int32()),
                 SchemaField::MakeOptional(2, "value", int32())});

  auto count_expr = Expressions::Count("id");
  auto max_expr = Expressions::Max("value");
  auto min_expr = Expressions::Min("value");
  auto count_null_expr = Expressions::CountNull("value");
  auto count_star_expr = Expressions::CountStar();

  auto count_bound = BindAggregate(schema, count_expr);
  auto max_bound = BindAggregate(schema, max_expr);
  auto min_bound = BindAggregate(schema, min_expr);
  auto count_null_bound = BindAggregate(schema, count_null_expr);
  auto count_star_bound = BindAggregate(schema, count_star_expr);

  std::vector<std::shared_ptr<BoundAggregate>> aggregates{
      count_bound, max_bound, min_bound, count_null_bound, count_star_bound};
  ICEBERG_UNWRAP_OR_FAIL(auto evaluator, AggregateEvaluator::Make(aggregates));

  std::vector<VectorStructLike> rows{
      VectorStructLike({Scalar{int32_t{1}}, Scalar{int32_t{10}}}),
      VectorStructLike({Scalar{int32_t{2}}, Scalar{std::monostate{}}}),
      VectorStructLike({Scalar{std::monostate{}}, Scalar{int32_t{30}}}),
      VectorStructLike({Scalar{int32_t{3}}, Scalar{int32_t{2}}})};

  for (const auto& row : rows) {
    ASSERT_TRUE(evaluator->Update(row).has_value());
  }

  ICEBERG_UNWRAP_OR_FAIL(auto results, evaluator->GetResults());
  ASSERT_EQ(results.size(), 5);
  EXPECT_EQ(std::get<int64_t>(results[0].value()), 3);   // count
  EXPECT_EQ(std::get<int32_t>(results[1].value()), 30);  // max
  EXPECT_EQ(std::get<int32_t>(results[2].value()), 2);   // min
  EXPECT_EQ(std::get<int64_t>(results[3].value()), 1);   // count_null
  EXPECT_EQ(std::get<int64_t>(results[4].value()), 4);   // count_star
}

}  // namespace iceberg
