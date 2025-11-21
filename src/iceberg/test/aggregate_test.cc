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

#include "iceberg/exception.h"
#include "iceberg/expression/binder.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/row/struct_like.h"
#include "iceberg/schema.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

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
    ASSERT_TRUE(count_evaluator->Add(row).has_value());
    ASSERT_TRUE(count_null_evaluator->Add(row).has_value());
    ASSERT_TRUE(count_star_evaluator->Add(row).has_value());
  }

  ICEBERG_UNWRAP_OR_FAIL(auto count_result, count_evaluator->ResultLiteral());
  EXPECT_EQ(std::get<int64_t>(count_result.value()), 2);

  ICEBERG_UNWRAP_OR_FAIL(auto count_null_result, count_null_evaluator->ResultLiteral());
  EXPECT_EQ(std::get<int64_t>(count_null_result.value()), 1);

  ICEBERG_UNWRAP_OR_FAIL(auto count_star_result, count_star_evaluator->ResultLiteral());
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
    ASSERT_TRUE(max_eval->Add(row).has_value());
    ASSERT_TRUE(min_eval->Add(row).has_value());
  }

  ICEBERG_UNWRAP_OR_FAIL(auto max_result, max_eval->ResultLiteral());
  EXPECT_EQ(std::get<int32_t>(max_result.value()), 12);

  ICEBERG_UNWRAP_OR_FAIL(auto min_result, min_eval->ResultLiteral());
  EXPECT_EQ(std::get<int32_t>(min_result.value()), 2);
}

TEST(AggregateTest, MultipleAggregatesInEvaluator) {
  Schema schema({SchemaField::MakeOptional(1, "id", int32()),
                 SchemaField::MakeOptional(2, "value", int32())});

  auto count_expr = Expressions::Count("id");
  auto max_expr = Expressions::Max("value");

  auto count_bound = BindAggregate(schema, count_expr);
  auto max_bound = BindAggregate(schema, max_expr);

  std::vector<std::shared_ptr<BoundAggregate>> aggregates{count_bound, max_bound};
  ICEBERG_UNWRAP_OR_FAIL(auto evaluator, AggregateEvaluator::MakeList(aggregates));

  std::vector<VectorStructLike> rows{
      VectorStructLike({Scalar{int32_t{1}}, Scalar{int32_t{10}}}),
      VectorStructLike({Scalar{int32_t{2}}, Scalar{std::monostate{}}}),
      VectorStructLike({Scalar{std::monostate{}}, Scalar{int32_t{30}}})};

  for (const auto& row : rows) {
    ASSERT_TRUE(evaluator->Add(row).has_value());
  }

  ICEBERG_UNWRAP_OR_FAIL(auto results, evaluator->Results());
  ASSERT_EQ(results.size(), 2);
  EXPECT_EQ(std::get<int64_t>(results[0].value()), 2);
  EXPECT_EQ(std::get<int32_t>(results[1].value()), 30);
}

}  // namespace iceberg
