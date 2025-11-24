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

#include "iceberg/expression/binder.h"
#include "iceberg/expression/expression_visitor.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/expression/rewrite_not.h"
#include "iceberg/schema.h"
#include "iceberg/util/macros.h"

namespace iceberg {

class ResidualEvalVisitor : public BoundVisitor<std::shared_ptr<Expression>> {
 public:
  explicit ResidualEvalVisitor(const StructLike& partition_data,
                               std::shared_ptr<Schema> schema, bool case_sensitive)
      : case_sensitive_(case_sensitive),
        schema_(schema),
        partition_data_(partition_data) {}

  Result<std::shared_ptr<Expression>> AlwaysTrue() override {
    return Expressions::AlwaysTrue();
  }

  Result<std::shared_ptr<Expression>> AlwaysFalse() override {
    return Expressions::AlwaysFalse();
  }

  Result<std::shared_ptr<Expression>> Not(
      const std::shared_ptr<Expression>& child_result) override {
    return Expressions::Not(child_result);
  }

  Result<std::shared_ptr<Expression>> And(
      const std::shared_ptr<Expression>& left_result,
      const std::shared_ptr<Expression>& right_result) override {
    return Expressions::And(left_result, right_result);
  }

  Result<std::shared_ptr<Expression>> Or(
      const std::shared_ptr<Expression>& left_result,
      const std::shared_ptr<Expression>& right_result) override {
    return Expressions::Or(left_result, right_result);
  }

  Result<std::shared_ptr<Expression>> IsNull(
      const std::shared_ptr<BoundTerm>& term) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, term->Evaluate(partition_data_));
    return value.IsNull() ? AlwaysTrue() : AlwaysFalse();
  }

  Result<std::shared_ptr<Expression>> NotNull(
      const std::shared_ptr<BoundTerm>& term) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, term->Evaluate(partition_data_));
    return value.IsNull() ? AlwaysFalse() : AlwaysTrue();
  }

  Result<std::shared_ptr<Expression>> IsNaN(
      const std::shared_ptr<BoundTerm>& term) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, term->Evaluate(partition_data_));
    return value.IsNaN() ? AlwaysTrue() : AlwaysFalse();
  }

  Result<std::shared_ptr<Expression>> NotNaN(
      const std::shared_ptr<BoundTerm>& term) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, term->Evaluate(partition_data_));
    return value.IsNaN() ? AlwaysFalse() : AlwaysTrue();
  }

  Result<std::shared_ptr<Expression>> Lt(const std::shared_ptr<BoundTerm>& term,
                                         const Literal& lit) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, term->Evaluate(partition_data_));
    return value < lit ? AlwaysTrue() : AlwaysFalse();
  }

  Result<std::shared_ptr<Expression>> LtEq(const std::shared_ptr<BoundTerm>& term,
                                           const Literal& lit) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, term->Evaluate(partition_data_));
    return value <= lit ? AlwaysTrue() : AlwaysFalse();
  }

  Result<std::shared_ptr<Expression>> Gt(const std::shared_ptr<BoundTerm>& term,
                                         const Literal& lit) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, term->Evaluate(partition_data_));
    return value > lit ? AlwaysTrue() : AlwaysFalse();
  }

  Result<std::shared_ptr<Expression>> GtEq(const std::shared_ptr<BoundTerm>& term,
                                           const Literal& lit) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, term->Evaluate(partition_data_));
    return value >= lit ? AlwaysTrue() : AlwaysFalse();
  }

  Result<std::shared_ptr<Expression>> Eq(const std::shared_ptr<BoundTerm>& term,
                                         const Literal& lit) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, term->Evaluate(partition_data_));
    return value == lit ? AlwaysTrue() : AlwaysFalse();
  }

  Result<std::shared_ptr<Expression>> NotEq(const std::shared_ptr<BoundTerm>& term,
                                            const Literal& lit) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, term->Evaluate(partition_data_));
    return value != lit ? AlwaysTrue() : AlwaysFalse();
  }

  Result<std::shared_ptr<Expression>> In(
      const std::shared_ptr<BoundTerm>& term,
      const BoundSetPredicate::LiteralSet& literal_set) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, term->Evaluate(partition_data_));
    return literal_set.contains(value) ? AlwaysTrue() : AlwaysFalse();
  }

  Result<std::shared_ptr<Expression>> NotIn(
      const std::shared_ptr<BoundTerm>& term,
      const BoundSetPredicate::LiteralSet& literal_set) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, term->Evaluate(partition_data_));
    return literal_set.contains(value) ? AlwaysFalse() : AlwaysTrue();
  }

  Result<std::shared_ptr<Expression>> StartsWith(const std::shared_ptr<BoundTerm>& term,
                                                 const Literal& lit) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, term->Evaluate(partition_data_));

    // Both value and literal should be strings
    if (!std::holds_alternative<std::string>(value.value()) ||
        !std::holds_alternative<std::string>(lit.value())) {
      return AlwaysFalse();
    }

    const auto& str_value = std::get<std::string>(value.value());
    const auto& str_prefix = std::get<std::string>(lit.value());
    return str_value.starts_with(str_prefix) ? AlwaysTrue() : AlwaysFalse();
  }

  Result<std::shared_ptr<Expression>> NotStartsWith(
      const std::shared_ptr<BoundTerm>& term, const Literal& lit) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, term->Evaluate(partition_data_));

    // Both value and literal should be strings
    if (!std::holds_alternative<std::string>(value.value()) ||
        !std::holds_alternative<std::string>(lit.value())) {
      return AlwaysTrue();
    }

    const auto& str_value = std::get<std::string>(value.value());
    const auto& str_prefix = std::get<std::string>(lit.value());
    return str_value.starts_with(str_prefix) ? AlwaysFalse() : AlwaysTrue();
  }

  Result<std::shared_ptr<Expression>> Predicate(
      const std::shared_ptr<BoundPredicate>& pred) override {
    // TODO(xiao.dong) need to do Transform::project
    return nullptr;
  }

  Result<std::shared_ptr<Expression>> Predicate(
      const std::shared_ptr<UnboundPredicate>& pred) override {
    ICEBERG_ASSIGN_OR_RAISE(auto bound, pred->Bind(*schema_, case_sensitive_));
    if (bound->is_bound_predicate()) {
      ICEBERG_ASSIGN_OR_RAISE(
          auto bound_residual,
          Predicate(std::dynamic_pointer_cast<BoundPredicate>(bound)));
      if (bound_residual->is_bound_predicate() ||
          bound_residual->is_unbound_predicate()) {
        return pred;  // replace inclusive original unbound predicate
      }
      return bound_residual;  // use the non-predicate residual (e.g. alwaysTrue)
    }
    // if binding didn't result in a Predicate, return the expression
    return bound;
  }

 private:
  bool case_sensitive_;
  std::shared_ptr<Schema> schema_;
  const StructLike& partition_data_;
};

class UnpartitionedResidualEvaluator : public ResidualEvaluator {
 public:
  UnpartitionedResidualEvaluator(std::shared_ptr<Expression> expr,
                                 std::shared_ptr<Schema> schema, bool case_sensitive)
      : ResidualEvaluator(std::move(expr), std::move(schema), case_sensitive) {}

  Result<std::shared_ptr<Expression>> ResidualFor(
      const StructLike& partition_data) const override {
    return expr_;
  }
};

ResidualEvaluator::ResidualEvaluator(const std::shared_ptr<Expression>& expr,
                                     std::shared_ptr<Schema> schema, bool case_sensitive)
    : case_sensitive_(case_sensitive),
      expr_(std::move(expr)),
      schema_(std::move(schema)) {}

ResidualEvaluator::~ResidualEvaluator() = default;

Result<std::unique_ptr<ResidualEvaluator>> ResidualEvaluator::Make(
    std::shared_ptr<Expression> expr, const std::shared_ptr<PartitionSpec> spec,
    std::shared_ptr<Schema> schema, bool case_sensitive) {
  if (spec->fields().empty()) {
    return MakeUnpartitioned(expr);
  }
  ICEBERG_ASSIGN_OR_RAISE(auto partition_type, spec->PartitionType(*schema));
  auto field_span = partition_type->fields();
  std::vector<SchemaField> fields(field_span.begin(), field_span.end());
  auto partition_schema = std::make_shared<Schema>(fields);
  ICEBERG_ASSIGN_OR_RAISE(auto rewrite_expr, RewriteNot::Visit(std::move(expr)));
  ICEBERG_ASSIGN_OR_RAISE(auto partition_expr,
                          Binder::Bind(*partition_schema, rewrite_expr, case_sensitive));
  return std::unique_ptr<ResidualEvaluator>(new ResidualEvaluator(
      std::move(partition_expr), std::move(schema), case_sensitive));
}

Result<std::unique_ptr<ResidualEvaluator>> ResidualEvaluator::MakeUnpartitioned(
    std::shared_ptr<Expression> expr) {
  return std::make_unique<UnpartitionedResidualEvaluator>(std::move(expr), nullptr, true);
}

Result<std::shared_ptr<Expression>> ResidualEvaluator::ResidualFor(
    const StructLike& partition_data) const {
  ResidualEvalVisitor visitor(partition_data, schema_, case_sensitive_);
  return Visit<std::shared_ptr<Expression>, ResidualEvalVisitor>(expr_, visitor);
}

}  // namespace iceberg
