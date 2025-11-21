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

#include "iceberg/expression/evaluator.h"

#include "iceberg/expression/binder.h"
#include "iceberg/expression/expression_visitor.h"
#include "iceberg/schema.h"
#include "iceberg/util/macros.h"

namespace iceberg {

class EvalVisitor : public BoundVisitor<bool> {
 public:
  explicit EvalVisitor(const StructLike& row) : row_(row) {}

  Result<bool> AlwaysTrue() override { return true; }

  Result<bool> AlwaysFalse() override { return false; }

  Result<bool> Not(bool child_result) override { return !child_result; }

  Result<bool> And(bool left_result, bool right_result) override {
    return left_result && right_result;
  }

  Result<bool> Or(bool left_result, bool right_result) override {
    return left_result || right_result;
  }

  Result<bool> IsNull(const std::shared_ptr<BoundTerm>& term) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, term->Evaluate(row_));
    return value.IsNull();
  }

  Result<bool> NotNull(const std::shared_ptr<BoundTerm>& term) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, IsNull(term));
    return !value;
  }

  Result<bool> IsNaN(const std::shared_ptr<BoundTerm>& term) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, term->Evaluate(row_));
    return value.IsNaN();
  }

  Result<bool> NotNaN(const std::shared_ptr<BoundTerm>& term) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, IsNaN(term));
    return !value;
  }

  Result<bool> Lt(const std::shared_ptr<BoundTerm>& term, const Literal& lit) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, term->Evaluate(row_));
    return value < lit;
  }

  Result<bool> LtEq(const std::shared_ptr<BoundTerm>& term, const Literal& lit) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, term->Evaluate(row_));
    return value <= lit;
  }

  Result<bool> Gt(const std::shared_ptr<BoundTerm>& term, const Literal& lit) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, term->Evaluate(row_));
    return value > lit;
  }

  Result<bool> GtEq(const std::shared_ptr<BoundTerm>& term, const Literal& lit) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, term->Evaluate(row_));
    return value >= lit;
  }

  Result<bool> Eq(const std::shared_ptr<BoundTerm>& term, const Literal& lit) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, term->Evaluate(row_));
    return value == lit;
  }

  Result<bool> NotEq(const std::shared_ptr<BoundTerm>& term,
                     const Literal& lit) override {
    ICEBERG_ASSIGN_OR_RAISE(auto eq_result, Eq(term, lit));
    return !eq_result;
  }

  Result<bool> In(const std::shared_ptr<BoundTerm>& term,
                  const BoundSetPredicate::LiteralSet& literal_set) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, term->Evaluate(row_));
    return literal_set.contains(value);
  }

  Result<bool> NotIn(const std::shared_ptr<BoundTerm>& term,
                     const BoundSetPredicate::LiteralSet& literal_set) override {
    ICEBERG_ASSIGN_OR_RAISE(auto in_result, In(term, literal_set));
    return !in_result;
  }

  Result<bool> StartsWith(const std::shared_ptr<BoundTerm>& term,
                          const Literal& lit) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, term->Evaluate(row_));

    // Both value and literal should be strings
    if (!std::holds_alternative<std::string>(value.value()) ||
        !std::holds_alternative<std::string>(lit.value())) {
      return false;
    }

    const auto& str_value = std::get<std::string>(value.value());
    const auto& str_prefix = std::get<std::string>(lit.value());
    return str_value.starts_with(str_prefix);
  }

  Result<bool> NotStartsWith(const std::shared_ptr<BoundTerm>& term,
                             const Literal& lit) override {
    ICEBERG_ASSIGN_OR_RAISE(auto starts_result, StartsWith(term, lit));
    return !starts_result;
  }

 private:
  const StructLike& row_;
};

Evaluator::Evaluator(std::shared_ptr<Expression> bound_expr)
    : bound_expr_(std::move(bound_expr)) {}

Evaluator::~Evaluator() = default;

Result<std::unique_ptr<Evaluator>> Evaluator::Make(const Schema& schema,
                                                   std::shared_ptr<Expression> unbound,
                                                   bool case_sensitive) {
  ICEBERG_ASSIGN_OR_RAISE(auto bound_expr, Binder::Bind(schema, unbound, case_sensitive));
  return std::unique_ptr<Evaluator>(new Evaluator(std::move(bound_expr)));
}

Result<bool> Evaluator::Eval(const StructLike& row) const {
  EvalVisitor visitor(row);
  return Visit<bool, EvalVisitor>(bound_expr_, visitor);
}

}  // namespace iceberg
