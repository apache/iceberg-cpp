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

#include "iceberg/expression/expression.h"
#include "iceberg/expression/expression_visitor.h"
#include "iceberg/expression/predicate.h"
#include "iceberg/partition_spec.h"
#include "iceberg/row/struct_like.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
#include "iceberg/transform.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

std::shared_ptr<Expression> always_true() { return True::Instance(); }
std::shared_ptr<Expression> always_false() { return False::Instance(); }

class ResidualVisitor : public BoundVisitor<std::shared_ptr<Expression>> {
 public:
  static Result<ResidualVisitor> Make(const PartitionSpec& spec, const Schema& schema,
                                      const StructLike& partition_data,
                                      bool case_sensitive) {
    ICEBERG_ASSIGN_OR_RAISE(auto partition_type, spec.PartitionType(schema));
    auto partition_schema = FromStructType(std::move(*partition_type), std::nullopt);
    return ResidualVisitor(spec, schema, std::move(partition_schema), partition_data,
                           case_sensitive);
  }

  Result<std::shared_ptr<Expression>> AlwaysTrue() override { return always_true(); }

  Result<std::shared_ptr<Expression>> AlwaysFalse() override { return always_false(); }

  Result<std::shared_ptr<Expression>> Not(
      const std::shared_ptr<Expression>& child_result) override {
    return Not::MakeFolded(child_result);
  }

  Result<std::shared_ptr<Expression>> And(
      const std::shared_ptr<Expression>& left_result,
      const std::shared_ptr<Expression>& right_result) override {
    return And::MakeFolded(left_result, right_result);
  }

  Result<std::shared_ptr<Expression>> Or(
      const std::shared_ptr<Expression>& left_result,
      const std::shared_ptr<Expression>& right_result) override {
    return Or::MakeFolded(left_result, right_result);
  }

  Result<std::shared_ptr<Expression>> IsNull(
      const std::shared_ptr<Bound>& expr) override {
    return expr->Evaluate(partition_data_).transform([](const auto& value) {
      return value.IsNull() ? always_true() : always_false();
    });
  }

  Result<std::shared_ptr<Expression>> NotNull(
      const std::shared_ptr<Bound>& expr) override {
    return expr->Evaluate(partition_data_).transform([](const auto& value) {
      return value.IsNull() ? always_false() : always_true();
    });
  }

  Result<std::shared_ptr<Expression>> IsNaN(const std::shared_ptr<Bound>& expr) override {
    return expr->Evaluate(partition_data_).transform([](const auto& value) {
      return value.IsNaN() ? always_true() : always_false();
    });
  }

  Result<std::shared_ptr<Expression>> NotNaN(
      const std::shared_ptr<Bound>& expr) override {
    return expr->Evaluate(partition_data_).transform([](const auto& value) {
      return value.IsNaN() ? always_false() : always_true();
    });
  }

  Result<std::shared_ptr<Expression>> Lt(const std::shared_ptr<Bound>& expr,
                                         const Literal& lit) override {
    return expr->Evaluate(partition_data_).transform([&lit](const auto& value) {
      return value < lit ? always_true() : always_false();
    });
  }

  Result<std::shared_ptr<Expression>> LtEq(const std::shared_ptr<Bound>& expr,
                                           const Literal& lit) override {
    return expr->Evaluate(partition_data_).transform([&lit](const auto& value) {
      return value <= lit ? always_true() : always_false();
    });
  }

  Result<std::shared_ptr<Expression>> Gt(const std::shared_ptr<Bound>& expr,
                                         const Literal& lit) override {
    return expr->Evaluate(partition_data_).transform([&lit](const auto& value) {
      return value > lit ? always_true() : always_false();
    });
  }

  Result<std::shared_ptr<Expression>> GtEq(const std::shared_ptr<Bound>& expr,
                                           const Literal& lit) override {
    return expr->Evaluate(partition_data_).transform([&lit](const auto& value) {
      return value >= lit ? always_true() : always_false();
    });
  }

  Result<std::shared_ptr<Expression>> Eq(const std::shared_ptr<Bound>& expr,
                                         const Literal& lit) override {
    return expr->Evaluate(partition_data_).transform([&lit](const auto& value) {
      return value == lit ? always_true() : always_false();
    });
  }

  Result<std::shared_ptr<Expression>> NotEq(const std::shared_ptr<Bound>& expr,
                                            const Literal& lit) override {
    return expr->Evaluate(partition_data_).transform([&lit](const auto& value) {
      return value != lit ? always_true() : always_false();
    });
  }

  Result<std::shared_ptr<Expression>> StartsWith(const std::shared_ptr<Bound>& expr,
                                                 const Literal& lit) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, expr->Evaluate(partition_data_));

    if (!std::holds_alternative<std::string>(value.value()) ||
        !std::holds_alternative<std::string>(lit.value())) {
      return InvalidExpression("Both value and literal should be strings");
    }

    const auto& str_value = std::get<std::string>(value.value());
    const auto& str_prefix = std::get<std::string>(lit.value());
    return str_value.starts_with(str_prefix) ? always_true() : always_false();
  }

  Result<std::shared_ptr<Expression>> NotStartsWith(const std::shared_ptr<Bound>& expr,
                                                    const Literal& lit) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, expr->Evaluate(partition_data_));

    if (!std::holds_alternative<std::string>(value.value()) ||
        !std::holds_alternative<std::string>(lit.value())) {
      return InvalidExpression("Both value and literal should be strings");
    }

    const auto& str_value = std::get<std::string>(value.value());
    const auto& str_prefix = std::get<std::string>(lit.value());
    return str_value.starts_with(str_prefix) ? always_false() : always_true();
  }

  Result<std::shared_ptr<Expression>> In(
      const std::shared_ptr<Bound>& expr,
      const BoundSetPredicate::LiteralSet& literal_set) override {
    return expr->Evaluate(partition_data_).transform([&literal_set](const auto& value) {
      return literal_set.contains(value) ? always_true() : always_false();
    });
  }

  Result<std::shared_ptr<Expression>> NotIn(
      const std::shared_ptr<Bound>& expr,
      const BoundSetPredicate::LiteralSet& literal_set) override {
    return expr->Evaluate(partition_data_).transform([&literal_set](const auto& value) {
      return literal_set.contains(value) ? always_false() : always_true();
    });
  }

  Result<std::shared_ptr<Expression>> Predicate(
      const std::shared_ptr<BoundPredicate>& pred) override;

  Result<std::shared_ptr<Expression>> Predicate(
      const std::shared_ptr<UnboundPredicate>& pred) override {
    ICEBERG_ASSIGN_OR_RAISE(auto bound, pred->Bind(schema_, case_sensitive_));
    if (bound->is_bound_predicate()) {
      ICEBERG_ASSIGN_OR_RAISE(
          auto residual, Predicate(std::dynamic_pointer_cast<BoundPredicate>(bound)));
      if (residual->is_bound_predicate()) {
        // replace inclusive original unbound predicate
        return pred;
      }
      return residual;
    }
    // if binding didn't result in a Predicate, return the expression
    return bound;
  }

 private:
  ResidualVisitor(const PartitionSpec& spec, const Schema& schema,
                  std::unique_ptr<Schema> partition_schema,
                  const StructLike& partition_data, bool case_sensitive)
      : spec_(spec),
        schema_(schema),
        partition_schema_(std::move(partition_schema)),
        partition_data_(partition_data),
        case_sensitive_(case_sensitive) {}

  const PartitionSpec& spec_;
  const Schema& schema_;
  std::unique_ptr<Schema> partition_schema_;
  const StructLike& partition_data_;
  bool case_sensitive_;
};

Result<std::shared_ptr<Expression>> ResidualVisitor::Predicate(
    const std::shared_ptr<BoundPredicate>& pred) {
  // Get the strict projection and inclusive projection of this predicate in partition
  // data, then use them to determine whether to return the original predicate. The
  // strict projection returns true iff the original predicate would have returned true,
  // so the predicate can be eliminated if the strict projection evaluates to true.
  // Similarly the inclusive projection returns false iff the original predicate would
  // have returned false, so the predicate can also be eliminated if the inclusive
  // projection evaluates to false.

  // If there is no strict projection or if it evaluates to false, then return the
  // predicate.
  ICEBERG_ASSIGN_OR_RAISE(
      auto parts, spec_.GetFieldsBySourceId(pred->reference()->field().field_id()));
  if (parts.empty()) {
    // Not associated with a partition field, can't be evaluated
    return pred;
  }

  for (const auto& part : parts) {
    // Check the strict projection
    ICEBERG_ASSIGN_OR_RAISE(auto strict_projection, part.get().transform()->ProjectStrict(
                                                        part.get().name(), pred));
    std::shared_ptr<Expression> strict_result = nullptr;

    if (strict_projection != nullptr) {
      ICEBERG_ASSIGN_OR_RAISE(
          auto bound_strict,
          strict_projection->Bind(*partition_schema_, case_sensitive_));
      if (bound_strict->is_bound_predicate()) {
        ICEBERG_ASSIGN_OR_RAISE(
            strict_result, BoundVisitor::Predicate(
                               std::dynamic_pointer_cast<BoundPredicate>(bound_strict)));
      } else {
        // If the result is not a predicate, then it must be a constant like alwaysTrue
        // or alwaysFalse
        strict_result = std::move(bound_strict);
      }
    }

    if (strict_result != nullptr && strict_result->op() == Expression::Operation::kTrue) {
      // If strict is true, returning true
      return always_true();
    }

    // Check the inclusive projection
    ICEBERG_ASSIGN_OR_RAISE(auto inclusive_projection,
                            part.get().transform()->Project(part.get().name(), pred));
    std::shared_ptr<Expression> inclusive_result = nullptr;

    if (inclusive_projection != nullptr) {
      ICEBERG_ASSIGN_OR_RAISE(
          auto bound_inclusive,
          inclusive_projection->Bind(*partition_schema_, case_sensitive_));

      if (bound_inclusive->is_bound_predicate()) {
        ICEBERG_ASSIGN_OR_RAISE(
            inclusive_result,
            BoundVisitor::Predicate(
                std::dynamic_pointer_cast<BoundPredicate>(bound_inclusive)));
      } else {
        // If the result is not a predicate, then it must be a constant like alwaysTrue
        // or alwaysFalse
        inclusive_result = std::move(bound_inclusive);
      }
    }

    if (inclusive_result != nullptr &&
        inclusive_result->op() == Expression::Operation::kFalse) {
      // If inclusive is false, returning false
      return always_false();
    }
  }

  // Neither strict nor inclusive predicate was conclusive, returning the original pred
  return pred;
}

// Unpartitioned residual evaluator that always returns the original expression
class UnpartitionedResidualEvaluator : public ResidualEvaluator {
 public:
  explicit UnpartitionedResidualEvaluator(std::shared_ptr<Expression> expr)
      : ResidualEvaluator(std::move(expr), *PartitionSpec::Unpartitioned(),
                          *kEmptySchema_, true) {}

  Result<std::shared_ptr<Expression>> ResidualFor(
      const StructLike& /*partition_data*/) const override {
    return expr_;
  }

 private:
  // Store an empty schema to avoid dangling reference when passing to base class
  inline static const std::shared_ptr<Schema> kEmptySchema_ =
      std::make_shared<Schema>(std::vector<SchemaField>{}, std::nullopt);
};

}  // namespace

ResidualEvaluator::ResidualEvaluator(std::shared_ptr<Expression> expr,
                                     const PartitionSpec& spec, const Schema& schema,
                                     bool case_sensitive)
    : expr_(std::move(expr)),
      spec_(spec),
      schema_(schema),
      case_sensitive_(case_sensitive) {}

ResidualEvaluator::~ResidualEvaluator() = default;

Result<std::unique_ptr<ResidualEvaluator>> ResidualEvaluator::Unpartitioned(
    std::shared_ptr<Expression> expr) {
  return std::unique_ptr<ResidualEvaluator>(
      new UnpartitionedResidualEvaluator(std::move(expr)));
}

Result<std::unique_ptr<ResidualEvaluator>> ResidualEvaluator::Make(
    std::shared_ptr<Expression> expr, const PartitionSpec& spec, const Schema& schema,
    bool case_sensitive) {
  if (spec.fields().empty()) {
    return Unpartitioned(std::move(expr));
  }
  return std::unique_ptr<ResidualEvaluator>(
      new ResidualEvaluator(std::move(expr), spec, schema, case_sensitive));
}

Result<std::shared_ptr<Expression>> ResidualEvaluator::ResidualFor(
    const StructLike& partition_data) const {
  ICEBERG_ASSIGN_OR_RAISE(
      auto visitor,
      ResidualVisitor::Make(spec_, schema_, partition_data, case_sensitive_));
  return Visit<std::shared_ptr<Expression>, ResidualVisitor>(expr_, visitor);
}

}  // namespace iceberg
