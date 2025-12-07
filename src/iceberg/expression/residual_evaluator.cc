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

#include <cstddef>

#include "iceberg/expression/binder.h"
#include "iceberg/expression/expression.h"
#include "iceberg/expression/expression_visitor.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/expression/predicate.h"
#include "iceberg/partition_spec.h"
#include "iceberg/row/struct_like.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
#include "iceberg/transform.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/macros.h"

namespace iceberg {

class ResidualVisitor : public BoundVisitor<std::shared_ptr<Expression>> {
 public:
  ResidualVisitor(const std::shared_ptr<PartitionSpec>& spec,
                  const std::shared_ptr<Schema>& schema, const StructLike& partition_data,
                  bool case_sensitive)
      : spec_(spec),
        schema_(schema),
        partition_data_(partition_data),
        case_sensitive_(case_sensitive) {
    ICEBERG_ASSIGN_OR_THROW(auto partition_type_, spec_->PartitionType(*schema));
    partition_schema_ = FromStructType(std::move(*partition_type_), std::nullopt);
  }

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
      const std::shared_ptr<Bound>& expr) override {
    return IsNullImpl(expr);
  }

  Result<std::shared_ptr<Expression>> NotNull(
      const std::shared_ptr<Bound>& expr) override {
    return NotNullImpl(expr);
  }

  Result<std::shared_ptr<Expression>> IsNaN(const std::shared_ptr<Bound>& expr) override {
    return IsNaNImpl(expr);
  }

  Result<std::shared_ptr<Expression>> NotNaN(
      const std::shared_ptr<Bound>& expr) override {
    return NotNaNImpl(expr);
  }

  Result<std::shared_ptr<Expression>> Lt(const std::shared_ptr<Bound>& expr,
                                         const Literal& lit) override {
    return LtImpl(expr, lit);
  }

  Result<std::shared_ptr<Expression>> LtEq(const std::shared_ptr<Bound>& expr,
                                           const Literal& lit) override {
    return LtEqImpl(expr, lit);
  }

  Result<std::shared_ptr<Expression>> Gt(const std::shared_ptr<Bound>& expr,
                                         const Literal& lit) override {
    return GtImpl(expr, lit);
  }

  Result<std::shared_ptr<Expression>> GtEq(const std::shared_ptr<Bound>& expr,
                                           const Literal& lit) override {
    return GtEqImpl(expr, lit);
  }

  Result<std::shared_ptr<Expression>> Eq(const std::shared_ptr<Bound>& expr,
                                         const Literal& lit) override {
    return EqImpl(expr, lit);
  }

  Result<std::shared_ptr<Expression>> NotEq(const std::shared_ptr<Bound>& expr,
                                            const Literal& lit) override {
    return NotEqImpl(expr, lit);
  }

  Result<std::shared_ptr<Expression>> StartsWith(const std::shared_ptr<Bound>& expr,
                                                 const Literal& lit) override {
    return StartsWithImpl(expr, lit);
  }

  Result<std::shared_ptr<Expression>> NotStartsWith(const std::shared_ptr<Bound>& expr,
                                                    const Literal& lit) override {
    return NotStartsWithImpl(expr, lit);
  }

  Result<std::shared_ptr<Expression>> In(
      const std::shared_ptr<Bound>& expr,
      const BoundSetPredicate::LiteralSet& literal_set) override {
    return InImpl(expr, literal_set);
  }

  Result<std::shared_ptr<Expression>> NotIn(
      const std::shared_ptr<Bound>& expr,
      const BoundSetPredicate::LiteralSet& literal_set) override {
    return NotInImpl(expr, literal_set);
  }

  Result<std::shared_ptr<Expression>> Predicate(
      const std::shared_ptr<BoundPredicate>& pred) override;

  Result<std::shared_ptr<Expression>> Predicate(
      const std::shared_ptr<UnboundPredicate>& pred) override {
    ICEBERG_ASSIGN_OR_RAISE(auto bound_predicate, pred->Bind(*schema_, case_sensitive_));
    if (bound_predicate->is_bound_predicate()) {
      ICEBERG_ASSIGN_OR_RAISE(
          auto residual,
          Predicate(std::dynamic_pointer_cast<BoundPredicate>(bound_predicate)));
      if (residual->is_bound_predicate()) {
        // replace inclusive original unbound predicate
        return pred;
      }
      return residual;
    }
    // if binding didn't result in a Predicate, return the expression
    return bound_predicate;
  }

 private:
  // Helper methods for bound predicates
  Result<std::shared_ptr<Expression>> EvaluateBoundPredicate(
      const std::shared_ptr<BoundPredicate>& pred);

  Result<std::shared_ptr<Expression>> IsNullImpl(const std::shared_ptr<Bound>& expr);
  Result<std::shared_ptr<Expression>> NotNullImpl(const std::shared_ptr<Bound>& expr);
  Result<std::shared_ptr<Expression>> IsNaNImpl(const std::shared_ptr<Bound>& expr);
  Result<std::shared_ptr<Expression>> NotNaNImpl(const std::shared_ptr<Bound>& expr);
  Result<std::shared_ptr<Expression>> LtImpl(const std::shared_ptr<Bound>& expr,
                                             const Literal& lit);
  Result<std::shared_ptr<Expression>> LtEqImpl(const std::shared_ptr<Bound>& expr,
                                               const Literal& lit);
  Result<std::shared_ptr<Expression>> GtImpl(const std::shared_ptr<Bound>& expr,
                                             const Literal& lit);
  Result<std::shared_ptr<Expression>> GtEqImpl(const std::shared_ptr<Bound>& expr,
                                               const Literal& lit);
  Result<std::shared_ptr<Expression>> EqImpl(const std::shared_ptr<Bound>& expr,
                                             const Literal& lit);
  Result<std::shared_ptr<Expression>> NotEqImpl(const std::shared_ptr<Bound>& expr,
                                                const Literal& lit);
  Result<std::shared_ptr<Expression>> InImpl(
      const std::shared_ptr<Bound>& expr,
      const BoundSetPredicate::LiteralSet& literal_set);
  Result<std::shared_ptr<Expression>> NotInImpl(
      const std::shared_ptr<Bound>& expr,
      const BoundSetPredicate::LiteralSet& literal_set);
  Result<std::shared_ptr<Expression>> StartsWithImpl(const std::shared_ptr<Bound>& expr,
                                                     const Literal& lit);
  Result<std::shared_ptr<Expression>> NotStartsWithImpl(
      const std::shared_ptr<Bound>& expr, const Literal& lit);

  std::shared_ptr<PartitionSpec> spec_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<Schema> partition_schema_;
  const StructLike& partition_data_;
  bool case_sensitive_;
};

Result<std::shared_ptr<Expression>> ResidualVisitor::IsNullImpl(
    const std::shared_ptr<Bound>& expr) {
  ICEBERG_ASSIGN_OR_RAISE(auto value, expr->Evaluate(partition_data_));
  if (value.IsNull()) {
    return Expressions::AlwaysTrue();
  }
  return Expressions::AlwaysFalse();
}

Result<std::shared_ptr<Expression>> ResidualVisitor::NotNullImpl(
    const std::shared_ptr<Bound>& expr) {
  ICEBERG_ASSIGN_OR_RAISE(auto value, expr->Evaluate(partition_data_));
  if (value.IsNull()) {
    return Expressions::AlwaysFalse();
  }
  return Expressions::AlwaysTrue();
}

Result<std::shared_ptr<Expression>> ResidualVisitor::IsNaNImpl(
    const std::shared_ptr<Bound>& expr) {
  ICEBERG_ASSIGN_OR_RAISE(auto value, expr->Evaluate(partition_data_));
  if (value.IsNaN()) {
    return Expressions::AlwaysTrue();
  }
  return Expressions::AlwaysFalse();
}

Result<std::shared_ptr<Expression>> ResidualVisitor::NotNaNImpl(
    const std::shared_ptr<Bound>& expr) {
  ICEBERG_ASSIGN_OR_RAISE(auto value, expr->Evaluate(partition_data_));
  if (value.IsNaN()) {
    return Expressions::AlwaysFalse();
  }
  return Expressions::AlwaysTrue();
}

Result<std::shared_ptr<Expression>> ResidualVisitor::LtImpl(
    const std::shared_ptr<Bound>& expr, const Literal& lit) {
  ICEBERG_ASSIGN_OR_RAISE(auto value, expr->Evaluate(partition_data_));
  if (value < lit) {
    return Expressions::AlwaysTrue();
  }
  return Expressions::AlwaysFalse();
}

Result<std::shared_ptr<Expression>> ResidualVisitor::LtEqImpl(
    const std::shared_ptr<Bound>& expr, const Literal& lit) {
  ICEBERG_ASSIGN_OR_RAISE(auto value, expr->Evaluate(partition_data_));
  if (value <= lit) {
    return Expressions::AlwaysTrue();
  }
  return Expressions::AlwaysFalse();
}

Result<std::shared_ptr<Expression>> ResidualVisitor::GtImpl(
    const std::shared_ptr<Bound>& expr, const Literal& lit) {
  ICEBERG_ASSIGN_OR_RAISE(auto value, expr->Evaluate(partition_data_));
  if (value > lit) {
    return Expressions::AlwaysTrue();
  }
  return Expressions::AlwaysFalse();
}

Result<std::shared_ptr<Expression>> ResidualVisitor::GtEqImpl(
    const std::shared_ptr<Bound>& expr, const Literal& lit) {
  ICEBERG_ASSIGN_OR_RAISE(auto value, expr->Evaluate(partition_data_));
  if (value >= lit) {
    return Expressions::AlwaysTrue();
  }
  return Expressions::AlwaysFalse();
}

Result<std::shared_ptr<Expression>> ResidualVisitor::EqImpl(
    const std::shared_ptr<Bound>& expr, const Literal& lit) {
  ICEBERG_ASSIGN_OR_RAISE(auto value, expr->Evaluate(partition_data_));
  if (value == lit) {
    return Expressions::AlwaysTrue();
  }
  return Expressions::AlwaysFalse();
}

Result<std::shared_ptr<Expression>> ResidualVisitor::NotEqImpl(
    const std::shared_ptr<Bound>& expr, const Literal& lit) {
  ICEBERG_ASSIGN_OR_RAISE(auto value, expr->Evaluate(partition_data_));
  if (value != lit) {
    return Expressions::AlwaysTrue();
  }
  return Expressions::AlwaysFalse();
}

Result<std::shared_ptr<Expression>> ResidualVisitor::InImpl(
    const std::shared_ptr<Bound>& expr,
    const BoundSetPredicate::LiteralSet& literal_set) {
  ICEBERG_ASSIGN_OR_RAISE(auto value, expr->Evaluate(partition_data_));
  if (literal_set.contains(value)) {
    return Expressions::AlwaysTrue();
  }
  return Expressions::AlwaysFalse();
}

Result<std::shared_ptr<Expression>> ResidualVisitor::NotInImpl(
    const std::shared_ptr<Bound>& expr,
    const BoundSetPredicate::LiteralSet& literal_set) {
  ICEBERG_ASSIGN_OR_RAISE(auto value, expr->Evaluate(partition_data_));
  if (literal_set.contains(value)) {
    return Expressions::AlwaysFalse();
  }
  return Expressions::AlwaysTrue();
}

Result<std::shared_ptr<Expression>> ResidualVisitor::StartsWithImpl(
    const std::shared_ptr<Bound>& expr, const Literal& lit) {
  ICEBERG_ASSIGN_OR_RAISE(auto value, expr->Evaluate(partition_data_));

  // Both value and literal should be strings
  if (!std::holds_alternative<std::string>(value.value()) ||
      !std::holds_alternative<std::string>(lit.value())) {
    return Expressions::AlwaysFalse();
  }

  const auto& str_value = std::get<std::string>(value.value());
  const auto& str_prefix = std::get<std::string>(lit.value());
  if (str_value.starts_with(str_prefix)) {
    return Expressions::AlwaysTrue();
  }
  return Expressions::AlwaysFalse();
}

Result<std::shared_ptr<Expression>> ResidualVisitor::NotStartsWithImpl(
    const std::shared_ptr<Bound>& expr, const Literal& lit) {
  ICEBERG_ASSIGN_OR_RAISE(auto value, expr->Evaluate(partition_data_));

  // Both value and literal should be strings
  if (!std::holds_alternative<std::string>(value.value()) ||
      !std::holds_alternative<std::string>(lit.value())) {
    return Expressions::AlwaysTrue();
  }

  const auto& str_value = std::get<std::string>(value.value());
  const auto& str_prefix = std::get<std::string>(lit.value());
  if (str_value.starts_with(str_prefix)) {
    return Expressions::AlwaysFalse();
  }
  return Expressions::AlwaysTrue();
}

Result<std::shared_ptr<Expression>> ResidualVisitor::EvaluateBoundPredicate(
    const std::shared_ptr<BoundPredicate>& pred) {
  ICEBERG_DCHECK(pred != nullptr, "BoundPredicate cannot be null");

  switch (pred->kind()) {
    case BoundPredicate::Kind::kUnary: {
      switch (pred->op()) {
        case Expression::Operation::kIsNull:
          return IsNullImpl(pred->term());
        case Expression::Operation::kNotNull:
          return NotNullImpl(pred->term());
        case Expression::Operation::kIsNan:
          return IsNaNImpl(pred->term());
        case Expression::Operation::kNotNan:
          return NotNaNImpl(pred->term());
        default:
          return InvalidExpression("Invalid operation for BoundUnaryPredicate: {}",
                                   ToString(pred->op()));
      }
    }
    case BoundPredicate::Kind::kLiteral: {
      const auto& literal_pred =
          internal::checked_cast<const BoundLiteralPredicate&>(*pred);
      switch (pred->op()) {
        case Expression::Operation::kLt:
          return LtImpl(pred->term(), literal_pred.literal());
        case Expression::Operation::kLtEq:
          return LtEqImpl(pred->term(), literal_pred.literal());
        case Expression::Operation::kGt:
          return GtImpl(pred->term(), literal_pred.literal());
        case Expression::Operation::kGtEq:
          return GtEqImpl(pred->term(), literal_pred.literal());
        case Expression::Operation::kEq:
          return EqImpl(pred->term(), literal_pred.literal());
        case Expression::Operation::kNotEq:
          return NotEqImpl(pred->term(), literal_pred.literal());
        case Expression::Operation::kStartsWith:
          return StartsWithImpl(pred->term(), literal_pred.literal());
        case Expression::Operation::kNotStartsWith:
          return NotStartsWithImpl(pred->term(), literal_pred.literal());
        default:
          return InvalidExpression("Invalid operation for BoundLiteralPredicate: {}",
                                   ToString(pred->op()));
      }
    }
    case BoundPredicate::Kind::kSet: {
      const auto& set_pred = internal::checked_cast<const BoundSetPredicate&>(*pred);
      switch (pred->op()) {
        case Expression::Operation::kIn:
          return InImpl(pred->term(), set_pred.literal_set());
        case Expression::Operation::kNotIn:
          return NotInImpl(pred->term(), set_pred.literal_set());
        default:
          return InvalidExpression("Invalid operation for BoundSetPredicate: {}",
                                   ToString(pred->op()));
      }
    }
  }

  return InvalidExpression("Unsupported bound predicate: {}", pred->ToString());
}

Result<std::shared_ptr<Expression>> ResidualVisitor::Predicate(
    const std::shared_ptr<BoundPredicate>& pred) {
  // Get the strict projection and inclusive projection of this predicate in partition
  // data, then use them to determine whether to return the original predicate. The
  // strict projection returns true iff the original predicate would have returned true,
  // so the predicate can be eliminated if the strict projection evaluates to true.
  // Similarly the inclusive projection returns false iff the original predicate would
  // have returned false, so the predicate can also be eliminated if the inclusive
  // projection evaluates to false.

  // Get the field ID from the predicate's reference
  const auto& ref = pred->reference();
  int32_t field_id = ref->field().field_id();

  // Find partition fields that match this source field ID
  std::vector<const PartitionField*> matching_fields;
  for (const auto& field : spec_->fields()) {
    if (field.source_id() == field_id) {
      matching_fields.push_back(&field);
    }
  }

  if (matching_fields.empty()) {
    // Not associated with a partition field, can't be evaluated
    return pred;
  }

  for (const auto* part : matching_fields) {
    // Check the strict projection
    ICEBERG_ASSIGN_OR_RAISE(auto strict_projection,
                            part->transform()->ProjectStrict(part->name(), pred));
    std::shared_ptr<Expression> strict_result = nullptr;

    if (strict_projection != nullptr) {
      // Bind the projected predicate to partition type
      ICEBERG_ASSIGN_OR_RAISE(
          auto bound_strict,
          Binder::Bind(*partition_schema_,
                       std::shared_ptr<Expression>(std::move(strict_projection)),
                       case_sensitive_));

      if (bound_strict->is_bound_predicate()) {
        // Evaluate the bound predicate against partition data
        ICEBERG_ASSIGN_OR_RAISE(
            strict_result, EvaluateBoundPredicate(
                               std::dynamic_pointer_cast<BoundPredicate>(bound_strict)));
      } else {
        // If the result is not a predicate, then it must be a constant like alwaysTrue
        // or alwaysFalse
        strict_result = bound_strict;
      }
    }

    if (strict_result != nullptr && strict_result->op() == Expression::Operation::kTrue) {
      // If strict is true, returning true
      return Expressions::AlwaysTrue();
    }

    // Check the inclusive projection
    ICEBERG_ASSIGN_OR_RAISE(auto inclusive_projection,
                            part->transform()->Project(part->name(), pred));
    std::shared_ptr<Expression> inclusive_result = nullptr;

    if (inclusive_projection != nullptr) {
      // Bind the projected predicate to partition type
      ICEBERG_ASSIGN_OR_RAISE(
          auto bound_inclusive,
          Binder::Bind(*partition_schema_,
                       std::shared_ptr<Expression>(std::move(inclusive_projection)),
                       case_sensitive_));

      if (bound_inclusive->is_bound_predicate()) {
        // Evaluate the bound predicate against partition data
        ICEBERG_ASSIGN_OR_RAISE(
            inclusive_result,
            EvaluateBoundPredicate(
                std::dynamic_pointer_cast<BoundPredicate>(bound_inclusive)));
      } else {
        // If the result is not a predicate, then it must be a constant like alwaysTrue
        // or alwaysFalse
        inclusive_result = bound_inclusive;
      }
    }

    if (inclusive_result != nullptr &&
        inclusive_result->op() == Expression::Operation::kFalse) {
      // If inclusive is false, returning false
      return Expressions::AlwaysFalse();
    }
  }

  // Neither strict nor inclusive predicate was conclusive, returning the original pred
  return pred;
}

ResidualEvaluator::ResidualEvaluator(std::shared_ptr<Expression> expr,
                                     const std::shared_ptr<PartitionSpec>& spec,
                                     const std::shared_ptr<Schema>& schema,
                                     bool case_sensitive)
    : expr_(std::move(expr)),
      spec_(spec),
      schema_(schema),
      case_sensitive_(case_sensitive) {}

ResidualEvaluator::~ResidualEvaluator() = default;

namespace {

// Unpartitioned residual evaluator that always returns the original expression
class UnpartitionedResidualEvaluator : public ResidualEvaluator {
 public:
  explicit UnpartitionedResidualEvaluator(std::shared_ptr<Expression> expr)
      : ResidualEvaluator(std::move(expr), PartitionSpec::Unpartitioned(), nullptr,
                          true) {}

  Result<std::shared_ptr<Expression>> ResidualFor(
      const StructLike& /*partition_data*/) const override {
    return expr_;
  }
};

}  // namespace

Result<std::unique_ptr<ResidualEvaluator>> ResidualEvaluator::Unpartitioned(
    std::shared_ptr<Expression> expr) {
  return std::unique_ptr<ResidualEvaluator>(
      new UnpartitionedResidualEvaluator(std::move(expr)));
}

Result<std::unique_ptr<ResidualEvaluator>> ResidualEvaluator::Make(
    const std::shared_ptr<PartitionSpec>& spec, const std::shared_ptr<Schema>& schema,
    std::shared_ptr<Expression> expr, bool case_sensitive) {
  if (spec->fields().empty()) {
    return Unpartitioned(std::move(expr));
  }
  return std::unique_ptr<ResidualEvaluator>(
      new ResidualEvaluator(std::move(expr), spec, schema, case_sensitive));
}

Result<std::shared_ptr<Expression>> ResidualEvaluator::ResidualFor(
    const StructLike& partition_data) const {
  ResidualVisitor visitor(spec_, schema_, partition_data, case_sensitive_);
  return Visit<std::shared_ptr<Expression>, ResidualVisitor>(expr_, visitor);
}

}  // namespace iceberg
