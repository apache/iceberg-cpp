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

#include <memory>
#include <vector>

#include "iceberg/expression/expression.h"
#include "iceberg/expression/expression_visitor.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/expression/predicate.h"
#include "iceberg/expression/rewrite_not.h"
#include "iceberg/expression/term.h"
#include "iceberg/partition_field.h"
#include "iceberg/partition_spec.h"
#include "iceberg/result.h"
#include "iceberg/transform.h"
#include "iceberg/util/macros.h"

namespace iceberg {

// Implementation detail - not exported
class ProjectionVisitor : public ExpressionVisitor<std::shared_ptr<Expression>> {
 public:
  ~ProjectionVisitor() override = default;

  ProjectionVisitor(const std::shared_ptr<PartitionSpec>& spec,
                    const std::shared_ptr<Schema>& schema, bool case_sensitive)
      : spec_(spec), schema_(schema), case_sensitive_(case_sensitive) {}

  Result<std::shared_ptr<Expression>> AlwaysTrue() override { return True::Instance(); }

  Result<std::shared_ptr<Expression>> AlwaysFalse() override { return False::Instance(); }

  Result<std::shared_ptr<Expression>> Not(
      const std::shared_ptr<Expression>& child_result) override {
    return InvalidExpression("Project called on expression with a not");
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

  Result<std::shared_ptr<Expression>> Predicate(
      const std::shared_ptr<UnboundPredicate>& pred) override {
    ICEBERG_ASSIGN_OR_RAISE(auto bound_pred, pred->Bind(*schema_, case_sensitive_));
    if (bound_pred->is_bound_predicate()) {
      auto bound_predicate = std::dynamic_pointer_cast<BoundPredicate>(bound_pred);
      ICEBERG_DCHECK(
          bound_predicate != nullptr,
          "Expected bound_predicate to be non-null after is_bound_predicate() check");
      return Predicate(bound_predicate);
    }
    return bound_pred;
  }

  Result<std::shared_ptr<Expression>> Predicate(
      const std::shared_ptr<BoundPredicate>& pred) override {
    return InvalidExpression("Bound predicates are not supported in projections");
  }

 protected:
  const std::shared_ptr<PartitionSpec>& spec_;
  const std::shared_ptr<Schema>& schema_;
  bool case_sensitive_;

  /// \brief Get partition fields that match the predicate's term.
  std::vector<const PartitionField*> GetFieldsByPredicate(
      const std::shared_ptr<BoundPredicate>& pred) const {
    int32_t source_id;
    switch (pred->term()->kind()) {
      case Term::Kind::kReference: {
        const auto& ref = pred->term()->reference();
        source_id = ref->field().field_id();
        break;
      }
      case Term::Kind::kTransform: {
        const auto& transform =
            internal::checked_pointer_cast<BoundTransform>(pred->term());
        source_id = transform->reference()->field().field_id();
        break;
      }
      default:
        std::unreachable();
    }

    std::vector<const PartitionField*> result;
    for (const auto& field : spec_->fields()) {
      if (field.source_id() == source_id) {
        result.push_back(&field);
      }
    }
    return result;
  }
};

ProjectionEvaluator::ProjectionEvaluator(std::unique_ptr<ProjectionVisitor> visitor)
    : visitor_(std::move(visitor)) {}

ProjectionEvaluator::~ProjectionEvaluator() = default;

/// \brief Inclusive projection visitor.
///
/// Uses AND to combine projections from multiple partition fields.
class InclusiveProjectionVisitor : public ProjectionVisitor {
 public:
  ~InclusiveProjectionVisitor() override = default;

  InclusiveProjectionVisitor(const std::shared_ptr<PartitionSpec>& spec,
                             const std::shared_ptr<Schema>& schema, bool case_sensitive)
      : ProjectionVisitor(spec, schema, case_sensitive) {}

  Result<std::shared_ptr<Expression>> Predicate(
      const std::shared_ptr<BoundPredicate>& pred) override {
    ICEBERG_DCHECK(pred != nullptr, "Predicate cannot be null");
    // Find partition fields that match the predicate's term
    auto partition_fields = GetFieldsByPredicate(pred);
    if (partition_fields.empty()) {
      // The predicate has no partition column
      return AlwaysTrue();
    }

    // Project the predicate for each partition field and combine with AND
    //
    // consider (d = 2019-01-01) with bucket(7, d) and bucket(5, d)
    // projections: b1 = bucket(7, '2019-01-01') = 5, b2 = bucket(5, '2019-01-01') = 0
    // any value where b1 != 5 or any value where b2 != 0 cannot be the '2019-01-01'
    //
    // similarly, if partitioning by day(ts) and hour(ts), the more restrictive
    // projection should be used. ts = 2019-01-01T01:00:00 produces day=2019-01-01 and
    // hour=2019-01-01-01. the value will be in 2019-01-01-01 and not in 2019-01-01-02.
    std::shared_ptr<Expression> result = True::Instance();
    for (const auto* part_field : partition_fields) {
      ICEBERG_ASSIGN_OR_RAISE(auto projected,
                              part_field->transform()->Project(part_field->name(), pred));
      if (projected != nullptr) {
        result =
            Expressions::And(result, std::shared_ptr<Expression>(projected.release()));
      }
    }

    return result;
  }

 protected:
};

/// \brief Strict projection evaluator.
///
/// Uses OR to combine projections from multiple partition fields.
class StrictProjectionVisitor : public ProjectionVisitor {
 public:
  ~StrictProjectionVisitor() override = default;

  StrictProjectionVisitor(const std::shared_ptr<PartitionSpec>& spec,
                          const std::shared_ptr<Schema>& schema, bool case_sensitive)
      : ProjectionVisitor(spec, schema, case_sensitive) {}

  Result<std::shared_ptr<Expression>> Predicate(
      const std::shared_ptr<BoundPredicate>& pred) override {
    ICEBERG_DCHECK(pred != nullptr, "Predicate cannot be null");
    // Find partition fields that match the predicate's term
    auto partition_fields = GetFieldsByPredicate(pred);
    if (partition_fields.empty()) {
      // The predicate has no matching partition columns
      return AlwaysFalse();
    }

    // Project the predicate for each partition field and combine with OR
    //
    // consider (ts > 2019-01-01T01:00:00) with day(ts) and hour(ts)
    // projections: d >= 2019-01-02 and h >= 2019-01-01-02 (note the inclusive bounds).
    // any timestamp where either projection predicate is true must match the original
    // predicate. For example, ts = 2019-01-01T03:00:00 matches the hour projection but
    // not the day, but does match the original predicate.
    std::shared_ptr<Expression> result = False::Instance();
    for (const auto* part_field : partition_fields) {
      ICEBERG_ASSIGN_OR_RAISE(auto projected, part_field->transform()->ProjectStrict(
                                                  part_field->name(), pred));
      if (projected != nullptr) {
        result =
            Expressions::Or(result, std::shared_ptr<Expression>(projected.release()));
      }
    }

    return result;
  }
};

Result<std::shared_ptr<Expression>> ProjectionEvaluator::Project(
    const std::shared_ptr<Expression>& expr) {
  // Projections assume that there are no NOT nodes in the expression tree. To ensure that
  // this is the case, the expression is rewritten to push all NOT nodes down to the
  // expression leaf nodes.
  //
  // This is necessary to ensure that the default expression returned when a predicate
  // can't be projected is correct.
  ICEBERG_ASSIGN_OR_RAISE(auto rewritten, RewriteNot::Visit(expr));
  return Visit<std::shared_ptr<Expression>, ProjectionVisitor>(rewritten, *visitor_);
}

std::unique_ptr<ProjectionEvaluator> Projections::Inclusive(
    const std::shared_ptr<PartitionSpec>& spec, const std::shared_ptr<Schema>& schema,
    bool case_sensitive) {
  auto visitor =
      std::make_unique<InclusiveProjectionVisitor>(spec, schema, case_sensitive);
  return std::unique_ptr<ProjectionEvaluator>(
      new ProjectionEvaluator(std::move(visitor)));
}

std::unique_ptr<ProjectionEvaluator> Projections::Strict(
    const std::shared_ptr<PartitionSpec>& spec, const std::shared_ptr<Schema>& schema,
    bool case_sensitive) {
  auto visitor = std::make_unique<StrictProjectionVisitor>(spec, schema, case_sensitive);
  return std::unique_ptr<ProjectionEvaluator>(
      new ProjectionEvaluator(std::move(visitor)));
}

}  // namespace iceberg
