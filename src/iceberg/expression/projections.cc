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

#include "iceberg/expression/expression.h"
#include "iceberg/expression/expression_visitor.h"
#include "iceberg/expression/predicate.h"
#include "iceberg/expression/rewrite_not.h"
#include "iceberg/partition_spec.h"
#include "iceberg/result.h"
#include "iceberg/transform.h"
#include "iceberg/util/macros.h"

namespace iceberg {

class ProjectionVisitor : public ExpressionVisitor<std::shared_ptr<Expression>> {
 public:
  ~ProjectionVisitor() override = default;

  ProjectionVisitor(const PartitionSpec& spec, const Schema& schema, bool case_sensitive)
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
    return And::MakeFolded(left_result, right_result);
  }

  Result<std::shared_ptr<Expression>> Or(
      const std::shared_ptr<Expression>& left_result,
      const std::shared_ptr<Expression>& right_result) override {
    return Or::MakeFolded(left_result, right_result);
  }

  Result<std::shared_ptr<Expression>> Predicate(
      const std::shared_ptr<UnboundPredicate>& pred) override {
    ICEBERG_ASSIGN_OR_RAISE(auto bound_pred, pred->Bind(schema_, case_sensitive_));
    if (bound_pred->is_bound_predicate()) {
      return Predicate(std::dynamic_pointer_cast<BoundPredicate>(bound_pred));
    }
    return bound_pred;
  }

  Result<std::shared_ptr<Expression>> Predicate(
      const std::shared_ptr<BoundPredicate>& pred) override {
    return InvalidExpression("Bound predicates are not supported in projections");
  }

 protected:
  const PartitionSpec& spec_;
  const Schema& schema_;
  bool case_sensitive_;
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

  InclusiveProjectionVisitor(const PartitionSpec& spec, const Schema& schema,
                             bool case_sensitive)
      : ProjectionVisitor(spec, schema, case_sensitive) {}

  Result<std::shared_ptr<Expression>> Predicate(
      const std::shared_ptr<BoundPredicate>& pred) override {
    ICEBERG_DCHECK(pred != nullptr, "Predicate cannot be null");
    // Find partition fields that match the predicate's term
    ICEBERG_ASSIGN_OR_RAISE(
        auto parts, spec_.GetFieldsBySourceId(pred->reference()->field().field_id()));
    if (parts.empty()) {
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
    for (const auto& part : parts) {
      ICEBERG_ASSIGN_OR_RAISE(auto projected,
                              part.get().transform()->Project(part.get().name(), pred));
      if (projected != nullptr) {
        ICEBERG_ASSIGN_OR_RAISE(result,
                                And::MakeFolded(std::move(result), std::move(projected)));
      }
    }

    return result;
  }
};

/// \brief Strict projection evaluator.
///
/// Uses OR to combine projections from multiple partition fields.
class StrictProjectionVisitor : public ProjectionVisitor {
 public:
  ~StrictProjectionVisitor() override = default;

  StrictProjectionVisitor(const PartitionSpec& spec, const Schema& schema,
                          bool case_sensitive)
      : ProjectionVisitor(spec, schema, case_sensitive) {}

  Result<std::shared_ptr<Expression>> Predicate(
      const std::shared_ptr<BoundPredicate>& pred) override {
    ICEBERG_DCHECK(pred != nullptr, "Predicate cannot be null");
    // Find partition fields that match the predicate's term
    ICEBERG_ASSIGN_OR_RAISE(
        auto parts, spec_.GetFieldsBySourceId(pred->reference()->field().field_id()));
    if (parts.empty()) {
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
    for (const auto& part : parts) {
      ICEBERG_ASSIGN_OR_RAISE(
          auto projected, part.get().transform()->ProjectStrict(part.get().name(), pred));
      if (projected != nullptr) {
        ICEBERG_ASSIGN_OR_RAISE(result,
                                Or::MakeFolded(std::move(result), std::move(projected)));
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

std::unique_ptr<ProjectionEvaluator> Projections::Inclusive(const PartitionSpec& spec,
                                                            const Schema& schema,
                                                            bool case_sensitive) {
  auto visitor =
      std::make_unique<InclusiveProjectionVisitor>(spec, schema, case_sensitive);
  return std::unique_ptr<ProjectionEvaluator>(
      new ProjectionEvaluator(std::move(visitor)));
}

std::unique_ptr<ProjectionEvaluator> Projections::Strict(const PartitionSpec& spec,
                                                         const Schema& schema,
                                                         bool case_sensitive) {
  auto visitor = std::make_unique<StrictProjectionVisitor>(spec, schema, case_sensitive);
  return std::unique_ptr<ProjectionEvaluator>(
      new ProjectionEvaluator(std::move(visitor)));
}

}  // namespace iceberg
