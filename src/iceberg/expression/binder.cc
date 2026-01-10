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

#include "iceberg/expression/binder.h"

#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

Result<std::optional<bool>> CombineResults(const std::optional<bool>& is_left_bound,
                                           const std::optional<bool>& is_right_bound) {
  if (is_left_bound.has_value()) {
    ICEBERG_CHECK(!is_right_bound.has_value() || is_left_bound == is_right_bound,
                  "Found partially bound expression");
    return is_left_bound;
  } else {
    return is_right_bound;
  }
}

}  // anonymous namespace

Binder::Binder(const Schema& schema, bool case_sensitive)
    : schema_(schema), case_sensitive_(case_sensitive) {}

Result<std::shared_ptr<Expression>> Binder::Bind(const Schema& schema,
                                                 const std::shared_ptr<Expression>& expr,
                                                 bool case_sensitive) {
  Binder binder(schema, case_sensitive);
  return Visit<std::shared_ptr<Expression>, Binder>(expr, binder);
}

Result<std::shared_ptr<Expression>> Binder::AlwaysTrue() { return True::Instance(); }

Result<std::shared_ptr<Expression>> Binder::AlwaysFalse() { return False::Instance(); }

Result<std::shared_ptr<Expression>> Binder::Not(
    const std::shared_ptr<Expression>& child_result) {
  return iceberg::Not::MakeFolded(child_result);
}

Result<std::shared_ptr<Expression>> Binder::And(
    const std::shared_ptr<Expression>& left_result,
    const std::shared_ptr<Expression>& right_result) {
  return iceberg::And::MakeFolded(left_result, right_result);
}

Result<std::shared_ptr<Expression>> Binder::Or(
    const std::shared_ptr<Expression>& left_result,
    const std::shared_ptr<Expression>& right_result) {
  return iceberg::Or::MakeFolded(left_result, right_result);
}

Result<std::shared_ptr<Expression>> Binder::Predicate(
    const std::shared_ptr<UnboundPredicate>& pred) {
  ICEBERG_DCHECK(pred != nullptr, "Predicate cannot be null");
  return pred->Bind(schema_, case_sensitive_);
}

Result<std::shared_ptr<Expression>> Binder::Predicate(
    const std::shared_ptr<BoundPredicate>& pred) {
  ICEBERG_DCHECK(pred != nullptr, "Predicate cannot be null");
  return InvalidExpression("Found already bound predicate: {}", pred->ToString());
}

Result<std::shared_ptr<Expression>> Binder::Aggregate(
    const std::shared_ptr<BoundAggregate>& aggregate) {
  ICEBERG_DCHECK(aggregate != nullptr, "Aggregate cannot be null");
  return InvalidExpression("Found already bound aggregate: {}", aggregate->ToString());
}

Result<std::shared_ptr<Expression>> Binder::Aggregate(
    const std::shared_ptr<UnboundAggregate>& aggregate) {
  ICEBERG_DCHECK(aggregate != nullptr, "Aggregate cannot be null");
  return aggregate->Bind(schema_, case_sensitive_);
}

Result<bool> IsBoundVisitor::IsBound(const std::shared_ptr<Expression>& expr) {
  ICEBERG_DCHECK(expr != nullptr, "Expression cannot be null");
  IsBoundVisitor visitor;
  auto visit_result = Visit<std::optional<bool>, IsBoundVisitor>(expr, visitor);
  ICEBERG_RETURN_UNEXPECTED(visit_result);
  auto result = std::move(visit_result.value());
  // If the result is null, return true
  return result.value_or(true);
}

Result<std::optional<bool>> IsBoundVisitor::AlwaysTrue() { return std::nullopt; }

Result<std::optional<bool>> IsBoundVisitor::AlwaysFalse() { return std::nullopt; }

Result<std::optional<bool>> IsBoundVisitor::Not(const std::optional<bool>& child_result) {
  return child_result;
}

Result<std::optional<bool>> IsBoundVisitor::And(const std::optional<bool>& left_result,
                                                const std::optional<bool>& right_result) {
  return CombineResults(left_result, right_result);
}

Result<std::optional<bool>> IsBoundVisitor::Or(const std::optional<bool>& left_result,
                                               const std::optional<bool>& right_result) {
  return CombineResults(left_result, right_result);
}

Result<std::optional<bool>> IsBoundVisitor::Predicate(
    const std::shared_ptr<BoundPredicate>& pred) {
  return true;
}

Result<std::optional<bool>> IsBoundVisitor::Predicate(
    const std::shared_ptr<UnboundPredicate>& pred) {
  return false;
}

Result<std::optional<bool>> IsBoundVisitor::Aggregate(
    const std::shared_ptr<BoundAggregate>& aggregate) {
  return true;
}

Result<std::optional<bool>> IsBoundVisitor::Aggregate(
    const std::shared_ptr<UnboundAggregate>& aggregate) {
  return false;
}

}  // namespace iceberg
