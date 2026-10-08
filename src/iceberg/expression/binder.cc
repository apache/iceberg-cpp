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

#include "iceberg/result.h"
#include "iceberg/util/macros.h"

namespace iceberg {

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
  ICEBERG_PRECHECK(pred != nullptr, "Predicate cannot be null");
  return pred->Bind(schema_, case_sensitive_);
}

Result<std::shared_ptr<Expression>> Binder::Predicate(
    const std::shared_ptr<BoundPredicate>& pred) {
  ICEBERG_PRECHECK(pred != nullptr, "Predicate cannot be null");
  return InvalidExpression("Found already bound predicate: {}", pred->ToString());
}

Result<std::shared_ptr<Expression>> Binder::Aggregate(
    const std::shared_ptr<BoundAggregate>& aggregate) {
  ICEBERG_PRECHECK(aggregate != nullptr, "Aggregate cannot be null");
  return InvalidExpression("Found already bound aggregate: {}", aggregate->ToString());
}

Result<std::shared_ptr<Expression>> Binder::Aggregate(
    const std::shared_ptr<UnboundAggregate>& aggregate) {
  ICEBERG_PRECHECK(aggregate != nullptr, "Aggregate cannot be null");
  return aggregate->Bind(schema_, case_sensitive_);
}

Result<bool> IsBoundVisitor::IsBound(const std::shared_ptr<Expression>& expr) {
  ICEBERG_PRECHECK(expr != nullptr, "Expression cannot be null");
  IsBoundVisitor visitor;
  ICEBERG_ASSIGN_OR_RAISE(auto is_bound, Visit<std::optional<bool>>(expr, visitor));
  return is_bound.value_or(false);
}

Result<std::optional<bool>> IsBoundVisitor::AlwaysTrue() { return std::nullopt; }

Result<std::optional<bool>> IsBoundVisitor::AlwaysFalse() { return std::nullopt; }

Result<std::optional<bool>> IsBoundVisitor::Not(const std::optional<bool>& child_result) {
  return child_result;
}

Result<std::optional<bool>> IsBoundVisitor::And(const std::optional<bool>& left_result,
                                                const std::optional<bool>& right_result) {
  if (!left_result.has_value()) {
    return right_result;
  }
  if (right_result.has_value() && left_result != right_result) {
    return InvalidExpression("Found partially bound expression");
  }
  return left_result;
}

Result<std::optional<bool>> IsBoundVisitor::Or(const std::optional<bool>& left_result,
                                               const std::optional<bool>& right_result) {
  return And(left_result, right_result);
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

Result<std::unordered_set<int32_t>> ReferenceVisitor::GetReferencedFieldIds(
    const std::shared_ptr<Expression>& expr) {
  ICEBERG_PRECHECK(expr != nullptr, "Expression cannot be null");
  ReferenceVisitor visitor;
  return Visit<FieldIdsSetRef, ReferenceVisitor>(expr, visitor);
}

Result<FieldIdsSetRef> ReferenceVisitor::AlwaysTrue() { return referenced_field_ids_; }

Result<FieldIdsSetRef> ReferenceVisitor::AlwaysFalse() { return referenced_field_ids_; }

Result<FieldIdsSetRef> ReferenceVisitor::Not(
    [[maybe_unused]] const FieldIdsSetRef& child_result) {
  return referenced_field_ids_;
}

Result<FieldIdsSetRef> ReferenceVisitor::And(
    [[maybe_unused]] const FieldIdsSetRef& left_result,
    [[maybe_unused]] const FieldIdsSetRef& right_result) {
  return referenced_field_ids_;
}

Result<FieldIdsSetRef> ReferenceVisitor::Or(
    [[maybe_unused]] const FieldIdsSetRef& left_result,
    [[maybe_unused]] const FieldIdsSetRef& right_result) {
  return referenced_field_ids_;
}

Result<FieldIdsSetRef> ReferenceVisitor::Predicate(
    const std::shared_ptr<BoundPredicate>& pred) {
  referenced_field_ids_.insert(pred->reference()->field_id());
  return referenced_field_ids_;
}

Result<FieldIdsSetRef> ReferenceVisitor::Predicate(
    [[maybe_unused]] const std::shared_ptr<UnboundPredicate>& pred) {
  return InvalidExpression("Cannot get referenced field IDs from unbound predicate");
}

Result<FieldIdsSetRef> ReferenceVisitor::Aggregate(
    const std::shared_ptr<BoundAggregate>& aggregate) {
  referenced_field_ids_.insert(aggregate->reference()->field_id());
  return referenced_field_ids_;
}

Result<FieldIdsSetRef> ReferenceVisitor::Aggregate(
    [[maybe_unused]] const std::shared_ptr<UnboundAggregate>& aggregate) {
  return InvalidExpression("Cannot get referenced field IDs from unbound aggregate");
}

}  // namespace iceberg
