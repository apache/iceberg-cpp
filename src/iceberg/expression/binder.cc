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
  return Visit<bool, IsBoundVisitor>(expr, visitor);
}

Result<bool> IsBoundVisitor::AlwaysTrue() { return true; }

Result<bool> IsBoundVisitor::AlwaysFalse() { return true; }

Result<bool> IsBoundVisitor::Not(bool child_result) { return child_result; }

Result<bool> IsBoundVisitor::And(bool left_result, bool right_result) {
  return left_result && right_result;
}

Result<bool> IsBoundVisitor::Or(bool left_result, bool right_result) {
  return left_result && right_result;
}

Result<bool> IsBoundVisitor::Predicate(const std::shared_ptr<BoundPredicate>& pred) {
  return true;
}

Result<bool> IsBoundVisitor::Predicate(const std::shared_ptr<UnboundPredicate>& pred) {
  return false;
}

Result<bool> IsBoundVisitor::Aggregate(const std::shared_ptr<BoundAggregate>& aggregate) {
  return true;
}

Result<bool> IsBoundVisitor::Aggregate(
    const std::shared_ptr<UnboundAggregate>& aggregate) {
  return false;
}

}  // namespace iceberg
