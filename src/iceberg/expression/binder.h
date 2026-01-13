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

#pragma once

/// \file iceberg/expression/binder.h
/// Bind an expression to a schema.

#include <functional>
#include <unordered_set>

#include "iceberg/expression/expression_visitor.h"

namespace iceberg {

class ICEBERG_EXPORT Binder : public ExpressionVisitor<std::shared_ptr<Expression>> {
 public:
  Binder(const Schema& schema, bool case_sensitive);

  static Result<std::shared_ptr<Expression>> Bind(const Schema& schema,
                                                  const std::shared_ptr<Expression>& expr,
                                                  bool case_sensitive);

  Result<std::shared_ptr<Expression>> AlwaysTrue() override;
  Result<std::shared_ptr<Expression>> AlwaysFalse() override;
  Result<std::shared_ptr<Expression>> Not(
      const std::shared_ptr<Expression>& child_result) override;
  Result<std::shared_ptr<Expression>> And(
      const std::shared_ptr<Expression>& left_result,
      const std::shared_ptr<Expression>& right_result) override;
  Result<std::shared_ptr<Expression>> Or(
      const std::shared_ptr<Expression>& left_result,
      const std::shared_ptr<Expression>& right_result) override;
  Result<std::shared_ptr<Expression>> Predicate(
      const std::shared_ptr<BoundPredicate>& pred) override;
  Result<std::shared_ptr<Expression>> Predicate(
      const std::shared_ptr<UnboundPredicate>& pred) override;
  Result<std::shared_ptr<Expression>> Aggregate(
      const std::shared_ptr<BoundAggregate>& aggregate) override;
  Result<std::shared_ptr<Expression>> Aggregate(
      const std::shared_ptr<UnboundAggregate>& aggregate) override;

 private:
  const Schema& schema_;
  const bool case_sensitive_;
};

class ICEBERG_EXPORT IsBoundVisitor : public ExpressionVisitor<bool> {
 public:
  static Result<bool> IsBound(const std::shared_ptr<Expression>& expr);

  Result<bool> AlwaysTrue() override;
  Result<bool> AlwaysFalse() override;
  Result<bool> Not(bool child_result) override;
  Result<bool> And(bool left_result, bool right_result) override;
  Result<bool> Or(bool left_result, bool right_result) override;
  Result<bool> Predicate(const std::shared_ptr<BoundPredicate>& pred) override;
  Result<bool> Predicate(const std::shared_ptr<UnboundPredicate>& pred) override;
  Result<bool> Aggregate(const std::shared_ptr<BoundAggregate>& aggregate) override;
  Result<bool> Aggregate(const std::shared_ptr<UnboundAggregate>& aggregate) override;
};

using FieldIdsSetRef = std::reference_wrapper<std::unordered_set<int32_t>>;

/// \brief Visitor to collect referenced field IDs from an expression.
class ICEBERG_EXPORT ReferenceVisitor : public ExpressionVisitor<FieldIdsSetRef> {
 public:
  static Result<std::unordered_set<int32_t>> GetReferencedFieldIds(
      const std::shared_ptr<Expression>& expr);

  Result<FieldIdsSetRef> AlwaysTrue() override;
  Result<FieldIdsSetRef> AlwaysFalse() override;
  Result<FieldIdsSetRef> Not(const FieldIdsSetRef& child_result) override;
  Result<FieldIdsSetRef> And(const FieldIdsSetRef& left_result,
                             const FieldIdsSetRef& right_result) override;
  Result<FieldIdsSetRef> Or(const FieldIdsSetRef& left_result,
                            const FieldIdsSetRef& right_result) override;
  Result<FieldIdsSetRef> Predicate(const std::shared_ptr<BoundPredicate>& pred) override;
  Result<FieldIdsSetRef> Predicate(
      const std::shared_ptr<UnboundPredicate>& pred) override;
  Result<FieldIdsSetRef> Aggregate(
      const std::shared_ptr<BoundAggregate>& aggregate) override;
  Result<FieldIdsSetRef> Aggregate(
      const std::shared_ptr<UnboundAggregate>& aggregate) override;

 private:
  std::unordered_set<int32_t> referenced_field_ids_;
};

}  // namespace iceberg
