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

/// \file iceberg/expression/sanitize_expression.h
/// Replace literal values in an expression with type-aware placeholders.

#include <cstdint>
#include <memory>
#include <string>

#include "iceberg/expression/expression_visitor.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Rewrites an expression tree so that literal values are replaced with
/// type-aware placeholders (e.g. "(2-digit-int)", "(hash-3f9a1c02)",
/// "(date-5-days-ago)"), while preserving the predicate/column/operator structure.
///
/// Mirrors Java's `org.apache.iceberg.expressions.ExpressionUtil.sanitize`. Used before
/// handing a scan's row filter to a MetricsReporter so that literal predicate values
/// (which may be sensitive, e.g. PII) are never exposed to metrics consumers.
class ICEBERG_EXPORT SanitizeExpression
    : public ExpressionVisitor<std::shared_ptr<Expression>> {
 public:
  /// \brief Sanitize an expression tree, replacing literals with placeholders.
  static Result<std::shared_ptr<Expression>> Sanitize(
      const std::shared_ptr<Expression>& expr);

  /// \brief Bind `expr` to `schema` first, falling back to sanitizing the unbound
  /// expression if binding fails. Mirrors Java's `ExpressionUtil.sanitize(StructType,
  /// Expression, boolean)`.
  static Result<std::shared_ptr<Expression>> Sanitize(
      const Schema& schema, const std::shared_ptr<Expression>& expr, bool case_sensitive);

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

 private:
  SanitizeExpression();

  /// Current time, microseconds since epoch, captured once per Sanitize() call.
  int64_t now_;
  /// Current day, days since epoch (UTC), captured once per Sanitize() call.
  int32_t today_;
};

}  // namespace iceberg
