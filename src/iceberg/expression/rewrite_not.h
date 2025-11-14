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

/// \file iceberg/expression/rewrite_not.h
/// Rewrite NOT expression to negated predicate.

#include <memory>

#include "iceberg/expression/expression_visitor.h"

namespace iceberg {

class ICEBERG_EXPORT RewriteNot : public ExpressionVisitor<std::shared_ptr<Expression>> {
 public:
  static Result<std::shared_ptr<Expression>> Visit(std::shared_ptr<Expression> expr);

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
};

}  // namespace iceberg
