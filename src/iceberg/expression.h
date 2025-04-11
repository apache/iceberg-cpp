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

/// \file iceberg/expression.h
/// Expression interface for Iceberg table operations.

#include "iceberg/expected.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"

namespace iceberg {

/// \brief Represents a boolean expression tree.
class ICEBERG_EXPORT Expression {
 public:
  /// Operation types for expressions
  enum class Operation {
    kTrue,
    kFalse,
    kIsNull,
    kNotNull,
    kIsNan,
    kNotNan,
    kLt,
    kLtEq,
    kGt,
    kGtEq,
    kEq,
    kNotEq,
    kIn,
    kNotIn,
    kNot,
    kAnd,
    kOr,
    kStartsWith,
    kNotStartsWith,
    kCount,
    kCountStar,
    kMax,
    kMin
  };

  virtual ~Expression() = default;

  /// \brief Returns the operation for an expression node.
  virtual Operation Op() const = 0;

  /// \brief Returns the negation of this expression, equivalent to not(this).
  virtual Result<std::shared_ptr<Expression>> Negate() const {
    return unexpected(
        Error(ErrorKind::kInvalidExpression, "Expression cannot be negated"));
  }

  /// \brief Returns whether this expression will accept the same values as another.
  /// \param other another expression
  /// \return true if the expressions are equivalent
  virtual bool IsEquivalentTo(const Expression& other) const {
    // only bound predicates can be equivalent
    return false;
  }

  virtual std::string ToString() const { return "Expression"; }
};

}  // namespace iceberg
