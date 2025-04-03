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
/// Boolean expression tree for Iceberg table operations.

#include <string>

#include "iceberg/error.h"
#include "iceberg/expected.h"
#include "iceberg/iceberg_export.h"

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
  [[nodiscard]] virtual Operation Op() const = 0;

  /// \brief Returns the negation of this expression, equivalent to not(this).
  [[nodiscard]] virtual expected<std::shared_ptr<Expression>, Error> Negate() const {
    return unexpected<Error>(
        {ErrorKind::kInvalidExpression, "This expression cannot be negated"});
  }

  /// \brief Returns whether this expression will accept the same values as another.
  ///
  /// If this returns true, the expressions are guaranteed to return the same evaluation
  /// for the same input. However, if this returns false the expressions may return the
  /// same evaluation for the same input. That is, expressions may be equivalent even if
  /// this returns false.
  ///
  /// For best results, rewrite not and bind expressions before calling this method.
  ///
  /// \param other another expression
  /// \return true if the expressions are equivalent
  [[nodiscard]] virtual bool IsEquivalentTo(const Expression& other) const {
    // only bound predicates can be equivalent
    return false;
  }

  /// \brief Convert operation string to enum
  static expected<Operation, Error> FromString(const std::string& operation_type);

  /// \brief Returns the operation used when this is negated.
  static expected<Operation, Error> Negate(Operation op);

  /// \brief Returns the equivalent operation when the left and right operands are
  /// exchanged.
  static expected<Operation, Error> FlipLR(Operation op);
};

}  // namespace iceberg
