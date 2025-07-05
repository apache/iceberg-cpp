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

#include <memory>
#include <string>

#include "iceberg/result.h"
#include "iceberg/schema.h"

namespace iceberg {

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

/// \brief Returns whether the operation is a predicate operation.
constexpr bool IsPredicate(Operation op) {
  switch (op) {
    case Operation::kTrue:
    case Operation::kFalse:
    case Operation::kIsNull:
    case Operation::kNotNull:
    case Operation::kIsNan:
    case Operation::kNotNan:
    case Operation::kLt:
    case Operation::kLtEq:
    case Operation::kGt:
    case Operation::kGtEq:
    case Operation::kEq:
    case Operation::kNotEq:
    case Operation::kIn:
    case Operation::kNotIn:
    case Operation::kNot:
    case Operation::kAnd:
    case Operation::kOr:
    case Operation::kStartsWith:
    case Operation::kNotStartsWith:
      return true;
    case Operation::kCount:
    case Operation::kCountStar:
    case Operation::kMax:
    case Operation::kMin:
      return false;
  }
  return false;
}

constexpr bool IsUnaryPredicate(Operation op) {
  switch (op) {
    case Operation::kIsNull:
    case Operation::kNotNull:
    case Operation::kIsNan:
    case Operation::kNotNan:
      return true;
    default:
      return false;
  }
}
constexpr bool IsBinaryPredicate(Operation op) {
  switch (op) {
    case Operation::kLt:
    case Operation::kLtEq:
    case Operation::kGt:
    case Operation::kGtEq:
    case Operation::kEq:
    case Operation::kNotEq:
    case Operation::kIn:
    case Operation::kNotIn:
    case Operation::kAnd:
    case Operation::kOr:
    case Operation::kStartsWith:
    case Operation::kNotStartsWith:
      return true;
    default:
      return false;
  }
}

template <typename T>
concept Bindable = requires(const T& expr, const Schema& schema, bool case_sensitive) {
  // Must have a BoundType alias that defines what type it binds to
  typename T::BoundType;
  // Must have a Bind method with the correct signature
  { expr.Bind(schema, case_sensitive) } -> std::same_as<Result<typename T::BoundType>>;
};

/// \brief Concept for types that behave like predicates (bound or unbound)
template <typename T>
concept PredicateLike = requires(const T& pred) {
  // Must have an operation type
  { pred.op() } -> std::same_as<Operation>;
  // Must be convertible to string
  { pred.ToString() } -> std::same_as<std::string>;
  // // Must have a Negate method that returns a shared_ptr to the same concept
  // { pred.Negate() } -> std::convertible_to<std::shared_ptr<T>>;
  // Must support equality comparison
  { pred.Equals(pred) } -> std::same_as<bool>;
};

}  // namespace iceberg
