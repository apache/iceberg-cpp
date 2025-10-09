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

#include "iceberg/expression/expression.h"

#include <format>
#include <utility>

#include "iceberg/util/formatter_internal.h"
#include "iceberg/util/macros.h"

namespace iceberg {

// True implementation
const std::shared_ptr<True>& True::Instance() {
  static const std::shared_ptr<True> instance{new True()};
  return instance;
}

Result<std::shared_ptr<Expression>> True::Negate() const { return False::Instance(); }

// False implementation
const std::shared_ptr<False>& False::Instance() {
  static const std::shared_ptr<False> instance = std::shared_ptr<False>(new False());
  return instance;
}

Result<std::shared_ptr<Expression>> False::Negate() const { return True::Instance(); }

// And implementation
And::And(std::shared_ptr<Expression> left, std::shared_ptr<Expression> right)
    : left_(std::move(left)), right_(std::move(right)) {}

std::string And::ToString() const {
  return std::format("({} and {})", left_->ToString(), right_->ToString());
}

Result<std::shared_ptr<Expression>> And::Negate() const {
  // De Morgan's law: not(A and B) = (not A) or (not B)
  ICEBERG_ASSIGN_OR_RAISE(auto left_negated, left_->Negate());
  ICEBERG_ASSIGN_OR_RAISE(auto right_negated, right_->Negate());
  return std::make_shared<Or>(std::move(left_negated), std::move(right_negated));
}

bool And::Equals(const Expression& expr) const {
  if (expr.op() == Operation::kAnd) {
    const auto& other = static_cast<const And&>(expr);
    return (left_->Equals(*other.left()) && right_->Equals(*other.right())) ||
           (left_->Equals(*other.right()) && right_->Equals(*other.left()));
  }
  return false;
}

// Or implementation
Or::Or(std::shared_ptr<Expression> left, std::shared_ptr<Expression> right)
    : left_(std::move(left)), right_(std::move(right)) {}

std::string Or::ToString() const {
  return std::format("({} or {})", left_->ToString(), right_->ToString());
}

Result<std::shared_ptr<Expression>> Or::Negate() const {
  // De Morgan's law: not(A or B) = (not A) and (not B)
  ICEBERG_ASSIGN_OR_RAISE(auto left_negated, left_->Negate());
  ICEBERG_ASSIGN_OR_RAISE(auto right_negated, right_->Negate());
  return std::make_shared<And>(std::move(left_negated), std::move(right_negated));
}

bool Or::Equals(const Expression& expr) const {
  if (expr.op() == Operation::kOr) {
    const auto& other = static_cast<const Or&>(expr);
    return (left_->Equals(*other.left()) && right_->Equals(*other.right())) ||
           (left_->Equals(*other.right()) && right_->Equals(*other.left()));
  }
  return false;
}

std::string_view ToString(Expression::Operation op) {
  switch (op) {
    case Expression::Operation::kAnd:
      return "AND";
    case Expression::Operation::kOr:
      return "OR";
    case Expression::Operation::kTrue:
      return "TRUE";
    case Expression::Operation::kFalse:
      return "FALSE";
    case Expression::Operation::kIsNull:
      return "IS_NULL";
    case Expression::Operation::kNotNull:
      return "NOT_NULL";
    case Expression::Operation::kIsNan:
      return "IS_NAN";
    case Expression::Operation::kNotNan:
      return "NOT_NAN";
    case Expression::Operation::kLt:
      return "LT";
    case Expression::Operation::kLtEq:
      return "LT_EQ";
    case Expression::Operation::kGt:
      return "GT";
    case Expression::Operation::kGtEq:
      return "GT_EQ";
    case Expression::Operation::kEq:
      return "EQ";
    case Expression::Operation::kNotEq:
      return "NOT_EQ";
    case Expression::Operation::kIn:
      return "IN";
    case Expression::Operation::kNotIn:
      return "NOT_IN";
    case Expression::Operation::kStartsWith:
      return "STARTS_WITH";
    case Expression::Operation::kNotStartsWith:
      return "NOT_STARTS_WITH";
    case Expression::Operation::kCount:
      return "COUNT";
    case Expression::Operation::kNot:
      return "NOT";
    case Expression::Operation::kCountStar:
      return "COUNT_STAR";
    case Expression::Operation::kMax:
      return "MAX";
    case Expression::Operation::kMin:
      return "MIN";
  }
  std::unreachable();
}

Result<Expression::Operation> Negate(Expression::Operation op) {
  switch (op) {
    case Expression::Operation::kIsNull:
      return Expression::Operation::kNotNull;
    case Expression::Operation::kNotNull:
      return Expression::Operation::kIsNull;
    case Expression::Operation::kIsNan:
      return Expression::Operation::kNotNan;
    case Expression::Operation::kNotNan:
      return Expression::Operation::kIsNan;
    case Expression::Operation::kLt:
      return Expression::Operation::kGtEq;
    case Expression::Operation::kLtEq:
      return Expression::Operation::kGt;
    case Expression::Operation::kGt:
      return Expression::Operation::kLtEq;
    case Expression::Operation::kGtEq:
      return Expression::Operation::kLt;
    case Expression::Operation::kEq:
      return Expression::Operation::kNotEq;
    case Expression::Operation::kNotEq:
      return Expression::Operation::kEq;
    case Expression::Operation::kIn:
      return Expression::Operation::kNotIn;
    case Expression::Operation::kNotIn:
      return Expression::Operation::kIn;
    case Expression::Operation::kStartsWith:
      return Expression::Operation::kNotStartsWith;
    case Expression::Operation::kNotStartsWith:
      return Expression::Operation::kStartsWith;
    case Expression::Operation::kTrue:
      return Expression::Operation::kFalse;
    case Expression::Operation::kFalse:
      return Expression::Operation::kTrue;
    case Expression::Operation::kAnd:
      return Expression::Operation::kOr;
    case Expression::Operation::kOr:
      return Expression::Operation::kAnd;
    case Expression::Operation::kNot:
    case Expression::Operation::kCountStar:
    case Expression::Operation::kMax:
    case Expression::Operation::kMin:
    case Expression::Operation::kCount:
      return InvalidArgument("No negation for operation: {}", op);
  }
  std::unreachable();
}

}  // namespace iceberg
