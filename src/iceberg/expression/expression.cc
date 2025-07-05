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

#include "iceberg/util/checked_cast.h"

namespace iceberg {

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

  // Must have a Negate method that returns a shared_ptr to the same concept
  { pred.Negate() } -> std::convertible_to<std::shared_ptr<T>>;

  // Must support equality comparison
  { pred.Equals(pred) } -> std::same_as<bool>;
};

/// \brief Concept specifically for unbound predicates that can be bound
template <typename T>
concept UnboundPredicate = PredicateLike<T> && requires(const T& pred) {
  // Must have a BoundType alias
  typename T::BoundType;

  // Must be bindable to a schema
  requires Bindable<T>;
};

/// \brief Concept specifically for bound predicates
template <typename T>
concept BoundPredicateLike = PredicateLike<T> && requires(const T& pred) {
  // Must have type information
  { pred.type() } -> std::convertible_to<std::shared_ptr<Type>>;

  // Must report that it's bound
  { pred.IsBound() } -> std::convertible_to<bool>;
};

// Internal implementation classes

/// \brief An Expression that is always true.
class True final : public Predicate {
 public:
  /// \brief Returns the singleton instance
  static const std::shared_ptr<True>& Instance();

  Operation op() const override { return Operation::kTrue; }

  std::string ToString() const override { return "true"; }

  std::shared_ptr<Predicate> Negate() const override;

  bool Equals(const Expression& other) const override {
    return other.op() == Operation::kTrue;
  }

 private:
  constexpr True() = default;
};

/// \brief An expression that is always false.
class False final : public Predicate {
 public:
  /// \brief Returns the singleton instance
  static const std::shared_ptr<False>& Instance();

  Operation op() const override { return Operation::kFalse; }

  std::string ToString() const override { return "false"; }

  std::shared_ptr<Predicate> Negate() const override;

  bool Equals(const Expression& other) const override {
    return other.op() == Operation::kFalse;
  }

 private:
  constexpr False() = default;
};

/// \brief An Expression that represents a logical AND operation between two expressions.
class AndImpl final : public Predicate {
 public:
  /// \brief Constructs an And expression from two sub-expressions.
  AndImpl(std::shared_ptr<Predicate> left, std::shared_ptr<Predicate> right);

  /// \brief Returns the left operand of the AND expression.
  const std::shared_ptr<Predicate>& left() const { return left_; }

  /// \brief Returns the right operand of the AND expression.
  const std::shared_ptr<Predicate>& right() const { return right_; }

  Operation op() const override { return Operation::kAnd; }

  std::string ToString() const override;

  std::shared_ptr<Predicate> Negate() const override;

  bool Equals(const Expression& other) const override;

 private:
  std::shared_ptr<Predicate> left_;
  std::shared_ptr<Predicate> right_;
};

/// \brief An Expression that represents a logical OR operation between two expressions.
class OrImpl final : public Predicate {
 public:
  /// \brief Constructs an Or expression from two sub-expressions.
  OrImpl(std::shared_ptr<Predicate> left, std::shared_ptr<Predicate> right);

  /// \brief Returns the left operand of the OR expression.
  const std::shared_ptr<Predicate>& left() const { return left_; }

  /// \brief Returns the right operand of the OR expression.
  const std::shared_ptr<Predicate>& right() const { return right_; }

  Operation op() const override { return Operation::kOr; }

  std::string ToString() const override;

  std::shared_ptr<Predicate> Negate() const override;

  bool Equals(const Expression& other) const override;

 private:
  std::shared_ptr<Predicate> left_;
  std::shared_ptr<Predicate> right_;
};

// Implementation of True
const std::shared_ptr<True>& True::Instance() {
  static const std::shared_ptr<True> instance{new True()};
  return instance;
}

std::shared_ptr<Predicate> True::Negate() const { return False::Instance(); }

// Implementation of False
const std::shared_ptr<False>& False::Instance() {
  static const std::shared_ptr<False> instance = std::shared_ptr<False>(new False());
  return instance;
}

std::shared_ptr<Predicate> False::Negate() const { return True::Instance(); }

// Implementation of AndImpl
AndImpl::AndImpl(std::shared_ptr<Predicate> left, std::shared_ptr<Predicate> right)
    : left_(std::move(left)), right_(std::move(right)) {}

std::string AndImpl::ToString() const {
  return std::format("({} and {})", left_->ToString(), right_->ToString());
}

std::shared_ptr<Predicate> AndImpl::Negate() const {
  // De Morgan's law: not(A and B) = (not A) or (not B)
  auto left_negated = left_->Negate();
  auto right_negated = right_->Negate();
  return std::make_shared<OrImpl>(left_negated, right_negated);
}

bool AndImpl::Equals(const Expression& expr) const {
  if (expr.op() == Operation::kAnd) {
    const auto& other = iceberg::internal::checked_cast<const AndImpl&>(expr);
    return (left_->Equals(*other.left()) && right_->Equals(*other.right())) ||
           (left_->Equals(*other.right()) && right_->Equals(*other.left()));
  }
  return false;
}

// Implementation of OrImpl
OrImpl::OrImpl(std::shared_ptr<Predicate> left, std::shared_ptr<Predicate> right)
    : left_(std::move(left)), right_(std::move(right)) {}

std::string OrImpl::ToString() const {
  return std::format("({} or {})", left_->ToString(), right_->ToString());
}

std::shared_ptr<Predicate> OrImpl::Negate() const {
  // De Morgan's law: not(A or B) = (not A) and (not B)
  auto left_negated = left_->Negate();
  auto right_negated = right_->Negate();
  return std::make_shared<AndImpl>(left_negated, right_negated);
}

bool OrImpl::Equals(const Expression& expr) const {
  if (expr.op() == Operation::kOr) {
    const auto& other = iceberg::internal::checked_cast<const OrImpl&>(expr);
    return (left_->Equals(*other.left()) && right_->Equals(*other.right())) ||
           (left_->Equals(*other.right()) && right_->Equals(*other.left()));
  }
  return false;
}

// Implementation of Predicate static factory methods
const std::shared_ptr<Predicate>& Predicate::AlwaysTrue() {
  static const std::shared_ptr<Predicate> instance = True::Instance();
  return instance;
}

const std::shared_ptr<Predicate>& Predicate::AlwaysFalse() {
  static const std::shared_ptr<Predicate> instance = False::Instance();
  return instance;
}

std::shared_ptr<Predicate> Predicate::And(std::shared_ptr<Predicate> left,
                                          std::shared_ptr<Predicate> right) {
  /*
  auto left_op = left->op();
  auto right_op = right->op();
  if (left_op == Operation::kFalse || right_op == Operation::kFalse) {
    return False::Instance();
  }
  if (left_op == Operation::kTrue && right_op == Operation::kTrue) {
    return left;
  }
  */
  return std::make_shared<AndImpl>(std::move(left), std::move(right));
}

std::shared_ptr<Predicate> Predicate::Or(std::shared_ptr<Predicate> left,
                                         std::shared_ptr<Predicate> right) {
  /*
  auto left_op = left->op();
  auto right_op = right->op();
  if (left_op == Operation::kTrue || right_op == Operation::kTrue) {
    return False::Instance();
  }
  if (left_op == Operation::kFalse && right_op == Operation::kFalse) {
    return left;
  }
  */
  return std::make_shared<OrImpl>(std::move(left), std::move(right));
}

}  // namespace iceberg
