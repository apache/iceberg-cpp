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

#include "iceberg/exception.h"
#include "iceberg/expression/term.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/macros.h"
#include "literal.h"

namespace iceberg {

// Internal implementation classes

/// \brief An Expression that is always true.
template <PredicateLike PredicateType>
class TrueImpl final : public PredicateType {
 public:
  /// \brief Returns the singleton instance
  static const std::shared_ptr<TrueImpl>& Instance() {
    static const std::shared_ptr<TrueImpl> instance =
        std::shared_ptr<TrueImpl>(new TrueImpl());
    return instance;
  }

  Operation op() const override { return Operation::kTrue; }

  std::string ToString() const override { return "true"; }

  std::shared_ptr<Predicate> Negate() const override;

  bool Equals(const Expression& other) const override {
    return other.op() == Operation::kTrue;
  }

 protected:
  constexpr TrueImpl() = default;
};

/// \brief An expression that is always false.
template <PredicateLike PredicateType>
class FalseImpl final : public PredicateType {
 public:
  /// \brief Returns the singleton instance
  static const std::shared_ptr<PredicateType>& Instance() {
    static const std::shared_ptr<PredicateType> instance =
        std::shared_ptr<FalseImpl>(new FalseImpl());
    return instance;
  }

  Operation op() const override { return Operation::kFalse; }

  std::string ToString() const override { return "false"; }

  std::shared_ptr<Predicate> Negate() const override {
    return TrueImpl<PredicateType>::Instance();
  }

  bool Equals(const Expression& other) const override {
    return other.op() == Operation::kFalse;
  }

 private:
  constexpr FalseImpl() = default;
};

template <PredicateLike PredicateType>
std::shared_ptr<Predicate> TrueImpl<PredicateType>::Negate() const {
  return FalseImpl<PredicateType>::Instance();
}

/// \brief An Expression that represents a logical AND operation between two expressions.
template <PredicateLike PredicateType>
class AndImpl final : public PredicateType {
 public:
  /// \brief Constructs an And expression from two sub-expressions.
  AndImpl(std::shared_ptr<Predicate> left, std::shared_ptr<Predicate> right)
      : left_(std::move(left)), right_(std::move(right)) {}

  /// \brief Returns the left operand of the AND expression.
  const std::shared_ptr<Predicate>& left() const { return left_; }

  /// \brief Returns the right operand of the AND expression.
  const std::shared_ptr<Predicate>& right() const { return right_; }

  Operation op() const override { return Operation::kAnd; }

  std::string ToString() const override {
    return std::format("({} and {})", left_->ToString(), right_->ToString());
  }

  std::shared_ptr<PredicateType> Negate() const override;

  bool Equals(const Expression& expr) const override {
    if (expr.op() == Operation::kAnd) {
      const auto& other =
          iceberg::internal::checked_cast<const AndImpl<PredicateType>&>(expr);
      return (left_->Equals(*other.left()) && right_->Equals(*other.right())) ||
             (left_->Equals(*other.right()) && right_->Equals(*other.left()));
    }
    return false;
  }

 private:
  std::shared_ptr<Predicate> left_;
  std::shared_ptr<Predicate> right_;
};

/// \brief An Expression that represents a logical OR operation between two expressions.
template <PredicateLike PredicateType>
class OrImpl final : public PredicateType {
 public:
  /// \brief Constructs an Or expression from two sub-expressions.
  OrImpl(std::shared_ptr<PredicateType> left, std::shared_ptr<PredicateType> right)
      : left_(std::move(left)), right_(std::move(right)) {}

  /// \brief Returns the left operand of the OR expression.
  const std::shared_ptr<PredicateType>& left() const { return left_; }

  /// \brief Returns the right operand of the OR expression.
  const std::shared_ptr<PredicateType>& right() const { return right_; }

  Operation op() const override { return Operation::kOr; }

  std::string ToString() const override {
    return std::format("({} or {})", left_->ToString(), right_->ToString());
  }

  std::shared_ptr<PredicateType> Negate() const override;

  bool Equals(const Expression& expr) const override {
    if (expr.op() == Operation::kOr) {
      const auto& other =
          iceberg::internal::checked_cast<const OrImpl<PredicateType>&>(expr);
      return (left_->Equals(*other.left()) && right_->Equals(*other.right())) ||
             (left_->Equals(*other.right()) && right_->Equals(*other.left()));
    }
    return false;
  }

 private:
  std::shared_ptr<PredicateType> left_;
  std::shared_ptr<PredicateType> right_;
};

template <PredicateLike PredicateType>
std::shared_ptr<PredicateType> AndImpl<PredicateType>::Negate() const {
  // De Morgan's law: not(A and B) = (not A) or (not B)
  auto left_negated = left_->Negate();
  auto right_negated = right_->Negate();
  return std::make_shared<OrImpl<PredicateType>>(std::move(left_negated),
                                                 std::move(right_negated));
}

template <PredicateLike PredicateType>
std::shared_ptr<PredicateType> OrImpl<PredicateType>::Negate() const {
  // De Morgan's law: not(A or B) = (not A) and (not B)
  auto left_negated = left_->Negate();
  auto right_negated = right_->Negate();
  return std::make_shared<AndImpl<PredicateType>>(left_negated, right_negated);
}

using True = TrueImpl<Predicate>;
using BoundTrue = TrueImpl<BoundPredicate>;
using False = FalseImpl<Predicate>;
using BoundFalse = FalseImpl<BoundPredicate>;
using AndPredicate = AndImpl<Predicate>;
using BoundAndPredicate = AndImpl<BoundPredicate>;
using OrPredicate = OrImpl<Predicate>;
using BoundOrPredicate = OrImpl<BoundPredicate>;

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
  return std::make_shared<AndPredicate>(std::move(left), std::move(right));
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
  return std::make_shared<OrPredicate>(std::move(left), std::move(right));
}

/// Unary predicate, for example, `a IS NULL`, which `a` is a Term.
///
/// Note that this would not include UnaryPredicates like
/// `COALESCE(a, b) is not null`.
template <typename ReferenceType>
struct UnaryPredicateBase {
  UnaryPredicateBase(Operation in_op, ReferenceType in_reference)
      : unary_op(in_op), reference(std::move(in_reference)) {
    if (!IsUnaryPredicate(unary_op)) {
      throw IcebergError(
          std::format("UnaryPredicateBase: operation {} is not a unary predicate",
                      static_cast<int>(unary_op)));
    }
  }

  Operation unary_op;
  ReferenceType reference;
};

class BoundUnaryPredicate;

class UnaryPredicate final : public UnaryPredicateBase<Reference>, public Predicate {
 public:
  using BoundType = BoundUnaryPredicate;

  UnaryPredicate(Operation op, Reference reference)
      : UnaryPredicateBase<Reference>(op, std::move(reference)) {}

  std::string ToString() const override {
    switch (this->unary_op) {
      case Operation::kIsNull:
        return std::format("{} IS NULL", reference.ToString());
      case Operation::kNotNull:
        return std::format("{} IS NOT NULL", reference.ToString());
      case Operation::kIsNan:
        return std::format("{} IS NAN", reference.ToString());
      case Operation::kNotNan:
        return std::format("{} IS NOT NAN", reference.ToString());
      default:
        return std::format("UnaryPredicate({})", static_cast<int>(unary_op));
    }
  }

  std::shared_ptr<Predicate> Negate() const override {
    Operation negated_op;
    switch (op()) {
      case Operation::kIsNull:
        negated_op = Operation::kNotNull;
        break;
      case Operation::kNotNull:
        negated_op = Operation::kIsNull;
        break;
      case Operation::kIsNan:
        negated_op = Operation::kNotNan;
        break;
      case Operation::kNotNan:
        negated_op = Operation::kIsNan;
        break;
      default:
        throw IcebergError(std::format("Cannot negate unary predicate with operation {}",
                                       static_cast<int>(op())));
    }
    return std::make_shared<UnaryPredicate>(negated_op, reference);
  }

  bool Equals(const Expression& other) const override {
    if (other.op() != op()) {
      return false;
    }
    const auto& other_unary =
        iceberg::internal::checked_cast<const UnaryPredicate&>(other);
    return reference.Equals(other_unary.reference);
  }

  Result<std::unique_ptr<BoundExpression>> Bind(const Schema& schema,
                                                bool case_sensitive) const override;
  Operation op() const override { return unary_op; }
};

class BoundUnaryPredicate final : public UnaryPredicateBase<BoundReference>,
                                  public BoundPredicate {
 public:
  BoundUnaryPredicate(Operation op, BoundReference reference)
      : UnaryPredicateBase<BoundReference>(op, std::move(reference)) {}

  std::string ToString() const override {
    switch (op()) {
      case Operation::kIsNull:
        return std::format("{} IS NULL", reference.ToString());
      case Operation::kNotNull:
        return std::format("{} IS NOT NULL", reference.ToString());
      case Operation::kIsNan:
        return std::format("{} IS NAN", reference.ToString());
      case Operation::kNotNan:
        return std::format("{} IS NOT NAN", reference.ToString());
      default:
        return std::format("BoundUnaryPredicate({})", static_cast<int>(op()));
    }
  }

  bool Equals(const BoundExpression& other) const override {
    if (other.op() != op()) {
      return false;
    }
    const auto& other_unary =
        iceberg::internal::checked_cast<const BoundUnaryPredicate&>(other);
    return reference.Equals(other_unary.reference);
  }

  Operation op() const override { return unary_op; }
};

Result<std::unique_ptr<BoundExpression>> UnaryPredicate::Bind(const Schema& schema,
                                                              bool case_sensitive) const {
  return nullptr;
}

/// Binary predicate, for example, `a = 10`, `b > 5`, etc.
///
/// Represents comparisons between a term (Reference) and a literal value.
template <typename ReferenceType>
struct BinaryPredicateBase {
  BinaryPredicateBase(Operation in_op, ReferenceType in_reference, Literal in_literal)
      : binary_op(in_op),
        reference(std::move(in_reference)),
        literal(std::move(in_literal)) {
    if (!IsBinaryPredicate(binary_op)) {
      throw IcebergError(
          std::format("BinaryPredicateBase: operation {} is not a binary predicate",
                      static_cast<int>(binary_op)));
    }
  }

  Operation binary_op;
  ReferenceType reference;
  Literal literal;
};

class BoundBinaryPredicate;

class BinaryPredicate final : public BinaryPredicateBase<Reference>, public Predicate {
 public:
  using BoundType = BoundBinaryPredicate;

  BinaryPredicate(Operation op, Reference reference, Literal literal)
      : BinaryPredicateBase<Reference>(op, std::move(reference), std::move(literal)) {}

  std::string ToString() const override {
    std::string op_str;
    switch (binary_op) {
      case Operation::kEq:
        op_str = " = ";
        break;
      case Operation::kNotEq:
        op_str = " != ";
        break;
      case Operation::kLt:
        op_str = " < ";
        break;
      case Operation::kLtEq:
        op_str = " <= ";
        break;
      case Operation::kGt:
        op_str = " > ";
        break;
      case Operation::kGtEq:
        op_str = " >= ";
        break;
      case Operation::kStartsWith:
        return std::format("{} STARTS WITH {}", reference.ToString(), literal.ToString());
      case Operation::kNotStartsWith:
        return std::format("{} NOT STARTS WITH {}", reference.ToString(),
                           literal.ToString());
      default:
        return std::format("BinaryPredicate({}, {}, {})", static_cast<int>(binary_op),
                           reference.ToString(), literal.ToString());
    }
    return std::format("{}{}{}", reference.ToString(), op_str, literal.ToString());
  }

  std::shared_ptr<Predicate> Negate() const override {
    Operation negated_op;
    switch (binary_op) {
      case Operation::kEq:
        negated_op = Operation::kNotEq;
        break;
      case Operation::kNotEq:
        negated_op = Operation::kEq;
        break;
      case Operation::kLt:
        negated_op = Operation::kGtEq;
        break;
      case Operation::kLtEq:
        negated_op = Operation::kGt;
        break;
      case Operation::kGt:
        negated_op = Operation::kLtEq;
        break;
      case Operation::kGtEq:
        negated_op = Operation::kLt;
        break;
      case Operation::kStartsWith:
        negated_op = Operation::kNotStartsWith;
        break;
      case Operation::kNotStartsWith:
        negated_op = Operation::kStartsWith;
        break;
      default:
        throw IcebergError(std::format("Cannot negate binary predicate with operation {}",
                                       static_cast<int>(binary_op)));
    }
    return std::make_shared<BinaryPredicate>(negated_op, reference, literal);
  }

  bool Equals(const Expression& other) const override {
    if (other.op() != binary_op) {
      return false;
    }
    const auto& other_binary =
        iceberg::internal::checked_cast<const BinaryPredicate&>(other);
    return reference.Equals(other_binary.reference) && literal == other_binary.literal;
  }

  Result<std::unique_ptr<BoundExpression>> Bind(const Schema& schema,
                                                bool case_sensitive) const override;

  Operation op() const override { return binary_op; }
};

class BoundBinaryPredicate final : public BinaryPredicateBase<BoundReference>,
                                   public BoundPredicate {
 public:
  BoundBinaryPredicate(Operation op, BoundReference reference, Literal literal)
      : BinaryPredicateBase<BoundReference>(op, std::move(reference),
                                            std::move(literal)) {}

  std::string ToString() const override {
    std::string op_str;
    switch (binary_op) {
      case Operation::kEq:
        op_str = " = ";
        break;
      case Operation::kNotEq:
        op_str = " != ";
        break;
      case Operation::kLt:
        op_str = " < ";
        break;
      case Operation::kLtEq:
        op_str = " <= ";
        break;
      case Operation::kGt:
        op_str = " > ";
        break;
      case Operation::kGtEq:
        op_str = " >= ";
        break;
      case Operation::kStartsWith:
        return std::format("{} STARTS WITH {}", reference.ToString(), literal.ToString());
      case Operation::kNotStartsWith:
        return std::format("{} NOT STARTS WITH {}", reference.ToString(),
                           literal.ToString());
      default:
        return std::format("BoundBinaryPredicate({}, {}, {})",
                           static_cast<int>(binary_op), reference.ToString(),
                           literal.ToString());
    }
    return std::format("{}{}{}", reference.ToString(), op_str, literal.ToString());
  }

  bool Equals(const BoundExpression& other) const override {
    if (other.op() != binary_op) {
      return false;
    }
    const auto& other_binary =
        iceberg::internal::checked_cast<const BoundBinaryPredicate&>(other);
    return reference.Equals(other_binary.reference) && literal == other_binary.literal;
  }

  Operation op() const override { return binary_op; }
};

// Implementation of BinaryPredicate::Bind
Result<std::unique_ptr<BoundExpression>> BinaryPredicate::Bind(
    const Schema& schema, bool case_sensitive) const {
  return nullptr;
}

}  // namespace iceberg
