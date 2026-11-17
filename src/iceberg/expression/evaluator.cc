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

#include "iceberg/expression/evaluator.h"

#include <algorithm>
#include <cmath>
#include <compare>
#include <cstring>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "iceberg/expression/binder.h"
#include "iceberg/expression/expression_visitor.h"
#include "iceberg/expression/predicate.h"
#include "iceberg/expression/term.h"
#include "iceberg/row/struct_like.h"
#include "iceberg/schema.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

class EvalVisitor : public BoundVisitor<bool> {
 public:
  explicit EvalVisitor(const StructLike& row) : row_(row) {}

  Result<bool> AlwaysTrue() override { return true; }

  Result<bool> AlwaysFalse() override { return false; }

  Result<bool> Not(bool child_result) override { return !child_result; }

  Result<bool> And(bool left_result, bool right_result) override {
    return left_result && right_result;
  }

  Result<bool> Or(bool left_result, bool right_result) override {
    return left_result || right_result;
  }

  Result<bool> IsNull(const std::shared_ptr<Bound>& expr) override {
    // References can expose validity without converting their value to a literal.
    // This also supports composite and geospatial fields. Transforms must still
    // evaluate their result, which may be null even when the input is not.
    if (auto ref = std::dynamic_pointer_cast<BoundReference>(expr)) {
      ICEBERG_ASSIGN_OR_RAISE(auto value, ref->accessor().Get(row_));
      return std::holds_alternative<std::monostate>(value);
    }
    ICEBERG_ASSIGN_OR_RAISE(auto value, expr->Evaluate(row_));
    return value.IsNull();
  }

  Result<bool> NotNull(const std::shared_ptr<Bound>& expr) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, IsNull(expr));
    return !value;
  }

  Result<bool> IsNaN(const std::shared_ptr<Bound>& expr) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, expr->Evaluate(row_));
    return value.IsNaN();
  }

  Result<bool> NotNaN(const std::shared_ptr<Bound>& expr) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, IsNaN(expr));
    return !value;
  }

  Result<bool> Lt(const std::shared_ptr<Bound>& expr, const Literal& lit) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, expr->Evaluate(row_));
    return value < lit;
  }

  Result<bool> LtEq(const std::shared_ptr<Bound>& expr, const Literal& lit) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, expr->Evaluate(row_));
    return value <= lit;
  }

  Result<bool> Gt(const std::shared_ptr<Bound>& expr, const Literal& lit) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, expr->Evaluate(row_));
    return value > lit;
  }

  Result<bool> GtEq(const std::shared_ptr<Bound>& expr, const Literal& lit) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, expr->Evaluate(row_));
    return value >= lit;
  }

  Result<bool> Eq(const std::shared_ptr<Bound>& expr, const Literal& lit) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, expr->Evaluate(row_));
    return value == lit;
  }

  Result<bool> NotEq(const std::shared_ptr<Bound>& expr, const Literal& lit) override {
    ICEBERG_ASSIGN_OR_RAISE(auto eq_result, Eq(expr, lit));
    return !eq_result;
  }

  Result<bool> In(const std::shared_ptr<Bound>& expr,
                  const BoundSetPredicate::LiteralSet& literal_set) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, expr->Evaluate(row_));
    return literal_set.contains(value);
  }

  Result<bool> NotIn(const std::shared_ptr<Bound>& expr,
                     const BoundSetPredicate::LiteralSet& literal_set) override {
    ICEBERG_ASSIGN_OR_RAISE(auto in_result, In(expr, literal_set));
    return !in_result;
  }

  Result<bool> StartsWith(const std::shared_ptr<Bound>& expr,
                          const Literal& lit) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, expr->Evaluate(row_));

    // Both value and literal should be strings
    if (!std::holds_alternative<std::string>(value.value()) ||
        !std::holds_alternative<std::string>(lit.value())) {
      return false;
    }

    const auto& str_value = std::get<std::string>(value.value());
    const auto& str_prefix = std::get<std::string>(lit.value());
    return str_value.starts_with(str_prefix);
  }

  Result<bool> NotStartsWith(const std::shared_ptr<Bound>& expr,
                             const Literal& lit) override {
    ICEBERG_ASSIGN_OR_RAISE(auto starts_result, StartsWith(expr, lit));
    return !starts_result;
  }

 private:
  const StructLike& row_;
};

}  // namespace

namespace internal {

/// \brief One node of a compiled bound expression, evaluated once per row.
class EvaluatorNode {
 public:
  virtual ~EvaluatorNode() = default;
  virtual Result<bool> Evaluate(const StructLike& row) const = 0;
};

}  // namespace internal

namespace {

using internal::EvaluatorNode;
using NodePtr = std::unique_ptr<const EvaluatorNode>;
using Operation = Expression::Operation;

class ConstantNode : public EvaluatorNode {
 public:
  explicit ConstantNode(bool value) : value_(value) {}
  Result<bool> Evaluate(const StructLike& /*row*/) const override { return value_; }

 private:
  bool value_;
};

class NotNode : public EvaluatorNode {
 public:
  explicit NotNode(NodePtr child) : child_(std::move(child)) {}
  Result<bool> Evaluate(const StructLike& row) const override {
    ICEBERG_ASSIGN_OR_RAISE(auto result, child_->Evaluate(row));
    return !result;
  }

 private:
  NodePtr child_;
};

// Evaluates the right child only if the left child does not decide the result, like
// VisitEvaluator, so an error from a skipped right child is not reported.
class AndNode : public EvaluatorNode {
 public:
  AndNode(NodePtr left, NodePtr right)
      : left_(std::move(left)), right_(std::move(right)) {}
  Result<bool> Evaluate(const StructLike& row) const override {
    ICEBERG_ASSIGN_OR_RAISE(auto left, left_->Evaluate(row));
    if (!left) {
      return false;
    }
    return right_->Evaluate(row);
  }

 private:
  NodePtr left_;
  NodePtr right_;
};

class OrNode : public EvaluatorNode {
 public:
  OrNode(NodePtr left, NodePtr right)
      : left_(std::move(left)), right_(std::move(right)) {}
  Result<bool> Evaluate(const StructLike& row) const override {
    ICEBERG_ASSIGN_OR_RAISE(auto left, left_->Evaluate(row));
    if (left) {
      return true;
    }
    return right_->Evaluate(row);
  }

 private:
  NodePtr left_;
  NodePtr right_;
};

// Evaluates an expression with EvalVisitor, which converts values to literals.
// Used for expressions without a raw-value fast path, and as the fast path's
// fallback, so both report identical results and errors.
class VisitorNode : public EvaluatorNode {
 public:
  explicit VisitorNode(std::shared_ptr<Expression> expr) : expr_(std::move(expr)) {}
  Result<bool> Evaluate(const StructLike& row) const override {
    EvalVisitor visitor(row);
    return Visit<bool, EvalVisitor>(expr_, visitor);
  }

 private:
  std::shared_ptr<Expression> expr_;
};

// Compares `lhs` with `rhs` as unsigned bytes, like std::vector<uint8_t> and
// std::string comparisons.
std::strong_ordering CompareBytes(std::string_view lhs, const void* rhs,
                                  size_t rhs_size) {
  const size_t common = std::min(lhs.size(), rhs_size);
  if (common > 0) {
    if (int result = std::memcmp(lhs.data(), rhs, common); result != 0) {
      return result <=> 0;
    }
  }
  return lhs.size() <=> rhs_size;
}

bool HasRawComparison(TypeId type_id) {
  switch (type_id) {
    case TypeId::kBoolean:
    case TypeId::kInt:
    case TypeId::kDate:
    case TypeId::kLong:
    case TypeId::kTime:
    case TypeId::kTimestamp:
    case TypeId::kTimestampTz:
    case TypeId::kTimestampNs:
    case TypeId::kTimestampTzNs:
    case TypeId::kFloat:
    case TypeId::kDouble:
    case TypeId::kDecimal:
    case TypeId::kString:
    case TypeId::kBinary:
    case TypeId::kFixed:
    case TypeId::kUuid:
      return true;
    default:
      return false;
  }
}

int32_t DecimalScale(const Type& type) {
  return type.type_id() == TypeId::kDecimal
             ? internal::checked_cast<const DecimalType&>(type).scale()
             : 0;
}

// A literal or unary predicate on a field reference, evaluated on the field's raw
// scalar instead of a Literal built for each row. Matches Literal ordering: null
// and out-of-range literals are unordered, floats use the total order, decimals
// compare across scales, and strings and bytes compare as unsigned bytes. Any
// value it cannot compare that way is evaluated by the fallback.
class ReferencePredicateNode : public EvaluatorNode {
 public:
  ReferencePredicateNode(const BoundReference& reference, Operation op,
                         const Literal* literal, std::shared_ptr<Expression> predicate)
      : accessor_(reference.accessor()),
        type_id_(reference.type()->type_id()),
        scale_(DecimalScale(*reference.type())),
        op_(op),
        literal_(literal),
        literal_scale_(literal != nullptr ? DecimalScale(*literal->type()) : 0),
        fallback_(std::move(predicate)) {}

  Result<bool> Evaluate(const StructLike& row) const override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, accessor_.Get(row));
    const bool is_null = std::holds_alternative<std::monostate>(value);
    switch (op_) {
      case Operation::kIsNull:
        return is_null;
      case Operation::kNotNull:
        return !is_null;
      case Operation::kIsNan:
      case Operation::kNotNan: {
        bool is_nan;
        if (is_null) {
          is_nan = false;
        } else if (const auto* f = std::get_if<float>(&value);
                   type_id_ == TypeId::kFloat && f != nullptr) {
          is_nan = std::isnan(*f);
        } else if (const auto* d = std::get_if<double>(&value);
                   type_id_ == TypeId::kDouble && d != nullptr) {
          is_nan = std::isnan(*d);
        } else {
          return fallback_.Evaluate(row);
        }
        return op_ == Operation::kIsNan ? is_nan : !is_nan;
      }
      case Operation::kStartsWith:
      case Operation::kNotStartsWith: {
        const auto* prefix = std::get_if<std::string>(&literal_->value());
        bool starts = false;
        if (const auto* str = std::get_if<std::string_view>(&value);
            str != nullptr && prefix != nullptr) {
          starts = str->starts_with(*prefix);
        } else if (!is_null) {
          return fallback_.Evaluate(row);
        }
        return op_ == Operation::kStartsWith ? starts : !starts;
      }
      default:
        break;
    }

    auto order = Compare(value);
    if (!order.has_value()) {
      return fallback_.Evaluate(row);
    }
    switch (op_) {
      case Operation::kLt:
        return *order < 0;
      case Operation::kLtEq:
        return *order <= 0;
      case Operation::kGt:
        return *order > 0;
      case Operation::kGtEq:
        return *order >= 0;
      case Operation::kEq:
        return *order == 0;
      case Operation::kNotEq:
        return !(*order == 0);
      default:
        return fallback_.Evaluate(row);
    }
  }

  static bool Supports(const BoundPredicate& predicate, TypeId type_id) {
    using Kind = BoundPredicate::Kind;
    const auto kind = predicate.kind();
    switch (predicate.op()) {
      case Operation::kIsNull:
      case Operation::kNotNull:
        return kind == Kind::kUnary;
      case Operation::kIsNan:
      case Operation::kNotNan:
        return kind == Kind::kUnary &&
               (type_id == TypeId::kFloat || type_id == TypeId::kDouble);
      case Operation::kStartsWith:
      case Operation::kNotStartsWith:
        return kind == Kind::kLiteral && type_id == TypeId::kString;
      case Operation::kLt:
      case Operation::kLtEq:
      case Operation::kGt:
      case Operation::kGtEq:
      case Operation::kEq:
      case Operation::kNotEq:
        return kind == Kind::kLiteral && HasRawComparison(type_id);
      default:
        return false;
    }
  }

 private:
  // Orders `value` against the literal like Literal::operator<=>, or returns
  // std::nullopt when the scalar does not hold the field type's representation.
  std::optional<std::partial_ordering> Compare(const Scalar& value) const {
    const auto& literal = literal_->value();
    if (std::holds_alternative<std::monostate>(value) || literal_->IsNull() ||
        literal_->IsAboveMax() || literal_->IsBelowMin()) {
      return std::partial_ordering::unordered;
    }
    auto compare = [&]<typename ScalarT, typename LiteralT>(
                       auto&& order) -> std::optional<std::partial_ordering> {
      const auto* lhs = std::get_if<ScalarT>(&value);
      const auto* rhs = std::get_if<LiteralT>(&literal);
      if (lhs == nullptr || rhs == nullptr) {
        return std::nullopt;
      }
      return order(*lhs, *rhs);
    };
    auto three_way = [](const auto& lhs, const auto& rhs) -> std::partial_ordering {
      return lhs <=> rhs;
    };
    auto total_order = [](auto lhs, auto rhs) -> std::partial_ordering {
      return std::strong_order(lhs, rhs);
    };
    auto bytes = [](std::string_view lhs, const auto& rhs) -> std::partial_ordering {
      return CompareBytes(lhs, rhs.data(), rhs.size());
    };

    switch (type_id_) {
      case TypeId::kBoolean:
        return compare.template operator()<bool, bool>(three_way);
      case TypeId::kInt:
      case TypeId::kDate:
        return compare.template operator()<int32_t, int32_t>(three_way);
      case TypeId::kLong:
      case TypeId::kTime:
      case TypeId::kTimestamp:
      case TypeId::kTimestampTz:
      case TypeId::kTimestampNs:
      case TypeId::kTimestampTzNs:
        return compare.template operator()<int64_t, int64_t>(three_way);
      case TypeId::kFloat:
        return compare.template operator()<float, float>(total_order);
      case TypeId::kDouble:
        return compare.template operator()<double, double>(total_order);
      case TypeId::kDecimal:
        return compare.template operator()<Decimal, Decimal>(
            [this](const Decimal& lhs, const Decimal& rhs) {
              return Decimal::Compare(lhs, rhs, scale_, literal_scale_);
            });
      case TypeId::kString:
        return compare.template operator()<std::string_view, std::string>(bytes);
      case TypeId::kBinary:
      case TypeId::kFixed:
        return compare.template operator()<std::string_view, std::vector<uint8_t>>(bytes);
      case TypeId::kUuid: {
        // Literal conversion rejects values that are not 16 bytes; let it report that.
        const auto* raw = std::get_if<std::string_view>(&value);
        if (raw == nullptr || raw->size() != Uuid::kLength) {
          return std::nullopt;
        }
        return compare.template operator()<std::string_view, Uuid>(
            [](std::string_view lhs, const Uuid& rhs) -> std::partial_ordering {
              return CompareBytes(lhs, rhs.bytes().data(), Uuid::kLength);
            });
      }
      default:
        return std::nullopt;
    }
  }

  // Reads the referenced field's raw scalar from a row. Owned by the bound
  // reference, which the evaluator keeps alive through its bound expression.
  const StructLikeAccessor& accessor_;
  // The field's type, which selects the scalar representation to compare.
  TypeId type_id_;
  // The field's decimal scale, or 0 for other types.
  int32_t scale_;
  // The predicate operation, e.g. kLt or kIsNull.
  Operation op_;
  // The literal to compare with, or nullptr for unary predicates. Owned by the
  // bound predicate, which the evaluator keeps alive.
  const Literal* literal_;
  // The literal's decimal scale, or 0 for other types or unary predicates.
  int32_t literal_scale_;
  // Evaluates the whole predicate with Literal semantics for rows whose scalar
  // does not have the field type's representation, so they keep the same result
  // or error as before.
  VisitorNode fallback_;
};

// Returns a fast node for a predicate on a field reference, or nullptr.
NodePtr CompileReferencePredicate(const std::shared_ptr<Expression>& expr) {
  const auto* predicate = dynamic_cast<const BoundPredicate*>(expr.get());
  if (predicate == nullptr) {
    return nullptr;
  }
  const auto* reference = dynamic_cast<const BoundReference*>(predicate->term().get());
  if (reference == nullptr) {
    return nullptr;
  }
  const TypeId type_id = reference->type()->type_id();
  if (!ReferencePredicateNode::Supports(*predicate, type_id)) {
    return nullptr;
  }

  const Literal* literal = nullptr;
  switch (predicate->kind()) {
    case BoundPredicate::Kind::kUnary:
      break;
    case BoundPredicate::Kind::kLiteral:
      literal =
          &internal::checked_cast<const BoundLiteralPredicate&>(*predicate).literal();
      // Raw values carry the field's representation, so the literal must share it.
      if (literal->type()->type_id() != type_id) {
        return nullptr;
      }
      break;
    case BoundPredicate::Kind::kSet:
      return nullptr;
  }
  return std::make_unique<ReferencePredicateNode>(*reference, predicate->op(), literal,
                                                  expr);
}

NodePtr Compile(const std::shared_ptr<Expression>& expr) {
  if (expr->is_bound_predicate()) {
    if (auto node = CompileReferencePredicate(expr)) {
      return node;
    }
    return std::make_unique<VisitorNode>(expr);
  }
  if (expr->is_unbound_predicate() || expr->is_bound_aggregate() ||
      expr->is_unbound_aggregate()) {
    return std::make_unique<VisitorNode>(expr);
  }
  switch (expr->op()) {
    case Operation::kTrue:
      return std::make_unique<ConstantNode>(true);
    case Operation::kFalse:
      return std::make_unique<ConstantNode>(false);
    case Operation::kNot:
      return std::make_unique<NotNode>(
          Compile(internal::checked_pointer_cast<Not>(expr)->child()));
    case Operation::kAnd: {
      const auto& and_expr = internal::checked_pointer_cast<And>(expr);
      return std::make_unique<AndNode>(Compile(and_expr->left()),
                                       Compile(and_expr->right()));
    }
    case Operation::kOr: {
      const auto& or_expr = internal::checked_pointer_cast<Or>(expr);
      return std::make_unique<OrNode>(Compile(or_expr->left()),
                                      Compile(or_expr->right()));
    }
    default:
      // Reports the visitor's error for unknown operations when evaluated.
      return std::make_unique<VisitorNode>(expr);
  }
}

}  // namespace

Evaluator::Evaluator(std::shared_ptr<Expression> bound_expr,
                     std::unique_ptr<const internal::EvaluatorNode> root)
    : bound_expr_(std::move(bound_expr)), root_(std::move(root)) {}

Evaluator::~Evaluator() = default;

Result<std::unique_ptr<Evaluator>> Evaluator::Make(const Schema& schema,
                                                   std::shared_ptr<Expression> unbound,
                                                   bool case_sensitive) {
  ICEBERG_ASSIGN_OR_RAISE(auto bound_expr, Binder::Bind(schema, unbound, case_sensitive));
  auto root = Compile(bound_expr);
  return std::unique_ptr<Evaluator>(
      new Evaluator(std::move(bound_expr), std::move(root)));
}

Result<bool> Evaluator::Evaluate(const StructLike& row) const {
  return root_->Evaluate(row);
}

}  // namespace iceberg
