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

#include <algorithm>
#include <memory>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>

#include "iceberg/expression/literal.h"
#include "iceberg/expression/predicate.h"
#include "iceberg/expression/term.h"
#include "iceberg/result.h"
#include "iceberg/transform.h"
#include "iceberg/transform_function.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/decimal.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/string_util.h"

namespace iceberg {

class ProjectionUtil {
 private:
  static Result<Literal> AdjustLiteral(const Literal& literal, int adjustment) {
    switch (literal.type()->type_id()) {
      case TypeId::kInt:
        return Literal::Int(std::get<int32_t>(literal.value()) + adjustment);
      case TypeId::kLong:
        return Literal::Long(std::get<int64_t>(literal.value()) + adjustment);
      case TypeId::kDate:
        return Literal::Date(std::get<int32_t>(literal.value()) + adjustment);
      case TypeId::kTimestamp:
        return Literal::Timestamp(std::get<int64_t>(literal.value()) + adjustment);
      case TypeId::kTimestampTz:
        return Literal::TimestampTz(std::get<int64_t>(literal.value()) + adjustment);
      case TypeId::kDecimal: {
        const auto& decimal_type =
            internal::checked_cast<const DecimalType&>(*literal.type());
        Decimal adjusted = std::get<Decimal>(literal.value()) + Decimal(adjustment);
        return Literal::Decimal(adjusted.value(), decimal_type.precision(),
                                decimal_type.scale());
      }
      default:
        return NotSupported("{} is not a valid literal type for value adjustment",
                            literal.type()->ToString());
    }
  }

  static Result<Literal> PlusOne(const Literal& literal) {
    return AdjustLiteral(literal, /*adjustment=*/+1);
  }

  static Result<Literal> MinusOne(const Literal& literal) {
    return AdjustLiteral(literal, /*adjustment=*/-1);
  }

  static Result<std::unique_ptr<UnboundPredicate>> MakePredicate(
      Expression::Operation op, std::string_view name,
      const std::shared_ptr<TransformFunction>& func, const Literal& literal) {
    ICEBERG_ASSIGN_OR_RAISE(auto ref, NamedReference::Make(std::string(name)));
    ICEBERG_ASSIGN_OR_RAISE(auto lit, func->Transform(literal));
    return UnboundPredicateImpl<BoundReference>::Make(op, std::move(ref), std::move(lit));
  }

  static Result<std::unique_ptr<UnboundPredicate>> TransformSet(
      std::string_view name, const std::shared_ptr<BoundSetPredicate>& pred,
      const std::shared_ptr<TransformFunction>& func) {
    std::vector<Literal> transformed;
    transformed.reserve(pred->literal_set().size());
    for (const auto& lit : pred->literal_set()) {
      ICEBERG_ASSIGN_OR_RAISE(auto transformed_lit, func->Transform(lit));
      transformed.push_back(std::move(transformed_lit));
    }
    ICEBERG_ASSIGN_OR_RAISE(auto ref, NamedReference::Make(std::string(name)));
    return UnboundPredicateImpl<BoundReference>::Make(pred->op(), std::move(ref),
                                                      std::move(transformed));
  }

  static Result<std::unique_ptr<UnboundPredicate>> TruncateByteArray(
      std::string_view name, const std::shared_ptr<BoundLiteralPredicate>& pred,
      const std::shared_ptr<TransformFunction>& func) {
    switch (pred->op()) {
      case Expression::Operation::kLt:
      case Expression::Operation::kLtEq:
        return MakePredicate(Expression::Operation::kLtEq, name, func, pred->literal());
      case Expression::Operation::kGt:
      case Expression::Operation::kGtEq:
        return MakePredicate(Expression::Operation::kGtEq, name, func, pred->literal());
      case Expression::Operation::kEq:
      case Expression::Operation::kStartsWith:
        return MakePredicate(pred->op(), name, func, pred->literal());
      default:
        return nullptr;
    }
  }

  static Result<std::unique_ptr<UnboundPredicate>> TruncateByteArrayStrict(
      std::string_view name, const std::shared_ptr<BoundLiteralPredicate>& pred,
      const std::shared_ptr<TransformFunction>& func) {
    switch (pred->op()) {
      case Expression::Operation::kLt:
      case Expression::Operation::kLtEq:
        return MakePredicate(Expression::Operation::kLt, name, func, pred->literal());
      case Expression::Operation::kGt:
      case Expression::Operation::kGtEq:
        return MakePredicate(Expression::Operation::kGt, name, func, pred->literal());
      case Expression::Operation::kNotEq:
        return MakePredicate(Expression::Operation::kNotEq, name, func, pred->literal());
      default:
        return nullptr;
    }
  }

  // Apply to int32, int64, decimal, and temporal types
  static Result<std::unique_ptr<UnboundPredicate>> TransformNumeric(
      std::string_view name, const std::shared_ptr<BoundLiteralPredicate>& pred,
      const std::shared_ptr<TransformFunction>& func) {
    switch (func->source_type()->type_id()) {
      case TypeId::kInt:
      case TypeId::kLong:
      case TypeId::kDecimal:
      case TypeId::kDate:
      case TypeId::kTimestamp:
      case TypeId::kTimestampTz:
        break;
      default:
        return NotSupported("{} is not a valid input type for numeric transform",
                            func->source_type()->ToString());
    }

    switch (pred->op()) {
      case Expression::Operation::kLt: {
        // adjust closed and then transform ltEq
        ICEBERG_ASSIGN_OR_RAISE(auto adjusted, MinusOne(pred->literal()));
        return MakePredicate(Expression::Operation::kLtEq, name, func, adjusted);
      }
      case Expression::Operation::kGt: {
        // adjust closed and then transform gtEq
        ICEBERG_ASSIGN_OR_RAISE(auto adjusted, PlusOne(pred->literal()));
        return MakePredicate(Expression::Operation::kGtEq, name, func, adjusted);
      }
      case Expression::Operation::kLtEq:
      case Expression::Operation::kGtEq:
      case Expression::Operation::kEq:
        return MakePredicate(pred->op(), name, func, pred->literal());
      default:
        return nullptr;
    }
  }

  static Result<std::unique_ptr<UnboundPredicate>> TransformNumericStrict(
      std::string_view name, const std::shared_ptr<BoundLiteralPredicate>& pred,
      const std::shared_ptr<TransformFunction>& func) {
    switch (func->source_type()->type_id()) {
      case TypeId::kInt:
      case TypeId::kLong:
      case TypeId::kDecimal:
      case TypeId::kDate:
      case TypeId::kTimestamp:
      case TypeId::kTimestampTz:
        break;
      default:
        return NotSupported("{} is not a valid input type for numeric transform",
                            func->source_type()->ToString());
    }

    switch (pred->op()) {
      case Expression::Operation::kLtEq: {
        ICEBERG_ASSIGN_OR_RAISE(auto adjusted, PlusOne(pred->literal()));
        return MakePredicate(Expression::Operation::kLt, name, func, adjusted);
      }
      case Expression::Operation::kGtEq: {
        ICEBERG_ASSIGN_OR_RAISE(auto adjusted, MinusOne(pred->literal()));
        return MakePredicate(Expression::Operation::kGt, name, func, adjusted);
      }
      case Expression::Operation::kLt:
      case Expression::Operation::kGt:
      case Expression::Operation::kNotEq:
        return MakePredicate(pred->op(), name, func, pred->literal());
      default:
        return nullptr;
    }
  }

  static Result<std::unique_ptr<UnboundPredicate>> TruncateStringLiteral(
      std::string_view name, const std::shared_ptr<BoundLiteralPredicate>& pred,
      const std::shared_ptr<TransformFunction>& func) {
    const auto op = pred->op();
    if (op != Expression::Operation::kStartsWith &&
        op != Expression::Operation::kNotStartsWith) {
      return TruncateByteArray(name, pred, func);
    }

    const auto& literal = pred->literal();
    const auto length =
        StringUtils::CodePointCount(std::get<std::string>(literal.value()));
    const auto width = static_cast<size_t>(
        internal::checked_pointer_cast<TruncateTransform>(func)->width());

    if (length < width) {
      return MakePredicate(op, name, func, literal);
    }

    if (length == width) {
      if (op == Expression::Operation::kStartsWith) {
        return MakePredicate(Expression::Operation::kEq, name, func, literal);
      } else {
        return MakePredicate(Expression::Operation::kNotEq, name, func, literal);
      }
    }

    if (op == Expression::Operation::kStartsWith) {
      return TruncateByteArray(name, pred, func);
    }

    return nullptr;
  }

  static Result<std::unique_ptr<UnboundPredicate>> TruncateStringLiteralStrict(
      std::string_view name, const std::shared_ptr<BoundLiteralPredicate>& pred,
      const std::shared_ptr<TransformFunction>& func) {
    const auto op = pred->op();
    if (op != Expression::Operation::kStartsWith &&
        op != Expression::Operation::kNotStartsWith) {
      return TruncateByteArrayStrict(name, pred, func);
    }

    const auto& literal = pred->literal();
    const auto length =
        StringUtils::CodePointCount(std::get<std::string>(literal.value()));
    const auto width = static_cast<size_t>(
        internal::checked_pointer_cast<TruncateTransform>(func)->width());

    if (length < width) {
      return MakePredicate(op, name, func, literal);
    }

    if (length == width) {
      if (op == Expression::Operation::kStartsWith) {
        return MakePredicate(Expression::Operation::kEq, name, func, literal);
      } else {
        return MakePredicate(Expression::Operation::kNotEq, name, func, literal);
      }
    }

    if (op == Expression::Operation::kNotStartsWith) {
      return MakePredicate(Expression::Operation::kNotStartsWith, name, func, literal);
    }

    return nullptr;
  }

  // Fixes an inclusive projection to account for incorrectly transformed values.
  // align with Java implementation:
  // https://github.com/apache/iceberg/blob/1.10.x/api/src/main/java/org/apache/iceberg/transforms/ProjectionUtil.java#L275
  static Result<std::unique_ptr<UnboundPredicate>> FixInclusiveTimeProjection(
      std::unique_ptr<UnboundPredicateImpl<BoundReference>> projected) {
    if (projected == nullptr) {
      return nullptr;
    }

    // adjust the predicate for values that were 1 larger than the correct transformed
    // value
    switch (projected->op()) {
      case Expression::Operation::kLt: {
        ICEBERG_DCHECK(!projected->literals().empty(), "Expected at least one literal");
        const auto& literal = projected->literals().front();
        ICEBERG_DCHECK(std::holds_alternative<int32_t>(literal.value()),
                       "Expected int32_t");
        if (auto value = std::get<int32_t>(literal.value()); value < 0) {
          return UnboundPredicateImpl<BoundReference>::Make(Expression::Operation::kLt,
                                                            std::move(projected->term()),
                                                            Literal::Int(value + 1));
        }

        return projected;
      }

      case Expression::Operation::kLtEq: {
        ICEBERG_DCHECK(!projected->literals().empty(), "Expected at least one literal");
        const auto& literal = projected->literals().front();
        ICEBERG_DCHECK(std::holds_alternative<int32_t>(literal.value()),
                       "Expected int32_t");

        if (auto value = std::get<int32_t>(literal.value()); value < 0) {
          return UnboundPredicateImpl<BoundReference>::Make(Expression::Operation::kLtEq,
                                                            std::move(projected->term()),
                                                            Literal::Int(value + 1));
        }
        return projected;
      }

      case Expression::Operation::kGt:
      case Expression::Operation::kGtEq:
        // incorrect projected values are already greater than the bound for GT, GT_EQ
        return projected;

      case Expression::Operation::kEq: {
        ICEBERG_DCHECK(!projected->literals().empty(), "Expected at least one literal");
        const auto& literal = projected->literals().front();
        ICEBERG_DCHECK(std::holds_alternative<int32_t>(literal.value()),
                       "Expected int32_t");
        if (auto value = std::get<int32_t>(literal.value()); value < 0) {
          // match either the incorrect value (projectedValue + 1) or the correct value
          // (projectedValue)
          return UnboundPredicateImpl<BoundReference>::Make(
              Expression::Operation::kIn, std::move(projected->term()),
              {literal, Literal::Int(value + 1)});
        }
        return projected;
      }

      case Expression::Operation::kIn: {
        ICEBERG_DCHECK(!projected->literals().empty(), "Expected at least one literal");
        const auto& literals = projected->literals();
        ICEBERG_DCHECK(
            std::ranges::all_of(literals,
                                [](const auto& lit) {
                                  return std::holds_alternative<int32_t>(lit.value());
                                }),
            "Expected int32_t");
        std::unordered_set<int32_t> value_set;
        bool has_negative_value = false;
        for (const auto& lit : literals) {
          auto value = std::get<int32_t>(lit.value());
          value_set.insert(value);
          if (value < 0) {
            value_set.insert(value + 1);
            has_negative_value = true;
          }
        }
        if (has_negative_value) {
          auto values =
              std::views::transform(value_set,
                                    [](int32_t value) { return Literal::Int(value); }) |
              std::ranges::to<std::vector>();
          return UnboundPredicateImpl<BoundReference>::Make(Expression::Operation::kIn,
                                                            std::move(projected->term()),
                                                            std::move(values));
        }
        return projected;
      }

      case Expression::Operation::kNotIn:
      case Expression::Operation::kNotEq:
        // there is no inclusive projection for NOT_EQ and NOT_IN
        return nullptr;

      default:
        return projected;
    }
  }

  // Fixes a strict projection to account for incorrectly transformed values.
  // align with Java implementation:
  // https://github.com/apache/iceberg/blob/1.10.x/api/src/main/java/org/apache/iceberg/transforms/ProjectionUtil.java#L347
  static Result<std::unique_ptr<UnboundPredicate>> FixStrictTimeProjection(
      std::unique_ptr<UnboundPredicateImpl<BoundReference>> projected) {
    if (projected == nullptr) {
      return nullptr;
    }

    switch (projected->op()) {
      case Expression::Operation::kLt:
      case Expression::Operation::kLtEq:
        // the correct bound is a correct strict projection for the incorrectly
        // transformed values.
        return projected;

      case Expression::Operation::kGt: {
        // GT and GT_EQ need to be adjusted because values that do not match the predicate
        // may have been transformed into partition values that match the projected
        // predicate.
        ICEBERG_DCHECK(!projected->literals().empty(), "Expected at least one literal");
        const auto& literal = projected->literals().front();
        ICEBERG_DCHECK(std::holds_alternative<int32_t>(literal.value()),
                       "Expected int32_t");
        if (auto value = std::get<int32_t>(literal.value()); value <= 0) {
          return UnboundPredicateImpl<BoundReference>::Make(Expression::Operation::kGt,
                                                            std::move(projected->term()),
                                                            Literal::Int(value + 1));
        }
        return projected;
      }

      case Expression::Operation::kGtEq: {
        ICEBERG_DCHECK(!projected->literals().empty(), "Expected at least one literal");
        const auto& literal = projected->literals().front();
        ICEBERG_DCHECK(std::holds_alternative<int32_t>(literal.value()),
                       "Expected int32_t");
        if (auto value = std::get<int32_t>(literal.value()); value <= 0) {
          return UnboundPredicateImpl<BoundReference>::Make(Expression::Operation::kGtEq,
                                                            std::move(projected->term()),
                                                            Literal::Int(value + 1));
        }
        return projected;
      }

      case Expression::Operation::kEq:
      case Expression::Operation::kIn:
        // there is no strict projection for EQ and IN
        return nullptr;

      case Expression::Operation::kNotEq: {
        ICEBERG_DCHECK(!projected->literals().empty(), "Expected at least one literal");
        const auto& literal = projected->literals().front();
        ICEBERG_DCHECK(std::holds_alternative<int32_t>(literal.value()),
                       "Expected int32_t");
        if (auto value = std::get<int32_t>(literal.value()); value < 0) {
          return UnboundPredicateImpl<BoundReference>::Make(
              Expression::Operation::kNotIn, std::move(projected->term()),
              {literal, Literal::Int(value + 1)});
        }
        return projected;
      }

      case Expression::Operation::kNotIn: {
        ICEBERG_DCHECK(!projected->literals().empty(), "Expected at least one literal");
        const auto& literals = projected->literals();
        ICEBERG_DCHECK(
            std::ranges::all_of(literals,
                                [](const auto& lit) {
                                  return std::holds_alternative<int32_t>(lit.value());
                                }),
            "Expected int32_t");
        std::unordered_set<int32_t> value_set;
        bool has_negative_value = false;
        for (const auto& lit : literals) {
          auto value = std::get<int32_t>(lit.value());
          value_set.insert(value);
          if (value < 0) {
            value_set.insert(value + 1);
            has_negative_value = true;
          }
        }
        if (has_negative_value) {
          auto values =
              std::views::transform(value_set,
                                    [](int32_t value) { return Literal::Int(value); }) |
              std::ranges::to<std::vector>();
          return UnboundPredicateImpl<BoundReference>::Make(Expression::Operation::kNotIn,
                                                            std::move(projected->term()),
                                                            std::move(values));
        }
        return projected;
      }

      default:
        return nullptr;
    }
  }

 public:
  static Result<std::unique_ptr<UnboundPredicate>> IdentityProject(
      std::string_view name, const std::shared_ptr<BoundPredicate>& pred) {
    ICEBERG_ASSIGN_OR_RAISE(auto ref, NamedReference::Make(std::string(name)));
    switch (pred->kind()) {
      case BoundPredicate::Kind::kUnary: {
        return UnboundPredicateImpl<BoundReference>::Make(pred->op(), std::move(ref));
      }
      case BoundPredicate::Kind::kLiteral: {
        const auto& literalPredicate =
            internal::checked_pointer_cast<BoundLiteralPredicate>(pred);
        return UnboundPredicateImpl<BoundReference>::Make(pred->op(), std::move(ref),
                                                          literalPredicate->literal());
      }
      case BoundPredicate::Kind::kSet: {
        const auto& setPredicate =
            internal::checked_pointer_cast<BoundSetPredicate>(pred);
        return UnboundPredicateImpl<BoundReference>::Make(
            pred->op(), std::move(ref),
            std::vector<Literal>(setPredicate->literal_set().begin(),
                                 setPredicate->literal_set().end()));
      }
    }
    std::unreachable();
  }

  static Result<std::unique_ptr<UnboundPredicate>> BucketProject(
      std::string_view name, const std::shared_ptr<BoundPredicate>& pred,
      const std::shared_ptr<TransformFunction>& func) {
    ICEBERG_ASSIGN_OR_RAISE(auto ref, NamedReference::Make(std::string(name)));
    switch (pred->kind()) {
      case BoundPredicate::Kind::kUnary: {
        return UnboundPredicateImpl<BoundReference>::Make(pred->op(), std::move(ref));
      }
      case BoundPredicate::Kind::kLiteral: {
        if (pred->op() == Expression::Operation::kEq) {
          const auto& literalPredicate =
              internal::checked_pointer_cast<BoundLiteralPredicate>(pred);
          ICEBERG_ASSIGN_OR_RAISE(auto transformed,
                                  func->Transform(literalPredicate->literal()));
          return UnboundPredicateImpl<BoundReference>::Make(pred->op(), std::move(ref),
                                                            std::move(transformed));
        }
        break;
      }
      case BoundPredicate::Kind::kSet: {
        // notIn can't be projected
        if (pred->op() == Expression::Operation::kIn) {
          const auto& setPredicate =
              internal::checked_pointer_cast<BoundSetPredicate>(pred);
          return TransformSet(name, setPredicate, func);
        }
        break;
      }
    }

    // comparison predicates can't be projected, notEq can't be projected
    // TODO(anyone): small ranges can be projected.
    // for example, (x > 0) and (x < 3) can be turned into in({1, 2}) and projected.
    return nullptr;
  }

  static Result<std::unique_ptr<UnboundPredicate>> TruncateProject(
      std::string_view name, const std::shared_ptr<BoundPredicate>& pred,
      const std::shared_ptr<TransformFunction>& func) {
    ICEBERG_ASSIGN_OR_RAISE(auto ref, NamedReference::Make(std::string(name)));
    // Handle unary predicates uniformly for all types
    if (pred->kind() == BoundPredicate::Kind::kUnary) {
      return UnboundPredicateImpl<BoundReference>::Make(pred->op(), std::move(ref));
    }

    // Handle set predicates (kIn) uniformly for all types
    if (pred->kind() == BoundPredicate::Kind::kSet) {
      if (pred->op() == Expression::Operation::kIn) {
        const auto& setPredicate =
            internal::checked_pointer_cast<BoundSetPredicate>(pred);
        return TransformSet(name, setPredicate, func);
      }
      return nullptr;
    }

    // Handle literal predicates based on source type
    const auto& literalPredicate =
        internal::checked_pointer_cast<BoundLiteralPredicate>(pred);

    switch (func->source_type()->type_id()) {
      case TypeId::kInt:
      case TypeId::kLong:
      case TypeId::kDecimal:
        return TransformNumeric(name, literalPredicate, func);
      case TypeId::kString:
        return TruncateStringLiteral(name, literalPredicate, func);
      case TypeId::kBinary:
        return TruncateByteArray(name, literalPredicate, func);
      default:
        return NotSupported("{} is not a valid input type for truncate transform",
                            func->source_type()->ToString());
    }
  }

  static Result<std::unique_ptr<UnboundPredicate>> TemporalProject(
      std::string_view name, const std::shared_ptr<BoundPredicate>& pred,
      const std::shared_ptr<TransformFunction>& func) {
    ICEBERG_ASSIGN_OR_RAISE(auto ref, NamedReference::Make(std::string(name)));
    if (pred->kind() == BoundPredicate::Kind::kUnary) {
      return UnboundPredicateImpl<BoundReference>::Make(pred->op(), std::move(ref));
    } else if (pred->kind() == BoundPredicate::Kind::kLiteral) {
      const auto& literalPredicate =
          internal::checked_pointer_cast<BoundLiteralPredicate>(pred);
      ICEBERG_ASSIGN_OR_RAISE(auto projected,
                              TransformNumeric(name, literalPredicate, func));
      if (func->transform_type() != TransformType::kDay ||
          func->source_type()->type_id() != TypeId::kDate) {
        return FixInclusiveTimeProjection(
            internal::checked_pointer_cast<UnboundPredicateImpl<BoundReference>>(
                std::move(projected)));
      }
      return projected;
    } else if (pred->kind() == BoundPredicate::Kind::kSet &&
               pred->op() == Expression::Operation::kIn) {
      const auto& setPredicate = internal::checked_pointer_cast<BoundSetPredicate>(pred);
      ICEBERG_ASSIGN_OR_RAISE(auto projected, TransformSet(name, setPredicate, func));
      if (func->transform_type() != TransformType::kDay ||
          func->source_type()->type_id() != TypeId::kDate) {
        return FixInclusiveTimeProjection(
            internal::checked_pointer_cast<UnboundPredicateImpl<BoundReference>>(
                std::move(projected)));
      }
      return projected;
    }

    return nullptr;
  }

  static Result<std::unique_ptr<UnboundPredicate>> RemoveTransform(
      std::string_view name, const std::shared_ptr<BoundPredicate>& pred) {
    ICEBERG_ASSIGN_OR_RAISE(auto ref, NamedReference::Make(std::string(name)));
    switch (pred->kind()) {
      case BoundPredicate::Kind::kUnary: {
        return UnboundPredicateImpl<BoundReference>::Make(pred->op(), std::move(ref));
      }
      case BoundPredicate::Kind::kLiteral: {
        const auto& literalPredicate =
            internal::checked_pointer_cast<BoundLiteralPredicate>(pred);
        return UnboundPredicateImpl<BoundReference>::Make(pred->op(), std::move(ref),
                                                          literalPredicate->literal());
      }
      case BoundPredicate::Kind::kSet: {
        const auto& setPredicate =
            internal::checked_pointer_cast<BoundSetPredicate>(pred);
        return UnboundPredicateImpl<BoundReference>::Make(
            pred->op(), std::move(ref),
            std::vector<Literal>(setPredicate->literal_set().begin(),
                                 setPredicate->literal_set().end()));
      }
    }
    std::unreachable();
  }

  static Result<std::unique_ptr<UnboundPredicate>> BucketProjectStrict(
      std::string_view name, const std::shared_ptr<BoundPredicate>& pred,
      const std::shared_ptr<TransformFunction>& func) {
    ICEBERG_ASSIGN_OR_RAISE(auto ref, NamedReference::Make(std::string(name)));
    switch (pred->kind()) {
      case BoundPredicate::Kind::kUnary: {
        return UnboundPredicateImpl<BoundReference>::Make(pred->op(), std::move(ref));
      }
      case BoundPredicate::Kind::kLiteral: {
        if (pred->op() == Expression::Operation::kNotEq) {
          const auto& literalPredicate =
              internal::checked_pointer_cast<BoundLiteralPredicate>(pred);
          ICEBERG_ASSIGN_OR_RAISE(auto transformed,
                                  func->Transform(literalPredicate->literal()));
          // TODO(anyone): need to translate not(eq(...)) into notEq in expressions
          return UnboundPredicateImpl<BoundReference>::Make(pred->op(), std::move(ref),
                                                            std::move(transformed));
        }
        break;
      }
      case BoundPredicate::Kind::kSet: {
        if (pred->op() == Expression::Operation::kNotIn) {
          const auto& setPredicate =
              internal::checked_pointer_cast<BoundSetPredicate>(pred);
          return TransformSet(name, setPredicate, func);
        }
        break;
      }
    }

    // no strict projection for comparison or equality
    return nullptr;
  }

  static Result<std::unique_ptr<UnboundPredicate>> TruncateProjectStrict(
      std::string_view name, const std::shared_ptr<BoundPredicate>& pred,
      const std::shared_ptr<TransformFunction>& func) {
    ICEBERG_ASSIGN_OR_RAISE(auto ref, NamedReference::Make(std::string(name)));
    // Handle unary predicates uniformly for all types
    if (pred->kind() == BoundPredicate::Kind::kUnary) {
      return UnboundPredicateImpl<BoundReference>::Make(pred->op(), std::move(ref));
    }

    // Handle set predicates (kNotIn) uniformly for all types
    if (pred->kind() == BoundPredicate::Kind::kSet) {
      if (pred->op() == Expression::Operation::kNotIn) {
        const auto& setPredicate =
            internal::checked_pointer_cast<BoundSetPredicate>(pred);
        return TransformSet(name, setPredicate, func);
      }
      return nullptr;
    }

    // Handle literal predicates based on source type
    const auto& literalPredicate =
        internal::checked_pointer_cast<BoundLiteralPredicate>(pred);

    switch (func->source_type()->type_id()) {
      case TypeId::kInt:
      case TypeId::kLong:
      case TypeId::kDecimal:
        return TransformNumericStrict(name, literalPredicate, func);
      case TypeId::kString:
        return TruncateStringLiteralStrict(name, literalPredicate, func);
      case TypeId::kBinary:
        return TruncateByteArrayStrict(name, literalPredicate, func);
      default:
        return NotSupported("{} is not a valid input type for truncate transform",
                            func->source_type()->ToString());
    }
  }

  static Result<std::unique_ptr<UnboundPredicate>> TemporalProjectStrict(
      std::string_view name, const std::shared_ptr<BoundPredicate>& pred,
      const std::shared_ptr<TransformFunction>& func) {
    ICEBERG_ASSIGN_OR_RAISE(auto ref, NamedReference::Make(std::string(name)));
    if (pred->kind() == BoundPredicate::Kind::kUnary) {
      return UnboundPredicateImpl<BoundReference>::Make(pred->op(), std::move(ref));
    } else if (pred->kind() == BoundPredicate::Kind::kLiteral) {
      const auto& literalPredicate =
          internal::checked_pointer_cast<BoundLiteralPredicate>(pred);
      ICEBERG_ASSIGN_OR_RAISE(auto projected,
                              TransformNumericStrict(name, literalPredicate, func));
      if (func->transform_type() != TransformType::kDay ||
          func->source_type()->type_id() != TypeId::kDate) {
        return FixStrictTimeProjection(
            internal::checked_pointer_cast<UnboundPredicateImpl<BoundReference>>(
                std::move(projected)));
      }
      return projected;
    } else if (pred->kind() == BoundPredicate::Kind::kSet &&
               pred->op() == Expression::Operation::kNotIn) {
      const auto& setPredicate = internal::checked_pointer_cast<BoundSetPredicate>(pred);
      ICEBERG_ASSIGN_OR_RAISE(auto projected, TransformSet(name, setPredicate, func));
      if (func->transform_type() != TransformType::kDay ||
          func->source_type()->type_id() != TypeId::kDate) {
        return FixStrictTimeProjection(
            internal::checked_pointer_cast<UnboundPredicateImpl<BoundReference>>(
                std::move(projected)));
      }
      return projected;
    }

    return nullptr;
  }
};

}  // namespace iceberg
