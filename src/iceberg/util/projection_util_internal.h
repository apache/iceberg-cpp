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
#include <utility>

#include "iceberg/expression/literal.h"
#include "iceberg/expression/predicate.h"
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
  static Result<std::unique_ptr<UnboundPredicate>> TransformSet(
      std::string_view name, const std::shared_ptr<BoundSetPredicate>& predicate,
      const std::shared_ptr<TransformFunction>& func) {
    std::vector<Literal> transformed;
    transformed.reserve(predicate->literal_set().size());
    for (const auto& lit : predicate->literal_set()) {
      ICEBERG_ASSIGN_OR_RAISE(auto transformed_lit, func->Transform(lit));
      transformed.push_back(std::move(transformed_lit));
    }
    ICEBERG_ASSIGN_OR_RAISE(auto ref, NamedReference::Make(std::string(name)));
    return UnboundPredicateImpl<BoundReference>::Make(predicate->op(), std::move(ref),
                                                      std::move(transformed));
  }

  // General transform for all literal predicates.  This is used as a fallback for special
  // cases that are not handled by the other transform functions.
  static Result<std::unique_ptr<UnboundPredicate>> GenericTransform(
      std::unique_ptr<NamedReference> ref,
      const std::shared_ptr<BoundLiteralPredicate>& predicate,
      const std::shared_ptr<TransformFunction>& func) {
    ICEBERG_ASSIGN_OR_RAISE(auto transformed, func->Transform(predicate->literal()));
    switch (predicate->op()) {
      case Expression::Operation::kLt:
      case Expression::Operation::kLtEq: {
        return UnboundPredicateImpl<BoundReference>::Make(
            Expression::Operation::kLtEq, std::move(ref), std::move(transformed));
      }
      case Expression::Operation::kGt:
      case Expression::Operation::kGtEq: {
        return UnboundPredicateImpl<BoundReference>::Make(
            Expression::Operation::kGtEq, std::move(ref), std::move(transformed));
      }
      case Expression::Operation::kEq: {
        return UnboundPredicateImpl<BoundReference>::Make(
            Expression::Operation::kEq, std::move(ref), std::move(transformed));
      }
      default:
        return nullptr;
    }
  }

  static Result<std::unique_ptr<UnboundPredicate>> TruncateByteArray(
      std::string_view name, const std::shared_ptr<BoundLiteralPredicate>& predicate,
      const std::shared_ptr<TransformFunction>& func) {
    ICEBERG_ASSIGN_OR_RAISE(auto ref, NamedReference::Make(std::string(name)));
    switch (predicate->op()) {
      case Expression::Operation::kStartsWith: {
        ICEBERG_ASSIGN_OR_RAISE(auto transformed, func->Transform(predicate->literal()));
        return UnboundPredicateImpl<BoundReference>::Make(
            Expression::Operation::kStartsWith, std::move(ref), std::move(transformed));
      }
      default:
        return GenericTransform(std::move(ref), predicate, func);
    }
  }

  template <typename T>
    requires std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t>
  static Result<std::unique_ptr<UnboundPredicate>> TruncateInteger(
      std::string_view name, const std::shared_ptr<BoundLiteralPredicate>& predicate,
      const std::shared_ptr<TransformFunction>& func) {
    const Literal& literal = predicate->literal();
    ICEBERG_ASSIGN_OR_RAISE(auto ref, NamedReference::Make(std::string(name)));

    switch (predicate->op()) {
      case Expression::Operation::kLt: {
        // adjust closed and then transform ltEq
        if constexpr (std::is_same_v<T, int32_t>) {
          ICEBERG_ASSIGN_OR_RAISE(
              auto transformed,
              func->Transform(Literal::Int(std::get<int32_t>(literal.value()) - 1)));
          return UnboundPredicateImpl<BoundReference>::Make(
              Expression::Operation::kLtEq, std::move(ref), std::move(transformed));
        } else {
          ICEBERG_ASSIGN_OR_RAISE(
              auto transformed,
              func->Transform(Literal::Long(std::get<int64_t>(literal.value()) - 1)));
          return UnboundPredicateImpl<BoundReference>::Make(
              Expression::Operation::kLtEq, std::move(ref), std::move(transformed));
        }
      }
      case Expression::Operation::kGt: {
        // adjust closed and then transform gtEq
        if constexpr (std::is_same_v<T, int32_t>) {
          ICEBERG_ASSIGN_OR_RAISE(
              auto transformed,
              func->Transform(Literal::Int(std::get<int32_t>(literal.value()) + 1)));
          return UnboundPredicateImpl<BoundReference>::Make(
              Expression::Operation::kGtEq, std::move(ref), std::move(transformed));
        } else {
          ICEBERG_ASSIGN_OR_RAISE(
              auto transformed,
              func->Transform(Literal::Long(std::get<int64_t>(literal.value()) + 1)));
          return UnboundPredicateImpl<BoundReference>::Make(
              Expression::Operation::kGtEq, std::move(ref), std::move(transformed));
        }
      }
      default:
        return GenericTransform(std::move(ref), predicate, func);
    }
  }

  static Result<std::unique_ptr<UnboundPredicate>> TransformTemporal(
      std::string_view name, const std::shared_ptr<BoundLiteralPredicate>& predicate,
      const std::shared_ptr<TransformFunction>& func) {
    const Literal& literal = predicate->literal();
    ICEBERG_ASSIGN_OR_RAISE(auto ref, NamedReference::Make(std::string(name)));

    switch (func->source_type()->type_id()) {
      case TypeId::kDate: {
        switch (predicate->op()) {
          case Expression::Operation::kLt: {
            ICEBERG_ASSIGN_OR_RAISE(
                auto transformed,
                func->Transform(Literal::Date(std::get<int32_t>(literal.value()) - 1)));
            return UnboundPredicateImpl<BoundReference>::Make(
                Expression::Operation::kLtEq, std::move(ref), std::move(transformed));
          }
          case Expression::Operation::kGt: {
            ICEBERG_ASSIGN_OR_RAISE(
                auto transformed,
                func->Transform(Literal::Date(std::get<int32_t>(literal.value()) + 1)));
            return UnboundPredicateImpl<BoundReference>::Make(
                Expression::Operation::kGtEq, std::move(ref), std::move(transformed));
          }
          default:
            return GenericTransform(std::move(ref), predicate, func);
        }
      }
      case TypeId::kTimestamp: {
        switch (predicate->op()) {
          case Expression::Operation::kLt: {
            ICEBERG_ASSIGN_OR_RAISE(auto transformed,
                                    func->Transform(Literal::Timestamp(
                                        std::get<int64_t>(literal.value()) - 1)));
            return UnboundPredicateImpl<BoundReference>::Make(
                Expression::Operation::kLtEq, std::move(ref), std::move(transformed));
          }
          case Expression::Operation::kGt: {
            ICEBERG_ASSIGN_OR_RAISE(auto transformed,
                                    func->Transform(Literal::Timestamp(
                                        std::get<int64_t>(literal.value()) + 1)));
            return UnboundPredicateImpl<BoundReference>::Make(
                Expression::Operation::kGtEq, std::move(ref), std::move(transformed));
          }
          default:
            return GenericTransform(std::move(ref), predicate, func);
        }
      }
      case TypeId::kTimestampTz: {
        switch (predicate->op()) {
          case Expression::Operation::kLt: {
            ICEBERG_ASSIGN_OR_RAISE(auto transformed,
                                    func->Transform(Literal::TimestampTz(
                                        std::get<int64_t>(literal.value()) - 1)));
            return UnboundPredicateImpl<BoundReference>::Make(
                Expression::Operation::kLtEq, std::move(ref), std::move(transformed));
          }
          case Expression::Operation::kGt: {
            ICEBERG_ASSIGN_OR_RAISE(auto transformed,
                                    func->Transform(Literal::TimestampTz(
                                        std::get<int64_t>(literal.value()) + 1)));
            return UnboundPredicateImpl<BoundReference>::Make(
                Expression::Operation::kGtEq, std::move(ref), std::move(transformed));
          }
          default:
            return GenericTransform(std::move(ref), predicate, func);
        }
      }
      default:
        return NotSupported("{} is not a valid input type for temporal transform",
                            func->source_type()->ToString());
    }
  }

  static Result<std::unique_ptr<UnboundPredicate>> TruncateDecimal(
      std::string_view name, const std::shared_ptr<BoundLiteralPredicate>& predicate,
      const std::shared_ptr<TransformFunction>& func) {
    const Literal& boundary = predicate->literal();
    ICEBERG_ASSIGN_OR_RAISE(auto ref, NamedReference::Make(std::string(name)));

    // For boundary adjustments, extract type info once
    auto make_adjusted_literal = [&boundary](int adjustment) {
      const auto& type = internal::checked_pointer_cast<DecimalType>(boundary.type());
      Decimal adjusted = std::get<Decimal>(boundary.value()) + Decimal(adjustment);
      return Literal::Decimal(adjusted.value(), type->precision(), type->scale());
    };

    switch (predicate->op()) {
      case Expression::Operation::kLt: {
        // adjust closed and then transform ltEq
        ICEBERG_ASSIGN_OR_RAISE(auto transformed,
                                func->Transform(make_adjusted_literal(-1)));
        return UnboundPredicateImpl<BoundReference>::Make(
            Expression::Operation::kLtEq, std::move(ref), std::move(transformed));
      }
      case Expression::Operation::kGt: {
        // adjust closed and then transform gtEq
        ICEBERG_ASSIGN_OR_RAISE(auto transformed,
                                func->Transform(make_adjusted_literal(1)));
        return UnboundPredicateImpl<BoundReference>::Make(
            Expression::Operation::kGtEq, std::move(ref), std::move(transformed));
      }
      default:
        return GenericTransform(std::move(ref), predicate, func);
    }
  }

  static Result<std::unique_ptr<UnboundPredicate>> TruncateStringLiteral(
      std::string_view name, const std::shared_ptr<BoundLiteralPredicate>& predicate,
      const std::shared_ptr<TransformFunction>& func) {
    const auto op = predicate->op();
    if (op != Expression::Operation::kStartsWith &&
        op != Expression::Operation::kNotStartsWith) {
      return TruncateByteArray(name, predicate, func);
    }

    const auto& truncate_transform =
        internal::checked_pointer_cast<TruncateTransform>(func);
    const auto& str_value = std::get<std::string>(predicate->literal().value());
    const auto width = truncate_transform->width();
    ICEBERG_ASSIGN_OR_RAISE(auto ref, NamedReference::Make(std::string(name)));

    if (StringUtils::CodePointCount(str_value) < width) {
      return UnboundPredicateImpl<BoundReference>::Make(op, std::move(ref),
                                                        predicate->literal());
    }

    if (StringUtils::CodePointCount(str_value) == width) {
      if (op == Expression::Operation::kStartsWith) {
        return UnboundPredicateImpl<BoundReference>::Make(
            Expression::Operation::kEq, std::move(ref), predicate->literal());
      } else {
        return UnboundPredicateImpl<BoundReference>::Make(
            Expression::Operation::kNotEq, std::move(ref), predicate->literal());
      }
    }

    if (op == Expression::Operation::kStartsWith) {
      ICEBERG_ASSIGN_OR_RAISE(auto transformed, func->Transform(predicate->literal()));
      return UnboundPredicateImpl<BoundReference>::Make(
          Expression::Operation::kStartsWith, std::move(ref), std::move(transformed));
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
        auto value = std::get<int32_t>(literal.value());
        if (value < 0) {
          return UnboundPredicateImpl<BoundReference>::Make(Expression::Operation::kLt,
                                                            std::move(projected->term()),
                                                            Literal::Int(value + 1));
        }

        return std::move(projected);
      }

      case Expression::Operation::kLtEq: {
        ICEBERG_DCHECK(!projected->literals().empty(), "Expected at least one literal");
        const auto& literal = projected->literals().front();
        ICEBERG_DCHECK(std::holds_alternative<int32_t>(literal.value()),
                       "Expected int32_t");
        auto value = std::get<int32_t>(literal.value());
        if (value < 0) {
          return UnboundPredicateImpl<BoundReference>::Make(Expression::Operation::kLtEq,
                                                            std::move(projected->term()),
                                                            Literal::Int(value + 1));
        }
        return std::move(projected);
      }

      case Expression::Operation::kGt:
      case Expression::Operation::kGtEq:
        // incorrect projected values are already greater than the bound for GT, GT_EQ
        return std::move(projected);

      case Expression::Operation::kEq: {
        ICEBERG_DCHECK(!projected->literals().empty(), "Expected at least one literal");
        const auto& literal = projected->literals().front();
        ICEBERG_DCHECK(std::holds_alternative<int32_t>(literal.value()),
                       "Expected int32_t");
        auto value = std::get<int32_t>(literal.value());
        if (value < 0) {
          // match either the incorrect value (projectedValue + 1) or the correct value
          // (projectedValue)
          return UnboundPredicateImpl<BoundReference>::Make(
              Expression::Operation::kIn, std::move(projected->term()),
              {literal, Literal::Int(value + 1)});
        }
        return std::move(projected);
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
        return std::move(projected);
      }

      case Expression::Operation::kNotIn:
      case Expression::Operation::kNotEq:
        // there is no inclusive projection for NOT_EQ and NOT_IN
        return nullptr;

      default:
        return std::move(projected);
    }
  }

 public:
  static Result<std::unique_ptr<UnboundPredicate>> IdentityProject(
      std::string_view name, const std::shared_ptr<BoundPredicate>& predicate) {
    ICEBERG_ASSIGN_OR_RAISE(auto ref, NamedReference::Make(std::string(name)));
    switch (predicate->kind()) {
      case BoundPredicate::Kind::kUnary: {
        return UnboundPredicateImpl<BoundReference>::Make(predicate->op(),
                                                          std::move(ref));
      }
      case BoundPredicate::Kind::kLiteral: {
        const auto& literalPredicate =
            internal::checked_pointer_cast<BoundLiteralPredicate>(predicate);
        return UnboundPredicateImpl<BoundReference>::Make(predicate->op(), std::move(ref),
                                                          literalPredicate->literal());
      }
      case BoundPredicate::Kind::kSet: {
        const auto& setPredicate =
            internal::checked_pointer_cast<BoundSetPredicate>(predicate);
        return UnboundPredicateImpl<BoundReference>::Make(
            predicate->op(), std::move(ref),
            std::vector<Literal>(setPredicate->literal_set().begin(),
                                 setPredicate->literal_set().end()));
      }
    }
    std::unreachable();
  }

  static Result<std::unique_ptr<UnboundPredicate>> BucketProject(
      std::string_view name, const std::shared_ptr<BoundPredicate>& predicate,
      const std::shared_ptr<TransformFunction>& func) {
    ICEBERG_ASSIGN_OR_RAISE(auto ref, NamedReference::Make(std::string(name)));
    switch (predicate->kind()) {
      case BoundPredicate::Kind::kUnary: {
        return UnboundPredicateImpl<BoundReference>::Make(predicate->op(),
                                                          std::move(ref));
      }
      case BoundPredicate::Kind::kLiteral: {
        if (predicate->op() == Expression::Operation::kEq) {
          const auto& literalPredicate =
              internal::checked_pointer_cast<BoundLiteralPredicate>(predicate);
          ICEBERG_ASSIGN_OR_RAISE(auto transformed,
                                  func->Transform(literalPredicate->literal()));
          return UnboundPredicateImpl<BoundReference>::Make(
              predicate->op(), std::move(ref), std::move(transformed));
        }
        break;
      }
      case BoundPredicate::Kind::kSet: {
        // notIn can't be projected
        if (predicate->op() == Expression::Operation::kIn) {
          const auto& setPredicate =
              internal::checked_pointer_cast<BoundSetPredicate>(predicate);
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
      std::string_view name, const std::shared_ptr<BoundPredicate>& predicate,
      const std::shared_ptr<TransformFunction>& func) {
    ICEBERG_ASSIGN_OR_RAISE(auto ref, NamedReference::Make(std::string(name)));
    // Handle unary predicates uniformly for all types
    if (predicate->kind() == BoundPredicate::Kind::kUnary) {
      return UnboundPredicateImpl<BoundReference>::Make(predicate->op(), std::move(ref));
    }

    // Handle set predicates (kIn) uniformly for all types
    if (predicate->kind() == BoundPredicate::Kind::kSet) {
      if (predicate->op() == Expression::Operation::kIn) {
        const auto& setPredicate =
            internal::checked_pointer_cast<BoundSetPredicate>(predicate);
        return TransformSet(name, setPredicate, func);
      }
      return nullptr;
    }

    // Handle literal predicates based on source type
    const auto& literalPredicate =
        internal::checked_pointer_cast<BoundLiteralPredicate>(predicate);

    switch (func->source_type()->type_id()) {
      case TypeId::kInt:
        return TruncateInteger<int32_t>(name, literalPredicate, func);
      case TypeId::kLong:
        return TruncateInteger<int64_t>(name, literalPredicate, func);
      case TypeId::kDecimal:
        return TruncateDecimal(name, literalPredicate, func);
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
      std::string_view name, const std::shared_ptr<BoundPredicate>& predicate,
      const std::shared_ptr<TransformFunction>& func) {
    ICEBERG_ASSIGN_OR_RAISE(auto ref, NamedReference::Make(std::string(name)));
    if (predicate->kind() == BoundPredicate::Kind::kUnary) {
      return UnboundPredicateImpl<BoundReference>::Make(predicate->op(), std::move(ref));
    } else if (predicate->kind() == BoundPredicate::Kind::kLiteral) {
      const auto& literalPredicate =
          internal::checked_pointer_cast<BoundLiteralPredicate>(predicate);
      ICEBERG_ASSIGN_OR_RAISE(auto projected,
                              TransformTemporal(name, literalPredicate, func));
      if (func->transform_type() != TransformType::kDay ||
          func->source_type()->type_id() != TypeId::kDate) {
        return FixInclusiveTimeProjection(
            internal::checked_pointer_cast<UnboundPredicateImpl<BoundReference>>(
                std::move(projected)));
      }
      return projected;
    } else if (predicate->kind() == BoundPredicate::Kind::kSet &&
               predicate->op() == Expression::Operation::kIn) {
      const auto& setPredicate =
          internal::checked_pointer_cast<BoundSetPredicate>(predicate);
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
      std::string_view name, const std::shared_ptr<BoundPredicate>& predicate) {
    ICEBERG_ASSIGN_OR_RAISE(auto ref, NamedReference::Make(std::string(name)));
    switch (predicate->kind()) {
      case BoundPredicate::Kind::kUnary: {
        return UnboundPredicateImpl<BoundReference>::Make(predicate->op(),
                                                          std::move(ref));
      }
      case BoundPredicate::Kind::kLiteral: {
        const auto& literalPredicate =
            internal::checked_pointer_cast<BoundLiteralPredicate>(predicate);
        return UnboundPredicateImpl<BoundReference>::Make(predicate->op(), std::move(ref),
                                                          literalPredicate->literal());
      }
      case BoundPredicate::Kind::kSet: {
        const auto& setPredicate =
            internal::checked_pointer_cast<BoundSetPredicate>(predicate);
        return UnboundPredicateImpl<BoundReference>::Make(
            predicate->op(), std::move(ref),
            std::vector<Literal>(setPredicate->literal_set().begin(),
                                 setPredicate->literal_set().end()));
      }
    }
    std::unreachable();
  }
};

}  // namespace iceberg
