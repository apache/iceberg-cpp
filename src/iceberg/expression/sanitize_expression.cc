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

#include "iceberg/expression/sanitize_expression.h"

#include <chrono>
#include <cmath>
#include <cstdint>
#include <format>
#include <limits>
#include <regex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "iceberg/expression/binder.h"
#include "iceberg/expression/literal.h"
#include "iceberg/expression/predicate.h"
#include "iceberg/expression/term.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"
#include "iceberg/util/bucket_util.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/temporal_util.h"

namespace iceberg {

namespace {

std::string SanitizeDate(int32_t days, int32_t today) {
  std::string is_past = today > days ? "ago" : "from-now";
  int32_t diff = std::abs(today - days);
  if (diff == 0) {
    return "(date-today)";
  } else if (diff < 90) {
    return "(date-" + std::to_string(diff) + "-days-" + is_past + ")";
  }

  return "(date)";
}

std::string SanitizeTimestamp(int64_t micros, int64_t now) {
  constexpr int64_t kMicrosPerHour = 60LL * 60LL * 1'000'000LL;
  constexpr int64_t kFiveMinutesInMicros = 5LL * 60LL * 1'000'000LL;
  constexpr int64_t kThreeDaysInHours = 3LL * 24LL;
  constexpr int64_t kNinetyDaysInHours = 90LL * 24LL;

  std::string is_past = now > micros ? "ago" : "from-now";
  int64_t diff = std::abs(now - micros);
  if (diff < kFiveMinutesInMicros) {
    return "(timestamp-about-now)";
  }

  int64_t hours = diff / kMicrosPerHour;
  if (hours <= kThreeDaysInHours) {
    return "(timestamp-" + std::to_string(hours) + "-hours-" + is_past + ")";
  } else if (hours < kNinetyDaysInHours) {
    int64_t days = hours / 24;
    return "(timestamp-" + std::to_string(days) + "-days-" + is_past + ")";
  }

  return "(timestamp)";
}

std::string SanitizeNumber(double value, std::string_view type) {
  int32_t num_digits =
      value == 0 ? 1 : static_cast<int32_t>(std::log10(std::abs(value))) + 1;
  return std::format("({}-digit-{})", num_digits, type);
}

Result<std::string> SanitizeSimpleString(std::string_view value) {
  ICEBERG_ASSIGN_OR_RAISE(auto hash,
                          BucketUtils::BucketIndex(Literal::String(std::string(value)),
                                                   std::numeric_limits<int32_t>::max()));
  return std::format("(hash-{:08x})", hash);
}

Result<std::string> SanitizeString(std::string_view value, int64_t now, int32_t today) {
  static const std::regex kDate(R"(\d{4}-\d{2}-\d{2})");
  static const std::regex kTime(R"(\d{2}:\d{2}(:\d{2}(.\d{1,9})?)?)");
  static const std::regex kTimestamp(
      R"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2}(.\d{1,9})?)?)");
  static const std::regex kTimestampTz(
      R"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2}(.\d{1,9})?)?([-+]\d{2}:\d{2}|Z))");
  static const std::regex kTimestampNs(
      R"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2}(.\d{7,9})?)?)");
  static const std::regex kTimestampTzNs(
      R"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2}(.\d{7,9})?)?([-+]\d{2}:\d{2}|Z))");

  try {
    if (std::regex_match(value.begin(), value.end(), kDate)) {
      auto days = TemporalUtils::ParseDay(value);
      return days.has_value() ? SanitizeDate(*days, today) : SanitizeSimpleString(value);
    }
    if (std::regex_match(value.begin(), value.end(), kTimestampNs)) {
      auto nanos = TemporalUtils::ParseTimestampNs(value);
      return nanos.has_value()
                 ? SanitizeTimestamp(TemporalUtils::NanosToMicros(*nanos), now)
                 : SanitizeSimpleString(value);
    }
    if (std::regex_match(value.begin(), value.end(), kTimestampTzNs)) {
      auto nanos = TemporalUtils::ParseTimestampNsWithZone(value);
      return nanos.has_value()
                 ? SanitizeTimestamp(TemporalUtils::NanosToMicros(*nanos), now)
                 : SanitizeSimpleString(value);
    }
    if (std::regex_match(value.begin(), value.end(), kTimestamp)) {
      auto micros = TemporalUtils::ParseTimestamp(value);
      return micros.has_value() ? SanitizeTimestamp(*micros, now)
                                : SanitizeSimpleString(value);
    }
    if (std::regex_match(value.begin(), value.end(), kTimestampTz)) {
      auto micros = TemporalUtils::ParseTimestampWithZone(value);
      return micros.has_value() ? SanitizeTimestamp(*micros, now)
                                : SanitizeSimpleString(value);
    }
    if (std::regex_match(value.begin(), value.end(), kTime)) {
      return std::string("(time)");
    }
    return SanitizeSimpleString(value);
  } catch (const std::exception&) {
    // Don't throw when parsing failed in sanitizeString default to simple string
    // sanitization.
    return SanitizeSimpleString(value);
  }
}

Result<std::string> SanitizePlaceholder(const Literal& literal, int64_t now,
                                        int32_t today) {
  if (literal.IsNull()) {
    return std::string("(null)");
  }
  const auto& value = literal.value();
  switch (literal.type()->type_id()) {
    case TypeId::kString:
      return SanitizeString(std::get<std::string>(value), now, today);
    case TypeId::kDate:
      return SanitizeDate(std::get<int32_t>(value), today);
    case TypeId::kTimestamp:
    case TypeId::kTimestampTz:
      return SanitizeTimestamp(std::get<int64_t>(value), now);
    case TypeId::kTimestampNs:
    case TypeId::kTimestampTzNs:
      return SanitizeTimestamp(TemporalUtils::NanosToMicros(std::get<int64_t>(value)),
                               now);
    case TypeId::kTime:
      return std::string("(time)");
    case TypeId::kInt:
      return SanitizeNumber(std::get<int32_t>(value), "int");
    case TypeId::kLong:
      return SanitizeNumber(static_cast<double>(std::get<int64_t>(value)), "int");
    case TypeId::kFloat:
      return SanitizeNumber(std::get<float>(value), "float");
    case TypeId::kDouble:
      return SanitizeNumber(std::get<double>(value), "float");
    default:
      return SanitizeSimpleString(literal.ToString());
  }
}

Result<Literal> SanitizeLiteral(const Literal& literal, int64_t now, int32_t today) {
  ICEBERG_ASSIGN_OR_RAISE(auto placeholder, SanitizePlaceholder(literal, now, today));
  return Literal::String(std::move(placeholder));
}

// Mirrors Java's ExpressionUtil.unbind(BoundTerm): a transform term (bucket/day/etc.)
// is rebuilt as a transform term over a fresh reference, instead of being collapsed to
// a plain column reference.
Result<std::shared_ptr<UnboundTerm<BoundTransform>>> MakeSanitizedTransformTerm(
    std::string_view name, const std::shared_ptr<Transform>& transform) {
  ICEBERG_ASSIGN_OR_RAISE(auto named_ref, NamedReference::Make(std::string(name)));
  std::shared_ptr<NamedReference> shared_ref = std::move(named_ref);
  ICEBERG_ASSIGN_OR_RAISE(auto unbound_transform,
                          UnboundTransform::Make(std::move(shared_ref), transform));
  return std::shared_ptr<UnboundTerm<BoundTransform>>(std::move(unbound_transform));
}

template <typename B>
Result<std::shared_ptr<Expression>> MakeSanitizedPredicateOverTerm(
    Expression::Operation op, std::shared_ptr<UnboundTerm<B>> term,
    std::vector<Literal> values) {
  if (values.empty()) {
    ICEBERG_ASSIGN_OR_RAISE(auto pred, UnboundPredicateImpl<B>::Make(op, term));
    return std::shared_ptr<Expression>(std::move(pred));
  }
  if (values.size() == 1) {
    ICEBERG_ASSIGN_OR_RAISE(
        auto pred, UnboundPredicateImpl<B>::Make(op, term, std::move(values[0])));
    return std::shared_ptr<Expression>(std::move(pred));
  }
  ICEBERG_ASSIGN_OR_RAISE(auto pred,
                          UnboundPredicateImpl<B>::Make(op, term, std::move(values)));
  return std::shared_ptr<Expression>(std::move(pred));
}

// Rebuilds a sanitized predicate over a bound `term`, preserving whether it was a plain
// column reference or a transform -- only the literal values are replaced with
// placeholders.
Result<std::shared_ptr<Expression>> MakeSanitizedPredicate(
    Expression::Operation op, const std::shared_ptr<BoundTerm>& term,
    std::vector<Literal> values) {
  if (term->kind() == Term::Kind::kTransform) {
    const auto& bound_transform = internal::checked_cast<const BoundTransform&>(*term);
    ICEBERG_ASSIGN_OR_RAISE(auto transform_term,
                            MakeSanitizedTransformTerm(term->reference()->name(),
                                                       bound_transform.transform()));
    return MakeSanitizedPredicateOverTerm<BoundTransform>(op, std::move(transform_term),
                                                          std::move(values));
  }
  ICEBERG_ASSIGN_OR_RAISE(auto named_ref,
                          NamedReference::Make(std::string(term->reference()->name())));
  std::shared_ptr<UnboundTerm<BoundReference>> ref_term = std::move(named_ref);
  return MakeSanitizedPredicateOverTerm<BoundReference>(op, std::move(ref_term),
                                                        std::move(values));
}

// Rebuilds a sanitized predicate over an unbound `term`.
Result<std::shared_ptr<Expression>> MakeSanitizedPredicate(Expression::Operation op,
                                                           const Term& term,
                                                           std::vector<Literal> values) {
  if (term.kind() == Term::Kind::kTransform) {
    const auto& unbound_transform = internal::checked_cast<const UnboundTransform&>(term);
    // reference() is non-const on Unbound<B> but never mutates state; same pattern as
    // json_serde.cc's ToJson(const UnboundTransform&).
    auto& mut = const_cast<UnboundTransform&>(unbound_transform);
    ICEBERG_ASSIGN_OR_RAISE(auto transform_term,
                            MakeSanitizedTransformTerm(mut.reference()->name(),
                                                       unbound_transform.transform()));
    return MakeSanitizedPredicateOverTerm<BoundTransform>(op, std::move(transform_term),
                                                          std::move(values));
  }
  const auto& named_reference = internal::checked_cast<const NamedReference&>(term);
  ICEBERG_ASSIGN_OR_RAISE(auto named_ref,
                          NamedReference::Make(std::string(named_reference.name())));
  std::shared_ptr<UnboundTerm<BoundReference>> ref_term = std::move(named_ref);
  return MakeSanitizedPredicateOverTerm<BoundReference>(op, std::move(ref_term),
                                                        std::move(values));
}

}  // namespace

SanitizeExpression::SanitizeExpression() {
  auto now = std::chrono::system_clock::now();
  now_ = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch())
             .count();
  today_ = static_cast<int32_t>(
      std::chrono::duration_cast<std::chrono::days>(now.time_since_epoch()).count());
}

Result<std::shared_ptr<Expression>> SanitizeExpression::Sanitize(
    const std::shared_ptr<Expression>& expr) {
  ICEBERG_DCHECK(expr != nullptr, "Expression cannot be null");
  SanitizeExpression visitor;
  return iceberg::Visit<std::shared_ptr<Expression>, SanitizeExpression>(expr, visitor);
}

Result<std::shared_ptr<Expression>> SanitizeExpression::AlwaysTrue() {
  return True::Instance();
}

Result<std::shared_ptr<Expression>> SanitizeExpression::AlwaysFalse() {
  return False::Instance();
}

Result<std::shared_ptr<Expression>> SanitizeExpression::Not(
    const std::shared_ptr<Expression>& child_result) {
  return iceberg::Not::MakeFolded(child_result);
}

Result<std::shared_ptr<Expression>> SanitizeExpression::And(
    const std::shared_ptr<Expression>& left_result,
    const std::shared_ptr<Expression>& right_result) {
  return iceberg::And::MakeFolded(left_result, right_result);
}

Result<std::shared_ptr<Expression>> SanitizeExpression::Or(
    const std::shared_ptr<Expression>& left_result,
    const std::shared_ptr<Expression>& right_result) {
  return iceberg::Or::MakeFolded(left_result, right_result);
}

Result<std::shared_ptr<Expression>> SanitizeExpression::Predicate(
    const std::shared_ptr<BoundPredicate>& pred) {
  switch (pred->kind()) {
    case BoundPredicate::Kind::kUnary:
      return MakeSanitizedPredicate(pred->op(), pred->term(), {});
    case BoundPredicate::Kind::kLiteral: {
      const auto& literal_pred =
          internal::checked_cast<const BoundLiteralPredicate&>(*pred);
      ICEBERG_ASSIGN_OR_RAISE(auto placeholder,
                              SanitizeLiteral(literal_pred.literal(), now_, today_));
      return MakeSanitizedPredicate(pred->op(), pred->term(), {std::move(placeholder)});
    }
    case BoundPredicate::Kind::kSet: {
      const auto& set_pred = internal::checked_cast<const BoundSetPredicate&>(*pred);
      std::vector<Literal> placeholders;
      placeholders.reserve(set_pred.literal_set().size());
      for (const auto& literal : set_pred.literal_set()) {
        ICEBERG_ASSIGN_OR_RAISE(auto placeholder, SanitizeLiteral(literal, now_, today_));
        placeholders.push_back(std::move(placeholder));
      }
      return MakeSanitizedPredicate(pred->op(), pred->term(), std::move(placeholders));
    }
  }
  return InvalidExpression("Unsupported bound predicate kind for sanitization");
}

Result<std::shared_ptr<Expression>> SanitizeExpression::Predicate(
    const std::shared_ptr<UnboundPredicate>& pred) {
  auto literals = pred->literals();
  std::vector<Literal> placeholders;
  placeholders.reserve(literals.size());
  for (const auto& literal : literals) {
    ICEBERG_ASSIGN_OR_RAISE(auto placeholder, SanitizeLiteral(literal, now_, today_));
    placeholders.push_back(std::move(placeholder));
  }
  return MakeSanitizedPredicate(pred->op(), pred->unbound_term(),
                                std::move(placeholders));
}

Result<std::shared_ptr<Expression>> SanitizeExpression::Sanitize(
    const Schema& schema, const std::shared_ptr<Expression>& expr, bool case_sensitive) {
  auto bound = Binder::Bind(schema, expr, case_sensitive);
  if (bound.has_value()) {
    return Sanitize(*bound);
  }
  return Sanitize(expr);
}
// TODO(evindj) : add StringSanitizer for logging.
}  // namespace iceberg
