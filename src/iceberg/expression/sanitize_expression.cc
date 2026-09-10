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
  uint32_t diff = today > days ? static_cast<uint32_t>(static_cast<uint32_t>(today) -
                                                       static_cast<uint32_t>(days))
                               : static_cast<uint32_t>(static_cast<uint32_t>(days) -
                                                       static_cast<uint32_t>(today));
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
  uint64_t diff = now > micros ? static_cast<uint64_t>(static_cast<uint64_t>(now) -
                                                       static_cast<uint64_t>(micros))
                               : static_cast<uint64_t>(static_cast<uint64_t>(micros) -
                                                       static_cast<uint64_t>(now));
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
  if (!std::isfinite(value)) {
    return std::format("({})", type);
  }
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
    case TypeId::kBinary:
    case TypeId::kFixed:
      return SanitizeSimpleString(literal.ToString());
    default:
      return SanitizeSimpleString(literal.ToString());
  }
}

Result<Literal> SanitizeLiteral(const Literal& literal, int64_t now, int32_t today) {
  ICEBERG_ASSIGN_OR_RAISE(auto placeholder, SanitizePlaceholder(literal, now, today));
  return Literal::String(std::move(placeholder));
}

Result<std::shared_ptr<UnboundTerm<BoundTransform>>> MakeSanitizedTransformTerm(
    std::string_view name, const std::shared_ptr<Transform>& transform) {
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<NamedReference> named_ref,
                          NamedReference::Make(std::string(name)));
  return UnboundTransform::Make(std::move(named_ref), transform);
}

template <typename B>
Result<std::shared_ptr<Expression>> MakeSanitizedPredicateOverTerm(
    Expression::Operation op, std::shared_ptr<UnboundTerm<B>> term,
    std::vector<Literal> values) {
  if (values.empty()) {
    return UnboundPredicateImpl<B>::Make(op, term);
  }
  if (values.size() == 1) {
    return UnboundPredicateImpl<B>::Make(op, term, std::move(values[0]));
  }
  return UnboundPredicateImpl<B>::Make(op, term, std::move(values));
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
  return MakeSanitizedPredicateOverTerm<BoundReference>(op, std::move(named_ref),
                                                        std::move(values));
}

template <typename B>
Result<std::shared_ptr<Expression>> MakeSanitizedUnboundPredicate(
    const std::shared_ptr<UnboundPredicate>& pred, std::vector<Literal> values) {
  auto typed_pred = std::dynamic_pointer_cast<UnboundPredicateImpl<B>>(pred);
  if (typed_pred == nullptr) [[unlikely]] {
    return InvalidExpression("Unexpected unbound predicate term type");
  }
  return MakeSanitizedPredicateOverTerm<B>(pred->op(), typed_pred->term(),
                                           std::move(values));
}

}  // namespace

SanitizeExpression::SanitizeExpression() {
  auto now = std::chrono::system_clock::now().time_since_epoch();
  auto now_millis = std::chrono::duration_cast<std::chrono::milliseconds>(now);
  now_ = std::chrono::duration_cast<std::chrono::microseconds>(now_millis).count();
  today_ = static_cast<int32_t>(
      std::chrono::duration_cast<std::chrono::days>(now_millis).count());
}

Result<std::shared_ptr<Expression>> SanitizeExpression::Sanitize(
    const std::shared_ptr<Expression>& expr) {
  ICEBERG_DCHECK(expr != nullptr, "Expression cannot be null");
  SanitizeExpression visitor;
  return Visit<std::shared_ptr<Expression>, SanitizeExpression>(expr, visitor);
}

Result<std::shared_ptr<Expression>> SanitizeExpression::AlwaysTrue() {
  return True::Instance();
}

Result<std::shared_ptr<Expression>> SanitizeExpression::AlwaysFalse() {
  return False::Instance();
}

Result<std::shared_ptr<Expression>> SanitizeExpression::Not(
    const std::shared_ptr<Expression>& child_result) {
  return Not::MakeFolded(child_result);
}

Result<std::shared_ptr<Expression>> SanitizeExpression::And(
    const std::shared_ptr<Expression>& left_result,
    const std::shared_ptr<Expression>& right_result) {
  return And::MakeFolded(left_result, right_result);
}

Result<std::shared_ptr<Expression>> SanitizeExpression::Or(
    const std::shared_ptr<Expression>& left_result,
    const std::shared_ptr<Expression>& right_result) {
  return Or::MakeFolded(left_result, right_result);
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
  switch (pred->op()) {
    case Expression::Operation::kIsNull:
    case Expression::Operation::kNotNull:
    case Expression::Operation::kIsNan:
    case Expression::Operation::kNotNan:
      return pred;
    case Expression::Operation::kLt:
    case Expression::Operation::kLtEq:
    case Expression::Operation::kGt:
    case Expression::Operation::kGtEq:
    case Expression::Operation::kEq:
    case Expression::Operation::kNotEq:
    case Expression::Operation::kStartsWith:
    case Expression::Operation::kNotStartsWith:
    case Expression::Operation::kIn:
    case Expression::Operation::kNotIn:
      break;
    default:
      return InvalidExpression(
          "Unsupported unbound predicate operation for sanitization");
  }

  auto literals = pred->literals();
  std::vector<Literal> placeholders;
  placeholders.reserve(literals.size());
  for (const auto& literal : literals) {
    ICEBERG_ASSIGN_OR_RAISE(auto placeholder, SanitizeLiteral(literal, now_, today_));
    placeholders.push_back(std::move(placeholder));
  }
  switch (pred->unbound_term().kind()) {
    case Term::Kind::kReference:
      return MakeSanitizedUnboundPredicate<BoundReference>(pred, std::move(placeholders));
    case Term::Kind::kTransform:
      return MakeSanitizedUnboundPredicate<BoundTransform>(pred, std::move(placeholders));
    case Term::Kind::kExtract:
      return NotSupported("Cannot sanitize an extract predicate");
  }
  std::unreachable();
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
