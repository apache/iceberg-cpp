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

#include "iceberg/util/temporal_util.h"

#include <chrono>
#include <utility>

#include "iceberg/expression/literal.h"

namespace iceberg {

namespace {

using namespace std::chrono;  // NOLINT

constexpr auto kEpochYmd = year{1970} / January / 1;
constexpr auto kEpochDays = sys_days(kEpochYmd);

inline constexpr year_month_day DateToYmd(int32_t days_since_epoch) {
  return {kEpochDays + days{days_since_epoch}};
}

inline constexpr year_month_day TimestampToYmd(int64_t micros_since_epoch) {
  return {floor<days>(sys_time<microseconds>(microseconds{micros_since_epoch}))};
}

template <typename Duration>
  requires std::is_same_v<Duration, days> || std::is_same_v<Duration, hours>
inline constexpr int32_t TimestampToDuration(int64_t micros_since_epoch) {
  return static_cast<int32_t>(
      floor<Duration>(
          sys_time<microseconds>(microseconds{micros_since_epoch}).time_since_epoch())
          .count());
}

inline constexpr int32_t MonthsSinceEpoch(const year_month_day& ymd) {
  auto delta = ymd.year() - kEpochYmd.year();
  // Calculate the month as months from 1970-01
  // Note: January is month 1, so we subtract 1 to get zero-based month count.
  return static_cast<int32_t>(delta.count() * 12 + static_cast<unsigned>(ymd.month()) -
                              1);
}

template <TypeId type_id>
Result<Literal> ExtractYearImpl(const Literal& literal) {
  std::unreachable();
}

template <>
Result<Literal> ExtractYearImpl<TypeId::kDate>(const Literal& literal) {
  auto value = std::get<int32_t>(literal.value());
  auto ymd = DateToYmd(value);
  return Literal::Int(static_cast<int32_t>(ymd.year()));
}

template <>
Result<Literal> ExtractYearImpl<TypeId::kTimestamp>(const Literal& literal) {
  auto value = std::get<int64_t>(literal.value());
  auto ymd = TimestampToYmd(value);
  return Literal::Int(static_cast<int32_t>(ymd.year()));
}

template <>
Result<Literal> ExtractYearImpl<TypeId::kTimestampTz>(const Literal& literal) {
  return ExtractYearImpl<TypeId::kTimestamp>(literal);
}

template <TypeId type_id>
Result<Literal> ExtractMonthImpl(const Literal& literal) {
  std::unreachable();
}

template <>
Result<Literal> ExtractMonthImpl<TypeId::kDate>(const Literal& literal) {
  auto value = std::get<int32_t>(literal.value());
  auto ymd = DateToYmd(value);
  return Literal::Int(MonthsSinceEpoch(ymd));
}

template <>
Result<Literal> ExtractMonthImpl<TypeId::kTimestamp>(const Literal& literal) {
  auto value = std::get<int64_t>(literal.value());
  auto ymd = TimestampToYmd(value);
  return Literal::Int(MonthsSinceEpoch(ymd));
}

template <>
Result<Literal> ExtractMonthImpl<TypeId::kTimestampTz>(const Literal& literal) {
  return ExtractMonthImpl<TypeId::kTimestamp>(literal);
}

template <TypeId type_id>
Result<Literal> ExtractDayImpl(const Literal& literal) {
  std::unreachable();
}

template <>
Result<Literal> ExtractDayImpl<TypeId::kDate>(const Literal& literal) {
  return Literal::Int(std::get<int32_t>(literal.value()));
}

template <>
Result<Literal> ExtractDayImpl<TypeId::kTimestamp>(const Literal& literal) {
  auto value = std::get<int64_t>(literal.value());
  return Literal::Int(TimestampToDuration<days>(value));
}

template <>
Result<Literal> ExtractDayImpl<TypeId::kTimestampTz>(const Literal& literal) {
  return ExtractDayImpl<TypeId::kTimestamp>(literal);
}

template <TypeId type_id>
Result<Literal> ExtractHourImpl(const Literal& literal) {
  std::unreachable();
}

template <>
Result<Literal> ExtractHourImpl<TypeId::kTimestamp>(const Literal& literal) {
  auto value = std::get<int64_t>(literal.value());
  return Literal::Int(TimestampToDuration<hours>(value));
}

template <>
Result<Literal> ExtractHourImpl<TypeId::kTimestampTz>(const Literal& literal) {
  return ExtractHourImpl<TypeId::kTimestamp>(literal);
}

}  // namespace

#define DISPATCH_EXTRACT_YEAR(type_id) \
  case type_id:                        \
    return ExtractYearImpl<type_id>(literal);

Result<Literal> TemporalUtils::ExtractYear(const Literal& literal) {
  if (literal.IsNull()) [[unlikely]] {
    return Literal::Null(int32());
  }

  if (literal.IsAboveMax() || literal.IsBelowMin()) [[unlikely]] {
    return NotSupported("Cannot extract year from {}", literal.ToString());
  }

  switch (literal.type()->type_id()) {
    DISPATCH_EXTRACT_YEAR(TypeId::kDate)
    DISPATCH_EXTRACT_YEAR(TypeId::kTimestamp)
    DISPATCH_EXTRACT_YEAR(TypeId::kTimestampTz)
    default:
      return NotSupported("Extract year from type {} is not supported",
                          literal.type()->ToString());
  }
}

#define DISPATCH_EXTRACT_MONTH(type_id) \
  case type_id:                         \
    return ExtractMonthImpl<type_id>(literal);

Result<Literal> TemporalUtils::ExtractMonth(const Literal& literal) {
  if (literal.IsNull()) [[unlikely]] {
    return Literal::Null(int32());
  }

  if (literal.IsAboveMax() || literal.IsBelowMin()) [[unlikely]] {
    return NotSupported("Cannot extract month from {}", literal.ToString());
  }

  switch (literal.type()->type_id()) {
    DISPATCH_EXTRACT_MONTH(TypeId::kDate)
    DISPATCH_EXTRACT_MONTH(TypeId::kTimestamp)
    DISPATCH_EXTRACT_MONTH(TypeId::kTimestampTz)
    default:
      return NotSupported("Extract month from type {} is not supported",
                          literal.type()->ToString());
  }
}

#define DISPATCH_EXTRACT_DAY(type_id) \
  case type_id:                       \
    return ExtractDayImpl<type_id>(literal);

Result<Literal> TemporalUtils::ExtractDay(const Literal& literal) {
  if (literal.IsNull()) [[unlikely]] {
    return Literal::Null(int32());
  }

  if (literal.IsAboveMax() || literal.IsBelowMin()) [[unlikely]] {
    return NotSupported("Cannot extract day from {}", literal.ToString());
  }

  switch (literal.type()->type_id()) {
    DISPATCH_EXTRACT_DAY(TypeId::kDate)
    DISPATCH_EXTRACT_DAY(TypeId::kTimestamp)
    DISPATCH_EXTRACT_DAY(TypeId::kTimestampTz)
    default:
      return NotSupported("Extract day from type {} is not supported",
                          literal.type()->ToString());
  }
}

#define DISPATCH_EXTRACT_HOUR(type_id) \
  case type_id:                        \
    return ExtractHourImpl<type_id>(literal);

Result<Literal> TemporalUtils::ExtractHour(const Literal& literal) {
  if (literal.IsNull()) [[unlikely]] {
    return Literal::Null(int32());
  }

  if (literal.IsAboveMax() || literal.IsBelowMin()) [[unlikely]] {
    return NotSupported("Cannot extract hour from {}", literal.ToString());
  }

  switch (literal.type()->type_id()) {
    DISPATCH_EXTRACT_HOUR(TypeId::kTimestamp)
    DISPATCH_EXTRACT_HOUR(TypeId::kTimestampTz)
    default:
      return NotSupported("Extract hour from type {} is not supported",
                          literal.type()->ToString());
  }
}

}  // namespace iceberg
