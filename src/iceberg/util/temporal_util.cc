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
#include <cstdint>
#include <limits>
#include <utility>

#include "iceberg/expression/literal.h"
#include "iceberg/util/int128.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/math_util_internal.h"
#include "iceberg/util/string_util.h"

namespace iceberg {

namespace {

using namespace std::chrono;  // NOLINT

/// Parse a timezone offset of the form "+HH:mm" or "-HH:mm" and return the
/// offset in microseconds (positive for east of UTC, negative for west).
Result<int64_t> ParseTimezoneOffset(std::string_view offset) {
  if (offset.size() != 6 || (offset[0] != '+' && offset[0] != '-') || offset[3] != ':') {
    return InvalidArgument("Invalid timezone offset: '{}'", offset);
  }
  bool negative = offset[0] == '-';
  ICEBERG_ASSIGN_OR_RAISE(auto hours,
                          StringUtils::ParseNumber<int64_t>(offset.substr(1, 2)));
  ICEBERG_ASSIGN_OR_RAISE(auto minutes,
                          StringUtils::ParseNumber<int64_t>(offset.substr(4, 2)));
  if (hours > 18 || minutes > 59) [[unlikely]] {
    return InvalidArgument("Invalid timezone offset: '{}'", offset);
  }

  if (hours == 18 && minutes != 0) [[unlikely]] {
    return InvalidArgument("Timezone offset '{}' not in range [-18:00, +18:00]", offset);
  }

  auto micros = hours * internal::kSecondsPerHour * internal::kMicrosPerSecond +
                minutes * internal::kSecondsPerMinute * internal::kMicrosPerSecond;
  return negative ? -micros : micros;
}

Result<std::pair<std::string_view, int64_t>> ParseTimestampWithZoneSuffix(
    std::string_view str) {
  if (str.empty()) [[unlikely]] {
    return InvalidArgument("Invalid timestamptz string: '{}'", str);
  }

  int64_t offset_micros = 0;
  std::string_view timestamp_part;

  if (str.back() == 'Z') {
    timestamp_part = str.substr(0, str.size() - 1);
  } else if (str.size() >= 6 &&
             (str[str.size() - 6] == '+' || str[str.size() - 6] == '-')) {
    ICEBERG_ASSIGN_OR_RAISE(offset_micros,
                            ParseTimezoneOffset(str.substr(str.size() - 6)));
    timestamp_part = str.substr(0, str.size() - 6);
  } else {
    return InvalidArgument("Invalid timestamptz string (missing timezone suffix): '{}'",
                           str);
  }

  return std::make_pair(timestamp_part, offset_micros);
}

Result<int64_t> TimestampFromDayTime(int32_t days, int64_t time_units,
                                     int64_t units_per_day, int64_t offset_micros,
                                     int64_t units_per_micro) {
  const auto offset_units =
      static_cast<int128_t>(offset_micros) * static_cast<int128_t>(units_per_micro);
  const auto timestamp =
      static_cast<int128_t>(days) * static_cast<int128_t>(units_per_day) +
      static_cast<int128_t>(time_units) - offset_units;

  if (timestamp > std::numeric_limits<int64_t>::max() ||
      timestamp < std::numeric_limits<int64_t>::min()) [[unlikely]] {
    return InvalidArgument("Timestamp value is out of int64 range");
  }

  return static_cast<int64_t>(timestamp);
}

/// Parse fractional seconds (after '.') and return micros.
/// Digits beyond 6 are truncated.
Result<int64_t> ParseFractionalMicros(std::string_view frac) {
  if (frac.empty() || frac.size() > 9) [[unlikely]] {
    return InvalidArgument("Invalid fractional seconds: '{}'", frac);
  }
  if (frac.size() > 6) frac = frac.substr(0, 6);
  ICEBERG_ASSIGN_OR_RAISE(auto val, StringUtils::ParseNumber<int32_t>(frac));
  for (size_t i = frac.size(); i < 6; ++i) {
    val *= 10;
  }
  return static_cast<int64_t>(val);
}

/// Parse fractional seconds (after '.') and return nanos.
Result<int64_t> ParseFractionalNanos(std::string_view frac) {
  if (frac.empty() || frac.size() > 9) [[unlikely]] {
    return InvalidArgument("Invalid fractional seconds: '{}'", frac);
  }
  ICEBERG_ASSIGN_OR_RAISE(auto val, StringUtils::ParseNumber<int32_t>(frac));
  for (size_t i = frac.size(); i < 9; ++i) {
    val *= 10;
  }
  return static_cast<int64_t>(val);
}

template <typename TimeScaleParser>
Result<int64_t> ParseTimeWithFraction(std::string_view str, int64_t units_per_second,
                                      TimeScaleParser&& parse_fraction) {
  if (str.size() < 5 || str[2] != ':') [[unlikely]] {
    return InvalidArgument("Invalid time string: '{}'", str);
  }

  ICEBERG_ASSIGN_OR_RAISE(auto hours,
                          StringUtils::ParseNumber<int64_t>(str.substr(0, 2)));
  ICEBERG_ASSIGN_OR_RAISE(auto minutes,
                          StringUtils::ParseNumber<int64_t>(str.substr(3, 2)));
  int64_t seconds = 0;

  int64_t frac_units = 0;
  if (str.size() > 5) {
    if (str[5] != ':' || str.size() < 8) [[unlikely]] {
      return InvalidArgument("Invalid time string: '{}'", str);
    }
    ICEBERG_ASSIGN_OR_RAISE(seconds, StringUtils::ParseNumber<int64_t>(str.substr(6, 2)));
    if (str.size() > 8) {
      if (str[8] != '.') [[unlikely]] {
        return InvalidArgument("Invalid time string: '{}'", str);
      }
      ICEBERG_ASSIGN_OR_RAISE(frac_units, parse_fraction(str.substr(9)));
    }
  }

  if (hours < 0 || hours > 23 || minutes < 0 || minutes > 59 || seconds < 0 ||
      seconds > 59) [[unlikely]] {
    return InvalidArgument("Invalid time string: '{}'", str);
  }

  return hours * internal::kSecondsPerHour * units_per_second +
         minutes * internal::kSecondsPerMinute * units_per_second +
         seconds * units_per_second + frac_units;
}

inline constexpr year_month_day DateToYmd(int32_t days_since_epoch) {
  return {internal::kEpochDays + days{days_since_epoch}};
}

inline constexpr year_month_day TimestampToYmd(int64_t micros_since_epoch) {
  return {floor<days>(sys_time<microseconds>(microseconds{micros_since_epoch}))};
}

inline constexpr year_month_day TimestampNsToYmd(int64_t nanos_since_epoch) {
  return {floor<days>(sys_time<nanoseconds>(nanoseconds{nanos_since_epoch}))};
}

template <typename Duration>
  requires std::is_same_v<Duration, days> || std::is_same_v<Duration, hours>
inline constexpr int32_t TimestampToDuration(int64_t micros_since_epoch) {
  return static_cast<int32_t>(
      floor<Duration>(
          sys_time<microseconds>(microseconds{micros_since_epoch}).time_since_epoch())
          .count());
}

template <typename Duration>
  requires std::is_same_v<Duration, days> || std::is_same_v<Duration, hours>
inline constexpr int32_t TimestampNsToDuration(int64_t nanos_since_epoch) {
  return static_cast<int32_t>(
      floor<Duration>(
          sys_time<nanoseconds>(nanoseconds{nanos_since_epoch}).time_since_epoch())
          .count());
}

inline constexpr int32_t MonthsSinceEpoch(const year_month_day& ymd) {
  auto delta = ymd.year() - internal::kEpochYmd.year();
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
  return Literal::Int((ymd.year() - internal::kEpochYmd.year()).count());
}

template <>
Result<Literal> ExtractYearImpl<TypeId::kTimestamp>(const Literal& literal) {
  auto value = std::get<int64_t>(literal.value());
  auto ymd = TimestampToYmd(value);
  return Literal::Int((ymd.year() - internal::kEpochYmd.year()).count());
}

template <>
Result<Literal> ExtractYearImpl<TypeId::kTimestampNs>(const Literal& literal) {
  auto value = std::get<int64_t>(literal.value());
  auto ymd = TimestampNsToYmd(value);
  return Literal::Int((ymd.year() - internal::kEpochYmd.year()).count());
}

template <>
Result<Literal> ExtractYearImpl<TypeId::kTimestampTz>(const Literal& literal) {
  return ExtractYearImpl<TypeId::kTimestamp>(literal);
}

template <>
Result<Literal> ExtractYearImpl<TypeId::kTimestampTzNs>(const Literal& literal) {
  return ExtractYearImpl<TypeId::kTimestampNs>(literal);
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
Result<Literal> ExtractMonthImpl<TypeId::kTimestampNs>(const Literal& literal) {
  auto value = std::get<int64_t>(literal.value());
  auto ymd = TimestampNsToYmd(value);
  return Literal::Int(MonthsSinceEpoch(ymd));
}

template <>
Result<Literal> ExtractMonthImpl<TypeId::kTimestampTz>(const Literal& literal) {
  return ExtractMonthImpl<TypeId::kTimestamp>(literal);
}

template <>
Result<Literal> ExtractMonthImpl<TypeId::kTimestampTzNs>(const Literal& literal) {
  return ExtractMonthImpl<TypeId::kTimestampNs>(literal);
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
Result<Literal> ExtractDayImpl<TypeId::kTimestampNs>(const Literal& literal) {
  auto value = std::get<int64_t>(literal.value());
  return Literal::Int(TimestampNsToDuration<days>(value));
}

template <>
Result<Literal> ExtractDayImpl<TypeId::kTimestampTz>(const Literal& literal) {
  return ExtractDayImpl<TypeId::kTimestamp>(literal);
}

template <>
Result<Literal> ExtractDayImpl<TypeId::kTimestampTzNs>(const Literal& literal) {
  return ExtractDayImpl<TypeId::kTimestampNs>(literal);
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
Result<Literal> ExtractHourImpl<TypeId::kTimestampNs>(const Literal& literal) {
  auto value = std::get<int64_t>(literal.value());
  return Literal::Int(TimestampNsToDuration<hours>(value));
}

template <>
Result<Literal> ExtractHourImpl<TypeId::kTimestampTz>(const Literal& literal) {
  return ExtractHourImpl<TypeId::kTimestamp>(literal);
}

template <>
Result<Literal> ExtractHourImpl<TypeId::kTimestampTzNs>(const Literal& literal) {
  return ExtractHourImpl<TypeId::kTimestampNs>(literal);
}

}  // namespace

int64_t TemporalUtils::NanosToMicros(int64_t nanos) {
  return FloorDiv(nanos, internal::kNanosPerMicro);
}

Result<int64_t> TemporalUtils::MicrosToNanos(int64_t micros) {
  return MultiplyExact(micros, internal::kNanosPerMicro);
}

Result<int32_t> TemporalUtils::ParseDay(std::string_view str) {
  auto dash1 = str.find('-', (!str.empty() && (str[0] == '-' || str[0] == '+')) ? 1 : 0);
  auto dash2 = str.find('-', dash1 + 1);
  if (str.size() < 10 || dash1 == std::string_view::npos ||
      dash2 == std::string_view::npos) [[unlikely]] {
    return InvalidArgument("Invalid date string: '{}'", str);
  }
  auto year_str = str.substr(0, dash1);
  if (!year_str.empty() && year_str[0] == '+') {
    year_str = year_str.substr(1);
  }
  ICEBERG_ASSIGN_OR_RAISE(auto year_value, StringUtils::ParseNumber<int32_t>(year_str));
  ICEBERG_ASSIGN_OR_RAISE(auto month_value, StringUtils::ParseNumber<int32_t>(str.substr(
                                                dash1 + 1, dash2 - dash1 - 1)));
  ICEBERG_ASSIGN_OR_RAISE(auto day_value,
                          StringUtils::ParseNumber<int32_t>(str.substr(dash2 + 1)));

  auto ymd = std::chrono::year{year_value} /
             std::chrono::month{static_cast<unsigned>(month_value)} /
             std::chrono::day{static_cast<unsigned>(day_value)};
  if (!ymd.ok()) [[unlikely]] {
    return InvalidArgument("Invalid date: '{}'", str);
  }

  auto days_since_epoch = std::chrono::sys_days{ymd} - internal::kEpochDays;
  return static_cast<int32_t>(days_since_epoch.count());
}

Result<int64_t> TemporalUtils::ParseTime(std::string_view str) {
  return ParseTimeWithFraction(str, internal::kMicrosPerSecond, ParseFractionalMicros);
}

Result<int64_t> TemporalUtils::ParseTimeNs(std::string_view str) {
  return ParseTimeWithFraction(str, internal::kNanosPerSecond, ParseFractionalNanos);
}

Result<int64_t> TemporalUtils::ParseTimestamp(std::string_view str) {
  auto t_pos = str.find('T');
  if (t_pos == std::string_view::npos) [[unlikely]] {
    return InvalidArgument("Invalid timestamp string (missing 'T'): '{}'", str);
  }

  ICEBERG_ASSIGN_OR_RAISE(auto days_since_epoch, ParseDay(str.substr(0, t_pos)));
  ICEBERG_ASSIGN_OR_RAISE(auto time_micros, ParseTime(str.substr(t_pos + 1)));

  return TimestampFromDayTime(days_since_epoch, time_micros, internal::kMicrosPerDay,
                              /*offset_micros=*/0, /*units_per_micro=*/1);
}

Result<int64_t> TemporalUtils::ParseTimestampNs(std::string_view str) {
  auto t_pos = str.find('T');
  if (t_pos == std::string_view::npos) [[unlikely]] {
    return InvalidArgument("Invalid timestamp string (missing 'T'): '{}'", str);
  }

  ICEBERG_ASSIGN_OR_RAISE(auto days_since_epoch, ParseDay(str.substr(0, t_pos)));
  ICEBERG_ASSIGN_OR_RAISE(auto time_nanos, ParseTimeNs(str.substr(t_pos + 1)));

  return TimestampFromDayTime(days_since_epoch, time_nanos, internal::kNanosPerDay,
                              /*offset_micros=*/0,
                              /*units_per_micro=*/internal::kNanosPerMicro);
}

Result<int64_t> TemporalUtils::ParseTimestampWithZone(std::string_view str) {
  ICEBERG_ASSIGN_OR_RAISE(auto timestamp_with_offset, ParseTimestampWithZoneSuffix(str));
  const auto [timestamp_part, offset_micros] = timestamp_with_offset;

  auto t_pos = timestamp_part.find('T');
  if (t_pos == std::string_view::npos) [[unlikely]] {
    return InvalidArgument("Invalid timestamp string (missing 'T'): '{}'",
                           timestamp_part);
  }

  ICEBERG_ASSIGN_OR_RAISE(auto days_since_epoch,
                          ParseDay(timestamp_part.substr(0, t_pos)));
  ICEBERG_ASSIGN_OR_RAISE(auto time_micros, ParseTime(timestamp_part.substr(t_pos + 1)));

  return TimestampFromDayTime(days_since_epoch, time_micros, internal::kMicrosPerDay,
                              offset_micros,
                              /*units_per_micro=*/1);
}

Result<int64_t> TemporalUtils::ParseTimestampNsWithZone(std::string_view str) {
  ICEBERG_ASSIGN_OR_RAISE(auto timestamp_with_offset, ParseTimestampWithZoneSuffix(str));
  const auto [timestamp_part, offset_micros] = timestamp_with_offset;

  auto t_pos = timestamp_part.find('T');
  if (t_pos == std::string_view::npos) [[unlikely]] {
    return InvalidArgument("Invalid timestamp string (missing 'T'): '{}'",
                           timestamp_part);
  }

  ICEBERG_ASSIGN_OR_RAISE(auto days_since_epoch,
                          ParseDay(timestamp_part.substr(0, t_pos)));
  ICEBERG_ASSIGN_OR_RAISE(auto time_nanos, ParseTimeNs(timestamp_part.substr(t_pos + 1)));

  return TimestampFromDayTime(days_since_epoch, time_nanos, internal::kNanosPerDay,
                              offset_micros,
                              /*units_per_micro=*/internal::kNanosPerMicro);
}

Result<bool> TemporalUtils::IsUtcOffset(std::string_view str) {
  ICEBERG_ASSIGN_OR_RAISE(auto timestamp_with_offset, ParseTimestampWithZoneSuffix(str));
  return timestamp_with_offset.second == 0;
}

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
    DISPATCH_EXTRACT_YEAR(TypeId::kTimestampNs)
    DISPATCH_EXTRACT_YEAR(TypeId::kTimestampTzNs)
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
    DISPATCH_EXTRACT_MONTH(TypeId::kTimestampNs)
    DISPATCH_EXTRACT_MONTH(TypeId::kTimestampTzNs)
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
    DISPATCH_EXTRACT_DAY(TypeId::kTimestampNs)
    DISPATCH_EXTRACT_DAY(TypeId::kTimestampTzNs)
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
    DISPATCH_EXTRACT_HOUR(TypeId::kTimestampNs)
    DISPATCH_EXTRACT_HOUR(TypeId::kTimestampTzNs)
    default:
      return NotSupported("Extract hour from type {} is not supported",
                          literal.type()->ToString());
  }
}

}  // namespace iceberg
