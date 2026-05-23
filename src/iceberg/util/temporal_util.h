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

#include <chrono>
#include <cstdint>
#include <string_view>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg::internal {

inline constexpr int64_t kNanosPerMicro = 1000;
inline constexpr int64_t kMicrosPerMilli = 1000;
inline constexpr int64_t kMicrosPerSecond = 1000 * kMicrosPerMilli;
inline constexpr int64_t kSecondsPerMinute = 60;
inline constexpr int64_t kMinutesPerHour = 60;
inline constexpr int64_t kHoursPerDay = 24;
inline constexpr int64_t kSecondsPerHour = kMinutesPerHour * kSecondsPerMinute;
inline constexpr int64_t kSecondsPerDay = kHoursPerDay * kSecondsPerHour;
inline constexpr int64_t kMicrosPerDay = kSecondsPerDay * kMicrosPerSecond;
inline constexpr int64_t kNanosPerMilli = kMicrosPerMilli * kNanosPerMicro;
inline constexpr int64_t kNanosPerSecond = kMicrosPerSecond * kNanosPerMicro;
inline constexpr int64_t kNanosPerDay = kMicrosPerDay * kNanosPerMicro;

inline constexpr auto kEpochYmd = std::chrono::year{1970} / std::chrono::January / 1;
inline constexpr auto kEpochDays = std::chrono::sys_days{kEpochYmd};

}  // namespace iceberg::internal

namespace iceberg {

class ICEBERG_EXPORT TemporalUtils {
 public:
  /// \brief Convert nanoseconds since epoch to microseconds using floor division.
  static int64_t NanosToMicros(int64_t nanos);

  /// \brief Convert microseconds since epoch to nanoseconds, failing on overflow.
  static Result<int64_t> MicrosToNanos(int64_t micros);

  /// \brief Parses a date string in "[+-]yyyy-MM-dd" format into days since epoch.
  ///
  /// Supports an optional '+' or '-' prefix for extended years beyond 9999.
  ///
  /// \param str The date string to parse.
  /// \return The number of days since 1970-01-01, or an error.
  static Result<int32_t> ParseDay(std::string_view str);

  /// \brief Parses a time string into microseconds from midnight.
  ///
  /// Accepts ISO-8601 local time formats: "HH:mm", "HH:mm:ss", or
  /// "HH:mm:ss.f" where the fractional part can be 1-9 digits.
  /// Digits beyond 6 (microsecond precision) are truncated.
  ///
  /// \param str The time string to parse.
  /// \return The number of microseconds from midnight, or an error.
  static Result<int64_t> ParseTime(std::string_view str);

  /// \brief Parses a time string into nanoseconds from midnight.
  ///
  /// Accepts ISO-8601 local time formats: "HH:mm", "HH:mm:ss", or
  /// "HH:mm:ss.f" where the fractional part can be 1-9 digits.
  /// Digits beyond 9 (nanosecond precision) are truncated.
  ///
  /// \param str The time string to parse.
  /// \return The number of nanoseconds from midnight, or an error.
  static Result<int64_t> ParseTimeNs(std::string_view str);

  /// \brief Parses a timestamp string into microseconds since epoch.
  ///
  /// Accepts ISO-8601 local date-time formats: "yyyy-MM-ddTHH:mm",
  /// "yyyy-MM-ddTHH:mm:ss", or "yyyy-MM-ddTHH:mm:ss.f" where the
  /// fractional part can be 1-9 digits (truncated to microseconds).
  ///
  /// \param str The timestamp string to parse.
  /// \return The number of microseconds since epoch, or an error.
  static Result<int64_t> ParseTimestamp(std::string_view str);

  /// \brief Parses a timestamp string into nanoseconds since epoch.
  ///
  /// Accepts ISO-8601 local date-time formats: "yyyy-MM-ddTHH:mm",
  /// "yyyy-MM-ddTHH:mm:ss", or "yyyy-MM-ddTHH:mm:ss.f" where the
  /// fractional part can be 1-9 digits.
  ///
  /// \param str The timestamp string to parse.
  /// \return The number of nanoseconds since epoch, or an error.
  static Result<int64_t> ParseTimestampNs(std::string_view str);

  /// \brief Parses a timestamp-with-zone string into microseconds since epoch (UTC).
  ///
  /// Accepts the same formats as ParseTimestamp, with a timezone suffix:
  /// "Z", "+HH:mm", or "-HH:mm". Non-UTC offsets are converted to UTC.
  /// The seconds and fractional parts are optional (e.g. "yyyy-MM-ddTHH:mm+00:00").
  ///
  /// \param str The timestamp string to parse.
  /// \return The number of microseconds since epoch (UTC), or an error.
  static Result<int64_t> ParseTimestampWithZone(std::string_view str);

  /// \brief Parses a timestamp-with-zone string into nanoseconds since epoch (UTC).
  ///
  /// Accepts the same formats as ParseTimestampNs, with a timezone suffix:
  /// "Z", "+HH:mm", or "-HH:mm". Non-UTC offsets are converted to UTC.
  /// The seconds and fractional parts are optional (e.g. "yyyy-MM-ddTHH:mm+00:00").
  ///
  /// \param str The timestamp string to parse.
  /// \return The number of nanoseconds since epoch (UTC), or an error.
  static Result<int64_t> ParseTimestampNsWithZone(std::string_view str);

  /// \brief Extract a date or timestamp year, as years from 1970
  static Result<Literal> ExtractYear(const Literal& literal);

  /// \brief Extract a date or timestamp month, as months from 1970-01-01
  static Result<Literal> ExtractMonth(const Literal& literal);

  /// \brief Extract a date or timestamp day, as days from 1970-01-01
  static Result<Literal> ExtractDay(const Literal& literal);

  /// \brief Extract a timestamp hour, as hours from 1970-01-01 00:00:00
  static Result<Literal> ExtractHour(const Literal& literal);
};

}  // namespace iceberg
