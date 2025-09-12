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

#include "iceberg/util/date_time_util.h"

#include <chrono>
#include <cstdint>
#include <iomanip>
#include <sstream>

#include "iceberg/exception.h"

namespace iceberg {

namespace {

// Helper function to parse fractional seconds from input stream
Result<int64_t> ParseAndAddFractionalSeconds(std::istringstream& in) {
  if (in.peek() != '.') {
    return 0LL;
  }

  in.ignore();
  std::string fractional_str;
  char c;
  while (in.get(c) && std::isdigit(c)) {
    fractional_str += c;
  }
  if (in) {
    in.unget();
  }

  if (fractional_str.length() > 6) {
    fractional_str.resize(6);
  }

  return ParseFractionalSeconds(fractional_str);
}

}  // namespace

int32_t MicrosToDays(int64_t micros_since_epoch) {
  std::chrono::microseconds micros(micros_since_epoch);
  auto days_duration = std::chrono::floor<std::chrono::days>(micros);
  return static_cast<int32_t>(days_duration.count());
}

time_t TimegmCustom(std::tm* tm) {
#if defined(_WIN32)
  return _mkgmtime(tm);
#else
  return timegm(tm);
#endif
}

Result<int64_t> ParseFractionalSeconds(const std::string& fractional_str) {
  if (fractional_str.empty()) {
    return 0LL;
  }

  if (fractional_str.length() > 6) {
    return InvalidArgument("Fractional seconds cannot exceed 6 digits");
  }

  // Validate that all characters are digits
  for (char c : fractional_str) {
    if (!std::isdigit(c)) {
      return InvalidArgument("Fractional seconds must contain only digits");
    }
  }

  try {
    std::string padded_fractional = fractional_str;
    padded_fractional.append(6 - fractional_str.length(), '0');
    return std::stoll(padded_fractional);
  } catch (const std::exception&) {
    return InvalidArgument("Failed to parse fractional seconds '{}'", fractional_str);
  }
}

Result<int32_t> ParseDateString(const std::string& date_str) {
  std::istringstream in{date_str};
  std::tm tm = {};

  // Parse "YYYY-MM-DD" into days since 1970-01-01 epoch.
  in >> std::get_time(&tm, "%Y-%m-%d");

  if (in.fail() || tm.tm_mday == 0 || !in.eof()) {
    return InvalidArgument("Failed to parse '{}' as a valid Date (expected YYYY-MM-DD)",
                           date_str);
  }

  auto time_point = std::chrono::system_clock::from_time_t(TimegmCustom(&tm));
  auto days_since_epoch = std::chrono::floor<std::chrono::days>(time_point);
  return static_cast<int32_t>(days_since_epoch.time_since_epoch().count());
}

Result<int64_t> ParseTimeString(const std::string& time_str) {
  std::istringstream in{time_str};
  std::tm tm = {};

  // Parse "HH:MM:SS.ffffff" into microseconds since midnight.
  in >> std::get_time(&tm, "%H:%M:%S");

  if (in.fail()) {
    return InvalidArgument(
        "Failed to parse '{}' as a valid Time (expected HH:MM:SS.ffffff)", time_str);
  }

  int64_t total_micros = (tm.tm_hour * 3600LL + tm.tm_min * 60LL + tm.tm_sec) * 1000000LL;

  auto fractional_result = ParseAndAddFractionalSeconds(in);
  if (!fractional_result.has_value()) {
    return std::unexpected(fractional_result.error());
  }
  total_micros += fractional_result.value();

  if (in.peek() != EOF) {
    return InvalidArgument("Unconsumed characters found after parsing Time '{}'",
                           time_str);
  }

  return total_micros;
}

Result<int64_t> ParseTimestampString(const std::string& timestamp_str) {
  std::istringstream in{timestamp_str};
  std::tm tm = {};

  // Parse "YYYY-MM-DDTHH:MM:SS.ffffff"
  in >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");

  if (in.fail()) {
    return InvalidArgument(
        "Failed to parse '{}' as a valid Timestamp (expected YYYY-MM-DDTHH:MM:SS...)",
        timestamp_str);
  }

  auto seconds_since_epoch = TimegmCustom(&tm);
  int64_t total_micros = seconds_since_epoch * 1000000LL;

  auto fractional_result = ParseAndAddFractionalSeconds(in);
  if (!fractional_result.has_value()) {
    return std::unexpected(fractional_result.error());
  }
  total_micros += fractional_result.value();

  if (in.peek() != EOF) {
    return InvalidArgument("Unconsumed characters found after parsing Timestamp '{}'",
                           timestamp_str);
  }

  return total_micros;
}

Result<int64_t> ParseTimestampTzString(const std::string& timestamptz_str) {
  std::istringstream in{timestamptz_str};
  std::tm tm = {};

  // Parse "YYYY-MM-DDTHH:MM:SS.ffffff" and optional 'Z'
  in >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");

  if (in.fail()) {
    return InvalidArgument(
        "Failed to parse '{}' as a valid Timestamp (expected YYYY-MM-DDTHH:MM:SS...)",
        timestamptz_str);
  }

  auto seconds_since_epoch = TimegmCustom(&tm);
  int64_t total_micros = seconds_since_epoch * 1000000LL;

  auto fractional_result = ParseAndAddFractionalSeconds(in);
  if (!fractional_result.has_value()) {
    return std::unexpected(fractional_result.error());
  }
  total_micros += fractional_result.value();

  // NOTE: This implementation DOES NOT support timezone offsets like
  // '+08:00' or '-07:00'. It only supports the UTC designator 'Z'.
  if (in.peek() == 'Z') {
    in.ignore();  // Consume 'Z'
  }

  if (in.peek() != EOF) {
    return InvalidArgument("Unconsumed characters found after parsing Timestamp '{}'",
                           timestamptz_str);
  }

  return total_micros;
}

}  // namespace iceberg
