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

#include "iceberg/util/transform_util.h"

#include <array>
#include <charconv>
#include <chrono>

#include "iceberg/util/macros.h"

namespace iceberg {

namespace {
constexpr auto kEpochDate = std::chrono::year{1970} / std::chrono::January / 1;
constexpr int64_t kMicrosPerMillis = 1'000;
constexpr int64_t kMicrosPerSecond = 1'000'000;
constexpr int64_t kMicrosPerDay = 86'400'000'000LL;

/// Parse fractional seconds (after '.') and return micros.
/// Accepts 1-6 digits, zero-padded on the right to 6 digits.
Result<int64_t> ParseFractionalMicros(std::string_view frac) {
  int32_t val = 0;
  auto [_, ec] = std::from_chars(frac.data(), frac.data() + frac.size(), val);
  if (frac.empty() || frac.size() > 6 || ec != std::errc{}) {
    return InvalidArgument("Invalid fractional seconds: '{}'", frac);
  }
  // Right-pad to 6 digits: "500" → 500000, "001" → 1000, "000001" → 1
  for (size_t i = frac.size(); i < 6; ++i) {
    val *= 10;
  }
  return static_cast<int64_t>(val);
}
}  // namespace

std::string TransformUtil::HumanYear(int32_t year_ordinal) {
  auto y = kEpochDate + std::chrono::years{year_ordinal};
  return std::format("{:%Y}", y);
}

std::string TransformUtil::HumanMonth(int32_t month_ordinal) {
  auto ym = kEpochDate + std::chrono::months(month_ordinal);
  return std::format("{:%Y-%m}", ym);
}

std::string TransformUtil::HumanDay(int32_t day_ordinal) {
  auto ymd = std::chrono::sys_days(kEpochDate) + std::chrono::days{day_ordinal};
  return std::format("{:%F}", ymd);
}

std::string TransformUtil::HumanHour(int32_t hour_ordinal) {
  auto tp = std::chrono::time_point<std::chrono::system_clock, std::chrono::hours>{
      std::chrono::hours{hour_ordinal}};
  return std::format("{:%F-%H}", tp);
}

std::string TransformUtil::HumanTime(int64_t micros_from_midnight) {
  std::chrono::hh_mm_ss<std::chrono::seconds> hms{
      std::chrono::seconds{micros_from_midnight / kMicrosPerSecond}};
  auto micros = micros_from_midnight % kMicrosPerSecond;
  if (micros == 0 && hms.seconds().count() == 0) {
    return std::format("{:%R}", hms);
  } else if (micros == 0) {
    return std::format("{:%T}", hms);
  } else if (micros % kMicrosPerMillis == 0) {
    return std::format("{:%T}.{:03d}", hms, micros / kMicrosPerMillis);
  } else {
    return std::format("{:%T}.{:06d}", hms, micros);
  }
}

std::string TransformUtil::HumanTimestamp(int64_t timestamp_micros) {
  auto tp = std::chrono::time_point<std::chrono::system_clock, std::chrono::seconds>{
      std::chrono::seconds(timestamp_micros / kMicrosPerSecond)};
  auto micros = timestamp_micros % kMicrosPerSecond;
  if (micros == 0) {
    return std::format("{:%FT%T}", tp);
  } else if (micros % kMicrosPerMillis == 0) {
    return std::format("{:%FT%T}.{:03d}", tp, micros / kMicrosPerMillis);
  } else {
    return std::format("{:%FT%T}.{:06d}", tp, micros);
  }
}

std::string TransformUtil::HumanTimestampWithZone(int64_t timestamp_micros) {
  auto tp = std::chrono::time_point<std::chrono::system_clock, std::chrono::seconds>{
      std::chrono::seconds(timestamp_micros / kMicrosPerSecond)};
  auto micros = timestamp_micros % kMicrosPerSecond;
  if (micros == 0) {
    return std::format("{:%FT%T}+00:00", tp);
  } else if (micros % kMicrosPerMillis == 0) {
    return std::format("{:%FT%T}.{:03d}+00:00", tp, micros / kMicrosPerMillis);
  } else {
    return std::format("{:%FT%T}.{:06d}+00:00", tp, micros);
  }
}

Result<int32_t> TransformUtil::ParseDay(std::string_view str) {
  // Expected format: "yyyy-MM-dd" )
  // Parse year, month, day manually
  auto dash1 = str.find('-', str[0] == '-' ? 1 : 0);
  auto dash2 = str.find('-', dash1 + 1);
  if (str.size() < 10 || dash1 == std::string_view::npos ||
      dash2 == std::string_view::npos) [[unlikely]] {
    return InvalidArgument("Invalid date string: '{}'", str);
  }
  int32_t year = 0, month = 0, day = 0;
  auto [_, e1] = std::from_chars(str.data(), str.data() + dash1, year);
  auto [__, e2] = std::from_chars(str.data() + dash1 + 1, str.data() + dash2, month);
  auto [___, e3] = std::from_chars(str.data() + dash2 + 1, str.data() + str.size(), day);

  if (e1 != std::errc{} || e2 != std::errc{} || e3 != std::errc{}) [[unlikely]] {
    return InvalidArgument("Invalid year in date string: '{}'", str);
  }

  auto ymd = std::chrono::year{year} / std::chrono::month{static_cast<unsigned>(month)} /
             std::chrono::day{static_cast<unsigned>(day)};
  if (!ymd.ok()) [[unlikely]] {
    return InvalidArgument("Invalid date: '{}'", str);
  }

  auto days = std::chrono::sys_days(ymd) - std::chrono::sys_days(kEpochDate);
  return static_cast<int32_t>(days.count());
}

Result<int64_t> TransformUtil::ParseTime(std::string_view str) {
  int64_t hours = 0, minutes = 0, seconds = 0;

  auto [_, eh] = std::from_chars(str.data(), str.data() + 2, hours);

  auto [__, em] = std::from_chars(str.data() + 3, str.data() + 5, minutes);

  if ((em != std::errc{}) || (eh != std::errc{}) || (str.size()) < 5) [[unlikely]] {
    return InvalidArgument("Invalid time string: '{}'", str);
  }

  int64_t frac_micros = 0;
  if (str.size() > 5) {
    auto [_, es] = std::from_chars(str.data() + 6, str.data() + 8, seconds);
    if (str[5] != ':' || str.size() < 8 || es != std::errc{}) [[unlikely]] {
      return InvalidArgument("Invalid time string: '{}'", str);
    }
    if (str.size() > 8) {
      if (str[8] != '.') [[unlikely]] {
        return InvalidArgument("Invalid time string: '{}'", str);
      }
      ICEBERG_ASSIGN_OR_RAISE(frac_micros, ParseFractionalMicros(str.substr(9)));
    }
  }

  return hours * 3'600 * kMicrosPerSecond + minutes * 60 * kMicrosPerSecond +
         seconds * kMicrosPerSecond + frac_micros;
}

Result<int64_t> TransformUtil::ParseTimestamp(std::string_view str) {
  // Format: "yyyy-MM-ddTHH:mm:ss[.SSS[SSS]]"
  auto t_pos = str.find('T');
  if (t_pos == std::string_view::npos) [[unlikely]] {
    return InvalidArgument("Invalid timestamp string (missing 'T'): '{}'", str);
  }

  ICEBERG_ASSIGN_OR_RAISE(auto days, ParseDay(str.substr(0, t_pos)));
  ICEBERG_ASSIGN_OR_RAISE(auto time_micros, ParseTime(str.substr(t_pos + 1)));

  return static_cast<int64_t>(days) * kMicrosPerDay + time_micros;
}

Result<int64_t> TransformUtil::ParseTimestampWithZone(std::string_view str) {
  // Format: same as ParseTimestamp but with "+00:00" suffix
  constexpr std::string_view kZoneSuffix = "+00:00";
  if (str.size() < kZoneSuffix.size() ||
      str.substr(str.size() - kZoneSuffix.size()) != kZoneSuffix) [[unlikely]] {
    return InvalidArgument("Invalid timestamptz string (missing '+00:00' suffix): '{}'",
                           str);
  }
  return ParseTimestamp(str.substr(0, str.size() - kZoneSuffix.size()));
}

std::string TransformUtil::Base64Encode(std::string_view str_to_encode) {
  static constexpr std::string_view kBase64Chars =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  int32_t i = 0;
  int32_t j = 0;
  std::array<unsigned char, 3> char_array_3;
  std::array<unsigned char, 4> char_array_4;

  std::string encoded;
  encoded.reserve((str_to_encode.size() + 2) * 4 / 3);

  for (unsigned char byte : str_to_encode) {
    char_array_3[i++] = byte;
    if (i == 3) {
      char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
      char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
      char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
      char_array_4[3] = char_array_3[2] & 0x3f;

      for (j = 0; j < 4; j++) {
        encoded += kBase64Chars[char_array_4[j]];
      }

      i = 0;
    }
  }

  if (i) {
    for (j = i; j < 3; j++) {
      char_array_3[j] = '\0';
    }

    char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
    char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
    char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
    char_array_4[3] = char_array_3[2] & 0x3f;

    for (j = 0; j < i + 1; j++) {
      encoded += kBase64Chars[char_array_4[j]];
    }

    while (i++ < 3) {
      encoded += '=';
    }
  }

  return encoded;
}

}  // namespace iceberg
