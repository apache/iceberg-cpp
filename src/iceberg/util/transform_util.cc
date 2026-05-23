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
#include <chrono>
#include <format>

#include "iceberg/util/temporal_util.h"

namespace iceberg {

std::string TransformUtil::HumanYear(int32_t year_ordinal) {
  auto y = internal::kEpochYmd + std::chrono::years{year_ordinal};
  return std::format("{:%Y}", y);
}

std::string TransformUtil::HumanMonth(int32_t month_ordinal) {
  auto ym = internal::kEpochYmd + std::chrono::months(month_ordinal);
  return std::format("{:%Y-%m}", ym);
}

std::string TransformUtil::HumanDay(int32_t day_ordinal) {
  auto ymd = internal::kEpochDays + std::chrono::days{day_ordinal};
  return std::format("{:%F}", ymd);
}

std::string TransformUtil::HumanHour(int32_t hour_ordinal) {
  auto tp = std::chrono::time_point<std::chrono::system_clock, std::chrono::hours>{
      std::chrono::hours{hour_ordinal}};
  return std::format("{:%F-%H}", tp);
}

std::string TransformUtil::HumanTime(int64_t micros_from_midnight) {
  std::chrono::hh_mm_ss<std::chrono::seconds> hms{
      std::chrono::seconds{micros_from_midnight / internal::kMicrosPerSecond}};
  auto micros = micros_from_midnight % internal::kMicrosPerSecond;
  if (micros == 0 && hms.seconds().count() == 0) {
    return std::format("{:%R}", hms);
  } else if (micros == 0) {
    return std::format("{:%T}", hms);
  } else if (micros % internal::kMicrosPerMilli == 0) {
    return std::format("{:%T}.{:03d}", hms, micros / internal::kMicrosPerMilli);
  } else {
    return std::format("{:%T}.{:06d}", hms, micros);
  }
}

std::string TransformUtil::HumanTimestamp(int64_t timestamp_micros) {
  const auto micros_since_epoch = std::chrono::microseconds{timestamp_micros};
  const auto seconds_since_epoch =
      std::chrono::floor<std::chrono::seconds>(micros_since_epoch);
  auto tp = std::chrono::time_point<std::chrono::system_clock, std::chrono::seconds>{
      seconds_since_epoch};
  auto micros = std::chrono::duration_cast<std::chrono::microseconds>(micros_since_epoch -
                                                                      seconds_since_epoch)
                    .count();
  if (micros == 0) {
    return std::format("{:%FT%T}", tp);
  } else if (micros % internal::kMicrosPerMilli == 0) {
    return std::format("{:%FT%T}.{:03d}", tp, micros / internal::kMicrosPerMilli);
  } else {
    return std::format("{:%FT%T}.{:06d}", tp, micros);
  }
}

std::string TransformUtil::HumanTimestampNs(int64_t timestamp_nanos) {
  const auto nanos_since_epoch = std::chrono::nanoseconds{timestamp_nanos};
  const auto seconds_since_epoch =
      std::chrono::floor<std::chrono::seconds>(nanos_since_epoch);
  auto tp = std::chrono::time_point<std::chrono::system_clock, std::chrono::seconds>{
      seconds_since_epoch};
  auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(nanos_since_epoch -
                                                                    seconds_since_epoch)
                   .count();
  if (nanos == 0) {
    return std::format("{:%FT%T}", tp);
  } else if (nanos % internal::kNanosPerMilli == 0) {
    return std::format("{:%FT%T}.{:03d}", tp, nanos / internal::kNanosPerMilli);
  } else if (nanos % internal::kNanosPerMicro == 0) {
    return std::format("{:%FT%T}.{:06d}", tp, nanos / internal::kNanosPerMicro);
  } else {
    return std::format("{:%FT%T}.{:09d}", tp, nanos);
  }
}

std::string TransformUtil::HumanTimestampWithZone(int64_t timestamp_micros) {
  const auto micros_since_epoch = std::chrono::microseconds{timestamp_micros};
  const auto seconds_since_epoch =
      std::chrono::floor<std::chrono::seconds>(micros_since_epoch);
  auto tp = std::chrono::time_point<std::chrono::system_clock, std::chrono::seconds>{
      seconds_since_epoch};
  auto micros = std::chrono::duration_cast<std::chrono::microseconds>(micros_since_epoch -
                                                                      seconds_since_epoch)
                    .count();
  if (micros == 0) {
    return std::format("{:%FT%T}+00:00", tp);
  } else if (micros % internal::kMicrosPerMilli == 0) {
    return std::format("{:%FT%T}.{:03d}+00:00", tp, micros / internal::kMicrosPerMilli);
  } else {
    return std::format("{:%FT%T}.{:06d}+00:00", tp, micros);
  }
}

std::string TransformUtil::HumanTimestampNsWithZone(int64_t timestamp_nanos) {
  const auto nanos_since_epoch = std::chrono::nanoseconds{timestamp_nanos};
  const auto seconds_since_epoch =
      std::chrono::floor<std::chrono::seconds>(nanos_since_epoch);
  auto tp = std::chrono::time_point<std::chrono::system_clock, std::chrono::seconds>{
      seconds_since_epoch};
  auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(nanos_since_epoch -
                                                                    seconds_since_epoch)
                   .count();
  if (nanos == 0) {
    return std::format("{:%FT%T}+00:00", tp);
  } else if (nanos % internal::kNanosPerMilli == 0) {
    return std::format("{:%FT%T}.{:03d}+00:00", tp, nanos / internal::kNanosPerMilli);
  } else if (nanos % internal::kNanosPerMicro == 0) {
    return std::format("{:%FT%T}.{:06d}+00:00", tp, nanos / internal::kNanosPerMicro);
  } else {
    return std::format("{:%FT%T}.{:09d}+00:00", tp, nanos);
  }
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
