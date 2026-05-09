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

namespace {

// Shared base64 decode logic. The decode table maps ASCII char → 6-bit value.
// 0xFF means invalid character.
std::string Base64DecodeWithTable(std::string_view input,
                                  const std::array<uint8_t, 256>& table) {
  // Strip trailing padding
  while (!input.empty() && input.back() == '=') {
    input.remove_suffix(1);
  }
  if (input.empty()) {
    return {};
  }

  std::string output;
  output.reserve((input.size() * 3) / 4);

  uint32_t buffer = 0;
  int bits_collected = 0;

  for (char c : input) {
    uint8_t val = table[static_cast<uint8_t>(c)];
    if (val == 0xFF) {
      return {};  // Invalid character
    }
    buffer = (buffer << 6) | val;
    bits_collected += 6;
    if (bits_collected >= 8) {
      bits_collected -= 8;
      output.push_back(static_cast<char>((buffer >> bits_collected) & 0xFF));
    }
  }

  return output;
}

// Standard base64 decode table: A-Z=0-25, a-z=26-51, 0-9=52-61, +=62, /=63
constexpr std::array<uint8_t, 256> kBase64DecodeTable = [] {
  std::array<uint8_t, 256> table{};
  table.fill(0xFF);
  for (int i = 0; i < 26; ++i) {
    table[static_cast<size_t>('A' + i)] = static_cast<uint8_t>(i);
    table[static_cast<size_t>('a' + i)] = static_cast<uint8_t>(26 + i);
  }
  for (int i = 0; i < 10; ++i) {
    table[static_cast<size_t>('0' + i)] = static_cast<uint8_t>(52 + i);
  }
  table[static_cast<size_t>('+')] = 62;
  table[static_cast<size_t>('/')] = 63;
  return table;
}();

// Base64url decode table: same as standard but '-'=62, '_'=63
constexpr std::array<uint8_t, 256> kBase64UrlDecodeTable = [] {
  std::array<uint8_t, 256> table{};
  table.fill(0xFF);
  for (int i = 0; i < 26; ++i) {
    table[static_cast<size_t>('A' + i)] = static_cast<uint8_t>(i);
    table[static_cast<size_t>('a' + i)] = static_cast<uint8_t>(26 + i);
  }
  for (int i = 0; i < 10; ++i) {
    table[static_cast<size_t>('0' + i)] = static_cast<uint8_t>(52 + i);
  }
  table[static_cast<size_t>('-')] = 62;
  table[static_cast<size_t>('_')] = 63;
  return table;
}();

}  // namespace

std::string TransformUtil::Base64Decode(std::string_view encoded) {
  return Base64DecodeWithTable(encoded, kBase64DecodeTable);
}

std::string TransformUtil::Base64UrlDecode(std::string_view encoded) {
  return Base64DecodeWithTable(encoded, kBase64UrlDecodeTable);
}

}  // namespace iceberg
