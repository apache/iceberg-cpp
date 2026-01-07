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

#include "iceberg/util/timepoint.h"

namespace iceberg {

namespace {
const int32_t kEpochYear = 1970;
}  // namespace

std::string TransformUtil::HumanYear(int32_t year_ordinal) {
  return std::format("{:04d}", kEpochYear + year_ordinal);
}

std::string TransformUtil::HumanMonth(int32_t month_ordinal) {
  int32_t year = kEpochYear + month_ordinal / 12;
  int32_t month = month_ordinal % 12 + 1;
  if (month <= 0) {
    year--;
    month += 12;
  }
  return std::format("{:04d}-{:02d}", year, month);
}

std::string TransformUtil::HumanDay(int32_t day_ordinal) {
  auto tp = std::chrono::time_point<std::chrono::system_clock, std::chrono::days>{
      std::chrono::days{day_ordinal}};
  return std::format("{:%F}", tp);
}

std::string TransformUtil::HumanHour(int32_t hour_ordinal) {
  auto tp = std::chrono::time_point<std::chrono::system_clock, std::chrono::hours>{
      std::chrono::hours{hour_ordinal}};
  return std::format("{:%F-%H}", tp);
}

std::string TransformUtil::HumanTime(int64_t micros_from_midnight) {
  auto tp = std::chrono::time_point<std::chrono::system_clock, std::chrono::seconds>{
      std::chrono::seconds{micros_from_midnight / kMicrosPerSecond}};
  auto micros = micros_from_midnight % kMicrosPerSecond;
  if (micros == 0) {
    return std::format("{:%T}", tp);
  } else if (micros % 1000 == 0) {
    return std::format("{:%T}.{:03d}", tp, micros / kMicrosPerMillis);
  } else {
    return std::format("{:%T}.{:06d}", tp, micros);
  }
}

std::string TransformUtil::HumanTimestamp(int64_t timestamp_micros) {
  return FormatUnixMicro(timestamp_micros);
}

std::string TransformUtil::HumanTimestampWithZone(int64_t timestamp_micros) {
  return FormatUnixMicroTz(timestamp_micros);
}

std::string TransformUtil::Base64Encode(std::string_view str_to_encode) {
  static const std::string base64_chars =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  int32_t i = 0;
  int32_t j = 0;
  std::array<char, 3> char_array_3;
  std::array<char, 4> char_array_4;

  std::string encoded;
  encoded.reserve((str_to_encode.size() + 2) * 4 / 3);

  for (char byte : str_to_encode) {
    char_array_3[i++] = byte;
    if (i == 3) {
      char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
      char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
      char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
      char_array_4[3] = char_array_3[2] & 0x3f;

      for (j = 0; j < 4; j++) encoded += base64_chars[char_array_4[j]];
      i = 0;
    }
  }

  if (i) {
    for (j = i; j < 3; j++) char_array_3[j] = '\0';

    char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
    char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
    char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
    char_array_4[3] = char_array_3[2] & 0x3f;

    for (j = 0; j < i + 1; j++) encoded += base64_chars[char_array_4[j]];

    while (i++ < 3) encoded += '=';
  }

  return encoded;
}

}  // namespace iceberg
