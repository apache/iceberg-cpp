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

#include "iceberg/util/url_encoder.h"

#include <locale>

namespace iceberg {

namespace {

bool IsUnreserved(unsigned char c) {
  return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
         c == '-' || c == '.' || c == '_' || c == '~';
}

// Helper: convert hex char to int (0–15), returns -1 if invalid
constexpr int8_t FromHex(char c) {
  if (c >= '0' && c <= '9') return c - '0';
  if (c >= 'A' && c <= 'F') return c - 'A' + 10;
  if (c >= 'a' && c <= 'f') return c - 'a' + 10;
  return -1;
}

}  // namespace

std::string UrlEncoder::Encode(std::string_view str_to_encode) {
  static const char* kHexChars = "0123456789ABCDEF";
  std::string result;
  result.reserve(str_to_encode.size() * 3 / 2 /* Heuristic reservation */);

  for (char c : str_to_encode) {
    if (IsUnreserved(c)) {
      result += c;
    } else {
      result += '%';
      result += kHexChars[c >> 4];
      result += kHexChars[c & 0xF];
    }
  }

  return result;
}

std::string UrlEncoder::Decode(std::string_view str_to_decode) {
  std::string result;
  result.reserve(str_to_decode.size());

  for (size_t i = 0; i < str_to_decode.size(); ++i) {
    char c = str_to_decode[i];
    if (c == '%' && i + 2 < str_to_decode.size()) {
      int8_t hi = FromHex(str_to_decode[i + 1]);
      int8_t lo = FromHex(str_to_decode[i + 2]);

      if (hi != -1 && lo != -1) {
        result += static_cast<char>((hi << 4) | lo);
        i += 2;
        continue;
      }
    }
    // Not a valid %XX sequence, copy as-is
    result += c;
  }

  return result;
}

}  // namespace iceberg
