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

#include <iomanip>
#include <sstream>

namespace iceberg {

std::string UrlEncoder::Encode(std::string_view str_to_encode) {
  std::stringstream escaped;
  escaped.fill('0');
  escaped << std::hex;

  for (unsigned char c : str_to_encode) {
    // reserve letters, numbers and -._~
    if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
      escaped << c;
    } else {
      escaped << '%' << std::setw(2) << static_cast<int>(c) << std::setfill('0');
    }
  }
  return escaped.str();
}

std::string UrlEncoder::Decode(std::string_view str_to_decode) {
  std::string result;
  result.reserve(str_to_decode.size());

  for (size_t i = 0; i < str_to_decode.size(); ++i) {
    char c = str_to_decode[i];
    if (c == '%' && i + 2 < str_to_decode.size()) {
      std::string hex(str_to_decode.substr(i + 1, 2));
      try {
        char decoded = static_cast<char>(std::stoi(hex, nullptr, 16));
        result += decoded;
        i += 2;
      } catch (...) {
        result += c;
      }
    } else if (c == '+') {
      // In application/x-www-form-urlencoded, '+' represents a whitespace.
      result += ' ';
    } else {
      result += c;
    }
  }

  return result;
}

}  // namespace iceberg
