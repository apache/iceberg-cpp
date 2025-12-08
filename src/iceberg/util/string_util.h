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

#include <algorithm>
#include <ranges>
#include <string>

#include "iceberg/iceberg_export.h"

namespace iceberg {

class ICEBERG_EXPORT StringUtils {
 public:
  static std::string ToLower(std::string_view str) {
    return str | std::ranges::views::transform([](char c) { return std::tolower(c); }) |
           std::ranges::to<std::string>();
  }

  static std::string ToUpper(std::string_view str) {
    return str | std::ranges::views::transform([](char c) { return std::toupper(c); }) |
           std::ranges::to<std::string>();
  }

  static bool EqualsIgnoreCase(std::string_view lhs, std::string_view rhs) {
    return std::ranges::equal(
        lhs, rhs, [](char lc, char rc) { return std::tolower(lc) == std::tolower(rc); });
  }

  /// \brief Count the number of code points in a UTF-8 string.
  static size_t CodePointCount(std::string_view str) {
    size_t count = 0;
    for (char i : str) {
      if ((i & 0xC0) != 0x80) {
        count++;
      }
    }
    return count;
  }
};

/// \brief Transparent hash function that supports std::string_view as lookup key
///
/// Enables std::unordered_map to directly accept std::string_view lookup keys
/// without creating temporary std::string objects, using C++20's transparent lookup.
struct ICEBERG_EXPORT StringHash {
  using hash_type = std::hash<std::string_view>;
  using is_transparent = void;

  std::size_t operator()(std::string_view str) const { return hash_type{}(str); }
  std::size_t operator()(const char* str) const { return hash_type{}(str); }
  std::size_t operator()(const std::string& str) const { return hash_type{}(str); }
};

}  // namespace iceberg
