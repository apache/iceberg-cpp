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
#include <cerrno>
#include <charconv>
#include <ranges>
#include <string>
#include <string_view>
#include <type_traits>
#include <typeinfo>
#include <utility>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/util/macros.h"

namespace iceberg {

template <typename T>
concept FromChars = requires(const char* p, T& v) { std::from_chars(p, p, v); };

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

  static bool StartsWithIgnoreCase(std::string_view str, std::string_view prefix) {
    if (str.size() < prefix.size()) {
      return false;
    }
    return EqualsIgnoreCase(str.substr(0, prefix.size()), prefix);
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

  template <typename T>
    requires std::is_arithmetic_v<T> && FromChars<T> && (!std::same_as<T, bool>)
  static Result<T> ParseNumber(std::string_view str) {
    T value = 0;
    auto [ptr, ec] = std::from_chars(str.data(), str.data() + str.size(), value);
    if (ec == std::errc()) [[likely]] {
      return value;
    }
    if (ec == std::errc::invalid_argument) {
      return InvalidArgument("Failed to parse {} from string '{}': invalid argument",
                             typeid(T).name(), str);
    }
    if (ec == std::errc::result_out_of_range) {
      return InvalidArgument("Failed to parse {} from string '{}': value out of range",
                             typeid(T).name(), str);
    }
    std::unreachable();
  }

  /// \brief Decode a hex string (upper or lower case) into bytes.
  /// Returns an error if the string has odd length or contains invalid hex characters.
  static Result<std::vector<uint8_t>> HexStringToBytes(std::string_view hex) {
    if (hex.size() % 2 != 0) [[unlikely]] {
      return InvalidArgument("Hex string must have even length, got: {}", hex.size());
    }
    std::vector<uint8_t> bytes;
    bytes.reserve(hex.size() / 2);
    auto nibble = [](char c) -> Result<uint8_t> {
      if (c >= '0' && c <= '9') return static_cast<uint8_t>(c - '0');
      if (c >= 'a' && c <= 'f') return static_cast<uint8_t>(c - 'a' + 10);
      if (c >= 'A' && c <= 'F') return static_cast<uint8_t>(c - 'A' + 10);
      return InvalidArgument("Invalid hex character: '{}'", c);
    };
    for (size_t i = 0; i < hex.size(); i += 2) {
      ICEBERG_ASSIGN_OR_RAISE(auto hi, nibble(hex[i]));
      ICEBERG_ASSIGN_OR_RAISE(auto lo, nibble(hex[i + 1]));
      bytes.push_back(static_cast<uint8_t>((hi << 4) | lo));
    }
    return bytes;
  }

  template <typename T>
    requires std::is_floating_point_v<T> && (!FromChars<T>)
  static Result<T> ParseNumber(std::string_view str) {
    T value{};
    // strto* require null-terminated input; string_view does not guarantee it.
    std::string owned(str);
    const char* start = owned.c_str();
    char* end = nullptr;
    errno = 0;

    if constexpr (std::same_as<T, float>) {
      value = std::strtof(start, &end);
    } else if constexpr (std::same_as<T, double>) {
      value = std::strtod(start, &end);
    } else {
      value = std::strtold(start, &end);
    }

    if (end == start || end != start + static_cast<std::ptrdiff_t>(owned.size())) {
      return InvalidArgument("Failed to parse {} from string '{}': invalid argument",
                             typeid(T).name(), str);
    }
    if (errno == ERANGE) {
      return InvalidArgument("Failed to parse {} from string '{}': value out of range",
                             typeid(T).name(), str);
    }
    return value;
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

/// \brief Transparent equality function that supports std::string_view as lookup key
struct ICEBERG_EXPORT StringEqual {
  using is_transparent = void;

  bool operator()(std::string_view lhs, std::string_view rhs) const { return lhs == rhs; }
  bool operator()(const std::string& lhs, const std::string& rhs) const {
    return lhs == rhs;
  }
};

}  // namespace iceberg
