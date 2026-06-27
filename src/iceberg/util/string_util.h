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

namespace iceberg {

template <typename T>
concept FromChars = requires(const char* p, T& v) { std::from_chars(p, p, v); };

class ICEBERG_EXPORT StringUtils {
 public:
  /// \brief Lower-case a UTF-8 string using Unicode simple (1:1) case mapping.
  ///
  /// Intended for case-insensitive name matching, similar to Iceberg Java's
  /// toLowerCase(Locale.ROOT). The mapping is locale-independent, matching the intent
  /// of Locale.ROOT. It uses simple (1:1) case mapping rather than Java's full case
  /// mapping, so results differ for a few code points; e.g. U+0130 (capital I with dot
  /// above) maps to U+0069 ("i") here, but to U+0069 U+0307 ("i" + combining dot above)
  /// in Java. For ASCII and the large majority of letters the two agree.
  ///
  /// Invalid UTF-8 input is returned unchanged.
  /// See https://github.com/apache/iceberg-cpp/issues/613.
  static std::string ToLower(std::string_view str);

  /// \brief Upper-case the ASCII letters (a-z) in a string; all other bytes, including
  /// multi-byte UTF-8 sequences, are left unchanged.
  ///
  /// Deliberately ASCII-only and, unlike ToLower, not Unicode-aware. It is only used to
  /// normalize ASCII enum/codec strings (e.g. "gzip" -> "GZIP", "all" -> "ALL") for
  /// case-insensitive comparison. A Unicode upper-case is intentionally not provided:
  /// simple case mapping would be wrong for some letters (e.g. "ß" (U+00DF) would stay
  /// unchanged instead of becoming "SS"), and no caller needs it.
  static std::string ToUpper(std::string_view str) {
    return str | std::ranges::views::transform(ToUpperAscii) |
           std::ranges::to<std::string>();
  }

  /// \brief Case-insensitive equality; compares the ToLower forms of both operands and
  /// therefore inherits ToLower's Unicode simple-mapping behavior.
  static bool EqualsIgnoreCase(std::string_view lhs, std::string_view rhs) {
    return ToLower(lhs) == ToLower(rhs);
  }

  /// \brief Case-insensitive prefix test, comparing the ToLower forms of both inputs.
  ///
  /// Inherits ToLower's Unicode simple-mapping behavior. The whole strings are
  /// lower-cased rather than byte-slicing str to prefix.size(), because ToLower can
  /// change a string's byte length (e.g. "İ" (U+0130) is two bytes but maps to "i"),
  /// so a byte slice could split a code point or reject a valid match.
  static bool StartsWithIgnoreCase(std::string_view str, std::string_view prefix) {
    return ToLower(str).starts_with(ToLower(prefix));
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
      if (ptr != str.data() + str.size()) {
        return InvalidArgument("Failed to parse {} from string '{}': trailing characters",
                               typeid(T).name(), str);
      }
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
  static Result<std::vector<uint8_t>> HexStringToBytes(std::string_view hex);

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

 private:
  // Avoids std::toupper, which is locale-dependent and has undefined behavior for
  // negative char values.
  static constexpr char ToUpperAscii(char c) noexcept {
    return (c >= 'a' && c <= 'z') ? static_cast<char>(c - 'a' + 'A') : c;
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
