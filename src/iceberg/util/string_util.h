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
#include <optional>
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
  /// Pure-ASCII input takes a byte-wise fast path; utf8proc is only invoked when a
  /// non-ASCII byte (>= 0x80) is present. The function is total: it never fails, and
  /// input need not be valid UTF-8. A byte that does not begin a valid UTF-8 sequence
  /// is copied through unchanged and decoding resumes at the next byte, so the valid
  /// code points around it are still lower-cased.
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

  /// \brief Case-insensitive equality using Unicode simple (1:1) case mapping.
  ///
  /// Equal when the ToLower forms of both operands are equal, so folding follows
  /// ToLower's rules (e.g. "İ" (U+0130) folds to "i"). Defined for any byte sequence:
  /// ToLower passes invalid UTF-8 bytes through unchanged, so they compare verbatim.
  static bool EqualsIgnoreCase(std::string_view lhs, std::string_view rhs) {
    const std::optional<bool> fast = AsciiEqualsIgnoreCase(lhs, rhs);
    return fast.has_value() ? *fast : (ToLower(lhs) == ToLower(rhs));
  }

  /// \brief Case-insensitive prefix test using Unicode simple (1:1) case mapping.
  ///
  /// True when the ToLower form of str starts with the ToLower form of prefix, so folding
  /// follows ToLower's rules (e.g. "İ" (U+0130) folds to "i"). Defined for any byte
  /// sequence: ToLower passes invalid UTF-8 bytes through unchanged, so they compare
  /// verbatim.
  static bool StartsWithIgnoreCase(std::string_view str, std::string_view prefix) {
    if (prefix.size() <= str.size()) {
      const std::optional<bool> fast =
          AsciiEqualsIgnoreCase(str.substr(0, prefix.size()), prefix);
      if (fast.has_value()) {
        return *fast;
      }
    }
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
  // ASCII-only case mappings. These avoid std::toupper/std::tolower, which are
  // locale-dependent and have undefined behavior for negative char values.
  static constexpr char ToUpperAscii(char c) noexcept {
    return (c >= 'a' && c <= 'z') ? static_cast<char>(c - 'a' + 'A') : c;
  }
  static constexpr char ToLowerAscii(char c) noexcept {
    return (c >= 'A' && c <= 'Z') ? static_cast<char>(c - 'A' + 'a') : c;
  }

  // True if c is a 7-bit ASCII byte (< 0x80). The cast is required because char may be
  // signed, which would make bytes >= 0x80 compare as negative.
  static constexpr bool IsAsciiByte(char c) noexcept {
    return (static_cast<unsigned char>(c) & 0x80) == 0;
  }

  // Case-insensitive equality decided in a single byte-wise pass, without allocating.
  // Returns nullopt once a byte of either operand is non-ASCII, because folding can then
  // be non-ASCII and length-changing (e.g. "İ" (U+0130) -> "i"), which only ToLower
  // knows.
  static std::optional<bool> AsciiEqualsIgnoreCase(std::string_view a,
                                                   std::string_view b) {
    const size_t n = std::min(a.size(), b.size());
    for (size_t i = 0; i < n; ++i) {
      if (!IsAsciiByte(a[i]) || !IsAsciiByte(b[i])) {
        return std::nullopt;
      }
      if (ToLowerAscii(a[i]) != ToLowerAscii(b[i])) {
        return false;
      }
    }
    return a.size() == b.size();
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
