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

#include <cstdint>
#include <string>
#include <utility>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

class ICEBERG_EXPORT TruncateUtils {
 public:
  /// \brief Truncate a UTF-8 string to a specified number of code points.
  ///
  /// \param source The input string to truncate.
  /// \param L The maximum number of code points allowed in the output string.
  /// \return A valid UTF-8 string truncated to L code points.
  /// If the input string is already valid and has fewer than L code points, it is
  /// returned unchanged.
  static std::string TruncateUTF8(std::string source, size_t L) {
    size_t code_point_count = 0;
    size_t safe_point = 0;

    for (size_t i = 0; i < source.size(); ++i) {
      // Start of a new UTF-8 code point
      if ((source[i] & 0xC0) != 0x80) {
        code_point_count++;
        if (code_point_count > static_cast<size_t>(L)) {
          safe_point = i;
          break;
        }
      }
    }

    if (safe_point != 0) {
      // Resize the string to the safe point
      source.resize(safe_point);
    }

    return source;
  }

  /// \brief Truncate a UTF-8 string to a specified number of code points for
  /// use as an upper-bound value.
  ///
  /// When truncation is required, the returned value is the smallest UTF-8
  /// string greater than the truncated prefix. When no truncation is needed
  /// for the given width, the original string may be returned unchanged.
  ///
  /// \param source The input string to truncate.
  /// \param L The maximum number of code points allowed in the output string.
  /// \return A Result containing the original string (if no truncation is
  /// needed), or the smallest string greater than the truncated prefix, or an
  /// error if no such value exists or the input is invalid UTF-8.
  static Result<std::string> TruncateUTF8Max(const std::string& source, size_t L);

  /// \brief Truncate an integer v, either int32_t or int64_t, to v - (v % W).
  ///
  /// The remainder, v % W, must be positive. For languages where % can produce negative
  /// values, the correct truncate function is: v - (((v % W) + W) % W)
  template <typename T>
    requires std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t>
  static inline T TruncateInteger(T v, int32_t W) {
    return v - (((v % W) + W) % W);
  }

  /// \brief Truncate a Decimal to a specified width.
  /// \param decimal The input Decimal to truncate.
  /// \param width The width to truncate to.
  /// \return A Decimal truncated to the specified width.
  static Decimal TruncateDecimal(const Decimal& decimal, int32_t width);

  /// \brief Truncate a Literal to a specified width.
  /// \param literal The input Literal to truncate.
  /// \param width The width to truncate to.
  /// \return A Result containing the truncated Literal or an error.
  /// Supported types are: INT, LONG, DECIMAL, STRING, BINARY.
  /// Reference:
  /// - [Truncate Transform
  /// Details](https://iceberg.apache.org/spec/#truncate-transform-details)
  static Result<Literal> TruncateLiteral(const Literal& literal, int32_t width);

  /// \brief Truncate a Literal to a specified width for use as an upper-bound value.
  ///
  /// When truncation is required, the returned value is the smallest Literal greater than
  /// the truncated prefix. When no truncation is needed for the given width, the original
  /// Literal may be returned unchanged.
  ///
  /// \param value The input Literal maximum value to truncate.
  /// \param width The width to truncate to.
  /// \return A Result containing either the original Literal (if no truncation is needed)
  /// or the smallest Literal greater than the truncated prefix, or an error if no such
  /// value exists or cannot be represented.
  static Result<Literal> TruncateLiteralMax(const Literal& value, int32_t width);
};

}  // namespace iceberg
