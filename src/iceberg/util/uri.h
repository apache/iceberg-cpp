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
#include <cctype>
#include <cstddef>
#include <string_view>

/// \file iceberg/util/uri.h
/// \brief URI scheme detection utilities per RFC 3986.

namespace iceberg {

/// \brief Check whether a string begins with a valid RFC 3986 URI scheme
/// followed by ':'.
///
/// A scheme (RFC 3986 §3.1) is defined as:
///   scheme = ALPHA *( ALPHA / DIGIT / "+" / "-" / "." )
///
/// A single character before ':' is treated as a Windows drive letter, not a
/// scheme (e.g., "C:\path").
///
/// \param value The string to inspect.
/// \param scheme_colon_pos If non-null and the function returns true, set to the
///   position of the scheme-delimiting ':' so callers can reuse it without a
///   redundant search.
/// \return true if \p value starts with a valid URI scheme followed by ':'.
inline bool IsUriScheme(std::string_view value, std::size_t* scheme_colon_pos = nullptr) {
  auto colon_pos = value.find(':');
  // Reject if there is no ':', an empty scheme (colon_pos == 0), or only a
  // single character before ':' (colon_pos == 1), which is a Windows drive
  // letter (e.g. "C:\path"), not a URI scheme.
  if (colon_pos == std::string_view::npos || colon_pos <= 1) {
    return false;
  }
  if (!std::isalpha(static_cast<unsigned char>(value[0]))) {
    return false;
  }
  bool valid = std::ranges::all_of(value.substr(1, colon_pos - 1), [](char c) {
    return std::isalpha(static_cast<unsigned char>(c)) ||
           std::isdigit(static_cast<unsigned char>(c)) || c == '+' || c == '-' ||
           c == '.';
  });
  if (valid && scheme_colon_pos != nullptr) {
    *scheme_colon_pos = colon_pos;
  }
  return valid;
}

}  // namespace iceberg
