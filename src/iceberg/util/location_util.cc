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

#include "iceberg/util/location_util.h"

#include <algorithm>

namespace iceberg {

namespace {

// ASCII by design: RFC 3986 restricts schemes to ASCII, and <cctype> is
// locale-sensitive.
constexpr bool IsAsciiAlpha(char c) {
  return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}

constexpr bool IsAsciiAlnum(char c) { return IsAsciiAlpha(c) || (c >= '0' && c <= '9'); }

/// Whether `candidate` is a syntactically valid URI scheme (RFC 3986 section
/// 3.1): a letter followed by letters, digits, `+`, `-` or `.`.
bool IsValidScheme(std::string_view candidate) {
  if (candidate.empty() || !IsAsciiAlpha(candidate.front())) {
    return false;
  }
  return std::ranges::all_of(candidate, [](char c) {
    return IsAsciiAlnum(c) || c == '+' || c == '-' || c == '.';
  });
}

}  // namespace

std::string_view LocationUtil::ParseScheme(std::string_view location) {
  const auto colon = location.find(':');
  if (colon == std::string_view::npos || colon == 0) {
    return {};
  }
  const auto candidate = location.substr(0, colon);
  // Cannot be a scheme -> a path whose first segment has a colon, such as the
  // extended-length Windows form `\\?\C:\...`.
  if (!IsValidScheme(candidate)) {
    return {};
  }
#ifdef _WIN32
  // A single letter before the colon is a drive, not a scheme.
  if (candidate.size() == 1) {
    return {};
  }
#endif
  return candidate;
}

}  // namespace iceberg
