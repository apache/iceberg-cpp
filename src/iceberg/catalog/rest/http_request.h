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
#include <cstdint>
#include <map>
#include <string>
#include <string_view>

#include "iceberg/catalog/rest/iceberg_rest_export.h"

namespace iceberg::rest {

/// \brief HTTP method enumeration.
enum class HttpMethod : uint8_t { kGet, kPost, kPut, kDelete, kHead };

/// \brief Convert HttpMethod to string representation.
constexpr std::string_view ToString(HttpMethod method) {
  switch (method) {
    case HttpMethod::kGet:
      return "GET";
    case HttpMethod::kPost:
      return "POST";
    case HttpMethod::kPut:
      return "PUT";
    case HttpMethod::kDelete:
      return "DELETE";
    case HttpMethod::kHead:
      return "HEAD";
  }
  return "UNKNOWN";
}

/// \brief Case-insensitive ordering for HTTP header names.
///
/// HTTP header names are case-insensitive. This comparator also matches
/// cpr::Header's single-value map model.
struct CaseInsensitiveHeaderLess {
  using is_transparent = void;

  bool operator()(std::string_view lhs, std::string_view rhs) const noexcept {
    const auto min_size = lhs.size() < rhs.size() ? lhs.size() : rhs.size();
    for (std::size_t i = 0; i < min_size; ++i) {
      auto left = static_cast<unsigned char>(lhs[i]);
      auto right = static_cast<unsigned char>(rhs[i]);
      const int lower_left = std::tolower(left);
      const int lower_right = std::tolower(right);
      if (lower_left < lower_right) return true;
      if (lower_left > lower_right) return false;
    }
    return lhs.size() < rhs.size();
  }
};

/// \brief Single-value HTTP headers with case-insensitive names.
///
/// Repeated outgoing headers are intentionally not represented here. The
/// SigV4 path signs headers through the AWS SDK request model, and the final
/// transport uses cpr::Header; both are single-value, map-like containers that
/// fold duplicate names. Keeping the REST request model single-value avoids
/// exposing repeated-header behavior that cannot survive signing or transport.
using HttpHeaders = std::map<std::string, std::string, CaseInsensitiveHeaderLess>;

/// \brief An outgoing HTTP request. Mirrors Java's HttpRequest so signing
/// implementations like SigV4 see method, url, headers, and body together.
struct ICEBERG_REST_EXPORT HttpRequest {
  HttpMethod method = HttpMethod::kGet;
  std::string url;
  HttpHeaders headers;
  std::string body;
};

}  // namespace iceberg::rest
