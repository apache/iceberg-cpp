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
#include <string_view>
#include <unordered_map>

#include "iceberg/catalog/rest/iceberg_rest_export.h"

namespace iceberg::rest {

/// \brief HTTP method enumeration.
enum class HttpMethod : uint8_t { kGet, kPost, kPut, kDelete, kHead };

/// \brief Convert HttpMethod to string representation.
constexpr std::string_view ToString(HttpMethod method);

/// \brief An outgoing HTTP request. Mirrors Java's HttpRequest so signing
/// implementations like SigV4 see method, url, headers, and body together.
struct ICEBERG_REST_EXPORT HttpRequest {
  HttpMethod method = HttpMethod::kGet;
  std::string url;
  std::unordered_map<std::string, std::string> headers;
  std::string body;
};

}  // namespace iceberg::rest
