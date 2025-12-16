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

#include "iceberg/catalog/rest/endpoint.h"

#include <format>
#include <string_view>

namespace iceberg::rest {

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

Result<Endpoint> Endpoint::Make(HttpMethod method, std::string_view path) {
  if (path.empty()) {
    return InvalidArgument("Endpoint cannot have empty path");
  }
  return Endpoint(method, path);
}

Result<Endpoint> Endpoint::FromString(std::string_view str) {
  auto space_pos = str.find(' ');
  if (space_pos == std::string_view::npos ||
      str.find(' ', space_pos + 1) != std::string_view::npos) {
    return InvalidArgument(
        "Invalid endpoint format (must consist of two elements separated by a single "
        "space): '{}'",
        str);
  }

  auto method_str = str.substr(0, space_pos);
  auto path_str = str.substr(space_pos + 1);

  if (path_str.empty()) {
    return InvalidArgument("Invalid endpoint format: path is empty");
  }

  // Parse HTTP method
  HttpMethod method;
  if (method_str == "GET") {
    method = HttpMethod::kGet;
  } else if (method_str == "POST") {
    method = HttpMethod::kPost;
  } else if (method_str == "PUT") {
    method = HttpMethod::kPut;
  } else if (method_str == "DELETE") {
    method = HttpMethod::kDelete;
  } else if (method_str == "HEAD") {
    method = HttpMethod::kHead;
  } else {
    return InvalidArgument("Invalid HTTP method: '{}'", method_str);
  }

  return Make(method, std::string(path_str));
}

std::string Endpoint::ToString() const {
  return std::format("{} {}", rest::ToString(method_), path_);
}

}  // namespace iceberg::rest
