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

#include <string>
#include <string_view>

#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/result.h"

/// \file iceberg/catalog/rest/endpoint.h
/// Endpoint definitions for Iceberg REST API operations.

namespace iceberg::rest {

/// \brief HTTP method enumeration.
enum class HttpMethod : uint8_t { kGet, kPost, kPut, kDelete, kHead };

/// \brief Convert HttpMethod to string representation.
constexpr std::string_view ToString(HttpMethod method);

/// \brief An Endpoint is an immutable value object identifying a specific REST API
/// operation. It consists of:
/// - HTTP method (GET, POST, DELETE, etc.)
/// - Path template (e.g., "/v1/{prefix}/namespaces/{namespace}")
class ICEBERG_REST_EXPORT Endpoint {
 public:
  /// \brief Make an endpoint with method and path template.
  ///
  /// \param method HTTP method (GET, POST, etc.)
  /// \param path Path template with placeholders (e.g., "/v1/{prefix}/tables")
  /// \return Endpoint instance or error if invalid
  static Result<Endpoint> Make(HttpMethod method, std::string_view path);

  /// \brief Parse endpoint from string representation. "METHOD" have to be all
  /// upper-cased.
  ///
  /// \param str String in format "METHOD /path/template" (e.g., "GET /v1/namespaces")
  /// \return Endpoint instance or error if malformed.
  static Result<Endpoint> FromString(std::string_view str);

  /// \brief Get the HTTP method.
  constexpr HttpMethod method() const { return method_; }

  /// \brief Get the path template.
  std::string_view path() const { return path_; }

  /// \brief Serialize to "METHOD /path" format.
  std::string ToString() const;

  constexpr bool operator==(const Endpoint& other) const {
    return method_ == other.method_ && path_ == other.path_;
  }

  // Namespace endpoints
  static Endpoint ListNamespaces() {
    return {HttpMethod::kGet, "/v1/{prefix}/namespaces"};
  }
  static Endpoint GetNamespaceProperties() {
    return {HttpMethod::kGet, "/v1/{prefix}/namespaces/{namespace}"};
  }
  static Endpoint NamespaceExists() {
    return {HttpMethod::kHead, "/v1/{prefix}/namespaces/{namespace}"};
  }
  static Endpoint CreateNamespace() {
    return {HttpMethod::kPost, "/v1/{prefix}/namespaces"};
  }
  static Endpoint UpdateNamespace() {
    return {HttpMethod::kPost, "/v1/{prefix}/namespaces/{namespace}/properties"};
  }
  static Endpoint DropNamespace() {
    return {HttpMethod::kDelete, "/v1/{prefix}/namespaces/{namespace}"};
  }

  // Table endpoints
  static Endpoint ListTables() {
    return {HttpMethod::kGet, "/v1/{prefix}/namespaces/{namespace}/tables"};
  }
  static Endpoint LoadTable() {
    return {HttpMethod::kGet, "/v1/{prefix}/namespaces/{namespace}/tables/{table}"};
  }
  static Endpoint TableExists() {
    return {HttpMethod::kHead, "/v1/{prefix}/namespaces/{namespace}/tables/{table}"};
  }
  static Endpoint CreateTable() {
    return {HttpMethod::kPost, "/v1/{prefix}/namespaces/{namespace}/tables"};
  }
  static Endpoint UpdateTable() {
    return {HttpMethod::kPost, "/v1/{prefix}/namespaces/{namespace}/tables/{table}"};
  }
  static Endpoint DeleteTable() {
    return {HttpMethod::kDelete, "/v1/{prefix}/namespaces/{namespace}/tables/{table}"};
  }
  static Endpoint RenameTable() {
    return {HttpMethod::kPost, "/v1/{prefix}/tables/rename"};
  }
  static Endpoint RegisterTable() {
    return {HttpMethod::kPost, "/v1/{prefix}/namespaces/{namespace}/register"};
  }
  static Endpoint ReportMetrics() {
    return {HttpMethod::kPost,
            "/v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics"};
  }
  static Endpoint TableCredentials() {
    return {HttpMethod::kGet,
            "/v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials"};
  }

  // Transaction endpoints
  static Endpoint CommitTransaction() {
    return {HttpMethod::kPost, "/v1/{prefix}/transactions/commit"};
  }

 private:
  Endpoint(HttpMethod method, std::string_view path) : method_(method), path_(path) {}

  HttpMethod method_;
  std::string path_;
};

}  // namespace iceberg::rest

// Specialize std::hash for Endpoint
namespace std {
template <>
struct hash<iceberg::rest::Endpoint> {
  std::size_t operator()(const iceberg::rest::Endpoint& endpoint) const noexcept {
    std::size_t h1 = std::hash<int32_t>{}(static_cast<int32_t>(endpoint.method()));
    std::size_t h2 = std::hash<std::string_view>{}(endpoint.path());
    return h1 ^ (h2 + 0x9e3779b9 + (h1 << 6) + (h1 >> 2));
  }
};
}  // namespace std
