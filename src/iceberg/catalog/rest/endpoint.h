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
enum class HttpMethod : uint8_t { GET, POST, PUT, DELETE, HEAD };

/// \brief Convert HttpMethod to string representation.
constexpr std::string_view ToString(HttpMethod method);

/// \brief An Endpoint is an immutable value object identifying a specific REST API
/// operation. It consists of:
/// - HTTP method (GET, POST, DELETE, etc.)
/// - Path template (e.g., "/v1/{prefix}/namespaces/{namespace}")
class ICEBERG_REST_EXPORT Endpoint {
 public:
  /// \brief Create an endpoint with method and path template.
  ///
  /// \param method HTTP method (GET, POST, etc.)
  /// \param path_template Path template with placeholders (e.g., "/v1/{prefix}/tables")
  /// \return Endpoint instance or error if invalid
  static Result<Endpoint> Create(HttpMethod method, std::string path_template);

  /// \brief Parse endpoint from string representation.
  ///
  /// \param str String in format "METHOD /path/template" (e.g., "GET /v1/namespaces")
  /// \return Endpoint instance or error if malformed
  static Result<Endpoint> FromString(std::string_view str);

  /// \brief Get the HTTP method.
  constexpr HttpMethod method() const { return method_; }

  /// \brief Get the path template.
  std::string_view path_template() const { return path_template_; }

  /// \brief Serialize to "METHOD /path" format.
  std::string ToString() const;

  /// \brief Equality comparison operator.
  constexpr bool operator==(const Endpoint& other) const {
    return method_ == other.method_ && path_template_ == other.path_template_;
  }

  /// \brief Three-way comparison operator (C++20).
  constexpr auto operator<=>(const Endpoint& other) const {
    if (auto cmp = method_ <=> other.method_; cmp != 0) {
      return cmp;
    }
    return path_template_ <=> other.path_template_;
  }

  // Namespace endpoints
  static Endpoint ListNamespaces() {
    return {HttpMethod::GET, "/v1/{prefix}/namespaces"};
  }
  static Endpoint GetNamespaceProperties() {
    return {HttpMethod::GET, "/v1/{prefix}/namespaces/{namespace}"};
  }
  static Endpoint NamespaceExists() {
    return {HttpMethod::HEAD, "/v1/{prefix}/namespaces/{namespace}"};
  }
  static Endpoint CreateNamespace() {
    return {HttpMethod::POST, "/v1/{prefix}/namespaces"};
  }
  static Endpoint UpdateNamespace() {
    return {HttpMethod::POST, "/v1/{prefix}/namespaces/{namespace}/properties"};
  }
  static Endpoint DropNamespace() {
    return {HttpMethod::DELETE, "/v1/{prefix}/namespaces/{namespace}"};
  }

  // Table endpoints
  static Endpoint ListTables() {
    return {HttpMethod::GET, "/v1/{prefix}/namespaces/{namespace}/tables"};
  }
  static Endpoint LoadTable() {
    return {HttpMethod::GET, "/v1/{prefix}/namespaces/{namespace}/tables/{table}"};
  }
  static Endpoint TableExists() {
    return {HttpMethod::HEAD, "/v1/{prefix}/namespaces/{namespace}/tables/{table}"};
  }
  static Endpoint CreateTable() {
    return {HttpMethod::POST, "/v1/{prefix}/namespaces/{namespace}/tables"};
  }
  static Endpoint UpdateTable() {
    return {HttpMethod::POST, "/v1/{prefix}/namespaces/{namespace}/tables/{table}"};
  }
  static Endpoint DeleteTable() {
    return {HttpMethod::DELETE, "/v1/{prefix}/namespaces/{namespace}/tables/{table}"};
  }
  static Endpoint RenameTable() {
    return {HttpMethod::POST, "/v1/{prefix}/tables/rename"};
  }
  static Endpoint RegisterTable() {
    return {HttpMethod::POST, "/v1/{prefix}/namespaces/{namespace}/register"};
  }
  static Endpoint ReportMetrics() {
    return {HttpMethod::POST,
            "/v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics"};
  }
  static Endpoint TableCredentials() {
    return {HttpMethod::GET,
            "/v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials"};
  }

  // Transaction endpoints
  static Endpoint CommitTransaction() {
    return {HttpMethod::POST, "/v1/{prefix}/transactions/commit"};
  }

 private:
  Endpoint(HttpMethod method, std::string_view path_template)
      : method_(method), path_template_(path_template) {}

  HttpMethod method_;
  std::string path_template_;
};

}  // namespace iceberg::rest
