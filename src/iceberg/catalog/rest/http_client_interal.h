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

#include <memory>
#include <mutex>
#include <string>

#include <cpr/cpr.h>
#include <nlohmann/json.hpp>

#include "iceberg/catalog/rest/config.h"
#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/result.h"

/// \file iceberg/catalog/rest/http_client.h
/// \brief Http client for Iceberg REST API.

namespace iceberg::rest {

/// \brief HTTP client for making requests to Iceberg REST Catalog API.
///
/// This class wraps the CPR library and provides a type-safe interface for making
/// HTTP requests. It handles authentication, headers, and response parsing.
class ICEBERG_REST_EXPORT HttpClient {
 public:
  /// \brief Factory function to create and initialize an HttpClient.
  /// This is the preferred way to construct an HttpClient, as it can handle
  /// potential errors during configuration parsing (e.g., invalid headers).
  /// \param config The catalog configuration.
  /// \return A Result containing a unique_ptr to the HttpClient, or an Error.
  static Result<std::unique_ptr<HttpClient>> Make(const RestCatalogConfig& config);

  HttpClient(const HttpClient&) = delete;
  HttpClient& operator=(const HttpClient&) = delete;
  HttpClient(HttpClient&&) = default;
  HttpClient& operator=(HttpClient&&) = default;

  /// \brief Sends a GET request.
  /// \param target The target path relative to the base URL (e.g., "/v1/namespaces").
  Result<cpr::Response> Get(const std::string& target, const cpr::Parameters& params = {},
                            const cpr::Header& headers = {});

  /// \brief Sends a POST request.
  /// \param target The target path relative to the base URL (e.g., "/v1/namespaces").
  Result<cpr::Response> Post(const std::string& target, const cpr::Body& body,
                             const cpr::Parameters& params = {},
                             const cpr::Header& headers = {});

  /// \brief Sends a HEAD request.
  /// \param target The target path relative to the base URL (e.g., "/v1/namespaces").
  Result<cpr::Response> Head(const std::string& target,
                             const cpr::Parameters& params = {},
                             const cpr::Header& headers = {});

  /// \brief Sends a DELETE request.
  /// \param target The target path relative to the base URL (e.g., "/v
  Result<cpr::Response> Delete(const std::string& target,
                               const cpr::Parameters& params = {},
                               const cpr::Header& headers = {});

 private:
  /// \brief Private constructor. Use the static Create() factory function instead.
  explicit HttpClient(cpr::Header session_headers);

  /// \brief Internal helper to execute a request.
  template <typename Func>
  Result<cpr::Response> Execute(const std::string& target, const cpr::Parameters& params,
                                const cpr::Header& request_headers,
                                Func&& perform_request);

  cpr::Header default_headers_;
  std::unique_ptr<cpr::Session> session_;
};

}  // namespace iceberg::rest
