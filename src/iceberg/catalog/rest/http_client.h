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
#include <memory>
#include <string>
#include <unordered_map>

#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/catalog/rest/type_fwd.h"
#include "iceberg/result.h"

/// \file iceberg/catalog/rest/http_client.h
/// \brief Http client for Iceberg REST API.

namespace cpr {
class ConnectionPool;
}  // namespace cpr

namespace iceberg::rest {

/// \brief A simple wrapper for cpr::Response.
///
/// This class encapsulates the details of the underlying cpr library's response,
/// providing a consistent interface that is independent of the specific network
/// library used.
class ICEBERG_REST_EXPORT HttpResponse {
 public:
  HttpResponse();
  ~HttpResponse();

  HttpResponse(const HttpResponse&) = delete;
  HttpResponse& operator=(const HttpResponse&) = delete;
  HttpResponse(HttpResponse&&) noexcept;
  HttpResponse& operator=(HttpResponse&&) noexcept;

  /// \brief Get the HTTP status code of the response.
  int32_t status_code() const;

  /// \brief Get the body of the response as a string.
  std::string body() const;

  /// \brief Get the headers of the response as a map.
  std::unordered_map<std::string, std::string> headers() const;

 private:
  friend class HttpClient;
  class Impl;
  std::unique_ptr<Impl> impl_;
};

/// \brief HTTP client for making requests to Iceberg REST Catalog API.
class ICEBERG_REST_EXPORT HttpClient {
 public:
  explicit HttpClient(std::unordered_map<std::string, std::string> default_headers = {});
  ~HttpClient();

  HttpClient(const HttpClient&) = delete;
  HttpClient& operator=(const HttpClient&) = delete;
  HttpClient(HttpClient&&) = delete;
  HttpClient& operator=(HttpClient&&) = delete;

  /// \brief Sends a GET request.
  Result<HttpResponse> Get(const std::string& path,
                           const std::unordered_map<std::string, std::string>& params,
                           const std::unordered_map<std::string, std::string>& headers,
                           const ErrorHandler& error_handler, auth::AuthSession& session);

  /// \brief Sends a POST request.
  Result<HttpResponse> Post(const std::string& path, const std::string& body,
                            const std::unordered_map<std::string, std::string>& headers,
                            const ErrorHandler& error_handler,
                            auth::AuthSession& session);

  /// \brief Sends a POST request with form data.
  Result<HttpResponse> PostForm(
      const std::string& path,
      const std::unordered_map<std::string, std::string>& form_data,
      const std::unordered_map<std::string, std::string>& headers,
      const ErrorHandler& error_handler, auth::AuthSession& session);

  /// \brief Sends a HEAD request.
  Result<HttpResponse> Head(const std::string& path,
                            const std::unordered_map<std::string, std::string>& headers,
                            const ErrorHandler& error_handler,
                            auth::AuthSession& session);

  /// \brief Sends a DELETE request.
  Result<HttpResponse> Delete(const std::string& path,
                              const std::unordered_map<std::string, std::string>& params,
                              const std::unordered_map<std::string, std::string>& headers,
                              const ErrorHandler& error_handler,
                              auth::AuthSession& session);

 private:
  std::unordered_map<std::string, std::string> default_headers_;
  std::unique_ptr<cpr::ConnectionPool> connection_pool_;
};

}  // namespace iceberg::rest
