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

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

#include "iceberg/catalog/rest/constant.h"
#include "iceberg/catalog/rest/http_request.h"
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
  HttpResponse(int32_t status_code, std::string body);
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

/// \brief Transport settings shared by the config, init and catalog clients.
///
/// Property names and defaults mirror the Java `HTTPClient` (`rest.client.*`).
struct ICEBERG_REST_EXPORT HttpClientOptions {
  /// `rest.client.connection-timeout-ms`; Java defaults to three minutes.
  /// Zero disables the configured connection deadline.
  std::optional<int64_t> connection_timeout_ms = kDefaultConnectionTimeoutMs;
  /// `rest.client.socket-timeout-ms`, the maximum transfer inactivity.
  /// Unset or zero leaves it unbounded.
  std::optional<int32_t> socket_timeout_ms;
  /// `rest.client.max-retries`, the maximum number of retries per request.
  int max_retries = 5;
  /// `rest.client.user-agent`; unset sends `iceberg-cpp/<version>`.
  std::optional<std::string> user_agent;
  /// `rest.client.idempotency-key-lifetime`, parsed from an ISO-8601 duration.
  /// Unset disables the bound; zero disables retries of keyed requests.
  std::optional<std::chrono::milliseconds> idempotency_key_lifetime;

  static Result<HttpClientOptions> FromProperties(
      const std::unordered_map<std::string, std::string>& properties);
};

/// \brief HTTP client for making requests to Iceberg REST Catalog API.
class ICEBERG_REST_EXPORT HttpClient {
 public:
  /// \brief Create a client using Java-style transport options.
  static std::shared_ptr<HttpClient> Make(
      std::unordered_map<std::string, std::string> default_headers = {},
      HttpClientOptions options = {}, std::optional<std::string> base_uri = std::nullopt);
  /// \brief Construct with a connection timeout and a legacy total request timeout.
  /// \param connection_timeout,socket_timeout Zero leaves requests unbounded.
  explicit HttpClient(std::unordered_map<std::string, std::string> default_headers = {},
                      std::chrono::milliseconds connection_timeout =
                          std::chrono::milliseconds(kDefaultConnectionTimeoutMs),
                      std::chrono::milliseconds socket_timeout = {},
                      std::optional<std::string> base_uri = std::nullopt);
  virtual ~HttpClient();

  HttpClient(const HttpClient&) = delete;
  HttpClient& operator=(const HttpClient&) = delete;
  HttpClient(HttpClient&&) = delete;
  HttpClient& operator=(HttpClient&&) = delete;

  /// \brief Sends a GET request.
  virtual Result<HttpResponse> Get(
      const std::string& path, const std::unordered_map<std::string, std::string>& params,
      const std::unordered_map<std::string, std::string>& headers,
      const ErrorHandler& error_handler, auth::AuthSession& session);

  /// \brief Sends a POST request.
  virtual Result<HttpResponse> Post(
      const std::string& path, const std::string& body,
      const std::unordered_map<std::string, std::string>& headers,
      const ErrorHandler& error_handler, auth::AuthSession& session);

  /// \brief Sends a POST request with form data.
  virtual Result<HttpResponse> PostForm(
      const std::string& path,
      const std::unordered_map<std::string, std::string>& form_data,
      const std::unordered_map<std::string, std::string>& headers,
      const ErrorHandler& error_handler, auth::AuthSession& session);

  /// \brief Sends a HEAD request.
  virtual Result<HttpResponse> Head(
      const std::string& path,
      const std::unordered_map<std::string, std::string>& headers,
      const ErrorHandler& error_handler, auth::AuthSession& session);

  /// \brief Sends a DELETE request.
  virtual Result<HttpResponse> Delete(
      const std::string& path, const std::unordered_map<std::string, std::string>& params,
      const std::unordered_map<std::string, std::string>& headers,
      const ErrorHandler& error_handler, auth::AuthSession& session);

 private:
  struct WithOptions {};
  HttpClient(WithOptions, std::unordered_map<std::string, std::string> default_headers,
             HttpClientOptions options, std::chrono::milliseconds request_timeout,
             std::optional<std::string> base_uri);

  /// \brief Build a request URI from this client's base URI and a path.
  Result<std::string> RequestUri(const std::string& path) const;

  /// \brief Authenticate once, then send and retry the authenticated request.
  Result<HttpResponse> Execute(HttpMethod method, const std::string& url,
                               HttpHeaders headers, std::string body,
                               const ErrorHandler& error_handler,
                               auth::AuthSession& session);

  std::optional<std::string> base_uri_;
  HttpClientOptions options_;
  HttpHeaders default_headers_;
  std::chrono::milliseconds request_timeout_{};
  std::unique_ptr<cpr::ConnectionPool> connection_pool_;
};

}  // namespace iceberg::rest
