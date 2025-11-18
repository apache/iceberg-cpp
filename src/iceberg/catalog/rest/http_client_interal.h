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
#include "iceberg/catalog/rest/http_response.h"
#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/result.h"

/// \file iceberg/catalog/rest/http_client.h
/// \brief Http client for Iceberg REST API.

namespace iceberg::rest {

/// \brief HTTP client for making requests to Iceberg REST Catalog API.
class ICEBERG_REST_EXPORT HttpClient {
 public:
  explicit HttpClient(const RestCatalogConfig&);

  HttpClient(const HttpClient&) = delete;
  HttpClient& operator=(const HttpClient&) = delete;
  HttpClient(HttpClient&&) = default;
  HttpClient& operator=(HttpClient&&) = default;

  /// \brief Sends a GET request.
  /// \param target The target path relative to the base URL (e.g., "/v1/namespaces").
  Result<HttpResponse> Get(const std::string& target, const cpr::Parameters& params = {},
                           const cpr::Header& headers = {});

  /// \brief Sends a POST request.
  /// \param target The target path relative to the base URL (e.g., "/v1/namespaces").
  Result<HttpResponse> Post(const std::string& target, const cpr::Body& body,
                            const cpr::Parameters& params = {},
                            const cpr::Header& headers = {});

  /// \brief Sends a HEAD request.
  /// \param target The target path relative to the base URL (e.g., "/v1/namespaces").
  Result<HttpResponse> Head(const std::string& target, const cpr::Parameters& params = {},
                            const cpr::Header& headers = {});

  /// \brief Sends a DELETE request.
  /// \param target The target path relative to the base URL (e.g., "/v
  Result<HttpResponse> Delete(const std::string& target,
                              const cpr::Parameters& params = {},
                              const cpr::Header& headers = {});

 private:
  /// \brief Internal helper to execute a request.
  template <typename Func>
  Result<HttpResponse> Execute(const std::string& target, const cpr::Parameters& params,
                               const cpr::Header& request_headers,
                               Func&& perform_request) {
    cpr::Header combined_headers = default_headers_;
    combined_headers.insert(request_headers.begin(), request_headers.end());

    session_->SetUrl(cpr::Url{target});
    session_->SetParameters(params);
    session_->SetHeader(combined_headers);

    cpr::Response response = perform_request(*session_);
    return HttpResponse{std::move(response)};
  }

  cpr::Header default_headers_;
  std::unique_ptr<cpr::Session> session_;
};

}  // namespace iceberg::rest
