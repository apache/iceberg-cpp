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
#include <unordered_map>

#include <cpr/cpr.h>
#include <nlohmann/json.hpp>

#include "iceberg/catalog/rest/catalog_properties.h"
#include "iceberg/catalog/rest/error_handlers.h"
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
  HttpClient(HttpClient&&) = delete;
  HttpClient& operator=(HttpClient&&) = delete;

  /// \brief Sends a GET request.
  Result<HttpResponse> Get(const std::string& path,
                           const std::unordered_map<std::string, std::string>& params,
                           const std::unordered_map<std::string, std::string>& headers,
                           const ErrorHandler& error_handler);

  /// \brief Sends a POST request.
  Result<HttpResponse> Post(const std::string& path, const std::string& body,
                            const std::unordered_map<std::string, std::string>& headers,
                            const ErrorHandler& error_handler);

  /// \brief Sends a POST request with form data.
  Result<HttpResponse> PostForm(
      const std::string& path,
      const std::unordered_map<std::string, std::string>& form_data,
      const std::unordered_map<std::string, std::string>& headers,
      const ErrorHandler& error_handler);

  /// \brief Sends a HEAD request.
  Result<HttpResponse> Head(const std::string& path,
                            const std::unordered_map<std::string, std::string>& headers,
                            const ErrorHandler& error_handler);

  /// \brief Sends a DELETE request.
  Result<HttpResponse> Delete(const std::string& path,
                              const std::unordered_map<std::string, std::string>& headers,
                              const ErrorHandler& error_handler);

 private:
  void PrepareSession(const std::string& path,
                      const std::unordered_map<std::string, std::string>& request_headers,
                      const std::unordered_map<std::string, std::string>& params = {});

  std::unordered_map<std::string, std::string> default_headers_;

  // Mutex to protect the non-thread-safe cpr::Session.
  mutable std::mutex session_mutex_;

  // TODO(Li Feiyang): use connection pool to support external multi-threaded concurrent
  // calls
  std::unique_ptr<cpr::Session> session_;
};

}  // namespace iceberg::rest
