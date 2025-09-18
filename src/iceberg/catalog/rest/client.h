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

#include <map>
#include <memory>
#include <stdexcept>
#include <string>

#include <nlohmann/json.hpp>

#include "iceberg/iceberg_export.h"

namespace iceberg::catalog::rest {

using HttpHeaders = std::map<std::string, std::string>;

/**
 * @brief Base exception for REST client errors.
 */
class ICEBERG_EXPORT RestException : public std::runtime_error {
 public:
  explicit RestException(const std::string& message) : std::runtime_error(message) {}
};

/**
 * @brief Exception thrown when the server returns an error response.
 *
 * This corresponds to the generic `E` type in the Rust implementation,
 * which is deserialized from the error response body.
 */
class ICEBERG_EXPORT ServerErrorException : public RestException {
 public:
  ServerErrorException(int status_code, std::string response_body,
                       nlohmann::json error_payload)
      : RestException("Server returned an error"),
        status_code_(status_code),
        response_body_(std::move(response_body)),
        error_payload_(std::move(error_payload)) {}

  int status_code() const { return status_code_; }
  const std::string& response_body() const { return response_body_; }
  const nlohmann::json& error_payload() const { return error_payload_; }

 private:
  int status_code_;
  std::string response_body_;
  nlohmann::json error_payload_;
};

/**
 * @brief Exception thrown when parsing the response body (either success or error) fails.
 *
 * This corresponds to the `Error::new(ErrorKind::Unexpected, ...)` part in Rust.
 */
class ICEBERG_EXPORT ResponseParseException : public RestException {
 public:
  ResponseParseException(std::string message, std::string response_body)
      : RestException(std::move(message)), response_body_(std::move(response_body)) {}

  const std::string& response_body() const { return response_body_; }

 private:
  std::string response_body_;
};

class ICEBERG_EXPORT HttpClient {
 public:
  HttpClient(const std::string& base_uri, const HttpHeaders& common_headers);
  ~HttpClient();

  /**
   * @brief Performs a GET request, expecting a JSON response on success.
   * @param path The request path, relative to the base URI.
   * @param headers Additional request-specific headers.
   * @return The parsed JSON body of the response.
   * @throws ServerErrorException if the server returns a non-200 status code.
   * @throws ResponseParseException if the response body cannot be parsed as JSON.
   */
  nlohmann::json Get(const std::string& path, const HttpHeaders& headers);

  /**
   * @brief Performs a POST request, expecting a JSON response on success.
   * @param path The request path.
   * @param headers Additional headers.
   * @param body The JSON body to send with the request.
   * @return The parsed JSON body of the response.
   * @throws ServerErrorException if the server returns a non-201 status code.
   * @throws ResponseParseException if the response body cannot be parsed as JSON.
   */
  nlohmann::json Post(const std::string& path, const HttpHeaders& headers,
                      const nlohmann::json& body);

  /**
   * @brief Performs a DELETE request, expecting no response body on success.
   * @param path The request path.
   * @param headers Additional headers.
   * @throws ServerErrorException if the server returns a non-204 status code.
   * @throws ResponseParseException if the error response body cannot be parsed as JSON.
   */
  void Delete(const std::string& path, const HttpHeaders& headers);

  // ... other HTTP methods can be added here following the same pattern.

 private:
  struct Impl;
  std::unique_ptr<Impl> pimpl_;
};

}  // namespace iceberg::catalog::rest
