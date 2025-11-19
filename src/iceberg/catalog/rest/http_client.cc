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

#include "iceberg/catalog/rest/http_client.h"

#include <nlohmann/json.hpp>

#include "cpr/body.h"
#include "cpr/cprtypes.h"
#include "iceberg/catalog/rest/config.h"
#include "iceberg/catalog/rest/json_internal.h"
#include "iceberg/json_internal.h"
#include "iceberg/util/macros.h"

namespace iceberg::rest {

namespace {

/// \brief Merges global default headers with request-specific headers.
///
/// Combines the global headers derived from RestCatalogConfig with the headers
/// passed in the specific request. Request-specific headers have higher priority
/// and will override global defaults if the keys conflict (e.g., overriding
/// the default "Content-Type").
cpr::Header MergeHeaders(const std::unordered_map<std::string, std::string>& defaults,
                         const std::unordered_map<std::string, std::string>& overrides) {
  cpr::Header combined_headers = {defaults.begin(), defaults.end()};
  for (const auto& [key, val] : overrides) {
    combined_headers.insert_or_assign(key, val);
  }
  return combined_headers;
}

/// \brief Converts a map of string key-value pairs to cpr::Parameters.
cpr::Parameters GetParameters(
    const std::unordered_map<std::string, std::string>& params) {
  cpr::Parameters cpr_params;
  for (const auto& [key, val] : params) {
    cpr_params.Add({key, val});
  }
  return cpr_params;
}

bool IsSuccessful(int32_t status_code) {
  return status_code == 200      // OK
         || status_code == 202   // Accepted
         || status_code == 204   // No Content
         || status_code == 304;  // Not Modified
}

Status HandleFailureResponse(const cpr::Response& response,
                             const ErrorHandler& error_handler) {
  if (!IsSuccessful(response.status_code)) {
    ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.text));
    ICEBERG_ASSIGN_OR_RAISE(auto error_response, ErrorResponseFromJson(json));
    return error_handler.Accept(error_response.error);
  }
  return {};
}

}  // namespace

void HttpClient::PrepareSession(
    const std::string& path,
    const std::unordered_map<std::string, std::string>& request_headers,
    const std::unordered_map<std::string, std::string>& params) {
  session_->SetUrl(cpr::Url{path});
  session_->SetParameters(GetParameters(params));
  session_->RemoveContent();
  auto final_headers = MergeHeaders(default_headers_, request_headers);
  session_->SetHeader(final_headers);
}

HttpClient::HttpClient(const RestCatalogConfig& config)
    : default_headers_{config.GetExtraHeaders()},
      session_{std::make_unique<cpr::Session>()} {}

Result<HttpResponse> HttpClient::Get(
    const std::string& path, const std::unordered_map<std::string, std::string>& params,
    const std::unordered_map<std::string, std::string>& headers,
    const ErrorHandler& error_handler) {
  std::lock_guard<std::mutex> lock(session_mutex_);

  PrepareSession(path, headers, params);
  cpr::Response response = session_->Get();
  ICEBERG_RETURN_UNEXPECTED(HandleFailureResponse(response, error_handler));
  return HttpResponse(std::move(response));
}

Result<HttpResponse> HttpClient::Post(
    const std::string& path, const std::string& body,
    const std::unordered_map<std::string, std::string>& headers,
    const ErrorHandler& error_handler) {
  std::lock_guard<std::mutex> lock(session_mutex_);

  PrepareSession(path, headers);
  session_->SetBody(cpr::Body{body});
  cpr::Response response = session_->Post();
  ICEBERG_RETURN_UNEXPECTED(HandleFailureResponse(response, error_handler));
  return HttpResponse(std::move(response));
}

Result<HttpResponse> HttpClient::PostForm(
    const std::string& path,
    const std::unordered_map<std::string, std::string>& form_data,
    const std::unordered_map<std::string, std::string>& headers,
    const ErrorHandler& error_handler) {
  std::lock_guard<std::mutex> lock(session_mutex_);

  PrepareSession(path, headers);
  std::vector<cpr::Pair> pair_list;
  pair_list.reserve(form_data.size());
  for (const auto& [key, val] : form_data) {
    pair_list.emplace_back(key, val);
  }
  session_->SetPayload(cpr::Payload(pair_list.begin(), pair_list.end()));
  cpr::Response response = session_->Post();
  ICEBERG_RETURN_UNEXPECTED(HandleFailureResponse(response, error_handler));
  return HttpResponse(std::move(response));
}

Result<HttpResponse> HttpClient::Head(
    const std::string& path, const std::unordered_map<std::string, std::string>& headers,
    const ErrorHandler& error_handler) {
  std::lock_guard<std::mutex> lock(session_mutex_);

  PrepareSession(path, headers);
  cpr::Response response = session_->Head();
  ICEBERG_RETURN_UNEXPECTED(HandleFailureResponse(response, error_handler));
  return HttpResponse(std::move(response));
}

Result<HttpResponse> HttpClient::Delete(
    const std::string& path, const std::unordered_map<std::string, std::string>& headers,
    const ErrorHandler& error_handler) {
  std::lock_guard<std::mutex> lock(session_mutex_);

  PrepareSession(path, headers);
  cpr::Response response = session_->Delete();
  ICEBERG_RETURN_UNEXPECTED(HandleFailureResponse(response, error_handler));
  return HttpResponse(std::move(response));
}

}  // namespace iceberg::rest
