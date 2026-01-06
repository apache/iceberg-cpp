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

#include <cpr/cpr.h>
#include <nlohmann/json.hpp>

#include "iceberg/catalog/rest/constant.h"
#include "iceberg/catalog/rest/error_handlers.h"
#include "iceberg/catalog/rest/json_internal.h"
#include "iceberg/catalog/rest/rest_util.h"
#include "iceberg/json_internal.h"
#include "iceberg/result.h"
#include "iceberg/util/macros.h"

namespace iceberg::rest {

class HttpResponse::Impl {
 public:
  explicit Impl(cpr::Response&& response) : response_(std::move(response)) {}
  ~Impl() = default;

  int32_t status_code() const { return static_cast<int32_t>(response_.status_code); }

  std::string body() const { return response_.text; }

  std::unordered_map<std::string, std::string> headers() const {
    return {response_.header.begin(), response_.header.end()};
  }

 private:
  cpr::Response response_;
};

HttpResponse::HttpResponse() = default;
HttpResponse::~HttpResponse() = default;
HttpResponse::HttpResponse(HttpResponse&&) noexcept = default;
HttpResponse& HttpResponse::operator=(HttpResponse&&) noexcept = default;

int32_t HttpResponse::status_code() const { return impl_->status_code(); }

std::string HttpResponse::body() const { return impl_->body(); }

std::unordered_map<std::string, std::string> HttpResponse::headers() const {
  return impl_->headers();
}

namespace {

/// \brief Default error type for unparseable REST responses.
constexpr std::string_view kRestExceptionType = "RESTException";

/// \brief Merges global default headers with request-specific headers.
///
/// Combines the global headers derived from RestCatalogProperties with the headers
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

/// \brief Checks if the HTTP status code indicates a successful response.
bool IsSuccessful(int32_t status_code) {
  return status_code == 200      // OK
         || status_code == 202   // Accepted
         || status_code == 204   // No Content
         || status_code == 304;  // Not Modified
}

/// \brief Builds a default ErrorResponse when the response body cannot be parsed.
ErrorResponse BuildDefaultErrorResponse(const cpr::Response& response) {
  return {
      .code = static_cast<uint32_t>(response.status_code),
      .type = std::string(kRestExceptionType),
      .message = !response.reason.empty() ? response.reason
                                          : GetStandardReasonPhrase(response.status_code),
  };
}

/// \brief Tries to parse the response body as an ErrorResponse.
Result<ErrorResponse> TryParseErrorResponse(const std::string& text) {
  if (text.empty()) {
    return InvalidArgument("Empty response body");
  }
  ICEBERG_ASSIGN_OR_RAISE(auto json_result, FromJsonString(text));
  ICEBERG_ASSIGN_OR_RAISE(auto error_result, ErrorResponseFromJson(json_result));
  return error_result;
}

/// \brief Handles failure responses by invoking the provided error handler.
Status HandleFailureResponse(const cpr::Response& response,
                             const ErrorHandler& error_handler) {
  if (IsSuccessful(response.status_code)) {
    return {};
  }
  auto parse_result = TryParseErrorResponse(response.text);
  const ErrorResponse final_error =
      parse_result.value_or(BuildDefaultErrorResponse(response));
  return error_handler.Accept(final_error);
}

}  // namespace

void HttpClient::PrepareSession(
    const std::string& path, HttpMethod method,
    const std::unordered_map<std::string, std::string>& params,
    const std::unordered_map<std::string, std::string>& headers) {
  session_->SetUrl(cpr::Url{path});
  session_->SetParameters(GetParameters(params));
  session_->RemoveContent();
  // clear lingering POST mode state from prior requests. CURLOPT_POST is implicitly set
  // to 1 by POST requests, and this state is not reset by RemoveContent(), so we must
  // manually enforce HTTP GET to clear it.
  curl_easy_setopt(session_->GetCurlHolder()->handle, CURLOPT_HTTPGET, 1L);
  switch (method) {
    case HttpMethod::kGet:
      session_->PrepareGet();
      break;
    case HttpMethod::kPost:
      session_->PreparePost();
      break;
    case HttpMethod::kPut:
      session_->PreparePut();
      break;
    case HttpMethod::kDelete:
      session_->PrepareDelete();
      break;
    case HttpMethod::kHead:
      session_->PrepareHead();
      break;
  }
  auto final_headers = MergeHeaders(default_headers_, headers);
  session_->SetHeader(final_headers);
}

HttpClient::HttpClient(std::unordered_map<std::string, std::string> default_headers)
    : default_headers_{std::move(default_headers)},
      session_{std::make_unique<cpr::Session>()} {
  // Set default Content-Type for all requests (including GET/HEAD/DELETE).
  // Many systems require that content type is set regardless and will fail,
  // even on an empty bodied request.
  default_headers_[kHeaderContentType] = kMimeTypeApplicationJson;
  default_headers_[kHeaderUserAgent] = kUserAgent;
}

HttpClient::~HttpClient() = default;

Result<HttpResponse> HttpClient::Get(
    const std::string& path, const std::unordered_map<std::string, std::string>& params,
    const std::unordered_map<std::string, std::string>& headers,
    const ErrorHandler& error_handler) {
  cpr::Response response;
  {
    std::lock_guard guard(session_mutex_);
    PrepareSession(path, HttpMethod::kGet, params, headers);
    response = session_->Get();
  }

  ICEBERG_RETURN_UNEXPECTED(HandleFailureResponse(response, error_handler));
  HttpResponse http_response;
  http_response.impl_ = std::make_unique<HttpResponse::Impl>(std::move(response));
  return http_response;
}

Result<HttpResponse> HttpClient::Post(
    const std::string& path, const std::string& body,
    const std::unordered_map<std::string, std::string>& headers,
    const ErrorHandler& error_handler) {
  cpr::Response response;
  {
    std::lock_guard guard(session_mutex_);
    PrepareSession(path, HttpMethod::kPost, /*params=*/{}, headers);
    session_->SetBody(cpr::Body{body});
    response = session_->Post();
  }

  ICEBERG_RETURN_UNEXPECTED(HandleFailureResponse(response, error_handler));
  HttpResponse http_response;
  http_response.impl_ = std::make_unique<HttpResponse::Impl>(std::move(response));
  return http_response;
}

Result<HttpResponse> HttpClient::PostForm(
    const std::string& path,
    const std::unordered_map<std::string, std::string>& form_data,
    const std::unordered_map<std::string, std::string>& headers,
    const ErrorHandler& error_handler) {
  cpr::Response response;

  {
    std::lock_guard guard(session_mutex_);

    // Override default Content-Type (application/json) with form-urlencoded
    auto form_headers = headers;
    form_headers[kHeaderContentType] = kMimeTypeFormUrlEncoded;

    PrepareSession(path, HttpMethod::kPost, /*params=*/{}, form_headers);
    std::vector<cpr::Pair> pair_list;
    pair_list.reserve(form_data.size());
    for (const auto& [key, val] : form_data) {
      pair_list.emplace_back(key, val);
    }
    session_->SetPayload(cpr::Payload(pair_list.begin(), pair_list.end()));

    response = session_->Post();
  }

  ICEBERG_RETURN_UNEXPECTED(HandleFailureResponse(response, error_handler));
  HttpResponse http_response;
  http_response.impl_ = std::make_unique<HttpResponse::Impl>(std::move(response));
  return http_response;
}

Result<HttpResponse> HttpClient::Head(
    const std::string& path, const std::unordered_map<std::string, std::string>& headers,
    const ErrorHandler& error_handler) {
  cpr::Response response;
  {
    std::lock_guard guard(session_mutex_);
    PrepareSession(path, HttpMethod::kHead, /*params=*/{}, headers);
    response = session_->Head();
  }

  ICEBERG_RETURN_UNEXPECTED(HandleFailureResponse(response, error_handler));
  HttpResponse http_response;
  http_response.impl_ = std::make_unique<HttpResponse::Impl>(std::move(response));
  return http_response;
}

Result<HttpResponse> HttpClient::Delete(
    const std::string& path, const std::unordered_map<std::string, std::string>& params,
    const std::unordered_map<std::string, std::string>& headers,
    const ErrorHandler& error_handler) {
  cpr::Response response;
  {
    std::lock_guard guard(session_mutex_);
    PrepareSession(path, HttpMethod::kDelete, params, headers);
    response = session_->Delete();
  }

  ICEBERG_RETURN_UNEXPECTED(HandleFailureResponse(response, error_handler));
  HttpResponse http_response;
  http_response.impl_ = std::make_unique<HttpResponse::Impl>(std::move(response));
  return http_response;
}

}  // namespace iceberg::rest
