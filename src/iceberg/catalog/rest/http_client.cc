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

#include <map>

#include <cpr/cpr.h>
#include <nlohmann/json.hpp>

#include "iceberg/catalog/rest/auth/auth_session.h"
#include "iceberg/catalog/rest/constant.h"
#include "iceberg/catalog/rest/error_handlers.h"
#include "iceberg/catalog/rest/json_serde_internal.h"
#include "iceberg/catalog/rest/rest_util.h"
#include "iceberg/json_serde_internal.h"
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

/// \brief Merge default headers with per-request headers (per-request wins).
std::unordered_map<std::string, std::string> MergeHeaders(
    const std::unordered_map<std::string, std::string>& default_headers,
    const std::unordered_map<std::string, std::string>& request_headers) {
  std::unordered_map<std::string, std::string> merged(default_headers);
  for (const auto& [key, val] : request_headers) {
    merged.insert_or_assign(key, val);
  }
  return merged;
}

cpr::Header ToCprHeader(const auth::HTTPRequest& request) {
  return {request.headers.begin(), request.headers.end()};
}

/// \brief Append URL-encoded query parameters to a URL, sorted by key.
Result<std::string> AppendQueryString(
    const std::string& base_url,
    const std::unordered_map<std::string, std::string>& params) {
  if (params.empty()) return base_url;
  std::map<std::string, std::string> sorted(params.begin(), params.end());
  std::string url = base_url + "?";
  bool first = true;
  for (const auto& [k, v] : sorted) {
    if (!first) url += "&";
    ICEBERG_ASSIGN_OR_RAISE(auto ek, EncodeString(k));
    ICEBERG_ASSIGN_OR_RAISE(auto ev, EncodeString(v));
    url += ek + "=" + ev;
    first = false;
  }
  return url;
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

HttpClient::HttpClient(std::unordered_map<std::string, std::string> default_headers)
    : default_headers_{std::move(default_headers)},
      connection_pool_{std::make_unique<cpr::ConnectionPool>()} {
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
    const ErrorHandler& error_handler, auth::AuthSession& session) {
  ICEBERG_ASSIGN_OR_RAISE(auto url, AppendQueryString(path, params));
  ICEBERG_ASSIGN_OR_RAISE(
      auto authenticated,
      session.Authenticate({.method = HttpMethod::kGet,
                            .url = std::move(url),
                            .headers = MergeHeaders(default_headers_, headers),
                            .body = ""}));
  cpr::Response response = cpr::Get(cpr::Url{authenticated.url},
                                    ToCprHeader(authenticated), *connection_pool_);

  ICEBERG_RETURN_UNEXPECTED(HandleFailureResponse(response, error_handler));
  HttpResponse http_response;
  http_response.impl_ = std::make_unique<HttpResponse::Impl>(std::move(response));
  return http_response;
}

Result<HttpResponse> HttpClient::Post(
    const std::string& path, const std::string& body,
    const std::unordered_map<std::string, std::string>& headers,
    const ErrorHandler& error_handler, auth::AuthSession& session) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto authenticated,
      session.Authenticate({.method = HttpMethod::kPost,
                            .url = path,
                            .headers = MergeHeaders(default_headers_, headers),
                            .body = body}));
  cpr::Response response =
      cpr::Post(cpr::Url{authenticated.url}, cpr::Body{authenticated.body},
                ToCprHeader(authenticated), *connection_pool_);

  ICEBERG_RETURN_UNEXPECTED(HandleFailureResponse(response, error_handler));
  HttpResponse http_response;
  http_response.impl_ = std::make_unique<HttpResponse::Impl>(std::move(response));
  return http_response;
}

Result<HttpResponse> HttpClient::PostForm(
    const std::string& path,
    const std::unordered_map<std::string, std::string>& form_data,
    const std::unordered_map<std::string, std::string>& headers,
    const ErrorHandler& error_handler, auth::AuthSession& session) {
  std::unordered_map<std::string, std::string> form_headers(headers);
  form_headers.insert_or_assign(kHeaderContentType, kMimeTypeFormUrlEncoded);
  std::vector<cpr::Pair> pair_list;
  pair_list.reserve(form_data.size());
  for (const auto& [key, val] : form_data) {
    pair_list.emplace_back(key, val);
  }
  // Sign the exact bytes cpr will put on the wire.
  std::string encoded_body =
      cpr::Payload(pair_list.begin(), pair_list.end()).GetContent();
  ICEBERG_ASSIGN_OR_RAISE(
      auto authenticated,
      session.Authenticate({.method = HttpMethod::kPost,
                            .url = path,
                            .headers = MergeHeaders(default_headers_, form_headers),
                            .body = std::move(encoded_body)}));
  cpr::Response response =
      cpr::Post(cpr::Url{authenticated.url}, cpr::Body{authenticated.body},
                ToCprHeader(authenticated), *connection_pool_);

  ICEBERG_RETURN_UNEXPECTED(HandleFailureResponse(response, error_handler));
  HttpResponse http_response;
  http_response.impl_ = std::make_unique<HttpResponse::Impl>(std::move(response));
  return http_response;
}

Result<HttpResponse> HttpClient::Head(
    const std::string& path, const std::unordered_map<std::string, std::string>& headers,
    const ErrorHandler& error_handler, auth::AuthSession& session) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto authenticated,
      session.Authenticate({.method = HttpMethod::kHead,
                            .url = path,
                            .headers = MergeHeaders(default_headers_, headers),
                            .body = ""}));
  cpr::Response response = cpr::Head(cpr::Url{authenticated.url},
                                     ToCprHeader(authenticated), *connection_pool_);

  ICEBERG_RETURN_UNEXPECTED(HandleFailureResponse(response, error_handler));
  HttpResponse http_response;
  http_response.impl_ = std::make_unique<HttpResponse::Impl>(std::move(response));
  return http_response;
}

Result<HttpResponse> HttpClient::Delete(
    const std::string& path, const std::unordered_map<std::string, std::string>& params,
    const std::unordered_map<std::string, std::string>& headers,
    const ErrorHandler& error_handler, auth::AuthSession& session) {
  ICEBERG_ASSIGN_OR_RAISE(auto url, AppendQueryString(path, params));
  ICEBERG_ASSIGN_OR_RAISE(
      auto authenticated,
      session.Authenticate({.method = HttpMethod::kDelete,
                            .url = std::move(url),
                            .headers = MergeHeaders(default_headers_, headers),
                            .body = ""}));
  cpr::Response response = cpr::Delete(cpr::Url{authenticated.url},
                                       ToCprHeader(authenticated), *connection_pool_);

  ICEBERG_RETURN_UNEXPECTED(HandleFailureResponse(response, error_handler));
  HttpResponse http_response;
  http_response.impl_ = std::make_unique<HttpResponse::Impl>(std::move(response));
  return http_response;
}

}  // namespace iceberg::rest
