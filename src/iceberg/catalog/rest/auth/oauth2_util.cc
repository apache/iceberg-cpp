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

#include "iceberg/catalog/rest/auth/oauth2_util.h"

#include <nlohmann/json.hpp>

#include "iceberg/catalog/rest/auth/auth_properties.h"
#include "iceberg/catalog/rest/auth/auth_session.h"
#include "iceberg/catalog/rest/error_handlers.h"
#include "iceberg/catalog/rest/http_client.h"
#include "iceberg/catalog/rest/json_serde_internal.h"
#include "iceberg/json_serde_internal.h"
#include "iceberg/util/base64.h"
#include "iceberg/util/macros.h"

namespace iceberg::rest::auth {

namespace {

constexpr std::string_view kGrantType = "grant_type";
constexpr std::string_view kClientCredentials = "client_credentials";
constexpr std::string_view kClientId = "client_id";
constexpr std::string_view kClientSecret = "client_secret";
constexpr std::string_view kScope = "scope";

}  // namespace

std::unordered_map<std::string, std::string> AuthHeaders(const std::string& token) {
  if (!token.empty()) {
    return {{std::string(kAuthorizationHeader), std::string(kBearerPrefix) + token}};
  }
  return {};
}

Result<OAuthTokenResponse> FetchToken(HttpClient& client, AuthSession& session,
                                      const AuthProperties& properties) {
  std::unordered_map<std::string, std::string> form_data{
      {std::string(kGrantType), std::string(kClientCredentials)},
      {std::string(kClientSecret), properties.client_secret()},
      {std::string(kScope), properties.scope()},
  };
  if (!properties.client_id().empty()) {
    form_data.emplace(std::string(kClientId), properties.client_id());
  }
  for (const auto& [key, value] : properties.optional_oauth_params()) {
    form_data.emplace(key, value);
  }

  ICEBERG_ASSIGN_OR_RAISE(
      auto response,
      client.PostForm(properties.oauth2_server_uri(), form_data,
                      /*headers=*/{}, *OAuthErrorHandler::Instance(), session));

  ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.body()));
  ICEBERG_ASSIGN_OR_RAISE(auto token_response, FromJson<OAuthTokenResponse>(json));
  ICEBERG_RETURN_UNEXPECTED(token_response.Validate());
  return token_response;
}

std::optional<int64_t> ExpiresAtMillis(std::string_view token) {
  if (token.empty()) {
    return std::nullopt;
  }

  // A JWT has exactly 3 dot-separated parts: header.payload.signature
  auto first_dot = token.find('.');
  if (first_dot == std::string_view::npos) {
    return std::nullopt;
  }
  auto second_dot = token.find('.', first_dot + 1);
  if (second_dot == std::string_view::npos) {
    return std::nullopt;
  }
  // Ensure there are exactly 3 parts (no additional dots after the signature).
  // Note: JWE tokens have 5 segments — they are intentionally not supported here
  // and will return nullopt (graceful degradation to not scheduling refresh).
  if (token.find('.', second_dot + 1) != std::string_view::npos) {
    return std::nullopt;
  }

  // Extract and decode the payload (second part).
  // Note: Base64::UrlDecode returns an error on invalid input, and Ok("") on empty input.
  // A valid JWT payload is never empty (at minimum "{}"), so empty result reliably
  // indicates the token is not a JWT we can parse.
  std::string_view payload_b64 = token.substr(first_dot + 1, second_dot - first_dot - 1);
  auto payload_result = Base64::UrlDecode(payload_b64);
  if (!payload_result.has_value() || payload_result->empty()) {
    return std::nullopt;
  }
  const std::string& payload = *payload_result;

  // Parse JSON and extract "exp" claim
  auto json = nlohmann::json::parse(payload, nullptr, /*allow_exceptions=*/false);
  if (json.is_discarded() || !json.is_object()) {
    return std::nullopt;
  }
  auto it = json.find("exp");
  if (it == json.end() || !it->is_number()) {
    return std::nullopt;
  }
  auto exp_seconds = static_cast<int64_t>(it->get<double>());
  return exp_seconds * 1000;  // Convert seconds to milliseconds
}

}  // namespace iceberg::rest::auth
