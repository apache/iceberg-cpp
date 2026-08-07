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
constexpr std::string_view kTokenExchange =
    "urn:ietf:params:oauth:grant-type:token-exchange";
constexpr std::string_view kClientId = "client_id";
constexpr std::string_view kClientSecret = "client_secret";
constexpr std::string_view kScope = "scope";
constexpr std::string_view kSubjectToken = "subject_token";
constexpr std::string_view kSubjectTokenType = "subject_token_type";
constexpr std::string_view kActorToken = "actor_token";
constexpr std::string_view kActorTokenType = "actor_token_type";

Result<OAuthTokenResponse> ParseTokenResponse(const std::string& response_body) {
  ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response_body));
  ICEBERG_ASSIGN_OR_RAISE(auto token_response, FromJson<OAuthTokenResponse>(json));
  ICEBERG_RETURN_UNEXPECTED(token_response.Validate());
  return token_response;
}

}  // namespace

std::unordered_map<std::string, std::string> AuthHeaders(const std::string& token) {
  if (!token.empty()) {
    return {{std::string(kAuthorizationHeader), std::string(kBearerPrefix) + token}};
  }
  return {};
}

bool IsValidTokenType(std::string_view token_type) {
  return token_type == AuthProperties::kAccessTokenType ||
         token_type == AuthProperties::kRefreshTokenType ||
         token_type == AuthProperties::kIdTokenType ||
         token_type == AuthProperties::kSaml1TokenType ||
         token_type == AuthProperties::kSaml2TokenType ||
         token_type == AuthProperties::kJwtTokenType;
}

std::array<std::string_view, 5> TokenPreferenceOrder() {
  return {
      std::string_view(AuthProperties::kIdTokenType),
      std::string_view(AuthProperties::kAccessTokenType),
      std::string_view(AuthProperties::kJwtTokenType),
      std::string_view(AuthProperties::kSaml2TokenType),
      std::string_view(AuthProperties::kSaml1TokenType),
  };
}

std::optional<OAuth2Token> FindPreferredTypedToken(
    const std::unordered_map<std::string, std::string>& credentials) {
  for (std::string_view token_type : TokenPreferenceOrder()) {
    auto token_it = credentials.find(std::string(token_type));
    if (token_it != credentials.end()) {
      return OAuth2Token{
          .token_type = token_it->first,
          .token = token_it->second,
      };
    }
  }
  return std::nullopt;
}

std::unordered_map<std::string, std::string> FilterTableSessionProperties(
    const std::unordered_map<std::string, std::string>& properties) {
  std::unordered_map<std::string, std::string> filtered;
  auto token_it = properties.find(AuthProperties::kToken.key());
  if (token_it != properties.end()) {
    filtered.emplace(token_it->first, token_it->second);
  }
  for (std::string_view token_type : TokenPreferenceOrder()) {
    auto token_it = properties.find(std::string(token_type));
    if (token_it != properties.end()) {
      filtered.emplace(token_it->first, token_it->second);
    }
  }
  return filtered;
}

Result<std::unordered_map<std::string, std::string>> BuildTokenExchangeForm(
    const TokenExchangeRequest& request) {
  if (request.subject.token.empty()) {
    return InvalidArgument("OAuth2 subject token must not be empty");
  }
  if (!IsValidTokenType(request.subject.token_type)) {
    return InvalidArgument("Invalid OAuth2 subject token type: '{}'",
                           request.subject.token_type);
  }
  if (request.actor.has_value()) {
    if (request.actor->token.empty()) {
      return InvalidArgument("OAuth2 actor token must not be empty");
    }
    if (!IsValidTokenType(request.actor->token_type)) {
      return InvalidArgument("Invalid OAuth2 actor token type: '{}'",
                             request.actor->token_type);
    }
  }

  std::unordered_map<std::string, std::string> form_data{
      {std::string(kGrantType), std::string(kTokenExchange)},
      {std::string(kScope), request.scope},
      {std::string(kSubjectToken), request.subject.token},
      {std::string(kSubjectTokenType), request.subject.token_type},
  };
  if (request.actor.has_value()) {
    form_data.emplace(kActorToken, request.actor->token);
    form_data.emplace(kActorTokenType, request.actor->token_type);
  }
  for (const auto& [key, value] : request.optional_oauth_params) {
    form_data.insert_or_assign(key, value);
  }
  return form_data;
}

Result<OAuthTokenResponse> ExchangeToken(
    HttpClient& client, AuthSession& session,
    const std::unordered_map<std::string, std::string>& extra_headers,
    const TokenExchangeRequest& request) {
  ICEBERG_ASSIGN_OR_RAISE(auto form_data, BuildTokenExchangeForm(request));
  ICEBERG_ASSIGN_OR_RAISE(
      auto response, client.PostForm(request.oauth2_server_uri, form_data, extra_headers,
                                     *OAuthErrorHandler::Instance(), session));
  return ParseTokenResponse(response.body());
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
  return ParseTokenResponse(response.body());
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
