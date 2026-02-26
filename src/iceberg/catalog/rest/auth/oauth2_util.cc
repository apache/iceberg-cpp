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

#include "iceberg/catalog/rest/auth/auth_session.h"
#include "iceberg/catalog/rest/error_handlers.h"
#include "iceberg/catalog/rest/http_client.h"
#include "iceberg/json_serde_internal.h"
#include "iceberg/util/json_util_internal.h"
#include "iceberg/util/macros.h"

namespace iceberg::rest::auth {

namespace {

constexpr std::string_view kAccessToken = "access_token";
constexpr std::string_view kTokenType = "token_type";
constexpr std::string_view kExpiresIn = "expires_in";
constexpr std::string_view kRefreshToken = "refresh_token";
constexpr std::string_view kScope = "scope";

constexpr std::string_view kGrantType = "grant_type";
constexpr std::string_view kClientCredentials = "client_credentials";
constexpr std::string_view kClientId = "client_id";
constexpr std::string_view kClientSecret = "client_secret";

}  // namespace

Status OAuthTokenResponse::Validate() const {
  if (access_token.empty()) {
    return ValidationFailed("OAuth2 token response missing required 'access_token'");
  }
  if (token_type.empty()) {
    return ValidationFailed("OAuth2 token response missing required 'token_type'");
  }
  return {};
}

Result<OAuthTokenResponse> OAuthTokenResponseFromJsonString(const std::string& json_str) {
  ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(json_str));

  OAuthTokenResponse response;
  ICEBERG_ASSIGN_OR_RAISE(response.access_token,
                          GetJsonValue<std::string>(json, kAccessToken));
  ICEBERG_ASSIGN_OR_RAISE(response.token_type,
                          GetJsonValue<std::string>(json, kTokenType));
  ICEBERG_ASSIGN_OR_RAISE(response.expires_in,
                          GetJsonValueOrDefault<int64_t>(json, kExpiresIn, 0));
  ICEBERG_ASSIGN_OR_RAISE(response.refresh_token,
                          GetJsonValueOrDefault<std::string>(json, kRefreshToken));
  ICEBERG_ASSIGN_OR_RAISE(response.scope,
                          GetJsonValueOrDefault<std::string>(json, kScope));
  ICEBERG_RETURN_UNEXPECTED(response.Validate());
  return response;
}

Result<OAuthTokenResponse> FetchToken(HttpClient& client,
                                      const std::string& token_endpoint,
                                      const std::string& client_id,
                                      const std::string& client_secret,
                                      const std::string& scope, AuthSession& session) {
  std::unordered_map<std::string, std::string> form_data = {
      {std::string(kGrantType), std::string(kClientCredentials)},
      {std::string(kClientId), client_id},
      {std::string(kClientSecret), client_secret},
  };
  if (!scope.empty()) {
    form_data[std::string(kScope)] = scope;
  }

  ICEBERG_ASSIGN_OR_RAISE(auto response,
                          client.PostForm(token_endpoint, form_data, /*headers=*/{},
                                          *DefaultErrorHandler::Instance(), session));

  return OAuthTokenResponseFromJsonString(response.body());
}

Result<OAuthTokenResponse> RefreshToken(HttpClient& client,
                                        const std::string& token_endpoint,
                                        const std::string& client_id,
                                        const std::string& refresh_token,
                                        const std::string& scope, AuthSession& session) {
  // TODO(lishuxu): Implement refresh_token grant type.
  return NotImplemented("RefreshToken is not yet implemented");
}

Result<OAuthTokenResponse> ExchangeToken(HttpClient& client,
                                         const std::string& token_endpoint,
                                         const std::string& subject_token,
                                         const std::string& subject_token_type,
                                         const std::string& scope, AuthSession& session) {
  // TODO(lishuxu): Implement token exchange (RFC 8693).
  return NotImplemented("ExchangeToken is not yet implemented");
}

}  // namespace iceberg::rest::auth
