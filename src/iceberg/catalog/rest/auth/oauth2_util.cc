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

#include <utility>

#include <nlohmann/json.hpp>

#include "iceberg/catalog/rest/auth/auth_session.h"
#include "iceberg/catalog/rest/error_handlers.h"
#include "iceberg/catalog/rest/http_client.h"
#include "iceberg/catalog/rest/json_serde_internal.h"
#include "iceberg/json_serde_internal.h"
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
                      /*headers=*/{}, *DefaultErrorHandler::Instance(), session));

  ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.body()));
  ICEBERG_ASSIGN_OR_RAISE(auto token_response, FromJson<OAuthTokenResponse>(json));
  ICEBERG_RETURN_UNEXPECTED(token_response.Validate());
  return token_response;
}

Result<OAuthTokenResponse> RefreshToken(HttpClient& client, AuthSession& session,
                                        const std::string& token_endpoint,
                                        const std::string& client_id,
                                        const std::string& refresh_token,
                                        const std::string& scope) {
  std::unordered_map<std::string, std::string> form_data{
      {std::string(kGrantType), "refresh_token"},
      {"refresh_token", refresh_token},
      {std::string(kClientId), client_id},
  };
  if (!scope.empty()) {
    form_data.emplace(std::string(kScope), scope);
  }

  ICEBERG_ASSIGN_OR_RAISE(auto response,
                          client.PostForm(token_endpoint, form_data, /*headers=*/{},
                                          *DefaultErrorHandler::Instance(), session));

  ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.body()));
  ICEBERG_ASSIGN_OR_RAISE(auto token_response, FromJson<OAuthTokenResponse>(json));
  ICEBERG_RETURN_UNEXPECTED(token_response.Validate());
  return token_response;
}

}  // namespace iceberg::rest::auth
