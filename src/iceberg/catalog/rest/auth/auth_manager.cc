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

#include "iceberg/catalog/rest/auth/auth_manager.h"

#include <optional>

#include "iceberg/catalog/rest/auth/auth_manager_internal.h"
#include "iceberg/catalog/rest/auth/auth_properties.h"
#include "iceberg/catalog/rest/auth/auth_session.h"
#include "iceberg/catalog/rest/auth/oauth2_util.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/transform_util.h"

namespace iceberg::rest::auth {

Result<std::shared_ptr<AuthSession>> AuthManager::InitSession(
    HttpClient& init_client,
    const std::unordered_map<std::string, std::string>& properties) {
  // By default, use the catalog session for initialization
  return CatalogSession(init_client, properties);
}

Result<std::shared_ptr<AuthSession>> AuthManager::ContextualSession(
    [[maybe_unused]] const std::unordered_map<std::string, std::string>& context,
    std::shared_ptr<AuthSession> parent) {
  // By default, return the parent session as-is
  return parent;
}

Result<std::shared_ptr<AuthSession>> AuthManager::TableSession(
    [[maybe_unused]] const TableIdentifier& table,
    [[maybe_unused]] const std::unordered_map<std::string, std::string>& properties,
    std::shared_ptr<AuthSession> parent) {
  // By default, return the parent session as-is
  return parent;
}

/// \brief Authentication manager that performs no authentication.
class NoopAuthManager : public AuthManager {
 public:
  Result<std::shared_ptr<AuthSession>> CatalogSession(
      [[maybe_unused]] HttpClient& client,
      [[maybe_unused]] const std::unordered_map<std::string, std::string>& properties)
      override {
    return AuthSession::MakeDefault({});
  }
};

Result<std::unique_ptr<AuthManager>> MakeNoopAuthManager(
    [[maybe_unused]] std::string_view name,
    [[maybe_unused]] const std::unordered_map<std::string, std::string>& properties) {
  return std::make_unique<NoopAuthManager>();
}

/// \brief Authentication manager that performs basic authentication.
class BasicAuthManager : public AuthManager {
 public:
  Result<std::shared_ptr<AuthSession>> CatalogSession(
      [[maybe_unused]] HttpClient& client,
      const std::unordered_map<std::string, std::string>& properties) override {
    auto username_it = properties.find(AuthProperties::kBasicUsername);
    ICEBERG_PRECHECK(username_it != properties.end() && !username_it->second.empty(),
                     "Missing required property '{}'", AuthProperties::kBasicUsername);
    auto password_it = properties.find(AuthProperties::kBasicPassword);
    ICEBERG_PRECHECK(password_it != properties.end() && !password_it->second.empty(),
                     "Missing required property '{}'", AuthProperties::kBasicPassword);
    std::string credential = username_it->second + ":" + password_it->second;
    return AuthSession::MakeDefault(
        {{std::string(kAuthorizationHeader),
          "Basic " + TransformUtil::Base64Encode(credential)}});
  }
};

Result<std::unique_ptr<AuthManager>> MakeBasicAuthManager(
    [[maybe_unused]] std::string_view name,
    [[maybe_unused]] const std::unordered_map<std::string, std::string>& properties) {
  return std::make_unique<BasicAuthManager>();
}

/// \brief OAuth2 authentication manager.
class OAuth2Manager : public AuthManager {
 public:
  Result<std::shared_ptr<AuthSession>> InitSession(
      HttpClient& init_client,
      const std::unordered_map<std::string, std::string>& properties) override {
    ICEBERG_ASSIGN_OR_RAISE(auto config, AuthProperties::FromProperties(properties));
    // No token refresh during init (short-lived session).
    config.Set(AuthProperties::kKeepRefreshed, false);

    // Credential takes priority: fetch a fresh token for the config request.
    if (!config.credential().empty()) {
      auto init_session = AuthSession::MakeDefault(AuthHeaders(config.token()));
      ICEBERG_ASSIGN_OR_RAISE(init_token_response_,
                              FetchToken(init_client, *init_session, config));
      return AuthSession::MakeDefault(AuthHeaders(init_token_response_->access_token));
    }

    if (!config.token().empty()) {
      return AuthSession::MakeDefault(AuthHeaders(config.token()));
    }

    return AuthSession::MakeDefault({});
  }

  Result<std::shared_ptr<AuthSession>> CatalogSession(
      HttpClient& client,
      const std::unordered_map<std::string, std::string>& properties) override {
    ICEBERG_ASSIGN_OR_RAISE(auto config, AuthProperties::FromProperties(properties));

    // Reuse token from init phase.
    if (init_token_response_.has_value()) {
      auto token_response = std::move(*init_token_response_);
      init_token_response_.reset();
      return AuthSession::MakeOAuth2(token_response, config.oauth2_server_uri(),
                                     config.client_id(), config.client_secret(),
                                     config.scope(), client);
    }

    // If token is provided, use it directly.
    if (!config.token().empty()) {
      return AuthSession::MakeDefault(AuthHeaders(config.token()));
    }

    // Fetch a new token using client_credentials grant.
    if (!config.credential().empty()) {
      auto base_session = AuthSession::MakeDefault(AuthHeaders(config.token()));
      OAuthTokenResponse token_response;
      ICEBERG_ASSIGN_OR_RAISE(token_response, FetchToken(client, *base_session, config));
      // TODO(lishuxu): should we directly pass config to the MakeOAuth2 call?
      return AuthSession::MakeOAuth2(token_response, config.oauth2_server_uri(),
                                     config.client_id(), config.client_secret(),
                                     config.scope(), client);
    }

    return AuthSession::MakeDefault({});
  }

  // TODO(lishuxu): Override TableSession() for token exchange (RFC 8693).
  // TODO(lishuxu): Override ContextualSession() for per-context exchange.

 private:
  /// Cached token from InitSession
  std::optional<OAuthTokenResponse> init_token_response_;
};

Result<std::unique_ptr<AuthManager>> MakeOAuth2Manager(
    [[maybe_unused]] std::string_view name,
    [[maybe_unused]] const std::unordered_map<std::string, std::string>& properties) {
  return std::make_unique<OAuth2Manager>();
}

}  // namespace iceberg::rest::auth
