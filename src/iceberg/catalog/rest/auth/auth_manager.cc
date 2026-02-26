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
#include "iceberg/catalog/rest/catalog_properties.h"
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
        {{"Authorization", "Basic " + TransformUtil::Base64Encode(credential)}});
  }
};

Result<std::unique_ptr<AuthManager>> MakeBasicAuthManager(
    [[maybe_unused]] std::string_view name,
    [[maybe_unused]] const std::unordered_map<std::string, std::string>& properties) {
  return std::make_unique<BasicAuthManager>();
}

/// \brief OAuth2 authentication manager.
///
/// Two-phase init: InitSession fetches and caches a token for the config request;
/// CatalogSession reuses the cached token and enables refresh.
class OAuth2AuthManager : public AuthManager {
 public:
  Result<std::shared_ptr<AuthSession>> InitSession(
      HttpClient& init_client,
      const std::unordered_map<std::string, std::string>& properties) override {
    // Credential takes priority: fetch a fresh token for the config request.
    auto credential_it = properties.find(AuthProperties::kOAuth2Credential);
    if (credential_it != properties.end() && !credential_it->second.empty()) {
      ICEBERG_ASSIGN_OR_RAISE(auto ctx, ParseOAuth2Context(properties));
      auto noop_session = AuthSession::MakeDefault({});
      ICEBERG_ASSIGN_OR_RAISE(init_token_response_,
                              FetchToken(init_client, ctx.token_endpoint, ctx.client_id,
                                         ctx.client_secret, ctx.scope, *noop_session));
      return AuthSession::MakeDefault(
          {{"Authorization", "Bearer " + init_token_response_->access_token}});
    }

    auto token_it = properties.find(AuthProperties::kOAuth2Token);
    if (token_it != properties.end() && !token_it->second.empty()) {
      return AuthSession::MakeDefault({{"Authorization", "Bearer " + token_it->second}});
    }

    return AuthSession::MakeDefault({});
  }

  Result<std::shared_ptr<AuthSession>> CatalogSession(
      HttpClient& client,
      const std::unordered_map<std::string, std::string>& properties) override {
    if (init_token_response_.has_value()) {
      auto token_response = std::move(*init_token_response_);
      init_token_response_.reset();
      ICEBERG_ASSIGN_OR_RAISE(auto ctx, ParseOAuth2Context(properties));
      return AuthSession::MakeOAuth2(token_response, ctx.token_endpoint, ctx.client_id,
                                     ctx.client_secret, ctx.scope, client);
    }

    // If token is provided, use it directly.
    auto token_it = properties.find(AuthProperties::kOAuth2Token);
    if (token_it != properties.end() && !token_it->second.empty()) {
      return AuthSession::MakeDefault({{"Authorization", "Bearer " + token_it->second}});
    }

    // Fetch a new token using client_credentials grant.
    auto credential_it = properties.find(AuthProperties::kOAuth2Credential);
    if (credential_it != properties.end() && !credential_it->second.empty()) {
      ICEBERG_ASSIGN_OR_RAISE(auto ctx, ParseOAuth2Context(properties));
      auto noop_session = AuthSession::MakeDefault({});
      OAuthTokenResponse token_response;
      ICEBERG_ASSIGN_OR_RAISE(token_response,
                              FetchToken(client, ctx.token_endpoint, ctx.client_id,
                                         ctx.client_secret, ctx.scope, *noop_session));
      return AuthSession::MakeOAuth2(token_response, ctx.token_endpoint, ctx.client_id,
                                     ctx.client_secret, ctx.scope, client);
    }

    return AuthSession::MakeDefault({});
  }

  // TODO(lishuxu): Override TableSession() to support token exchange (RFC 8693).
  // TODO(lishuxu): Override ContextualSession() to support per-context token exchange.

 private:
  struct OAuth2Context {
    std::string client_id;
    std::string client_secret;
    std::string token_endpoint;
    std::string scope;
  };

  /// \brief Parse credential, token endpoint, and scope from properties.
  static Result<OAuth2Context> ParseOAuth2Context(
      const std::unordered_map<std::string, std::string>& properties) {
    OAuth2Context ctx;

    auto credential_it = properties.find(AuthProperties::kOAuth2Credential);
    if (credential_it == properties.end() || credential_it->second.empty()) {
      return InvalidArgument("OAuth2 authentication requires '{}' property",
                             AuthProperties::kOAuth2Credential);
    }
    const auto& credential = credential_it->second;
    auto colon_pos = credential.find(':');
    if (colon_pos == std::string::npos) {
      return InvalidArgument(
          "Invalid OAuth2 credential format: expected 'client_id:client_secret'");
    }
    ctx.client_id = credential.substr(0, colon_pos);
    ctx.client_secret = credential.substr(colon_pos + 1);

    auto uri_it = properties.find(AuthProperties::kOAuth2ServerUri);
    if (uri_it != properties.end() && !uri_it->second.empty()) {
      ctx.token_endpoint = uri_it->second;
    } else {
      // {uri}/v1/oauth/tokens.
      auto catalog_uri_it = properties.find(RestCatalogProperties::kUri.key());
      if (catalog_uri_it == properties.end() || catalog_uri_it->second.empty()) {
        return InvalidArgument(
            "OAuth2 authentication requires '{}' or '{}' property to determine "
            "token endpoint",
            AuthProperties::kOAuth2ServerUri, RestCatalogProperties::kUri.key());
      }
      std::string_view base = catalog_uri_it->second;
      while (!base.empty() && base.back() == '/') {
        base.remove_suffix(1);
      }
      ctx.token_endpoint =
          std::string(base) + "/" + std::string(AuthProperties::kOAuth2DefaultTokenPath);
    }

    ctx.scope = AuthProperties::kOAuth2DefaultScope;
    auto scope_it = properties.find(AuthProperties::kOAuth2Scope);
    if (scope_it != properties.end() && !scope_it->second.empty()) {
      ctx.scope = scope_it->second;
    }

    return ctx;
  }

  /// Cached token from InitSession
  std::optional<OAuthTokenResponse> init_token_response_;
};

Result<std::unique_ptr<AuthManager>> MakeOAuth2AuthManager(
    [[maybe_unused]] std::string_view name,
    [[maybe_unused]] const std::unordered_map<std::string, std::string>& properties) {
  return std::make_unique<OAuth2AuthManager>();
}

}  // namespace iceberg::rest::auth
