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

#include <algorithm>
#include <array>
#include <chrono>
#include <optional>
#include <string_view>
#include <utility>

#include "iceberg/catalog/rest/auth/auth_manager_internal.h"
#include "iceberg/catalog/rest/auth/auth_properties.h"
#include "iceberg/catalog/rest/auth/auth_session.h"
#include "iceberg/catalog/rest/auth/auth_session_cache_internal.h"
#include "iceberg/catalog/rest/auth/auth_session_internal.h"
#include "iceberg/catalog/rest/auth/oauth2_util.h"
#include "iceberg/catalog/rest/auth/token_refresh_scheduler.h"
#include "iceberg/catalog/session_context.h"
#include "iceberg/util/base64.h"
#include "iceberg/util/macros.h"

namespace iceberg::rest::auth {

namespace {

constexpr std::string_view kAuthorizationHeader = "Authorization";

const std::array<std::string_view, 5> kTokenPreferenceOrder = {
    AuthProperties::kIdTokenType,    AuthProperties::kAccessTokenType,
    AuthProperties::kJwtTokenType,   AuthProperties::kSaml2TokenType,
    AuthProperties::kSaml1TokenType,
};

std::optional<std::pair<std::string, std::string>> FindPreferredTypedToken(
    const std::unordered_map<std::string, std::string>& credentials) {
  for (std::string_view token_type : kTokenPreferenceOrder) {
    auto token_it = credentials.find(std::string(token_type));
    if (token_it != credentials.end()) {
      return std::pair{token_it->first, token_it->second};
    }
  }
  return std::nullopt;
}

std::unordered_map<std::string, std::string> FilterTableSessionProperties(
    const std::unordered_map<std::string, std::string>& properties) {
  std::unordered_map<std::string, std::string> filtered;
  if (auto token_it = properties.find(AuthProperties::kToken.key());
      token_it != properties.end()) {
    filtered.emplace(token_it->first, token_it->second);
  }
  for (std::string_view token_type : kTokenPreferenceOrder) {
    auto token_it = properties.find(std::string(token_type));
    if (token_it != properties.end()) {
      filtered.emplace(token_it->first, token_it->second);
    }
  }
  return filtered;
}

}  // namespace

Result<std::shared_ptr<AuthSession>> AuthManager::InitSession(
    std::shared_ptr<HttpClient> init_client,
    const std::unordered_map<std::string, std::string>& properties) {
  // By default, use the catalog session for initialization
  return CatalogSession(std::move(init_client), properties);
}

Result<std::shared_ptr<AuthSession>> AuthManager::ContextualSession(
    [[maybe_unused]] const SessionContext& context, std::shared_ptr<AuthSession> parent) {
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
      [[maybe_unused]] std::shared_ptr<HttpClient> client,
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
      [[maybe_unused]] std::shared_ptr<HttpClient> client,
      const std::unordered_map<std::string, std::string>& properties) override {
    auto username_it = properties.find(AuthProperties::kBasicUsername);
    ICEBERG_PRECHECK(username_it != properties.end() && !username_it->second.empty(),
                     "Missing required property '{}'", AuthProperties::kBasicUsername);
    auto password_it = properties.find(AuthProperties::kBasicPassword);
    ICEBERG_PRECHECK(password_it != properties.end() && !password_it->second.empty(),
                     "Missing required property '{}'", AuthProperties::kBasicPassword);
    std::string credential = username_it->second + ":" + password_it->second;
    return AuthSession::MakeDefault(
        {{std::string(kAuthorizationHeader), "Basic " + Base64::Encode(credential)}});
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
      std::shared_ptr<HttpClient> init_client,
      const std::unordered_map<std::string, std::string>& properties) override {
    ICEBERG_PRECHECK(init_client != nullptr,
                     "OAuth2 initialization HTTP client must not be null");
    ICEBERG_ASSIGN_OR_RAISE(auto config, AuthProperties::FromProperties(properties));
    // No token refresh during init (short-lived session).
    config.Set(AuthProperties::kKeepRefreshed, false);

    // Credential takes priority: fetch a fresh token for the config request.
    if (!config.credential().empty()) {
      auto init_session =
          AuthSession::MakeDefault(OAuth2Util::AuthHeaders(config.token()));
      start_time_ = std::chrono::steady_clock::now();
      ICEBERG_ASSIGN_OR_RAISE(
          auth_response_, OAuth2Util::FetchToken(*init_client, *init_session, config));
      return AuthSession::MakeDefault(
          OAuth2Util::AuthHeaders(auth_response_->access_token));
    }

    if (!config.token().empty()) {
      return AuthSession::MakeDefault(OAuth2Util::AuthHeaders(config.token()));
    }

    return AuthSession::MakeDefault({});
  }

  Result<std::shared_ptr<AuthSession>> CatalogSession(
      std::shared_ptr<HttpClient> shared_client,
      const std::unordered_map<std::string, std::string>& properties) override {
    ICEBERG_ASSIGN_OR_RAISE(auto config, AuthProperties::FromProperties(properties));
    ICEBERG_PRECHECK(shared_client != nullptr,
                     "OAuth2 catalog session HTTP client must not be null");
    refresh_client_ = std::move(shared_client);

    // Initialize catalog-level configuration
    keep_refreshed_ = config.keep_refreshed();
    exchange_enabled_ = config.exchange_enabled();
    session_timeout_ =
        std::chrono::milliseconds(config.Get(AuthProperties::kSessionTimeoutMs));

    // Create session cache
    ICEBERG_ASSIGN_OR_RAISE(
        session_cache_,
        internal::AuthSessionCache::Make(
            session_timeout_, [](std::shared_ptr<AuthSession> session) {
              if (auto oauth2 =
                      std::dynamic_pointer_cast<internal::OAuth2Session>(session)) {
                oauth2->StopRefreshing();
              }
            }));

    // Periodically reclaim idle sessions. Sweep at most once a minute and at least
    // once a second, so a zero timeout still gets periodic cleanup.
    auto sweep_interval = std::clamp(session_timeout_, std::chrono::milliseconds(1000),
                                     std::chrono::milliseconds(60000));
    ICEBERG_RETURN_UNEXPECTED(session_cache_->StartPeriodicSweep(
        TokenRefreshScheduler::Instance(), sweep_interval));

    // Reuse the token response and start time from the init phase.
    if (auth_response_.has_value()) {
      return internal::MakeOAuth2Session(
          *auth_response_, config.oauth2_server_uri(), config.client_id(),
          config.client_secret(), config.scope(), config.keep_refreshed(),
          config.optional_oauth_params(), refresh_client_, start_time_);
    }

    // TODO(lishuxu): Honor token-refresh-enabled for catalog bearer tokens.
    // If token is provided, use it directly.
    if (!config.token().empty()) {
      OAuthTokenResponse token_response{
          .access_token = config.token(),
          .token_type = "bearer",
          .issued_token_type = AuthProperties::kAccessTokenType,
      };
      return AuthSession::MakeOAuth2(token_response, config.oauth2_server_uri(),
                                     config.client_id(), config.client_secret(),
                                     config.scope(), /*keep_refreshed=*/false,
                                     config.optional_oauth_params(), refresh_client_);
    }

    // Fetch a new token using client_credentials grant.
    if (!config.credential().empty()) {
      auto base_session =
          AuthSession::MakeDefault(OAuth2Util::AuthHeaders(config.token()));
      OAuthTokenResponse token_response;
      ICEBERG_ASSIGN_OR_RAISE(
          token_response,
          OAuth2Util::FetchToken(*refresh_client_, *base_session, config));
      return AuthSession::MakeOAuth2(token_response, config.oauth2_server_uri(),
                                     config.client_id(), config.client_secret(),
                                     config.scope(), config.keep_refreshed(),
                                     config.optional_oauth_params(), refresh_client_);
    }

    return MakeSession(AccessTokenResponse(""), config, /*keep_refreshed=*/false);
  }

  Result<std::shared_ptr<AuthSession>> ContextualSession(
      const SessionContext& context, std::shared_ptr<AuthSession> parent) override {
    // Use session_id as cache key for contextual sessions
    std::string cache_key = context.session_id.empty() ? "" : "ctx:" + context.session_id;
    return MaybeCreateChildSession(context.credentials, /*allow_credential=*/true,
                                   std::move(parent), cache_key);
  }

  Result<std::shared_ptr<AuthSession>> TableSession(
      [[maybe_unused]] const TableIdentifier& table,
      const std::unordered_map<std::string, std::string>& properties,
      std::shared_ptr<AuthSession> parent) override {
    // Use token value as cache key for table sessions
    auto token_it = properties.find(AuthProperties::kToken.key());
    std::string cache_key;
    if (token_it != properties.end() && !token_it->second.empty()) {
      cache_key = "tbl:" + token_it->second;
    }
    return MaybeCreateChildSession(FilterTableSessionProperties(properties),
                                   /*allow_credential=*/false, std::move(parent),
                                   cache_key);
  }

  Status Close() override {
    if (session_cache_) {
      session_cache_->Close();
    }
    refresh_client_.reset();
    return {};
  }

 private:
  static OAuthTokenResponse AccessTokenResponse(std::string token) {
    return {
        .access_token = std::move(token),
        .token_type = "bearer",
        .issued_token_type = AuthProperties::kAccessTokenType,
    };
  }

  static Result<AuthProperties> ChildConfig(const OAuth2SessionInfo& parent_info,
                                            const std::string& credential) {
    auto properties = parent_info.optional_oauth_params;
    properties[AuthProperties::kCredential.key()] = credential;
    properties[AuthProperties::kScope.key()] = parent_info.scope;
    properties[AuthProperties::kOAuth2ServerUri.key()] = parent_info.oauth2_server_uri;
    return AuthProperties::FromProperties(properties);
  }

  Result<std::shared_ptr<AuthSession>> MakeSession(
      const OAuthTokenResponse& token_response, const AuthProperties& config,
      bool keep_refreshed) const {
    ICEBERG_PRECHECK(refresh_client_ != nullptr,
                     "OAuth2 catalog session must be initialized before child sessions");
    return AuthSession::MakeOAuth2(token_response, config.oauth2_server_uri(),
                                   config.client_id(), config.client_secret(),
                                   config.scope(), keep_refreshed,
                                   config.optional_oauth_params(), refresh_client_);
  }

  Result<std::shared_ptr<AuthSession>> MaybeCreateChildSession(
      const std::unordered_map<std::string, std::string>& credentials,
      bool allow_credential, std::shared_ptr<AuthSession> parent,
      const std::string& cache_key) {
    auto token_it = credentials.find(AuthProperties::kToken.key());
    auto credential_it = credentials.find(AuthProperties::kCredential.key());
    auto typed_token = FindPreferredTypedToken(credentials);
    if (token_it == credentials.end() &&
        (!allow_credential || credential_it == credentials.end()) &&
        !typed_token.has_value()) {
      return parent;
    }

    ICEBERG_PRECHECK(refresh_client_ != nullptr,
                     "OAuth2 catalog session must be initialized before child sessions");
    auto parent_info = parent->OAuth2Info();
    ICEBERG_PRECHECK(parent_info.has_value(),
                     "OAuth2 child session requires OAuth2 parent metadata");

    // Use cache if we have a valid cache_key
    if (!cache_key.empty() && session_cache_) {
      return session_cache_->Get(cache_key,
                                 [&]() -> Result<std::shared_ptr<AuthSession>> {
                                   return CreateChildSessionUncached(
                                       credentials, allow_credential, *parent_info);
                                 });
    }

    // No cache, create session directly
    return CreateChildSessionUncached(credentials, allow_credential, *parent_info);
  }

  Result<std::shared_ptr<AuthSession>> CreateChildSessionUncached(
      const std::unordered_map<std::string, std::string>& credentials,
      bool allow_credential, const OAuth2SessionInfo& parent_info) {
    auto token_it = credentials.find(AuthProperties::kToken.key());
    auto credential_it = credentials.find(AuthProperties::kCredential.key());
    auto typed_token = FindPreferredTypedToken(credentials);

    if (token_it != credentials.end()) {
      // Token child session: don't include parent credential in config.
      // Token-based sessions are not refreshed; when token exchange is enabled,
      // refresh will use the token itself rather than the parent credential.
      auto properties = parent_info.optional_oauth_params;
      properties[AuthProperties::kScope.key()] = parent_info.scope;
      properties[AuthProperties::kOAuth2ServerUri.key()] = parent_info.oauth2_server_uri;
      ICEBERG_ASSIGN_OR_RAISE(auto config, AuthProperties::FromProperties(properties));
      return MakeSession(AccessTokenResponse(token_it->second), config,
                         /*keep_refreshed=*/false);
    }

    if (allow_credential && credential_it != credentials.end()) {
      ICEBERG_ASSIGN_OR_RAISE(auto config,
                              ChildConfig(parent_info, credential_it->second));
      // Use parent session headers for FetchToken authentication
      auto temp_parent = AuthSession::MakeDefault(parent_info.headers);
      ICEBERG_ASSIGN_OR_RAISE(
          auto response, OAuth2Util::FetchToken(*refresh_client_, *temp_parent, config));
      // Credential child sessions use keep_refreshed from catalog level
      return MakeSession(response, config, keep_refreshed_);
    }

    std::optional<std::string> actor_token;
    std::optional<std::string> actor_token_type;
    if (!parent_info.token.empty()) {
      actor_token = parent_info.token;
      actor_token_type = parent_info.issued_token_type;
    }
    // Use parent session headers for ExchangeToken authentication
    auto temp_parent = AuthSession::MakeDefault(parent_info.headers);
    ICEBERG_ASSIGN_OR_RAISE(
        auto response,
        OAuth2Util::ExchangeToken(*refresh_client_, *temp_parent, {}, typed_token->second,
                                  typed_token->first, actor_token, actor_token_type,
                                  parent_info.scope, parent_info.oauth2_server_uri,
                                  parent_info.optional_oauth_params));
    ICEBERG_ASSIGN_OR_RAISE(auto config,
                            ChildConfig(parent_info, parent_info.credential));
    return MakeSession(response, config, /*keep_refreshed=*/false);
  }

  /// Token response and start time captured by InitSession.
  std::optional<OAuthTokenResponse> auth_response_;
  std::optional<std::chrono::steady_clock::time_point> start_time_;
  std::shared_ptr<HttpClient> refresh_client_;

  // Catalog-level configuration initialized in CatalogSession
  bool keep_refreshed_ = true;
  bool exchange_enabled_ = true;
  std::chrono::milliseconds session_timeout_{3'600'000};
  std::shared_ptr<internal::AuthSessionCache> session_cache_;
};

Result<std::unique_ptr<AuthManager>> MakeOAuth2Manager(
    [[maybe_unused]] std::string_view name,
    [[maybe_unused]] const std::unordered_map<std::string, std::string>& properties) {
  return std::make_unique<OAuth2Manager>();
}

}  // namespace iceberg::rest::auth
