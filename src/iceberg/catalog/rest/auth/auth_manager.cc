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

#include <array>
#include <chrono>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "iceberg/catalog/rest/auth/auth_manager_internal.h"
#include "iceberg/catalog/rest/auth/auth_properties.h"
#include "iceberg/catalog/rest/auth/auth_session.h"
#include "iceberg/catalog/rest/auth/auth_session_cache_internal.h"
#include "iceberg/catalog/rest/auth/auth_session_internal.h"
#include "iceberg/catalog/rest/auth/oauth2_util.h"
#include "iceberg/catalog/session_context.h"
#include "iceberg/logging/log_macros.h"
#include "iceberg/util/base64.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/string_util.h"

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

/// \brief Credential used to create an OAuth2 child session.
struct ChildCredential {
  enum class Kind { kToken, kCredential, kTokenExchange };

  Kind kind;
  std::string value;
  /// Subject token type; only set for kTokenExchange.
  std::string token_type;
};

/// \brief Select the credential for a child session.
///
/// The order matches Java: a bearer token, then a client credential (only when
/// `allow_credential` is true), then a typed token in kTokenPreferenceOrder.
std::optional<ChildCredential> SelectChildCredential(
    const std::unordered_map<std::string, std::string>& credentials,
    bool allow_credential) {
  if (auto token_it = credentials.find(AuthProperties::kToken.key());
      token_it != credentials.end()) {
    return ChildCredential{.kind = ChildCredential::Kind::kToken,
                           .value = token_it->second};
  }
  if (allow_credential) {
    if (auto credential_it = credentials.find(AuthProperties::kCredential.key());
        credential_it != credentials.end()) {
      return ChildCredential{.kind = ChildCredential::Kind::kCredential,
                             .value = credential_it->second};
    }
  }
  if (auto typed_token = FindPreferredTypedToken(credentials); typed_token.has_value()) {
    return ChildCredential{.kind = ChildCredential::Kind::kTokenExchange,
                           .value = std::move(typed_token->second),
                           .token_type = std::move(typed_token->first)};
  }
  return std::nullopt;
}

/// \brief Actor token sent with a token exchange.
struct ActorToken {
  std::string token;
  std::string token_type;
};

/// \brief The parent session's token, sent as the actor of a token exchange, or
/// std::nullopt if the parent has no token.
std::optional<ActorToken> ExchangeActor(const OAuth2SessionInfo& parent_info) {
  if (parent_info.token.empty()) {
    return std::nullopt;
  }
  return ActorToken{.token = parent_info.token,
                    .token_type = parent_info.issued_token_type};
}

void AppendCacheKeyPart(std::string& key, std::string_view part) {
  key += std::to_string(part.size());
  key += ':';
  key += part;
}

/// \brief Build the cache key of a table session.
///
/// The key covers every input that determines how the session authenticates, so
/// two table sessions are shared only when they would send the same credentials.
/// With P(s) = "<byte length of s>:<s>", the formats are:
///
///   bearer token:   "tbl:" + P("token") + P(token)
///   token exchange: "tbl:" + P("exchange") + P(subject type) + P(subject)
///                          + P(actor type) + P(actor)
///
/// A bearer token is sent as-is, so its key does not depend on the parent. An
/// exchanged token also depends on the actor, the parent's token; the actor parts
/// are empty when the parent has no token, matching the exchange request. The
/// length prefix keeps the key unambiguous although token types contain ':'.
///
/// For example, bearer token "abc" gives "tbl:5:token3:abc", and JWT "eyJ.a.b"
/// exchanged under the parent access token "tok" gives (on one line):
///
///   tbl:8:exchange36:urn:ietf:params:oauth:token-type:jwt7:eyJ.a.b
///   45:urn:ietf:params:oauth:token-type:access_token3:tok
///
/// An empty token gives an empty key, which disables caching.
std::string TableCacheKey(const ChildCredential& credential,
                          const OAuth2SessionInfo& parent_info) {
  if (credential.value.empty()) {
    return {};
  }
  std::string key = "tbl:";
  switch (credential.kind) {
    case ChildCredential::Kind::kToken:
      AppendCacheKeyPart(key, "token");
      AppendCacheKeyPart(key, credential.value);
      return key;
    case ChildCredential::Kind::kTokenExchange: {
      auto actor = ExchangeActor(parent_info);
      AppendCacheKeyPart(key, "exchange");
      AppendCacheKeyPart(key, credential.token_type);
      AppendCacheKeyPart(key, credential.value);
      AppendCacheKeyPart(key, actor.has_value() ? actor->token_type : "");
      AppendCacheKeyPart(key, actor.has_value() ? actor->token : "");
      return key;
    }
    case ChildCredential::Kind::kCredential:
      // Table sessions never use a client credential.
      return {};
  }
  std::unreachable();
}

/// \brief Parse auth.session-timeout-ms, returning an error for a malformed or
/// negative value rather than throwing as AuthProperties::Get() would.
Result<std::chrono::milliseconds> ParseSessionTimeout(const AuthProperties& config) {
  const auto& entry = AuthProperties::kSessionTimeoutMs;
  int64_t timeout_ms = entry.value();
  if (auto it = config.configs().find(entry.key()); it != config.configs().end()) {
    ICEBERG_ASSIGN_OR_RAISE(timeout_ms, StringUtils::ParseNumber<int64_t>(it->second));
  }
  ICEBERG_PRECHECK(timeout_ms >= 0, "Invalid value for {}: {} (must not be negative)",
                   entry.key(), timeout_ms);
  return std::chrono::milliseconds(timeout_ms);
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
  void SetAuthRefreshExecutor(Executor* executor) override {
    refresh_executor_ = executor;
  }

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
      // TODO(lishuxu): Match Java OAuth2Util.AuthSession.fromTokenResponse here.
      return AuthSession::MakeDefault(
          OAuth2Util::AuthHeaders(auth_response_->access_token));
    }

    if (!config.token().empty()) {
      // TODO(lishuxu): Match Java OAuth2Util.AuthSession.fromAccessToken here.
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
    ICEBERG_ASSIGN_OR_RAISE(session_timeout_, ParseSessionTimeout(config));

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

    // Reclaim idle sessions in the background on the refresh executor. Without an
    // executor, the cache sweeps from Get() instead.
    if (refresh_executor_ != nullptr) {
      ICEBERG_RETURN_UNEXPECTED(session_cache_->StartPeriodicSweep(
          *refresh_executor_,
          internal::AuthSessionCache::DefaultSweepInterval(session_timeout_)));
    }

    // Reuse the token response and start time from the init phase.
    if (auth_response_.has_value()) {
      return MakeSession(*auth_response_, config, config.keep_refreshed(), start_time_);
    }

    // TODO(lishuxu): Honor token-refresh-enabled for catalog bearer tokens, matching
    // Java. If token is provided, use it directly.
    if (!config.token().empty()) {
      OAuthTokenResponse token_response{
          .access_token = config.token(),
          .token_type = "bearer",
          .issued_token_type = AuthProperties::kAccessTokenType,
      };
      return MakeSession(token_response, config, /*keep_refreshed=*/false);
    }

    // Fetch a new token using client_credentials grant.
    if (!config.credential().empty()) {
      auto base_session =
          AuthSession::MakeDefault(OAuth2Util::AuthHeaders(config.token()));
      OAuthTokenResponse token_response;
      ICEBERG_ASSIGN_OR_RAISE(
          token_response,
          OAuth2Util::FetchToken(*refresh_client_, *base_session, config));
      return MakeSession(token_response, config, config.keep_refreshed());
    }

    return MakeSession(AccessTokenResponse(""), config, /*keep_refreshed=*/false);
  }

  Result<std::shared_ptr<AuthSession>> ContextualSession(
      const SessionContext& context, std::shared_ptr<AuthSession> parent) override {
    auto credential =
        SelectChildCredential(context.credentials, /*allow_credential=*/true);
    if (!credential.has_value()) {
      return parent;
    }
    ICEBERG_ASSIGN_OR_RAISE(auto parent_info, ParentInfo(*parent));
    // Contextual sessions are cached by session ID, whatever credential they use.
    std::string cache_key = context.session_id.empty() ? "" : "ctx:" + context.session_id;
    return GetOrCreateChildSession(*credential, parent_info, cache_key);
  }

  Result<std::shared_ptr<AuthSession>> TableSession(
      [[maybe_unused]] const TableIdentifier& table,
      const std::unordered_map<std::string, std::string>& properties,
      std::shared_ptr<AuthSession> parent) override {
    auto credential = SelectChildCredential(FilterTableSessionProperties(properties),
                                            /*allow_credential=*/false);
    if (!credential.has_value()) {
      return parent;
    }
    ICEBERG_ASSIGN_OR_RAISE(auto parent_info, ParentInfo(*parent));
    return GetOrCreateChildSession(*credential, parent_info,
                                   TableCacheKey(*credential, parent_info));
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
      bool keep_refreshed,
      std::optional<std::chrono::steady_clock::time_point> token_request_started_at =
          std::nullopt) const {
    ICEBERG_PRECHECK(refresh_client_ != nullptr,
                     "OAuth2 catalog session must be initialized before child sessions");
    return internal::MakeOAuth2Session(token_response, config.oauth2_server_uri(),
                                       config.client_id(), config.client_secret(),
                                       config.scope(), keep_refreshed,
                                       config.optional_oauth_params(), refresh_client_,
                                       token_request_started_at, refresh_executor_);
  }

  Result<OAuth2SessionInfo> ParentInfo(const AuthSession& parent) const {
    ICEBERG_PRECHECK(refresh_client_ != nullptr,
                     "OAuth2 catalog session must be initialized before child sessions");
    auto parent_info = parent.OAuth2Info();
    ICEBERG_PRECHECK(parent_info.has_value(),
                     "OAuth2 child session requires OAuth2 parent metadata");
    return std::move(*parent_info);
  }

  Result<std::shared_ptr<AuthSession>> GetOrCreateChildSession(
      const ChildCredential& credential, const OAuth2SessionInfo& parent_info,
      const std::string& cache_key) {
    // Use cache if we have a valid cache_key
    if (!cache_key.empty() && session_cache_) {
      return session_cache_->Get(
          cache_key, [&]() -> Result<std::shared_ptr<AuthSession>> {
            return CreateChildSessionUncached(credential, parent_info);
          });
    }

    // No cache, create session directly
    return CreateChildSessionUncached(credential, parent_info);
  }

  Result<std::shared_ptr<AuthSession>> CreateChildSessionUncached(
      const ChildCredential& credential, const OAuth2SessionInfo& parent_info) {
    switch (credential.kind) {
      case ChildCredential::Kind::kToken:
        return CreateTokenChildSession(credential.value, parent_info);
      case ChildCredential::Kind::kCredential:
        return CreateCredentialChildSession(credential.value, parent_info);
      case ChildCredential::Kind::kTokenExchange:
        return CreateTokenExchangeChildSession(credential.value, credential.token_type,
                                               parent_info);
    }
    std::unreachable();
  }

  Result<std::shared_ptr<AuthSession>> CreateTokenChildSession(
      const std::string& token, const OAuth2SessionInfo& parent_info) {
    // Token child session: don't include parent credential in config.
    // Token-based sessions are not refreshed; when token exchange is enabled,
    // refresh will use the token itself rather than the parent credential.
    auto properties = parent_info.optional_oauth_params;
    properties[AuthProperties::kScope.key()] = parent_info.scope;
    properties[AuthProperties::kOAuth2ServerUri.key()] = parent_info.oauth2_server_uri;
    ICEBERG_ASSIGN_OR_RAISE(auto config, AuthProperties::FromProperties(properties));
    return MakeSession(AccessTokenResponse(token), config, /*keep_refreshed=*/false);
  }

  Result<std::shared_ptr<AuthSession>> CreateCredentialChildSession(
      const std::string& credential, const OAuth2SessionInfo& parent_info) {
    ICEBERG_ASSIGN_OR_RAISE(auto config, ChildConfig(parent_info, credential));
    // Use parent session headers for FetchToken authentication
    auto temp_parent = AuthSession::MakeDefault(parent_info.headers);
    ICEBERG_ASSIGN_OR_RAISE(
        auto response, OAuth2Util::FetchToken(*refresh_client_, *temp_parent, config));
    // Credential child sessions follow the catalog's keep_refreshed, but refreshing
    // needs an executor. Without one, the child keeps its initial token.
    if (keep_refreshed_ && refresh_executor_ == nullptr) {
      std::call_once(no_refresh_executor_warning_, [] {
        ICEBERG_LOG_WARN(
            "OAuth2 credential child sessions will not refresh their tokens because "
            "no auth executor was provided");
      });
    }
    return MakeSession(response, config, keep_refreshed_ && refresh_executor_ != nullptr);
  }

  Result<std::shared_ptr<AuthSession>> CreateTokenExchangeChildSession(
      const std::string& subject_token, const std::string& subject_token_type,
      const OAuth2SessionInfo& parent_info) {
    std::optional<std::string> actor_token;
    std::optional<std::string> actor_token_type;
    if (auto actor = ExchangeActor(parent_info); actor.has_value()) {
      actor_token = std::move(actor->token);
      actor_token_type = std::move(actor->token_type);
    }
    // Use parent session headers for ExchangeToken authentication
    auto temp_parent = AuthSession::MakeDefault(parent_info.headers);
    ICEBERG_ASSIGN_OR_RAISE(
        auto response,
        OAuth2Util::ExchangeToken(*refresh_client_, *temp_parent, {}, subject_token,
                                  subject_token_type, actor_token, actor_token_type,
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
  Executor* refresh_executor_ = nullptr;

  // Catalog-level configuration initialized in CatalogSession
  bool keep_refreshed_ = true;
  std::chrono::milliseconds session_timeout_{3'600'000};
  std::shared_ptr<internal::AuthSessionCache> session_cache_;
  std::once_flag no_refresh_executor_warning_;
};

Result<std::unique_ptr<AuthManager>> MakeOAuth2Manager(
    [[maybe_unused]] std::string_view name,
    [[maybe_unused]] const std::unordered_map<std::string, std::string>& properties) {
  return std::make_unique<OAuth2Manager>();
}

}  // namespace iceberg::rest::auth
