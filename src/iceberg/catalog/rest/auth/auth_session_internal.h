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

#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>

#include "iceberg/catalog/rest/auth/auth_properties.h"
#include "iceberg/catalog/rest/auth/auth_session.h"
#include "iceberg/catalog/rest/auth/oauth2_util.h"
#include "iceberg/catalog/rest/auth/token_refresh_scheduler.h"
#include "iceberg/catalog/rest/http_client.h"
#include "iceberg/util/macros.h"

namespace iceberg::rest::auth::internal {

inline std::optional<std::chrono::steady_clock::time_point> TokenExpirationTime(
    const OAuthTokenResponse& response,
    std::chrono::steady_clock::time_point request_started_at,
    std::chrono::system_clock::time_point now_system = std::chrono::system_clock::now(),
    std::chrono::steady_clock::time_point now_steady = std::chrono::steady_clock::now()) {
  if (auto exp_ms = OAuth2Util::ExpiresAtMillis(response.access_token);
      exp_ms.has_value()) {
    auto expiration_system =
        std::chrono::system_clock::time_point(std::chrono::milliseconds(*exp_ms));
    return now_steady + (expiration_system - now_system);
  }
  if (response.expires_in_secs.has_value()) {
    return request_started_at + std::chrono::seconds(*response.expires_in_secs);
  }
  return std::nullopt;
}

/// \brief Internal OAuth2 authentication session.
class OAuth2Session final : public AuthSession,
                            public std::enable_shared_from_this<OAuth2Session> {
 public:
  struct Config {
    std::string token_endpoint;
    std::string client_id;
    std::string client_secret;
    std::string scope;
    std::unordered_map<std::string, std::string> optional_oauth_params;
    bool keep_refreshed;
  };

  static Result<std::shared_ptr<OAuth2Session>> Make(
      const OAuthTokenResponse& initial_token, Config config,
      std::shared_ptr<HttpClient> client,
      std::optional<std::chrono::steady_clock::time_point> token_request_started_at) {
    ICEBERG_PRECHECK(client != nullptr, "OAuth2 session HTTP client must not be null");
    ICEBERG_ASSIGN_OR_RAISE(auto refresh_properties, MakeRefreshProperties(config));
    auto session = std::shared_ptr<OAuth2Session>(new OAuth2Session(
        std::move(config), std::move(refresh_properties), std::move(client)));
    session->SetInitialToken(initial_token, token_request_started_at);
    return session;
  }

  Result<HttpRequest> Authenticate(HttpRequest request) override {
    std::shared_lock lock(mutex_);
    for (const auto& [key, value] : headers_) {
      request.headers.try_emplace(key, value);
    }
    return request;
  }

  std::optional<OAuth2SessionInfo> OAuth2Info() const override {
    std::shared_lock lock(mutex_);
    return OAuth2SessionInfo{
        .token = token_,
        .issued_token_type = issued_token_type_,
        .credential = Credential(config_),
        .scope = config_.scope,
        .oauth2_server_uri = config_.token_endpoint,
        .optional_oauth_params = config_.optional_oauth_params,
    };
  }

  Status Close() override { return CloseImpl(); }

  ~OAuth2Session() override { std::ignore = CloseImpl(); }

 private:
  OAuth2Session(Config config, AuthProperties refresh_properties,
                std::shared_ptr<HttpClient> client)
      : config_(std::move(config)),
        refresh_properties_(std::move(refresh_properties)),
        client_(std::move(client)) {}

  Status CloseImpl() {
    bool expected = false;
    if (!closed_.compare_exchange_strong(expected, true)) {
      return {};
    }
    TokenRefreshScheduler::Instance().Cancel(scheduled_task_id_.exchange(0));
    std::unique_lock lock(refresh_mutex_);
    refresh_cv_.wait(lock, [this] { return active_refresh_count_ == 0; });
    TokenRefreshScheduler::Instance().Cancel(scheduled_task_id_.exchange(0));
    return {};
  }

  static std::string Credential(const Config& config) {
    return config.client_id.empty() ? config.client_secret
                                    : config.client_id + ":" + config.client_secret;
  }

  static Result<AuthProperties> MakeRefreshProperties(const Config& config) {
    std::unordered_map<std::string, std::string> properties =
        config.optional_oauth_params;
    properties[AuthProperties::kCredential.key()] = Credential(config);
    properties[AuthProperties::kScope.key()] = config.scope;
    properties[AuthProperties::kOAuth2ServerUri.key()] = config.token_endpoint;
    return AuthProperties::FromProperties(properties);
  }

  class RefreshAttemptGuard {
   public:
    explicit RefreshAttemptGuard(OAuth2Session& session) : session_(session) {
      std::lock_guard lock(session_.refresh_mutex_);
      ++session_.active_refresh_count_;
    }

    ~RefreshAttemptGuard() {
      bool notify = false;
      {
        std::lock_guard lock(session_.refresh_mutex_);
        notify = --session_.active_refresh_count_ == 0;
      }
      if (notify) {
        session_.refresh_cv_.notify_all();
      }
    }

   private:
    OAuth2Session& session_;
  };

  void UpdateTokenState(const OAuthTokenResponse& token_response,
                        std::optional<std::chrono::steady_clock::time_point>
                            token_request_started_at = std::nullopt) {
    token_ = token_response.access_token;
    issued_token_type_ = token_response.issued_token_type.empty()
                             ? AuthProperties::kAccessTokenType
                             : token_response.issued_token_type;
    headers_ = OAuth2Util::AuthHeaders(token_);
    expires_at_ = std::chrono::steady_clock::time_point{};
    auto request_started_at =
        token_request_started_at.value_or(std::chrono::steady_clock::now());
    if (auto expiration = TokenExpirationTime(token_response, request_started_at);
        expiration.has_value()) {
      expires_at_ = *expiration;
    }
  }

  void SetInitialToken(
      const OAuthTokenResponse& token_response,
      std::optional<std::chrono::steady_clock::time_point> token_request_started_at) {
    UpdateTokenState(token_response, token_request_started_at);
    if (config_.keep_refreshed &&
        expires_at_ != std::chrono::steady_clock::time_point{}) {
      ScheduleRefresh();
    }
  }

  void DoRefresh() {
    DoRefreshAttempt(0, std::chrono::milliseconds(200), std::chrono::steady_clock::now());
  }

  void DoRefreshAttempt(int attempt, std::chrono::milliseconds backoff,
                        std::chrono::steady_clock::time_point refresh_started_at) {
    static constexpr int kMaxRetries = 5;
    static constexpr auto kMaxBackoff = std::chrono::milliseconds(10'000);
    RefreshAttemptGuard guard(*this);
    if (closed_.load()) return;

    auto empty_session = AuthSession::MakeDefault({});
    // TODO(lishuxu): Honor token-exchange-enabled and refresh via token exchange,
    // matching Java.
    auto result = OAuth2Util::FetchToken(*client_, *empty_session, refresh_properties_);
    if (result.has_value()) {
      auto& response = result.value();
      {
        std::unique_lock lock(mutex_);
        UpdateTokenState(response, refresh_started_at);
      }
      ScheduleRefresh();
      return;
    }

    if (attempt + 1 < kMaxRetries && !closed_.load()) {
      auto next_backoff =
          std::min(std::chrono::duration_cast<std::chrono::milliseconds>(backoff * 2),
                   kMaxBackoff);
      std::weak_ptr<OAuth2Session> weak_self = shared_from_this();
      auto retry_id = TokenRefreshScheduler::Instance().Schedule(
          backoff, [weak_self = std::move(weak_self), next_attempt = attempt + 1,
                    next_backoff, refresh_started_at] {
            if (auto self = weak_self.lock()) {
              self->DoRefreshAttempt(next_attempt, next_backoff, refresh_started_at);
            }
          });
      scheduled_task_id_.store(retry_id);
    }
  }

  void ScheduleRefresh() {
    if (!config_.keep_refreshed || closed_.load()) return;
    auto delay = CalculateRefreshDelay();
    if (delay < std::chrono::milliseconds::zero()) return;

    std::weak_ptr<OAuth2Session> weak_self = shared_from_this();
    auto new_id = TokenRefreshScheduler::Instance().Schedule(
        delay, [weak_self = std::move(weak_self)] {
          if (auto self = weak_self.lock()) self->DoRefresh();
        });
    scheduled_task_id_.store(new_id);
  }

  std::chrono::milliseconds CalculateRefreshDelay() const {
    std::shared_lock lock(mutex_);
    auto now = std::chrono::steady_clock::now();
    if (expires_at_ == std::chrono::steady_clock::time_point{}) {
      return std::chrono::milliseconds(-1);
    }
    if (expires_at_ <= now) return std::chrono::milliseconds::zero();
    auto expires_in =
        std::chrono::duration_cast<std::chrono::milliseconds>(expires_at_ - now);
    auto refresh_window = std::min(expires_in / 10, std::chrono::milliseconds(300'000));
    auto wait_time = expires_in - refresh_window;
    return std::max(wait_time, std::chrono::milliseconds(10));
  }

  mutable std::shared_mutex mutex_;
  std::string token_;
  std::string issued_token_type_;
  std::unordered_map<std::string, std::string> headers_;
  std::chrono::steady_clock::time_point expires_at_{};
  Config config_;
  AuthProperties refresh_properties_;
  std::shared_ptr<HttpClient> client_;
  std::atomic<uint64_t> scheduled_task_id_{0};
  std::atomic<bool> closed_{false};
  std::mutex refresh_mutex_;
  std::condition_variable refresh_cv_;
  int active_refresh_count_ = 0;
};

Result<std::shared_ptr<AuthSession>> MakeOAuth2Session(
    const OAuthTokenResponse& initial_token, const std::string& token_endpoint,
    const std::string& client_id, const std::string& client_secret,
    const std::string& scope, bool keep_refreshed,
    const std::unordered_map<std::string, std::string>& optional_oauth_params,
    std::shared_ptr<HttpClient> client,
    std::optional<std::chrono::steady_clock::time_point> token_request_started_at);

}  // namespace iceberg::rest::auth::internal
