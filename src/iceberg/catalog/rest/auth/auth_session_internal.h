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
#include <chrono>
#include <condition_variable>
#include <exception>
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
#include "iceberg/catalog/rest/http_client.h"
#include "iceberg/logging/log_macros.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/retry_util.h"
#include "iceberg/util/scheduled_executor_internal.h"

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
    /// Borrowed executor for background refresh. Null disables refresh.
    Executor* refresh_executor = nullptr;
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
        client_(std::move(client)) {
    if (config_.refresh_executor != nullptr) {
      scheduler_.emplace(*config_.refresh_executor);
    }
  }

  Status CloseImpl() {
    // Stop queued refreshes before shutting down the scheduler.
    {
      std::lock_guard lock(refresh_mutex_);
      closed_ = true;
    }
    if (scheduler_) {
      scheduler_->Shutdown();
    }
    std::unique_lock lock(refresh_mutex_);
    refresh_cv_.wait(lock, [this] { return active_refresh_count_ == 0; });
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
      active_ = !session_.closed_;
      if (active_) {
        ++session_.active_refresh_count_;
      }
    }

    ~RefreshAttemptGuard() {
      if (!active_) {
        return;
      }
      bool notify = false;
      {
        std::lock_guard lock(session_.refresh_mutex_);
        notify = --session_.active_refresh_count_ == 0;
      }
      if (notify) {
        session_.refresh_cv_.notify_all();
      }
    }

    bool IsActive() const { return active_; }

   private:
    OAuth2Session& session_;
    bool active_ = false;
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
    if (!scheduler_) {
      return;
    }
    // Refresh an already-expired initial token immediately.
    if (auto delay = CalculateRefreshDelay()) {
      ScheduleRefresh(*delay);
    }
  }

  // Complete one refresh, including HTTP retries, on the executor's worker.
  std::optional<std::chrono::milliseconds> Refresh() {
    RefreshAttemptGuard guard(*this);
    if (!guard.IsActive()) {
      return std::nullopt;
    }

    auto result = RetryRunner<retry::StopRetryOn<>>({.num_retries = 5}).Run([this] {
      return RefreshToken();
    });
    if (!result.has_value()) {
      ICEBERG_LOG_WARN("Failed to refresh OAuth2 token: {}", result.error().message);
      return std::nullopt;
    }
    return *result;
  }

  Result<std::optional<std::chrono::milliseconds>> RefreshToken() {
    {
      std::lock_guard lock(refresh_mutex_);
      if (closed_) {
        return std::nullopt;
      }
    }
    try {
      auto request_started_at = std::chrono::steady_clock::now();
      auto empty_session = AuthSession::MakeDefault({});
      // TODO(lishuxu): Honor token-exchange-enabled when refreshing tokens.
      ICEBERG_ASSIGN_OR_RAISE(
          auto response,
          OAuth2Util::FetchToken(*client_, *empty_session, refresh_properties_));
      {
        std::unique_lock lock(mutex_);
        UpdateTokenState(response, request_started_at);
      }
      auto delay = CalculateRefreshDelay();
      if (delay.has_value() && *delay <= std::chrono::milliseconds::zero()) {
        return TokenExpired("Refreshed OAuth2 token is already expired");
      }
      return delay;
    } catch (const std::exception& e) {
      return UnknownError("OAuth2 token refresh threw: {}", e.what());
    } catch (...) {
      return UnknownError("OAuth2 token refresh threw a non-standard exception");
    }
  }

  static constexpr int kMaxSubmissionRetries = 5;

  void ScheduleRefresh(std::chrono::milliseconds delay, int submission_retries = 0) {
    auto weak_self = weak_from_this();
    ExecutorTask refresh([weak_self] {
      if (auto self = weak_self.lock()) {
        if (auto next_delay = self->Refresh()) {
          self->ScheduleRefresh(*next_delay);
        }
      }
    });
    ::iceberg::internal::TaskSubmissionCallback on_submission(
        [weak_self, submission_retries](Status status) {
          if (status.has_value()) {
            return;
          }
          if (submission_retries >= kMaxSubmissionRetries) {
            ICEBERG_LOG_WARN(
                "Stopping OAuth2 token refresh after repeated submission failures: {}",
                status.error().message);
            return;
          }
          if (auto self = weak_self.lock()) {
            // Requeue on the timer; HTTP retries start only after Submit succeeds.
            auto retry_delay = std::chrono::milliseconds(200) * (1 << submission_retries);
            self->ScheduleRefresh(retry_delay, submission_retries + 1);
          }
        });
    scheduler_->Schedule(delay, std::move(refresh), std::move(on_submission));
  }

  /// Returns nullopt if expiration is unknown.
  std::optional<std::chrono::milliseconds> CalculateRefreshDelay() const {
    std::shared_lock lock(mutex_);
    if (expires_at_ == std::chrono::steady_clock::time_point{}) {
      return std::nullopt;
    }
    auto now = std::chrono::steady_clock::now();
    if (expires_at_ <= now) {
      return std::chrono::milliseconds::zero();
    }
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
  bool closed_ = false;
  std::mutex refresh_mutex_;
  std::condition_variable refresh_cv_;
  int active_refresh_count_ = 0;
  std::optional<::iceberg::internal::ScheduledExecutor> scheduler_;
};

/// Returns InvalidArgument if keep_refreshed is true without an executor.
Result<std::shared_ptr<AuthSession>> MakeOAuth2Session(
    const OAuthTokenResponse& initial_token, const std::string& token_endpoint,
    const std::string& client_id, const std::string& client_secret,
    const std::string& scope, bool keep_refreshed,
    const std::unordered_map<std::string, std::string>& optional_oauth_params,
    std::shared_ptr<HttpClient> client,
    std::optional<std::chrono::steady_clock::time_point> token_request_started_at,
    Executor* refresh_executor);

}  // namespace iceberg::rest::auth::internal
