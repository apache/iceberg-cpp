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

#include "iceberg/catalog/rest/auth/auth_session.h"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <utility>

#include "iceberg/catalog/rest/auth/auth_properties.h"
#include "iceberg/catalog/rest/auth/oauth2_util.h"
#include "iceberg/catalog/rest/auth/token_refresh_scheduler.h"
#include "iceberg/catalog/rest/http_client.h"
#include "iceberg/util/macros.h"

namespace iceberg::rest::auth {

namespace {

/// \brief Default implementation that adds static headers to requests.
class DefaultAuthSession : public AuthSession {
 public:
  explicit DefaultAuthSession(std::unordered_map<std::string, std::string> headers)
      : headers_(std::move(headers)) {}

  Result<HttpRequest> Authenticate(HttpRequest request) override {
    for (const auto& [key, value] : headers_) {
      request.headers.try_emplace(key, value);
    }
    return request;
  }

 private:
  std::unordered_map<std::string, std::string> headers_;
};

/// \brief OAuth2 session with automatic token refresh.
class OAuth2AuthSession : public AuthSession,
                          public std::enable_shared_from_this<OAuth2AuthSession> {
 public:
  struct Config {
    std::string token_endpoint;
    std::string client_id;
    std::string client_secret;
    std::string scope;
    std::unordered_map<std::string, std::string> optional_oauth_params;
    bool keep_refreshed;
  };

  /// \brief Create an OAuth2 session and optionally schedule refresh.
  static Result<std::shared_ptr<OAuth2AuthSession>> Make(
      const OAuthTokenResponse& initial_token, Config config, HttpClient& client) {
    ICEBERG_ASSIGN_OR_RAISE(auto refresh_properties, MakeRefreshProperties(config));
    auto session = std::shared_ptr<OAuth2AuthSession>(
        new OAuth2AuthSession(std::move(config), std::move(refresh_properties), client));
    session->SetInitialToken(initial_token);
    return session;
  }

  Result<HttpRequest> Authenticate(HttpRequest request) override {
    std::shared_lock lock(mutex_);
    for (const auto& [key, value] : headers_) {
      request.headers.try_emplace(key, value);
    }
    return request;
  }

  Status Close() override { return CloseImpl(); }

  ~OAuth2AuthSession() override { std::ignore = CloseImpl(); }

 private:
  OAuth2AuthSession(Config config, AuthProperties refresh_properties, HttpClient& client)
      : config_(std::move(config)),
        refresh_properties_(std::move(refresh_properties)),
        client_(client) {}

  Status CloseImpl() {
    bool expected = false;
    if (!closed_.compare_exchange_strong(expected, true)) {
      return {};  // Already closed
    }
    TokenRefreshScheduler::Instance().Cancel(scheduled_task_id_.exchange(0));
    std::unique_lock lock(refresh_mutex_);
    refresh_cv_.wait(lock, [this] { return active_refresh_count_ == 0; });
    TokenRefreshScheduler::Instance().Cancel(scheduled_task_id_.exchange(0));
    return {};
  }

  static Result<AuthProperties> MakeRefreshProperties(const Config& config) {
    std::unordered_map<std::string, std::string> properties =
        config.optional_oauth_params;
    properties[AuthProperties::kCredential.key()] =
        config.client_id.empty() ? config.client_secret
                                 : config.client_id + ":" + config.client_secret;
    properties[AuthProperties::kScope.key()] = config.scope;
    properties[AuthProperties::kOAuth2ServerUri.key()] = config.token_endpoint;

    return AuthProperties::FromProperties(properties);
  }

  class RefreshAttemptGuard {
   public:
    explicit RefreshAttemptGuard(OAuth2AuthSession& session) : session_(session) {
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
    OAuth2AuthSession& session_;
  };

  void SetInitialToken(const OAuthTokenResponse& token_response) {
    token_ = token_response.access_token;
    headers_ = {{std::string(kAuthorizationHeader), std::string(kBearerPrefix) + token_}};

    // Determine expiration time
    if (token_response.expires_in_secs.has_value()) {
      expires_at_ = std::chrono::steady_clock::now() +
                    std::chrono::seconds(*token_response.expires_in_secs);
    } else if (auto exp_ms = ExpiresAtMillis(token_); exp_ms.has_value()) {
      // Convert absolute epoch millis to steady_clock time_point
      auto now_sys = std::chrono::system_clock::now();
      auto now_steady = std::chrono::steady_clock::now();
      auto exp_sys =
          std::chrono::system_clock::time_point(std::chrono::milliseconds(*exp_ms));
      expires_at_ = now_steady + (exp_sys - now_sys);
    }

    if (config_.keep_refreshed &&
        expires_at_ != std::chrono::steady_clock::time_point{}) {
      ScheduleRefresh();
    }
  }

  void DoRefresh() { DoRefreshAttempt(0, std::chrono::milliseconds(200)); }

  /// \brief Single refresh attempt. On failure, schedules a retry via the
  /// scheduler (non-blocking) instead of sleeping on the worker thread.
  void DoRefreshAttempt(int attempt, std::chrono::milliseconds backoff) {
    static constexpr int kMaxRetries = 5;
    static constexpr auto kMaxBackoff = std::chrono::milliseconds(10'000);

    RefreshAttemptGuard guard(*this);
    if (closed_.load()) return;

    // Use an empty session for the refresh request (no auth headers —
    // avoids circular dependency of using an expired token to refresh itself)
    auto empty_session = AuthSession::MakeDefault({});

    auto result = FetchToken(client_, *empty_session, refresh_properties_);
    if (result.has_value()) {
      auto& response = result.value();
      {
        std::unique_lock lock(mutex_);
        token_ = response.access_token;
        headers_ = {
            {std::string(kAuthorizationHeader), std::string(kBearerPrefix) + token_}};

        // Reset before deriving new expiry
        expires_at_ = std::chrono::steady_clock::time_point{};

        if (response.expires_in_secs.has_value()) {
          expires_at_ = std::chrono::steady_clock::now() +
                        std::chrono::seconds(*response.expires_in_secs);
        } else if (auto exp_ms = ExpiresAtMillis(token_); exp_ms.has_value()) {
          auto now_sys = std::chrono::system_clock::now();
          auto now_steady = std::chrono::steady_clock::now();
          auto exp_sys =
              std::chrono::system_clock::time_point(std::chrono::milliseconds(*exp_ms));
          expires_at_ = now_steady + (exp_sys - now_sys);
        }
      }
      // Note: ScheduleRefresh must be called outside the lock.
      ScheduleRefresh();
      return;  // Success
    }

    // Schedule retry with exponential backoff (non-blocking)
    if (attempt + 1 < kMaxRetries && !closed_.load()) {
      auto next_backoff =
          std::min(std::chrono::duration_cast<std::chrono::milliseconds>(backoff * 2),
                   kMaxBackoff);
      std::weak_ptr<OAuth2AuthSession> weak_self = shared_from_this();
      auto retry_id = TokenRefreshScheduler::Instance().Schedule(
          backoff,
          [weak_self = std::move(weak_self), next_attempt = attempt + 1, next_backoff] {
            if (auto self = weak_self.lock()) {
              self->DoRefreshAttempt(next_attempt, next_backoff);
            }
          });
      scheduled_task_id_.store(retry_id);
    }
    // All retries exhausted — stop refreshing silently.
    // Next request will use the expired token; server returns 401.
  }

  /// \brief Schedule the next token refresh based on expiration time.
  ///
  /// Must be called outside any lock on mutex_ (CalculateRefreshDelay
  /// acquires shared_lock internally).
  void ScheduleRefresh() {
    if (!config_.keep_refreshed || closed_.load()) return;

    auto delay = CalculateRefreshDelay();
    if (delay < std::chrono::milliseconds::zero()) return;

    std::weak_ptr<OAuth2AuthSession> weak_self = shared_from_this();
    auto new_id = TokenRefreshScheduler::Instance().Schedule(
        delay, [weak_self = std::move(weak_self)] {
          if (auto self = weak_self.lock()) {
            self->DoRefresh();
          }
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
    // Refresh window: 10% of remaining time, capped at 5 minutes
    auto refresh_window = std::min(expires_in / 10, std::chrono::milliseconds(300'000));
    auto wait_time = expires_in - refresh_window;
    return std::max(wait_time, std::chrono::milliseconds(10));
  }

  mutable std::shared_mutex mutex_;  // protects token_, headers_, expires_at_
  std::string token_;
  std::unordered_map<std::string, std::string> headers_;
  std::chrono::steady_clock::time_point expires_at_{};

  Config config_;
  AuthProperties refresh_properties_;
  HttpClient& client_;  // It should outlive the session
  std::atomic<uint64_t> scheduled_task_id_{0};
  std::atomic<bool> closed_{false};
  std::mutex refresh_mutex_;
  std::condition_variable refresh_cv_;
  int active_refresh_count_ = 0;
};

}  // namespace

std::shared_ptr<AuthSession> AuthSession::MakeDefault(
    std::unordered_map<std::string, std::string> headers) {
  return std::make_shared<DefaultAuthSession>(std::move(headers));
}

Result<std::shared_ptr<AuthSession>> AuthSession::MakeOAuth2(
    const OAuthTokenResponse& initial_token, const std::string& token_endpoint,
    const std::string& client_id, const std::string& client_secret,
    const std::string& scope, bool keep_refreshed,
    const std::unordered_map<std::string, std::string>& optional_oauth_params,
    HttpClient& client) {
  OAuth2AuthSession::Config config{
      .token_endpoint = token_endpoint,
      .client_id = client_id,
      .client_secret = client_secret,
      .scope = scope,
      .optional_oauth_params = optional_oauth_params,
      .keep_refreshed = keep_refreshed,
  };
  ICEBERG_ASSIGN_OR_RAISE(
      auto session, OAuth2AuthSession::Make(initial_token, std::move(config), client));
  return std::static_pointer_cast<AuthSession>(std::move(session));
}

}  // namespace iceberg::rest::auth
