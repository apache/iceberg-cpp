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
#include <memory>
#include <shared_mutex>
#include <utility>

#include "iceberg/catalog/rest/auth/auth_properties.h"
#include "iceberg/catalog/rest/auth/oauth2_util.h"
#include "iceberg/catalog/rest/auth/token_refresh_scheduler.h"
#include "iceberg/catalog/rest/http_client.h"

namespace iceberg::rest::auth {

namespace {

/// \brief Default implementation that adds static headers to requests.
class DefaultAuthSession : public AuthSession {
 public:
  explicit DefaultAuthSession(std::unordered_map<std::string, std::string> headers)
      : headers_(std::move(headers)) {}

  Status Authenticate(std::unordered_map<std::string, std::string>& headers) override {
    for (const auto& [key, value] : headers_) {
      headers.try_emplace(key, value);
    }
    return {};
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
  static std::shared_ptr<OAuth2AuthSession> Create(
      const OAuthTokenResponse& initial_token, Config config, HttpClient& client) {
    auto session = std::shared_ptr<OAuth2AuthSession>(
        new OAuth2AuthSession(std::move(config), client));
    session->SetInitialToken(initial_token);
    return session;
  }

  Status Authenticate(std::unordered_map<std::string, std::string>& headers) override {
    std::shared_lock lock(mutex_);
    for (const auto& [key, value] : headers_) {
      headers.try_emplace(key, value);
    }
    return {};
  }

  Status Close() override {
    bool expected = false;
    if (!closed_.compare_exchange_strong(expected, true)) {
      return {};  // Already closed
    }
    TokenRefreshScheduler::Instance().Cancel(scheduled_task_id_.load());
    return {};
  }

 private:
  OAuth2AuthSession(Config config, HttpClient& client)
      : config_(std::move(config)), client_(client) {}

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

    if (closed_.load()) return;

    // Build credential and properties once (invariant across retries)
    std::string credential = config_.client_id.empty()
                                 ? config_.client_secret
                                 : config_.client_id + ":" + config_.client_secret;

    // Use an empty session for the refresh request (no auth headers —
    // avoids circular dependency of using an expired token to refresh itself)
    auto empty_session = AuthSession::MakeDefault({});

    AuthProperties props;
    props.Set(AuthProperties::kCredential, credential);
    props.Set(AuthProperties::kScope, config_.scope);
    props.Set(AuthProperties::kOAuth2ServerUri, config_.token_endpoint);
    for (const auto& [key, value] : config_.optional_oauth_params) {
      props.mutable_configs().insert_or_assign(key, value);
    }

    auto result = FetchToken(client_, *empty_session, props);
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
    if (attempt + 1 < kMaxRetries) {
      auto next_backoff =
          std::min(std::chrono::duration_cast<std::chrono::milliseconds>(backoff * 2),
                   kMaxBackoff);
      std::weak_ptr<OAuth2AuthSession> weak_self = shared_from_this();
      TokenRefreshScheduler::Instance().Schedule(
          backoff,
          [weak_self = std::move(weak_self), next_attempt = attempt + 1, next_backoff] {
            if (auto self = weak_self.lock()) {
              self->DoRefreshAttempt(next_attempt, next_backoff);
            }
          });
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
    if (delay <= std::chrono::milliseconds::zero()) return;

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
  HttpClient& client_;
  std::atomic<uint64_t> scheduled_task_id_{0};
  std::atomic<bool> closed_{false};
};

}  // namespace

std::shared_ptr<AuthSession> AuthSession::MakeDefault(
    std::unordered_map<std::string, std::string> headers) {
  return std::make_shared<DefaultAuthSession>(std::move(headers));
}

std::shared_ptr<AuthSession> AuthSession::MakeOAuth2(
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
  return OAuth2AuthSession::Create(initial_token, std::move(config), client);
}

}  // namespace iceberg::rest::auth
