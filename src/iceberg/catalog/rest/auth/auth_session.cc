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

#include <chrono>
#include <mutex>
#include <utility>

#include "iceberg/catalog/rest/auth/oauth2_util.h"
#include "iceberg/catalog/rest/error_handlers.h"
#include "iceberg/catalog/rest/http_client.h"
#include "iceberg/catalog/rest/json_serde_internal.h"
#include "iceberg/catalog/rest/types.h"
#include "iceberg/json_serde_internal.h"
#include "iceberg/util/macros.h"

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

using SteadyClock = std::chrono::steady_clock;
using TimePoint = SteadyClock::time_point;

/// \brief OAuth2 session with transparent token refresh.
///
/// Thread-safe: multiple concurrent callers of Authenticate() are serialized
/// through a mutex. Only the first caller that detects an expired token
/// performs the refresh; the rest wait and reuse the result.
class OAuth2AuthSession : public AuthSession {
 public:
  OAuth2AuthSession(OAuthTokenResponse initial_token, std::string token_endpoint,
                    std::string client_id, std::string client_secret, std::string scope,
                    HttpClient& client, std::chrono::seconds expiry_margin)
      : token_(std::move(initial_token)),
        token_endpoint_(std::move(token_endpoint)),
        client_id_(std::move(client_id)),
        client_secret_(std::move(client_secret)),
        scope_(std::move(scope)),
        client_(client),
        expiry_margin_(expiry_margin) {
    UpdateExpiry();
  }

  Status Authenticate(std::unordered_map<std::string, std::string>& headers) override {
    std::lock_guard lock(mutex_);
    if (IsExpired()) {
      ICEBERG_RETURN_UNEXPECTED(DoRefresh());
    }
    headers.insert_or_assign(std::string(kAuthorizationHeader),
                             std::string(kBearerPrefix) + token_.access_token);
    return {};
  }

 private:
  bool IsExpired() const {
    if (expires_at_ == TimePoint{}) {
      return false;  // no expiry info → assume valid
    }
    return SteadyClock::now() >= expires_at_;
  }

  void UpdateExpiry() {
    if (token_.expires_in_secs.has_value() && *token_.expires_in_secs > 0) {
      expires_at_ = SteadyClock::now() + std::chrono::seconds(*token_.expires_in_secs) -
                    expiry_margin_;
    } else {
      expires_at_ = {};  // no expiry
    }
  }

  /// Try refresh_token grant first, fall back to client_credentials.
  Status DoRefresh() {
    // Use a noop session for the refresh request itself to avoid recursion.
    auto noop = AuthSession::MakeDefault({});

    if (!token_.refresh_token.empty()) {
      auto result = RefreshToken(client_, *noop, token_endpoint_, client_id_,
                                 token_.refresh_token, scope_);
      if (result.has_value()) {
        token_ = std::move(*result);
        UpdateExpiry();
        return {};
      }
      // refresh_token grant failed, fall through to client_credentials
    }

    // Re-fetch using client_credentials
    if (!client_secret_.empty()) {
      // Build a minimal AuthProperties-compatible fetch
      auto noop2 = AuthSession::MakeDefault({});
      std::unordered_map<std::string, std::string> form_data{
          {"grant_type", "client_credentials"},
          {"client_id", client_id_},
          {"client_secret", client_secret_},
      };
      if (!scope_.empty()) {
        form_data.emplace("scope", scope_);
      }
      auto response = client_.PostForm(token_endpoint_, form_data, /*headers=*/{},
                                       *DefaultErrorHandler::Instance(), *noop2);
      if (!response.has_value()) {
        return AuthenticationFailed("Failed to refresh OAuth2 token: {}",
                                    response.error().message);
      }
      auto json = FromJsonString(response->body());
      if (!json.has_value()) {
        return AuthenticationFailed("Failed to parse OAuth2 token response");
      }
      auto token_result = FromJson<OAuthTokenResponse>(*json);
      if (!token_result.has_value()) {
        return AuthenticationFailed("Failed to deserialize OAuth2 token response");
      }
      token_ = std::move(*token_result);
      UpdateExpiry();
      return {};
    }

    return AuthenticationFailed(
        "Failed to refresh OAuth2 token: no refresh_token "
        "or client_secret available");
  }

  std::mutex mutex_;
  OAuthTokenResponse token_;
  TimePoint expires_at_{};

  const std::string token_endpoint_;
  const std::string client_id_;
  const std::string client_secret_;
  const std::string scope_;
  HttpClient& client_;
  const std::chrono::seconds expiry_margin_;
};

}  // namespace

std::shared_ptr<AuthSession> AuthSession::MakeDefault(
    std::unordered_map<std::string, std::string> headers) {
  return std::make_shared<DefaultAuthSession>(std::move(headers));
}

std::shared_ptr<AuthSession> AuthSession::MakeOAuth2(
    const OAuthTokenResponse& initial_token, const std::string& token_endpoint,
    const std::string& client_id, const std::string& client_secret,
    const std::string& scope, HttpClient& client, int64_t expiry_margin_seconds) {
  return std::make_shared<OAuth2AuthSession>(initial_token, token_endpoint, client_id,
                                             client_secret, scope, client,
                                             std::chrono::seconds(expiry_margin_seconds));
}

}  // namespace iceberg::rest::auth
