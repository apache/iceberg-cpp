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
#include "iceberg/catalog/rest/auth/auth_session_internal.h"
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
    std::shared_ptr<HttpClient> client) {
  return internal::MakeOAuth2Session(
      initial_token, token_endpoint, client_id, client_secret, scope, keep_refreshed,
      optional_oauth_params, std::move(client), std::nullopt);
}

Result<std::shared_ptr<AuthSession>> internal::MakeOAuth2Session(
    const OAuthTokenResponse& initial_token, const std::string& token_endpoint,
    const std::string& client_id, const std::string& client_secret,
    const std::string& scope, bool keep_refreshed,
    const std::unordered_map<std::string, std::string>& optional_oauth_params,
    std::shared_ptr<HttpClient> client,
    std::optional<std::chrono::steady_clock::time_point> token_request_started_at) {
  internal::OAuth2Session::Config config{
      .token_endpoint = token_endpoint,
      .client_id = client_id,
      .client_secret = client_secret,
      .scope = scope,
      .optional_oauth_params = optional_oauth_params,
      .keep_refreshed = keep_refreshed,
  };
  ICEBERG_ASSIGN_OR_RAISE(auto session, internal::OAuth2Session::Make(
                                            initial_token, std::move(config),
                                            std::move(client), token_request_started_at));
  return std::static_pointer_cast<AuthSession>(std::move(session));
}

}  // namespace iceberg::rest::auth
