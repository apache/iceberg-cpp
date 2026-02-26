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

#include <utility>

#include "iceberg/catalog/rest/auth/oauth2_util.h"

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

}  // namespace

std::shared_ptr<AuthSession> AuthSession::MakeDefault(
    std::unordered_map<std::string, std::string> headers) {
  return std::make_shared<DefaultAuthSession>(std::move(headers));
}

std::shared_ptr<AuthSession> AuthSession::MakeOAuth2(
    const OAuthTokenResponse& initial_token, const std::string& /*token_endpoint*/,
    const std::string& /*client_id*/, const std::string& /*client_secret*/,
    const std::string& /*scope*/, HttpClient& /*client*/) {
  // TODO(lishuxu): Replace with OAuth2AuthSession that:
  //   - Stores access_token, refresh_token, expires_in, and token_endpoint
  //   - Checks token expiration in Authenticate() and transparently refreshes
  //     using RefreshToken() (or re-fetches via client_credentials if no
  //     refresh_token is available)
  // For now, fall back to a static session with the initial token.
  return MakeDefault({{"Authorization", "Bearer " + initial_token.access_token}});
}

}  // namespace iceberg::rest::auth
