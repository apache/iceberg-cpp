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

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

#include "iceberg/catalog/rest/http_request.h"
#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/catalog/rest/type_fwd.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

/// \file iceberg/catalog/rest/auth/auth_session.h
/// \brief Authentication session interface for REST catalog.

namespace iceberg::rest::auth {

/// \brief OAuth2 metadata used to derive child authentication sessions.
struct ICEBERG_REST_EXPORT OAuth2SessionInfo {
  std::string token;
  std::string issued_token_type;
  std::string credential;
  std::string scope;
  std::string oauth2_server_uri;
  std::unordered_map<std::string, std::string> optional_oauth_params;
  std::unordered_map<std::string, std::string> headers;
};

/// \brief An authentication session that can authenticate outgoing HTTP requests.
class ICEBERG_REST_EXPORT AuthSession {
 public:
  virtual ~AuthSession() = default;

  /// \brief Authenticate an outgoing HTTP request.
  ///
  /// Returns a request with authentication information (e.g., an Authorization
  /// header) added. Implementations must be idempotent. The request is passed
  /// by value so callers can move request bodies into the authentication path.
  ///
  /// \param request The request to authenticate.
  /// \return The authenticated request on success, or one of:
  ///         - AuthenticationFailed: General authentication failure (invalid credentials,
  ///         etc.)
  ///         - TokenExpired: Authentication token has expired and needs refresh
  ///         - NotAuthorized: Not authenticated (401)
  ///         - IOError: Network or connection errors when reaching auth server
  ///         - RestError: HTTP errors from authentication service
  virtual Result<HttpRequest> Authenticate(HttpRequest request) = 0;

  /// \brief Return OAuth2 metadata when this is an OAuth2 session.
  virtual std::optional<OAuth2SessionInfo> OAuth2Info() const { return std::nullopt; }

  /// \brief Stop background refresh and release session resources.
  /// \return Status of the close operation.
  virtual Status Close() { return {}; }

  /// \brief Create a session with static headers.
  ///
  /// This factory method creates a session that adds a fixed set of headers to each
  /// request. It is suitable for authentication methods that use static credentials,
  /// such as Basic auth or static bearer tokens.
  ///
  /// \param headers The headers to add to each request for authentication.
  /// \return A new session that adds the given headers to requests.
  static std::shared_ptr<AuthSession> MakeDefault(
      std::unordered_map<std::string, std::string> headers);

  /// \brief Create an OAuth2 session with automatic token refresh.
  ///
  /// Refreshes expiring tokens in the background when keep_refreshed is true.
  /// Authenticate() uses the cached token without refreshing it.
  ///
  /// \param initial_token The initial token response from FetchToken().
  /// \param token_endpoint Full URL of the OAuth2 token endpoint for refresh.
  /// \param client_id OAuth2 client ID for refresh requests.
  /// \param client_secret OAuth2 client secret for re-fetch if refresh fails.
  /// \param scope OAuth2 scope for refresh requests.
  /// \param keep_refreshed Whether to schedule automatic token refresh.
  /// \param optional_oauth_params Optional OAuth params (audience, resource) for refresh.
  /// \param client HTTP client for making refresh requests. The session retains
  ///        ownership of the client.
  /// \param auth_executor Borrowed refresh executor; required if keep_refreshed is true.
  /// Ignored otherwise. Must enqueue asynchronously and outlive the session and its
  /// tasks.
  /// \return The session, or InvalidArgument if refresh is enabled without an executor.
  static Result<std::shared_ptr<AuthSession>> MakeOAuth2(
      const OAuthTokenResponse& initial_token, const std::string& token_endpoint,
      const std::string& client_id, const std::string& client_secret,
      const std::string& scope, bool keep_refreshed,
      const std::unordered_map<std::string, std::string>& optional_oauth_params,
      std::shared_ptr<HttpClient> client, Executor* auth_executor = nullptr);
};

}  // namespace iceberg::rest::auth
