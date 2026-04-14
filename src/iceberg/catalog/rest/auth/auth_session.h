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
#include <string>
#include <unordered_map>

#include "iceberg/catalog/rest/endpoint.h"
#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/catalog/rest/type_fwd.h"
#include "iceberg/result.h"

/// \file iceberg/catalog/rest/auth/auth_session.h
/// \brief Authentication session interface for REST catalog.

namespace iceberg::rest::auth {

/// \brief An outgoing HTTP request passed through an AuthSession. Mirrors the
/// HTTPRequest type used by the Java reference implementation so signing
/// implementations like SigV4 can operate on method, url, headers, and body
/// as a single value.
struct ICEBERG_REST_EXPORT HTTPRequest {
  HttpMethod method = HttpMethod::kGet;
  std::string url;
  std::unordered_map<std::string, std::string> headers;
  std::string body;
};

/// \brief An authentication session that can authenticate outgoing HTTP requests.
class ICEBERG_REST_EXPORT AuthSession {
 public:
  virtual ~AuthSession() = default;

  /// \brief Authenticate an outgoing HTTP request.
  ///
  /// Returns a new request with authentication information (e.g., an
  /// Authorization header) added. Implementations must be idempotent and must
  /// not mutate the input request.
  ///
  /// \param request The request to authenticate.
  /// \return The authenticated request on success, or one of:
  ///         - AuthenticationFailed: General authentication failure (invalid credentials,
  ///         etc.)
  ///         - TokenExpired: Authentication token has expired and needs refresh
  ///         - NotAuthorized: Not authenticated (401)
  ///         - IOError: Network or connection errors when reaching auth server
  ///         - RestError: HTTP errors from authentication service
  virtual Result<HTTPRequest> Authenticate(const HTTPRequest& request) = 0;

  /// \brief Close the session and release any resources.
  ///
  /// This method is called when the session is no longer needed. For stateful
  /// sessions (e.g., OAuth2 with token refresh), this should stop any background
  /// threads and release resources.
  ///
  /// \return Status indicating success or failure of closing the session.
  virtual Status Close() { return {}; }

  /// \brief Create a default session with static headers.
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
  /// This factory method creates a session that holds an access token and
  /// optionally a refresh token. When Authenticate() is called and the token
  /// is expired, it transparently refreshes the token before setting the
  /// Authorization header.
  ///
  /// \param initial_token The initial token response from FetchToken().
  /// \param token_endpoint Full URL of the OAuth2 token endpoint for refresh.
  /// \param client_id OAuth2 client ID for refresh requests.
  /// \param client_secret OAuth2 client secret for re-fetch if refresh fails.
  /// \param scope OAuth2 scope for refresh requests.
  /// \param client HTTP client for making refresh requests.
  /// \return A new session that manages token lifecycle automatically.
  static std::shared_ptr<AuthSession> MakeOAuth2(const OAuthTokenResponse& initial_token,
                                                 const std::string& token_endpoint,
                                                 const std::string& client_id,
                                                 const std::string& client_secret,
                                                 const std::string& scope,
                                                 HttpClient& client);
};

}  // namespace iceberg::rest::auth
