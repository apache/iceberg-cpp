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

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/catalog/rest/type_fwd.h"
#include "iceberg/catalog/rest/types.h"
#include "iceberg/result.h"

/// \file iceberg/catalog/rest/auth/oauth2_util.h
/// \brief OAuth2 token utilities for REST catalog authentication.

namespace iceberg::rest::auth {

/// \brief OAuth2 token and authentication utilities.
class ICEBERG_REST_EXPORT OAuth2Util {
 public:
  OAuth2Util() = delete;

  /// \brief Fetch an OAuth2 token using the client_credentials grant type.
  ///
  /// \param client HTTP client to use for the request.
  /// \param session Auth session for the request headers.
  /// \param properties Auth configuration containing credential, scope,
  ///        token endpoint, and optional OAuth params.
  /// \return The token response or an error.
  static Result<OAuthTokenResponse> FetchToken(HttpClient& client, AuthSession& session,
                                               const AuthProperties& properties);

  /// \brief Build auth headers from a token string.
  ///
  /// \param token Bearer token string (may be empty).
  /// \return Headers map with Authorization header if token is non-empty.
  static std::unordered_map<std::string, std::string> AuthHeaders(
      const std::string& token);

  /// \brief Exchange an OAuth2 token using the RFC 8693 grant type.
  ///
  /// \param client HTTP client to use for the request.
  /// \param session Auth session for the request headers.
  /// \param extra_headers Request headers applied before session authentication.
  /// \param subject_token Subject token to exchange.
  /// \param subject_token_type Subject token type.
  /// \param actor_token Optional actor token.
  /// \param actor_token_type Optional actor token type.
  /// \param scope OAuth2 scope.
  /// \param oauth2_server_uri Token exchange endpoint.
  /// \param optional_params Optional OAuth parameters.
  /// \return The token response or an error.
  static Result<OAuthTokenResponse> ExchangeToken(
      HttpClient& client, AuthSession& session,
      const std::unordered_map<std::string, std::string>& extra_headers,
      const std::string& subject_token, const std::string& subject_token_type,
      const std::optional<std::string>& actor_token,
      const std::optional<std::string>& actor_token_type, const std::string& scope,
      const std::string& oauth2_server_uri,
      const std::unordered_map<std::string, std::string>& optional_params);

  /// \brief Extract expiration time from a JWT token.
  ///
  /// Decodes the JWT payload (base64url) and reads the "exp" claim.
  /// Returns std::nullopt if the token is not a valid JWT or has no "exp" claim.
  ///
  /// \param token A token string containing three dot-separated JWT segments.
  /// \return Expiration time as milliseconds since epoch, or std::nullopt.
  static std::optional<int64_t> ExpiresAtMillis(std::string_view token);

 private:
  static Result<std::unordered_map<std::string, std::string>> TokenExchangeRequest(
      const std::string& subject_token, const std::string& subject_token_type,
      const std::optional<std::string>& actor_token,
      const std::optional<std::string>& actor_token_type, const std::string& scope,
      const std::unordered_map<std::string, std::string>& optional_params);
};

}  // namespace iceberg::rest::auth
