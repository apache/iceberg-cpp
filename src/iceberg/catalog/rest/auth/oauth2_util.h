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

#include <array>
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

inline constexpr std::string_view kAuthorizationHeader = "Authorization";
inline constexpr std::string_view kBearerPrefix = "Bearer ";

struct ICEBERG_REST_EXPORT OAuth2Token {
  std::string token_type;
  std::string token;
};

struct ICEBERG_REST_EXPORT TokenExchangeRequest {
  std::string oauth2_server_uri;
  OAuth2Token subject;
  std::optional<OAuth2Token> actor;
  std::string scope;
  std::unordered_map<std::string, std::string> optional_oauth_params;
};

/// \brief Fetch an OAuth2 token using the client_credentials grant type.
///
/// \param client HTTP client to use for the request.
/// \param session Auth session for the request headers.
/// \param properties Auth configuration containing credential, scope,
///        token endpoint, and optional OAuth params.
/// \return The token response or an error.
ICEBERG_REST_EXPORT Result<OAuthTokenResponse> FetchToken(
    HttpClient& client, AuthSession& session, const AuthProperties& properties);

/// \brief Build auth headers from a token string.
///
/// \param token Bearer token string (may be empty).
/// \return Headers map with Authorization header if token is non-empty.
ICEBERG_REST_EXPORT std::unordered_map<std::string, std::string> AuthHeaders(
    const std::string& token);

/// \brief Return whether a token type is a supported RFC token type.
ICEBERG_REST_EXPORT bool IsValidTokenType(std::string_view token_type);

/// \brief Return the preferred order for typed OAuth tokens.
ICEBERG_REST_EXPORT std::array<std::string_view, 5> TokenPreferenceOrder();

/// \brief Find the highest-preference typed OAuth token in credentials.
ICEBERG_REST_EXPORT std::optional<OAuth2Token> FindPreferredTypedToken(
    const std::unordered_map<std::string, std::string>& credentials);

/// \brief Filter table session properties to allowed OAuth credentials.
ICEBERG_REST_EXPORT std::unordered_map<std::string, std::string>
FilterTableSessionProperties(
    const std::unordered_map<std::string, std::string>& properties);

/// \brief Build RFC 8693 token exchange form data.
///
/// \param request Token exchange request values.
/// \return Form data or an error if token values are invalid.
ICEBERG_REST_EXPORT Result<std::unordered_map<std::string, std::string>>
BuildTokenExchangeForm(const TokenExchangeRequest& request);

/// \brief Exchange an OAuth2 token using the RFC 8693 grant type.
///
/// \param client HTTP client to use for the request.
/// \param session Auth session for the request headers.
/// \param extra_headers Request headers applied before session authentication.
/// \param request Token exchange endpoint and form values.
/// \return The token response or an error.
ICEBERG_REST_EXPORT Result<OAuthTokenResponse> ExchangeToken(
    HttpClient& client, AuthSession& session,
    const std::unordered_map<std::string, std::string>& extra_headers,
    const TokenExchangeRequest& request);

/// \brief Extract expiration time from a JWT token.
///
/// Decodes the JWT payload (base64url) and reads the "exp" claim.
/// Returns std::nullopt if the token is not a valid JWT or has no "exp" claim.
///
/// \param token A token string. If it is a JWT (three dot-separated base64url
///        segments), the "exp" claim is extracted from the payload.
/// \return Expiration time as milliseconds since epoch, or std::nullopt.
ICEBERG_REST_EXPORT std::optional<int64_t> ExpiresAtMillis(std::string_view token);

}  // namespace iceberg::rest::auth
