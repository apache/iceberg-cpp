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
#include <string>

#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/catalog/rest/type_fwd.h"
#include "iceberg/result.h"

/// \file iceberg/catalog/rest/auth/oauth2_util.h
/// \brief OAuth2 token utilities for REST catalog authentication.

namespace iceberg::rest::auth {

/// \brief Response from an OAuth2 token endpoint.
struct ICEBERG_REST_EXPORT OAuthTokenResponse {
  std::string access_token;   // required
  std::string token_type;     // required, typically "bearer"
  int64_t expires_in = 0;     // optional, seconds until expiration
  std::string refresh_token;  // optional
  std::string scope;          // optional

  /// \brief Validates the token response.
  Status Validate() const;

  bool operator==(const OAuthTokenResponse&) const = default;
};

/// \brief Parse an OAuthTokenResponse from a JSON string.
///
/// \param json_str The JSON string to parse.
/// \return The parsed token response or an error.
ICEBERG_REST_EXPORT Result<OAuthTokenResponse> OAuthTokenResponseFromJsonString(
    const std::string& json_str);

/// \brief Fetch an OAuth2 token using the client_credentials grant type.
///
/// Sends a POST request with form-encoded body to the token endpoint:
///   grant_type=client_credentials&client_id=...&client_secret=...&scope=...
///
/// \param client HTTP client to use for the request.
/// \param token_endpoint Full URL of the OAuth2 token endpoint.
/// \param client_id OAuth2 client ID.
/// \param client_secret OAuth2 client secret.
/// \param scope OAuth2 scope to request.
/// \param session Auth session for the request (typically a no-op session).
/// \return The token response or an error.
ICEBERG_REST_EXPORT Result<OAuthTokenResponse> FetchToken(
    HttpClient& client, const std::string& token_endpoint, const std::string& client_id,
    const std::string& client_secret, const std::string& scope, AuthSession& session);

/// \brief Refresh an expired access token using a refresh_token grant.
ICEBERG_REST_EXPORT Result<OAuthTokenResponse> RefreshToken(
    HttpClient& client, const std::string& token_endpoint, const std::string& client_id,
    const std::string& refresh_token, const std::string& scope, AuthSession& session);

/// \brief Exchange a token for a scoped token using RFC 8693 Token Exchange.
ICEBERG_REST_EXPORT Result<OAuthTokenResponse> ExchangeToken(
    HttpClient& client, const std::string& token_endpoint,
    const std::string& subject_token, const std::string& subject_token_type,
    const std::string& scope, AuthSession& session);

}  // namespace iceberg::rest::auth
