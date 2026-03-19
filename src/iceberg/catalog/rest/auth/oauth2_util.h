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

}  // namespace iceberg::rest::auth
