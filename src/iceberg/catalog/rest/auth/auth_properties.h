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

#include <string_view>

/// \file iceberg/catalog/rest/auth/auth_properties.h
/// \brief Property keys and constants for REST catalog authentication.

namespace iceberg::rest::auth {

/// \brief Property keys and constants for authentication configuration.
///
/// This struct defines all the property keys used to configure authentication
/// for the REST catalog. It follows the same naming conventions as Java Iceberg.
struct AuthProperties {
  /// \brief Property key for specifying the authentication type.
  static constexpr std::string_view kAuthType = "rest.auth.type";
  /// \brief Authentication type: no authentication.
  static constexpr std::string_view kAuthTypeNone = "none";
  /// \brief Authentication type: HTTP Basic authentication.
  static constexpr std::string_view kAuthTypeBasic = "basic";
  /// \brief Authentication type: OAuth2 authentication.
  static constexpr std::string_view kAuthTypeOAuth2 = "oauth2";
  /// \brief Authentication type: AWS SigV4 authentication.
  static constexpr std::string_view kAuthTypeSigV4 = "sigv4";
  /// \brief Property key for Basic auth username.
  static constexpr std::string_view kBasicUsername = "rest.auth.basic.username";
  /// \brief Property key for Basic auth password.
  static constexpr std::string_view kBasicPassword = "rest.auth.basic.password";
  /// \brief Property key for OAuth2 token (bearer token).
  static constexpr std::string_view kOAuth2Token = "token";
  /// \brief Property key for OAuth2 credential (client_id:client_secret).
  static constexpr std::string_view kOAuth2Credential = "credential";
  /// \brief Property key for OAuth2 scope.
  static constexpr std::string_view kOAuth2Scope = "scope";
  /// \brief Property key for OAuth2 server URI.
  static constexpr std::string_view kOAuth2ServerUri = "oauth2-server-uri";
  /// \brief Property key for enabling token refresh.
  static constexpr std::string_view kOAuth2TokenRefreshEnabled = "token-refresh-enabled";
  /// \brief Default OAuth2 scope for catalog operations.
  static constexpr std::string_view kOAuth2DefaultScope = "catalog";
  /// \brief Property key for SigV4 region.
  static constexpr std::string_view kSigV4Region = "rest.auth.sigv4.region";
  /// \brief Property key for SigV4 service name.
  static constexpr std::string_view kSigV4Service = "rest.auth.sigv4.service";
  /// \brief Property key for SigV4 delegate auth type.
  static constexpr std::string_view kSigV4DelegateAuthType =
      "rest.auth.sigv4.delegate-auth-type";
};

}  // namespace iceberg::rest::auth
