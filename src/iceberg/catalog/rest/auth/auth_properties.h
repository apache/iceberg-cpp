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
#include <unordered_map>

#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/result.h"
#include "iceberg/util/config.h"

/// \file iceberg/catalog/rest/auth/auth_properties.h
/// \brief Property keys and configuration for REST catalog authentication.

namespace iceberg::rest::auth {

/// \brief Authentication properties
class ICEBERG_REST_EXPORT AuthProperties : public ConfigBase<AuthProperties> {
 public:
  template <typename T>
  using Entry = const ConfigBase<AuthProperties>::Entry<T>;

  // ---- Authentication type constants (not Entry-based) ----

  inline static const std::string kAuthType = "rest.auth.type";
  inline static const std::string kAuthTypeNone = "none";
  inline static const std::string kAuthTypeBasic = "basic";
  inline static const std::string kAuthTypeOAuth2 = "oauth2";
  inline static const std::string kAuthTypeSigV4 = "sigv4";

  // ---- Basic auth entries ----

  inline static const std::string kBasicUsername = "rest.auth.basic.username";
  inline static const std::string kBasicPassword = "rest.auth.basic.password";

  // ---- SigV4 entries ----

  inline static const std::string kSigV4Region = "rest.auth.sigv4.region";
  inline static const std::string kSigV4Service = "rest.auth.sigv4.service";
  inline static const std::string kSigV4DelegateAuthType =
      "rest.auth.sigv4.delegate-auth-type";

  // ---- SigV4 AWS credential entries ----

  /// AWS region for SigV4 signing.
  inline static const std::string kSigV4SigningRegion = "rest.signing-region";
  /// AWS service name for SigV4 signing.
  inline static const std::string kSigV4SigningName = "rest.signing-name";
  inline static const std::string kSigV4SigningNameDefault = "execute-api";
  /// Static access key ID for SigV4 signing.
  inline static const std::string kSigV4AccessKeyId = "rest.access-key-id";
  /// Static secret access key for SigV4 signing.
  inline static const std::string kSigV4SecretAccessKey = "rest.secret-access-key";
  /// Optional session token for SigV4 signing.
  inline static const std::string kSigV4SessionToken = "rest.session-token";

  // ---- OAuth2 entries ----

  inline static Entry<std::string> kToken{"token", ""};
  inline static Entry<std::string> kCredential{"credential", ""};
  inline static Entry<std::string> kScope{"scope", "catalog"};
  inline static Entry<std::string> kOAuth2ServerUri{"oauth2-server-uri",
                                                    "v1/oauth/tokens"};
  inline static Entry<bool> kKeepRefreshed{"token-refresh-enabled", true};
  inline static Entry<bool> kExchangeEnabled{"token-exchange-enabled", true};
  inline static Entry<std::string> kAudience{"audience", ""};
  inline static Entry<std::string> kResource{"resource", ""};

  /// \brief Build an AuthProperties from a properties map.
  static Result<AuthProperties> FromProperties(
      const std::unordered_map<std::string, std::string>& properties);

  /// \brief Get the bearer token.
  std::string token() const { return Get(kToken); }
  /// \brief Get the raw credential string.
  std::string credential() const { return Get(kCredential); }
  /// \brief Get the OAuth2 scope.
  std::string scope() const { return Get(kScope); }
  /// \brief Get the token endpoint URI.
  std::string oauth2_server_uri() const { return Get(kOAuth2ServerUri); }
  /// \brief Whether token refresh is enabled.
  bool keep_refreshed() const { return Get(kKeepRefreshed); }
  /// \brief Whether token exchange is enabled.
  bool exchange_enabled() const { return Get(kExchangeEnabled); }

  /// \brief Parsed client_id from credential (empty if no colon).
  const std::string& client_id() const { return client_id_; }
  /// \brief Parsed client_secret from credential.
  const std::string& client_secret() const { return client_secret_; }

  /// \brief Build optional OAuth params (audience, resource) from config.
  std::unordered_map<std::string, std::string> optional_oauth_params() const;

 private:
  std::string client_id_;
  std::string client_secret_;
  std::string token_type_;
  std::optional<int64_t> expires_at_millis_;
};

}  // namespace iceberg::rest::auth
