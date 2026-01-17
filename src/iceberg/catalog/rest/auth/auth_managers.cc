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

#include "iceberg/catalog/rest/auth/auth_managers.h"

#include <algorithm>
#include <cctype>

#include "iceberg/catalog/rest/auth/auth_properties.h"
#include "iceberg/util/string_util.h"

namespace iceberg::rest::auth {

namespace {

/// \brief Registry type for AuthManager factories with heterogeneous lookup support.
using AuthManagerRegistry =
    std::unordered_map<std::string, AuthManagerFactory, StringHash, StringEqual>;

// Infer the authentication type from properties.
std::string InferAuthType(
    const std::unordered_map<std::string, std::string>& properties) {
  auto it = properties.find(AuthProperties::kAuthType);
  if (it != properties.end() && !it->second.empty()) {
    return StringUtils::ToLower(it->second);
  }

  // Infer from OAuth2 properties (credential or token)
  bool has_credential = properties.contains(AuthProperties::kOAuth2Credential);
  bool has_token = properties.contains(AuthProperties::kOAuth2Token);
  if (has_credential || has_token) {
    return AuthProperties::kAuthTypeOAuth2;
  }

  return AuthProperties::kAuthTypeNone;
}

// Get the global registry of auth manager factories.
AuthManagerRegistry& GetRegistry() {
  static AuthManagerRegistry registry;
  return registry;
}

}  // namespace

void AuthManagers::Register(std::string_view auth_type, AuthManagerFactory factory) {
  GetRegistry()[StringUtils::ToLower(auth_type)] = std::move(factory);
}

Result<std::unique_ptr<AuthManager>> AuthManagers::Load(
    std::string_view name,
    const std::unordered_map<std::string, std::string>& properties) {
  std::string auth_type = InferAuthType(properties);

  auto& registry = GetRegistry();
  auto it = registry.find(auth_type);
  if (it == registry.end()) {
    // TODO(Li Shuxu): Fallback to default auth manager implementations
    return NotImplemented("Authentication type '{}' is not supported", auth_type);
  }

  return it->second(name, properties);
}

}  // namespace iceberg::rest::auth
