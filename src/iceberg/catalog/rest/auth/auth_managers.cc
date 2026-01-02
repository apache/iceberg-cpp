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

#include "iceberg/catalog/rest/auth/auth_properties.h"

namespace iceberg::rest::auth {

namespace {

/// \brief Convert a string to lowercase for case-insensitive comparison.
std::string ToLower(std::string_view str) {
  std::string result(str);
  std::transform(result.begin(), result.end(), result.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  return result;
}

/// \brief Infer the authentication type from properties.
///
/// If no explicit auth type is set, this function tries to infer it from
/// other properties. For example, if "credential" or "token" is present,
/// it implies OAuth2 authentication.
std::string InferAuthType(
    const std::unordered_map<std::string, std::string>& properties) {
  // Check for explicit auth type
  auto it = properties.find(std::string(AuthProperties::kAuthType));
  if (it != properties.end() && !it->second.empty()) {
    return ToLower(it->second);
  }

  // Infer from OAuth2 properties
  bool has_credential =
      properties.contains(std::string(AuthProperties::kOAuth2Credential));
  bool has_token = properties.contains(std::string(AuthProperties::kOAuth2Token));

  if (has_credential || has_token) {
    return std::string(AuthProperties::kAuthTypeOAuth2);
  }

  // Default to no authentication
  return std::string(AuthProperties::kAuthTypeNone);
}

}  // namespace

std::unordered_map<std::string, AuthManagerFactory>& AuthManagers::GetRegistry() {
  static std::unordered_map<std::string, AuthManagerFactory> registry;
  return registry;
}

void AuthManagers::Register(const std::string& auth_type, AuthManagerFactory factory) {
  GetRegistry()[ToLower(auth_type)] = std::move(factory);
}

Result<std::unique_ptr<AuthManager>> AuthManagers::Load(
    const std::string& name,
    const std::unordered_map<std::string, std::string>& properties) {
  std::string auth_type = InferAuthType(properties);

  auto& registry = GetRegistry();
  auto it = registry.find(auth_type);
  if (it == registry.end()) {
    return NotImplemented("Authentication type '{}' is not supported", auth_type);
  }

  return it->second(name, properties);
}

}  // namespace iceberg::rest::auth
