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

#include <unordered_set>

#include "iceberg/catalog/rest/auth/auth_properties.h"
#include "iceberg/catalog/rest/auth/auth_session.h"
#include "iceberg/util/string_util.h"

namespace iceberg::rest::auth {

namespace {

/// \brief Registry type for AuthManager factories with heterogeneous lookup support.
using AuthManagerRegistry =
    std::unordered_map<std::string, AuthManagerFactory, StringHash, StringEqual>;

const std::unordered_set<std::string, StringHash, StringEqual>& KnownAuthTypes() {
  static const std::unordered_set<std::string, StringHash, StringEqual> kAuthTypes = {
      AuthProperties::kAuthTypeNone,
      AuthProperties::kAuthTypeBasic,
      AuthProperties::kAuthTypeOAuth2,
      AuthProperties::kAuthTypeSigV4,
  };
  return kAuthTypes;
}

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

/// \brief Authentication manager that performs no authentication.
class NoopAuthManager : public AuthManager {
 public:
  static Result<std::unique_ptr<AuthManager>> Make(
      [[maybe_unused]] std::string_view name,
      [[maybe_unused]] const std::unordered_map<std::string, std::string>& properties) {
    return std::make_unique<NoopAuthManager>();
  }

  Result<std::shared_ptr<AuthSession>> CatalogSession(
      [[maybe_unused]] HttpClient& client,
      [[maybe_unused]] const std::unordered_map<std::string, std::string>& properties)
      override {
    return AuthSession::MakeDefault({});
  }
};

template <typename T>
AuthManagerFactory MakeAuthFactory() {
  return
      [](std::string_view name, const std::unordered_map<std::string, std::string>& props)
          -> Result<std::unique_ptr<AuthManager>> { return T::Make(name, props); };
}

AuthManagerRegistry CreateDefaultRegistry() {
  return {
      {AuthProperties::kAuthTypeNone, MakeAuthFactory<NoopAuthManager>()},
  };
}

// Get the global registry of auth manager factories.
AuthManagerRegistry& GetRegistry() {
  static AuthManagerRegistry registry = CreateDefaultRegistry();
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
    if (KnownAuthTypes().contains(auth_type)) {
      return NotImplemented("Authentication type '{}' is not yet supported", auth_type);
    }
    return InvalidArgument("Unknown authentication type: '{}'", auth_type);
  }

  return it->second(name, properties);
}

}  // namespace iceberg::rest::auth
