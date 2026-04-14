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

#include "iceberg/catalog/rest/auth/auth_manager_internal.h"
#ifdef ICEBERG_BUILD_SIGV4
#  include "iceberg/catalog/rest/auth/sigv4_auth_manager.h"
#endif
#include "iceberg/catalog/rest/auth/auth_properties.h"
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
  bool has_credential = properties.contains(AuthProperties::kCredential.key());
  bool has_token = properties.contains(AuthProperties::kToken.key());
  if (has_credential || has_token) {
    return AuthProperties::kAuthTypeOAuth2;
  }

  return AuthProperties::kAuthTypeNone;
}

AuthManagerRegistry CreateDefaultRegistry() {
  AuthManagerRegistry registry = {
      {AuthProperties::kAuthTypeNone, MakeNoopAuthManager},
      {AuthProperties::kAuthTypeBasic, MakeBasicAuthManager},
      {AuthProperties::kAuthTypeOAuth2, MakeOAuth2Manager},
  };
#ifdef ICEBERG_BUILD_SIGV4
  registry[AuthProperties::kAuthTypeSigV4] = MakeSigV4AuthManager;
#endif
  return registry;
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

#ifdef ICEBERG_BUILD_SIGV4
Result<std::unique_ptr<AuthManager>> MakeSigV4AuthManager(
    std::string_view name,
    const std::unordered_map<std::string, std::string>& properties) {
  // Determine the delegate auth type. Default to OAuth2 if not specified.
  std::string delegate_type = AuthProperties::kAuthTypeOAuth2;
  auto it = properties.find(AuthProperties::kSigV4DelegateAuthType);
  if (it != properties.end() && !it->second.empty()) {
    delegate_type = StringUtils::ToLower(it->second);
  }

  // Prevent circular delegation (sigv4 -> sigv4 -> ...).
  ICEBERG_PRECHECK(delegate_type != AuthProperties::kAuthTypeSigV4,
                   "Cannot delegate a SigV4 auth manager to another SigV4 auth "
                   "manager (delegate_type='{}')",
                   delegate_type);

  // Load the delegate auth manager.
  auto delegate_props = properties;
  delegate_props[AuthProperties::kAuthType] = delegate_type;

  ICEBERG_ASSIGN_OR_RAISE(auto delegate, AuthManagers::Load(name, delegate_props));
  return std::make_unique<SigV4AuthManager>(std::move(delegate));
}
#endif

}  // namespace iceberg::rest::auth
