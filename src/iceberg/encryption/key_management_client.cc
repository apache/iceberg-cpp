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

#include "iceberg/encryption/key_management_client.h"

#include <mutex>

#include "iceberg/util/macros.h"
#include "iceberg/util/string_util.h"

namespace iceberg {

namespace {

struct RegistryState {
  std::mutex mutex;
  std::unordered_map<std::string, KmsRegistry::Factory> factories;
};

RegistryState& State() {
  static RegistryState state;
  return state;
}

}  // namespace

void KmsRegistry::Register(std::string name, Factory factory) {
  auto& state = State();
  std::lock_guard lock(state.mutex);
  state.factories.insert_or_assign(std::move(name), std::move(factory));
}

Result<std::shared_ptr<KeyManagementClient>> KmsRegistry::Load(
    std::string_view name, const Properties& properties) {
  Factory factory;
  {
    auto& state = State();
    std::lock_guard lock(state.mutex);
    auto it = state.factories.find(std::string(name));
    if (it == state.factories.end()) {
      return NotFound(
          "KMS client not found: {}. Built-in clients must be enabled at build time "
          "(ICEBERG_KMS_AWS, ICEBERG_KMS_GCP, ICEBERG_KMS_AZURE); custom clients must be "
          "registered with KmsRegistry::Register",
          name);
    }
    factory = it->second;
  }
  ICEBERG_ASSIGN_OR_RAISE(auto client, factory());
  ICEBERG_PRECHECK(client != nullptr, "KMS client factory for {} returned null", name);
  ICEBERG_RETURN_UNEXPECTED(client->Initialize(properties));
  return std::shared_ptr<KeyManagementClient>(std::move(client));
}

Result<std::shared_ptr<KeyManagementClient>> KmsRegistry::FromCatalogProperties(
    const Properties& properties) {
  auto type_it = properties.find(std::string(kKmsType));
  auto impl_it = properties.find(std::string(kKmsImpl));
  // Like Java, a property that is present but empty counts as set, so that a
  // misconfigured catalog fails instead of silently going unencrypted.
  bool has_type = type_it != properties.end();
  bool has_impl = impl_it != properties.end();

  if (has_type && has_impl) {
    return InvalidArgument("Cannot set both KMS type ({}) and KMS impl ({})",
                           type_it->second, impl_it->second);
  }
  if (has_impl) {
    return Load(impl_it->second, properties);
  }
  if (has_type) {
    auto type = StringUtils::ToLower(type_it->second);
    if (type != kKmsTypeAws && type != kKmsTypeGcp && type != kKmsTypeAzure) {
      return InvalidArgument("Unsupported KMS type: {}", type_it->second);
    }
    return Load(type, properties);
  }
  return std::shared_ptr<KeyManagementClient>{};
}

}  // namespace iceberg
