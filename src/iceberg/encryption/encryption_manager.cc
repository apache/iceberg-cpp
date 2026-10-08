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

#include "iceberg/encryption/encryption_manager.h"

#include <mutex>

#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

struct RegistryState {
  std::mutex mutex;
  EncryptionManagerRegistry::Factory factory;
};

RegistryState& State() {
  static RegistryState state;
  return state;
}

}  // namespace

void EncryptionManagerRegistry::Register(Factory factory) {
  auto& state = State();
  std::lock_guard lock(state.mutex);
  state.factory = std::move(factory);
}

Result<std::shared_ptr<EncryptionManager>> EncryptionManagerRegistry::Make(
    Options options) {
  ICEBERG_PRECHECK(!options.table_key_id.empty(), "Invalid encryption key ID: empty");
  ICEBERG_PRECHECK(options.data_key_length == 16 || options.data_key_length == 24 ||
                       options.data_key_length == 32,
                   "Invalid data key length: {} (must be 16, 24, or 32)",
                   options.data_key_length);
  ICEBERG_PRECHECK(options.kms != nullptr,
                   "Cannot create an encryption manager without a KMS client; set the "
                   "encryption.kms-type or encryption.kms-impl catalog property");
  Factory factory;
  {
    auto& state = State();
    std::lock_guard lock(state.mutex);
    factory = state.factory;
  }
  if (!factory) {
    return NotSupported(
        "Table encryption is not available: link iceberg_bundle built with "
        "ICEBERG_ENCRYPTION=ON and call iceberg::encryption::RegisterAll()");
  }
  return factory(std::move(options));
}

}  // namespace iceberg
