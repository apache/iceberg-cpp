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

#include "iceberg/encryption/in_memory_kms.h"

#include "iceberg/encryption/aes_gcm_internal.h"
#include "iceberg/util/base64.h"
#include "iceberg/util/macros.h"

namespace iceberg {

void InMemoryKms::Register() {
  KmsRegistry::Register(std::string(kName),
                        []() -> Result<std::unique_ptr<KeyManagementClient>> {
                          return std::make_unique<InMemoryKms>();
                        });
}

Status InMemoryKms::Initialize(
    const std::unordered_map<std::string, std::string>& properties) {
  for (const auto& [name, value] : properties) {
    if (!name.starts_with(kKeyPropertyPrefix)) {
      continue;
    }
    ICEBERG_ASSIGN_OR_RAISE(auto key, Base64::Decode(value));
    master_keys_.insert_or_assign(name.substr(kKeyPropertyPrefix.size()),
                                  std::vector<uint8_t>(key.begin(), key.end()));
  }
  return {};
}

Result<const std::vector<uint8_t>*> InMemoryKms::MasterKey(std::string_view id) const {
  auto it = master_keys_.find(std::string(id));
  if (it == master_keys_.end()) {
    return NotFound("Master key {} is not found", id);
  }
  return &it->second;
}

Result<std::vector<uint8_t>> InMemoryKms::WrapKey(std::span<const uint8_t> key,
                                                  std::string_view wrapping_key_id) {
  ICEBERG_ASSIGN_OR_RAISE(auto master_key, MasterKey(wrapping_key_id));
  ICEBERG_ASSIGN_OR_RAISE(auto cipher, encryption::AesGcmCipher::Make(*master_key));
  return cipher->Encrypt(key, {});
}

Result<std::vector<uint8_t>> InMemoryKms::UnwrapKey(std::span<const uint8_t> wrapped_key,
                                                    std::string_view wrapping_key_id) {
  ICEBERG_ASSIGN_OR_RAISE(auto master_key, MasterKey(wrapping_key_id));
  ICEBERG_ASSIGN_OR_RAISE(auto cipher, encryption::AesGcmCipher::Make(*master_key));
  return cipher->Decrypt(wrapped_key, {});
}

}  // namespace iceberg
