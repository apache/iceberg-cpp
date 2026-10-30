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
  // Decode into a staging map so a bad property leaves master_keys_ untouched;
  // properties is unordered, so a partial update would be non-deterministic.
  std::unordered_map<std::string, std::vector<uint8_t>> decoded;
  for (const auto& [name, value] : properties) {
    if (!name.starts_with(kKeyPropertyPrefix)) {
      continue;
    }
    auto id = name.substr(kKeyPropertyPrefix.size());
    ICEBERG_PRECHECK(!id.empty(), "Master key property {} has an empty key id", name);
    ICEBERG_ASSIGN_OR_RAISE(auto key, Base64::Decode(value));
    ICEBERG_PRECHECK(key.size() == 16 || key.size() == 24 || key.size() == 32,
                     "Master key {} must be 16, 24 or 32 bytes, got {}", name,
                     key.size());
    decoded.insert_or_assign(std::move(id), std::vector<uint8_t>(key.begin(), key.end()));
  }
  for (auto& [id, key] : decoded) {
    master_keys_.insert_or_assign(id, std::move(key));
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
