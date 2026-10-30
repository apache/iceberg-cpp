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

/// \file iceberg/encryption/in_memory_kms.h
/// KMS client with master keys held in memory, for tests and local development.

#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "iceberg/encryption/key_management_client.h"
#include "iceberg/iceberg_bundle_export.h"

namespace iceberg {

/// \brief KMS client with in-memory master keys. Not for production use.
///
/// Wire compatible with Java's MemoryMockKMS (used by Java tests through UnitestKMS):
/// keys are wrapped with AES-GCM under the master key, as nonce | ciphertext | tag,
/// without AAD.
class ICEBERG_BUNDLE_EXPORT InMemoryKms : public KeyManagementClient {
 public:
  /// \brief The name used with `encryption.kms-impl` after RegisterInMemoryKms().
  static constexpr std::string_view kName = "in-memory";
  /// \brief Catalog property prefix for master keys: `encryption.in-memory-kms.key.<id>`
  /// = Base64 of the key bytes.
  static constexpr std::string_view kKeyPropertyPrefix = "encryption.in-memory-kms.key.";

  InMemoryKms() = default;
  explicit InMemoryKms(std::unordered_map<std::string, std::vector<uint8_t>> master_keys)
      : master_keys_(std::move(master_keys)) {}

  /// \brief Register this client in KmsRegistry under kName.
  static void Register();

  /// \brief Add master keys from `encryption.in-memory-kms.key.<id>` properties.
  Status Initialize(
      const std::unordered_map<std::string, std::string>& properties) override;

  Result<std::vector<uint8_t>> WrapKey(std::span<const uint8_t> key,
                                       std::string_view wrapping_key_id) override;
  Result<std::vector<uint8_t>> UnwrapKey(std::span<const uint8_t> wrapped_key,
                                         std::string_view wrapping_key_id) override;

 private:
  Result<const std::vector<uint8_t>*> MasterKey(std::string_view id) const;

  std::unordered_map<std::string, std::vector<uint8_t>> master_keys_;
};

}  // namespace iceberg
