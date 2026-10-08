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

/// \file iceberg/encryption/standard_encryption_manager.h
/// Envelope encryption manager, compatible with Java's StandardEncryptionManager.

#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "iceberg/encryption/encryption_manager.h"
#include "iceberg/iceberg_bundle_export.h"

namespace iceberg {

/// \brief Standard Iceberg envelope encryption.
///
/// Key hierarchy:
/// - table master key: in the KMS, ID in table property `encryption.key-id`;
/// - key encryption keys (KEK): wrapped by the KMS and kept in `encryption-keys`
///   with `encrypted-by-id` = the table key ID and a KEY_TIMESTAMP property. A KEK is
///   used for 730 days (NIST SP 800-57), then a new one is created;
/// - manifest list key metadata: AES-GCM encrypted by a KEK with the KEK timestamp as
///   AAD, kept in `encryption-keys` and referenced by the snapshot `key-id`;
/// - manifest key metadata: in the manifest list `key_metadata` field;
/// - data and delete file key metadata: in the manifest `key_metadata` field.
///
/// Unwrapped KEKs are cached for one hour. Thread-safe.
class ICEBERG_BUNDLE_EXPORT StandardEncryptionManager : public EncryptionManager {
 public:
  static constexpr std::string_view kKeyTimestamp = "KEY_TIMESTAMP";
  static constexpr int32_t kAadPrefixLength = 16;
  static constexpr std::chrono::milliseconds kKekLifespan{730LL * 24 * 3600 * 1000};
  static constexpr std::chrono::milliseconds kUnwrappedKekCacheExpiry{3600LL * 1000};

  static Result<std::shared_ptr<StandardEncryptionManager>> Make(
      EncryptionManagerRegistry::Options options);

  Result<StandardKeyMetadata> NewKeyMetadata() override;
  Result<std::unique_ptr<InputFile>> NewDecryptingInputFile(
      std::unique_ptr<InputFile> encrypted,
      std::span<const uint8_t> key_metadata) override;
  Result<std::unique_ptr<OutputFile>> NewEncryptingOutputFile(
      std::unique_ptr<OutputFile> plain,
      const StandardKeyMetadata& key_metadata) override;
  Result<std::vector<uint8_t>> DecryptManifestListKeyMetadata(
      std::string_view manifest_list_key_id) override;
  Result<ManifestListEncryptionKeys> RegisterManifestListKeyMetadata(
      const StandardKeyMetadata& key_metadata) override;

  /// \brief All keys: those from table metadata and those created by this manager.
  std::vector<EncryptedKey> encryption_keys() const;

  const std::string& table_key_id() const { return table_key_id_; }

  /// \brief Shift the clock, for tests of KEK rotation and cache expiry.
  void SetTestTimeShift(std::chrono::milliseconds shift);

 private:
  struct CachedKek {
    std::vector<uint8_t> key;
    int64_t unwrapped_at_ms;
  };

  explicit StandardEncryptionManager(EncryptionManagerRegistry::Options options);

  // The following require mutex_ to be held.
  Result<std::vector<uint8_t>> UnwrapKek(const std::string& kek_id);
  Result<std::string> CurrentKekId();
  void AddKey(EncryptedKey key);

  int64_t NowMs() const;

  const std::string table_key_id_;
  const int32_t data_key_length_;
  const std::shared_ptr<KeyManagementClient> kms_;

  mutable std::mutex mutex_;
  std::chrono::milliseconds time_shift_{0};
  std::vector<std::string> key_order_;
  std::unordered_map<std::string, EncryptedKey> keys_;
  std::unordered_map<std::string, CachedKek> unwrapped_keks_;
};

namespace encryption {

/// \brief Register StandardEncryptionManager in EncryptionManagerRegistry.
ICEBERG_BUNDLE_EXPORT void RegisterStandardEncryptionManager();

}  // namespace encryption

}  // namespace iceberg
