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

#include "iceberg/encryption/standard_encryption_manager.h"

#include <array>
#include <charconv>
#include <optional>

#include "iceberg/encryption/aes_gcm_internal.h"
#include "iceberg/encryption/aes_gcm_stream.h"
#include "iceberg/util/base64.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

using encryption::AesGcmCipher;
using encryption::SecureRandomBytes;

std::span<const uint8_t> AsBytes(std::string_view s) {
  return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
}

std::string ToString(std::span<const uint8_t> bytes) {
  return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
}

Result<std::string> GenerateKeyId() {
  std::array<uint8_t, 16> id{};
  ICEBERG_RETURN_UNEXPECTED(SecureRandomBytes(id));
  return Base64::Encode(ToString(id));
}

std::optional<int64_t> ParseTimestamp(const EncryptedKey& key) {
  auto it = key.properties.find(std::string(StandardEncryptionManager::kKeyTimestamp));
  if (it == key.properties.end()) {
    return std::nullopt;
  }
  int64_t value = 0;
  const auto& str = it->second;
  auto [ptr, ec] = std::from_chars(str.data(), str.data() + str.size(), value);
  if (ec != std::errc{} || ptr != str.data() + str.size()) {
    return std::nullopt;
  }
  return value;
}

}  // namespace

Result<std::shared_ptr<StandardEncryptionManager>> StandardEncryptionManager::Make(
    EncryptionManagerRegistry::Options options) {
  ICEBERG_PRECHECK(!options.table_key_id.empty(), "Invalid encryption key ID: empty");
  ICEBERG_PRECHECK(options.data_key_length == 16 || options.data_key_length == 24 ||
                       options.data_key_length == 32,
                   "Invalid data key length: {} (must be 16, 24, or 32)",
                   options.data_key_length);
  ICEBERG_PRECHECK(options.kms != nullptr, "Invalid KMS client: null");
  return std::shared_ptr<StandardEncryptionManager>(
      new StandardEncryptionManager(std::move(options)));
}

StandardEncryptionManager::StandardEncryptionManager(
    EncryptionManagerRegistry::Options options)
    : table_key_id_(std::move(options.table_key_id)),
      data_key_length_(options.data_key_length),
      kms_(std::move(options.kms)) {
  for (auto& key : options.encryption_keys) {
    AddKey(std::move(key));
  }
}

void StandardEncryptionManager::AddKey(EncryptedKey key) {
  auto key_id = key.key_id;
  if (keys_.insert_or_assign(key_id, std::move(key)).second) {
    key_order_.push_back(std::move(key_id));
  }
}

void StandardEncryptionManager::SetTestTimeShift(std::chrono::milliseconds shift) {
  std::lock_guard lock(mutex_);
  time_shift_ = shift;
}

int64_t StandardEncryptionManager::NowMs() const {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch() + time_shift_)
      .count();
}

Result<StandardKeyMetadata> StandardEncryptionManager::NewKeyMetadata() {
  StandardKeyMetadata metadata;
  metadata.encryption_key.resize(data_key_length_);
  ICEBERG_RETURN_UNEXPECTED(SecureRandomBytes(metadata.encryption_key));
  metadata.aad_prefix.emplace(kAadPrefixLength);
  ICEBERG_RETURN_UNEXPECTED(SecureRandomBytes(*metadata.aad_prefix));
  return metadata;
}

Result<std::unique_ptr<InputFile>> StandardEncryptionManager::NewDecryptingInputFile(
    std::unique_ptr<InputFile> encrypted, std::span<const uint8_t> key_metadata) {
  ICEBERG_PRECHECK(encrypted != nullptr, "Encrypted input file cannot be null");
  ICEBERG_ASSIGN_OR_RAISE(auto metadata, StandardKeyMetadata::Parse(key_metadata));
  if (!metadata.file_length.has_value()) {
    // The length must come from trusted key metadata, not from the file system, so that
    // truncation is detected (format/gcm-stream-spec.md, "File length").
    return Invalid("Cannot read AES GCM stream {}: key metadata has no file length",
                   encrypted->location());
  }
  return std::make_unique<AesGcmInputFile>(
      std::move(encrypted), std::move(metadata.encryption_key),
      metadata.aad_prefix.value_or(std::vector<uint8_t>{}), *metadata.file_length);
}

Result<std::unique_ptr<OutputFile>> StandardEncryptionManager::NewEncryptingOutputFile(
    std::unique_ptr<OutputFile> plain, const StandardKeyMetadata& key_metadata) {
  ICEBERG_PRECHECK(plain != nullptr, "Output file cannot be null");
  return std::make_unique<AesGcmOutputFile>(
      std::move(plain), key_metadata.encryption_key,
      key_metadata.aad_prefix.value_or(std::vector<uint8_t>{}));
}

Result<std::vector<uint8_t>> StandardEncryptionManager::UnwrapKek(
    const std::string& kek_id) {
  int64_t now = NowMs();
  if (auto it = unwrapped_keks_.find(kek_id); it != unwrapped_keks_.end()) {
    if (now - it->second.unwrapped_at_ms < kUnwrappedKekCacheExpiry.count()) {
      return it->second.key;
    }
    unwrapped_keks_.erase(it);
  }
  auto it = keys_.find(kek_id);
  if (it == keys_.end()) {
    return Invalid("Cannot find key encryption key with id {}", kek_id);
  }
  ICEBERG_ASSIGN_OR_RAISE(
      auto kek,
      kms_->UnwrapKey(AsBytes(it->second.encrypted_key_metadata), table_key_id_));
  unwrapped_keks_.insert_or_assign(kek_id, CachedKek{kek, now});
  return kek;
}

Result<std::vector<uint8_t>> StandardEncryptionManager::DecryptManifestListKeyMetadata(
    std::string_view manifest_list_key_id) {
  std::lock_guard lock(mutex_);
  auto it = keys_.find(std::string(manifest_list_key_id));
  if (it == keys_.end()) {
    return Invalid("Cannot find manifest list key metadata with id {}",
                   manifest_list_key_id);
  }
  const EncryptedKey& manifest_list_key = it->second;
  if (!manifest_list_key.encrypted_by_id.has_value() ||
      *manifest_list_key.encrypted_by_id == table_key_id_) {
    return Invalid("{} is not manifest list key metadata", manifest_list_key_id);
  }

  const std::string& kek_id = *manifest_list_key.encrypted_by_id;
  ICEBERG_ASSIGN_OR_RAISE(auto kek, UnwrapKek(kek_id));
  auto timestamp = keys_.at(kek_id).properties.find(std::string(kKeyTimestamp));
  if (timestamp == keys_.at(kek_id).properties.end()) {
    return Invalid("Key encryption key {} must be timestamped", kek_id);
  }

  // The KEK timestamp is the AAD, to prevent timestamp tampering attacks.
  ICEBERG_ASSIGN_OR_RAISE(auto cipher, AesGcmCipher::Make(kek));
  return cipher->Decrypt(AsBytes(manifest_list_key.encrypted_key_metadata),
                         AsBytes(timestamp->second));
}

Result<std::string> StandardEncryptionManager::CurrentKekId() {
  int64_t now = NowMs();
  for (const auto& key_id : key_order_) {
    const EncryptedKey& key = keys_.at(key_id);
    if (key.encrypted_by_id != table_key_id_) {
      continue;
    }
    auto timestamp = ParseTimestamp(key);
    if (timestamp.has_value() && now - *timestamp < kKekLifespan.count()) {
      return key_id;
    }
  }

  // No unexpired key encryption key: create one, wrapped by the table master key.
  std::vector<uint8_t> kek(data_key_length_);
  ICEBERG_RETURN_UNEXPECTED(SecureRandomBytes(kek));
  ICEBERG_ASSIGN_OR_RAISE(auto wrapped, kms_->WrapKey(kek, table_key_id_));
  ICEBERG_ASSIGN_OR_RAISE(auto kek_id, GenerateKeyId());
  AddKey(EncryptedKey{.key_id = kek_id,
                      .encrypted_key_metadata = ToString(wrapped),
                      .encrypted_by_id = table_key_id_,
                      .properties = {{std::string(kKeyTimestamp), std::to_string(now)}}});
  unwrapped_keks_.insert_or_assign(kek_id, CachedKek{std::move(kek), now});
  return kek_id;
}

Result<ManifestListEncryptionKeys>
StandardEncryptionManager::RegisterManifestListKeyMetadata(
    const StandardKeyMetadata& key_metadata) {
  ICEBERG_PRECHECK(key_metadata.file_length.has_value(),
                   "Manifest list key metadata must include the file length");
  std::lock_guard lock(mutex_);
  ICEBERG_ASSIGN_OR_RAISE(auto kek_id, CurrentKekId());
  ICEBERG_ASSIGN_OR_RAISE(auto kek, UnwrapKek(kek_id));
  const EncryptedKey& kek_entry = keys_.at(kek_id);
  const std::string& timestamp = kek_entry.properties.at(std::string(kKeyTimestamp));

  ICEBERG_ASSIGN_OR_RAISE(auto cipher, AesGcmCipher::Make(kek));
  ICEBERG_ASSIGN_OR_RAISE(auto encrypted,
                          cipher->Encrypt(key_metadata.Serialize(), AsBytes(timestamp)));
  ICEBERG_ASSIGN_OR_RAISE(auto file_key_id, GenerateKeyId());
  EncryptedKey file_key{.key_id = file_key_id,
                        .encrypted_key_metadata = ToString(encrypted),
                        .encrypted_by_id = kek_id};
  AddKey(file_key);
  return ManifestListEncryptionKeys{.key_encryption_key = keys_.at(kek_id),
                                    .file_key = std::move(file_key)};
}

std::vector<EncryptedKey> StandardEncryptionManager::encryption_keys() const {
  std::lock_guard lock(mutex_);
  std::vector<EncryptedKey> result;
  result.reserve(key_order_.size());
  for (const auto& key_id : key_order_) {
    result.push_back(keys_.at(key_id));
  }
  return result;
}

namespace encryption {

void RegisterStandardEncryptionManager() {
  EncryptionManagerRegistry::Register([](EncryptionManagerRegistry::Options options)
                                          -> Result<std::shared_ptr<EncryptionManager>> {
    ICEBERG_ASSIGN_OR_RAISE(auto manager,
                            StandardEncryptionManager::Make(std::move(options)));
    return manager;
  });
}

namespace {

[[maybe_unused]] const bool kStandardEncryptionManagerRegistered = []() {
  RegisterStandardEncryptionManager();
  return true;
}();

}  // namespace

}  // namespace encryption

}  // namespace iceberg
