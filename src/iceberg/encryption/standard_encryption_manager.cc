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
  if (ec != std::errc{} || ptr != str.data() + str.size() || value < 0) {
    return std::nullopt;
  }
  return value;
}

// Forwards to a file owned elsewhere, so one underlying file can serve both the native
// path and the AGS1 stream path of a Native wrapper.
class SharedInputFile : public InputFile {
 public:
  explicit SharedInputFile(std::shared_ptr<InputFile> file) : file_(std::move(file)) {}

  std::string_view location() const override { return file_->location(); }
  Result<int64_t> Size() const override { return file_->Size(); }
  Result<std::unique_ptr<SeekableInputStream>> Open() override { return file_->Open(); }

 private:
  std::shared_ptr<InputFile> file_;
};

class SharedOutputFile : public OutputFile {
 public:
  explicit SharedOutputFile(std::shared_ptr<OutputFile> file) : file_(std::move(file)) {}

  std::string_view location() const override { return file_->location(); }
  Result<std::unique_ptr<PositionOutputStream>> Create() override {
    return file_->Create();
  }
  Result<std::unique_ptr<PositionOutputStream>> CreateOrOverwrite() override {
    return file_->CreateOrOverwrite();
  }

 private:
  std::shared_ptr<OutputFile> file_;
};

// Mirrors Java's StandardDecryptedInputFile: the raw file and key metadata are available
// to native readers right away, while the AGS1 reader, which needs the encrypted file
// length from the key metadata, is built on first use of the InputFile methods.
class StandardDecryptedInputFile : public NativeEncryptionInputFile {
 public:
  StandardDecryptedInputFile(std::shared_ptr<InputFile> raw, StandardKeyMetadata metadata)
      : raw_(std::move(raw)), metadata_(std::move(metadata)) {}

  std::unique_ptr<InputFile> ReleaseInputFile() override {
    return std::make_unique<SharedInputFile>(raw_);
  }
  const NativeEncryptionKeyMetadata& key_metadata() const override { return metadata_; }

  std::string_view location() const override { return raw_->location(); }
  Result<int64_t> Size() const override {
    ICEBERG_ASSIGN_OR_RAISE(auto file, Decrypting());
    return file->Size();
  }
  Result<std::unique_ptr<SeekableInputStream>> Open() override {
    ICEBERG_ASSIGN_OR_RAISE(auto file, Decrypting());
    return file->Open();
  }

 private:
  Result<AesGcmInputFile*> Decrypting() const {
    if (decrypting_ == nullptr) {
      if (!metadata_.file_length.has_value()) {
        // The length must come from trusted key metadata, not from the file system, so
        // that truncation is detected (format/gcm-stream-spec.md, "File length").
        return Invalid("Cannot read AES GCM stream {}: key metadata has no file length",
                       raw_->location());
      }
      decrypting_ = std::make_unique<AesGcmInputFile>(
          std::make_unique<SharedInputFile>(raw_), metadata_.encryption_key,
          metadata_.aad_prefix.value_or(std::vector<uint8_t>{}), *metadata_.file_length);
    }
    return decrypting_.get();
  }

  std::shared_ptr<InputFile> raw_;
  StandardKeyMetadata metadata_;
  mutable std::unique_ptr<AesGcmInputFile> decrypting_;
};

// Mirrors Java's StandardEncryptedOutputFile: native writers use the plain file and the
// key metadata; the AGS1 writer over the same file and key is built on first use.
class StandardEncryptedOutputFile : public NativeEncryptionOutputFile {
 public:
  StandardEncryptedOutputFile(std::shared_ptr<OutputFile> plain,
                              StandardKeyMetadata metadata)
      : plain_(std::move(plain)), metadata_(std::move(metadata)) {}

  OutputFile& plain_output_file() const override { return *plain_; }
  OutputFile& encrypting_output_file() const override {
    if (encrypting_ == nullptr) {
      encrypting_ = std::make_unique<AesGcmOutputFile>(
          std::make_unique<SharedOutputFile>(plain_), metadata_.encryption_key,
          metadata_.aad_prefix.value_or(std::vector<uint8_t>{}));
    }
    return *encrypting_;
  }
  const NativeEncryptionKeyMetadata& key_metadata() const override { return metadata_; }

 private:
  std::shared_ptr<OutputFile> plain_;
  StandardKeyMetadata metadata_;
  mutable std::unique_ptr<AesGcmOutputFile> encrypting_;
};

Result<StandardKeyMetadata> CastOrParse(const EncryptionKeyMetadata& key_metadata) {
  if (const auto* standard = dynamic_cast<const StandardKeyMetadata*>(&key_metadata)) {
    return *standard;
  }
  auto buffer = key_metadata.buffer();
  return StandardKeyMetadata::Parse(buffer);
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
  unwrapped_keks_.insert_or_assign(kek_id, CachedKek{.key = kek, .unwrapped_at_ms = now});
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
  unwrapped_keks_.insert_or_assign(
      kek_id, CachedKek{.key = std::move(kek), .unwrapped_at_ms = now});
  return kek_id;
}

Result<FileEncryptionKeys> StandardEncryptionManager::RegisterManifestListKeyMetadata(
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
  return FileEncryptionKeys{.key_encryption_key = keys_.at(kek_id),
                            .file_key = std::move(file_key)};
}

Result<std::unique_ptr<InputFile>> StandardEncryptionManager::Decrypt(
    std::unique_ptr<EncryptedInputFile> encrypted) {
  ICEBERG_PRECHECK(encrypted != nullptr, "Encrypted input file cannot be null");
  // A native wrapper already carries both the native and the stream path.
  if (auto* native = dynamic_cast<NativeEncryptionInputFile*>(encrypted.get())) {
    encrypted.release();
    return std::unique_ptr<InputFile>(native);
  }
  ICEBERG_ASSIGN_OR_RAISE(auto metadata, CastOrParse(encrypted->key_metadata()));
  std::shared_ptr<InputFile> raw = encrypted->ReleaseInputFile();
  ICEBERG_PRECHECK(raw != nullptr, "Encrypted input file has no underlying file");
  return std::make_unique<StandardDecryptedInputFile>(std::move(raw),
                                                      std::move(metadata));
}

Result<std::unique_ptr<EncryptedOutputFile>> StandardEncryptionManager::Encrypt(
    std::unique_ptr<OutputFile> plain) {
  ICEBERG_PRECHECK(plain != nullptr, "Output file cannot be null");
  ICEBERG_ASSIGN_OR_RAISE(auto key_metadata, NewKeyMetadata());
  return std::make_unique<StandardEncryptedOutputFile>(std::move(plain),
                                                       std::move(key_metadata));
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
