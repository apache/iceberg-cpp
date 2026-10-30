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

#include <filesystem>

#include <gtest/gtest.h>

#include "iceberg/encryption/encryption_register.h"
#include "iceberg/encryption/in_memory_kms.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/test_resource.h"

namespace iceberg {

namespace {

/// \brief Deterministic test bytes: byte i is (i * 31 + seed).
std::vector<uint8_t> VectorBytes(size_t length, int seed) {
  std::vector<uint8_t> bytes(length);
  for (size_t i = 0; i < length; ++i) {
    bytes[i] = static_cast<uint8_t>(static_cast<int>(i) * 31 + seed);
  }
  return bytes;
}

// The master key for "keyA" used by the Java fixture under
// test/resources/encryption/table.
const std::vector<uint8_t> kMasterKeyA = {'0', '1', '2', '3', '4', '5', '6', '7',
                                          '8', '9', '0', '1', '2', '3', '4', '5'};

/// Counts calls to the KMS.
class CountingKms : public InMemoryKms {
 public:
  using InMemoryKms::InMemoryKms;
  Result<std::vector<uint8_t>> WrapKey(std::span<const uint8_t> key,
                                       std::string_view id) override {
    ++wraps;
    return InMemoryKms::WrapKey(key, id);
  }
  Result<std::vector<uint8_t>> UnwrapKey(std::span<const uint8_t> wrapped,
                                         std::string_view id) override {
    ++unwraps;
    return InMemoryKms::UnwrapKey(wrapped, id);
  }
  int wraps = 0;
  int unwraps = 0;
};

std::shared_ptr<CountingKms> MakeKms() {
  return std::make_shared<CountingKms>(
      std::unordered_map<std::string, std::vector<uint8_t>>{{"keyA", kMasterKeyA}});
}

}  // namespace

TEST(StandardEncryptionManagerTest, DecryptsJavaManifestListKeys) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto metadata,
      ReadTableMetadataFromResource("encryption/table/metadata/v5.metadata.json"));
  ASSERT_EQ(metadata->snapshots.size(), 4);
  ASSERT_EQ(metadata->encryption_keys.size(), 5);

  // Through the registry, as catalogs do
  encryption::RegisterAll();
  auto kms = MakeKms();
  ICEBERG_UNWRAP_OR_FAIL(auto manager, EncryptionManagerRegistry::Make(
                                           {.table_key_id = "keyA",
                                            .encryption_keys = metadata->encryption_keys,
                                            .kms = kms}));

  for (const auto& snapshot : metadata->snapshots) {
    ASSERT_TRUE(snapshot->key_id.has_value());
    ICEBERG_UNWRAP_OR_FAIL(auto decrypted,
                           manager->DecryptManifestListKeyMetadata(*snapshot->key_id));
    ICEBERG_UNWRAP_OR_FAIL(auto key_metadata, StandardKeyMetadata::Parse(decrypted));
    EXPECT_EQ(key_metadata.encryption_key.size(), 16);
    EXPECT_EQ(key_metadata.aad_prefix->size(), 16);
    // The trusted length is the stored (encrypted) size of the manifest list file
    auto name = std::filesystem::path(snapshot->manifest_list).filename().string();
    EXPECT_EQ(
        key_metadata.file_length,
        std::filesystem::file_size(GetResourcePath("encryption/table/metadata/" + name)));
  }
  // One key encryption key for all snapshots, unwrapped once
  EXPECT_EQ(kms->unwraps, 1);
}

TEST(StandardEncryptionManagerTest, RegisterAndDecryptManifestListKeyMetadata) {
  auto kms = MakeKms();
  ICEBERG_UNWRAP_OR_FAIL(auto manager, StandardEncryptionManager::Make(
                                           {.table_key_id = "keyA", .kms = kms}));

  ICEBERG_UNWRAP_OR_FAIL(auto key_metadata, manager->NewKeyMetadata());
  EXPECT_EQ(key_metadata.encryption_key.size(), 16);
  EXPECT_EQ(key_metadata.aad_prefix->size(), 16);
  EXPECT_FALSE(key_metadata.file_length.has_value());
  // The manifest list length is required
  EXPECT_THAT(manager->RegisterManifestListKeyMetadata(key_metadata),
              IsError(ErrorKind::kInvalidArgument));
  key_metadata.file_length = 999;

  ICEBERG_UNWRAP_OR_FAIL(auto keys,
                         manager->RegisterManifestListKeyMetadata(key_metadata));
  EXPECT_EQ(keys.key_encryption_key.encrypted_by_id, "keyA");
  EXPECT_TRUE(keys.key_encryption_key.properties.contains("KEY_TIMESTAMP"));
  EXPECT_EQ(keys.file_key.encrypted_by_id, keys.key_encryption_key.key_id);
  EXPECT_EQ(kms->wraps, 1);
  EXPECT_EQ(manager->encryption_keys().size(), 2);

  // A reader that only has the committed keys and the KMS can decrypt it
  ICEBERG_UNWRAP_OR_FAIL(auto reader,
                         StandardEncryptionManager::Make(
                             {.table_key_id = "keyA",
                              .encryption_keys = {keys.key_encryption_key, keys.file_key},
                              .kms = kms}));
  ICEBERG_UNWRAP_OR_FAIL(auto decrypted,
                         reader->DecryptManifestListKeyMetadata(keys.file_key.key_id));
  EXPECT_EQ(decrypted, key_metadata.Serialize());

  // Key encryption keys and unknown IDs are rejected
  EXPECT_THAT(reader->DecryptManifestListKeyMetadata(keys.key_encryption_key.key_id),
              IsError(ErrorKind::kInvalid));
  EXPECT_THAT(reader->DecryptManifestListKeyMetadata("missing"),
              IsError(ErrorKind::kInvalid));
}

TEST(StandardEncryptionManagerTest, RotatesKeyEncryptionKeys) {
  auto kms = MakeKms();
  ICEBERG_UNWRAP_OR_FAIL(auto manager, StandardEncryptionManager::Make(
                                           {.table_key_id = "keyA", .kms = kms}));
  StandardKeyMetadata key_metadata{.encryption_key = VectorBytes(16, 1),
                                   .aad_prefix = VectorBytes(16, 2),
                                   .file_length = 100};

  ICEBERG_UNWRAP_OR_FAIL(auto first,
                         manager->RegisterManifestListKeyMetadata(key_metadata));
  ICEBERG_UNWRAP_OR_FAIL(auto second,
                         manager->RegisterManifestListKeyMetadata(key_metadata));
  EXPECT_EQ(second.key_encryption_key.key_id, first.key_encryption_key.key_id);
  EXPECT_NE(second.file_key.key_id, first.file_key.key_id);

  manager->SetTestTimeShift(StandardEncryptionManager::kKekLifespan);
  ICEBERG_UNWRAP_OR_FAIL(auto third,
                         manager->RegisterManifestListKeyMetadata(key_metadata));
  EXPECT_NE(third.key_encryption_key.key_id, first.key_encryption_key.key_id);
  EXPECT_EQ(kms->wraps, 2);

  // Manifest lists encrypted with the old key encryption key stay readable
  ICEBERG_UNWRAP_OR_FAIL(auto decrypted,
                         manager->DecryptManifestListKeyMetadata(first.file_key.key_id));
  EXPECT_EQ(decrypted, key_metadata.Serialize());
}

TEST(StandardEncryptionManagerTest, UnwrappedKeyCacheExpires) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto metadata,
      ReadTableMetadataFromResource("encryption/table/metadata/v5.metadata.json"));
  auto kms = MakeKms();
  ICEBERG_UNWRAP_OR_FAIL(auto manager, StandardEncryptionManager::Make(
                                           {.table_key_id = "keyA",
                                            .encryption_keys = metadata->encryption_keys,
                                            .kms = kms}));
  const auto& key_id = *metadata->snapshots.front()->key_id;
  ASSERT_THAT(manager->DecryptManifestListKeyMetadata(key_id), IsOk());
  ASSERT_THAT(manager->DecryptManifestListKeyMetadata(key_id), IsOk());
  EXPECT_EQ(kms->unwraps, 1);

  manager->SetTestTimeShift(StandardEncryptionManager::kUnwrappedKekCacheExpiry);
  ASSERT_THAT(manager->DecryptManifestListKeyMetadata(key_id), IsOk());
  EXPECT_EQ(kms->unwraps, 2);
}

TEST(StandardEncryptionManagerTest, DecryptingFileRequiresLength) {
  auto kms = MakeKms();
  ICEBERG_UNWRAP_OR_FAIL(auto manager, StandardEncryptionManager::Make(
                                           {.table_key_id = "keyA", .kms = kms}));
  StandardKeyMetadata no_length{.encryption_key = VectorBytes(16, 1)};

  class NullInputFile : public InputFile {
   public:
    std::string_view location() const override { return "file"; }
    Result<int64_t> Size() const override { return 0; }
    Result<std::unique_ptr<SeekableInputStream>> Open() override {
      return NotImplemented("");
    }
  };
  EXPECT_THAT(manager->NewDecryptingInputFile(std::make_unique<NullInputFile>(),
                                              no_length.Serialize()),
              IsError(ErrorKind::kInvalid));
}

TEST(EncryptionManagerRegistryTest, ValidatesOptions) {
  encryption::RegisterAll();
  EXPECT_TRUE(encryption::IsEncryptionAvailable());
  EXPECT_THAT(EncryptionManagerRegistry::Make({.table_key_id = "keyA"}),
              IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(EncryptionManagerRegistry::Make({.table_key_id = "", .kms = MakeKms()}),
              IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(EncryptionManagerRegistry::Make(
                  {.table_key_id = "keyA", .data_key_length = 20, .kms = MakeKms()}),
              IsError(ErrorKind::kInvalidArgument));
}

}  // namespace iceberg
