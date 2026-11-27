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
#include <fstream>
#include <optional>

#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_io_util.h"
#include "iceberg/encryption/encrypted_files.h"
#include "iceberg/encryption/encryption_register.h"
#include "iceberg/encryption/encryption_util.h"
#include "iceberg/encryption/in_memory_kms.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/temp_file_test_base.h"
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
    ICEBERG_UNWRAP_OR_FAIL(auto decrypted, EncryptionUtil::DecryptManifestListKeyMetadata(
                                               manager, *snapshot->key_id));
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
  StandardKeyMetadata key_metadata(VectorBytes(16, 1), VectorBytes(16, 2), 100);

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

TEST(StandardEncryptionManagerTest, IgnoresOutOfRangeKeyTimestamp) {
  auto kms = MakeKms();
  ICEBERG_UNWRAP_OR_FAIL(auto manager, StandardEncryptionManager::Make(
                                           {.table_key_id = "keyA", .kms = kms}));
  StandardKeyMetadata key_metadata(VectorBytes(16, 1), VectorBytes(16, 2), 100);
  ICEBERG_UNWRAP_OR_FAIL(auto first,
                         manager->RegisterManifestListKeyMetadata(key_metadata));
  EXPECT_EQ(kms->wraps, 1);

  // A KEK whose KEY_TIMESTAMP is corrupted to INT64_MIN must never reach the
  // "now - timestamp" subtraction, which would overflow. It should instead be
  // treated as unparseable, forcing a fresh key encryption key to be created.
  std::vector<EncryptedKey> keys = manager->encryption_keys();
  for (auto& key : keys) {
    if (key.encrypted_by_id == "keyA") {
      key.properties[std::string(StandardEncryptionManager::kKeyTimestamp)] =
          "-9223372036854775808";
    }
  }
  ICEBERG_UNWRAP_OR_FAIL(
      auto reader, StandardEncryptionManager::Make(
                       {.table_key_id = "keyA", .encryption_keys = keys, .kms = kms}));
  ICEBERG_UNWRAP_OR_FAIL(auto second,
                         reader->RegisterManifestListKeyMetadata(key_metadata));
  EXPECT_NE(second.key_encryption_key.key_id, first.key_encryption_key.key_id);
  EXPECT_EQ(kms->wraps, 2);
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
  StandardKeyMetadata no_length(VectorBytes(16, 1));

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

namespace {

Result<std::vector<uint8_t>> ReadAll(InputFile& file) {
  ICEBERG_ASSIGN_OR_RAISE(auto size, file.Size());
  ICEBERG_ASSIGN_OR_RAISE(auto stream, file.Open());
  std::vector<uint8_t> out(size);
  if (size > 0) {
    ICEBERG_RETURN_UNEXPECTED(
        stream->ReadFully(0, std::as_writable_bytes(std::span(out))));
  }
  return out;
}

/// A NativeEncryptionKeyMetadata that implements nothing beyond the required members.
class MinimalNativeKeyMetadata : public NativeEncryptionKeyMetadata {
 public:
  std::vector<uint8_t> buffer() const override { return key_; }
  std::shared_ptr<EncryptionKeyMetadata> Copy() const override {
    return std::make_shared<MinimalNativeKeyMetadata>(*this);
  }
  const std::vector<uint8_t>& EncryptionKey() const override { return key_; }
  const std::optional<std::vector<uint8_t>>& AadPrefix() const override { return aad_; }

 private:
  std::vector<uint8_t> key_ = VectorBytes(16, 1);
  std::optional<std::vector<uint8_t>> aad_;
};

class StandardEncryptionManagerFileTest : public TempFileTestBase {
 protected:
  void SetUp() override {
    TempFileTestBase::SetUp();
    io_ = std::shared_ptr<FileIO>(arrow::MakeLocalFileIO());
  }

  std::shared_ptr<StandardEncryptionManager> NewManager() {
    auto manager =
        StandardEncryptionManager::Make({.table_key_id = "keyA", .kms = MakeKms()});
    EXPECT_THAT(manager, IsOk());
    return manager.value();
  }

  std::unique_ptr<OutputFile> NewOut(const std::string& path) {
    auto out = io_->NewOutputFile(path);
    EXPECT_THAT(out, IsOk());
    return std::move(out.value());
  }

  std::unique_ptr<InputFile> NewIn(const std::string& path) {
    auto in = io_->NewInputFile(path);
    EXPECT_THAT(in, IsOk());
    return std::move(in.value());
  }

  std::string WriteRaw(const std::vector<uint8_t>& bytes) {
    auto path = CreateNewTempFilePath();
    std::ofstream out(path, std::ios::binary);
    out.write(reinterpret_cast<const char*>(bytes.data()),
              static_cast<std::streamsize>(bytes.size()));
    return path;
  }

  /// Encrypt and decrypt two files through the batch overloads of `manager`.
  template <typename Manager>
  void RoundTripBatch(Manager& manager) {
    const std::vector<std::vector<uint8_t>> plains = {VectorBytes(100, 1),
                                                      VectorBytes(3000, 2)};
    std::vector<std::string> paths = {CreateNewTempFilePath(), CreateNewTempFilePath()};
    std::vector<std::unique_ptr<OutputFile>> outs;
    for (const auto& path : paths) {
      outs.push_back(NewOut(path));
    }
    ICEBERG_UNWRAP_OR_FAIL(auto encrypted, manager.Encrypt(std::move(outs)));
    ASSERT_EQ(encrypted.size(), 2u);

    std::vector<std::unique_ptr<EncryptedInputFile>> ins;
    for (size_t i = 0; i < encrypted.size(); ++i) {
      ICEBERG_UNWRAP_OR_FAIL(auto stream,
                             encrypted[i]->encrypting_output_file().CreateOrOverwrite());
      ASSERT_THAT(stream->Write(std::as_bytes(std::span(plains[i]))), IsOk());
      ASSERT_THAT(stream->Close(), IsOk());
      ICEBERG_UNWRAP_OR_FAIL(auto stored, stream->StoredLength());

      std::shared_ptr<EncryptionKeyMetadata> key_metadata;
      if (const auto* native = dynamic_cast<const NativeEncryptionKeyMetadata*>(
              &encrypted[i]->key_metadata())) {
        ICEBERG_UNWRAP_OR_FAIL(auto with_length, native->CopyWithLength(stored));
        key_metadata = with_length;
      } else {
        key_metadata = encrypted[i]->key_metadata().Copy();
      }
      ins.push_back(EncryptedFiles::Encrypted(NewIn(paths[i]), key_metadata));
    }

    ICEBERG_UNWRAP_OR_FAIL(auto decrypted, manager.Decrypt(std::move(ins)));
    ASSERT_EQ(decrypted.size(), 2u);
    for (size_t i = 0; i < decrypted.size(); ++i) {
      ICEBERG_UNWRAP_OR_FAIL(auto bytes, ReadAll(*decrypted[i]));
      EXPECT_EQ(bytes, plains[i]);
    }
  }

  std::shared_ptr<FileIO> io_;
};

}  // namespace

TEST_F(StandardEncryptionManagerFileTest, BatchOverloadsReachableFromConcreteAndBase) {
  auto standard = NewManager();
  std::shared_ptr<EncryptionManager> standard_as_base = standard;
  auto plaintext = std::make_shared<PlaintextEncryptionManager>();
  std::shared_ptr<EncryptionManager> plaintext_as_base = plaintext;

  // Calls through the concrete types only compile if the batch overloads are not hidden.
  RoundTripBatch(*standard);
  RoundTripBatch(*standard_as_base);
  RoundTripBatch(*plaintext);
  RoundTripBatch(*plaintext_as_base);
}

TEST_F(StandardEncryptionManagerFileTest, EncryptExposesNativeAndStreamPaths) {
  auto manager = NewManager();
  auto path = CreateNewTempFilePath();
  const auto plain = VectorBytes(5000, 7);

  ICEBERG_UNWRAP_OR_FAIL(auto encrypted, manager->Encrypt(NewOut(path)));
  auto* native = dynamic_cast<NativeEncryptionOutputFile*>(encrypted.get());
  ASSERT_NE(native, nullptr);

  // Native path: the plain file, and the key parameters.
  EXPECT_EQ(native->plain_output_file().location(), path);
  EXPECT_NE(&native->encrypting_output_file(), &native->plain_output_file());
  const auto& key_metadata = native->key_metadata();
  EXPECT_EQ(key_metadata.EncryptionKey().size(), 16u);
  ASSERT_TRUE(key_metadata.AadPrefix().has_value());
  EXPECT_EQ(key_metadata.AadPrefix()->size(),
            static_cast<size_t>(StandardEncryptionManager::kAadPrefixLength));
  ICEBERG_UNWRAP_OR_FAIL(auto initial_length, key_metadata.FileLength());
  EXPECT_FALSE(initial_length.has_value());

  // Stream path: the OutputFile methods write an AGS1 stream.
  EXPECT_EQ(native->location(), path);
  ICEBERG_UNWRAP_OR_FAIL(auto stream, native->CreateOrOverwrite());
  ASSERT_THAT(stream->Write(std::as_bytes(std::span(plain))), IsOk());
  ASSERT_THAT(stream->Close(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto stored, stream->StoredLength());
  EXPECT_GT(stored, static_cast<int64_t>(plain.size()));
  EXPECT_EQ(stored, static_cast<int64_t>(std::filesystem::file_size(path)));

  // The writer records the stored length on a copy that keeps the same key and AAD.
  ICEBERG_UNWRAP_OR_FAIL(auto with_length, key_metadata.CopyWithLength(stored));
  EXPECT_EQ(with_length->EncryptionKey(), key_metadata.EncryptionKey());
  EXPECT_EQ(with_length->AadPrefix(), key_metadata.AadPrefix());
  ICEBERG_UNWRAP_OR_FAIL(auto copied_length, with_length->FileLength());
  EXPECT_EQ(copied_length, stored);
  ICEBERG_UNWRAP_OR_FAIL(auto original_length, key_metadata.FileLength());
  EXPECT_FALSE(original_length.has_value());

  ICEBERG_UNWRAP_OR_FAIL(auto decrypted, manager->Decrypt(EncryptedFiles::Encrypted(
                                             NewIn(path), with_length)));
  EXPECT_NE(dynamic_cast<NativeEncryptionInputFile*>(decrypted.get()), nullptr);
  ICEBERG_UNWRAP_OR_FAIL(auto bytes, ReadAll(*decrypted));
  EXPECT_EQ(bytes, plain);
}

TEST_F(StandardEncryptionManagerFileTest, NativeReadDoesNotRequireFileLength) {
  auto manager = NewManager();
  const auto raw = VectorBytes(64, 3);
  auto path = WriteRaw(raw);
  auto no_length =
      std::make_shared<StandardKeyMetadata>(VectorBytes(16, 1), VectorBytes(16, 2));

  // Both an in-memory StandardKeyMetadata and its serialized form are accepted.
  std::vector<std::shared_ptr<EncryptionKeyMetadata>> variants = {
      no_length, std::make_shared<BaseEncryptionKeyMetadata>(no_length->Serialize())};
  for (const auto& key_metadata : variants) {
    ICEBERG_UNWRAP_OR_FAIL(auto decrypted, manager->Decrypt(EncryptedFiles::Encrypted(
                                               NewIn(path), key_metadata)));
    auto* native = dynamic_cast<NativeEncryptionInputFile*>(decrypted.get());
    ASSERT_NE(native, nullptr);
    EXPECT_EQ(native->key_metadata().EncryptionKey(), no_length->encryption_key);
    EXPECT_EQ(native->key_metadata().AadPrefix(), no_length->aad_prefix);

    // Native path: the still-encrypted raw bytes.
    auto released = native->ReleaseInputFile();
    ASSERT_NE(released, nullptr);
    ICEBERG_UNWRAP_OR_FAIL(auto bytes, ReadAll(*released));
    EXPECT_EQ(bytes, raw);

    // Stream path: needs the file length.
    EXPECT_THAT(native->Size(), IsError(ErrorKind::kInvalid));
    EXPECT_THAT(native->Open(), IsError(ErrorKind::kInvalid));
  }
}

TEST_F(StandardEncryptionManagerFileTest, DecryptReturnsNativeInputUnchanged) {
  auto manager = NewManager();
  auto path = WriteRaw(VectorBytes(64, 3));
  auto key_metadata =
      std::make_shared<StandardKeyMetadata>(VectorBytes(16, 1), VectorBytes(16, 2));

  ICEBERG_UNWRAP_OR_FAIL(
      auto first, manager->Decrypt(EncryptedFiles::Encrypted(NewIn(path), key_metadata)));
  auto* native = dynamic_cast<NativeEncryptionInputFile*>(first.release());
  ASSERT_NE(native, nullptr);

  ICEBERG_UNWRAP_OR_FAIL(auto second,
                         manager->Decrypt(std::unique_ptr<EncryptedInputFile>(native)));
  EXPECT_EQ(dynamic_cast<NativeEncryptionInputFile*>(second.get()), native);
}

TEST(NativeEncryptionKeyMetadataTest, StandardKeyMetadataCopiesWithLength) {
  StandardKeyMetadata metadata(VectorBytes(16, 1), VectorBytes(16, 2));
  ICEBERG_UNWRAP_OR_FAIL(auto no_length, metadata.FileLength());
  EXPECT_FALSE(no_length.has_value());

  ICEBERG_UNWRAP_OR_FAIL(auto copy, metadata.CopyWithLength(4242));
  EXPECT_EQ(copy->EncryptionKey(), metadata.encryption_key);
  EXPECT_EQ(copy->AadPrefix(), metadata.aad_prefix);
  ICEBERG_UNWRAP_OR_FAIL(auto length, copy->FileLength());
  EXPECT_EQ(length, 4242);
  EXPECT_FALSE(metadata.file_length.has_value());
}

TEST(NativeEncryptionKeyMetadataTest, FileLengthIsNotSupportedByDefault) {
  MinimalNativeKeyMetadata metadata;
  EXPECT_THAT(metadata.FileLength(), IsError(ErrorKind::kNotSupported));
  EXPECT_THAT(metadata.CopyWithLength(1), IsError(ErrorKind::kNotSupported));
}

TEST(BaseEncryptionKeyMetadataTest, DistinguishesAbsentFromEmpty) {
  auto absent = BaseEncryptionKeyMetadata::Empty();
  BaseEncryptionKeyMetadata empty_bytes(std::vector<uint8_t>{});
  BaseEncryptionKeyMetadata some_bytes(VectorBytes(4, 1));

  // Through the base interface, as generic code would see it.
  const EncryptionKeyMetadata& absent_base = *absent;
  const EncryptionKeyMetadata& empty_base = empty_bytes;
  EXPECT_FALSE(absent_base.has_value());
  EXPECT_TRUE(empty_base.has_value());
  EXPECT_TRUE(some_bytes.has_value());
  EXPECT_TRUE(StandardKeyMetadata(VectorBytes(16, 1)).has_value());
  EXPECT_TRUE(absent->buffer().empty());
  EXPECT_TRUE(empty_bytes.buffer().empty());

  // Copies preserve presence.
  auto absent_copy = std::dynamic_pointer_cast<BaseEncryptionKeyMetadata>(absent->Copy());
  auto empty_copy =
      std::dynamic_pointer_cast<BaseEncryptionKeyMetadata>(empty_bytes.Copy());
  ASSERT_NE(absent_copy, nullptr);
  ASSERT_NE(empty_copy, nullptr);
  EXPECT_FALSE(absent_copy->has_value());
  EXPECT_TRUE(empty_copy->has_value());
  EXPECT_EQ(some_bytes.Copy()->buffer(), some_bytes.buffer());
}

TEST_F(StandardEncryptionManagerFileTest, PlaintextManagerReportsAbsentKeyMetadata) {
  PlaintextEncryptionManager manager;
  ICEBERG_UNWRAP_OR_FAIL(auto encrypted,
                         manager.Encrypt(NewOut(CreateNewTempFilePath())));
  EXPECT_FALSE(encrypted->key_metadata().has_value());
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
