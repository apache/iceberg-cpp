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

/// \file iceberg/encryption/encryption_manager.h
/// Table encryption manager interface and the registry that creates one.

#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "iceberg/encryption/encrypted_key.h"
#include "iceberg/encryption/key_management_client.h"
#include "iceberg/encryption/standard_key_metadata.h"
#include "iceberg/file_io.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"

namespace iceberg {

/// \brief Keys created when the key metadata of a new manifest list is registered.
///
/// Both must be added to the table metadata `encryption-keys` in the same commit as
/// the snapshot whose `key-id` is `file_key.key_id`.
struct ICEBERG_EXPORT ManifestListEncryptionKeys {
  /// The key encryption key, wrapped by the table master key in the KMS.
  EncryptedKey key_encryption_key;
  /// The manifest list key metadata, encrypted by the key encryption key.
  EncryptedKey file_key;
};

/// \brief Encrypts and decrypts the files of one table.
///
/// The standard implementation (StandardEncryptionManager) needs a crypto provider
/// and lives in the bundle library; the core library reaches it through
/// EncryptionManagerRegistry.
class ICEBERG_EXPORT EncryptionManager {
 public:
  virtual ~EncryptionManager() = default;

  /// \brief Generate key metadata (random data key and AAD prefix) for a new file.
  virtual Result<StandardKeyMetadata> NewKeyMetadata() = 0;

  /// \brief Wrap an AES GCM Stream (AGS1) file in a decrypting InputFile.
  ///
  /// \param encrypted the encrypted file
  /// \param key_metadata serialized key metadata, including the file length
  virtual Result<std::unique_ptr<InputFile>> NewDecryptingInputFile(
      std::unique_ptr<InputFile> encrypted, std::span<const uint8_t> key_metadata) = 0;

  /// \brief Wrap a file in an AES GCM Stream (AGS1) encrypting OutputFile.
  virtual Result<std::unique_ptr<OutputFile>> NewEncryptingOutputFile(
      std::unique_ptr<OutputFile> plain, const StandardKeyMetadata& key_metadata) = 0;

  /// \brief Decrypt the key metadata of a manifest list.
  ///
  /// \param manifest_list_key_id the snapshot `key-id`
  /// \return the serialized StandardKeyMetadata of the manifest list file
  virtual Result<std::vector<uint8_t>> DecryptManifestListKeyMetadata(
      std::string_view manifest_list_key_id) = 0;

  /// \brief Encrypt the key metadata of a new manifest list with a key encryption key.
  ///
  /// Creates a key encryption key (wrapped by the table master key) if there is no
  /// unexpired one.
  virtual Result<ManifestListEncryptionKeys> RegisterManifestListKeyMetadata(
      const StandardKeyMetadata& key_metadata) = 0;
};

/// \brief Creates the encryption manager of encrypted tables.
///
/// The bundle library registers the standard implementation (see
/// iceberg::encryption::RegisterAll()). Without it, encrypted tables cannot be read or
/// written.
class ICEBERG_EXPORT EncryptionManagerRegistry {
 public:
  struct Options {
    /// The table master key ID in the KMS (table property `encryption.key-id`), taken
    /// from a trusted source.
    std::string table_key_id;
    /// Length of generated data keys: 16, 24 or 32 bytes.
    int32_t data_key_length = 16;
    /// The table metadata `encryption-keys`.
    std::vector<EncryptedKey> encryption_keys;
    /// The catalog's KMS client.
    std::shared_ptr<KeyManagementClient> kms;
  };

  using Factory =
      std::function<Result<std::shared_ptr<EncryptionManager>>(Options options)>;

  /// \brief Register the encryption manager implementation.
  static void Register(Factory factory);

  /// \brief Create an encryption manager.
  static Result<std::shared_ptr<EncryptionManager>> Make(Options options);
};

}  // namespace iceberg
