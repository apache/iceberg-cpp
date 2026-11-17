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
#include <string>
#include <vector>

#include "iceberg/encryption/encrypted_file.h"
#include "iceberg/encryption/encrypted_key.h"
#include "iceberg/encryption/key_management_client.h"
#include "iceberg/file_io.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"

namespace iceberg {

/// \brief Encrypts and decrypts the files of one table.
///
/// This interface does not require a particular key metadata encoding or encryption
/// algorithm; those are the business of the specific implementation (such as
/// StandardEncryptionManager, which needs a crypto provider and lives in the bundle
/// library) and of EncryptedFiles / EncryptionUtil, which build the EncryptedInputFile /
/// EncryptedOutputFile wrappers this interface consumes. The core library reaches the
/// standard implementation through EncryptionManagerRegistry.
class ICEBERG_EXPORT EncryptionManager {
 public:
  virtual ~EncryptionManager() = default;

  /// \brief Decrypt a file, given its raw bytes and key metadata.
  virtual Result<std::unique_ptr<InputFile>> Decrypt(
      std::unique_ptr<EncryptedInputFile> encrypted) = 0;

  /// \brief Wrap a plaintext output file so that it, and its key metadata, can be
  /// produced once writing is complete.
  virtual Result<std::unique_ptr<EncryptedOutputFile>> Encrypt(
      std::unique_ptr<OutputFile> plain) = 0;

  /// \brief Batch variant of Decrypt(std::unique_ptr<EncryptedInputFile>).
  ///
  /// The default implementation decrypts each file in order; implementations may
  /// override it to decrypt more efficiently, for example by batching KMS calls.
  virtual Result<std::vector<std::unique_ptr<InputFile>>> Decrypt(
      std::vector<std::unique_ptr<EncryptedInputFile>> encrypted);

  /// \brief Batch variant of Encrypt(std::unique_ptr<OutputFile>).
  virtual Result<std::vector<std::unique_ptr<EncryptedOutputFile>>> Encrypt(
      std::vector<std::unique_ptr<OutputFile>> plain);
};

/// \brief An EncryptionManager for unencrypted tables; wraps files without encrypting
/// them.
class ICEBERG_EXPORT PlaintextEncryptionManager : public EncryptionManager {
 public:
  Result<std::unique_ptr<InputFile>> Decrypt(
      std::unique_ptr<EncryptedInputFile> encrypted) override;

  Result<std::unique_ptr<EncryptedOutputFile>> Encrypt(
      std::unique_ptr<OutputFile> plain) override;
};

/// \brief Creates the encryption manager of encrypted tables.
///
/// The bundle library registers the standard implementation (see
/// iceberg::encryption::RegisterAll()). Without it, encrypted tables cannot be read or
/// written. A registry is used so that the core library can reach a standard-specific
/// implementation without depending on it directly; Options is therefore
/// standard-specific and not part of the general EncryptionManager contract.
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
