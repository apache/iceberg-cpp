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

/// \file iceberg/encryption/encrypted_file.h
/// Pairs a file handle with its key metadata, mirroring Java's
/// EncryptedInputFile / EncryptedOutputFile.

#include <memory>
#include <utility>

#include "iceberg/encryption/encryption_key_metadata.h"
#include "iceberg/file_io.h"
#include "iceberg/iceberg_export.h"

namespace iceberg {

/// \brief An encrypted input file together with the key metadata needed to decrypt it.
class ICEBERG_EXPORT EncryptedInputFile {
 public:
  virtual ~EncryptedInputFile() = default;

  /// \brief Give up the still-encrypted raw input file.
  virtual std::unique_ptr<InputFile> ReleaseInputFile() = 0;

  /// \brief The key metadata needed to decrypt this file.
  virtual const EncryptionKeyMetadata& key_metadata() const = 0;
};

/// \brief EncryptedInputFile backed by a plain InputFile and opaque key metadata.
class ICEBERG_EXPORT BaseEncryptedInputFile : public EncryptedInputFile {
 public:
  BaseEncryptedInputFile(std::unique_ptr<InputFile> encrypted_file,
                         std::shared_ptr<EncryptionKeyMetadata> key_metadata)
      : encrypted_file_(std::move(encrypted_file)),
        key_metadata_(std::move(key_metadata)) {}

  std::unique_ptr<InputFile> ReleaseInputFile() override {
    return std::move(encrypted_file_);
  }

  const EncryptionKeyMetadata& key_metadata() const override { return *key_metadata_; }

 private:
  std::unique_ptr<InputFile> encrypted_file_;
  std::shared_ptr<EncryptionKeyMetadata> key_metadata_;
};

/// \brief An encrypted input file that supports both read paths of Java's
/// NativeEncryptionInputFile.
///
/// - Native path, for formats such as Parquet that perform their own modular decryption:
///   ReleaseInputFile() gives the still-encrypted raw file and key_metadata() gives the
///   key parameters (and AAD prefix) the native reader needs.
/// - Stream path: the InputFile methods read the file as an AES GCM stream (AGS1),
///   decrypting it, and require the encrypted file length in the key metadata.
///
/// Implementations may defer building the stream reader until the InputFile methods are
/// first used, so native-only callers are not subject to the stream path's requirements.
class ICEBERG_EXPORT NativeEncryptionInputFile : public EncryptedInputFile,
                                                 public InputFile {
 public:
  const NativeEncryptionKeyMetadata& key_metadata() const override = 0;
};

/// \brief An encrypting output file together with the key metadata that was used (or
/// will be used) to encrypt it.
class ICEBERG_EXPORT EncryptedOutputFile {
 public:
  virtual ~EncryptedOutputFile() = default;

  /// \brief The output file that writers should use.
  ///
  /// Unlike EncryptedInputFile::ReleaseInputFile, this is a non-consuming accessor:
  /// implementations may need to keep ownership to finalize encryption on Close.
  virtual OutputFile& encrypting_output_file() const = 0;

  /// \brief The key metadata that was used to encrypt this file.
  virtual const EncryptionKeyMetadata& key_metadata() const = 0;
};

/// \brief EncryptedOutputFile backed by a plain OutputFile and opaque key metadata.
class ICEBERG_EXPORT BaseEncryptedOutputFile : public EncryptedOutputFile {
 public:
  BaseEncryptedOutputFile(std::unique_ptr<OutputFile> output_file,
                          std::shared_ptr<EncryptionKeyMetadata> key_metadata)
      : output_file_(std::move(output_file)), key_metadata_(std::move(key_metadata)) {}

  OutputFile& encrypting_output_file() const override { return *output_file_; }
  const EncryptionKeyMetadata& key_metadata() const override { return *key_metadata_; }

 private:
  std::unique_ptr<OutputFile> output_file_;
  std::shared_ptr<EncryptionKeyMetadata> key_metadata_;
};

/// \brief An encrypting output file that supports both write paths of Java's
/// NativeEncryptionOutputFile.
///
/// - Native path, for formats such as Parquet that perform their own modular encryption:
///   plain_output_file() gives the file to write the (already encrypted) bytes to and
///   key_metadata() gives the key parameters (and AAD prefix) the native writer needs.
/// - Stream path: encrypting_output_file(), and the OutputFile methods that delegate to
///   it, write the file as an AES GCM stream (AGS1).
class ICEBERG_EXPORT NativeEncryptionOutputFile : public EncryptedOutputFile,
                                                  public OutputFile {
 public:
  /// \brief The underlying plain output file, for native writers.
  virtual OutputFile& plain_output_file() const = 0;

  const NativeEncryptionKeyMetadata& key_metadata() const override = 0;

  // OutputFile, delegating to the stream path.
  std::string_view location() const override {
    return encrypting_output_file().location();
  }
  Result<std::unique_ptr<PositionOutputStream>> Create() override {
    return encrypting_output_file().Create();
  }
  Result<std::unique_ptr<PositionOutputStream>> CreateOrOverwrite() override {
    return encrypting_output_file().CreateOrOverwrite();
  }
};

}  // namespace iceberg
