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

/// \brief An encrypted input file for formats, such as Parquet, that perform their own
/// native (modular) decryption rather than using the AGS1 stream format.
///
/// Also implements InputFile so that the still-encrypted bytes remain directly
/// readable; the native reader uses key_metadata() to obtain the key parameters needed
/// to decrypt the individual modules it reads.
class ICEBERG_EXPORT NativeEncryptionInputFile : public EncryptedInputFile,
                                                 public InputFile {
 public:
  NativeEncryptionInputFile(std::unique_ptr<InputFile> encrypted_file,
                            std::shared_ptr<NativeEncryptionKeyMetadata> key_metadata)
      : encrypted_file_(std::move(encrypted_file)),
        key_metadata_(std::move(key_metadata)) {}

  // EncryptedInputFile
  std::unique_ptr<InputFile> ReleaseInputFile() override {
    return std::move(encrypted_file_);
  }
  const NativeEncryptionKeyMetadata& key_metadata() const override {
    return *key_metadata_;
  }

  // InputFile, passed through to the still-encrypted raw file.
  std::string_view location() const override { return encrypted_file_->location(); }
  Result<int64_t> Size() const override { return encrypted_file_->Size(); }
  Result<std::unique_ptr<SeekableInputStream>> Open() override {
    return encrypted_file_->Open();
  }

 private:
  std::unique_ptr<InputFile> encrypted_file_;
  std::shared_ptr<NativeEncryptionKeyMetadata> key_metadata_;
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

/// \brief An encrypted output file for formats, such as Parquet, that perform their own
/// native (modular) encryption rather than using the AGS1 stream format.
///
/// Also implements OutputFile so the plaintext bytes can be written directly; the
/// native writer uses key_metadata() to obtain the key parameters it needs to encrypt
/// the individual modules it writes.
class ICEBERG_EXPORT NativeEncryptionOutputFile : public EncryptedOutputFile,
                                                  public OutputFile {
 public:
  NativeEncryptionOutputFile(std::unique_ptr<OutputFile> plain_file,
                             std::shared_ptr<NativeEncryptionKeyMetadata> key_metadata)
      : plain_file_(std::move(plain_file)), key_metadata_(std::move(key_metadata)) {}

  // EncryptedOutputFile
  OutputFile& encrypting_output_file() const override { return *plain_file_; }
  const NativeEncryptionKeyMetadata& key_metadata() const override {
    return *key_metadata_;
  }

  // OutputFile, passed through to the plaintext file that the native writer fills in.
  std::string_view location() const override { return plain_file_->location(); }
  Result<std::unique_ptr<PositionOutputStream>> Create() override {
    return plain_file_->Create();
  }
  Result<std::unique_ptr<PositionOutputStream>> CreateOrOverwrite() override {
    return plain_file_->CreateOrOverwrite();
  }

 private:
  std::unique_ptr<OutputFile> plain_file_;
  std::shared_ptr<NativeEncryptionKeyMetadata> key_metadata_;
};

}  // namespace iceberg
