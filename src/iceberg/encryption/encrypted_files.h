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

/// \file iceberg/encryption/encrypted_files.h
/// Builds the generic EncryptedInputFile / EncryptedOutputFile wrappers that
/// EncryptionManager consumes and produces, mirroring Java's EncryptedFiles.

#include <memory>

#include "iceberg/encryption/encrypted_file.h"
#include "iceberg/iceberg_export.h"

namespace iceberg {

/// \brief Builds EncryptedInputFile / EncryptedOutputFile wrappers around a plain file
/// and its key metadata, for callers that do not need the native (modular) encryption
/// wrappers.
class ICEBERG_EXPORT EncryptedFiles {
 public:
  /// \brief Wrap a still-encrypted input file together with its key metadata.
  static std::unique_ptr<EncryptedInputFile> Encrypted(
      std::unique_ptr<InputFile> encrypted_file,
      std::shared_ptr<EncryptionKeyMetadata> key_metadata);

  /// \brief Wrap a plaintext output file together with the key metadata used (or to be
  /// used) to encrypt it.
  static std::unique_ptr<EncryptedOutputFile> Encrypted(
      std::unique_ptr<OutputFile> output_file,
      std::shared_ptr<EncryptionKeyMetadata> key_metadata);
};

}  // namespace iceberg
