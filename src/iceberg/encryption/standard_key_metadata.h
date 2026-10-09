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

/// \file iceberg/encryption/standard_key_metadata.h
/// Key metadata of the standard Iceberg encryption scheme.

#include <cstdint>
#include <optional>
#include <span>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"

namespace iceberg {

/// \brief Per-file key metadata of the standard encryption scheme.
///
/// Stored in the `key_metadata` field of data files, delete files and manifest files,
/// and (encrypted by a key encryption key) in the table metadata `encryption-keys` for
/// manifest lists.
///
/// The serialized form is compatible with Java's StandardKeyMetadata: a one-byte schema
/// version (1) followed by an Avro binary encoded record with the fields
/// `encryption_key: bytes`, `aad_prefix: union{null, bytes}` and
/// `file_length: union{null, long}`.
struct ICEBERG_EXPORT StandardKeyMetadata {
  static constexpr uint8_t kV1 = 1;

  /// \brief The data encryption key (16, 24 or 32 bytes).
  std::vector<uint8_t> encryption_key;
  /// \brief The AAD prefix (file ID) used to bind ciphertext to this file.
  std::optional<std::vector<uint8_t>> aad_prefix;
  /// \brief The encrypted (stored) file length, required to read AES GCM streams.
  std::optional<int64_t> file_length;

  /// \brief Parse serialized key metadata.
  static Result<StandardKeyMetadata> Parse(std::span<const uint8_t> buffer);

  /// \brief Serialize to the version-prefixed Avro binary form.
  std::vector<uint8_t> Serialize() const;

  /// \brief Return a copy with the given encrypted file length.
  StandardKeyMetadata WithFileLength(int64_t length) const;

  friend bool operator==(const StandardKeyMetadata&,
                         const StandardKeyMetadata&) = default;
};

}  // namespace iceberg
