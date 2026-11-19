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

/// \file iceberg/encryption/encryption_key_metadata.h
/// Opaque, serialized key metadata, mirroring Java's EncryptionKeyMetadata.

#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

#include "iceberg/iceberg_export.h"

namespace iceberg {

/// \brief Opaque serialized key metadata for one file.
///
/// Implementations are free to choose any serialization; EncryptionManager and its
/// callers treat the result as an opaque byte buffer.
class ICEBERG_EXPORT EncryptionKeyMetadata {
 public:
  virtual ~EncryptionKeyMetadata() = default;

  /// \brief The serialized form of this key metadata.
  ///
  /// Implementations must not cache this value if their fields can change after
  /// construction; it is recomputed on every call where that matters.
  virtual std::vector<uint8_t> buffer() const = 0;
};

/// \brief EncryptionKeyMetadata backed by a fixed, already-serialized buffer.
class ICEBERG_EXPORT BaseEncryptionKeyMetadata : public EncryptionKeyMetadata {
 public:
  explicit BaseEncryptionKeyMetadata(std::vector<uint8_t> buffer)
      : buffer_(std::move(buffer)) {}

  std::vector<uint8_t> buffer() const override { return buffer_; }

 private:
  std::vector<uint8_t> buffer_;
};

/// \brief Key metadata for the native (AES-GCM) encryption key parameters.
///
/// Exposes the data encryption key and AAD prefix directly, for formats (such as
/// Parquet) that perform their own native encryption rather than using the AGS1 stream
/// format.
class ICEBERG_EXPORT NativeEncryptionKeyMetadata : public EncryptionKeyMetadata {
 public:
  /// \brief The data encryption key.
  virtual const std::vector<uint8_t>& EncryptionKey() const = 0;

  /// \brief The AAD prefix (file ID) used to bind ciphertext to this file, if any.
  virtual const std::optional<std::vector<uint8_t>>& AadPrefix() const = 0;
};

}  // namespace iceberg
