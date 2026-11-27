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
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"

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

  /// \brief Whether key metadata is present.
  ///
  /// Absent key metadata (as for unencrypted files) has an empty buffer(), but so can
  /// present key metadata that is zero bytes long; this tells the two apart.
  virtual bool has_value() const { return true; }

  /// \brief A copy of this key metadata.
  virtual std::shared_ptr<EncryptionKeyMetadata> Copy() const = 0;
};

/// \brief EncryptionKeyMetadata backed by a fixed, already-serialized buffer.
///
/// The buffer is optional so that absent key metadata (Empty(), as used for unencrypted
/// files) can be told apart from present key metadata that happens to be zero bytes long.
class ICEBERG_EXPORT BaseEncryptionKeyMetadata : public EncryptionKeyMetadata {
 public:
  /// \brief Key metadata with the given bytes; a zero-length buffer is still present.
  explicit BaseEncryptionKeyMetadata(std::optional<std::vector<uint8_t>> buffer)
      : buffer_(std::move(buffer)) {}

  /// \brief Absent key metadata.
  static std::shared_ptr<BaseEncryptionKeyMetadata> Empty() {
    return std::make_shared<BaseEncryptionKeyMetadata>(std::nullopt);
  }

  /// \brief Whether key metadata is present, even if it is zero bytes long.
  bool has_value() const override { return buffer_.has_value(); }

  /// \brief The key metadata bytes, or an empty buffer if absent.
  std::vector<uint8_t> buffer() const override {
    return buffer_.value_or(std::vector<uint8_t>{});
  }

  std::shared_ptr<EncryptionKeyMetadata> Copy() const override {
    return std::make_shared<BaseEncryptionKeyMetadata>(*this);
  }

 private:
  std::optional<std::vector<uint8_t>> buffer_;
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

  /// \brief The encrypted (stored) file length, or nullopt if not yet known.
  ///
  /// Returns NotSupported unless the implementation tracks a file length.
  virtual Result<std::optional<int64_t>> FileLength() const {
    return NotSupported("FileLength is not implemented by this key metadata");
  }

  /// \brief A copy of this key metadata, with the same key and AAD prefix, that records
  /// the given encrypted file length.
  ///
  /// Writers call this after closing the file, once the stored length is known. Returns
  /// NotSupported unless the implementation tracks a file length.
  virtual Result<std::shared_ptr<NativeEncryptionKeyMetadata>> CopyWithLength(
      int64_t length) const {
    return NotSupported("CopyWithLength is not implemented by this key metadata");
  }
};

}  // namespace iceberg
