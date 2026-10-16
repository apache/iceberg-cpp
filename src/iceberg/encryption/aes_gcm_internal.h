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

/// \file iceberg/encryption/aes_gcm_internal.h
/// AES GCM cipher compatible with Java's org.apache.iceberg.encryption.Ciphers.

#include <cstdint>
#include <memory>
#include <span>
#include <vector>

#include "iceberg/result.h"

namespace iceberg::encryption {

inline constexpr int32_t kNonceLength = 12;
inline constexpr int32_t kGcmTagLength = 16;

/// \brief Fill `out` with cryptographically secure random bytes.
Status SecureRandomBytes(std::span<uint8_t> out);

/// \brief AES GCM with a random 12-byte nonce.
///
/// Ciphertext layout: nonce (12) | encrypted data | tag (16).
class AesGcmCipher {
 public:
  /// \brief Create a cipher for a 16, 24 or 32 byte key.
  static Result<std::unique_ptr<AesGcmCipher>> Make(std::span<const uint8_t> key);

  ~AesGcmCipher();

  /// \brief Encrypt `plaintext` into `out`, which must hold plaintext.size() + 28 bytes.
  Status Encrypt(std::span<const uint8_t> plaintext, std::span<const uint8_t> aad,
                 std::span<uint8_t> out);

  /// \brief Decrypt and verify `ciphertext` into `out`, which must hold
  /// ciphertext.size() - 28 bytes.
  Status Decrypt(std::span<const uint8_t> ciphertext, std::span<const uint8_t> aad,
                 std::span<uint8_t> out);

  Result<std::vector<uint8_t>> Encrypt(std::span<const uint8_t> plaintext,
                                       std::span<const uint8_t> aad);
  Result<std::vector<uint8_t>> Decrypt(std::span<const uint8_t> ciphertext,
                                       std::span<const uint8_t> aad);

 private:
  AesGcmCipher(std::vector<uint8_t> key, void* ctx);

  std::vector<uint8_t> key_;
  void* ctx_;  // EVP_CIPHER_CTX
};

}  // namespace iceberg::encryption
