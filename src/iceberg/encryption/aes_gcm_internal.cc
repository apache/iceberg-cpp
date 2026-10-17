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

#include "iceberg/encryption/aes_gcm_internal.h"

#include <limits>

#include <openssl/evp.h>
#include <openssl/rand.h>

#include "iceberg/util/macros.h"

namespace iceberg::encryption {

namespace {

const EVP_CIPHER* CipherForKey(size_t key_length) {
  switch (key_length) {
    case 16:
      return EVP_aes_128_gcm();
    case 24:
      return EVP_aes_192_gcm();
    case 32:
      return EVP_aes_256_gcm();
    default:
      return nullptr;
  }
}

EVP_CIPHER_CTX* Ctx(void* ctx) { return static_cast<EVP_CIPHER_CTX*>(ctx); }

}  // namespace

Status SecureRandomBytes(std::span<uint8_t> out) {
  if (out.size() > static_cast<size_t>(std::numeric_limits<int>::max()) ||
      RAND_bytes(out.data(), static_cast<int>(out.size())) != 1) {
    return IOError("Failed to generate secure random bytes");
  }
  return {};
}

Result<std::unique_ptr<AesGcmCipher>> AesGcmCipher::Make(std::span<const uint8_t> key) {
  if (CipherForKey(key.size()) == nullptr) {
    return InvalidArgument("Invalid key length: {} (must be 16, 24, or 32 bytes)",
                           key.size());
  }
  EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
  if (ctx == nullptr) {
    return IOError("Failed to create cipher context");
  }
  return std::unique_ptr<AesGcmCipher>(
      new AesGcmCipher(std::vector<uint8_t>(key.begin(), key.end()), ctx));
}

AesGcmCipher::AesGcmCipher(std::vector<uint8_t> key, void* ctx)
    : key_(std::move(key)), ctx_(ctx) {}

AesGcmCipher::~AesGcmCipher() { EVP_CIPHER_CTX_free(Ctx(ctx_)); }

Status AesGcmCipher::Encrypt(std::span<const uint8_t> plaintext,
                             std::span<const uint8_t> aad, std::span<uint8_t> out) {
  if (out.size() != plaintext.size() + kNonceLength + kGcmTagLength) {
    return InvalidArgument("Invalid ciphertext buffer size: {}", out.size());
  }
  if (plaintext.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
    return InvalidArgument("Plaintext too large: {}", plaintext.size());
  }

  auto nonce = out.first(kNonceLength);
  auto ciphertext = out.subspan(kNonceLength, plaintext.size());
  auto tag = out.last(kGcmTagLength);
  ICEBERG_RETURN_UNEXPECTED(SecureRandomBytes(nonce));

  auto* ctx = Ctx(ctx_);
  int len = 0;
  if (EVP_EncryptInit_ex(ctx, CipherForKey(key_.size()), nullptr, nullptr, nullptr) !=
          1 ||
      EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_IVLEN, kNonceLength, nullptr) != 1 ||
      EVP_EncryptInit_ex(ctx, nullptr, nullptr, key_.data(), nonce.data()) != 1) {
    return IOError("Failed to initialize AES GCM encryption");
  }
  if (!aad.empty() && EVP_EncryptUpdate(ctx, nullptr, &len, aad.data(),
                                        static_cast<int>(aad.size())) != 1) {
    return IOError("Failed to set AES GCM AAD");
  }
  if (!plaintext.empty() &&
      EVP_EncryptUpdate(ctx, ciphertext.data(), &len, plaintext.data(),
                        static_cast<int>(plaintext.size())) != 1) {
    return IOError("Failed to encrypt");
  }
  if (EVP_EncryptFinal_ex(ctx, ciphertext.data() + plaintext.size(), &len) != 1 ||
      EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_GET_TAG, kGcmTagLength, tag.data()) != 1) {
    return IOError("Failed to finalize AES GCM encryption");
  }
  return {};
}

Status AesGcmCipher::Decrypt(std::span<const uint8_t> ciphertext,
                             std::span<const uint8_t> aad, std::span<uint8_t> out) {
  if (ciphertext.size() < static_cast<size_t>(kNonceLength + kGcmTagLength)) {
    return Invalid(
        "Cannot decrypt cipher text of length {}: shorter than nonce and tag, text may "
        "not be encrypted with AES GCM",
        ciphertext.size());
  }
  size_t plaintext_length = ciphertext.size() - kNonceLength - kGcmTagLength;
  if (out.size() != plaintext_length) {
    return InvalidArgument("Invalid plaintext buffer size: {}", out.size());
  }

  auto nonce = ciphertext.first(kNonceLength);
  auto data = ciphertext.subspan(kNonceLength, plaintext_length);
  auto tag = ciphertext.last(kGcmTagLength);

  auto* ctx = Ctx(ctx_);
  int len = 0;
  if (EVP_DecryptInit_ex(ctx, CipherForKey(key_.size()), nullptr, nullptr, nullptr) !=
          1 ||
      EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_IVLEN, kNonceLength, nullptr) != 1 ||
      EVP_DecryptInit_ex(ctx, nullptr, nullptr, key_.data(), nonce.data()) != 1) {
    return IOError("Failed to initialize AES GCM decryption");
  }
  if (!aad.empty() && EVP_DecryptUpdate(ctx, nullptr, &len, aad.data(),
                                        static_cast<int>(aad.size())) != 1) {
    return IOError("Failed to set AES GCM AAD");
  }
  if (!data.empty() && EVP_DecryptUpdate(ctx, out.data(), &len, data.data(),
                                         static_cast<int>(data.size())) != 1) {
    return IOError("Failed to decrypt");
  }
  if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_TAG, kGcmTagLength,
                          const_cast<uint8_t*>(tag.data())) != 1 ||
      EVP_DecryptFinal_ex(ctx, out.data() + plaintext_length, &len) != 1) {
    return Invalid(
        "GCM tag check failed. Possible reasons: wrong decryption key; or "
        "corrupt/tampered data. AES GCM doesn't differentiate between these two.");
  }
  return {};
}

Result<std::vector<uint8_t>> AesGcmCipher::Encrypt(std::span<const uint8_t> plaintext,
                                                   std::span<const uint8_t> aad) {
  std::vector<uint8_t> out(plaintext.size() + kNonceLength + kGcmTagLength);
  ICEBERG_RETURN_UNEXPECTED(Encrypt(plaintext, aad, out));
  return out;
}

Result<std::vector<uint8_t>> AesGcmCipher::Decrypt(std::span<const uint8_t> ciphertext,
                                                   std::span<const uint8_t> aad) {
  if (ciphertext.size() < static_cast<size_t>(kNonceLength + kGcmTagLength)) {
    return Invalid("Cannot decrypt cipher text of length {}", ciphertext.size());
  }
  std::vector<uint8_t> out(ciphertext.size() - kNonceLength - kGcmTagLength);
  ICEBERG_RETURN_UNEXPECTED(Decrypt(ciphertext, aad, out));
  return out;
}

}  // namespace iceberg::encryption
