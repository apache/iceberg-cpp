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

#include "iceberg/encryption/aes_gcm_stream.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>
#include <utility>

#include "iceberg/encryption/aes_gcm_internal.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

using encryption::AesGcmCipher;

constexpr int64_t kPlainBlock = AesGcmStream::kPlainBlockSize;
constexpr int64_t kCipherBlock = AesGcmStream::kCipherBlockSize;
constexpr int64_t kBlockOverhead =
    AesGcmStream::kNonceLength + AesGcmStream::kGcmTagLength;

/// \brief Block AAD: file AAD prefix followed by the block index as a 4-byte LE int.
std::vector<uint8_t> BlockAad(const std::vector<uint8_t>& aad_prefix,
                              int32_t block_index) {
  std::vector<uint8_t> aad(aad_prefix);
  for (int i = 0; i < 4; ++i) {
    aad.push_back(
        static_cast<uint8_t>((static_cast<uint32_t>(block_index) >> (8 * i)) & 0xFF));
  }
  return aad;
}

std::array<std::byte, AesGcmStream::kHeaderLength> HeaderBytes() {
  std::array<std::byte, AesGcmStream::kHeaderLength> header{};
  std::memcpy(header.data(), AesGcmStream::kMagic.data(), AesGcmStream::kMagic.size());
  for (int i = 0; i < 4; ++i) {
    header[4 + i] = static_cast<std::byte>(
        (static_cast<uint32_t>(AesGcmStream::kPlainBlockSize) >> (8 * i)) & 0xFF);
  }
  return header;
}

class AesGcmInputStream : public SeekableInputStream {
 public:
  /// \brief Construct over an encrypted length already accepted by
  /// AesGcmStream::PlaintextLength, which bounds the block count.
  AesGcmInputStream(std::unique_ptr<SeekableInputStream> source,
                    std::unique_ptr<AesGcmCipher> cipher, std::vector<uint8_t> aad_prefix,
                    int64_t encrypted_length, int64_t plain_size)
      : source_(std::move(source)),
        cipher_(std::move(cipher)),
        aad_prefix_(std::move(aad_prefix)),
        cipher_block_(kCipherBlock),
        plain_block_(kPlainBlock),
        plain_size_(plain_size) {
    int64_t stream_length = encrypted_length - AesGcmStream::kHeaderLength;
    int64_t num_full_blocks = stream_length / kCipherBlock;
    int64_t last_bytes = stream_length - num_full_blocks * kCipherBlock;
    bool full_blocks_only = last_bytes == 0;
    num_blocks_ = full_blocks_only ? num_full_blocks : num_full_blocks + 1;
    last_cipher_block_size_ = full_blocks_only ? kCipherBlock : last_bytes;
  }

  Status ValidateHeader() {
    std::array<std::byte, AesGcmStream::kHeaderLength> header{};
    ICEBERG_RETURN_UNEXPECTED(source_->ReadFully(0, header));
    if (std::memcmp(header.data(), AesGcmStream::kMagic.data(),
                    AesGcmStream::kMagic.size()) != 0) {
      return Invalid("Invalid GCM stream: magic does not match AGS1");
    }
    uint32_t plain_block_size = 0;
    for (int i = 0; i < 4; ++i) {
      plain_block_size |= static_cast<uint32_t>(header[4 + i]) << (8 * i);
    }
    if (plain_block_size != static_cast<uint32_t>(AesGcmStream::kPlainBlockSize)) {
      return Invalid("Invalid GCM stream: block size {} != {}", plain_block_size,
                     AesGcmStream::kPlainBlockSize);
    }
    return {};
  }

  /// \brief Authenticate the sole block of an empty stream.
  ///
  /// An empty AGS1 file still carries one empty block with a nonce and a tag, so a
  /// reader that stops at plain_size_ == 0 would accept a wrong key, a wrong AAD
  /// prefix, a corrupt tag or a file truncated behind its trusted length. Java
  /// authenticates the block on the first read; an empty stream is never read from,
  /// so it is authenticated when the stream is opened.
  Status ValidateEmptyStream() {
    if (plain_size_ != 0) {
      return {};
    }
    return DecryptBlock(0);
  }

  Result<int64_t> Position() const override { return position_; }

  Status Seek(int64_t position) override {
    if (position < 0 || position > plain_size_) {
      return IOError("Invalid position {} for stream of length {}", position,
                     plain_size_);
    }
    position_ = position;
    return {};
  }

  Result<int64_t> Read(std::span<std::byte> out) override {
    int64_t total = 0;
    while (std::cmp_less(total, out.size()) && position_ < plain_size_) {
      int64_t block_index = position_ / kPlainBlock;
      ICEBERG_RETURN_UNEXPECTED(DecryptBlock(block_index));
      int64_t offset = position_ % kPlainBlock;
      int64_t n = std::min<int64_t>(current_block_size_ - offset,
                                    static_cast<int64_t>(out.size()) - total);
      std::memcpy(out.data() + total, plain_block_.data() + offset, n);
      total += n;
      position_ += n;
    }
    return total;
  }

  Status ReadFully(int64_t position, std::span<std::byte> out) override {
    // The position is bounded before the length is added to it, so that a position
    // near int64 max does not overflow the range check.
    if (position < 0 || position > plain_size_ ||
        std::cmp_greater(out.size(), plain_size_ - position)) {
      return IOError("Cannot read {} bytes at {} from stream of length {}", out.size(),
                     position, plain_size_);
    }
    ICEBERG_RETURN_UNEXPECTED(Seek(position));
    ICEBERG_ASSIGN_OR_RAISE(auto n, Read(out));
    if (std::cmp_not_equal(n, out.size())) {
      return IOError("Short read from GCM stream");
    }
    return {};
  }

  Status Close() override { return source_->Close(); }

 private:
  Status DecryptBlock(int64_t block_index) {
    if (block_index == current_block_index_) {
      return {};
    }
    // num_blocks_ is bounded by int32 max at construction, so the cast below is safe.
    int64_t size =
        block_index == num_blocks_ - 1 ? last_cipher_block_size_ : kCipherBlock;
    auto cipher_block = std::span(cipher_block_).first(size);
    ICEBERG_RETURN_UNEXPECTED(
        source_->ReadFully(AesGcmStream::kHeaderLength + block_index * kCipherBlock,
                           std::as_writable_bytes(cipher_block)));
    auto aad = BlockAad(aad_prefix_, static_cast<int32_t>(block_index));
    int64_t plain_size = size - kBlockOverhead;
    // Decryption writes plaintext before the tag is checked, so the cached block is
    // invalid until the whole block authenticates.
    current_block_index_ = -1;
    current_block_size_ = 0;
    ICEBERG_RETURN_UNEXPECTED(
        cipher_->Decrypt(cipher_block, aad, std::span(plain_block_).first(plain_size)));
    current_block_size_ = plain_size;
    current_block_index_ = block_index;
    return {};
  }

  std::unique_ptr<SeekableInputStream> source_;
  std::unique_ptr<AesGcmCipher> cipher_;
  std::vector<uint8_t> aad_prefix_;
  std::vector<uint8_t> cipher_block_;
  std::vector<uint8_t> plain_block_;
  int64_t num_blocks_ = 0;
  int64_t last_cipher_block_size_ = 0;
  int64_t plain_size_ = 0;
  int64_t position_ = 0;
  int64_t current_block_index_ = -1;
  int64_t current_block_size_ = 0;
};

class AesGcmOutputStream : public PositionOutputStream {
 public:
  AesGcmOutputStream(std::unique_ptr<PositionOutputStream> target,
                     std::unique_ptr<AesGcmCipher> cipher,
                     std::vector<uint8_t> aad_prefix)
      : target_(std::move(target)),
        cipher_(std::move(cipher)),
        aad_prefix_(std::move(aad_prefix)),
        plain_block_(kPlainBlock),
        cipher_block_(kCipherBlock) {}

  Result<int64_t> Position() const override {
    // Closing flushes the last block, which resets the in-block position, so the
    // plaintext length is captured before that happens.
    if (closed_) {
      return final_position_;
    }
    return block_index_ * kPlainBlock + position_in_block_;
  }

  Result<int64_t> StoredLength() const override { return target_->StoredLength(); }

  Status Write(std::span<const std::byte> data) override {
    if (closed_) {
      return IOError("Writing to closed stream");
    }
    ICEBERG_RETURN_UNEXPECTED(WriteHeaderIfNeeded());
    while (!data.empty()) {
      int64_t n = std::min<int64_t>(kPlainBlock - position_in_block_, data.size());
      std::memcpy(plain_block_.data() + position_in_block_, data.data(), n);
      position_in_block_ += n;
      data = data.subspan(n);
      if (position_in_block_ == kPlainBlock) {
        ICEBERG_RETURN_UNEXPECTED(EncryptAndWriteBlock());
      }
    }
    return {};
  }

  Status Flush() override { return target_->Flush(); }

  Status Close() override {
    if (closed_) {
      return {};
    }
    ICEBERG_RETURN_UNEXPECTED(WriteHeaderIfNeeded());
    ICEBERG_ASSIGN_OR_RAISE(final_position_, Position());
    // Like Java, an empty stream gets one empty block; otherwise only a non-empty
    // partial block is written.
    if (position_in_block_ > 0 || block_index_ == 0) {
      ICEBERG_RETURN_UNEXPECTED(EncryptAndWriteBlock());
    }
    closed_ = true;
    return target_->Close();
  }

 private:
  Status WriteHeaderIfNeeded() {
    if (!header_written_) {
      auto header = HeaderBytes();
      ICEBERG_RETURN_UNEXPECTED(target_->Write(header));
      header_written_ = true;
    }
    return {};
  }

  Status EncryptAndWriteBlock() {
    if (last_block_written_) {
      return Invalid("Cannot encrypt block: a partial block has already been written");
    }
    if (block_index_ == std::numeric_limits<int32_t>::max()) {
      return IOError("Cannot write block: exceeded Integer.MAX_VALUE blocks");
    }
    if (position_in_block_ != kPlainBlock) {
      last_block_written_ = true;
    }
    auto aad = BlockAad(aad_prefix_, static_cast<int32_t>(block_index_));
    auto out = std::span(cipher_block_).first(position_in_block_ + kBlockOverhead);
    ICEBERG_RETURN_UNEXPECTED(
        cipher_->Encrypt(std::span(plain_block_).first(position_in_block_), aad, out));
    ICEBERG_RETURN_UNEXPECTED(target_->Write(std::as_bytes(out)));
    position_in_block_ = 0;
    ++block_index_;
    return {};
  }

  std::unique_ptr<PositionOutputStream> target_;
  std::unique_ptr<AesGcmCipher> cipher_;
  std::vector<uint8_t> aad_prefix_;
  std::vector<uint8_t> plain_block_;
  std::vector<uint8_t> cipher_block_;
  int64_t position_in_block_ = 0;
  int64_t block_index_ = 0;
  int64_t final_position_ = 0;
  bool header_written_ = false;
  bool last_block_written_ = false;
  bool closed_ = false;
};

}  // namespace

Result<int64_t> AesGcmStream::PlaintextLength(int64_t encrypted_length) {
  if (encrypted_length < kMinStreamLength) {
    return Invalid(
        "Invalid encrypted stream: {} is shorter than the minimum possible stream "
        "length {}",
        encrypted_length, kMinStreamLength);
  }
  int64_t stream_length = encrypted_length - kHeaderLength;
  int64_t num_full_blocks = stream_length / kCipherBlockSize;
  int64_t last_bytes = stream_length - num_full_blocks * kCipherBlockSize;
  int64_t num_blocks = last_bytes == 0 ? num_full_blocks : num_full_blocks + 1;
  // Block AAD carries the block index as an int32, so Java bounds the block count
  // with Math.toIntExact. Reject here rather than when the block is decrypted.
  if (num_blocks > std::numeric_limits<int32_t>::max()) {
    return Invalid("Invalid encrypted stream: {} bytes exceeds the maximum of {} blocks",
                   encrypted_length, std::numeric_limits<int32_t>::max());
  }
  if (last_bytes != 0 && last_bytes < kNonceLength + kGcmTagLength) {
    return Invalid("Invalid GCM stream: truncated last block");
  }
  int64_t last_plain = last_bytes == 0 ? 0 : last_bytes - kNonceLength - kGcmTagLength;
  return num_full_blocks * kPlainBlockSize + last_plain;
}

AesGcmInputFile::AesGcmInputFile(std::unique_ptr<InputFile> source,
                                 std::vector<uint8_t> key,
                                 std::vector<uint8_t> aad_prefix,
                                 int64_t encrypted_length)
    : source_(std::move(source)),
      key_(std::move(key)),
      aad_prefix_(std::move(aad_prefix)),
      encrypted_length_(encrypted_length) {}

Result<int64_t> AesGcmInputFile::Size() const {
  return AesGcmStream::PlaintextLength(encrypted_length_);
}

Result<std::unique_ptr<SeekableInputStream>> AesGcmInputFile::Open() {
  ICEBERG_ASSIGN_OR_RAISE(auto plain_size,
                          AesGcmStream::PlaintextLength(encrypted_length_));
  ICEBERG_ASSIGN_OR_RAISE(auto cipher, AesGcmCipher::Make(key_));
  ICEBERG_ASSIGN_OR_RAISE(auto source, source_->Open());
  auto stream = std::make_unique<AesGcmInputStream>(
      std::move(source), std::move(cipher), aad_prefix_, encrypted_length_, plain_size);
  ICEBERG_RETURN_UNEXPECTED(stream->ValidateHeader());
  ICEBERG_RETURN_UNEXPECTED(stream->ValidateEmptyStream());
  return stream;
}

AesGcmOutputFile::AesGcmOutputFile(std::unique_ptr<OutputFile> target,
                                   std::vector<uint8_t> key,
                                   std::vector<uint8_t> aad_prefix)
    : target_(std::move(target)),
      key_(std::move(key)),
      aad_prefix_(std::move(aad_prefix)) {}

Result<std::unique_ptr<PositionOutputStream>> AesGcmOutputFile::Create() {
  ICEBERG_ASSIGN_OR_RAISE(auto cipher, encryption::AesGcmCipher::Make(key_));
  ICEBERG_ASSIGN_OR_RAISE(auto target, target_->Create());
  return std::make_unique<AesGcmOutputStream>(std::move(target), std::move(cipher),
                                              aad_prefix_);
}

Result<std::unique_ptr<PositionOutputStream>> AesGcmOutputFile::CreateOrOverwrite() {
  ICEBERG_ASSIGN_OR_RAISE(auto cipher, encryption::AesGcmCipher::Make(key_));
  ICEBERG_ASSIGN_OR_RAISE(auto target, target_->CreateOrOverwrite());
  return std::make_unique<AesGcmOutputStream>(std::move(target), std::move(cipher),
                                              aad_prefix_);
}

}  // namespace iceberg
