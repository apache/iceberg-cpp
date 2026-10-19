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

#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_io_util.h"
#include "iceberg/encryption/aes_gcm_internal.h"
#include "iceberg/encryption/aes_gcm_stream.h"
#include "iceberg/encryption/standard_key_metadata.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/temp_file_test_base.h"

namespace iceberg {

namespace {

using encryption::AesGcmCipher;

/// \brief Deterministic test bytes: byte i is (i * 31 + seed).
std::vector<uint8_t> TestBytes(size_t length, int seed) {
  std::vector<uint8_t> bytes(length);
  for (size_t i = 0; i < length; ++i) {
    bytes[i] = static_cast<uint8_t>(static_cast<int>(i) * 31 + seed);
  }
  return bytes;
}

std::span<const uint8_t> AsBytes(std::string_view s) {
  return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
}

// The fixtures below were produced by Iceberg Java. It used TestBytes(16, 1) as the
// data key, TestBytes(16, 2) as the file AAD prefix and TestBytes(16, 3) as the key
// encryption key.
const std::vector<uint8_t> kDataKey = TestBytes(16, 1);
const std::vector<uint8_t> kAadPrefix = TestBytes(16, 2);
const std::vector<uint8_t> kKek = TestBytes(16, 3);

// StandardEncryptionManager encrypts manifest list key metadata under the key
// encryption key, with the KEK timestamp as AAD.
constexpr std::string_view kKekTimestamp = "1791455219054";

// Manifest list key metadata encrypted by Java under kKek with kKekTimestamp
// as AAD: encryption key TestBytes(16, 1), AAD prefix TestBytes(16, 2),
// file length 4242.
const std::vector<uint8_t> kJavaManifestListKeyMetadata = {
    0xf2, 0x80, 0x57, 0xac, 0x14, 0xdc, 0x8d, 0xa7, 0xb2, 0x25, 0x58, 0x24, 0xa3, 0xe1,
    0x88, 0xb0, 0xde, 0x2e, 0x94, 0x09, 0x9b, 0x7d, 0xe4, 0x7d, 0x0e, 0xd6, 0xb0, 0x7a,
    0xb1, 0x86, 0x7b, 0xca, 0x61, 0x11, 0x2a, 0x11, 0x4e, 0xc1, 0xa8, 0x23, 0xd8, 0xa5,
    0xca, 0x43, 0x9c, 0x33, 0x6a, 0xe9, 0x6b, 0xf2, 0xc6, 0xdb, 0xa8, 0xe0, 0x2e, 0x7f,
    0xc0, 0xcc, 0x0e, 0xf0, 0xb7, 0xec, 0xb9, 0x1f, 0x06, 0x3a, 0xe7};
// AES GCM stream of TestBytes(0, 5) written by Java's AesGcmOutputStream
// with kDataKey and kAadPrefix.
const std::vector<uint8_t> kJavaAgs1Empty = {
    0x41, 0x47, 0x53, 0x31, 0x00, 0x00, 0x10, 0x00, 0x92, 0xd6, 0x53, 0x8c,
    0x2b, 0x71, 0x45, 0x37, 0x37, 0x86, 0x76, 0x8f, 0xef, 0xd4, 0xdc, 0x98,
    0x15, 0xf1, 0x93, 0x9d, 0x6c, 0x56, 0x4b, 0xae, 0x24, 0xff, 0xdc, 0xb4};
// AES GCM stream of TestBytes(1, 5) written by Java's AesGcmOutputStream
// with kDataKey and kAadPrefix.
const std::vector<uint8_t> kJavaAgs1OneByte = {
    0x41, 0x47, 0x53, 0x31, 0x00, 0x00, 0x10, 0x00, 0xf8, 0xde, 0xe0, 0x44, 0x00,
    0x8d, 0xbb, 0x2b, 0x6b, 0x29, 0x58, 0xb2, 0x70, 0x62, 0x46, 0xbb, 0xb1, 0xf8,
    0x76, 0x29, 0x69, 0x86, 0x14, 0x6c, 0x08, 0x98, 0x84, 0xdc, 0x3b};
// AES GCM stream of TestBytes(100, 5) written by Java's AesGcmOutputStream
// with kDataKey and kAadPrefix.
const std::vector<uint8_t> kJavaAgs1Hundred = {
    0x41, 0x47, 0x53, 0x31, 0x00, 0x00, 0x10, 0x00, 0x02, 0xb0, 0x1c, 0x1f, 0x7e, 0x37,
    0xeb, 0x85, 0xd3, 0x4b, 0x14, 0x75, 0x91, 0x83, 0xcf, 0x68, 0xde, 0xdd, 0xe0, 0xff,
    0xd3, 0xe0, 0xeb, 0x31, 0x10, 0x4b, 0x95, 0x86, 0x5c, 0x67, 0x1f, 0x68, 0xdf, 0x24,
    0xbd, 0x0a, 0x07, 0xf1, 0x06, 0xc7, 0x13, 0x1e, 0x42, 0xd4, 0x7a, 0xff, 0x87, 0x32,
    0xc3, 0xe5, 0xea, 0x75, 0x6a, 0x53, 0xaa, 0xab, 0xab, 0xe0, 0x81, 0x2c, 0x71, 0x4c,
    0xb4, 0x89, 0x32, 0x89, 0xc6, 0xbb, 0x84, 0x5d, 0x58, 0x93, 0x6b, 0x80, 0xbd, 0xa3,
    0xe6, 0xc3, 0xef, 0x53, 0x83, 0xa4, 0x00, 0x9c, 0x30, 0x02, 0xd4, 0xca, 0x7e, 0x0e,
    0xb3, 0xa3, 0xff, 0x4c, 0x96, 0xe3, 0x31, 0xa1, 0x6a, 0xc1, 0xf2, 0xb7, 0xa0, 0x40,
    0xc4, 0x0b, 0x5c, 0x45, 0x09, 0xb7, 0x35, 0xda, 0x6c, 0x03, 0xce, 0xd5, 0x59, 0xd0,
    0x47, 0xf9, 0x0c, 0x9d, 0x1f, 0x89, 0x2f, 0x1b, 0xcb, 0x44};

Result<std::vector<uint8_t>> ReadAll(InputFile& file) {
  ICEBERG_ASSIGN_OR_RAISE(auto size, file.Size());
  ICEBERG_ASSIGN_OR_RAISE(auto stream, file.Open());
  std::vector<uint8_t> out(size);
  if (size > 0) {
    ICEBERG_RETURN_UNEXPECTED(
        stream->ReadFully(0, std::as_writable_bytes(std::span(out))));
  }
  return out;
}

}  // namespace

TEST(AesGcmCipherTest, RoundTrip) {
  for (size_t key_length : {16, 24, 32}) {
    ICEBERG_UNWRAP_OR_FAIL(auto cipher, AesGcmCipher::Make(TestBytes(key_length, 1)));
    auto plaintext = TestBytes(100, 2);
    ICEBERG_UNWRAP_OR_FAIL(auto ciphertext, cipher->Encrypt(plaintext, AsBytes("aad")));
    EXPECT_EQ(ciphertext.size(), plaintext.size() + 28);
    ICEBERG_UNWRAP_OR_FAIL(auto decrypted, cipher->Decrypt(ciphertext, AsBytes("aad")));
    EXPECT_EQ(decrypted, plaintext);

    // Random nonces: encrypting twice gives different ciphertexts
    ICEBERG_UNWRAP_OR_FAIL(auto again, cipher->Encrypt(plaintext, AsBytes("aad")));
    EXPECT_NE(again, ciphertext);

    // Wrong AAD, tampered ciphertext and truncated ciphertext fail authentication
    EXPECT_THAT(cipher->Decrypt(ciphertext, AsBytes("other")),
                IsError(ErrorKind::kInvalid));
    ciphertext[20] ^= 1;
    EXPECT_THAT(cipher->Decrypt(ciphertext, AsBytes("aad")),
                IsError(ErrorKind::kInvalid));
    EXPECT_THAT(cipher->Decrypt(std::span(ciphertext).first(27), AsBytes("aad")),
                IsError(ErrorKind::kInvalid));
  }
  EXPECT_THAT(AesGcmCipher::Make(TestBytes(15, 1)), IsError(ErrorKind::kInvalidArgument));
}

TEST(AesGcmCipherTest, DecryptsJavaManifestListKeyMetadata) {
  ICEBERG_UNWRAP_OR_FAIL(auto cipher, AesGcmCipher::Make(kKek));
  ICEBERG_UNWRAP_OR_FAIL(auto decrypted, cipher->Decrypt(kJavaManifestListKeyMetadata,
                                                         AsBytes(kKekTimestamp)));
  ICEBERG_UNWRAP_OR_FAIL(auto metadata, StandardKeyMetadata::Parse(decrypted));
  EXPECT_EQ(metadata.encryption_key, TestBytes(16, 1));
  EXPECT_EQ(metadata.aad_prefix, TestBytes(16, 2));
  EXPECT_EQ(metadata.file_length, 4242);
}

class AesGcmStreamTest : public TempFileTestBase {
 protected:
  void SetUp() override {
    TempFileTestBase::SetUp();
    io_ = std::shared_ptr<FileIO>(arrow::MakeLocalFileIO());
  }

  // Write `plain` as an AGS1 stream and return the stored length.
  int64_t Write(const std::string& path, std::span<const uint8_t> plain,
                const std::vector<uint8_t>& key, const std::vector<uint8_t>& aad,
                size_t chunk = 1 << 20) {
    auto out = io_->NewOutputFile(path);
    EXPECT_THAT(out, IsOk());
    AesGcmOutputFile file(std::move(out.value()), key, aad);
    auto stream = file.CreateOrOverwrite();
    EXPECT_THAT(stream, IsOk());
    for (size_t pos = 0; pos < plain.size(); pos += chunk) {
      auto part = plain.subspan(pos, std::min(chunk, plain.size() - pos));
      EXPECT_THAT((*stream)->Write(std::as_bytes(part)), IsOk());
    }
    EXPECT_EQ((*stream)->Position().value(), static_cast<int64_t>(plain.size()));
    EXPECT_THAT((*stream)->Close(), IsOk());
    // Java's AesGcmOutputStream keeps reporting the plaintext length once closed
    EXPECT_EQ((*stream)->Position().value(), static_cast<int64_t>(plain.size()));
    auto stored = (*stream)->StoredLength().value();
    EXPECT_EQ(stored, static_cast<int64_t>(std::filesystem::file_size(path)));
    return stored;
  }

  std::unique_ptr<AesGcmInputFile> Open(const std::string& path,
                                        const std::vector<uint8_t>& key,
                                        const std::vector<uint8_t>& aad, int64_t length) {
    auto in = io_->NewInputFile(path);
    EXPECT_THAT(in, IsOk());
    return std::make_unique<AesGcmInputFile>(std::move(in.value()), key, aad, length);
  }

  // Materialize an inlined Java fixture as a file the reader can open.
  std::string WriteRaw(const std::vector<uint8_t>& bytes) {
    auto path = CreateNewTempFilePath();
    std::ofstream out(path, std::ios::binary);
    out.write(reinterpret_cast<const char*>(bytes.data()),
              static_cast<std::streamsize>(bytes.size()));
    out.close();
    return path;
  }

  std::shared_ptr<FileIO> io_;
};

TEST_F(AesGcmStreamTest, ReadsJavaVectors) {
  // Streams of TestBytes(0, 5), TestBytes(1, 5) and TestBytes(100, 5) written by Java
  const std::vector<std::pair<size_t, const std::vector<uint8_t>*>> vectors = {
      {0, &kJavaAgs1Empty}, {1, &kJavaAgs1OneByte}, {100, &kJavaAgs1Hundred}};
  for (const auto& [length, encrypted] : vectors) {
    auto path = WriteRaw(*encrypted);
    auto file = Open(path, kDataKey, kAadPrefix, static_cast<int64_t>(encrypted->size()));
    ICEBERG_UNWRAP_OR_FAIL(auto plain, ReadAll(*file));
    EXPECT_EQ(plain, TestBytes(length, 5)) << "plaintext length " << length;
  }
}

TEST_F(AesGcmStreamTest, WritesJavaCompatibleLayout) {
  // Same plaintext, key and AAD prefix as Java's 100-byte vector: same header and
  // length (the ciphertext differs only by the random nonce).
  auto path = CreateNewTempFilePath();
  auto stored = Write(path, TestBytes(100, 5), kDataKey, kAadPrefix);
  ASSERT_EQ(stored, static_cast<int64_t>(kJavaAgs1Hundred.size()));
  std::ifstream in(path, std::ios::binary);
  std::vector<uint8_t> header(8);
  in.read(reinterpret_cast<char*>(header.data()), 8);
  EXPECT_EQ(header,
            std::vector<uint8_t>(kJavaAgs1Hundred.begin(), kJavaAgs1Hundred.begin() + 8));
}

TEST_F(AesGcmStreamTest, RoundTripAcrossBlocks) {
  auto key = TestBytes(32, 7);
  auto aad = TestBytes(16, 9);
  constexpr size_t kBlock = AesGcmStream::kPlainBlockSize;
  for (size_t size :
       {size_t{0}, size_t{1}, kBlock - 1, kBlock, kBlock + 1, 2 * kBlock + 17}) {
    auto path = CreateNewTempFilePath();
    auto plain = TestBytes(size, 5);
    // Odd write sizes exercise writes that straddle blocks
    int64_t stored = Write(path, plain, key, aad, /*chunk=*/333331);
    size_t blocks = size == 0 ? 1 : (size + kBlock - 1) / kBlock;
    EXPECT_EQ(stored, static_cast<int64_t>(8 + size + blocks * 28));
    ICEBERG_UNWRAP_OR_FAIL(auto plain_size, AesGcmStream::PlaintextLength(stored));
    EXPECT_EQ(plain_size, static_cast<int64_t>(size));

    auto file = Open(path, key, aad, stored);
    ICEBERG_UNWRAP_OR_FAIL(auto read, ReadAll(*file));
    EXPECT_EQ(read, plain);

    if (size > kBlock + 10) {
      // Random access across a block boundary
      ICEBERG_UNWRAP_OR_FAIL(auto stream, file->Open());
      std::vector<std::byte> window(10);
      ASSERT_THAT(stream->ReadFully(kBlock - 5, window), IsOk());
      EXPECT_EQ(std::memcmp(window.data(), plain.data() + kBlock - 5, 10), 0);
    }
  }
}

TEST_F(AesGcmStreamTest, DetectsTampering) {
  auto key = TestBytes(16, 7);
  auto aad = TestBytes(16, 9);
  auto path = CreateNewTempFilePath();
  auto plain = TestBytes(AesGcmStream::kPlainBlockSize + 100, 5);
  int64_t stored = Write(path, plain, key, aad);

  // Wrong key or AAD prefix (a file swapped in from another manifest entry)
  EXPECT_THAT(ReadAll(*Open(path, TestBytes(16, 8), aad, stored)),
              IsError(ErrorKind::kInvalid));
  EXPECT_THAT(ReadAll(*Open(path, key, TestBytes(16, 10), stored)),
              IsError(ErrorKind::kInvalid));

  // Truncation: the trusted length from the key metadata no longer matches the file
  std::filesystem::resize_file(path, 8 + AesGcmStream::kCipherBlockSize);
  EXPECT_THAT(ReadAll(*Open(path, key, aad, stored)), IsError(ErrorKind::kIOError));
  // AGS1 blocks do not mark the end of the stream: a truncated file read with its own
  // (untrusted) length still decrypts. This is why readers must take the length from
  // the key metadata.
  auto truncated = Open(path, key, aad, 8 + AesGcmStream::kCipherBlockSize);
  ICEBERG_UNWRAP_OR_FAIL(auto first_block, ReadAll(*truncated));
  EXPECT_EQ(first_block.size(), AesGcmStream::kPlainBlockSize);

  // Bad magic
  std::filesystem::resize_file(path, 0);
  std::ofstream(path, std::ios::binary) << "XXXX0000" << std::string(28, '\0');
  EXPECT_THAT(Open(path, key, aad, 36)->Open(), IsError(ErrorKind::kInvalid));
  // Shorter than an empty stream
  EXPECT_THAT(Open(path, key, aad, 35)->Open(), IsError(ErrorKind::kInvalid));
}

TEST_F(AesGcmStreamTest, AuthenticatesEmptyStream) {
  auto key = TestBytes(16, 7);
  auto aad = TestBytes(16, 9);
  auto path = CreateNewTempFilePath();
  int64_t stored = Write(path, {}, key, aad);
  ASSERT_EQ(stored, AesGcmStream::kMinStreamLength);

  // An empty stream still carries one block with a nonce and a tag, so opening it
  // authenticates that block even though there is no plaintext to read.
  ICEBERG_UNWRAP_OR_FAIL(auto plain, ReadAll(*Open(path, key, aad, stored)));
  EXPECT_TRUE(plain.empty());

  // Wrong key or AAD prefix (a file swapped in from another manifest entry)
  EXPECT_THAT(Open(path, TestBytes(16, 8), aad, stored)->Open(),
              IsError(ErrorKind::kInvalid));
  EXPECT_THAT(Open(path, key, TestBytes(16, 10), stored)->Open(),
              IsError(ErrorKind::kInvalid));

  // Truncated to its header behind a trusted length that says otherwise
  std::filesystem::resize_file(path, AesGcmStream::kHeaderLength);
  EXPECT_THAT(Open(path, key, aad, stored)->Open(), IsError(ErrorKind::kIOError));

  // A corrupt tag
  auto corrupt = WriteRaw(kJavaAgs1Empty);
  {
    std::fstream out(corrupt, std::ios::binary | std::ios::in | std::ios::out);
    out.seekp(static_cast<std::streamoff>(kJavaAgs1Empty.size() - 1));
    out.put(static_cast<char>(kJavaAgs1Empty.back() ^ 0xFF));
  }
  EXPECT_THAT(
      Open(corrupt, kDataKey, kAadPrefix, static_cast<int64_t>(kJavaAgs1Empty.size()))
          ->Open(),
      IsError(ErrorKind::kInvalid));
}

TEST_F(AesGcmStreamTest, RejectsOutOfRangeReadFully) {
  auto key = TestBytes(16, 7);
  auto aad = TestBytes(16, 9);
  auto path = CreateNewTempFilePath();
  int64_t stored = Write(path, TestBytes(100, 5), key, aad);
  ICEBERG_UNWRAP_OR_FAIL(auto stream, Open(path, key, aad, stored)->Open());

  // A position near int64 max must be rejected without overflowing the range check
  std::vector<std::byte> one(1);
  EXPECT_THAT(stream->ReadFully(std::numeric_limits<int64_t>::max(), one),
              IsError(ErrorKind::kIOError));
  EXPECT_THAT(stream->ReadFully(-1, one), IsError(ErrorKind::kIOError));

  // Reading up to the last byte is allowed; one byte past it is not
  EXPECT_THAT(stream->ReadFully(99, one), IsOk());
  EXPECT_THAT(stream->ReadFully(100, one), IsError(ErrorKind::kIOError));
  EXPECT_THAT(stream->ReadFully(100, std::span<std::byte>{}), IsOk());
}

TEST_F(AesGcmStreamTest, RejectsInvalidEncryptedLength) {
  constexpr int64_t kHeader = AesGcmStream::kHeaderLength;
  constexpr int64_t kCipherBlock = AesGcmStream::kCipherBlockSize;
  constexpr int64_t kOverhead = AesGcmStream::kNonceLength + AesGcmStream::kGcmTagLength;

  // Shorter than an empty stream
  EXPECT_THAT(AesGcmStream::PlaintextLength(-1), IsError(ErrorKind::kInvalid));
  EXPECT_THAT(AesGcmStream::PlaintextLength(0), IsError(ErrorKind::kInvalid));
  EXPECT_THAT(AesGcmStream::PlaintextLength(AesGcmStream::kMinStreamLength - 1),
              IsError(ErrorKind::kInvalid));
  EXPECT_THAT(AesGcmStream::PlaintextLength(AesGcmStream::kMinStreamLength),
              HasValue(::testing::Eq(0)));

  // A last block too short to hold a nonce and a tag
  EXPECT_THAT(AesGcmStream::PlaintextLength(kHeader + kCipherBlock + kOverhead - 1),
              IsError(ErrorKind::kInvalid));
  EXPECT_THAT(AesGcmStream::PlaintextLength(kHeader + kCipherBlock + kOverhead),
              HasValue(::testing::Eq(AesGcmStream::kPlainBlockSize)));

  // More blocks than the int32 block index in the block AAD can address. The check
  // must happen here rather than when a block is decrypted, so that a caller asking
  // only for the size never sees a length it could not read back.
  constexpr int64_t kMaxBlocks = std::numeric_limits<int32_t>::max();
  EXPECT_THAT(AesGcmStream::PlaintextLength(kHeader + kMaxBlocks * kCipherBlock),
              HasValue(::testing::Eq(kMaxBlocks * AesGcmStream::kPlainBlockSize)));
  EXPECT_THAT(AesGcmStream::PlaintextLength(kHeader + kMaxBlocks * kCipherBlock + 1),
              IsError(ErrorKind::kInvalid));

  // Both Size() and Open() reject it, rather than Open() alone
  auto path = CreateNewTempFilePath();
  Write(path, TestBytes(10, 5), kDataKey, kAadPrefix);
  auto file = Open(path, kDataKey, kAadPrefix, kHeader + kMaxBlocks * kCipherBlock + 1);
  EXPECT_THAT(file->Size(), IsError(ErrorKind::kInvalid));
  EXPECT_THAT(file->Open(), IsError(ErrorKind::kInvalid));
}

}  // namespace iceberg
