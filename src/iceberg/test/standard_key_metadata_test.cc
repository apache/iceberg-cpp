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

#include "iceberg/encryption/standard_key_metadata.h"

#include <limits>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/test/matchers.h"

namespace iceberg {

namespace {

/// \brief Deterministic test bytes: byte i is (i * 31 + seed).
std::vector<uint8_t> TestBytes(size_t length, int seed) {
  std::vector<uint8_t> bytes(length);
  for (size_t i = 0; i < length; ++i) {
    bytes[i] = static_cast<uint8_t>(static_cast<int>(i) * 31 + seed);
  }
  return bytes;
}

// Serialized by Iceberg Java's StandardKeyMetadata, with encryption key
// TestBytes(16, 1), AAD prefix TestBytes(16, 2) and no file length.
const std::vector<uint8_t> kJavaKeyMetadata = {
    0x01, 0x20, 0x01, 0x20, 0x3f, 0x5e, 0x7d, 0x9c, 0xbb, 0xda, 0xf9, 0x18, 0x37,
    0x56, 0x75, 0x94, 0xb3, 0xd2, 0x02, 0x20, 0x02, 0x21, 0x40, 0x5f, 0x7e, 0x9d,
    0xbc, 0xdb, 0xfa, 0x19, 0x38, 0x57, 0x76, 0x95, 0xb4, 0xd3, 0x00};

// The same, with file length 1234567.
const std::vector<uint8_t> kJavaKeyMetadataWithLength = {
    0x01, 0x20, 0x01, 0x20, 0x3f, 0x5e, 0x7d, 0x9c, 0xbb, 0xda, 0xf9, 0x18, 0x37, 0x56,
    0x75, 0x94, 0xb3, 0xd2, 0x02, 0x20, 0x02, 0x21, 0x40, 0x5f, 0x7e, 0x9d, 0xbc, 0xdb,
    0xfa, 0x19, 0x38, 0x57, 0x76, 0x95, 0xb4, 0xd3, 0x02, 0x8e, 0xda, 0x96, 0x01};

}  // namespace

TEST(StandardKeyMetadataTest, ParseJavaVectors) {
  auto key = TestBytes(16, 1);
  auto aad_prefix = TestBytes(16, 2);

  const auto& with_length = kJavaKeyMetadataWithLength;
  ICEBERG_UNWRAP_OR_FAIL(auto parsed, StandardKeyMetadata::Parse(with_length));
  EXPECT_EQ(parsed.encryption_key, key);
  EXPECT_EQ(parsed.aad_prefix, aad_prefix);
  EXPECT_EQ(parsed.file_length, 1234567);
  // Serialization is byte-for-byte identical to Java
  EXPECT_EQ(parsed.Serialize(), with_length);

  const auto& without_length = kJavaKeyMetadata;
  ICEBERG_UNWRAP_OR_FAIL(auto parsed2, StandardKeyMetadata::Parse(without_length));
  EXPECT_EQ(parsed2.encryption_key, key);
  EXPECT_FALSE(parsed2.file_length.has_value());
  EXPECT_EQ(parsed2.Serialize(), without_length);
  EXPECT_EQ(parsed2.WithFileLength(1234567).Serialize(), with_length);
}

TEST(StandardKeyMetadataTest, RoundTrip) {
  StandardKeyMetadata metadata{.encryption_key = TestBytes(32, 7)};
  ICEBERG_UNWRAP_OR_FAIL(auto parsed, StandardKeyMetadata::Parse(metadata.Serialize()));
  EXPECT_EQ(parsed, metadata);

  // Zig-zag varint boundaries, including negative values and ten-byte encodings
  for (int64_t length :
       {int64_t{0}, int64_t{63}, int64_t{64}, int64_t{1} << 40, int64_t{-1},
        std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max()}) {
    auto with_length = metadata.WithFileLength(length);
    ICEBERG_UNWRAP_OR_FAIL(auto round_trip,
                           StandardKeyMetadata::Parse(with_length.Serialize()));
    EXPECT_EQ(round_trip, with_length);
  }

  // A present but empty AAD prefix is distinct from an absent one
  StandardKeyMetadata empty_aad{.encryption_key = TestBytes(16, 7),
                                .aad_prefix = std::vector<uint8_t>{}};
  ICEBERG_UNWRAP_OR_FAIL(auto parsed_empty,
                         StandardKeyMetadata::Parse(empty_aad.Serialize()));
  ASSERT_TRUE(parsed_empty.aad_prefix.has_value());
  EXPECT_TRUE(parsed_empty.aad_prefix->empty());
  ICEBERG_UNWRAP_OR_FAIL(auto parsed_null,
                         StandardKeyMetadata::Parse(metadata.Serialize()));
  EXPECT_FALSE(parsed_null.aad_prefix.has_value());
}

TEST(StandardKeyMetadataTest, InvalidInput) {
  EXPECT_THAT(StandardKeyMetadata::Parse(std::vector<uint8_t>{}),
              IsError(ErrorKind::kInvalid));
  // Unknown schema version
  EXPECT_THAT(StandardKeyMetadata::Parse(std::vector<uint8_t>{2, 0, 0, 0}),
              IsError(ErrorKind::kNotSupported));
  // Key length beyond the buffer
  EXPECT_THAT(StandardKeyMetadata::Parse(std::vector<uint8_t>{1, 0x20, 1, 2}),
              IsError(ErrorKind::kInvalid));
  // Invalid union branch
  EXPECT_THAT(StandardKeyMetadata::Parse(std::vector<uint8_t>{1, 0x02, 9, 0x04}),
              IsError(ErrorKind::kInvalid));
  // Truncated optional fields
  EXPECT_THAT(StandardKeyMetadata::Parse(std::vector<uint8_t>{1, 0x02, 9}),
              IsError(ErrorKind::kInvalid));
  // Negative key length (zig-zag 0x01 is -1)
  EXPECT_THAT(StandardKeyMetadata::Parse(std::vector<uint8_t>{1, 0x01, 9}),
              IsError(ErrorKind::kInvalid));
  // Truncated varint
  EXPECT_THAT(StandardKeyMetadata::Parse(std::vector<uint8_t>{1, 0x80}),
              IsError(ErrorKind::kInvalid));
  // Varint longer than ten bytes, rejected as by Java's Avro decoder
  std::vector<uint8_t> long_varint{1};
  long_varint.insert(long_varint.end(), 10, 0x80);
  long_varint.push_back(0x01);
  EXPECT_THAT(StandardKeyMetadata::Parse(long_varint), IsError(ErrorKind::kInvalid));
}

}  // namespace iceberg
