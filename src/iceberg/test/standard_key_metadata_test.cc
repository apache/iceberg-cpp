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

#include <gtest/gtest.h>

#include "iceberg/test/encryption_test_util.h"
#include "iceberg/test/matchers.h"

namespace iceberg {

TEST(StandardKeyMetadataTest, ParseJavaVectors) {
  auto key = ReadEncryptionVector("key.bin");
  auto aad_prefix = ReadEncryptionVector("aad_prefix.bin");
  ASSERT_EQ(key, VectorBytes(16, 1));
  ASSERT_EQ(aad_prefix, VectorBytes(16, 2));

  auto with_length = ReadEncryptionVector("key_metadata_with_length.bin");
  ICEBERG_UNWRAP_OR_FAIL(auto parsed, StandardKeyMetadata::Parse(with_length));
  EXPECT_EQ(parsed.encryption_key, key);
  EXPECT_EQ(parsed.aad_prefix, aad_prefix);
  EXPECT_EQ(parsed.file_length, 1234567);
  // Serialization is byte-for-byte identical to Java
  EXPECT_EQ(parsed.Serialize(), with_length);

  auto without_length = ReadEncryptionVector("key_metadata.bin");
  ICEBERG_UNWRAP_OR_FAIL(auto parsed2, StandardKeyMetadata::Parse(without_length));
  EXPECT_EQ(parsed2.encryption_key, key);
  EXPECT_FALSE(parsed2.file_length.has_value());
  EXPECT_EQ(parsed2.Serialize(), without_length);
  EXPECT_EQ(parsed2.WithFileLength(1234567).Serialize(), with_length);
}

TEST(StandardKeyMetadataTest, RoundTrip) {
  StandardKeyMetadata metadata{.encryption_key = VectorBytes(32, 7)};
  ICEBERG_UNWRAP_OR_FAIL(auto parsed, StandardKeyMetadata::Parse(metadata.Serialize()));
  EXPECT_EQ(parsed, metadata);

  // Large and negative-looking lengths survive the zig-zag varint encoding
  for (int64_t length : {int64_t{0}, int64_t{63}, int64_t{64}, int64_t{1} << 40}) {
    auto with_length = metadata.WithFileLength(length);
    ICEBERG_UNWRAP_OR_FAIL(auto round_trip,
                           StandardKeyMetadata::Parse(with_length.Serialize()));
    EXPECT_EQ(round_trip, with_length);
  }
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
}

}  // namespace iceberg
