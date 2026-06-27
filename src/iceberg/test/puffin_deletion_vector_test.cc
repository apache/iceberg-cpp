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

#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/deletes/roaring_position_bitmap.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/puffin/deletion_vector.h"
#include "iceberg/puffin/file_metadata.h"
#include "iceberg/puffin/puffin_reader.h"
#include "iceberg/puffin/puffin_writer.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_io.h"

namespace iceberg::puffin {

namespace {

RoaringPositionBitmap MakeBitmap(const std::vector<int64_t>& positions) {
  RoaringPositionBitmap bitmap;
  for (int64_t pos : positions) {
    bitmap.Add(pos);
  }
  return bitmap;
}

void ExpectContainsExactly(const RoaringPositionBitmap& bitmap,
                           const std::vector<int64_t>& positions) {
  EXPECT_EQ(bitmap.Cardinality(), positions.size());
  for (int64_t pos : positions) {
    EXPECT_TRUE(bitmap.Contains(pos)) << "missing position " << pos;
  }
}

}  // namespace

// ==================== Blob framing (length + magic + vector + CRC) ====================

TEST(DeletionVectorBlobTest, RoundTrip) {
  const std::vector<int64_t> positions = {0, 1, 5, 100, 4'000'000'000LL};
  auto bitmap = MakeBitmap(positions);

  ICEBERG_UNWRAP_OR_FAIL(auto blob, SerializeDeletionVectorBlob(bitmap));
  ICEBERG_UNWRAP_OR_FAIL(auto restored, DeserializeDeletionVectorBlob(blob));

  ExpectContainsExactly(restored, positions);
}

// Mirrors Java's TestBitmapPositionDeleteIndex#testAllContainerTypesIndexSerialization:
// spans two high-32-bit keys and exercises all Roaring container types (sparse
// "array", dense "bitset", and run containers after Optimize).
TEST(DeletionVectorBlobTest, RoundTripAllContainerTypesAcrossKeys) {
  constexpr int64_t kKeyStride = 0x100000000LL;  // 2^32: high-32-bit key
  constexpr int64_t kContainerStride = 1 << 16;  // 2^16: Roaring container
  auto pos = [](int64_t key, int64_t container, int64_t value) {
    return key * kKeyStride + container * kContainerStride + value;
  };

  RoaringPositionBitmap bitmap;
  int64_t expected = 0;
  auto add = [&](int64_t p) {
    bitmap.Add(p);
    ++expected;
  };
  auto add_range = [&](int64_t begin, int64_t end) {
    bitmap.AddRange(begin, end);
    expected += end - begin;
  };

  for (int64_t key : {int64_t{0}, int64_t{1}}) {
    add(pos(key, 0, 5));  // sparse -> array
    add(pos(key, 0, 7));
    add_range(pos(key, 1, 1), pos(key, 1, 1000));              // medium run
    add_range(pos(key, 2, 1), pos(key, 2, kContainerStride));  // dense -> bitset
  }

  bitmap.Optimize();  // run-length encode, as the DV writer does

  ICEBERG_UNWRAP_OR_FAIL(auto blob, SerializeDeletionVectorBlob(bitmap));
  ICEBERG_UNWRAP_OR_FAIL(auto restored, DeserializeDeletionVectorBlob(blob));

  EXPECT_EQ(restored.Cardinality(), static_cast<size_t>(expected));
  EXPECT_TRUE(restored.Contains(pos(0, 0, 5)));
  EXPECT_TRUE(restored.Contains(pos(1, 2, kContainerStride - 1)));
  EXPECT_TRUE(restored.Contains(pos(0, 1, 999)));
  EXPECT_FALSE(restored.Contains(pos(0, 0, 6)));
  EXPECT_FALSE(restored.Contains(pos(1, 1, 1000)));  // range end is exclusive
}

TEST(DeletionVectorBlobTest, RoundTripEmpty) {
  auto bitmap = MakeBitmap({});

  ICEBERG_UNWRAP_OR_FAIL(auto blob, SerializeDeletionVectorBlob(bitmap));
  ICEBERG_UNWRAP_OR_FAIL(auto restored, DeserializeDeletionVectorBlob(blob));

  EXPECT_TRUE(restored.IsEmpty());
}

TEST(DeletionVectorBlobTest, BlobLayout) {
  auto bitmap = MakeBitmap({1, 2, 3});
  ICEBERG_UNWRAP_OR_FAIL(auto blob, SerializeDeletionVectorBlob(bitmap));

  // length(4) + magic(4) + vector + crc(4)
  ASSERT_GE(blob.size(), 12u);

  // Magic sequence: 0xD1 0xD3 0x39 0x64 immediately follows the length prefix.
  EXPECT_EQ(blob[4], 0xD1);
  EXPECT_EQ(blob[5], 0xD3);
  EXPECT_EQ(blob[6], 0x39);
  EXPECT_EQ(blob[7], 0x64);

  // Length prefix (big-endian) equals magic + vector size.
  const uint32_t length =
      (static_cast<uint32_t>(blob[0]) << 24) | (static_cast<uint32_t>(blob[1]) << 16) |
      (static_cast<uint32_t>(blob[2]) << 8) | static_cast<uint32_t>(blob[3]);
  EXPECT_EQ(length, blob.size() - 8u);
}

TEST(DeletionVectorBlobTest, RejectsCorruptedCrc) {
  auto bitmap = MakeBitmap({1, 2, 3});
  ICEBERG_UNWRAP_OR_FAIL(auto blob, SerializeDeletionVectorBlob(bitmap));

  blob.back() ^= 0xFF;  // flip a bit in the trailing CRC
  EXPECT_THAT(DeserializeDeletionVectorBlob(blob), IsError(ErrorKind::kInvalidArgument));
}

TEST(DeletionVectorBlobTest, RejectsBadMagic) {
  auto bitmap = MakeBitmap({1, 2, 3});
  ICEBERG_UNWRAP_OR_FAIL(auto blob, SerializeDeletionVectorBlob(bitmap));

  blob[4] = 0x00;
  EXPECT_THAT(DeserializeDeletionVectorBlob(blob), IsError(ErrorKind::kInvalidArgument));
}

TEST(DeletionVectorBlobTest, RejectsTruncatedBlob) {
  std::vector<uint8_t> blob = {0x00, 0x00};
  EXPECT_THAT(DeserializeDeletionVectorBlob(blob), IsError(ErrorKind::kInvalidArgument));
}

TEST(DeletionVectorBlobTest, MakeBlobSetsSpecFields) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto blob, MakeDeletionVectorBlob(MakeBitmap({1, 2, 3}), "/data/file-a.parquet"));

  EXPECT_EQ(blob.type, StandardBlobTypes::kDeletionVectorV1);
  EXPECT_EQ(blob.input_fields,
            std::vector<int32_t>{MetadataColumns::kFilePositionColumnId});
  EXPECT_EQ(blob.snapshot_id, -1);
  EXPECT_EQ(blob.sequence_number, -1);
  EXPECT_EQ(blob.requested_compression, PuffinCompressionCodec::kNone);
  EXPECT_EQ(blob.properties.at(
                std::string(StandardDeletionVectorProperties::kReferencedDataFile)),
            "/data/file-a.parquet");
  EXPECT_EQ(
      blob.properties.at(std::string(StandardDeletionVectorProperties::kCardinality)),
      "3");
}

TEST(DeletionVectorBlobTest, MakeBlobRejectsEmptyReferencedFile) {
  EXPECT_THAT(MakeDeletionVectorBlob(MakeBitmap({1}), ""),
              IsError(ErrorKind::kInvalidArgument));
}

// ==================== End-to-end through a Puffin file ====================

TEST(DeletionVectorBlobTest, EndToEndThroughPuffinFile) {
  const std::vector<int64_t> positions = {0, 3, 7, 1'000, 5'000'000'000LL};
  MockFileIO io;
  const std::string location = "memory://dv.puffin";

  {
    ICEBERG_UNWRAP_OR_FAIL(auto output, io.NewOutputFile(location));
    ICEBERG_UNWRAP_OR_FAIL(auto writer, PuffinWriter::Make(std::move(output)));
    ICEBERG_UNWRAP_OR_FAIL(
        auto blob, MakeDeletionVectorBlob(MakeBitmap(positions), "/data/file-a.parquet"));
    ICEBERG_UNWRAP_OR_FAIL(auto meta, writer->Write(blob));
    EXPECT_EQ(meta.type, StandardBlobTypes::kDeletionVectorV1);
    EXPECT_TRUE(meta.compression_codec.empty());
    ASSERT_THAT(writer->Finish(), IsOk());
  }

  ICEBERG_UNWRAP_OR_FAIL(auto input, io.NewInputFile(location));
  ICEBERG_UNWRAP_OR_FAIL(auto reader, PuffinReader::Make(std::move(input)));
  ICEBERG_UNWRAP_OR_FAIL(auto file_metadata, reader->ReadFileMetadata());
  ASSERT_EQ(file_metadata.blobs.size(), 1u);
  EXPECT_EQ(file_metadata.blobs[0].properties.at(
                std::string(StandardDeletionVectorProperties::kReferencedDataFile)),
            "/data/file-a.parquet");

  ICEBERG_UNWRAP_OR_FAIL(auto blob_result, reader->ReadBlob(file_metadata.blobs[0]));
  const auto& bytes = blob_result.second;
  std::span<const uint8_t> blob_bytes(reinterpret_cast<const uint8_t*>(bytes.data()),
                                      bytes.size());
  ICEBERG_UNWRAP_OR_FAIL(auto restored, DeserializeDeletionVectorBlob(blob_bytes));

  ExpectContainsExactly(restored, positions);
}

}  // namespace iceberg::puffin
