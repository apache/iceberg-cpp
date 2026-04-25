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

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/puffin/file_metadata.h"
#include "iceberg/puffin/puffin_reader.h"
#include "iceberg/puffin/puffin_writer.h"
#include "iceberg/test/matchers.h"

namespace iceberg::puffin {

namespace {

std::vector<std::byte> ToBytes(std::initializer_list<uint8_t> values) {
  std::vector<std::byte> result;
  result.reserve(values.size());
  for (auto v : values) {
    result.push_back(static_cast<std::byte>(v));
  }
  return result;
}

std::vector<std::byte> ToBytes(std::string_view str) {
  return {reinterpret_cast<const std::byte*>(str.data()),
          reinterpret_cast<const std::byte*>(str.data() + str.size())};
}

}  // namespace

// ============================================================================
// PuffinWriter Tests
// ============================================================================

TEST(PuffinWriterTest, WriteEmptyFile) {
  PuffinWriter writer;
  auto result = writer.Finish();
  ASSERT_THAT(result, IsOk());
  auto& data = result.value();

  // Header magic (4) + footer start magic (4) + JSON payload + footer struct (12)
  EXPECT_GE(data.size(), 20u);
  // Header magic
  EXPECT_EQ(data[0], std::byte{0x50});
  EXPECT_EQ(data[1], std::byte{0x46});
  EXPECT_EQ(data[2], std::byte{0x41});
  EXPECT_EQ(data[3], std::byte{0x31});
  // Footer end magic
  auto sz = data.size();
  EXPECT_EQ(data[sz - 4], std::byte{0x50});
  EXPECT_EQ(data[sz - 3], std::byte{0x46});
  EXPECT_EQ(data[sz - 2], std::byte{0x41});
  EXPECT_EQ(data[sz - 1], std::byte{0x31});

  EXPECT_TRUE(writer.written_blobs_metadata().empty());
  ASSERT_TRUE(writer.footer_size().has_value());
}

TEST(PuffinWriterTest, WriterRejectsAfterFinish) {
  PuffinWriter writer;
  ASSERT_THAT(writer.Finish(), IsOk());

  // Double finish
  EXPECT_THAT(writer.Finish(), IsError(ErrorKind::kInvalid));

  // Add after finish
  Blob blob{.type = "a", .snapshot_id = 1, .sequence_number = 0};
  EXPECT_THAT(writer.Add(blob), IsError(ErrorKind::kInvalid));
}

TEST(PuffinWriterTest, WriteEmptyBlobData) {
  PuffinWriter writer;
  Blob blob{
      .type = "empty-blob",
      .input_fields = {1},
      .snapshot_id = 1,
      .sequence_number = 0,
      .data = {},
  };
  auto meta = writer.Add(blob);
  ASSERT_THAT(meta, IsOk());
  EXPECT_EQ(meta.value().offset, 4);
  EXPECT_EQ(meta.value().length, 0);

  auto result = writer.Finish();
  ASSERT_THAT(result, IsOk());

  PuffinReader reader(result.value());
  auto fm = reader.ReadFileMetadata();
  ASSERT_THAT(fm, IsOk());
  ASSERT_EQ(fm.value().blobs.size(), 1);

  auto blob_result = reader.ReadBlob(fm.value().blobs[0]);
  ASSERT_THAT(blob_result, IsOk());
  EXPECT_TRUE(blob_result.value().second.empty());
}

TEST(PuffinWriterTest, WriteLargeBlob) {
  PuffinWriter writer;
  std::vector<uint8_t> large_data(4096);
  for (size_t i = 0; i < large_data.size(); ++i) {
    large_data[i] = static_cast<uint8_t>(i & 0xFF);
  }
  Blob blob{
      .type = "large-blob",
      .input_fields = {1, 2, 3},
      .snapshot_id = 999,
      .sequence_number = 42,
      .data = large_data,
  };
  auto meta = writer.Add(blob);
  ASSERT_THAT(meta, IsOk());
  EXPECT_EQ(meta.value().length, 4096);

  auto result = writer.Finish();
  ASSERT_THAT(result, IsOk());

  PuffinReader reader(result.value());
  auto fm = reader.ReadFileMetadata();
  ASSERT_THAT(fm, IsOk());
  ASSERT_EQ(fm.value().blobs.size(), 1);

  auto blob_result = reader.ReadBlob(fm.value().blobs[0]);
  ASSERT_THAT(blob_result, IsOk());
  auto& read_data = blob_result.value().second;
  ASSERT_EQ(read_data.size(), 4096);
  for (size_t i = 0; i < read_data.size(); ++i) {
    EXPECT_EQ(read_data[i], static_cast<std::byte>(i & 0xFF))
        << "mismatch at index " << i;
  }
}

// ============================================================================
// Round-Trip Tests
// ============================================================================

TEST(PuffinRoundTripTest, SingleBlob) {
  PuffinWriter writer;
  EXPECT_FALSE(writer.footer_size().has_value());

  std::vector<uint8_t> blob_data = {0x01, 0x02, 0x03, 0x04, 0x05};
  Blob blob{
      .type = "test-blob",
      .input_fields = {1, 2},
      .snapshot_id = 42,
      .sequence_number = 7,
      .data = blob_data,
  };
  auto meta = writer.Add(blob);
  ASSERT_THAT(meta, IsOk());
  EXPECT_EQ(meta.value().type, "test-blob");
  EXPECT_EQ(meta.value().offset, 4);  // after header magic
  EXPECT_EQ(meta.value().length, 5);
  EXPECT_EQ(writer.written_blobs_metadata().size(), 1);

  auto file_result = writer.Finish({{"created-by", "test"}});
  ASSERT_THAT(file_result, IsOk());
  ASSERT_TRUE(writer.footer_size().has_value());
  EXPECT_GT(writer.footer_size().value(), 0);

  PuffinReader reader(file_result.value());
  auto fm = reader.ReadFileMetadata();
  ASSERT_THAT(fm, IsOk());
  ASSERT_EQ(fm.value().blobs.size(), 1);
  EXPECT_EQ(fm.value().blobs[0].type, "test-blob");
  EXPECT_EQ(fm.value().properties.at("created-by"), "test");

  auto blob_result = reader.ReadBlob(fm.value().blobs[0]);
  ASSERT_THAT(blob_result, IsOk());
  EXPECT_EQ(blob_result.value().second, ToBytes({0x01, 0x02, 0x03, 0x04, 0x05}));
}

TEST(PuffinRoundTripTest, MultipleBlobs) {
  PuffinWriter writer;
  EXPECT_TRUE(writer.written_blobs_metadata().empty());

  // Add first blob (no properties)
  std::vector<uint8_t> data1 = {'a', 'b', 'c'};
  ASSERT_THAT(writer.Add(Blob{.type = "first",
                              .input_fields = {1},
                              .snapshot_id = 1,
                              .sequence_number = 0,
                              .data = data1}),
              IsOk());
  EXPECT_EQ(writer.written_blobs_metadata().size(), 1);

  // Add second blob (with properties)
  std::vector<uint8_t> data2 = {'d', 'e', 'f', 'g'};
  auto meta2 = writer.Add(Blob{.type = "second",
                               .input_fields = {2},
                               .snapshot_id = 2,
                               .sequence_number = 1,
                               .data = data2,
                               .properties = {{"key", "val"}}});
  ASSERT_THAT(meta2, IsOk());
  // Second blob starts after header (4) + first blob (3)
  EXPECT_EQ(meta2.value().offset, 7);
  EXPECT_EQ(meta2.value().length, 4);
  EXPECT_EQ(writer.written_blobs_metadata().size(), 2);

  EXPECT_FALSE(writer.footer_size().has_value());
  auto file_result = writer.Finish();
  ASSERT_THAT(file_result, IsOk());
  ASSERT_TRUE(writer.footer_size().has_value());
  EXPECT_GT(writer.footer_size().value(), 0);

  // Read back
  PuffinReader reader(file_result.value());
  auto fm = reader.ReadFileMetadata();
  ASSERT_THAT(fm, IsOk());
  ASSERT_EQ(fm.value().blobs.size(), 2);

  // Verify properties: first has none, second has one
  EXPECT_TRUE(fm.value().blobs[0].properties.empty());
  EXPECT_EQ(fm.value().blobs[1].properties.at("key"), "val");

  // ReadAll
  auto all = reader.ReadAll(fm.value().blobs);
  ASSERT_THAT(all, IsOk());
  ASSERT_EQ(all.value().size(), 2);
  EXPECT_EQ(all.value()[0].second, ToBytes("abc"));
  EXPECT_EQ(all.value()[1].second, ToBytes("defg"));
}

TEST(PuffinRoundTripTest, WithProperties) {
  PuffinWriter writer;
  std::string text = "hello puffin";
  std::vector<uint8_t> blob_data(text.begin(), text.end());
  ASSERT_THAT(writer.Add(Blob{.type = "text-blob",
                              .input_fields = {1},
                              .snapshot_id = 100,
                              .sequence_number = 5,
                              .data = blob_data,
                              .properties = {{"encoding", "utf-8"}}}),
              IsOk());
  auto file_result = writer.Finish({{"created-by", "iceberg-cpp-test"}});
  ASSERT_THAT(file_result, IsOk());

  PuffinReader reader(file_result.value());
  auto fm = reader.ReadFileMetadata();
  ASSERT_THAT(fm, IsOk());
  EXPECT_EQ(fm.value().properties.at("created-by"), "iceberg-cpp-test");
  ASSERT_EQ(fm.value().blobs.size(), 1);
  EXPECT_EQ(fm.value().blobs[0].type, "text-blob");
  EXPECT_EQ(fm.value().blobs[0].properties.at("encoding"), "utf-8");

  auto blob_result = reader.ReadBlob(fm.value().blobs[0]);
  ASSERT_THAT(blob_result, IsOk());
  EXPECT_EQ(blob_result.value().second, ToBytes("hello puffin"));
}

// ============================================================================
// PuffinReader Error Tests
// ============================================================================

TEST(PuffinReaderTest, ReadEmptyFile) {
  PuffinWriter writer;
  auto result = writer.Finish();
  ASSERT_THAT(result, IsOk());

  PuffinReader reader(result.value());
  auto fm = reader.ReadFileMetadata();
  ASSERT_THAT(fm, IsOk());
  EXPECT_TRUE(fm.value().blobs.empty());
  EXPECT_TRUE(fm.value().properties.empty());
}

TEST(PuffinReaderTest, InvalidMagic) {
  auto bad_data = ToBytes({0x00, 0x00, 0x00, 0x00});
  PuffinReader reader(bad_data);
  EXPECT_THAT(reader.ReadFileMetadata(), IsError(ErrorKind::kInvalid));
}

TEST(PuffinReaderTest, TruncatedFile) {
  auto tiny = ToBytes({0x50, 0x46});
  PuffinReader reader(tiny);
  EXPECT_THAT(reader.ReadFileMetadata(), IsError(ErrorKind::kInvalid));
}

TEST(PuffinReaderTest, InvalidBlobOffset) {
  PuffinWriter writer;
  auto file_result = writer.Finish();
  ASSERT_THAT(file_result, IsOk());

  PuffinReader reader(file_result.value());
  BlobMetadata bad_meta{
      .type = "bad",
      .snapshot_id = 1,
      .sequence_number = 0,
      .offset = 9999,
      .length = 100,
  };
  EXPECT_THAT(reader.ReadBlob(bad_meta), IsError(ErrorKind::kInvalid));
}

// ============================================================================
// Java Binary Compatibility Tests
// ============================================================================

TEST(PuffinReaderTest, JavaEmptyPuffinCompatibility) {
  // Exact binary content of v1/empty-puffin-uncompressed.bin from Java test resources
  auto java_empty = ToBytes({
      0x50, 0x46, 0x41, 0x31,  // header magic
      0x50, 0x46, 0x41, 0x31,  // footer start magic
      0x7b, 0x22, 0x62, 0x6c, 0x6f, 0x62,
      0x73, 0x22, 0x3a, 0x5b, 0x5d, 0x7d,  // {"blobs":[]}
      0x0c, 0x00, 0x00, 0x00,              // payload size = 12
      0x00, 0x00, 0x00, 0x00,              // flags = 0
      0x50, 0x46, 0x41, 0x31,              // footer end magic
  });

  PuffinReader reader(java_empty);
  auto fm = reader.ReadFileMetadata();
  ASSERT_THAT(fm, IsOk());
  EXPECT_TRUE(fm.value().blobs.empty());
  EXPECT_TRUE(fm.value().properties.empty());
}

// Verify binary compatibility with Java's sample-metric-data-uncompressed.bin.
// This file contains two blobs: "abcdefghi" (9 bytes) and binary data including
// a null byte and emoji (83 bytes).
TEST(PuffinReaderTest, JavaSampleMetricDataCompatibility) {
  // clang-format off
  auto java_sample = ToBytes({
      // Header magic
      0x50, 0x46, 0x41, 0x31,
      // Blob 1: "abcdefghi" (9 bytes, offset=4)
      0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69,
      // Blob 2: "some blob \0 binary data 🤯 that is not very very very very
      //          very very long, is it?" (83 bytes, offset=13)
      0x73, 0x6f, 0x6d, 0x65, 0x20, 0x62, 0x6c, 0x6f, 0x62, 0x20, 0x00, 0x20,
      0x62, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x20, 0x64, 0x61, 0x74, 0x61, 0x20,
      0xf0, 0x9f, 0xa4, 0xaf, 0x20, 0x74, 0x68, 0x61, 0x74, 0x20, 0x69, 0x73,
      0x20, 0x6e, 0x6f, 0x74, 0x20, 0x76, 0x65, 0x72, 0x79, 0x20, 0x76, 0x65,
      0x72, 0x79, 0x20, 0x76, 0x65, 0x72, 0x79, 0x20, 0x76, 0x65, 0x72, 0x79,
      0x20, 0x76, 0x65, 0x72, 0x79, 0x20, 0x76, 0x65, 0x72, 0x79, 0x20, 0x6c,
      0x6f, 0x6e, 0x67, 0x2c, 0x20, 0x69, 0x73, 0x20, 0x69, 0x74, 0x3f,
      // Footer start magic
      0x50, 0x46, 0x41, 0x31,
      // Footer payload (JSON)
      0x7b, 0x22, 0x62, 0x6c, 0x6f, 0x62, 0x73, 0x22, 0x3a, 0x5b, 0x7b, 0x22,
      0x74, 0x79, 0x70, 0x65, 0x22, 0x3a, 0x22, 0x73, 0x6f, 0x6d, 0x65, 0x2d,
      0x62, 0x6c, 0x6f, 0x62, 0x22, 0x2c, 0x22, 0x66, 0x69, 0x65, 0x6c, 0x64,
      0x73, 0x22, 0x3a, 0x5b, 0x31, 0x5d, 0x2c, 0x22, 0x73, 0x6e, 0x61, 0x70,
      0x73, 0x68, 0x6f, 0x74, 0x2d, 0x69, 0x64, 0x22, 0x3a, 0x32, 0x2c, 0x22,
      0x73, 0x65, 0x71, 0x75, 0x65, 0x6e, 0x63, 0x65, 0x2d, 0x6e, 0x75, 0x6d,
      0x62, 0x65, 0x72, 0x22, 0x3a, 0x31, 0x2c, 0x22, 0x6f, 0x66, 0x66, 0x73,
      0x65, 0x74, 0x22, 0x3a, 0x34, 0x2c, 0x22, 0x6c, 0x65, 0x6e, 0x67, 0x74,
      0x68, 0x22, 0x3a, 0x39, 0x7d, 0x2c, 0x7b, 0x22, 0x74, 0x79, 0x70, 0x65,
      0x22, 0x3a, 0x22, 0x73, 0x6f, 0x6d, 0x65, 0x2d, 0x6f, 0x74, 0x68, 0x65,
      0x72, 0x2d, 0x62, 0x6c, 0x6f, 0x62, 0x22, 0x2c, 0x22, 0x66, 0x69, 0x65,
      0x6c, 0x64, 0x73, 0x22, 0x3a, 0x5b, 0x32, 0x5d, 0x2c, 0x22, 0x73, 0x6e,
      0x61, 0x70, 0x73, 0x68, 0x6f, 0x74, 0x2d, 0x69, 0x64, 0x22, 0x3a, 0x32,
      0x2c, 0x22, 0x73, 0x65, 0x71, 0x75, 0x65, 0x6e, 0x63, 0x65, 0x2d, 0x6e,
      0x75, 0x6d, 0x62, 0x65, 0x72, 0x22, 0x3a, 0x31, 0x2c, 0x22, 0x6f, 0x66,
      0x66, 0x73, 0x65, 0x74, 0x22, 0x3a, 0x31, 0x33, 0x2c, 0x22, 0x6c, 0x65,
      0x6e, 0x67, 0x74, 0x68, 0x22, 0x3a, 0x38, 0x33, 0x7d, 0x5d, 0x2c, 0x22,
      0x70, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x22, 0x3a,
      0x7b, 0x22, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x64, 0x2d, 0x62, 0x79,
      0x22, 0x3a, 0x22, 0x54, 0x65, 0x73, 0x74, 0x20, 0x31, 0x32, 0x33, 0x34,
      0x22, 0x7d, 0x7d,
      // Footer struct: payload_size (243 = 0xf3) + flags (0) + magic
      0xf3, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00,
      0x50, 0x46, 0x41, 0x31,
  });
  // clang-format on

  PuffinReader reader(java_sample);
  auto fm = reader.ReadFileMetadata();
  ASSERT_THAT(fm, IsOk());
  ASSERT_EQ(fm.value().blobs.size(), 2);
  EXPECT_EQ(fm.value().properties.at("created-by"), "Test 1234");

  // Blob 1: "some-blob", fields=[1], snapshot=2, seq=1
  EXPECT_EQ(fm.value().blobs[0].type, "some-blob");
  EXPECT_EQ(fm.value().blobs[0].input_fields, std::vector<int32_t>{1});
  EXPECT_EQ(fm.value().blobs[0].snapshot_id, 2);
  EXPECT_EQ(fm.value().blobs[0].sequence_number, 1);
  EXPECT_EQ(fm.value().blobs[0].offset, 4);
  EXPECT_EQ(fm.value().blobs[0].length, 9);

  // Blob 2: "some-other-blob", fields=[2], snapshot=2, seq=1
  EXPECT_EQ(fm.value().blobs[1].type, "some-other-blob");
  EXPECT_EQ(fm.value().blobs[1].input_fields, std::vector<int32_t>{2});
  EXPECT_EQ(fm.value().blobs[1].offset, 13);
  EXPECT_EQ(fm.value().blobs[1].length, 83);

  // Read blob 1 data: "abcdefghi"
  auto blob1 = reader.ReadBlob(fm.value().blobs[0]);
  ASSERT_THAT(blob1, IsOk());
  EXPECT_EQ(blob1.value().second, ToBytes("abcdefghi"));

  // Read blob 2 data: contains null byte and emoji
  auto blob2 = reader.ReadBlob(fm.value().blobs[1]);
  ASSERT_THAT(blob2, IsOk());
  EXPECT_EQ(blob2.value().second.size(), 83);
  // Verify null byte at position 10
  EXPECT_EQ(blob2.value().second[10], std::byte{0x00});
  // Verify emoji 🤯 (U+1F92F) at positions 24-27
  EXPECT_EQ(blob2.value().second[24], std::byte{0xf0});
  EXPECT_EQ(blob2.value().second[25], std::byte{0x9f});
  EXPECT_EQ(blob2.value().second[26], std::byte{0xa4});
  EXPECT_EQ(blob2.value().second[27], std::byte{0xaf});

  // ReadAll should return both blobs
  auto all = reader.ReadAll(fm.value().blobs);
  ASSERT_THAT(all, IsOk());
  ASSERT_EQ(all.value().size(), 2);
}

}  // namespace iceberg::puffin
