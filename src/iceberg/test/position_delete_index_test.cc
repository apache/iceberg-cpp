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

#include "iceberg/deletes/position_delete_index.h"

#include <cstdint>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/test/matchers.h"

namespace iceberg {

namespace {

// A source delete file matching a serialized blob: content_size_in_bytes and
// record_count are what Deserialize validates against.
std::shared_ptr<DataFile> DeleteFileFor(const std::vector<uint8_t>& blob,
                                        int64_t record_count) {
  return std::make_shared<DataFile>(DataFile{
      .content = DataFile::Content::kPositionDeletes,
      .file_path = "memory://dv.puffin",
      .file_format = FileFormatType::kPuffin,
      .record_count = record_count,
      .referenced_data_file = "data.parquet",
      .content_offset = 0,
      .content_size_in_bytes = static_cast<int64_t>(blob.size()),
  });
}

}  // namespace

TEST(PositionDeleteIndexTest, TestEmptyIndex) {
  PositionDeleteIndex index;
  ASSERT_TRUE(index.IsEmpty());
  ASSERT_EQ(index.Cardinality(), 0);
  ASSERT_FALSE(index.IsDeleted(0));
  ASSERT_FALSE(index.IsDeleted(100));
}

TEST(PositionDeleteIndexTest, TestSingleDelete) {
  PositionDeleteIndex index;
  index.Delete(42);

  ASSERT_FALSE(index.IsEmpty());
  ASSERT_EQ(index.Cardinality(), 1);
  ASSERT_TRUE(index.IsDeleted(42));
  ASSERT_FALSE(index.IsDeleted(41));
  ASSERT_FALSE(index.IsDeleted(43));
}

TEST(PositionDeleteIndexTest, TestMultipleDeletes) {
  PositionDeleteIndex index;
  index.Delete(10);
  index.Delete(20);
  index.Delete(30);

  ASSERT_EQ(index.Cardinality(), 3);
  ASSERT_TRUE(index.IsDeleted(10));
  ASSERT_TRUE(index.IsDeleted(20));
  ASSERT_TRUE(index.IsDeleted(30));
  ASSERT_FALSE(index.IsDeleted(15));
}

TEST(PositionDeleteIndexTest, TestDeleteRange) {
  PositionDeleteIndex index;
  index.Delete(10, 15);

  ASSERT_EQ(index.Cardinality(), 5);
  ASSERT_FALSE(index.IsDeleted(9));
  ASSERT_TRUE(index.IsDeleted(10));
  ASSERT_TRUE(index.IsDeleted(11));
  ASSERT_TRUE(index.IsDeleted(12));
  ASSERT_TRUE(index.IsDeleted(13));
  ASSERT_TRUE(index.IsDeleted(14));
  ASSERT_FALSE(index.IsDeleted(15));
}

TEST(PositionDeleteIndexTest, TestIsDeleted) {
  PositionDeleteIndex index;
  index.Delete(5);
  index.Delete(100);
  index.Delete(1000);

  ASSERT_TRUE(index.IsDeleted(5));
  ASSERT_TRUE(index.IsDeleted(100));
  ASSERT_TRUE(index.IsDeleted(1000));
  ASSERT_FALSE(index.IsDeleted(0));
  ASSERT_FALSE(index.IsDeleted(50));
  ASSERT_FALSE(index.IsDeleted(500));
}

TEST(PositionDeleteIndexTest, TestCardinality) {
  PositionDeleteIndex index;
  ASSERT_EQ(index.Cardinality(), 0);

  index.Delete(1);
  ASSERT_EQ(index.Cardinality(), 1);

  index.Delete(2);
  index.Delete(3);
  ASSERT_EQ(index.Cardinality(), 3);

  index.Delete(10, 20);
  ASSERT_EQ(index.Cardinality(), 13);
}

TEST(PositionDeleteIndexTest, TestMerge) {
  PositionDeleteIndex index1;
  index1.Delete(10);
  index1.Delete(20);

  PositionDeleteIndex index2;
  index2.Delete(20);
  index2.Delete(30);

  index1.Merge(index2);

  ASSERT_EQ(index1.Cardinality(), 3);
  ASSERT_TRUE(index1.IsDeleted(10));
  ASSERT_TRUE(index1.IsDeleted(20));
  ASSERT_TRUE(index1.IsDeleted(30));
}

TEST(PositionDeleteIndexTest, TestMergeEmpty) {
  PositionDeleteIndex index1;
  index1.Delete(10);

  PositionDeleteIndex index2;

  index1.Merge(index2);
  ASSERT_EQ(index1.Cardinality(), 1);
  ASSERT_TRUE(index1.IsDeleted(10));

  PositionDeleteIndex index3;
  PositionDeleteIndex index4;
  index4.Delete(20);

  index3.Merge(index4);
  ASSERT_EQ(index3.Cardinality(), 1);
  ASSERT_TRUE(index3.IsDeleted(20));
}

TEST(PositionDeleteIndexTest, TestInvalidPositions) {
  PositionDeleteIndex index;

  index.Delete(-1);
  index.Delete(-100);

  ASSERT_TRUE(index.IsEmpty());
  ASSERT_FALSE(index.IsDeleted(-1));
  ASSERT_FALSE(index.IsDeleted(-100));
}

TEST(PositionDeleteIndexTest, TestLargePositions) {
  PositionDeleteIndex index;

  int64_t large_pos = (int64_t{1} << 32) | 100;
  index.Delete(large_pos);

  ASSERT_EQ(index.Cardinality(), 1);
  ASSERT_TRUE(index.IsDeleted(large_pos));
  ASSERT_FALSE(index.IsDeleted(large_pos - 1));
  ASSERT_FALSE(index.IsDeleted(large_pos + 1));
}

TEST(PositionDeleteIndexTest, TestOverlappingRanges) {
  PositionDeleteIndex index;

  index.Delete(10, 20);
  index.Delete(15, 25);

  ASSERT_EQ(index.Cardinality(), 15);
  ASSERT_FALSE(index.IsDeleted(9));
  ASSERT_TRUE(index.IsDeleted(10));
  ASSERT_TRUE(index.IsDeleted(15));
  ASSERT_TRUE(index.IsDeleted(19));
  ASSERT_TRUE(index.IsDeleted(24));
  ASSERT_FALSE(index.IsDeleted(25));
}

TEST(PositionDeleteIndexTest, TestIdempotence) {
  PositionDeleteIndex index;

  index.Delete(42);
  index.Delete(42);
  index.Delete(42);

  ASSERT_EQ(index.Cardinality(), 1);
  ASSERT_TRUE(index.IsDeleted(42));
}

TEST(PositionDeleteIndexTest, TestMergeIdempotence) {
  PositionDeleteIndex index1;
  index1.Delete(10);
  index1.Delete(20);

  PositionDeleteIndex index2;
  index2.Delete(10);
  index2.Delete(20);

  index1.Merge(index2);
  index1.Merge(index2);

  ASSERT_EQ(index1.Cardinality(), 2);
  ASSERT_TRUE(index1.IsDeleted(10));
  ASSERT_TRUE(index1.IsDeleted(20));
}

// ==================== deletion-vector-v1 serialization ====================

TEST(PositionDeleteIndexTest, SerializeRoundTrip) {
  PositionDeleteIndex index;
  for (int64_t pos :
       {int64_t{0}, int64_t{1}, int64_t{5}, int64_t{100}, int64_t{4'000'000'000}}) {
    index.Delete(pos);
  }

  ICEBERG_UNWRAP_OR_FAIL(auto blob, index.Serialize());
  ICEBERG_UNWRAP_OR_FAIL(auto restored,
                         PositionDeleteIndex::Deserialize(blob, DeleteFileFor(blob, 5)));

  EXPECT_EQ(restored.Cardinality(), 5);
  for (int64_t pos :
       {int64_t{0}, int64_t{1}, int64_t{5}, int64_t{100}, int64_t{4'000'000'000}}) {
    EXPECT_TRUE(restored.IsDeleted(pos));
  }
}

TEST(PositionDeleteIndexTest, SerializeEmptyRoundTrip) {
  PositionDeleteIndex index;
  ICEBERG_UNWRAP_OR_FAIL(auto blob, index.Serialize());
  ICEBERG_UNWRAP_OR_FAIL(auto restored,
                         PositionDeleteIndex::Deserialize(blob, DeleteFileFor(blob, 0)));
  EXPECT_TRUE(restored.IsEmpty());
}

// Spans two high-32-bit keys and exercises all Roaring container types
// (sparse "array", dense "bitset", and run containers after optimization).
TEST(PositionDeleteIndexTest, SerializeAllContainerTypesAcrossKeys) {
  constexpr int64_t kKeyStride = 0x100000000LL;  // 2^32: high-32-bit key
  constexpr int64_t kContainerStride = 1 << 16;  // 2^16: Roaring container
  auto pos = [](int64_t key, int64_t container, int64_t value) {
    return key * kKeyStride + container * kContainerStride + value;
  };

  PositionDeleteIndex index;
  int64_t expected = 0;
  for (int64_t key : {int64_t{0}, int64_t{1}}) {
    index.Delete(pos(key, 0, 5));
    index.Delete(pos(key, 0, 7));
    expected += 2;
    index.Delete(pos(key, 1, 1), pos(key, 1, 1000));
    expected += 999;
    index.Delete(pos(key, 2, 1), pos(key, 2, kContainerStride));
    expected += kContainerStride - 1;
  }

  ICEBERG_UNWRAP_OR_FAIL(auto blob, index.Serialize());
  ICEBERG_UNWRAP_OR_FAIL(auto restored, PositionDeleteIndex::Deserialize(
                                            blob, DeleteFileFor(blob, expected)));

  EXPECT_EQ(restored.Cardinality(), expected);
  EXPECT_TRUE(restored.IsDeleted(pos(0, 0, 5)));
  EXPECT_TRUE(restored.IsDeleted(pos(1, 2, kContainerStride - 1)));
  EXPECT_TRUE(restored.IsDeleted(pos(0, 1, 999)));
  EXPECT_FALSE(restored.IsDeleted(pos(0, 0, 6)));
  EXPECT_FALSE(restored.IsDeleted(pos(1, 1, 1000)));  // range end is exclusive
}

TEST(PositionDeleteIndexTest, DeserializeRejectsCorruptedCrc) {
  PositionDeleteIndex index;
  index.Delete(1);
  index.Delete(2);
  ICEBERG_UNWRAP_OR_FAIL(auto blob, index.Serialize());

  blob.back() ^= 0xFF;
  EXPECT_THAT(PositionDeleteIndex::Deserialize(blob, DeleteFileFor(blob, 2)),
              IsError(ErrorKind::kInvalidArgument));
}

TEST(PositionDeleteIndexTest, DeserializeRejectsBadMagic) {
  PositionDeleteIndex index;
  index.Delete(1);
  ICEBERG_UNWRAP_OR_FAIL(auto blob, index.Serialize());

  blob[4] = 0x00;
  EXPECT_THAT(PositionDeleteIndex::Deserialize(blob, DeleteFileFor(blob, 1)),
              IsError(ErrorKind::kInvalidArgument));
}

TEST(PositionDeleteIndexTest, DeserializeRejectsTruncatedBlob) {
  std::vector<uint8_t> blob = {0x00, 0x00};
  EXPECT_THAT(PositionDeleteIndex::Deserialize(blob, DeleteFileFor(blob, 0)),
              IsError(ErrorKind::kInvalidArgument));
}

TEST(PositionDeleteIndexTest, DeserializeRejectsCardinalityMismatch) {
  PositionDeleteIndex index;
  index.Delete(1);
  index.Delete(2);
  ICEBERG_UNWRAP_OR_FAIL(auto blob, index.Serialize());

  // record_count in metadata disagrees with the bitmap cardinality.
  EXPECT_THAT(PositionDeleteIndex::Deserialize(blob, DeleteFileFor(blob, 99)),
              IsError(ErrorKind::kInvalidArgument));
}

TEST(PositionDeleteIndexTest, DeserializeRejectsContentSizeMismatch) {
  PositionDeleteIndex index;
  index.Delete(1);
  ICEBERG_UNWRAP_OR_FAIL(auto blob, index.Serialize());

  auto delete_file = DeleteFileFor(blob, 1);
  delete_file->content_size_in_bytes = static_cast<int64_t>(blob.size()) + 1;
  EXPECT_THAT(PositionDeleteIndex::Deserialize(blob, delete_file),
              IsError(ErrorKind::kInvalidArgument));
}

TEST(PositionDeleteIndexTest, DeserializePreservesSourceDeleteFile) {
  PositionDeleteIndex index;
  index.Delete(1);
  index.Delete(2);
  ICEBERG_UNWRAP_OR_FAIL(auto blob, index.Serialize());

  auto delete_file = DeleteFileFor(blob, 2);
  ICEBERG_UNWRAP_OR_FAIL(auto restored,
                         PositionDeleteIndex::Deserialize(blob, delete_file));
  ASSERT_EQ(restored.delete_files().size(), 1u);
  EXPECT_EQ(restored.delete_files()[0], delete_file);
}

TEST(PositionDeleteIndexTest, ConstructorsPreserveSourceDeleteFiles) {
  auto file_a = std::make_shared<DataFile>(DataFile{.file_path = "a.parquet"});
  auto file_b = std::make_shared<DataFile>(DataFile{.file_path = "b.parquet"});

  PositionDeleteIndex single(file_a);
  ASSERT_EQ(single.delete_files().size(), 1u);
  EXPECT_EQ(single.delete_files()[0], file_a);

  PositionDeleteIndex multiple(std::vector<std::shared_ptr<DataFile>>{file_a, file_b});
  ASSERT_EQ(multiple.delete_files().size(), 2u);
  EXPECT_EQ(multiple.delete_files()[0], file_a);
  EXPECT_EQ(multiple.delete_files()[1], file_b);
}

}  // namespace iceberg
