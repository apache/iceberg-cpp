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

#include <limits>
#include <memory>
#include <numeric>
#include <vector>

#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/memory_pool.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <parquet/metadata.h>
#include <parquet/properties.h>
#include <parquet/schema.h>

#include "iceberg/parquet/row_number_reader_internal.h"
#include "iceberg/test/matchers.h"

namespace iceberg::parquet {

namespace {

std::unique_ptr<::parquet::FileMetaData> MakeMetadata(
    const std::vector<int64_t>& row_group_sizes) {
  // Only row counts are needed; no column data or file I/O is involved.
  ::parquet::SchemaDescriptor schema;
  schema.Init(
      ::parquet::schema::GroupNode::Make("schema", ::parquet::Repetition::REQUIRED, {}));
  auto builder = ::parquet::FileMetaDataBuilder::Make(
      &schema, ::parquet::default_writer_properties());
  for (int64_t size : row_group_sizes) {
    auto* row_group = builder->AppendRowGroup();
    row_group->set_num_rows(size);
    row_group->Finish(0);
  }
  return builder->Finish();
}

void VerifyNextBatch(RowNumberReader& reader, int64_t batch_size,
                     const std::vector<int64_t>& expected) {
  ICEBERG_UNWRAP_OR_FAIL(auto array, reader.NextBatch(batch_size));
  ASSERT_TRUE(array->ValidateFull().ok());
  ASSERT_EQ(array->null_count(), 0);
  ASSERT_EQ(array->length(), expected.size());
  for (size_t i = 0; i < expected.size(); ++i) {
    EXPECT_EQ(array->Value(i), expected[i]) << "Row " << i;
  }
}

}  // namespace

TEST(RowNumberReaderTest, ReadsAllRowGroups) {
  auto metadata = MakeMetadata({2, 3, 4});
  ICEBERG_UNWRAP_OR_FAIL(
      auto reader,
      RowNumberReader::Make(*metadata, {0, 1, 2}, ::arrow::default_memory_pool()));
  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, 100, {0, 1, 2, 3, 4, 5, 6, 7, 8}));
  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, 1, {}));
  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, 1, {}));
}

TEST(RowNumberReaderTest, SkippedRowGroupKeepsFilePositions) {
  auto metadata = MakeMetadata({3, 3, 3});
  ICEBERG_UNWRAP_OR_FAIL(
      auto reader,
      RowNumberReader::Make(*metadata, {0, 2}, ::arrow::default_memory_pool()));

  // The first batch spans the gap left by the skipped row group.
  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, 5, {0, 1, 2, 6, 7}));
  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, 5, {8}));
  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, 1, {}));
}

TEST(RowNumberReaderTest, ReorderedAndRepeatedRowGroups) {
  auto metadata = MakeMetadata({2, 2, 2});
  ICEBERG_UNWRAP_OR_FAIL(
      auto reader,
      RowNumberReader::Make(*metadata, {2, 0, 2}, ::arrow::default_memory_pool()));

  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, 3, {4, 5, 0}));
  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, 3, {1, 4, 5}));
  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, 1, {}));
}

TEST(RowNumberReaderTest, BatchesNotAlignedWithRowGroups) {
  auto metadata = MakeMetadata({3, 3, 3});
  ICEBERG_UNWRAP_OR_FAIL(
      auto reader,
      RowNumberReader::Make(*metadata, {0, 1, 2}, ::arrow::default_memory_pool()));

  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, 2, {0, 1}));
  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, 4, {2, 3, 4, 5}));
  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, 4, {6, 7, 8}));
}

TEST(RowNumberReaderTest, EmptyRowGroups) {
  auto metadata = MakeMetadata({0, 2, 0, 1, 0});
  ICEBERG_UNWRAP_OR_FAIL(
      auto reader,
      RowNumberReader::Make(*metadata, {0, 1, 2, 3, 4}, ::arrow::default_memory_pool()));
  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, 2, {0, 1}));
  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, 2, {2}));
  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, 2, {}));
}

TEST(RowNumberReaderTest, NoRowGroups) {
  for (const auto& sizes : {std::vector<int64_t>{}, std::vector<int64_t>{5}}) {
    auto metadata = MakeMetadata(sizes);
    ICEBERG_UNWRAP_OR_FAIL(
        auto reader,
        RowNumberReader::Make(*metadata, {}, ::arrow::default_memory_pool()));
    ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, 10, {}));
  }
}

TEST(RowNumberReaderTest, ZeroAndNegativeBatchSizesDoNotAdvance) {
  auto metadata = MakeMetadata({2});
  ICEBERG_UNWRAP_OR_FAIL(
      auto reader, RowNumberReader::Make(*metadata, {0}, ::arrow::default_memory_pool()));
  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, 0, {}));
  EXPECT_THAT(reader->NextBatch(-1), IsError(ErrorKind::kInvalidArgument));
  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, 1, {0}));
  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, 0, {}));
  ASSERT_NO_FATAL_FAILURE(
      VerifyNextBatch(*reader, std::numeric_limits<int64_t>::max(), {1}));
}

TEST(RowNumberReaderTest, RowGroupIndexOutOfBounds) {
  auto metadata = MakeMetadata({5});
  for (int index : {-1, 1}) {
    EXPECT_THAT(RowNumberReader::Make(*metadata, {index}, ::arrow::default_memory_pool()),
                IsError(ErrorKind::kInvalidArgument));
  }
}

TEST(RowNumberReaderTest, NegativeRowCount) {
  auto metadata = MakeMetadata({-1});
  EXPECT_THAT(RowNumberReader::Make(*metadata, {0}, ::arrow::default_memory_pool()),
              IsError(ErrorKind::kInvalidArgument));
}

TEST(RowNumberReaderTest, OversizedBatchDoesNotAdvance) {
  auto metadata = MakeMetadata({std::numeric_limits<int64_t>::max()});
  ICEBERG_UNWRAP_OR_FAIL(
      auto reader, RowNumberReader::Make(*metadata, {0}, ::arrow::default_memory_pool()));
  EXPECT_THAT(reader->NextBatch(std::numeric_limits<int64_t>::max()),
              IsError(ErrorKind::kInvalidArgument));
  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, 2, {0, 1}));
}

TEST(RowNumberReaderTest, AllocationFailureDoesNotAdvance) {
  auto metadata = MakeMetadata({64, 64, 64, 64});
  ::arrow::ProxyMemoryPool tracked_pool(::arrow::default_memory_pool());
  ::arrow::CappedMemoryPool pool(&tracked_pool, 2048);
  ICEBERG_UNWRAP_OR_FAIL(auto reader, RowNumberReader::Make(*metadata, {0, 2, 3}, &pool));
  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, 1, {0}));

  // Leave room for the values buffer, but not the validity bitmap.
  auto held_buffer = ::arrow::AllocateBuffer(1024, &pool).ValueOrDie();
  EXPECT_FALSE(reader->NextBatch(128).has_value());
  EXPECT_EQ(pool.bytes_allocated(), 1024);
  EXPECT_FALSE(reader->NextBatch(128).has_value());
  EXPECT_EQ(pool.bytes_allocated(), 1024);

  // Retry the same request after freeing memory, including the row group gap.
  held_buffer.reset();
  EXPECT_EQ(pool.bytes_allocated(), 0);
  std::vector<int64_t> expected(128);
  std::iota(expected.begin(), expected.begin() + 63, 1);
  std::iota(expected.begin() + 63, expected.end(), 128);
  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, 128, expected));
  // The last row group was only partially consumed by the successful retry.
  expected.resize(63);
  std::iota(expected.begin(), expected.end(), 193);
  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, 128, expected));
  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, 1, {}));
}

TEST(RowNumberReaderTest, BatchesOwnBuffersFromMemoryPool) {
  auto metadata = MakeMetadata({4});
  ::arrow::ProxyMemoryPool pool(::arrow::default_memory_pool());
  ICEBERG_UNWRAP_OR_FAIL(auto reader, RowNumberReader::Make(*metadata, {0}, &pool));
  ICEBERG_UNWRAP_OR_FAIL(auto first_batch, reader->NextBatch(2));
  EXPECT_GT(pool.bytes_allocated(), 0);
  ASSERT_NO_FATAL_FAILURE(VerifyNextBatch(*reader, 2, {2, 3}));
  reader.reset();
  EXPECT_EQ(first_batch->Value(0), 0);
  EXPECT_EQ(first_batch->Value(1), 1);
}

}  // namespace iceberg::parquet
