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

#include <cerrno>
#include <cstdint>
#include <functional>
#include <memory>
#include <numeric>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <arrow/array.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <gtest/gtest.h>
#include <nanoarrow/nanoarrow.hpp>

#include "iceberg/arrow_row_builder_internal.h"
#include "iceberg/inspect/metadata_table.h"
#include "iceberg/inspect/metadata_table_stream_internal.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/test/inspect/metadata_table_test_base.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/stream.h"

namespace iceberg::internal {
namespace {

Status AppendRow(ArrowRowBuilder& builder, const int64_t& row) {
  ICEBERG_RETURN_UNEXPECTED(AppendInt(builder.column(0), row));
  return builder.FinishRow();
}

struct SourceState {
  int64_t reads = 0;
  int destructions = 0;
};

class CountingRows : public Stream<int64_t> {
 public:
  CountingRows(std::shared_ptr<SourceState> state, int64_t count,
               std::optional<int64_t> fail_at = std::nullopt)
      : state_(std::move(state)), count_(count), fail_at_(fail_at) {}

  ~CountingRows() override { ++state_->destructions; }

 protected:
  Result<std::optional<int64_t>> NextImpl() override {
    ++state_->reads;
    if (fail_at_ == next_) {
      return IOError("Row source failed");
    }
    if (next_ == count_) {
      return std::nullopt;
    }
    return next_++;
  }

 private:
  std::shared_ptr<SourceState> state_;
  int64_t count_;
  std::optional<int64_t> fail_at_;
  int64_t next_ = 0;
};

class MetadataTableStreamTest : public MetadataTableTestBase {
 protected:
  Schema schema_{std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())}};
};

TEST_F(MetadataTableStreamTest, VectorRowsSpanSeveralBatchesWithoutLoss) {
  std::vector<int64_t> rows(2 * MetadataTable::kBatchSize + 7);
  std::iota(rows.begin(), rows.end(), 0);
  ICEBERG_UNWRAP_OR_FAIL(auto stream,
                         MakeMetadataTableStream(schema_, std::move(rows), AppendRow));
  ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
  ASSERT_EQ(batches.size(), 3);
  int64_t expected = 0;
  for (const auto& batch : batches) {
    EXPECT_LE(batch->num_rows(), MetadataTable::kBatchSize);
    ASSERT_EQ(batch->num_columns(), 1);
    ASSERT_EQ(batch->column(0)->type_id(), ::arrow::Type::INT64);
    auto ids = std::static_pointer_cast<::arrow::Int64Array>(batch->column(0));
    for (int64_t i = 0; i < batch->num_rows(); ++i) {
      EXPECT_EQ(ids->Value(i), expected++);
    }
  }
  EXPECT_EQ(expected, 2 * MetadataTable::kBatchSize + 7);
}

TEST_F(MetadataTableStreamTest, LazyRowsAreReadOnlyForRequestedBatches) {
  auto state = std::make_shared<SourceState>();
  std::unique_ptr<Stream<int64_t>> rows =
      std::make_unique<CountingRows>(state, MetadataTable::kBatchSize + 1);
  ICEBERG_UNWRAP_OR_FAIL(auto stream,
                         MakeMetadataTableStream(schema_, std::move(rows), AppendRow));
  nanoarrow::UniqueArrayStream owned(&stream);
  EXPECT_EQ(state->reads, 0);
  nanoarrow::UniqueArray batch;
  ASSERT_EQ(owned->get_next(owned.get(), batch.get()), 0);
  ASSERT_NE(batch->release, nullptr);
  EXPECT_EQ(batch->length, MetadataTable::kBatchSize);
  EXPECT_EQ(state->reads, MetadataTable::kBatchSize);
  batch.reset();
  ASSERT_EQ(owned->get_next(owned.get(), batch.get()), 0);
  ASSERT_NE(batch->release, nullptr);
  EXPECT_EQ(batch->length, 1);
  EXPECT_EQ(state->reads, MetadataTable::kBatchSize + 2);
  batch.reset();
  for (int i = 0; i < 3; ++i) {
    ASSERT_EQ(owned->get_next(owned.get(), batch.get()), 0);
    EXPECT_EQ(batch->release, nullptr);
  }
  EXPECT_EQ(state->reads, MetadataTable::kBatchSize + 2);
  owned.reset();
  EXPECT_EQ(state->destructions, 1);
}

TEST_F(MetadataTableStreamTest, FailedAppendStopsCStreamPermanently) {
  auto state = std::make_shared<SourceState>();
  std::unique_ptr<Stream<int64_t>> rows =
      std::make_unique<CountingRows>(state, 3 * MetadataTable::kBatchSize);
  int64_t appended = 0;
  auto append = [&appended](ArrowRowBuilder& builder, const int64_t& row) -> Status {
    ++appended;
    if (row == MetadataTable::kBatchSize + 3) {
      return InvalidArrowData("Cannot append row");
    }
    return AppendRow(builder, row);
  };
  ICEBERG_UNWRAP_OR_FAIL(auto stream,
                         MakeMetadataTableStream(schema_, std::move(rows), append));
  nanoarrow::UniqueArrayStream owned(&stream);
  nanoarrow::UniqueArray batch;
  ASSERT_EQ(owned->get_next(owned.get(), batch.get()), 0);
  EXPECT_EQ(batch->length, MetadataTable::kBatchSize);
  batch.reset();
  for (int i = 0; i < 3; ++i) {
    EXPECT_EQ(owned->get_next(owned.get(), batch.get()), EIO);
    EXPECT_EQ(batch->release, nullptr);
    EXPECT_STREQ(owned->get_last_error(owned.get()), "Cannot append row");
  }
  EXPECT_EQ(state->reads, MetadataTable::kBatchSize + 4);
  EXPECT_EQ(appended, MetadataTable::kBatchSize + 4);
}

TEST_F(MetadataTableStreamTest, FailedSourceStopsCStreamPermanently) {
  auto state = std::make_shared<SourceState>();
  std::unique_ptr<Stream<int64_t>> rows = std::make_unique<CountingRows>(state, 2000, 3);
  ICEBERG_UNWRAP_OR_FAIL(auto stream,
                         MakeMetadataTableStream(schema_, std::move(rows), AppendRow));
  nanoarrow::UniqueArrayStream owned(&stream);
  nanoarrow::UniqueArray batch;
  for (int i = 0; i < 3; ++i) {
    EXPECT_EQ(owned->get_next(owned.get(), batch.get()), EIO);
    EXPECT_EQ(batch->release, nullptr);
    EXPECT_STREQ(owned->get_last_error(owned.get()), "Row source failed");
  }
  EXPECT_EQ(state->reads, 4);
}

TEST_F(MetadataTableStreamTest, RejectsNullSourceAndEmptyCallbacks) {
  EXPECT_THAT(
      MakeMetadataTableStream(schema_, std::unique_ptr<Stream<int64_t>>{}, AppendRow),
      IsError(ErrorKind::kInvalidArgument));
  std::function<Status(ArrowRowBuilder&, const int64_t&)> empty;
  EXPECT_THAT(MakeMetadataTableStream(schema_, std::vector<int64_t>{}, empty),
              IsError(ErrorKind::kInvalidArgument));
  decltype(&AppendRow) null_callback = nullptr;
  EXPECT_THAT(MakeMetadataTableStream(schema_, std::vector<int64_t>{}, null_callback),
              IsError(ErrorKind::kInvalidArgument));
}

TEST_F(MetadataTableStreamTest, EmptySourceRemainsExhausted) {
  auto state = std::make_shared<SourceState>();
  std::unique_ptr<Stream<int64_t>> rows = std::make_unique<CountingRows>(state, 0);
  ICEBERG_UNWRAP_OR_FAIL(auto stream,
                         MakeMetadataTableStream(schema_, std::move(rows), AppendRow));
  nanoarrow::UniqueArrayStream owned(&stream);
  nanoarrow::UniqueArray batch;
  for (int i = 0; i < 3; ++i) {
    ASSERT_EQ(owned->get_next(owned.get(), batch.get()), 0);
    EXPECT_EQ(batch->release, nullptr);
  }
  EXPECT_EQ(state->reads, 1);
}

TEST_F(MetadataTableStreamTest, CloseReleasesSourceAndRejectsReads) {
  auto state = std::make_shared<SourceState>();
  using RowsStream = MetadataTableRowsStream<int64_t, decltype(&AppendRow)>;
  ICEBERG_UNWRAP_OR_FAIL(
      auto stream,
      RowsStream::Make(schema_, std::make_unique<CountingRows>(state, 1), AppendRow));
  ASSERT_THAT(stream->Close(), IsOk());
  EXPECT_EQ(state->destructions, 1);
  EXPECT_THAT(stream->Next(), IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(stream->Schema(), IsError(ErrorKind::kInvalidArgument));
  ASSERT_THAT(stream->Close(), IsOk());
  stream.reset();
  EXPECT_EQ(state->destructions, 1);
}

TEST_F(MetadataTableStreamTest, EarlyReleaseDestroysSourceOnce) {
  auto state = std::make_shared<SourceState>();
  std::unique_ptr<Stream<int64_t>> rows = std::make_unique<CountingRows>(state, 2000);
  ICEBERG_UNWRAP_OR_FAIL(auto stream,
                         MakeMetadataTableStream(schema_, std::move(rows), AppendRow));
  nanoarrow::UniqueArrayStream owned(&stream);
  owned.reset();
  EXPECT_EQ(state->reads, 0);
  EXPECT_EQ(state->destructions, 1);
}

}  // namespace
}  // namespace iceberg::internal
