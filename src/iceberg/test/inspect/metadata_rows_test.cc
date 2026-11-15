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
#include <optional>
#include <utility>
#include <vector>

#include <arrow/array.h>
#include <arrow/type.h>
#include <gtest/gtest.h>
#include <nanoarrow/nanoarrow.hpp>

#include "iceberg/inspect/metadata_scan_util_internal.h"
#include "iceberg/inspect/metadata_table.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/test/inspect/metadata_table_test_base.h"
#include "iceberg/test/matchers.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"

namespace iceberg::internal {
namespace {

Status AppendIntRow(ArrowRowBuilder& builder, const int64_t& row) {
  ICEBERG_RETURN_UNEXPECTED(AppendInt(builder.column(0), row));
  return builder.FinishRow();
}

std::shared_ptr<Schema> IntSchema() {
  return std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});
}

class CountingRows : public Stream<int64_t> {
 public:
  explicit CountingRows(std::shared_ptr<int> reads) : reads_(std::move(reads)) {}

 protected:
  Result<std::optional<int64_t>> NextImpl() override {
    ++*reads_;
    if (next_ == 5) {
      return std::nullopt;
    }
    return next_++;
  }

 private:
  std::shared_ptr<int> reads_;
  int64_t next_ = 0;
};

TEST(MetadataRowsTest, RejectsInvalidInputsBeforeExport) {
  EXPECT_THAT(MakeMetadataBatchStream(IntSchema(), std::unique_ptr<Stream<int64_t>>{}, 2,
                                      AppendIntRow),
              IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(MakeMetadataBatchStream(std::shared_ptr<Schema>{}, std::vector<int64_t>{1},
                                      2, AppendIntRow),
              IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(
      MakeMetadataBatchStream(IntSchema(), std::vector<int64_t>{1}, 0, AppendIntRow),
      IsError(ErrorKind::kInvalidArgument));
  decltype(&AppendIntRow) callback = nullptr;
  EXPECT_THAT(MakeMetadataBatchStream(IntSchema(), std::vector<int64_t>{1}, 2, callback),
              IsError(ErrorKind::kInvalidArgument));
  std::function<Status(ArrowRowBuilder&, const int64_t&)> empty;
  EXPECT_THAT(MakeMetadataBatchStream(IntSchema(), std::vector<int64_t>{1}, 2, empty),
              IsError(ErrorKind::kInvalidArgument));
}

TEST(MetadataRowsTest, LazyBatchesAndExhaustion) {
  auto reads = std::make_shared<int>(0);
  std::unique_ptr<Stream<int64_t>> rows = std::make_unique<CountingRows>(reads);
  ICEBERG_UNWRAP_OR_FAIL(auto stream, MakeMetadataBatchStream(
                                          IntSchema(), std::move(rows), 2, AppendIntRow));
  nanoarrow::UniqueArrayStream owned(&stream);
  nanoarrow::UniqueArray batch;
  EXPECT_EQ(*reads, 0);
  for (int length : {2, 2, 1}) {
    ASSERT_EQ(owned->get_next(owned.get(), batch.get()), 0);
    ASSERT_NE(batch->release, nullptr);
    EXPECT_EQ(batch->length, length);
    batch.reset();
  }
  for (int i = 0; i < 3; ++i) {
    EXPECT_EQ(owned->get_next(owned.get(), batch.get()), 0);
    EXPECT_EQ(batch->release, nullptr);
  }
  EXPECT_EQ(*reads, 6);
}

TEST(MetadataRowsTest, AppendFailureIsTerminal) {
  auto reads = std::make_shared<int>(0);
  std::unique_ptr<Stream<int64_t>> rows = std::make_unique<CountingRows>(reads);
  auto append = [](ArrowRowBuilder& builder, const int64_t& row) -> Status {
    if (row == 3) {
      return InvalidArrowData("Row append failed");
    }
    return AppendIntRow(builder, row);
  };
  ICEBERG_UNWRAP_OR_FAIL(
      auto stream, MakeMetadataBatchStream(IntSchema(), std::move(rows), 2, append));
  nanoarrow::UniqueArrayStream owned(&stream);
  nanoarrow::UniqueArray batch;
  ASSERT_EQ(owned->get_next(owned.get(), batch.get()), 0);
  batch.reset();
  for (int i = 0; i < 3; ++i) {
    EXPECT_EQ(owned->get_next(owned.get(), batch.get()), EIO);
    EXPECT_EQ(batch->release, nullptr);
    EXPECT_STREQ(owned->get_last_error(owned.get()), "Row append failed");
  }
  EXPECT_EQ(*reads, 4);
}

}  // namespace
}  // namespace iceberg::internal
