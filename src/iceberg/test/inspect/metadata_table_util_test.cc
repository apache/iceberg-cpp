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

#include "iceberg/inspect/metadata_scan_task_reader.h"
#include "iceberg/inspect/metadata_scan_util_internal.h"
#include "iceberg/inspect/metadata_table.h"
#include "iceberg/inspect/metadata_table_util_internal.h"
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

Result<ArrowArrayStream> ReadIntTaskRows(const MetadataScanContext& context,
                                         const std::shared_ptr<FileIO>& io,
                                         int64_t batch_size) {
  ICEBERG_PRECHECK(io != nullptr && context.table_metadata != nullptr,
                   "Task lost its input owners");
  return MakeMetadataBatchStream(context.projected_schema, std::vector<int64_t>{1, 2, 3},
                                 batch_size, AppendIntRow);
}

class SnapshotMetadataSupportTest : public MetadataTableTestBase {
 protected:
  MetadataScanContext Context() {
    auto schema = IntSchema();
    return {.kind = MetadataTableKind::kFiles,
            .table_metadata = metadata_,
            .schema = schema,
            .projected_schema = schema,
            .io = io_};
  }
};

TEST_F(SnapshotMetadataSupportTest, TasksRetainOwnersAndCanBeReopened) {
  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeSnapshotMetadataScan(Context(), ReadIntTaskRows));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanTasks());
  ASSERT_EQ(tasks.size(), 1);
  scan.reset();
  table_.reset();
  metadata_.reset();
  io_.reset();
  for (int attempt = 0; attempt < 2; ++attempt) {
    ICEBERG_UNWRAP_OR_FAIL(auto reader, MetadataScanTaskReader::Make({.batch_size = 2}));
    ICEBERG_UNWRAP_OR_FAIL(auto stream, reader->Open(*tasks[0]));
    reader.reset();
    ICEBERG_UNWRAP_OR_FAIL(auto batches, ReadAllBatches(std::move(stream)));
    ASSERT_EQ(batches.size(), 2);
    EXPECT_EQ(batches[0]->num_rows(), 2);
    EXPECT_EQ(batches[1]->num_rows(), 1);
    ASSERT_EQ(batches[0]->column(0)->type_id(), ::arrow::Type::INT64);
    auto ids = std::static_pointer_cast<::arrow::Int64Array>(batches[0]->column(0));
    EXPECT_EQ(ids->Value(0), 1);
  }
}

TEST_F(SnapshotMetadataSupportTest, RejectsInvalidProjectionAndReader) {
  EXPECT_THAT(MakeSnapshotMetadataScan(Context(), nullptr),
              IsError(ErrorKind::kInvalidArgument));
  auto context = Context();
  context.projected_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(99, "missing", int64())});
  EXPECT_THAT(MakeSnapshotMetadataScan(context, ReadIntTaskRows),
              IsError(ErrorKind::kInvalidArgument));
  context = Context();
  context.ref_name = "dev";
  context.snapshot_id = 1;
  EXPECT_THAT(MakeSnapshotMetadataScan(context, ReadIntTaskRows),
              IsError(ErrorKind::kInvalidArgument));
}

TEST_F(SnapshotMetadataSupportTest, ProjectsAcrossPartitionSpecEvolution) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto old_spec,
      PartitionSpec::Make(1, {PartitionField(1, 1000, "old_id", Transform::Identity())}));
  ICEBERG_UNWRAP_OR_FAIL(
      auto new_spec,
      PartitionSpec::Make(2, {PartitionField(1, 1000, "new_id", Transform::Identity()),
                              PartitionField(2, 1001, "data", Transform::Identity())}));
  metadata_->partition_specs.clear();
  metadata_->partition_specs.push_back(std::move(old_spec));
  metadata_->partition_specs.push_back(std::move(new_spec));
  ICEBERG_UNWRAP_OR_FAIL(auto type, UnifiedPartitionType(*metadata_));
  ASSERT_EQ(type->fields().size(), 2);
  EXPECT_EQ(type->fields()[0].name(), "new_id");
  ICEBERG_UNWRAP_OR_FAIL(auto values,
                         ProjectPartitionValues(*type, *metadata_->partition_specs[0],
                                                PartitionValues(Literal::Int(7))));
  ASSERT_EQ(values.num_fields(), 2);
  EXPECT_EQ(values.values()[0], Literal::Long(7));
  EXPECT_TRUE(values.values()[1].IsNull());
}

}  // namespace
}  // namespace iceberg::internal
