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

std::shared_ptr<Schema> IntSchema() {
  return std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});
}

Result<ArrowArrayStream> ReadIntTaskRows(const MetadataScanContext& context,
                                         const std::shared_ptr<FileIO>& io,
                                         int64_t batch_size) {
  ICEBERG_PRECHECK(io != nullptr && context.table_metadata != nullptr,
                   "Task lost its input owners");
  const std::vector<MetadataTableColumn<int64_t>> columns{
      {.field_id = 1, .append = [](ArrowArray* array, const int64_t& row) {
         return AppendInt(array, row);
       }}};
  return MakeMetadataBatchStream<int64_t>(
      context.schema, context.projected_schema, columns, 3, batch_size,
      [metadata = context.table_metadata, io](size_t index) {
        return static_cast<int64_t>(index + 1);
      });
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

TEST_F(SnapshotMetadataSupportTest, ResolvesSnapshotIdsReferencesAndTimes) {
  auto first = std::make_shared<Snapshot>(
      Snapshot{.snapshot_id = 10, .timestamp_ms = TimePointMsFromUnixMs(1000)});
  auto second = std::make_shared<Snapshot>(
      Snapshot{.snapshot_id = 20, .timestamp_ms = TimePointMsFromUnixMs(2000)});
  auto metadata = metadata_;
  metadata->snapshots = {first, second};
  metadata->current_snapshot_id = 20;
  metadata->refs["tag"] = std::make_shared<SnapshotRef>(
      SnapshotRef{.snapshot_id = 10, .retention = SnapshotRef::Tag{}});
  metadata->snapshot_log = {
      {.timestamp_ms = TimePointMsFromUnixMs(1000), .snapshot_id = 10},
      {.timestamp_ms = TimePointMsFromUnixMs(2000), .snapshot_id = 20}};
  MetadataScanContext selection{.kind = MetadataTableKind::kFiles};
  ICEBERG_UNWRAP_OR_FAIL(auto current,
                         ResolveMetadataTableSnapshot(*metadata, selection));
  EXPECT_EQ(current, second);
  selection.snapshot_id = 10;
  ICEBERG_UNWRAP_OR_FAIL(auto by_id, ResolveMetadataTableSnapshot(*metadata, selection));
  EXPECT_EQ(by_id, first);
  selection.snapshot_id.reset();
  selection.ref_name = "tag";
  ICEBERG_UNWRAP_OR_FAIL(auto by_ref, ResolveMetadataTableSnapshot(*metadata, selection));
  EXPECT_EQ(by_ref, first);
  selection.ref_name.clear();
  selection.as_of_time_in_millis = 1500;
  ICEBERG_UNWRAP_OR_FAIL(auto by_time,
                         ResolveMetadataTableSnapshot(*metadata, selection));
  EXPECT_EQ(by_time, first);
  selection.ref_name = "tag";
  EXPECT_THAT(ResolveMetadataTableSnapshot(*metadata, selection),
              IsError(ErrorKind::kInvalidArgument));
  selection.as_of_time_in_millis.reset();
  selection.ref_name = "missing";
  EXPECT_THAT(ResolveMetadataTableSnapshot(*metadata, selection),
              IsError(ErrorKind::kInvalidArgument));
}

TEST_F(SnapshotMetadataSupportTest, PreservesSnapshotLookupErrorsAndEmptyTables) {
  auto selection = Context();
  for (const auto* ref : {"", "main"}) {
    selection.ref_name = ref;
    ICEBERG_UNWRAP_OR_FAIL(auto snapshot,
                           ResolveMetadataTableSnapshot(*metadata_, selection));
    EXPECT_EQ(snapshot, nullptr);
  }

  selection.ref_name = "missing";
  EXPECT_THAT(ResolveMetadataTableSnapshot(*metadata_, selection),
              IsError(ErrorKind::kInvalidArgument));
  metadata_->refs["null"] = nullptr;
  selection.ref_name = "null";
  EXPECT_THAT(ResolveMetadataTableSnapshot(*metadata_, selection),
              IsError(ErrorKind::kInvalidArgument));

  metadata_->refs["dangling"] = std::make_shared<SnapshotRef>(
      SnapshotRef{.snapshot_id = 999, .retention = SnapshotRef::Tag{}});
  selection.ref_name = "dangling";
  EXPECT_THAT(ResolveMetadataTableSnapshot(*metadata_, selection),
              IsError(ErrorKind::kNotFound));
  selection.ref_name = "main";
  metadata_->current_snapshot_id = 999;
  EXPECT_THAT(ResolveMetadataTableSnapshot(*metadata_, selection),
              IsError(ErrorKind::kNotFound));
}

}  // namespace
}  // namespace iceberg::internal
