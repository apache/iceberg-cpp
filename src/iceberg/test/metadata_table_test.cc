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

#include "iceberg/inspect/metadata_table.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/constants.h"
#include "iceberg/inspect/history_table.h"
#include "iceberg/inspect/metadata_scan.h"
#include "iceberg/inspect/metadata_scan_task.h"
#include "iceberg/inspect/snapshots_table.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/table.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/arrow_test_util.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_catalog.h"
#include "iceberg/test/mock_io.h"
#include "iceberg/type.h"

namespace iceberg {
namespace {

class PlannedMetadataScan : public MetadataScan {
 public:
  PlannedMetadataScan(internal::MetadataScanContext context,
                      std::vector<std::shared_ptr<MetadataScanTask>> tasks)
      : MetadataScan(std::move(context)), tasks_(std::move(tasks)) {}

  Result<MetadataScanTaskStreamPtr> PlanTasksStream() const override {
    return MetadataScanTaskStream::FromVector(tasks_);
  }

 private:
  const std::vector<std::shared_ptr<MetadataScanTask>> tasks_;
};

}  // namespace

class MetadataTableTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto schema = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64()),
                                 SchemaField::MakeOptional(2, "name", string())},
        1);
    auto metadata = std::make_shared<TableMetadata>(
        TableMetadata{.format_version = 2,
                      .schemas = {schema},
                      .current_schema_id = 1,
                      .current_snapshot_id = kInvalidSnapshotId});

    TableIdentifier ident{.ns = Namespace{.levels = {"db"}}, .name = "source_table"};
    ICEBERG_UNWRAP_OR_FAIL(table_, Table::Make(ident, metadata, "s3://bucket/meta.json",
                                               std::make_shared<MockFileIO>(),
                                               std::make_shared<MockCatalog>()));
  }

  static Result<ArrowArrayStream> BuildStream(MetadataScanBuilder& builder) {
    ICEBERG_ASSIGN_OR_RAISE(auto scan, builder.Build());
    return scan->ToArrowArrayStream();
  }

  /// \brief Import the schema of a stream and release the stream.
  static Result<std::shared_ptr<::arrow::Schema>> ReadSchema(ArrowArrayStream&& stream) {
    auto reader_result = ::arrow::ImportRecordBatchReader(&stream);
    if (!reader_result.ok()) {
      if (stream.release != nullptr) {
        stream.release(&stream);
      }
      return InvalidArrowData(reader_result.status().ToString());
    }
    return reader_result.ValueUnsafe()->schema();
  }

  std::shared_ptr<Table> table_;
};

TEST_F(MetadataTableTest, FactoryRejectsNullSourceTable) {
  auto result = MetadataTable::Make<SnapshotsTable>(nullptr);
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result, HasErrorMessage("Table cannot be null"));
}

TEST_F(MetadataTableTest, ScanBuilderSelectResolvesNamesImmediately) {
  ICEBERG_UNWRAP_OR_FAIL(auto snapshots_table,
                         MetadataTable::Make<SnapshotsTable>(table_));

  // Select() uses the case sensitivity in effect when it is called.
  ICEBERG_UNWRAP_OR_FAIL(auto builder, snapshots_table->NewScan());
  ICEBERG_UNWRAP_OR_FAIL(
      auto stream, BuildStream(builder->Select({"SNAPSHOT_ID"}).CaseSensitive(false)));
  ICEBERG_UNWRAP_OR_FAIL(auto schema, ReadSchema(std::move(stream)));
  EXPECT_EQ(schema->num_fields(), 0);

  ICEBERG_UNWRAP_OR_FAIL(auto insensitive_builder, snapshots_table->NewScan());
  ICEBERG_UNWRAP_OR_FAIL(
      auto insensitive_stream,
      BuildStream(insensitive_builder->CaseSensitive(false).Select({"SNAPSHOT_ID"})));
  ICEBERG_UNWRAP_OR_FAIL(auto insensitive_schema,
                         ReadSchema(std::move(insensitive_stream)));
  EXPECT_THAT(insensitive_schema->field_names(), ::testing::ElementsAre("snapshot_id"));

  // An invalid selection is reported when the scan is built.
  ICEBERG_UNWRAP_OR_FAIL(auto invalid_builder, snapshots_table->NewScan());
  auto invalid = invalid_builder->Select({"summary.key"}).Build();
  EXPECT_THAT(invalid, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(invalid, HasErrorMessage("Cannot project Map without value field"));
}

TEST_F(MetadataTableTest, ScanBuilderProject) {
  ICEBERG_UNWRAP_OR_FAIL(auto snapshots_table,
                         MetadataTable::Make<SnapshotsTable>(table_));
  auto projection = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(4, "operation", string()),
                               SchemaField::MakeRequired(2, "snapshot_id", int64())});

  // Project() replaces an earlier selection.
  ICEBERG_UNWRAP_OR_FAIL(auto builder, snapshots_table->NewScan());
  ICEBERG_UNWRAP_OR_FAIL(
      auto stream, BuildStream(builder->Select({"snapshot_id"}).Project(projection)));
  ICEBERG_UNWRAP_OR_FAIL(auto schema, ReadSchema(std::move(stream)));
  EXPECT_THAT(schema->field_names(), ::testing::ElementsAre("operation", "snapshot_id"));

  ICEBERG_UNWRAP_OR_FAIL(auto null_builder, snapshots_table->NewScan());
  EXPECT_THAT(null_builder->Project(nullptr).Build(),
              IsError(ErrorKind::kInvalidArgument));
}

TEST_F(MetadataTableTest, ScanBuilderRejectsConflictingSnapshotOptions) {
  ICEBERG_UNWRAP_OR_FAIL(auto table, MetadataTable::Make<SnapshotsTable>(table_));

  ICEBERG_UNWRAP_OR_FAIL(auto snapshot_builder, table->NewScan());
  auto snapshot_result = snapshot_builder->UseSnapshot(1).AsOfTime(2).Build();
  EXPECT_THAT(snapshot_result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(snapshot_result, HasErrorMessage("Cannot override snapshot"));

  ICEBERG_UNWRAP_OR_FAIL(auto ref_builder, table->NewScan());
  auto ref_result = ref_builder->UseRef("dev").UseRef("main").Build();
  EXPECT_THAT(ref_result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(ref_result, HasErrorMessage("Cannot override ref"));

  ICEBERG_UNWRAP_OR_FAIL(auto empty_ref_builder, table->NewScan());
  EXPECT_THAT(empty_ref_builder->UseRef("").Build(),
              IsError(ErrorKind::kValidationFailed));
}

TEST_F(MetadataTableTest, HistoryTableScanAndTimeTravelSelection) {
  ICEBERG_UNWRAP_OR_FAIL(auto history_table, MetadataTable::Make<HistoryTable>(table_));
  ICEBERG_UNWRAP_OR_FAIL(auto builder, history_table->NewScan());
  ICEBERG_UNWRAP_OR_FAIL(auto stream, BuildStream(builder->Select({"snapshot_id"})));
  ICEBERG_UNWRAP_OR_FAIL(auto schema, ReadSchema(std::move(stream)));
  EXPECT_THAT(schema->field_names(), ::testing::ElementsAre("snapshot_id"));

  // Tables without time travel reject snapshot selections.
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot_builder, history_table->NewScan());
  auto snapshot_result = snapshot_builder->UseSnapshot(1).Build();
  EXPECT_THAT(snapshot_result, IsError(ErrorKind::kNotSupported));
  EXPECT_THAT(snapshot_result, HasErrorMessage("does not support time travel"));
}

TEST_F(MetadataTableTest, ScanReadsEveryTaskInPlanningOrder) {
  auto projection = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(2, "snapshot_id", int64())});
  auto metadata = std::make_shared<TableMetadata>();
  metadata->snapshots = {std::make_shared<Snapshot>(Snapshot{.snapshot_id = 2})};
  ICEBERG_UNWRAP_OR_FAIL(
      auto first,
      StaticMetadataTask::Make(MetadataTableKind::kSnapshots, metadata, projection));
  auto next_metadata = std::make_shared<TableMetadata>();
  next_metadata->snapshots = {std::make_shared<Snapshot>(Snapshot{.snapshot_id = 1})};
  ICEBERG_UNWRAP_OR_FAIL(
      auto second,
      StaticMetadataTask::Make(MetadataTableKind::kSnapshots, next_metadata, projection));
  ICEBERG_UNWRAP_OR_FAIL(auto empty, StaticMetadataTask::Make(
                                         MetadataTableKind::kSnapshots,
                                         std::make_shared<TableMetadata>(), projection));
  PlannedMetadataScan scan({.kind = MetadataTableKind::kSnapshots,
                            .table_metadata = metadata,
                            .schema = SnapshotsTable::CanonicalSchema(),
                            .projected_schema = projection},
                           {empty, first, empty, second});
  ICEBERG_UNWRAP_OR_FAIL(auto stream, scan.ToArrowArrayStream());
  ICEBERG_UNWRAP_OR_FAIL(auto actual, ReadArrowTable(std::move(stream)));
  ICEBERG_UNWRAP_OR_FAIL(
      auto expected,
      TableFromJson(*projection, R"([{"snapshot_id":2},{"snapshot_id":1}])"));
  EXPECT_TRUE(TablesEqual(*expected, *actual));
}

}  // namespace iceberg
