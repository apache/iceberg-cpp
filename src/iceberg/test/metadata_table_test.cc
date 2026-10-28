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
#include <variant>
#include <vector>

#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/constants.h"
#include "iceberg/inspect/history_table.h"
#include "iceberg/inspect/snapshots_table.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/table.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_catalog.h"
#include "iceberg/test/mock_io.h"
#include "iceberg/type.h"
#include "iceberg/util/timepoint.h"

namespace iceberg {
namespace {

/// \brief Time-travel table that records the arguments of each scan.
class RecordingTimeTravelTable : public TimeTravelMetadataTable {
 public:
  explicit RecordingTimeTravelTable(std::shared_ptr<Table> table)
      : TimeTravelMetadataTable(std::move(table)) {}

  Kind kind() const noexcept override { return Kind::kSnapshots; }

  const std::shared_ptr<Schema>& schema() const override {
    static const auto schema = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64()),
                                 SchemaField::MakeOptional(2, "name", string())});
    return schema;
  }

  const SnapshotSelection& scanned_snapshot() const { return scanned_snapshot_; }
  const std::vector<std::string>& scanned_columns() const { return scanned_columns_; }

 protected:
  Result<ArrowArrayStream> ScanSnapshot(const SnapshotSelection& snapshot_selection,
                                        const Schema& projected_schema) const override {
    scanned_snapshot_ = snapshot_selection;
    scanned_columns_.clear();
    for (const auto& field : projected_schema.fields()) {
      scanned_columns_.emplace_back(field.name());
    }
    return NotSupported("Recording table does not return rows");
  }

 private:
  mutable SnapshotSelection scanned_snapshot_;
  mutable std::vector<std::string> scanned_columns_;
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
  ICEBERG_UNWRAP_OR_FAIL(auto stream,
                         builder->Select({"SNAPSHOT_ID"}).CaseSensitive(false).Build());
  ICEBERG_UNWRAP_OR_FAIL(auto schema, ReadSchema(std::move(stream)));
  EXPECT_EQ(schema->num_fields(), 0);

  ICEBERG_UNWRAP_OR_FAIL(auto insensitive_builder, snapshots_table->NewScan());
  ICEBERG_UNWRAP_OR_FAIL(
      auto insensitive_stream,
      insensitive_builder->CaseSensitive(false).Select({"SNAPSHOT_ID"}).Build());
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
  ICEBERG_UNWRAP_OR_FAIL(auto stream,
                         builder->Select({"snapshot_id"}).Project(projection).Build());
  ICEBERG_UNWRAP_OR_FAIL(auto schema, ReadSchema(std::move(stream)));
  EXPECT_THAT(schema->field_names(), ::testing::ElementsAre("operation", "snapshot_id"));

  ICEBERG_UNWRAP_OR_FAIL(auto null_builder, snapshots_table->NewScan());
  EXPECT_THAT(null_builder->Project(nullptr).Build(),
              IsError(ErrorKind::kInvalidArgument));
}

TEST_F(MetadataTableTest, SupportsTimeTravel) {
  ICEBERG_UNWRAP_OR_FAIL(auto snapshots_table,
                         MetadataTable::Make<SnapshotsTable>(table_));
  EXPECT_FALSE(snapshots_table->supports_time_travel());

  ICEBERG_UNWRAP_OR_FAIL(auto history_table, MetadataTable::Make<HistoryTable>(table_));
  EXPECT_FALSE(history_table->supports_time_travel());

  RecordingTimeTravelTable time_travel_table(table_);
  EXPECT_TRUE(time_travel_table.supports_time_travel());
}

TEST_F(MetadataTableTest, TimeTravelScanDefaults) {
  RecordingTimeTravelTable table(table_);
  ICEBERG_UNWRAP_OR_FAIL(auto builder, table.NewScan());
  EXPECT_THAT(builder->Build(), IsError(ErrorKind::kNotSupported));
  EXPECT_TRUE(std::holds_alternative<std::monostate>(table.scanned_snapshot().snapshot));
  EXPECT_TRUE(table.scanned_snapshot().ref_name.empty());
  EXPECT_THAT(table.scanned_columns(), ::testing::ElementsAre("id", "name"));
}

TEST_F(MetadataTableTest, TimeTravelScanSelectsColumnsAndSnapshot) {
  RecordingTimeTravelTable table(table_);
  ICEBERG_UNWRAP_OR_FAIL(auto builder, table.NewScan());
  EXPECT_THAT(builder->Select({"name"}).UseSnapshot(1).UseRef("dev").Build(),
              IsError(ErrorKind::kNotSupported));
  ASSERT_TRUE(std::holds_alternative<int64_t>(table.scanned_snapshot().snapshot));
  EXPECT_EQ(std::get<int64_t>(table.scanned_snapshot().snapshot), 1);
  EXPECT_EQ(table.scanned_snapshot().ref_name, "dev");
  EXPECT_THAT(table.scanned_columns(), ::testing::ElementsAre("name"));
}

TEST_F(MetadataTableTest, TimeTravelScanAsOfTime) {
  RecordingTimeTravelTable table(table_);
  ICEBERG_UNWRAP_OR_FAIL(auto builder, table.NewScan());
  EXPECT_THAT(builder->AsOfTime(1234567890000).Build(),
              IsError(ErrorKind::kNotSupported));
  ASSERT_TRUE(std::holds_alternative<TimePointMs>(table.scanned_snapshot().snapshot));
  EXPECT_EQ(std::get<TimePointMs>(table.scanned_snapshot().snapshot),
            TimePointMsFromUnixMs(1234567890000));
  EXPECT_TRUE(table.scanned_snapshot().ref_name.empty());
  EXPECT_THAT(table.scanned_columns(), ::testing::ElementsAre("id", "name"));
}

TEST_F(MetadataTableTest, ScanBuilderRejectsConflictingSnapshotOptions) {
  RecordingTimeTravelTable table(table_);

  ICEBERG_UNWRAP_OR_FAIL(auto snapshot_builder, table.NewScan());
  auto snapshot_result = snapshot_builder->UseSnapshot(1).AsOfTime(2).Build();
  EXPECT_THAT(snapshot_result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(snapshot_result, HasErrorMessage("Cannot override snapshot"));

  ICEBERG_UNWRAP_OR_FAIL(auto ref_builder, table.NewScan());
  auto ref_result = ref_builder->UseRef("dev").UseRef("main").Build();
  EXPECT_THAT(ref_result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(ref_result, HasErrorMessage("Cannot override ref"));

  ICEBERG_UNWRAP_OR_FAIL(auto empty_ref_builder, table.NewScan());
  EXPECT_THAT(empty_ref_builder->UseRef("").Build(),
              IsError(ErrorKind::kValidationFailed));
}

TEST_F(MetadataTableTest, HistoryTableScanIsNotSupported) {
  ICEBERG_UNWRAP_OR_FAIL(auto history_table, MetadataTable::Make<HistoryTable>(table_));
  ICEBERG_UNWRAP_OR_FAIL(auto builder, history_table->NewScan());
  EXPECT_THAT(builder->Select({"snapshot_id"}).Build(),
              IsError(ErrorKind::kNotSupported));

  // Tables without time travel reject snapshot selections.
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot_builder, history_table->NewScan());
  auto snapshot_result = snapshot_builder->UseSnapshot(1).Build();
  EXPECT_THAT(snapshot_result, IsError(ErrorKind::kNotSupported));
  EXPECT_THAT(snapshot_result, HasErrorMessage("does not support time travel"));
}

}  // namespace iceberg
