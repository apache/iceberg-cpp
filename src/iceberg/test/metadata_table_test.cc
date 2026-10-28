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
                                        const Schema& projected_schema) override {
    scanned_snapshot_ = snapshot_selection;
    scanned_columns_.clear();
    for (const auto& field : projected_schema.fields()) {
      scanned_columns_.emplace_back(field.name());
    }
    return NotSupported("Recording table does not return rows");
  }

 private:
  SnapshotSelection scanned_snapshot_;
  std::vector<std::string> scanned_columns_;
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

  std::shared_ptr<Table> table_;
};

TEST_F(MetadataTableTest, FactoryRejectsNullSourceTable) {
  auto result = MetadataTable::Make<SnapshotsTable>(nullptr);
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result, HasErrorMessage("Table cannot be null"));
}

TEST_F(MetadataTableTest, SupportsTimeTravel) {
  ICEBERG_UNWRAP_OR_FAIL(auto snapshots_table,
                         MetadataTable::Make<SnapshotsTable>(table_));
  EXPECT_FALSE(snapshots_table->supports_time_travel());

  ICEBERG_UNWRAP_OR_FAIL(auto history_table, MetadataTable::Make<HistoryTable>(table_));
  EXPECT_FALSE(history_table->supports_time_travel());
}

TEST_F(MetadataTableTest, TimeTravelScanDefaults) {
  RecordingTimeTravelTable table(table_);
  ICEBERG_UNWRAP_OR_FAIL(auto builder, table.NewScan());
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
  EXPECT_EQ(scan->schema(), table.schema());
  EXPECT_TRUE(scan->snapshot_selection().IsCurrentSnapshot());

  EXPECT_THAT(scan->ToArrowArrayStream(), IsError(ErrorKind::kNotSupported));
  EXPECT_TRUE(table.scanned_snapshot().IsCurrentSnapshot());
  EXPECT_THAT(table.scanned_columns(), ::testing::ElementsAre("id", "name"));
}

TEST_F(MetadataTableTest, TimeTravelScanSelectsColumnsAndSnapshot) {
  RecordingTimeTravelTable table(table_);
  ICEBERG_UNWRAP_OR_FAIL(auto builder, table.NewScan());
  ICEBERG_UNWRAP_OR_FAIL(auto scan,
                         builder->Select({"name"}).UseSnapshot(1).UseRef("dev").Build());
  ASSERT_EQ(scan->schema()->fields().size(), 1);
  EXPECT_EQ(scan->schema()->fields()[0].name(), "name");
  EXPECT_EQ(scan->snapshot_selection().snapshot,
            (std::variant<std::monostate, int64_t, TimePointMs>{int64_t{1}}));
  EXPECT_EQ(scan->snapshot_selection().ref_name, "dev");

  EXPECT_THAT(scan->ToArrowArrayStream(), IsError(ErrorKind::kNotSupported));
  EXPECT_EQ(table.scanned_snapshot().snapshot, scan->snapshot_selection().snapshot);
  EXPECT_EQ(table.scanned_snapshot().ref_name, "dev");
  EXPECT_THAT(table.scanned_columns(), ::testing::ElementsAre("name"));
}

TEST_F(MetadataTableTest, TimeTravelScanAsOfTime) {
  RecordingTimeTravelTable table(table_);
  ICEBERG_UNWRAP_OR_FAIL(auto builder, table.NewScan());
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->AsOfTime(1234567890000).Build());

  EXPECT_THAT(scan->ToArrowArrayStream(), IsError(ErrorKind::kNotSupported));
  ASSERT_TRUE(std::holds_alternative<TimePointMs>(table.scanned_snapshot().snapshot));
  EXPECT_EQ(std::get<TimePointMs>(table.scanned_snapshot().snapshot),
            TimePointMsFromUnixMs(1234567890000));
  EXPECT_TRUE(table.scanned_snapshot().ref_name.empty());
  EXPECT_THAT(table.scanned_columns(), ::testing::ElementsAre("id", "name"));
}

TEST_F(MetadataTableTest, ScanBuilderRejectsConflictingOptions) {
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

TEST_F(MetadataTableTest, HistoryTableScan) {
  ICEBERG_UNWRAP_OR_FAIL(auto history_table, MetadataTable::Make<HistoryTable>(table_));

  // Column selection builds, but the history table does not return rows yet.
  ICEBERG_UNWRAP_OR_FAIL(auto builder, history_table->NewScan());
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Select({"snapshot_id"}).Build());
  ASSERT_EQ(scan->schema()->fields().size(), 1);
  EXPECT_EQ(scan->schema()->fields()[0].name(), "snapshot_id");
  EXPECT_THAT(scan->ToArrowArrayStream(), IsError(ErrorKind::kNotSupported));

  // Tables without time travel reject snapshot selections when the scan is built.
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot_builder, history_table->NewScan());
  EXPECT_THAT(snapshot_builder->UseSnapshot(1).Build(),
              IsError(ErrorKind::kNotSupported));
  ICEBERG_UNWRAP_OR_FAIL(auto time_builder, history_table->NewScan());
  EXPECT_THAT(time_builder->AsOfTime(1).Build(), IsError(ErrorKind::kNotSupported));
}

}  // namespace iceberg
