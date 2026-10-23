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

TEST_F(MetadataTableTest, TimeTravelScanSelectsColumns) {
  RecordingTimeTravelTable table(table_);
  const ColumnSelection name_only{.names = std::vector<std::string>{"name"}};

  EXPECT_THAT(table.Scan(), IsError(ErrorKind::kNotSupported));
  EXPECT_TRUE(std::holds_alternative<std::monostate>(table.scanned_snapshot().snapshot));
  EXPECT_THAT(table.scanned_columns(), ::testing::ElementsAre("id", "name"));

  EXPECT_THAT(table.ScanColumns(name_only), IsError(ErrorKind::kNotSupported));
  EXPECT_TRUE(std::holds_alternative<std::monostate>(table.scanned_snapshot().snapshot));
  EXPECT_THAT(table.scanned_columns(), ::testing::ElementsAre("name"));

  const SnapshotSelection snapshot_one{.snapshot = int64_t{1}, .ref_name = "dev"};
  EXPECT_THAT(table.Scan(snapshot_one), IsError(ErrorKind::kNotSupported));
  EXPECT_EQ(table.scanned_snapshot().snapshot, snapshot_one.snapshot);
  EXPECT_EQ(table.scanned_snapshot().ref_name, "dev");
  EXPECT_THAT(table.scanned_columns(), ::testing::ElementsAre("id", "name"));

  EXPECT_THAT(table.ScanColumns(snapshot_one, name_only),
              IsError(ErrorKind::kNotSupported));
  EXPECT_EQ(table.scanned_snapshot().snapshot, snapshot_one.snapshot);
  EXPECT_THAT(table.scanned_columns(), ::testing::ElementsAre("name"));

  EXPECT_THAT(table.Scan({}), IsError(ErrorKind::kNotSupported));
  EXPECT_TRUE(std::holds_alternative<std::monostate>(table.scanned_snapshot().snapshot));
  EXPECT_THAT(table.scanned_columns(), ::testing::ElementsAre("id", "name"));
}

TEST_F(MetadataTableTest, HistoryTableScanWithColumnsIsNotSupported) {
  ICEBERG_UNWRAP_OR_FAIL(auto history_table, MetadataTable::Make<HistoryTable>(table_));
  EXPECT_THAT(history_table->ScanColumns(
                  ColumnSelection{.names = std::vector<std::string>{"snapshot_id"}}),
              IsError(ErrorKind::kNotSupported));
}

}  // namespace iceberg
