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

TEST_F(MetadataTableTest, ScanBuilderDefaultsToAllColumns) {
  ICEBERG_UNWRAP_OR_FAIL(auto snapshots_table,
                         MetadataTable::Make<SnapshotsTable>(table_));
  ICEBERG_UNWRAP_OR_FAIL(auto builder, snapshots_table->NewScan());
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
  EXPECT_EQ(scan->schema(), snapshots_table->schema());
}

TEST_F(MetadataTableTest, ScanBuilderSelectResolvesNamesImmediately) {
  ICEBERG_UNWRAP_OR_FAIL(auto snapshots_table,
                         MetadataTable::Make<SnapshotsTable>(table_));

  // Select() uses the case sensitivity in effect when it is called.
  ICEBERG_UNWRAP_OR_FAIL(auto builder, snapshots_table->NewScan());
  ICEBERG_UNWRAP_OR_FAIL(auto scan,
                         builder->Select({"SNAPSHOT_ID"}).CaseSensitive(false).Build());
  EXPECT_TRUE(scan->schema()->fields().empty());

  ICEBERG_UNWRAP_OR_FAIL(auto insensitive_builder, snapshots_table->NewScan());
  ICEBERG_UNWRAP_OR_FAIL(
      auto insensitive_scan,
      insensitive_builder->CaseSensitive(false).Select({"SNAPSHOT_ID"}).Build());
  ASSERT_EQ(insensitive_scan->schema()->fields().size(), 1);
  EXPECT_EQ(insensitive_scan->schema()->fields()[0].name(), "snapshot_id");

  // Later builder changes do not affect a built scan.
  insensitive_builder->Select({"operation"});
  ASSERT_EQ(insensitive_scan->schema()->fields().size(), 1);
  EXPECT_EQ(insensitive_scan->schema()->fields()[0].name(), "snapshot_id");

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

  ICEBERG_UNWRAP_OR_FAIL(auto builder, snapshots_table->NewScan());
  ICEBERG_UNWRAP_OR_FAIL(auto scan,
                         builder->Select({"snapshot_id"}).Project(projection).Build());
  EXPECT_EQ(scan->schema(), projection);

  ICEBERG_UNWRAP_OR_FAIL(auto null_builder, snapshots_table->NewScan());
  EXPECT_THAT(null_builder->Project(nullptr).Build(),
              IsError(ErrorKind::kInvalidArgument));
}

TEST_F(MetadataTableTest, HistoryTableScan) {
  ICEBERG_UNWRAP_OR_FAIL(auto history_table, MetadataTable::Make<HistoryTable>(table_));

  // Column selection builds, but the history table does not return rows yet.
  ICEBERG_UNWRAP_OR_FAIL(auto builder, history_table->NewScan());
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Select({"snapshot_id"}).Build());
  ASSERT_EQ(scan->schema()->fields().size(), 1);
  EXPECT_EQ(scan->schema()->fields()[0].name(), "snapshot_id");
  EXPECT_THAT(scan->ToArrowArrayStream(), IsError(ErrorKind::kNotSupported));
}

}  // namespace iceberg
