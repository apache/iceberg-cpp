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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

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
        TableMetadata{.format_version = 2, .schemas = {schema}, .current_schema_id = 1});

    TableIdentifier ident{.ns = Namespace{.levels = {"db"}}, .name = "source_table"};
    ICEBERG_UNWRAP_OR_FAIL(table_, Table::Make(ident, metadata, "s3://bucket/meta.json",
                                               std::make_shared<MockFileIO>(),
                                               std::make_shared<MockCatalog>()));
  }

  std::shared_ptr<Table> table_;
};

TEST_F(MetadataTableTest, FactoryRejectsNullSourceTable) {
  auto result = MetadataTable::Make(nullptr, MetadataTable::Kind::kSnapshots);
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result, HasErrorMessage("Table cannot be null"));
}

TEST_F(MetadataTableTest, SupportsTimeTravel) {
  ICEBERG_UNWRAP_OR_FAIL(auto snapshots_table,
                         MetadataTable::Make(table_, MetadataTable::Kind::kSnapshots));
  EXPECT_FALSE(snapshots_table->supports_time_travel());

  ICEBERG_UNWRAP_OR_FAIL(auto history_table,
                         MetadataTable::Make(table_, MetadataTable::Kind::kHistory));
  EXPECT_FALSE(history_table->supports_time_travel());
}

}  // namespace iceberg
