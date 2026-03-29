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

#include "iceberg/inspect/metadata_table_factory.h"
#include "iceberg/inspect/snapshots_table.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/table.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_catalog.h"
#include "iceberg/test/mock_io.h"

namespace iceberg {

class MetadataTableTest : public ::testing::Test {
 protected:
  void SetUp() override {
    io_ = std::make_shared<MockFileIO>();
    catalog_ = std::make_shared<MockCatalog>();

    auto schema = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64()),
                                 SchemaField::MakeOptional(2, "name", string())},
        1);
    metadata_ = std::make_shared<TableMetadata>(
        TableMetadata{.format_version = 2, .schemas = {schema}, .current_schema_id = 1});

    TableIdentifier source_ident{.ns = Namespace{.levels = {"db"}},
                                 .name = "source_table"};
    auto source_table_result =
        Table::Make(source_ident, metadata_, "s3://bucket/meta.json", io_, catalog_);
    EXPECT_THAT(source_table_result, IsOk());
    source_table_ = *source_table_result;

    auto snapshots_table_result = MetadataTableFactory::GetSnapshotsTable(source_table_);
    EXPECT_THAT(snapshots_table_result, IsOk());
    snapshots_table_ = *snapshots_table_result;
  }

  std::shared_ptr<MockFileIO> io_;
  std::shared_ptr<MockCatalog> catalog_;
  std::shared_ptr<TableMetadata> metadata_;
  std::shared_ptr<Table> source_table_;
  std::shared_ptr<SnapshotsTable> snapshots_table_;
};

TEST_F(MetadataTableTest, Constructor) {
  EXPECT_EQ(snapshots_table_->name().name, "source_table.snapshots");
  EXPECT_FALSE(snapshots_table_->uuid().empty());
  auto schema_result = snapshots_table_->schema();
  EXPECT_THAT(schema_result, IsOk());
  EXPECT_EQ((*schema_result)->schema_id(), 1);
}

TEST_F(MetadataTableTest, DelegatesToSourceTable) {
  EXPECT_EQ(snapshots_table_->location(), source_table_->location());
  EXPECT_EQ(snapshots_table_->last_updated_ms(), source_table_->last_updated_ms());
  EXPECT_EQ(snapshots_table_->metadata(), source_table_->metadata());
  EXPECT_EQ(snapshots_table_->catalog(), source_table_->catalog());
}

TEST_F(MetadataTableTest, NotSupportedOperations) {
  EXPECT_THAT(snapshots_table_->Refresh(), HasErrorMessage("Cannot"));
  EXPECT_THAT(snapshots_table_->NewTransaction(), HasErrorMessage("Cannot"));
  EXPECT_THAT(snapshots_table_->NewUpdateProperties(), HasErrorMessage("Cannot"));
  EXPECT_THAT(snapshots_table_->NewUpdateSchema(), HasErrorMessage("Cannot"));
  EXPECT_THAT(snapshots_table_->NewUpdateLocation(), HasErrorMessage("Cannot"));
  EXPECT_THAT(snapshots_table_->NewUpdatePartitionSpec(), HasErrorMessage("Cannot"));
  EXPECT_THAT(snapshots_table_->NewUpdateSortOrder(), HasErrorMessage("Cannot"));
  EXPECT_THAT(snapshots_table_->NewExpireSnapshots(), HasErrorMessage("Cannot"));
}

TEST_F(MetadataTableTest, SchemasAndSpecs) {
  auto schemas_result = snapshots_table_->schemas();
  EXPECT_THAT(schemas_result, IsOk());
  EXPECT_EQ(schemas_result->get().size(), 1);
  EXPECT_EQ(schemas_result->get().at(1)->schema_id(), 1);

  auto spec_result = snapshots_table_->spec();
  EXPECT_THAT(spec_result, IsOk());
  EXPECT_EQ(*spec_result, PartitionSpec::Unpartitioned());

  auto specs_result = snapshots_table_->specs();
  EXPECT_THAT(specs_result, IsOk());
  EXPECT_EQ(specs_result->get().size(), 1);
}

TEST_F(MetadataTableTest, SortOrders) {
  auto sort_order_result = snapshots_table_->sort_order();
  EXPECT_THAT(sort_order_result, IsOk());
  EXPECT_EQ(*sort_order_result, SortOrder::Unsorted());

  auto sort_orders_result = snapshots_table_->sort_orders();
  EXPECT_THAT(sort_orders_result, IsOk());
  EXPECT_EQ(sort_orders_result->get().size(), 1);
}

TEST_F(MetadataTableTest, Properties) {
  EXPECT_EQ(snapshots_table_->properties().configs().size(), 0);
}

TEST_F(MetadataTableTest, Snapshots) {
  // Assuming source table has no current snapshot
  auto cur_snapshot_result = snapshots_table_->current_snapshot();
  EXPECT_THAT(cur_snapshot_result, IsError(ErrorKind::kNotFound));
  auto snapshot_result = snapshots_table_->SnapshotById(1);
  EXPECT_THAT(snapshot_result, IsError(ErrorKind::kNotFound));
  EXPECT_TRUE(snapshots_table_->snapshots().empty());
}

TEST_F(MetadataTableTest, History) { EXPECT_TRUE(snapshots_table_->history().empty()); }

TEST_F(MetadataTableTest, LocationProvider) {
  auto lp_result = snapshots_table_->location_provider();
  EXPECT_THAT(lp_result, IsOk());
}

}  // namespace iceberg
