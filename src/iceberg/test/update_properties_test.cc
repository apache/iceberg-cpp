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

#include "iceberg/update/update_properties.h"

#include <memory>
#include <string>
#include <unordered_map>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/file_format.h"
#include "iceberg/partition_spec.h"
#include "iceberg/snapshot.h"
#include "iceberg/sort_order.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_catalog.h"

namespace iceberg {

namespace {

std::shared_ptr<TableMetadata> MakeBaseMetadata(
    std::unordered_map<std::string, std::string> properties) {
  auto metadata = std::make_shared<TableMetadata>();
  metadata->format_version = 2;
  metadata->table_uuid = "test-uuid";
  metadata->location = "s3://bucket/table";
  metadata->last_sequence_number = TableMetadata::kInitialSequenceNumber;
  metadata->last_updated_ms = TimePointMs{};
  metadata->last_column_id = 0;
  metadata->default_spec_id = PartitionSpec::kInitialSpecId;
  metadata->last_partition_id = 0;
  metadata->default_sort_order_id = SortOrder::kInitialSortOrderId;
  metadata->current_snapshot_id = Snapshot::kInvalidSnapshotId;
  metadata->next_row_id = TableMetadata::kInitialRowId;
  metadata->properties = std::move(properties);
  return metadata;
}

TableIdentifier MakeIdentifier() {
  return TableIdentifier{.ns = Namespace{{"ns"}}, .name = "tbl"};
}

}  // namespace

using ::testing::_;
using ::testing::ByMove;
using ::testing::Return;

TEST(UpdatePropertiesTest, ApplyMergesUpdatesAndRemovals) {
  auto metadata =
      MakeBaseMetadata({{"foo", "bar"}, {"keep", "yes"}, {"format-version", "2"}});
  Table table(MakeIdentifier(), metadata, "loc", /*io=*/nullptr, /*catalog=*/nullptr);

  auto updater = table.UpdateProperties();
  updater->Set("foo", "baz").Remove("keep").DefaultFormat(FileFormatType::kOrc);

  auto applied = updater->Apply();
  ASSERT_THAT(applied, IsOk());

  const auto& props = *applied;
  EXPECT_EQ(props.at("foo"), "baz");
  EXPECT_FALSE(props.contains("keep"));
  EXPECT_EQ(props.at(TableProperties::kDefaultFileFormat.key()), "orc");
}

TEST(UpdatePropertiesTest, CommitUsesCatalogAndRefreshesTable) {
  auto catalog = std::make_shared<MockCatalog>();
  auto base_metadata = MakeBaseMetadata({{"foo", "bar"}});
  Table table(MakeIdentifier(), base_metadata, "loc", /*io=*/nullptr, catalog);

  auto updated_metadata = MakeBaseMetadata({{"foo", "new"}});  // response metadata

  EXPECT_CALL(*catalog, LoadTable(table.name()))
      .WillOnce(Return(ByMove(Result<std::unique_ptr<Table>>{std::make_unique<Table>(
          table.name(), MakeBaseMetadata({{"foo", "bar"}}), "loc", nullptr, catalog)})));

  EXPECT_CALL(*catalog, UpdateTable(table.name(), _, _))
      .WillOnce(Return(ByMove(Result<std::unique_ptr<Table>>{std::make_unique<Table>(
          table.name(), updated_metadata, "loc2", nullptr, catalog)})));

  auto updater = table.UpdateProperties();
  updater->Set("foo", "new");

  EXPECT_THAT(updater->Commit(), IsOk());
  EXPECT_EQ(table.properties().configs().at("foo"), "new");
  EXPECT_EQ(table.location(), "s3://bucket/table");
}

}  // namespace iceberg
