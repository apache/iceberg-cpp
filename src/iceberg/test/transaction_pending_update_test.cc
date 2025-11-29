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

#include <gtest/gtest.h>

#include "iceberg/base_transaction.h"
#include "iceberg/partition_spec.h"
#include "iceberg/pending_update.h"
#include "iceberg/sort_order.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_update.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_catalog.h"

namespace iceberg {
namespace {
using ::testing::ElementsAre;
using ::testing::NiceMock;

TableIdentifier MakeIdentifier() {
  return TableIdentifier{
      .ns = Namespace{.levels = {"test_ns"}},
      .name = "test_table",
  };
}

std::shared_ptr<TableMetadata> CreateBaseMetadata() {
  auto metadata = std::make_shared<TableMetadata>();
  metadata->format_version = TableMetadata::kDefaultTableFormatVersion;
  metadata->table_uuid = "test-uuid";
  metadata->location = "s3://bucket/table";
  metadata->last_sequence_number = TableMetadata::kInitialSequenceNumber;
  metadata->last_updated_ms =
      TimePointMs{std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())};
  metadata->last_column_id = 0;
  metadata->default_spec_id = PartitionSpec::kInitialSpecId;
  metadata->last_partition_id = 0;
  metadata->current_snapshot_id = Snapshot::kInvalidSnapshotId;
  metadata->default_sort_order_id = SortOrder::kInitialSortOrderId;
  metadata->next_row_id = TableMetadata::kInitialRowId;
  metadata->properties = {{"existing", "value"}};
  return metadata;
}

std::shared_ptr<Table> CreateTestTable(const TableIdentifier& identifier,
                                       const std::shared_ptr<TableMetadata>& metadata,
                                       const std::shared_ptr<Catalog>& catalog) {
  return std::make_shared<Table>(identifier, metadata, "s3://bucket/table/metadata.json",
                                 nullptr, catalog);
}
}  // namespace

TEST(TransactionPendingUpdateTest, CommitSetPropertiesUsesCatalog) {
  auto metadata = CreateBaseMetadata();
  const auto identifier = MakeIdentifier();
  auto catalog = std::make_shared<NiceMock<MockCatalog>>();
  auto table =
      CreateTestTable(identifier, std::make_shared<TableMetadata>(*metadata), catalog);
  auto transaction = table->NewTransaction();
  auto update_properties = transaction->UpdateProperties();
  update_properties->Set("new-key", "new-value");

  EXPECT_CALL(*catalog,
              UpdateTable(::testing::Eq(identifier), ::testing::_, ::testing::_))
      .WillOnce([](const TableIdentifier& id,
                   const std::vector<std::unique_ptr<TableRequirement>>& /*requirements*/,
                   const std::vector<std::unique_ptr<TableUpdate>>& updates)
                    -> Result<std::unique_ptr<Table>> {
        EXPECT_EQ("test_table", id.name);
        EXPECT_EQ(1u, updates.size());
        const auto* set_update =
            dynamic_cast<const table::SetProperties*>(updates.front().get());
        EXPECT_NE(set_update, nullptr);
        const auto& updated = set_update->updated();
        auto it = updated.find("new-key");
        EXPECT_NE(it, updated.end());
        EXPECT_EQ("new-value", it->second);
        return Result<std::unique_ptr<Table>>(std::unique_ptr<Table>());
      });

  EXPECT_THAT(transaction->CommitTransaction(), IsOk());
}

TEST(TransactionPendingUpdateTest, RemovePropertiesSkipsMissingKeys) {
  auto metadata = CreateBaseMetadata();
  const auto identifier = MakeIdentifier();
  auto catalog = std::make_shared<NiceMock<MockCatalog>>();
  auto table =
      CreateTestTable(identifier, std::make_shared<TableMetadata>(*metadata), catalog);
  auto transaction = table->NewTransaction();

  auto update_properties = transaction->UpdateProperties();
  update_properties->Remove("missing").Remove("existing");

  EXPECT_CALL(*catalog,
              UpdateTable(::testing::Eq(identifier), ::testing::_, ::testing::_))
      .WillOnce([](const TableIdentifier&,
                   const std::vector<std::unique_ptr<TableRequirement>>& /*requirements*/,
                   const std::vector<std::unique_ptr<TableUpdate>>& updates)
                    -> Result<std::unique_ptr<Table>> {
        EXPECT_EQ(1u, updates.size());
        const auto* remove_update =
            dynamic_cast<const table::RemoveProperties*>(updates.front().get());
        EXPECT_NE(remove_update, nullptr);
        EXPECT_THAT(remove_update->removed(), ElementsAre("existing"));
        return Result<std::unique_ptr<Table>>(std::unique_ptr<Table>());
      });

  EXPECT_THAT(transaction->CommitTransaction(), IsOk());
}

TEST(TransactionPendingUpdateTest, AggregatesMultiplePendingUpdates) {
  auto metadata = CreateBaseMetadata();
  const auto identifier = MakeIdentifier();
  auto catalog = std::make_shared<NiceMock<MockCatalog>>();
  auto table =
      CreateTestTable(identifier, std::make_shared<TableMetadata>(*metadata), catalog);
  auto transaction = table->NewTransaction();

  auto update_properties = transaction->UpdateProperties();
  update_properties->Set("new-key", "new-value").Remove("existing");

  EXPECT_CALL(*catalog,
              UpdateTable(::testing::Eq(identifier), ::testing::_, ::testing::_))
      .WillOnce([](const TableIdentifier&,
                   const std::vector<std::unique_ptr<TableRequirement>>& /*requirements*/,
                   const std::vector<std::unique_ptr<TableUpdate>>& updates)
                    -> Result<std::unique_ptr<Table>> {
        EXPECT_EQ(2u, updates.size());

        const auto* set_update =
            dynamic_cast<const table::SetProperties*>(updates[0].get());
        EXPECT_NE(set_update, nullptr);
        const auto& updated = set_update->updated();
        auto it = updated.find("new-key");
        EXPECT_NE(it, updated.end());
        EXPECT_EQ("new-value", it->second);

        const auto* remove_update =
            dynamic_cast<const table::RemoveProperties*>(updates[1].get());
        EXPECT_NE(remove_update, nullptr);
        EXPECT_THAT(remove_update->removed(), ElementsAre("existing"));

        return Result<std::unique_ptr<Table>>(std::unique_ptr<Table>());
      });

  EXPECT_THAT(transaction->CommitTransaction(), IsOk());
}

}  // namespace iceberg
