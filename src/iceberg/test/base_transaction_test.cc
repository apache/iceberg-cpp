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

#include "iceberg/base_transaction.h"

#include <gtest/gtest.h>

#include "iceberg/partition_spec.h"
#include "iceberg/result.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_update.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_catalog.h"
#include "iceberg/update/update_properties.h"

namespace iceberg {
namespace {
std::shared_ptr<Table> CreateTestTable(const TableIdentifier& identifier,
                                       const std::shared_ptr<TableMetadata>& metadata,
                                       const std::shared_ptr<Catalog>& catalog) {
  return std::make_shared<Table>(identifier, metadata, "s3://bucket/table/metadata.json",
                                 nullptr, catalog);
}
}  // namespace

class BaseTransactionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create catalog and table identifier
    catalog_ = std::make_shared<::testing::NiceMock<MockCatalog>>();

    identifier_ = TableIdentifier(Namespace({"test"}), "test_table");
    auto metadata = std::make_shared<TableMetadata>();
    table_ =
        std::make_shared<Table>(identifier_, std::move(metadata),
                                "s3://bucket/table/metadata.json", nullptr, catalog_);
  }

  TableIdentifier identifier_;
  std::shared_ptr<MockCatalog> catalog_;
  std::shared_ptr<Table> table_;
};

TEST_F(BaseTransactionTest, CommitSetPropertiesUsesCatalog) {
  auto transaction = table_->NewTransaction();
  auto update_properties = transaction->UpdateProperties();
  update_properties->Set("new-key", "new-value");
  EXPECT_THAT(update_properties->Commit(), IsOk());

  EXPECT_CALL(*catalog_,
              UpdateTable(::testing::Eq(identifier_), ::testing::_, ::testing::_))
      .WillOnce([](const TableIdentifier& id,
                   std::vector<std::unique_ptr<TableRequirement>> /*requirements*/,
                   std::vector<std::unique_ptr<TableUpdate>> updates)
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
        return {std::unique_ptr<Table>()};
      });

  EXPECT_THAT(transaction->CommitTransaction(), IsOk());
}

TEST_F(BaseTransactionTest, RemovePropertiesSkipsMissingKeys) {
  auto transaction = table_->NewTransaction();
  auto update_properties = transaction->UpdateProperties();
  update_properties->Remove("missing").Remove("existing");
  EXPECT_THAT(update_properties->Commit(), IsOk());

  EXPECT_CALL(*catalog_,
              UpdateTable(::testing::Eq(identifier_), ::testing::_, ::testing::_))
      .WillOnce([](const TableIdentifier&,
                   std::vector<std::unique_ptr<TableRequirement>> /*requirements*/,
                   std::vector<std::unique_ptr<TableUpdate>> updates)
                    -> Result<std::unique_ptr<Table>> {
        EXPECT_EQ(1u, updates.size());
        const auto* remove_update =
            dynamic_cast<const table::RemoveProperties*>(updates.front().get());
        EXPECT_NE(remove_update, nullptr);
        EXPECT_THAT(remove_update->removed(),
                    ::testing::UnorderedElementsAre("missing", "existing"));
        return {std::unique_ptr<Table>()};
      });

  EXPECT_THAT(transaction->CommitTransaction(), IsOk());
}

TEST_F(BaseTransactionTest, AggregatesMultiplePendingUpdates) {
  auto transaction = table_->NewTransaction();
  auto update_properties = transaction->UpdateProperties();
  update_properties->Set("new-key", "new-value");
  EXPECT_THAT(update_properties->Commit(), IsOk());
  auto remove_properties = transaction->UpdateProperties();
  remove_properties->Remove("existing");
  EXPECT_THAT(remove_properties->Commit(), IsOk());

  EXPECT_CALL(*catalog_,
              UpdateTable(::testing::Eq(identifier_), ::testing::_, ::testing::_))
      .WillOnce([](const TableIdentifier&,
                   std::vector<std::unique_ptr<TableRequirement>> /*requirements*/,
                   std::vector<std::unique_ptr<TableUpdate>> updates)
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
        EXPECT_THAT(remove_update->removed(), ::testing::ElementsAre("existing"));

        return {std::unique_ptr<Table>()};
      });

  EXPECT_THAT(transaction->CommitTransaction(), IsOk());
}

TEST_F(BaseTransactionTest, FailsIfUpdateNotCommitted) {
  auto transaction = table_->NewTransaction();
  auto update_properties = transaction->UpdateProperties();
  update_properties->Set("new-key", "new-value");
  EXPECT_THAT(transaction->CommitTransaction(), IsError(ErrorKind::kInvalidState));
}

}  // namespace iceberg
