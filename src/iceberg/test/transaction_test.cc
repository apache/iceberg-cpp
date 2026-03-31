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

#include "iceberg/transaction.h"

#include "iceberg/expression/expressions.h"
#include "iceberg/expression/term.h"
#include "iceberg/sort_order.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_catalog.h"
#include "iceberg/test/update_test_base.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"
#include "iceberg/update/update_properties.h"
#include "iceberg/update/update_schema.h"
#include "iceberg/update/update_sort_order.h"

namespace iceberg {

class TransactionTest : public UpdateTestBase {};

TEST_F(TransactionTest, CreateTransaction) {
  ICEBERG_UNWRAP_OR_FAIL(auto txn, table_->NewTransaction());
  EXPECT_NE(txn, nullptr);
  EXPECT_EQ(txn->table(), table_);
}

TEST_F(TransactionTest, CommitEmptyTransaction) {
  ICEBERG_UNWRAP_OR_FAIL(auto txn, table_->NewTransaction());
  EXPECT_THAT(txn->Commit(), IsOk());
}

TEST_F(TransactionTest, CommitTransactionWithPropertyUpdate) {
  ICEBERG_UNWRAP_OR_FAIL(auto txn, table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto update, txn->NewUpdateProperties());

  update->Set("txn.property", "txn.value");
  EXPECT_THAT(update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto updated_table, txn->Commit());
  EXPECT_NE(updated_table, nullptr);

  // Reload table and verify the property was set
  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  const auto& props = reloaded->properties().configs();
  EXPECT_EQ(props.at("txn.property"), "txn.value");
}

TEST_F(TransactionTest, MultipleUpdatesInTransaction) {
  ICEBERG_UNWRAP_OR_FAIL(auto txn, table_->NewTransaction());

  // First update: set property
  ICEBERG_UNWRAP_OR_FAIL(auto update1, txn->NewUpdateProperties());
  update1->Set("key1", "value1").Set("key2", "value2");
  EXPECT_THAT(update1->Commit(), IsOk());

  // Second update: update sort order
  ICEBERG_UNWRAP_OR_FAIL(auto update2, txn->NewUpdateSortOrder());
  auto term =
      UnboundTransform::Make(Expressions::Ref("x"), Transform::Identity()).value();
  update2->AddSortField(std::move(term), SortDirection::kAscending, NullOrder::kFirst);
  EXPECT_THAT(update2->Commit(), IsOk());

  // Commit transaction
  ICEBERG_UNWRAP_OR_FAIL(auto updated_table, txn->Commit());

  // Verify properties were set
  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  const auto& props = reloaded->properties().configs();
  EXPECT_EQ(props.at("key1"), "value1");
  EXPECT_EQ(props.at("key2"), "value2");

  // Verify sort order was updated
  ICEBERG_UNWRAP_OR_FAIL(auto sort_order, reloaded->sort_order());
  std::vector<SortField> expected_fields;
  expected_fields.emplace_back(1, Transform::Identity(), SortDirection::kAscending,
                               NullOrder::kFirst);
  ICEBERG_UNWRAP_OR_FAIL(
      auto expected_sort_order,
      SortOrder::Make(sort_order->order_id(), std::move(expected_fields)));
  EXPECT_EQ(*sort_order, *expected_sort_order);
}

class TransactionRetryTest : public UpdateTestBase {
 protected:
  void SetUp() override {
    UpdateTestBase::SetUp();

    // Create a MockCatalog and wire it to the existing table
    mock_catalog_ = std::make_shared<::testing::NiceMock<MockCatalog>>();

    ON_CALL(*mock_catalog_, LoadTable(::testing::_))
        .WillByDefault([this](const TableIdentifier&) -> Result<std::shared_ptr<Table>> {
          return Table::Make(table_->name(), table_->metadata(),
                             std::string(table_->metadata_file_location()), table_->io(),
                             mock_catalog_);
        });

    // Create a table instance bound to the mock catalog
    auto result = Table::Make(table_->name(), table_->metadata(),
                              std::string(table_->metadata_file_location()), table_->io(),
                              mock_catalog_);
    ASSERT_THAT(result, IsOk());
    mock_table_ = std::move(result.value());
  }

  std::shared_ptr<::testing::NiceMock<MockCatalog>> mock_catalog_;
  std::shared_ptr<Table> mock_table_;
};

TEST_F(TransactionRetryTest, CommitRetrySucceedsAfterConflict) {
  int update_call_count = 0;
  ON_CALL(*mock_catalog_, UpdateTable(::testing::_, ::testing::_, ::testing::_))
      .WillByDefault([this, &update_call_count](
                         const TableIdentifier&,
                         const std::vector<std::unique_ptr<TableRequirement>>&,
                         const std::vector<std::unique_ptr<TableUpdate>>&)
                         -> Result<std::shared_ptr<Table>> {
        ++update_call_count;
        if (update_call_count == 1) {
          return CommitFailed("conflict on first attempt");
        }
        return Table::Make(mock_table_->name(), mock_table_->metadata(),
                           std::string(mock_table_->metadata_file_location()),
                           mock_table_->io(), mock_catalog_);
      });

  ICEBERG_UNWRAP_OR_FAIL(auto txn, mock_table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto update, txn->NewUpdateProperties());
  update->Set("retry.test", "value");
  EXPECT_THAT(update->Commit(), IsOk());

  auto result = txn->Commit();
  EXPECT_THAT(result, IsOk());
  EXPECT_EQ(update_call_count, 2);
}

TEST_F(TransactionRetryTest, CommitRetryExhausted) {
  int update_call_count = 0;
  ON_CALL(*mock_catalog_, UpdateTable(::testing::_, ::testing::_, ::testing::_))
      .WillByDefault(
          [&update_call_count](const TableIdentifier&,
                               const std::vector<std::unique_ptr<TableRequirement>>&,
                               const std::vector<std::unique_ptr<TableUpdate>>&)
              -> Result<std::shared_ptr<Table>> {
            ++update_call_count;
            return CommitFailed("always conflicts");
          });

  ICEBERG_UNWRAP_OR_FAIL(auto txn, mock_table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto update, txn->NewUpdateProperties());
  update->Set("retry.test", "value");
  EXPECT_THAT(update->Commit(), IsOk());

  auto result = txn->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kCommitFailed));
  EXPECT_EQ(update_call_count, 5);
}

TEST_F(TransactionRetryTest, CommitNonRetryableErrorStopsImmediately) {
  int update_call_count = 0;
  ON_CALL(*mock_catalog_, UpdateTable(::testing::_, ::testing::_, ::testing::_))
      .WillByDefault(
          [&update_call_count](const TableIdentifier&,
                               const std::vector<std::unique_ptr<TableRequirement>>&,
                               const std::vector<std::unique_ptr<TableUpdate>>&)
              -> Result<std::shared_ptr<Table>> {
            ++update_call_count;
            return CommitStateUnknown("unknown state");
          });

  ICEBERG_UNWRAP_OR_FAIL(auto txn, mock_table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto update, txn->NewUpdateProperties());
  update->Set("retry.test", "value");
  EXPECT_THAT(update->Commit(), IsOk());

  auto result = txn->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kCommitStateUnknown));
  EXPECT_EQ(update_call_count, 1);  // Should not retry
}

TEST_F(TransactionRetryTest, CreateTransactionDoesNotRetry) {
  int update_call_count = 0;
  ON_CALL(*mock_catalog_, UpdateTable(::testing::_, ::testing::_, ::testing::_))
      .WillByDefault(
          [&update_call_count](const TableIdentifier&,
                               const std::vector<std::unique_ptr<TableRequirement>>&,
                               const std::vector<std::unique_ptr<TableUpdate>>&)
              -> Result<std::shared_ptr<Table>> {
            ++update_call_count;
            return CommitFailed("conflict");
          });

  ICEBERG_UNWRAP_OR_FAIL(auto txn,
                         Transaction::Make(mock_table_, TransactionKind::kCreate));
  ICEBERG_UNWRAP_OR_FAIL(auto update, txn->NewUpdateProperties());
  update->Set("create.test", "value");
  EXPECT_THAT(update->Commit(), IsOk());

  auto result = txn->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kCommitFailed));
  EXPECT_EQ(update_call_count, 1);  // No retry for kCreate
}

TEST_F(TransactionRetryTest, NonRetryableUpdatePreventsRetry) {
  int update_call_count = 0;
  ON_CALL(*mock_catalog_, UpdateTable(::testing::_, ::testing::_, ::testing::_))
      .WillByDefault(
          [&update_call_count](const TableIdentifier&,
                               const std::vector<std::unique_ptr<TableRequirement>>&,
                               const std::vector<std::unique_ptr<TableUpdate>>&)
              -> Result<std::shared_ptr<Table>> {
            ++update_call_count;
            return CommitFailed("conflict");
          });

  ICEBERG_UNWRAP_OR_FAIL(auto txn, mock_table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto schema_update, txn->NewUpdateSchema());
  schema_update->AddColumn("new_col", int64());
  EXPECT_THAT(schema_update->Commit(), IsOk());

  auto result = txn->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kCommitFailed));
  EXPECT_EQ(update_call_count, 1);
}

}  // namespace iceberg
