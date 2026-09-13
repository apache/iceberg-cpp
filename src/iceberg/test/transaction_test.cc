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

#include <format>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "iceberg/exception.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/expression/term.h"
#include "iceberg/schema.h"
#include "iceberg/sort_order.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_catalog.h"
#include "iceberg/test/update_test_base.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"
#include "iceberg/update/fast_append.h"
#include "iceberg/update/set_snapshot.h"
#include "iceberg/update/update_properties.h"
#include "iceberg/update/update_schema.h"
#include "iceberg/update/update_sort_order.h"

namespace iceberg {

class TestPendingUpdate final : public PendingUpdate {
 public:
  explicit TestPendingUpdate(std::shared_ptr<TransactionContext> ctx)
      : PendingUpdate(std::move(ctx)) {}

  Kind kind() const override { return Kind::kUpdateProperties; }
  bool IsRetryable() const override { return true; }
};

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

TEST_F(TransactionTest, TemporaryTransactionDoesNotAttachToContext) {
  ICEBERG_UNWRAP_OR_FAIL(auto ctx,
                         TransactionContext::Make(table_, TransactionKind::kUpdate));
  ASSERT_FALSE(ctx->transaction.has_value());

  ICEBERG_UNWRAP_OR_FAIL(auto txn, Transaction::Make(ctx));

  EXPECT_NE(txn, nullptr);
  EXPECT_FALSE(ctx->transaction.has_value());
}

TEST_F(TransactionTest, StandaloneCommitRequiresSharedOwnership) {
  ICEBERG_UNWRAP_OR_FAIL(auto ctx,
                         TransactionContext::Make(table_, TransactionKind::kUpdate));
  auto update = std::make_unique<TestPendingUpdate>(ctx);

  EXPECT_THAT(update->Commit(),
              ::testing::AllOf(
                  IsError(ErrorKind::kInvalidArgument),
                  HasErrorMessage("PendingUpdate must be owned by std::shared_ptr")));
  EXPECT_FALSE(ctx->transaction.has_value());
}

TEST_F(TransactionTest, StandaloneCommitClearsTemporaryTransactionBinding) {
  ICEBERG_UNWRAP_OR_FAIL(auto ctx,
                         TransactionContext::Make(table_, TransactionKind::kUpdate));
  ICEBERG_UNWRAP_OR_FAIL(auto update, UpdateProperties::Make(ctx));
  update->Set("standalone.property", "standalone.value");

  EXPECT_THAT(update->Commit(), IsOk());
  EXPECT_FALSE(ctx->transaction.has_value());
}

TEST_F(TransactionTest, CommitNoOpUpdate) {
  ICEBERG_UNWRAP_OR_FAIL(auto txn, table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto update, txn->NewSetSnapshot());

  EXPECT_THAT(update->Commit(), IsOk());
  EXPECT_THAT(txn->Commit(), IsOk());
}

TEST_F(TransactionTest, ApplyFailureFinalizesTransaction) {
  ICEBERG_UNWRAP_OR_FAIL(auto txn, table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto update, txn->NewFastAppend());
  update->AppendFile(nullptr);

  EXPECT_THAT(update->Commit(), IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(txn->Commit(),
              ::testing::AllOf(IsError(ErrorKind::kValidationFailed),
                               HasErrorMessage("Transaction is not ready")));
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

TEST_F(TransactionRetryTest, StandaloneCommitRetryReappliesUpdate) {
  std::vector<size_t> update_counts;
  ON_CALL(*mock_catalog_, UpdateTable(::testing::_, ::testing::_, ::testing::_))
      .WillByDefault(
          [this, &update_counts](const TableIdentifier&,
                                 const std::vector<std::unique_ptr<TableRequirement>>&,
                                 const std::vector<std::unique_ptr<TableUpdate>>& updates)
              -> Result<std::shared_ptr<Table>> {
            update_counts.push_back(updates.size());
            if (update_counts.size() == 1) {
              return CommitFailed("conflict on first attempt");
            }
            return Table::Make(mock_table_->name(), mock_table_->metadata(),
                               std::string(mock_table_->metadata_file_location()),
                               mock_table_->io(), mock_catalog_);
          });
  EXPECT_CALL(*mock_catalog_, LoadTable(::testing::_))
      .WillOnce([this](const TableIdentifier&) -> Result<std::shared_ptr<Table>> {
        auto builder = TableMetadataBuilder::BuildFrom(mock_table_->metadata().get());
        ICEBERG_ASSIGN_OR_RAISE(auto metadata, builder->Build());
        return Table::Make(
            mock_table_->name(), std::shared_ptr<TableMetadata>(std::move(metadata)),
            std::format("{}.refreshed", mock_table_->metadata_file_location()),
            mock_table_->io(), mock_catalog_);
      });

  ICEBERG_UNWRAP_OR_FAIL(auto update, mock_table_->NewUpdateProperties());
  update->Set("retry.test", "value");

  EXPECT_THAT(update->Commit(), IsOk());
  EXPECT_THAT(update_counts, ::testing::ElementsAre(1U, 1U));
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

TEST_F(TransactionRetryTest, CommitExceptionMakesOutcomeUnknown) {
  int update_call_count = 0;
  ON_CALL(*mock_catalog_, UpdateTable(::testing::_, ::testing::_, ::testing::_))
      .WillByDefault(
          [&update_call_count](const TableIdentifier&,
                               const std::vector<std::unique_ptr<TableRequirement>>&,
                               const std::vector<std::unique_ptr<TableUpdate>>&)
              -> Result<std::shared_ptr<Table>> {
            ++update_call_count;
            throw std::runtime_error("injected catalog failure");
          });

  ICEBERG_UNWRAP_OR_FAIL(auto txn, mock_table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto properties, txn->NewUpdateProperties());
  properties->Set("exception.test", "value");
  EXPECT_THAT(properties->Commit(), IsOk());

  EXPECT_THAT(txn->Commit(), IsError(ErrorKind::kCommitStateUnknown));
  EXPECT_EQ(txn->state(), TransactionState::kCommitStateUnknown);
  EXPECT_THAT(txn->NewFastAppend(), IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(properties->Commit(), IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(txn->Commit(), IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(txn->Abort(), IsError(ErrorKind::kValidationFailed));
  EXPECT_EQ(update_call_count, 1);
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

TEST_F(TransactionTest, AppliedUpdateCannotCompleteAnotherPendingOperation) {
  ICEBERG_UNWRAP_OR_FAIL(auto txn, table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto first, txn->NewUpdateProperties());
  first->Set("first", "1");
  ASSERT_THAT(first->Commit(), IsOk());
  EXPECT_EQ(txn->state(), TransactionState::kReady);
  ICEBERG_UNWRAP_OR_FAIL(auto second, txn->NewUpdateProperties());
  EXPECT_THAT(first->Commit(), HasErrorMessage("not the current pending operation"));
  EXPECT_EQ(txn->state(), TransactionState::kUpdatePending);
  EXPECT_THAT(txn->Commit(), IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(txn->NewUpdateProperties(), IsError(ErrorKind::kValidationFailed));
  second->Set("second", "2");
  ASSERT_THAT(second->Commit(), IsOk());
  ASSERT_THAT(txn->Commit(), IsOk());
  EXPECT_EQ(txn->state(), TransactionState::kCommitted);
  EXPECT_EQ(ReloadMetadata()->properties.configs().at("first"), "1");
  EXPECT_EQ(ReloadMetadata()->properties.configs().at("second"), "2");
  EXPECT_THAT(txn->Abort(), IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(first->Commit(), IsError(ErrorKind::kValidationFailed));
}

TEST_F(TransactionTest, StandaloneCommitIsTerminal) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateProperties());
  update->Set("once", "value");
  ASSERT_THAT(update->Commit(), IsOk());
  EXPECT_THAT(update->Commit(), HasErrorMessage("Update is terminal"));
  EXPECT_THROW(update->Set("twice", "value"), IcebergError);
}

TEST_F(TransactionTest, AbortReadyAndPendingIsIdempotent) {
  for (bool add_update : {false, true}) {
    ICEBERG_UNWRAP_OR_FAIL(auto txn, table_->NewTransaction());
    std::shared_ptr<UpdateProperties> update;
    if (add_update) {
      ICEBERG_UNWRAP_OR_FAIL(update, txn->NewUpdateProperties());
      update->Set("discard", "value");
    }
    ASSERT_THAT(txn->Abort(), IsOk());
    EXPECT_EQ(txn->state(), TransactionState::kAborted);
    EXPECT_THAT(txn->Abort(), IsOk());
    EXPECT_THAT(txn->Commit(), IsError(ErrorKind::kValidationFailed));
    EXPECT_THAT(txn->NewFastAppend(), IsError(ErrorKind::kValidationFailed));
    if (update) {
      EXPECT_THAT(update->Commit(), IsError(ErrorKind::kValidationFailed));
      EXPECT_THROW(update->Set("reuse", "value"), IcebergError);
    }
  }
}

TEST_F(TransactionTest, ApplyFailureCannotBeCorrected) {
  ICEBERG_UNWRAP_OR_FAIL(auto txn, table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto update, txn->NewUpdateProperties());
  update->Set("format-version", "100");
  EXPECT_THAT(update->Commit(), IsError(ErrorKind::kInvalidArgument));
  EXPECT_EQ(txn->state(), TransactionState::kFailed);
  EXPECT_THROW(update->Set("format-version", "2"), IcebergError);
  EXPECT_THAT(update->Commit(), IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(txn->Commit(), IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(txn->NewUpdateProperties(), IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(txn->Abort(), IsOk());
  EXPECT_THAT(txn->Abort(), IsOk());
}

TEST_F(TransactionRetryTest, NonRetryableStandaloneUpdateStopsAtFirstConflict) {
  EXPECT_CALL(*mock_catalog_, UpdateTable(::testing::_, ::testing::_, ::testing::_))
      .Times(1)
      .WillOnce(::testing::Return(CommitFailed("conflict")));
  EXPECT_CALL(*mock_catalog_, LoadTable(::testing::_)).Times(0);
  ICEBERG_UNWRAP_OR_FAIL(auto update, mock_table_->NewUpdateSchema());
  update->AddColumn("new_column", int64());
  EXPECT_THAT(update->Commit(), IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(update->Commit(), IsError(ErrorKind::kValidationFailed));
}

TEST_F(TransactionRetryTest, FrozenMutationDoesNotPoisonReplay) {
  EXPECT_CALL(*mock_catalog_, UpdateTable(::testing::_, ::testing::_, ::testing::_))
      .WillOnce(::testing::Return(CommitFailed("conflict")))
      .WillOnce([this](const auto& name, const auto& requirements, const auto& updates) {
        return catalog_->UpdateTable(name, requirements, updates);
      });
  ICEBERG_UNWRAP_OR_FAIL(auto txn, mock_table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto update, txn->NewUpdateProperties());
  update->Set("frozen", "original");
  ASSERT_THAT(update->Commit(), IsOk());
  EXPECT_THROW(update->Set("frozen", "changed"), IcebergError);
  ASSERT_THAT(txn->Commit(), IsOk());
  EXPECT_EQ(ReloadMetadata()->properties.configs().at("frozen"), "original");
}

TEST_F(TransactionTest, FreezeCopiesSortTransformAliases) {
  FailCommits(2);
  ICEBERG_UNWRAP_OR_FAIL(auto txn, table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto update, txn->NewUpdateSortOrder());
  auto transform = Transform::Bucket(16);
  ICEBERG_UNWRAP_OR_FAIL(auto term,
                         UnboundTransform::Make(Expressions::Ref("x"), transform));
  update->AddSortField(std::move(term), SortDirection::kAscending, NullOrder::kFirst);
  ASSERT_THAT(update->Commit(), IsOk());
  *transform = *Transform::Bucket(32);
  ICEBERG_UNWRAP_OR_FAIL(auto preview, update->Validate());
  *preview->fields()[0].transform() = *Transform::Bucket(64);
  ASSERT_THAT(txn->Commit(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto order, ReloadMetadata()->SortOrder());
  EXPECT_EQ(*order->fields()[0].transform(), *Transform::Bucket(16));
}

TEST_F(TransactionTest, FreezeCopiesNestedSchemaTypes) {
  ICEBERG_UNWRAP_OR_FAIL(auto txn, table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto update, txn->NewUpdateSchema());
  auto type = decimal(9, 2);
  update->AddColumn("amount", type);
  ASSERT_THAT(update->Commit(), IsOk());
  *type = DecimalType(18, 4);
  ASSERT_THAT(txn->Commit(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto schema, ReloadMetadata()->Schema());
  ICEBERG_UNWRAP_OR_FAIL(auto field, schema->FindFieldByName("amount"));
  ASSERT_TRUE(field.has_value());
  EXPECT_EQ(*field->get().type(), *decimal(9, 2));
}

}  // namespace iceberg
