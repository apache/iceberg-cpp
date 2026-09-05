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
#include "iceberg/logging/log_level.h"
#include "iceberg/snapshot.h"
#include "iceberg/sort_order.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/logging_test_helpers.h"
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

namespace {
// True if any captured record has the given level and a message containing `needle`.
bool HasRecord(const std::vector<LogMessage>& records, LogLevel level,
               std::string_view needle) {
  for (const auto& record : records) {
    if (record.level == level && record.message.find(needle) != std::string::npos) {
      return true;
    }
  }
  return false;
}
}  // namespace

// A commit that succeeds after one retryable conflict emits a WARN for the retry
// (carrying the prior error) and an INFO for the eventual success.
TEST_F(TransactionRetryTest, CommitRetryEmitsRetryAndSuccessLogs) {
  auto capturing = std::make_shared<CapturingLogger>();
  capturing->SetLevel(LogLevel::kTrace);
  ScopedDefaultLogger guard(capturing);

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
  EXPECT_THAT(txn->Commit(), IsOk());

  auto records = capturing->records();
  EXPECT_TRUE(
      HasRecord(records, LogLevel::kWarn, "Retrying transaction commit (attempt 2)"))
      << "expected a retry WARN";
  EXPECT_TRUE(HasRecord(records, LogLevel::kWarn, "conflict on first attempt"))
      << "retry WARN should carry the prior error";
  EXPECT_TRUE(HasRecord(records, LogLevel::kInfo, "succeeded after 2 attempts"))
      << "expected a success INFO";
}

// A metadata-only retry must not attribute a snapshot committed concurrently by
// another writer to this transaction.
TEST_F(TransactionRetryTest, MetadataOnlyRetryDoesNotLogConcurrentSnapshot) {
  auto capturing = std::make_shared<CapturingLogger>();
  capturing->SetLevel(LogLevel::kTrace);
  ScopedDefaultLogger guard(capturing);

  constexpr int64_t kConcurrentSnapshotId = 987654321;
  auto metadata_builder = TableMetadataBuilder::BuildFrom(mock_table_->metadata().get());
  auto concurrent_snapshot = std::make_shared<Snapshot>(Snapshot{
      .snapshot_id = kConcurrentSnapshotId,
      .parent_snapshot_id = mock_table_->metadata()->current_snapshot_id,
      .sequence_number = mock_table_->metadata()->last_sequence_number + 1,
      .timestamp_ms = TimePointMs{},
      .manifest_list = "concurrent-manifest-list.avro",
      .summary = {{SnapshotSummaryFields::kOperation, "append"}},
  });
  metadata_builder->SetBranchSnapshot(concurrent_snapshot,
                                      std::string(SnapshotRef::kMainBranch));
  ICEBERG_UNWRAP_OR_FAIL(auto concurrent_metadata, metadata_builder->Build());
  auto concurrent_metadata_ptr =
      std::shared_ptr<TableMetadata>(std::move(concurrent_metadata));
  const std::string concurrent_metadata_location = "concurrent.metadata.json";

  ON_CALL(*mock_catalog_, LoadTable(::testing::_))
      .WillByDefault([this, concurrent_metadata_ptr, &concurrent_metadata_location](
                         const TableIdentifier&) -> Result<std::shared_ptr<Table>> {
        return Table::Make(mock_table_->name(), concurrent_metadata_ptr,
                           concurrent_metadata_location, mock_table_->io(),
                           mock_catalog_);
      });

  int update_call_count = 0;
  ON_CALL(*mock_catalog_, UpdateTable(::testing::_, ::testing::_, ::testing::_))
      .WillByDefault(
          [this, concurrent_metadata_ptr, &concurrent_metadata_location,
           &update_call_count](const TableIdentifier&,
                               const std::vector<std::unique_ptr<TableRequirement>>&,
                               const std::vector<std::unique_ptr<TableUpdate>>&)
              -> Result<std::shared_ptr<Table>> {
            if (++update_call_count == 1) {
              return CommitFailed("conflict on first attempt");
            }
            return Table::Make(mock_table_->name(), concurrent_metadata_ptr,
                               concurrent_metadata_location, mock_table_->io(),
                               mock_catalog_);
          });

  ICEBERG_UNWRAP_OR_FAIL(auto txn, mock_table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto update, txn->NewUpdateProperties());
  update->Set("retry.test", "value");
  ASSERT_THAT(update->Commit(), IsOk());
  ASSERT_THAT(txn->Commit(), IsOk());

  EXPECT_FALSE(HasRecord(capturing->records(), LogLevel::kInfo,
                         std::to_string(kConcurrentSnapshotId)))
      << "metadata-only commit attributed the concurrent snapshot to itself";
}

// A commit that exhausts its retries returns the final error without emitting a
// generic ERROR log. Genuine retry attempts still emit WARN records.
TEST_F(TransactionRetryTest, CommitRetryExhaustedDoesNotEmitErrorLog) {
  auto capturing = std::make_shared<CapturingLogger>();
  capturing->SetLevel(LogLevel::kTrace);
  ScopedDefaultLogger guard(capturing);

  ON_CALL(*mock_catalog_, UpdateTable(::testing::_, ::testing::_, ::testing::_))
      .WillByDefault([](const TableIdentifier&,
                        const std::vector<std::unique_ptr<TableRequirement>>&,
                        const std::vector<std::unique_ptr<TableUpdate>>&)
                         -> Result<std::shared_ptr<Table>> {
        return CommitFailed("always conflicts");
      });

  ICEBERG_UNWRAP_OR_FAIL(auto txn, mock_table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto update, txn->NewUpdateProperties());
  update->Set("retry.test", "value");
  EXPECT_THAT(update->Commit(), IsOk());
  EXPECT_THAT(txn->Commit(), IsError(ErrorKind::kCommitFailed));

  auto records = capturing->records();
  EXPECT_FALSE(HasRecord(records, LogLevel::kError, ""))
      << "the final commit error should be propagated without a generic ERROR log";
  // Retries 2..5 each log a WARN.
  EXPECT_TRUE(
      HasRecord(records, LogLevel::kWarn, "Retrying transaction commit (attempt 5)"));
}

// A commit that succeeds on the first attempt emits a plain success INFO (no
// "after N attempts"). This is the single-attempt case that was previously silent.
TEST_F(TransactionRetryTest, CommitSuccessEmitsInfoLog) {
  auto capturing = std::make_shared<CapturingLogger>();
  capturing->SetLevel(LogLevel::kTrace);
  ScopedDefaultLogger guard(capturing);

  ON_CALL(*mock_catalog_, UpdateTable(::testing::_, ::testing::_, ::testing::_))
      .WillByDefault([this](const TableIdentifier&,
                            const std::vector<std::unique_ptr<TableRequirement>>&,
                            const std::vector<std::unique_ptr<TableUpdate>>&)
                         -> Result<std::shared_ptr<Table>> {
        return Table::Make(mock_table_->name(), mock_table_->metadata(),
                           std::string(mock_table_->metadata_file_location()),
                           mock_table_->io(), mock_catalog_);
      });

  ICEBERG_UNWRAP_OR_FAIL(auto txn, mock_table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto update, txn->NewUpdateProperties());
  update->Set("retry.test", "value");
  EXPECT_THAT(update->Commit(), IsOk());
  EXPECT_THAT(txn->Commit(), IsOk());

  auto records = capturing->records();
  EXPECT_TRUE(HasRecord(records, LogLevel::kInfo, "Transaction commit succeeded"))
      << "expected a success INFO on a single-attempt commit";
  // No retry happened, so there must be no retry WARN.
  EXPECT_FALSE(HasRecord(records, LogLevel::kWarn, "Retrying transaction commit"));
}

TEST_F(TransactionRetryTest, CommitStateUnknownStopsImmediatelyWithoutErrorLog) {
  auto capturing = std::make_shared<CapturingLogger>();
  capturing->SetLevel(LogLevel::kTrace);
  ScopedDefaultLogger guard(capturing);

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
  EXPECT_FALSE(HasRecord(capturing->records(), LogLevel::kError, ""))
      << "an unknown commit state must not be logged as a confirmed failure";
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
