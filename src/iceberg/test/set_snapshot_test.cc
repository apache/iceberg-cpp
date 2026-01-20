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

#include "iceberg/update/set_snapshot.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/result.h"
#include "iceberg/snapshot.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/update_test_base.h"
#include "iceberg/transaction.h"

namespace iceberg {

class SetSnapshotTest : public UpdateTestBase {
 protected:
  // Snapshot IDs from TableMetadataV2Valid.json
  static constexpr int64_t kOldestSnapshotId = 3051729675574597004;
  static constexpr int64_t kCurrentSnapshotId = 3055729675574597004;

  // Timestamps from TableMetadataV2Valid.json
  static constexpr int64_t kOldestSnapshotTimestamp = 1515100955770;
  static constexpr int64_t kCurrentSnapshotTimestamp = 1555100955770;
};

TEST_F(SetSnapshotTest, SetCurrentSnapshotValid) {
  ICEBERG_UNWRAP_OR_FAIL(auto transaction, table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto set_snapshot, transaction->NewSetSnapshot());
  EXPECT_EQ(set_snapshot->kind(), PendingUpdate::Kind::kSetSnapshot);

  set_snapshot->SetCurrentSnapshot(kOldestSnapshotId);

  ICEBERG_UNWRAP_OR_FAIL(auto snapshot_id, set_snapshot->Apply());
  EXPECT_EQ(snapshot_id, kOldestSnapshotId);

  // Commit and verify the change was persisted
  EXPECT_THAT(set_snapshot->Commit(), IsOk());
  EXPECT_THAT(transaction->Commit(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto current_snapshot, reloaded->current_snapshot());
  EXPECT_EQ(current_snapshot->snapshot_id, kOldestSnapshotId);
}

TEST_F(SetSnapshotTest, SetCurrentSnapshotToCurrentSnapshot) {
  ICEBERG_UNWRAP_OR_FAIL(auto transaction, table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto set_snapshot, transaction->NewSetSnapshot());
  set_snapshot->SetCurrentSnapshot(kCurrentSnapshotId);

  ICEBERG_UNWRAP_OR_FAIL(auto snapshot_id, set_snapshot->Apply());
  EXPECT_EQ(snapshot_id, kCurrentSnapshotId);
}

TEST_F(SetSnapshotTest, SetCurrentSnapshotInvalid) {
  ICEBERG_UNWRAP_OR_FAIL(auto transaction, table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto set_snapshot, transaction->NewSetSnapshot());
  // Try to set to a non-existent snapshot
  int64_t invalid_snapshot_id = 9999999999999999;
  set_snapshot->SetCurrentSnapshot(invalid_snapshot_id);

  auto result = set_snapshot->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("is not found"));
}

TEST_F(SetSnapshotTest, RollbackToValid) {
  ICEBERG_UNWRAP_OR_FAIL(auto transaction, table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto set_snapshot, transaction->NewSetSnapshot());
  // Rollback to the oldest snapshot (which is an ancestor)
  set_snapshot->RollbackTo(kOldestSnapshotId);

  ICEBERG_UNWRAP_OR_FAIL(auto snapshot_id, set_snapshot->Apply());
  EXPECT_EQ(snapshot_id, kOldestSnapshotId);
}

TEST_F(SetSnapshotTest, RollbackToInvalidSnapshot) {
  ICEBERG_UNWRAP_OR_FAIL(auto transaction, table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto set_snapshot, transaction->NewSetSnapshot());
  // Try to rollback to a non-existent snapshot
  int64_t invalid_snapshot_id = 9999999999999999;
  set_snapshot->RollbackTo(invalid_snapshot_id);

  auto result = set_snapshot->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("unknown snapshot id"));
}

TEST_F(SetSnapshotTest, RollbackToTimeValidOldestSnapshot) {
  ICEBERG_UNWRAP_OR_FAIL(auto transaction, table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto set_snapshot, transaction->NewSetSnapshot());
  // Rollback to a time between the two snapshots
  // This should select the oldest snapshot
  int64_t time_between = (kOldestSnapshotTimestamp + kCurrentSnapshotTimestamp) / 2;
  set_snapshot->RollbackToTime(time_between);

  ICEBERG_UNWRAP_OR_FAIL(auto snapshot_id, set_snapshot->Apply());
  EXPECT_EQ(snapshot_id, kOldestSnapshotId);
}

TEST_F(SetSnapshotTest, RollbackToTimeBeforeAnySnapshot) {
  ICEBERG_UNWRAP_OR_FAIL(auto transaction, table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto set_snapshot, transaction->NewSetSnapshot());
  // Try to rollback to a time before any snapshot
  int64_t time_before_all = kOldestSnapshotTimestamp - 1000000;
  set_snapshot->RollbackToTime(time_before_all);

  // Should fail - no snapshot older than the specified time
  auto result = set_snapshot->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("no valid snapshot older than"));
}

TEST_F(SetSnapshotTest, RollbackToTimeExactMatch) {
  ICEBERG_UNWRAP_OR_FAIL(auto transaction, table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto set_snapshot, transaction->NewSetSnapshot());
  // Rollback to a timestamp just after the oldest snapshot
  int64_t time_just_after_oldest = kOldestSnapshotTimestamp + 1;
  set_snapshot->RollbackToTime(time_just_after_oldest);

  // Apply and verify - should return the oldest snapshot
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot_id, set_snapshot->Apply());
  EXPECT_EQ(snapshot_id, kOldestSnapshotId);
}

TEST_F(SetSnapshotTest, ApplyWithoutChanges) {
  ICEBERG_UNWRAP_OR_FAIL(auto transaction, table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto set_snapshot, transaction->NewSetSnapshot());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot_id, set_snapshot->Apply());
  EXPECT_EQ(snapshot_id, kCurrentSnapshotId);

  EXPECT_THAT(set_snapshot->Commit(), IsOk());
  EXPECT_THAT(transaction->Commit(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto current_snapshot, reloaded->current_snapshot());
  EXPECT_EQ(current_snapshot->snapshot_id, kCurrentSnapshotId);
}

}  // namespace iceberg
