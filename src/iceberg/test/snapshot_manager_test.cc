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

#include "iceberg/update/snapshot_manager.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/snapshot.h"
#include "iceberg/test/update_test_base.h"
#include "iceberg/transaction.h"

namespace iceberg {

class SnapshotManagerTest : public UpdateTestBase {
 protected:
  void SetUp() override {
    UpdateTestBase::SetUp();
    ICEBERG_UNWRAP_OR_FAIL(auto current, table_->current_snapshot());
    current_snapshot_id_ = current->snapshot_id;
    ASSERT_FALSE(table_->snapshots().empty());
    oldest_snapshot_id_ = table_->snapshots().front()->snapshot_id;
  }

  int64_t current_snapshot_id_{};
  int64_t oldest_snapshot_id_{};
};

TEST_F(SnapshotManagerTest, CreateBranch) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateBranch("branch1", current_snapshot_id_);
  ExpectCommitOk(manager->Commit());
  ExpectBranch("branch1", current_snapshot_id_);
}

TEST_F(SnapshotManagerTest, CreateBranchWithoutSnapshotId) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateBranch("branch1");
  ExpectCommitOk(manager->Commit());
  ExpectBranch("branch1", current_snapshot_id_);
}

TEST_F(SnapshotManagerTest, CreateBranchFailsWhenRefAlreadyExists) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateBranch("branch1", current_snapshot_id_);
  ExpectCommitOk(manager->Commit());

  ICEBERG_UNWRAP_OR_FAIL(auto new_manager, table_->NewSnapshotManager());
  new_manager->CreateBranch("branch1", current_snapshot_id_);
  ExpectCommitError(new_manager->Commit(), ErrorKind::kCommitFailed,
                    "branch 'branch1' was created concurrently");
}

TEST_F(SnapshotManagerTest, CreateTag) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateTag("tag1", current_snapshot_id_);
  ExpectCommitOk(manager->Commit());
  ExpectTag("tag1", current_snapshot_id_);
}

TEST_F(SnapshotManagerTest, CreateTagFailsWhenRefAlreadyExists) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateTag("tag1", current_snapshot_id_);
  ExpectCommitOk(manager->Commit());

  ICEBERG_UNWRAP_OR_FAIL(auto new_manager, table_->NewSnapshotManager());
  new_manager->CreateTag("tag1", current_snapshot_id_);
  ExpectCommitError(new_manager->Commit(), ErrorKind::kCommitFailed,
                    "tag 'tag1' was created concurrently");
}

TEST_F(SnapshotManagerTest, RemoveBranch) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateBranch("branch1", current_snapshot_id_);
  ExpectCommitOk(manager->Commit());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto new_manager, reloaded->NewSnapshotManager());
  new_manager->RemoveBranch("branch1");
  ExpectCommitOk(new_manager->Commit());
  ExpectNoRef("branch1");
}

TEST_F(SnapshotManagerTest, RemovingNonExistingBranchFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->RemoveBranch("non-existing");
  ExpectCommitError(manager->Commit(), ErrorKind::kValidationFailed,
                    "Branch does not exist: non-existing");
}

TEST_F(SnapshotManagerTest, RemovingMainBranchFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->RemoveBranch(std::string(SnapshotRef::kMainBranch));
  ExpectCommitError(manager->Commit(), ErrorKind::kValidationFailed,
                    "Cannot remove main branch");
}

TEST_F(SnapshotManagerTest, RemoveTag) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateTag("tag1", current_snapshot_id_);
  ExpectCommitOk(manager->Commit());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto new_manager, reloaded->NewSnapshotManager());
  new_manager->RemoveTag("tag1");
  ExpectCommitOk(new_manager->Commit());
  ExpectNoRef("tag1");
}

TEST_F(SnapshotManagerTest, RemovingNonExistingTagFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->RemoveTag("non-existing");
  ExpectCommitError(manager->Commit(), ErrorKind::kValidationFailed,
                    "Tag does not exist: non-existing");
}

TEST_F(SnapshotManagerTest, ReplaceBranch) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateBranch("branch1", oldest_snapshot_id_);
  manager->CreateBranch("branch2", current_snapshot_id_);
  ExpectCommitOk(manager->Commit());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto new_manager, reloaded->NewSnapshotManager());
  new_manager->ReplaceBranch("branch1", "branch2");
  ExpectCommitOk(new_manager->Commit());
  ExpectBranch("branch1", current_snapshot_id_);
}

TEST_F(SnapshotManagerTest, ReplaceBranchNonExistingToBranchFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateBranch("branch1", current_snapshot_id_);
  ExpectCommitOk(manager->Commit());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto new_manager, reloaded->NewSnapshotManager());
  new_manager->ReplaceBranch("branch1", "non-existing");
  ExpectCommitError(new_manager->Commit(), ErrorKind::kValidationFailed,
                    "Ref does not exist: non-existing");
}

TEST_F(SnapshotManagerTest, ReplaceBranchNonExistingFromBranchCreatesTheBranch) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateBranch("branch1", current_snapshot_id_);
  ExpectCommitOk(manager->Commit());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto new_manager, reloaded->NewSnapshotManager());
  new_manager->ReplaceBranch("new-branch", "branch1");
  ExpectCommitOk(new_manager->Commit());
  ExpectBranch("new-branch", current_snapshot_id_);
}

TEST_F(SnapshotManagerTest, FastForwardBranchNonExistingFromBranchCreatesTheBranch) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateBranch("branch1", current_snapshot_id_);
  ExpectCommitOk(manager->Commit());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto new_manager, reloaded->NewSnapshotManager());
  new_manager->FastForwardBranch("new-branch", "branch1");
  ExpectCommitOk(new_manager->Commit());
  ExpectBranch("new-branch", current_snapshot_id_);
}

TEST_F(SnapshotManagerTest, FastForwardBranchNonExistingToFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateBranch("branch1", current_snapshot_id_);
  ExpectCommitOk(manager->Commit());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto new_manager, reloaded->NewSnapshotManager());
  new_manager->FastForwardBranch("branch1", "non-existing");
  ExpectCommitError(new_manager->Commit(), ErrorKind::kValidationFailed,
                    "Ref does not exist: non-existing");
}

TEST_F(SnapshotManagerTest, ReplaceTag) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateTag("tag1", current_snapshot_id_);
  ExpectCommitOk(manager->Commit());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto new_manager, reloaded->NewSnapshotManager());
  new_manager->ReplaceTag("tag1", current_snapshot_id_);
  ExpectCommitOk(new_manager->Commit());
  ExpectTag("tag1", current_snapshot_id_);
}

TEST_F(SnapshotManagerTest, UpdatingBranchRetention) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateBranch("branch1", current_snapshot_id_);
  ExpectCommitOk(manager->Commit());

  {
    ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
    ICEBERG_UNWRAP_OR_FAIL(auto new_manager, reloaded->NewSnapshotManager());
    new_manager->SetMinSnapshotsToKeep("branch1", 10);
    new_manager->SetMaxSnapshotAgeMs("branch1", 20000);
    ExpectCommitOk(new_manager->Commit());
  }

  auto metadata = ReloadMetadata();
  auto ref = metadata->refs.at("branch1");
  EXPECT_EQ(ref->type(), SnapshotRefType::kBranch);
  const auto& branch = std::get<SnapshotRef::Branch>(ref->retention);
  EXPECT_EQ(branch.max_snapshot_age_ms, 20000);
  EXPECT_EQ(branch.min_snapshots_to_keep, 10);
}

TEST_F(SnapshotManagerTest, SettingBranchRetentionOnTagFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateTag("tag1", current_snapshot_id_);
  ExpectCommitOk(manager->Commit());

  {
    ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
    ICEBERG_UNWRAP_OR_FAIL(auto new_manager, reloaded->NewSnapshotManager());
    new_manager->SetMinSnapshotsToKeep("tag1", 10);
    ExpectCommitError(new_manager->Commit(), ErrorKind::kValidationFailed,
                      "Ref 'tag1' is a tag not a branch");
  }

  {
    ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
    ICEBERG_UNWRAP_OR_FAIL(auto new_manager, reloaded->NewSnapshotManager());
    new_manager->SetMaxSnapshotAgeMs("tag1", 10);
    ExpectCommitError(new_manager->Commit(), ErrorKind::kValidationFailed,
                      "Ref 'tag1' is a tag not a branch");
  }
}

TEST_F(SnapshotManagerTest, UpdatingBranchMaxRefAge) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateBranch("branch1", current_snapshot_id_);
  ExpectCommitOk(manager->Commit());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto new_manager, reloaded->NewSnapshotManager());
  new_manager->SetMaxRefAgeMs("branch1", 10000);
  ExpectCommitOk(new_manager->Commit());

  EXPECT_EQ(ReloadMetadata()->refs.at("branch1")->max_ref_age_ms(), 10000);
}

TEST_F(SnapshotManagerTest, UpdatingTagMaxRefAge) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateTag("tag1", current_snapshot_id_);
  ExpectCommitOk(manager->Commit());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto new_manager, reloaded->NewSnapshotManager());
  new_manager->SetMaxRefAgeMs("tag1", 10000);
  ExpectCommitOk(new_manager->Commit());

  EXPECT_EQ(ReloadMetadata()->refs.at("tag1")->max_ref_age_ms(), 10000);
}

TEST_F(SnapshotManagerTest, RenameBranch) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateBranch("branch1", current_snapshot_id_);
  ExpectCommitOk(manager->Commit());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto new_manager, reloaded->NewSnapshotManager());
  new_manager->RenameBranch("branch1", "branch2");
  ExpectCommitOk(new_manager->Commit());

  ExpectNoRef("branch1");
  ExpectBranch("branch2", current_snapshot_id_);
}

TEST_F(SnapshotManagerTest, FailRenamingMainBranch) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->RenameBranch(std::string(SnapshotRef::kMainBranch), "some-branch");
  ExpectCommitError(manager->Commit(), ErrorKind::kValidationFailed,
                    "Cannot rename main branch");
}

TEST_F(SnapshotManagerTest, RenamingNonExistingBranchFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->RenameBranch("some-missing-branch", "some-branch");
  ExpectCommitError(manager->Commit(), ErrorKind::kValidationFailed,
                    "Branch does not exist: some-missing-branch");
}

TEST_F(SnapshotManagerTest, RollbackToTime) {
  // The oldest snapshot has timestamp 1515100955770, the current has 1555100955770.
  // Rolling back to a time between them should land on the oldest snapshot.
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->RollbackToTime(1535100955770);
  ExpectCommitOk(manager->Commit());
  ExpectCurrentSnapshot(oldest_snapshot_id_);
}

TEST_F(SnapshotManagerTest, RollbackToTimeBeforeAllSnapshotsFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->RollbackToTime(1000);
  ExpectCommitError(manager->Commit(), ErrorKind::kValidationFailed,
                    "no valid snapshot older than");
}

TEST_F(SnapshotManagerTest, ReplaceBranchBySnapshotId) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateBranch("branch1", oldest_snapshot_id_);
  ExpectCommitOk(manager->Commit());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto new_manager, reloaded->NewSnapshotManager());
  new_manager->ReplaceBranch("branch1", current_snapshot_id_);
  ExpectCommitOk(new_manager->Commit());
  ExpectBranch("branch1", current_snapshot_id_);
}

TEST_F(SnapshotManagerTest, RefUpdatesFlushedBeforeSnapshotOperation) {
  // Interleave a ref operation (CreateBranch) with a snapshot operation
  // (SetCurrentSnapshot). The ref updates should be flushed before the snapshot
  // operation is applied, so both should take effect.
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateBranch("branch1", current_snapshot_id_);
  manager->SetCurrentSnapshot(oldest_snapshot_id_);
  ExpectCommitOk(manager->Commit());
  ExpectBranch("branch1", current_snapshot_id_);
  ExpectCurrentSnapshot(oldest_snapshot_id_);
}

TEST_F(SnapshotManagerTest, SnapshotOperationThenRefUpdates) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->SetCurrentSnapshot(oldest_snapshot_id_);
  manager->CreateBranch("branch1", current_snapshot_id_);
  ExpectCommitOk(manager->Commit());
  ExpectCurrentSnapshot(oldest_snapshot_id_);
  ExpectBranch("branch1", current_snapshot_id_);
}

TEST_F(SnapshotManagerTest, RollbackTo) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->RollbackTo(oldest_snapshot_id_);
  ExpectCommitOk(manager->Commit());
  ExpectCurrentSnapshot(oldest_snapshot_id_);
}

TEST_F(SnapshotManagerTest, SetCurrentSnapshot) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->SetCurrentSnapshot(oldest_snapshot_id_);
  ExpectCommitOk(manager->Commit());
  ExpectCurrentSnapshot(oldest_snapshot_id_);
}

TEST_F(SnapshotManagerTest, CreateReferencesAndRollback) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateBranch("branch1", current_snapshot_id_);
  manager->CreateTag("tag1", current_snapshot_id_);
  ExpectCommitOk(manager->Commit());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto new_manager, reloaded->NewSnapshotManager());
  new_manager->RollbackTo(oldest_snapshot_id_);
  ExpectCommitOk(new_manager->Commit());

  ExpectCurrentSnapshot(oldest_snapshot_id_);
  ExpectBranch("branch1", current_snapshot_id_);
  ExpectTag("tag1", current_snapshot_id_);
}

TEST_F(SnapshotManagerTest, SnapshotManagerThroughTransaction) {
  ICEBERG_UNWRAP_OR_FAIL(auto txn, table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto manager, SnapshotManager::Make(txn));
  manager->RollbackTo(oldest_snapshot_id_);
  ExpectCommitOk(txn->Commit());
  ExpectCurrentSnapshot(oldest_snapshot_id_);
}

TEST_F(SnapshotManagerTest, SnapshotManagerFromTableAllowsMultipleSnapshotOperations) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->SetCurrentSnapshot(oldest_snapshot_id_);
  manager->SetCurrentSnapshot(current_snapshot_id_);
  manager->RollbackTo(oldest_snapshot_id_);
  ExpectCommitOk(manager->Commit());
  ExpectCurrentSnapshot(oldest_snapshot_id_);
}

TEST_F(SnapshotManagerTest,
       SnapshotManagerFromTransactionAllowsMultipleSnapshotOperations) {
  ICEBERG_UNWRAP_OR_FAIL(auto txn, table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto manager, SnapshotManager::Make(txn));
  manager->SetCurrentSnapshot(oldest_snapshot_id_);
  manager->SetCurrentSnapshot(current_snapshot_id_);
  manager->RollbackTo(oldest_snapshot_id_);
  ExpectCommitOk(txn->Commit());
  ExpectCurrentSnapshot(oldest_snapshot_id_);
}

class SnapshotManagerMinimalTableTest : public MinimalUpdateTestBase {};

TEST_F(SnapshotManagerMinimalTableTest, CreateBranchOnEmptyTable) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateBranch("branch1");
  ExpectCommitOk(manager->Commit());

  auto metadata = ReloadMetadata();
  EXPECT_FALSE(metadata->refs.contains(std::string(SnapshotRef::kMainBranch)));
  auto it = metadata->refs.find("branch1");
  ASSERT_NE(it, metadata->refs.end());
  EXPECT_EQ(it->second->type(), SnapshotRefType::kBranch);
}

TEST_F(SnapshotManagerMinimalTableTest,
       CreateBranchOnEmptyTableFailsWhenRefAlreadyExists) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateBranch("branch1");
  ExpectCommitOk(manager->Commit());

  ICEBERG_UNWRAP_OR_FAIL(auto table_with_branch, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto new_manager, table_with_branch->NewSnapshotManager());
  new_manager->CreateBranch("branch1");
  ExpectCommitError(new_manager->Commit(), ErrorKind::kValidationFailed,
                    "Ref branch1 already exists");
}

}  // namespace iceberg
