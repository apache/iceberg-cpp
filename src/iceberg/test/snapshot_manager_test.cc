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

#include "iceberg/result.h"
#include "iceberg/snapshot.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/update_test_base.h"
#include "iceberg/transaction.h"
#include "iceberg/update/fast_append.h"

namespace iceberg {

class SnapshotManagerTest : public UpdateTestBase {
 protected:
  void SetUp() override {
    UpdateTestBase::SetUp();
    ExtractSnapshotIdsFromTable();
  }

  void ExtractSnapshotIdsFromTable() {
    ICEBERG_UNWRAP_OR_FAIL(auto current, table_->current_snapshot());
    current_snapshot_id_ = current->snapshot_id;
    ASSERT_FALSE(table_->snapshots().empty());
    oldest_snapshot_id_ = table_->snapshots().front()->snapshot_id;
  }

  // Snapshot IDs from the main table (TableMetadataV2Valid.json)
  int64_t current_snapshot_id_{};
  int64_t oldest_snapshot_id_{};
};

class SnapshotManagerMinimalTableTest : public MinimalUpdateTestBase {};

TEST_F(SnapshotManagerTest, CreateBranch) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateBranch("branch1", current_snapshot_id_);
  EXPECT_THAT(manager->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  auto it = reloaded->metadata()->refs.find("branch1");
  EXPECT_NE(it, reloaded->metadata()->refs.end());
  auto ref = it->second;
  EXPECT_EQ(ref->type(), SnapshotRefType::kBranch);
  EXPECT_EQ(ref->snapshot_id, current_snapshot_id_);
}

TEST_F(SnapshotManagerTest, CreateBranchWithoutSnapshotId) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateBranch("branch1");
  EXPECT_THAT(manager->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  auto it = reloaded->metadata()->refs.find("branch1");
  EXPECT_NE(it, reloaded->metadata()->refs.end());
  auto ref = it->second;
  EXPECT_EQ(ref->type(), SnapshotRefType::kBranch);
  EXPECT_EQ(ref->snapshot_id, current_snapshot_id_);
}

TEST_F(SnapshotManagerMinimalTableTest, CreateBranchOnEmptyTable) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, minimal_table_->NewSnapshotManager());
  manager->CreateBranch("branch1");
  EXPECT_THAT(manager->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  EXPECT_FALSE(
      reloaded->metadata()->refs.contains(std::string(SnapshotRef::kMainBranch)));
  auto it = reloaded->metadata()->refs.find("branch1");
  EXPECT_NE(it, reloaded->metadata()->refs.end());
  auto ref = it->second;
  EXPECT_EQ(ref->type(), SnapshotRefType::kBranch);
}

TEST_F(SnapshotManagerMinimalTableTest,
       CreateBranchOnEmptyTableFailsWhenRefAlreadyExists) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, minimal_table_->NewSnapshotManager());
  manager->CreateBranch("branch1");
  EXPECT_THAT(manager->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto table_with_branch, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto manager2, table_with_branch->NewSnapshotManager());
  manager2->CreateBranch("branch1");
  auto result = manager2->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Ref branch1 already exists"));
}

TEST_F(SnapshotManagerTest, CreateBranchFailsWhenRefAlreadyExists) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateBranch("branch1", current_snapshot_id_);
  EXPECT_THAT(manager->Commit(), IsOk());

  // Try to create a branch with an existing name
  ICEBERG_UNWRAP_OR_FAIL(auto manager2, table_->NewSnapshotManager());
  manager2->CreateBranch("branch1", current_snapshot_id_);
  auto result = manager2->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(result, HasErrorMessage("branch 'branch1' was created concurrently"));
}

TEST_F(SnapshotManagerTest, CreateTag) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateTag("tag1", current_snapshot_id_);
  EXPECT_THAT(manager->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  auto it = reloaded->metadata()->refs.find("tag1");
  EXPECT_NE(it, reloaded->metadata()->refs.end());
  auto ref = it->second;
  EXPECT_EQ(ref->type(), SnapshotRefType::kTag);
  EXPECT_EQ(ref->snapshot_id, current_snapshot_id_);
}

TEST_F(SnapshotManagerTest, CreateTagFailsWhenRefAlreadyExists) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateTag("tag1", current_snapshot_id_);
  EXPECT_THAT(manager->Commit(), IsOk());

  // Try to create a tag with an existing name
  ICEBERG_UNWRAP_OR_FAIL(auto manager2, table_->NewSnapshotManager());
  manager2->CreateTag("tag1", current_snapshot_id_);
  auto result = manager2->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(result, HasErrorMessage("tag 'tag1' was created concurrently"));
}

TEST_F(SnapshotManagerTest, RemoveBranch) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateBranch("branch1", current_snapshot_id_);
  EXPECT_THAT(manager->Commit(), IsOk());

  {
    ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
    ICEBERG_UNWRAP_OR_FAIL(auto manager2, reloaded->NewSnapshotManager());
    manager2->RemoveBranch("branch1");
    EXPECT_THAT(manager2->Commit(), IsOk());
  }

  {
    ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
    EXPECT_FALSE(reloaded->metadata()->refs.contains("branch1"));
  }
}

TEST_F(SnapshotManagerTest, RemovingNonExistingBranchFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->RemoveBranch("non-existing");
  auto result = manager->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Branch does not exist: non-existing"));
}

TEST_F(SnapshotManagerTest, RemovingMainBranchFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->RemoveBranch(std::string(SnapshotRef::kMainBranch));
  auto result = manager->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot remove main branch"));
}

TEST_F(SnapshotManagerTest, RemoveTag) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateTag("tag1", current_snapshot_id_);
  EXPECT_THAT(manager->Commit(), IsOk());

  {
    ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
    ICEBERG_UNWRAP_OR_FAIL(auto manager2, reloaded->NewSnapshotManager());
    manager2->RemoveTag("tag1");
    EXPECT_THAT(manager2->Commit(), IsOk());
  }

  {
    ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
    EXPECT_FALSE(reloaded->metadata()->refs.contains("tag1"));
  }
}

TEST_F(SnapshotManagerTest, RemovingNonExistingTagFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->RemoveTag("non-existing");
  auto result = manager->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Tag does not exist: non-existing"));
}

TEST_F(SnapshotManagerTest, ReplaceBranch) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateBranch("branch1", oldest_snapshot_id_);
  manager->CreateBranch("branch2", current_snapshot_id_);
  EXPECT_THAT(manager->Commit(), IsOk());

  {
    ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
    ICEBERG_UNWRAP_OR_FAIL(auto manager2, reloaded->NewSnapshotManager());
    manager2->ReplaceBranch("branch1", "branch2");
    EXPECT_THAT(manager2->Commit(), IsOk());
  }

  {
    ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
    auto it = reloaded->metadata()->refs.find("branch1");
    EXPECT_NE(it, reloaded->metadata()->refs.end());
    auto ref = it->second;
    EXPECT_NE(ref, nullptr);
    EXPECT_EQ(ref->snapshot_id, current_snapshot_id_);
  }
}

TEST_F(SnapshotManagerTest, ReplaceBranchNonExistingToBranchFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateBranch("branch1", current_snapshot_id_);
  EXPECT_THAT(manager->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto manager2, reloaded->NewSnapshotManager());
  manager2->ReplaceBranch("branch1", "non-existing");
  auto result = manager2->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Ref does not exist: non-existing"));
}

TEST_F(SnapshotManagerTest, ReplaceBranchNonExistingFromBranchCreatesTheBranch) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateBranch("branch1", current_snapshot_id_);
  EXPECT_THAT(manager->Commit(), IsOk());

  {
    ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
    ICEBERG_UNWRAP_OR_FAIL(auto manager2, reloaded->NewSnapshotManager());
    manager2->ReplaceBranch("new-branch", "branch1");
    EXPECT_THAT(manager2->Commit(), IsOk());
  }

  {
    ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
    auto it = reloaded->metadata()->refs.find("new-branch");
    EXPECT_NE(it, reloaded->metadata()->refs.end());
    auto ref = it->second;
    EXPECT_EQ(ref->type(), SnapshotRefType::kBranch);
    EXPECT_EQ(ref->snapshot_id, current_snapshot_id_);
  }
}

TEST_F(SnapshotManagerTest, FastForwardBranchNonExistingFromBranchCreatesTheBranch) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateBranch("branch1", current_snapshot_id_);
  EXPECT_THAT(manager->Commit(), IsOk());

  {
    ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
    ICEBERG_UNWRAP_OR_FAIL(auto manager2, reloaded->NewSnapshotManager());
    manager2->FastForwardBranch("new-branch", "branch1");
    EXPECT_THAT(manager2->Commit(), IsOk());
  }

  {
    ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
    auto it = reloaded->metadata()->refs.find("new-branch");
    EXPECT_NE(it, reloaded->metadata()->refs.end());
    auto ref = it->second;
    EXPECT_EQ(ref->type(), SnapshotRefType::kBranch);
    EXPECT_EQ(ref->snapshot_id, current_snapshot_id_);
  }
}

TEST_F(SnapshotManagerTest, FastForwardBranchNonExistingToFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateBranch("branch1", current_snapshot_id_);
  EXPECT_THAT(manager->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto manager2, reloaded->NewSnapshotManager());
  manager2->FastForwardBranch("branch1", "non-existing");
  auto result = manager2->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Ref does not exist: non-existing"));
}

TEST_F(SnapshotManagerTest, ReplaceTag) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateTag("tag1", current_snapshot_id_);
  EXPECT_THAT(manager->Commit(), IsOk());

  {
    ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
    ICEBERG_UNWRAP_OR_FAIL(auto manager2, reloaded->NewSnapshotManager());
    manager2->ReplaceTag("tag1", current_snapshot_id_);
    EXPECT_THAT(manager2->Commit(), IsOk());
  }

  {
    ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
    auto it = reloaded->metadata()->refs.find("tag1");
    EXPECT_NE(it, reloaded->metadata()->refs.end());
    auto ref = it->second;
    EXPECT_NE(ref, nullptr);
    EXPECT_EQ(ref->snapshot_id, current_snapshot_id_);
  }
}

TEST_F(SnapshotManagerTest, UpdatingBranchRetention) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateBranch("branch1", current_snapshot_id_);
  EXPECT_THAT(manager->Commit(), IsOk());

  {
    ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
    ICEBERG_UNWRAP_OR_FAIL(auto manager2, reloaded->NewSnapshotManager());
    manager2->SetMinSnapshotsToKeep("branch1", 10);
    manager2->SetMaxSnapshotAgeMs("branch1", 20000);
    EXPECT_THAT(manager2->Commit(), IsOk());
  }

  {
    ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
    auto it = reloaded->metadata()->refs.find("branch1");
    EXPECT_NE(it, reloaded->metadata()->refs.end());
    auto ref = it->second;
    EXPECT_EQ(ref->type(), SnapshotRefType::kBranch);
    const auto& branch = std::get<SnapshotRef::Branch>(ref->retention);
    EXPECT_EQ(branch.max_snapshot_age_ms, 20000);
    EXPECT_EQ(branch.min_snapshots_to_keep, 10);
  }
}

TEST_F(SnapshotManagerTest, SettingBranchRetentionOnTagFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateTag("tag1", current_snapshot_id_);
  EXPECT_THAT(manager->Commit(), IsOk());

  {
    ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
    ICEBERG_UNWRAP_OR_FAIL(auto manager2, reloaded->NewSnapshotManager());
    manager2->SetMinSnapshotsToKeep("tag1", 10);
    auto result = manager2->Commit();
    EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
    EXPECT_THAT(result, HasErrorMessage("Ref 'tag1' is a tag not a branch"));
  }

  {
    ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
    ICEBERG_UNWRAP_OR_FAIL(auto manager2, reloaded->NewSnapshotManager());
    manager2->SetMaxSnapshotAgeMs("tag1", 10);
    auto result = manager2->Commit();
    EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
    EXPECT_THAT(result, HasErrorMessage("Ref 'tag1' is a tag not a branch"));
  }
}

TEST_F(SnapshotManagerTest, UpdatingBranchMaxRefAge) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateBranch("branch1", current_snapshot_id_);
  EXPECT_THAT(manager->Commit(), IsOk());

  {
    ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
    ICEBERG_UNWRAP_OR_FAIL(auto manager2, reloaded->NewSnapshotManager());
    manager2->SetMaxRefAgeMs("branch1", 10000);
    EXPECT_THAT(manager2->Commit(), IsOk());
  }

  {
    ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
    auto it = reloaded->metadata()->refs.find("branch1");
    EXPECT_NE(it, reloaded->metadata()->refs.end());
    auto ref = it->second;
    EXPECT_EQ(ref->max_ref_age_ms(), 10000);
  }
}

TEST_F(SnapshotManagerTest, UpdatingTagMaxRefAge) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateTag("tag1", current_snapshot_id_);
  EXPECT_THAT(manager->Commit(), IsOk());

  {
    ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
    ICEBERG_UNWRAP_OR_FAIL(auto manager2, reloaded->NewSnapshotManager());
    manager2->SetMaxRefAgeMs("tag1", 10000);
    EXPECT_THAT(manager2->Commit(), IsOk());
  }

  {
    ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
    auto it = reloaded->metadata()->refs.find("tag1");
    EXPECT_NE(it, reloaded->metadata()->refs.end());
    auto ref = it->second;
    EXPECT_EQ(ref->max_ref_age_ms(), 10000);
  }
}

TEST_F(SnapshotManagerTest, RenameBranch) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateBranch("branch1", current_snapshot_id_);
  EXPECT_THAT(manager->Commit(), IsOk());

  {
    ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
    ICEBERG_UNWRAP_OR_FAIL(auto manager2, reloaded->NewSnapshotManager());
    manager2->RenameBranch("branch1", "branch2");
    EXPECT_THAT(manager2->Commit(), IsOk());
  }

  {
    ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
    auto it1 = reloaded->metadata()->refs.find("branch1");
    EXPECT_EQ(it1, reloaded->metadata()->refs.end());

    auto it2 = reloaded->metadata()->refs.find("branch2");
    EXPECT_NE(it2, reloaded->metadata()->refs.end());
    auto ref2 = it2->second;
    EXPECT_EQ(ref2->snapshot_id, current_snapshot_id_);
  }
}

TEST_F(SnapshotManagerTest, FailRenamingMainBranch) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->RenameBranch(std::string(SnapshotRef::kMainBranch), "some-branch");
  auto result = manager->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Cannot rename main branch"));
}

TEST_F(SnapshotManagerTest, RenamingNonExistingBranchFails) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->RenameBranch("some-missing-branch", "some-branch");
  auto result = manager->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Branch does not exist: some-missing-branch"));
}

TEST_F(SnapshotManagerTest, RollbackTo) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->RollbackTo(oldest_snapshot_id_);
  EXPECT_THAT(manager->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto current_snapshot, reloaded->current_snapshot());
  EXPECT_EQ(current_snapshot->snapshot_id, oldest_snapshot_id_);
}

TEST_F(SnapshotManagerTest, SetCurrentSnapshot) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->SetCurrentSnapshot(oldest_snapshot_id_);
  EXPECT_THAT(manager->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto current_snapshot, reloaded->current_snapshot());
  EXPECT_EQ(current_snapshot->snapshot_id, oldest_snapshot_id_);
}

TEST_F(SnapshotManagerTest, CreateReferencesAndRollback) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());
  manager->CreateBranch("branch1", current_snapshot_id_);
  manager->CreateTag("tag1", current_snapshot_id_);
  EXPECT_THAT(manager->Commit(), IsOk());

  {
    ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
    ICEBERG_UNWRAP_OR_FAIL(auto manager2, reloaded->NewSnapshotManager());
    manager2->RollbackTo(oldest_snapshot_id_);
    EXPECT_THAT(manager2->Commit(), IsOk());
  }

  {
    ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
    ICEBERG_UNWRAP_OR_FAIL(auto current_snapshot, reloaded->current_snapshot());
    EXPECT_EQ(current_snapshot->snapshot_id, oldest_snapshot_id_);

    auto branch_it = reloaded->metadata()->refs.find("branch1");
    EXPECT_NE(branch_it, reloaded->metadata()->refs.end());
    EXPECT_EQ(branch_it->second->snapshot_id, current_snapshot_id_);

    auto tag_it = reloaded->metadata()->refs.find("tag1");
    EXPECT_NE(tag_it, reloaded->metadata()->refs.end());
    EXPECT_EQ(tag_it->second->snapshot_id, current_snapshot_id_);
  }
}

TEST_F(SnapshotManagerTest, SnapshotManagerThroughTransaction) {
  ICEBERG_UNWRAP_OR_FAIL(auto txn, table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto manager, SnapshotManager::Make(txn));

  manager->RollbackTo(oldest_snapshot_id_);
  EXPECT_THAT(txn->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto current_snapshot, reloaded->current_snapshot());
  EXPECT_EQ(current_snapshot->snapshot_id, oldest_snapshot_id_);
}

TEST_F(SnapshotManagerTest, SnapshotManagerFromTableAllowsMultipleSnapshotOperations) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, table_->NewSnapshotManager());

  manager->SetCurrentSnapshot(oldest_snapshot_id_);
  manager->SetCurrentSnapshot(current_snapshot_id_);
  manager->RollbackTo(oldest_snapshot_id_);
  EXPECT_THAT(manager->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto current_snapshot, reloaded->current_snapshot());
  EXPECT_EQ(current_snapshot->snapshot_id, oldest_snapshot_id_);
}

TEST_F(SnapshotManagerTest,
       SnapshotManagerFromTransactionAllowsMultipleSnapshotOperations) {
  ICEBERG_UNWRAP_OR_FAIL(auto txn, table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto manager, SnapshotManager::Make(txn));

  manager->SetCurrentSnapshot(oldest_snapshot_id_);
  manager->SetCurrentSnapshot(current_snapshot_id_);
  manager->RollbackTo(oldest_snapshot_id_);
  EXPECT_THAT(txn->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto current_snapshot, reloaded->current_snapshot());
  EXPECT_EQ(current_snapshot->snapshot_id, oldest_snapshot_id_);
}

}  // namespace iceberg
