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

#include "iceberg/manage_snapshots.h"

#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "iceberg/result.h"
#include "iceberg/snapshot.h"
#include "iceberg/test/matchers.h"

namespace iceberg {

// Mock implementation of ManageSnapshots for testing
// This mock tracks which methods were called to verify behavior
class MockManageSnapshots : public ManageSnapshots {
 public:
  MockManageSnapshots() = default;

  Result<Snapshot> Apply() override {
    if (should_fail_) {
      return ValidationFailed("Mock validation failed");
    }
    apply_called_ = true;

    // Return a snapshot reflecting the configuration
    return Snapshot{
        .snapshot_id = current_snapshot_id_,
        .parent_snapshot_id = std::nullopt,
        .sequence_number = 1,
        .timestamp_ms = TimePointMs{std::chrono::milliseconds{1000}},
        .manifest_list = "s3://bucket/metadata/snap-manifest-list.avro",
        .summary = {},
        .schema_id = std::nullopt,
    };
  }

  Status Commit() override {
    if (should_fail_commit_) {
      return CommitFailed("Mock commit failed");
    }
    commit_called_ = true;
    return {};
  }

  ManageSnapshots& SetCurrentSnapshot(int64_t snapshot_id) override {
    current_snapshot_id_ = snapshot_id;
    return *this;
  }

  ManageSnapshots& RollbackToTime(int64_t timestamp_millis) override {
    rollback_timestamp_ = timestamp_millis;
    return *this;
  }

  ManageSnapshots& RollbackTo(int64_t snapshot_id) override {
    rollback_snapshot_id_ = snapshot_id;
    return *this;
  }

  ManageSnapshots& Cherrypick(int64_t snapshot_id) override {
    cherrypick_snapshot_id_ = snapshot_id;
    return *this;
  }

  ManageSnapshots& CreateBranch(std::string_view name) override {
    branches_created_.emplace_back(name);
    return *this;
  }

  ManageSnapshots& CreateBranch(std::string_view name, int64_t snapshot_id) override {
    branches_created_.emplace_back(name);
    branch_snapshots_[std::string(name)] = snapshot_id;
    return *this;
  }

  ManageSnapshots& RemoveBranch(std::string_view name) override {
    branches_removed_.emplace_back(name);
    return *this;
  }

  ManageSnapshots& RenameBranch(std::string_view name,
                                std::string_view new_name) override {
    branches_renamed_[std::string(name)] = std::string(new_name);
    return *this;
  }

  ManageSnapshots& ReplaceBranch(std::string_view name, int64_t snapshot_id) override {
    branches_replaced_[std::string(name)] = snapshot_id;
    return *this;
  }

  ManageSnapshots& ReplaceBranch(std::string_view from, std::string_view to) override {
    branch_replacements_[std::string(from)] = std::string(to);
    return *this;
  }

  ManageSnapshots& FastForwardBranch(std::string_view from,
                                     std::string_view to) override {
    branch_fast_forwards_[std::string(from)] = std::string(to);
    return *this;
  }

  ManageSnapshots& CreateTag(std::string_view name, int64_t snapshot_id) override {
    tags_created_[std::string(name)] = snapshot_id;
    return *this;
  }

  ManageSnapshots& RemoveTag(std::string_view name) override {
    tags_removed_.emplace_back(name);
    return *this;
  }

  ManageSnapshots& ReplaceTag(std::string_view name, int64_t snapshot_id) override {
    tags_replaced_[std::string(name)] = snapshot_id;
    return *this;
  }

  ManageSnapshots& SetMinSnapshotsToKeep(std::string_view branch_name,
                                         int min_snapshots_to_keep) override {
    min_snapshots_[std::string(branch_name)] = min_snapshots_to_keep;
    return *this;
  }

  ManageSnapshots& SetMaxSnapshotAgeMs(std::string_view branch_name,
                                       int64_t max_snapshot_age_ms) override {
    max_snapshot_age_[std::string(branch_name)] = max_snapshot_age_ms;
    return *this;
  }

  ManageSnapshots& SetMaxRefAgeMs(std::string_view name,
                                  int64_t max_ref_age_ms) override {
    max_ref_age_[std::string(name)] = max_ref_age_ms;
    return *this;
  }

  void SetShouldFail(bool fail) { should_fail_ = fail; }
  void SetShouldFailCommit(bool fail) { should_fail_commit_ = fail; }
  bool ApplyCalled() const { return apply_called_; }
  bool CommitCalled() const { return commit_called_; }

 private:
  bool should_fail_ = false;
  bool should_fail_commit_ = false;
  bool apply_called_ = false;
  bool commit_called_ = false;

  int64_t current_snapshot_id_ = 1;
  std::optional<int64_t> rollback_timestamp_;
  std::optional<int64_t> rollback_snapshot_id_;
  std::optional<int64_t> cherrypick_snapshot_id_;

  std::vector<std::string> branches_created_;
  std::unordered_map<std::string, int64_t> branch_snapshots_;
  std::vector<std::string> branches_removed_;
  std::unordered_map<std::string, std::string> branches_renamed_;
  std::unordered_map<std::string, int64_t> branches_replaced_;
  std::unordered_map<std::string, std::string> branch_replacements_;
  std::unordered_map<std::string, std::string> branch_fast_forwards_;

  std::unordered_map<std::string, int64_t> tags_created_;
  std::vector<std::string> tags_removed_;
  std::unordered_map<std::string, int64_t> tags_replaced_;

  std::unordered_map<std::string, int> min_snapshots_;
  std::unordered_map<std::string, int64_t> max_snapshot_age_;
  std::unordered_map<std::string, int64_t> max_ref_age_;
};

// ========================================================================
// SNAPSHOT ROLLBACK OPERATIONS TESTS
// ========================================================================

TEST(ManageSnapshotsTest, SetCurrentSnapshot) {
  MockManageSnapshots manage;
  manage.SetCurrentSnapshot(100);

  auto result = manage.Apply();
  ASSERT_THAT(result, IsOk());
  EXPECT_EQ(result.value().snapshot_id, 100);
}

TEST(ManageSnapshotsTest, RollbackToTime) {
  MockManageSnapshots manage;
  manage.RollbackToTime(1609459200000);

  auto result = manage.Apply();
  EXPECT_THAT(result, IsOk());
}

TEST(ManageSnapshotsTest, RollbackTo) {
  MockManageSnapshots manage;
  manage.RollbackTo(50);

  auto result = manage.Apply();
  EXPECT_THAT(result, IsOk());
}

TEST(ManageSnapshotsTest, Cherrypick) {
  MockManageSnapshots manage;
  manage.Cherrypick(75);

  auto result = manage.Apply();
  EXPECT_THAT(result, IsOk());
}

// ========================================================================
// BRANCH OPERATIONS TESTS
// ========================================================================

TEST(ManageSnapshotsTest, CreateBranchCurrent) {
  MockManageSnapshots manage;
  manage.CreateBranch("experiment");

  auto result = manage.Apply();
  EXPECT_THAT(result, IsOk());
}

TEST(ManageSnapshotsTest, CreateBranchWithSnapshot) {
  MockManageSnapshots manage;
  manage.CreateBranch("feature-branch", 100);

  auto result = manage.Apply();
  EXPECT_THAT(result, IsOk());
}

TEST(ManageSnapshotsTest, RemoveBranch) {
  MockManageSnapshots manage;
  manage.RemoveBranch("old-branch");

  auto result = manage.Apply();
  EXPECT_THAT(result, IsOk());
}

TEST(ManageSnapshotsTest, RenameBranch) {
  MockManageSnapshots manage;
  manage.RenameBranch("old-name", "new-name");

  auto result = manage.Apply();
  EXPECT_THAT(result, IsOk());
}

TEST(ManageSnapshotsTest, ReplaceBranchWithSnapshot) {
  MockManageSnapshots manage;
  manage.ReplaceBranch("my-branch", 200);

  auto result = manage.Apply();
  EXPECT_THAT(result, IsOk());
}

TEST(ManageSnapshotsTest, ReplaceBranchWithBranch) {
  MockManageSnapshots manage;
  manage.ReplaceBranch("from-branch", "to-branch");

  auto result = manage.Apply();
  EXPECT_THAT(result, IsOk());
}

TEST(ManageSnapshotsTest, FastForwardBranch) {
  MockManageSnapshots manage;
  manage.FastForwardBranch("my-branch", "main");

  auto result = manage.Apply();
  EXPECT_THAT(result, IsOk());
}

// ========================================================================
// TAG OPERATIONS TESTS
// ========================================================================

TEST(ManageSnapshotsTest, CreateTag) {
  MockManageSnapshots manage;
  manage.CreateTag("v1.0", 100);

  auto result = manage.Apply();
  EXPECT_THAT(result, IsOk());
}

TEST(ManageSnapshotsTest, RemoveTag) {
  MockManageSnapshots manage;
  manage.RemoveTag("old-tag");

  auto result = manage.Apply();
  EXPECT_THAT(result, IsOk());
}

TEST(ManageSnapshotsTest, ReplaceTag) {
  MockManageSnapshots manage;
  manage.ReplaceTag("v1.0", 150);

  auto result = manage.Apply();
  EXPECT_THAT(result, IsOk());
}

// ========================================================================
// RETENTION POLICY TESTS
// ========================================================================

TEST(ManageSnapshotsTest, SetMinSnapshotsToKeep) {
  MockManageSnapshots manage;
  manage.SetMinSnapshotsToKeep("main", 5);

  auto result = manage.Apply();
  EXPECT_THAT(result, IsOk());
}

TEST(ManageSnapshotsTest, SetMaxSnapshotAgeMs) {
  MockManageSnapshots manage;
  manage.SetMaxSnapshotAgeMs("main", 86400000);

  auto result = manage.Apply();
  EXPECT_THAT(result, IsOk());
}

TEST(ManageSnapshotsTest, SetMaxRefAgeMs) {
  MockManageSnapshots manage;
  manage.SetMaxRefAgeMs("experiment", 604800000);

  auto result = manage.Apply();
  EXPECT_THAT(result, IsOk());
}

// ========================================================================
// METHOD CHAINING TESTS
// ========================================================================

TEST(ManageSnapshotsTest, MethodChaining) {
  MockManageSnapshots manage;

  manage.CreateBranch("experiment", 100)
      .SetMinSnapshotsToKeep("experiment", 5)
      .SetMaxSnapshotAgeMs("experiment", 86400000)
      .CreateTag("v1.0", 100);

  auto result = manage.Apply();
  EXPECT_THAT(result, IsOk());
}

TEST(ManageSnapshotsTest, ComplexMethodChaining) {
  MockManageSnapshots manage;

  manage.SetCurrentSnapshot(100)
      .CreateBranch("feature", 100)
      .CreateTag("release-1.0", 100)
      .SetMinSnapshotsToKeep("feature", 3)
      .SetMaxSnapshotAgeMs("feature", 3600000)
      .SetMaxRefAgeMs("release-1.0", 2592000000);

  auto result = manage.Apply();
  EXPECT_THAT(result, IsOk());
}

// ========================================================================
// GENERAL TESTS
// ========================================================================

TEST(ManageSnapshotsTest, ApplySuccess) {
  MockManageSnapshots manage;
  auto result = manage.Apply();
  EXPECT_THAT(result, IsOk());
  EXPECT_TRUE(manage.ApplyCalled());
}

TEST(ManageSnapshotsTest, ApplyValidationFailed) {
  MockManageSnapshots manage;
  manage.SetShouldFail(true);
  auto result = manage.Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Mock validation failed"));
}

TEST(ManageSnapshotsTest, CommitSuccess) {
  MockManageSnapshots manage;
  auto status = manage.Commit();
  EXPECT_THAT(status, IsOk());
  EXPECT_TRUE(manage.CommitCalled());
}

TEST(ManageSnapshotsTest, CommitFailed) {
  MockManageSnapshots manage;
  manage.SetShouldFailCommit(true);
  auto status = manage.Commit();
  EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(status, HasErrorMessage("Mock commit failed"));
}

TEST(ManageSnapshotsTest, InheritanceFromPendingUpdate) {
  std::unique_ptr<PendingUpdate> base_ptr = std::make_unique<MockManageSnapshots>();
  auto status = base_ptr->Commit();
  EXPECT_THAT(status, IsOk());
}

TEST(ManageSnapshotsTest, InheritanceFromPendingUpdateTyped) {
  std::unique_ptr<PendingUpdateTyped<Snapshot>> typed_ptr =
      std::make_unique<MockManageSnapshots>();
  auto status = typed_ptr->Commit();
  EXPECT_THAT(status, IsOk());

  auto result = typed_ptr->Apply();
  EXPECT_THAT(result, IsOk());
}

}  // namespace iceberg
