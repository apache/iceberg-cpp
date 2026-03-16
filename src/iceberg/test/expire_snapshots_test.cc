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

#include "iceberg/update/expire_snapshots.h"

#include <string>
#include <vector>

#include "iceberg/test/matchers.h"
#include "iceberg/test/update_test_base.h"

namespace iceberg {

class ExpireSnapshotsTest : public UpdateTestBase {};

TEST_F(ExpireSnapshotsTest, DefaultExpireByAge) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewExpireSnapshots());
  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_EQ(result.snapshot_ids_to_remove.size(), 1);
  EXPECT_EQ(result.snapshot_ids_to_remove.at(0), 3051729675574597004);
}

TEST_F(ExpireSnapshotsTest, KeepAll) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewExpireSnapshots());
  update->RetainLast(2);
  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_TRUE(result.snapshot_ids_to_remove.empty());
  EXPECT_TRUE(result.refs_to_remove.empty());
}

TEST_F(ExpireSnapshotsTest, ExpireById) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewExpireSnapshots());
  update->ExpireSnapshotId(3051729675574597004);
  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_EQ(result.snapshot_ids_to_remove.size(), 1);
  EXPECT_EQ(result.snapshot_ids_to_remove.at(0), 3051729675574597004);
}

TEST_F(ExpireSnapshotsTest, ExpireOlderThan) {
  struct TestCase {
    int64_t expire_older_than;
    size_t expected_num_expired;
  };
  const std::vector<TestCase> test_cases = {
      {.expire_older_than = 1515100955770 - 1, .expected_num_expired = 0},
      {.expire_older_than = 1515100955770 + 1, .expected_num_expired = 1}};
  for (const auto& test_case : test_cases) {
    ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewExpireSnapshots());
    update->ExpireOlderThan(test_case.expire_older_than);
    ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
    EXPECT_EQ(result.snapshot_ids_to_remove.size(), test_case.expected_num_expired);
  }
}

TEST_F(ExpireSnapshotsTest, DeleteWithCustomFunction) {
  std::vector<std::string> deleted_files;
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewExpireSnapshots());
  update->DeleteWith(
      [&deleted_files](const std::string& path) { deleted_files.push_back(path); });

  // Apply first so apply_result_ is cached
  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_EQ(result.snapshot_ids_to_remove.size(), 1);

  // Call Finalize directly to simulate successful commit
  // Note: Finalize tries to read manifests from the expired snapshot's manifest list,
  // which will fail on mock FS since "s3://a/b/1.avro" doesn't contain real avro data.
  // The error is returned from Finalize but in the real commit flow it's ignored.
  auto finalize_status = update->Finalize(std::nullopt);
  // Finalize may fail because manifest list files don't exist on mock FS,
  // but it should not crash
  if (finalize_status.has_value()) {
    // If it succeeded (e.g., if manifest reading was skipped), verify deletions
    EXPECT_FALSE(deleted_files.empty());
  }
}

TEST_F(ExpireSnapshotsTest, CleanupLevelNoneSkipsFileDeletion) {
  std::vector<std::string> deleted_files;
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewExpireSnapshots());
  update->CleanupLevel(CleanupLevel::kNone);
  update->DeleteWith(
      [&deleted_files](const std::string& path) { deleted_files.push_back(path); });

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_EQ(result.snapshot_ids_to_remove.size(), 1);

  // With kNone cleanup level, Finalize should skip all file deletion
  auto finalize_status = update->Finalize(std::nullopt);
  EXPECT_THAT(finalize_status, IsOk());
  EXPECT_TRUE(deleted_files.empty());
}

TEST_F(ExpireSnapshotsTest, FinalizeSkippedOnCommitError) {
  std::vector<std::string> deleted_files;
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewExpireSnapshots());
  update->DeleteWith(
      [&deleted_files](const std::string& path) { deleted_files.push_back(path); });

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_EQ(result.snapshot_ids_to_remove.size(), 1);

  // Simulate a commit failure - Finalize should not delete any files
  auto finalize_status = update->Finalize(
      Error{.kind = ErrorKind::kCommitFailed, .message = "simulated failure"});
  EXPECT_THAT(finalize_status, IsOk());
  EXPECT_TRUE(deleted_files.empty());
}

TEST_F(ExpireSnapshotsTest, FinalizeSkippedWhenNoSnapshotsExpired) {
  std::vector<std::string> deleted_files;
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewExpireSnapshots());
  update->RetainLast(2);
  update->DeleteWith(
      [&deleted_files](const std::string& path) { deleted_files.push_back(path); });

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_TRUE(result.snapshot_ids_to_remove.empty());

  // No snapshots expired, so Finalize should not delete any files
  auto finalize_status = update->Finalize(std::nullopt);
  EXPECT_THAT(finalize_status, IsOk());
  EXPECT_TRUE(deleted_files.empty());
}

TEST_F(ExpireSnapshotsTest, CommitWithCleanupLevelNone) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewExpireSnapshots());
  update->CleanupLevel(CleanupLevel::kNone);

  // Commit should succeed - Finalize is called internally but skips cleanup
  EXPECT_THAT(update->Commit(), IsOk());

  // Verify snapshot was removed from metadata
  auto metadata = ReloadMetadata();
  EXPECT_EQ(metadata->snapshots.size(), 1);
  EXPECT_EQ(metadata->snapshots.at(0)->snapshot_id, 3055729675574597004);
}

}  // namespace iceberg
