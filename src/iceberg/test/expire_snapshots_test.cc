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

#include "iceberg/expire_snapshots.h"

#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/result.h"
#include "iceberg/snapshot.h"
#include "iceberg/test/matchers.h"

namespace iceberg {

// Mock implementation of ExpireSnapshots for testing
// This mock tracks which methods were called to verify behavior
class MockExpireSnapshots : public ExpireSnapshots {
 public:
  MockExpireSnapshots() = default;

  Result<std::vector<std::shared_ptr<Snapshot>>> Apply() override {
    if (should_fail_) {
      return ValidationFailed("Mock validation failed");
    }
    apply_called_ = true;

    // Return a vector of snapshots that reflects the configuration
    // In a real implementation, this would analyze the table and return
    // snapshots that match the expiration criteria
    std::vector<std::shared_ptr<Snapshot>> expired_snapshots;

    // Create mock snapshots for snapshots that would be expired
    for (int64_t id : snapshot_ids_to_expire_) {
      expired_snapshots.push_back(std::make_shared<Snapshot>(Snapshot{
          .snapshot_id = id,
          .parent_snapshot_id = std::nullopt,
          .sequence_number = 1,
          .timestamp_ms = TimePointMs{std::chrono::milliseconds{1000}},
          .manifest_list = "s3://bucket/metadata/snap-manifest-list.avro",
          .summary = {},
          .schema_id = std::nullopt,
      }));
    }

    return expired_snapshots;
  }

  Status Commit() override {
    if (should_fail_commit_) {
      return CommitFailed("Mock commit failed");
    }
    commit_called_ = true;

    // Simulate file deletion if callback is set
    if (delete_func_) {
      // In a real implementation, this would delete manifest and data files
      // For testing, just call the callback with test files
      (*delete_func_)("manifest-1.avro");
      (*delete_func_)("data-1.parquet");
    }

    return {};
  }

  ExpireSnapshots& ExpireSnapshotId(int64_t snapshot_id) override {
    snapshot_ids_to_expire_.push_back(snapshot_id);
    return *this;
  }

  ExpireSnapshots& ExpireOlderThan(int64_t timestamp_millis) override {
    expire_older_than_ms_ = timestamp_millis;
    return *this;
  }

  ExpireSnapshots& RetainLast(int num_snapshots) override {
    retain_last_ = num_snapshots;
    return *this;
  }

  ExpireSnapshots& DeleteWith(
      std::function<void(std::string_view)> delete_func) override {
    delete_func_ = std::move(delete_func);
    return *this;
  }

  ExpireSnapshots& CleanupLevel(enum CleanupLevel level) override {
    cleanup_level_ = level;
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

  std::vector<int64_t> snapshot_ids_to_expire_;
  std::optional<int64_t> expire_older_than_ms_;
  std::optional<int> retain_last_;
  std::optional<std::function<void(std::string_view)>> delete_func_;
  std::optional<enum CleanupLevel> cleanup_level_;
};

TEST(ExpireSnapshotsTest, ExpireSnapshotId) {
  MockExpireSnapshots expire;
  expire.ExpireSnapshotId(123);

  // Verify through public API: Apply() should return snapshots to expire
  auto result = expire.Apply();
  ASSERT_THAT(result, IsOk());

  const auto& snapshots = result.value();
  EXPECT_EQ(snapshots.size(), 1);
  EXPECT_EQ(snapshots[0]->snapshot_id, 123);
}

TEST(ExpireSnapshotsTest, ExpireMultipleSnapshotIds) {
  MockExpireSnapshots expire;
  expire.ExpireSnapshotId(100).ExpireSnapshotId(200).ExpireSnapshotId(300);

  // Verify through public API
  auto result = expire.Apply();
  ASSERT_THAT(result, IsOk());

  const auto& snapshots = result.value();
  EXPECT_EQ(snapshots.size(), 3);
  EXPECT_EQ(snapshots[0]->snapshot_id, 100);
  EXPECT_EQ(snapshots[1]->snapshot_id, 200);
  EXPECT_EQ(snapshots[2]->snapshot_id, 300);
}

TEST(ExpireSnapshotsTest, ExpireOlderThan) {
  MockExpireSnapshots expire;
  int64_t timestamp_millis = 1609459200000;  // 2021-01-01 00:00:00
  expire.ExpireOlderThan(timestamp_millis);

  // Just verify it doesn't error - timestamp filtering is implementation detail
  auto result = expire.Apply();
  EXPECT_THAT(result, IsOk());
}

TEST(ExpireSnapshotsTest, RetainLast) {
  MockExpireSnapshots expire;
  expire.RetainLast(5);

  // Verify it doesn't error
  auto result = expire.Apply();
  EXPECT_THAT(result, IsOk());
}

TEST(ExpireSnapshotsTest, DeleteWith) {
  MockExpireSnapshots expire;
  std::vector<std::string> deleted_files;

  // Set up callback to track deleted files
  expire.DeleteWith(
      [&deleted_files](std::string_view path) { deleted_files.emplace_back(path); });

  // Verify through public API: calling Commit() should invoke the callback
  auto status = expire.Commit();
  EXPECT_THAT(status, IsOk());

  // The mock implementation calls the delete callback with test files
  EXPECT_EQ(deleted_files.size(), 2);
  EXPECT_EQ(deleted_files[0], "manifest-1.avro");
  EXPECT_EQ(deleted_files[1], "data-1.parquet");
}

TEST(ExpireSnapshotsTest, CleanupLevelNone) {
  MockExpireSnapshots expire;
  expire.CleanupLevel(CleanupLevel::kNone);

  // Just verify it doesn't error
  auto status = expire.Commit();
  EXPECT_THAT(status, IsOk());
}

TEST(ExpireSnapshotsTest, CleanupLevelMetadataOnly) {
  MockExpireSnapshots expire;
  expire.CleanupLevel(CleanupLevel::kMetadataOnly);

  auto status = expire.Commit();
  EXPECT_THAT(status, IsOk());
}

TEST(ExpireSnapshotsTest, CleanupLevelAll) {
  MockExpireSnapshots expire;
  expire.CleanupLevel(CleanupLevel::kAll);

  auto status = expire.Commit();
  EXPECT_THAT(status, IsOk());
}

TEST(ExpireSnapshotsTest, MethodChaining) {
  MockExpireSnapshots expire;

  // Test that all methods return the reference for chaining
  expire.ExpireSnapshotId(100)
      .ExpireSnapshotId(200)
      .ExpireOlderThan(1609459200000)
      .RetainLast(5)
      .CleanupLevel(CleanupLevel::kMetadataOnly);

  // Verify through public API
  auto result = expire.Apply();
  ASSERT_THAT(result, IsOk());

  const auto& snapshots = result.value();
  EXPECT_EQ(snapshots.size(), 2);
}

TEST(ExpireSnapshotsTest, MethodChainingWithAllMethods) {
  MockExpireSnapshots expire;
  std::vector<std::string> deleted_files;

  // Chain all builder methods together
  expire.ExpireSnapshotId(100)
      .ExpireOlderThan(1609459200000)
      .RetainLast(5)
      .DeleteWith(
          [&deleted_files](std::string_view path) { deleted_files.emplace_back(path); })
      .CleanupLevel(CleanupLevel::kAll);

  // Verify through Apply()
  auto result = expire.Apply();
  ASSERT_THAT(result, IsOk());
  EXPECT_EQ(result.value().size(), 1);

  // Verify through Commit()
  auto status = expire.Commit();
  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(deleted_files.size(), 2);
}

TEST(ExpireSnapshotsTest, ApplySuccess) {
  MockExpireSnapshots expire;
  auto result = expire.Apply();
  EXPECT_THAT(result, IsOk());
  EXPECT_TRUE(expire.ApplyCalled());
}

TEST(ExpireSnapshotsTest, ApplyValidationFailed) {
  MockExpireSnapshots expire;
  expire.SetShouldFail(true);
  auto result = expire.Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Mock validation failed"));
}

TEST(ExpireSnapshotsTest, CommitSuccess) {
  MockExpireSnapshots expire;
  auto status = expire.Commit();
  EXPECT_THAT(status, IsOk());
  EXPECT_TRUE(expire.CommitCalled());
}

TEST(ExpireSnapshotsTest, CommitFailed) {
  MockExpireSnapshots expire;
  expire.SetShouldFailCommit(true);
  auto status = expire.Commit();
  EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(status, HasErrorMessage("Mock commit failed"));
}

TEST(ExpireSnapshotsTest, InheritanceFromPendingUpdate) {
  std::unique_ptr<PendingUpdate> base_ptr = std::make_unique<MockExpireSnapshots>();
  auto status = base_ptr->Commit();
  EXPECT_THAT(status, IsOk());
}

TEST(ExpireSnapshotsTest, InheritanceFromPendingUpdateTyped) {
  std::unique_ptr<PendingUpdateTyped<std::vector<std::shared_ptr<Snapshot>>>> typed_ptr =
      std::make_unique<MockExpireSnapshots>();
  auto status = typed_ptr->Commit();
  EXPECT_THAT(status, IsOk());

  auto result = typed_ptr->Apply();
  EXPECT_THAT(result, IsOk());
}

}  // namespace iceberg
