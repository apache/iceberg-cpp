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

#include "iceberg/snapshot_update.h"

#include <gtest/gtest.h>

#include "iceberg/result.h"
#include "iceberg/snapshot.h"
#include "iceberg/test/matchers.h"

namespace iceberg {

// Mock implementation of SnapshotUpdate for testing
// This mock tracks which methods were called to verify behavior
class MockSnapshotUpdate : public SnapshotUpdate<MockSnapshotUpdate> {
 public:
  MockSnapshotUpdate() = default;

  Result<Snapshot> Apply() override {
    if (should_fail_) {
      return ValidationFailed("Mock validation failed");
    }
    apply_called_ = true;

    // Return a Snapshot that reflects the configuration set via builder methods
    // This allows us to verify behavior through the public API
    return Snapshot{
        .snapshot_id = 1,
        .parent_snapshot_id = std::nullopt,
        .sequence_number = 1,
        .timestamp_ms = TimePointMs{std::chrono::milliseconds{1000}},
        .manifest_list = "s3://bucket/metadata/snap-1-manifest-list.avro",
        .summary = summary_,  // Summary is populated by Set() calls
        .schema_id = std::nullopt,
    };
  }

  Status Commit() override {
    if (should_fail_commit_) {
      return CommitFailed("Mock commit failed");
    }
    commit_called_ = true;

    // Simulate file deletion if callback is set
    if (delete_func_) {
      // In a real implementation, this would delete actual files
      // For testing, just call the callback
      (*delete_func_)("test-file-to-delete.parquet");
    }

    return {};
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
};

TEST(SnapshotUpdateTest, SetSummaryProperty) {
  MockSnapshotUpdate update;
  update.Set("operation", "append");

  // Verify through public API: the snapshot from Apply() should have the summary
  auto result = update.Apply();
  ASSERT_THAT(result, IsOk());

  const auto& snapshot = result.value();
  EXPECT_EQ(snapshot.summary.size(), 1);
  EXPECT_EQ(snapshot.summary.at("operation"), "append");
}

TEST(SnapshotUpdateTest, SetMultipleSummaryProperties) {
  MockSnapshotUpdate update;
  update.Set("operation", "append").Set("added-files-count", "5");

  // Verify through public API
  auto result = update.Apply();
  ASSERT_THAT(result, IsOk());

  const auto& snapshot = result.value();
  EXPECT_EQ(snapshot.summary.size(), 2);
  EXPECT_EQ(snapshot.summary.at("operation"), "append");
  EXPECT_EQ(snapshot.summary.at("added-files-count"), "5");
}

TEST(SnapshotUpdateTest, DeleteWith) {
  MockSnapshotUpdate update;
  std::vector<std::string> deleted_files;

  // Set up callback to track deleted files
  update.DeleteWith([&deleted_files](std::string_view path) {
    deleted_files.emplace_back(path);
  });

  // Verify through public API: calling Commit() should invoke the callback
  auto status = update.Commit();
  EXPECT_THAT(status, IsOk());

  // The mock implementation calls the delete callback with a test file
  EXPECT_EQ(deleted_files.size(), 1);
  EXPECT_EQ(deleted_files[0], "test-file-to-delete.parquet");
}

TEST(SnapshotUpdateTest, MethodChaining) {
  MockSnapshotUpdate update;

  // Test that all methods return the derived type for chaining
  update.Set("operation", "append")
      .Set("added-files-count", "5")
      .Set("added-records", "1000")
      .StageOnly();

  // Verify through public API
  auto result = update.Apply();
  ASSERT_THAT(result, IsOk());

  const auto& snapshot = result.value();
  EXPECT_EQ(snapshot.summary.size(), 3);
  EXPECT_EQ(snapshot.summary.at("operation"), "append");
  EXPECT_EQ(snapshot.summary.at("added-files-count"), "5");
  EXPECT_EQ(snapshot.summary.at("added-records"), "1000");
}

TEST(SnapshotUpdateTest, MethodChainingWithAllMethods) {
  MockSnapshotUpdate update;
  std::vector<std::string> deleted_files;

  // Chain all builder methods together
  update.Set("operation", "append")
      .Set("added-files-count", "5")
      .DeleteWith([&deleted_files](std::string_view path) {
        deleted_files.emplace_back(path);
      })
      .ToBranch("wap-branch")
      .StageOnly();

  // Verify through Apply()
  auto result = update.Apply();
  ASSERT_THAT(result, IsOk());

  const auto& snapshot = result.value();
  EXPECT_EQ(snapshot.summary.at("operation"), "append");
  EXPECT_EQ(snapshot.summary.at("added-files-count"), "5");

  // Verify through Commit()
  auto status = update.Commit();
  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(deleted_files.size(), 1);
}

TEST(SnapshotUpdateTest, ApplySuccess) {
  MockSnapshotUpdate update;
  auto result = update.Apply();
  EXPECT_THAT(result, IsOk());
  EXPECT_TRUE(update.ApplyCalled());
}

TEST(SnapshotUpdateTest, ApplyValidationFailed) {
  MockSnapshotUpdate update;
  update.SetShouldFail(true);
  auto result = update.Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Mock validation failed"));
}

TEST(SnapshotUpdateTest, CommitSuccess) {
  MockSnapshotUpdate update;
  auto status = update.Commit();
  EXPECT_THAT(status, IsOk());
  EXPECT_TRUE(update.CommitCalled());
}

TEST(SnapshotUpdateTest, CommitFailed) {
  MockSnapshotUpdate update;
  update.SetShouldFailCommit(true);
  auto status = update.Commit();
  EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(status, HasErrorMessage("Mock commit failed"));
}

TEST(SnapshotUpdateTest, InheritanceFromPendingUpdate) {
  std::unique_ptr<PendingUpdate> base_ptr = std::make_unique<MockSnapshotUpdate>();
  auto status = base_ptr->Commit();
  EXPECT_THAT(status, IsOk());
}

TEST(SnapshotUpdateTest, InheritanceFromPendingUpdateTyped) {
  std::unique_ptr<PendingUpdateTyped<Snapshot>> typed_ptr =
      std::make_unique<MockSnapshotUpdate>();
  auto status = typed_ptr->Commit();
  EXPECT_THAT(status, IsOk());

  auto result = typed_ptr->Apply();
  EXPECT_THAT(result, IsOk());
}

}  // namespace iceberg
