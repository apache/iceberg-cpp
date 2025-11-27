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

#include "iceberg/update_partition_statistics.h"

#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/result.h"
#include "iceberg/statistics_file.h"
#include "iceberg/test/matchers.h"

namespace iceberg {

// Mock implementation of UpdatePartitionStatistics for testing
// This mock tracks which methods were called to verify behavior
class MockUpdatePartitionStatistics : public UpdatePartitionStatistics {
 public:
  MockUpdatePartitionStatistics() = default;

  Result<std::vector<PartitionStatisticsFile>> Apply() override {
    if (should_fail_) {
      return ValidationFailed("Mock validation failed");
    }
    apply_called_ = true;

    // Return a vector of partition statistics files that reflects the configuration
    // In a real implementation, this would return the partition statistics files
    // that were set or modified
    return partition_statistics_files_;
  }

  Status Commit() override {
    if (should_fail_commit_) {
      return CommitFailed("Mock commit failed");
    }
    commit_called_ = true;
    return {};
  }

  UpdatePartitionStatistics& SetPartitionStatistics(
      const PartitionStatisticsFile& partition_statistics_file) override {
    partition_statistics_files_.push_back(partition_statistics_file);
    return *this;
  }

  UpdatePartitionStatistics& RemovePartitionStatistics(int64_t snapshot_id) override {
    removed_snapshot_ids_.push_back(snapshot_id);
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

  std::vector<PartitionStatisticsFile> partition_statistics_files_;
  std::vector<int64_t> removed_snapshot_ids_;
};

TEST(UpdatePartitionStatisticsTest, SetPartitionStatistics) {
  MockUpdatePartitionStatistics update;
  PartitionStatisticsFile stats{
      .snapshot_id = 100,
      .path = "s3://bucket/metadata/partition-stats-100.parquet",
      .file_size_in_bytes = 2048,
  };

  update.SetPartitionStatistics(stats);

  // Verify through public API: Apply() should return partition statistics files
  auto result = update.Apply();
  ASSERT_THAT(result, IsOk());

  const auto& files = result.value();
  EXPECT_EQ(files.size(), 1);
  EXPECT_EQ(files[0].snapshot_id, 100);
  EXPECT_EQ(files[0].path, "s3://bucket/metadata/partition-stats-100.parquet");
  EXPECT_EQ(files[0].file_size_in_bytes, 2048);
}

TEST(UpdatePartitionStatisticsTest, SetMultiplePartitionStatistics) {
  MockUpdatePartitionStatistics update;
  PartitionStatisticsFile stats1{
      .snapshot_id = 100,
      .path = "s3://bucket/metadata/partition-stats-100.parquet",
      .file_size_in_bytes = 2048,
  };
  PartitionStatisticsFile stats2{
      .snapshot_id = 200,
      .path = "s3://bucket/metadata/partition-stats-200.parquet",
      .file_size_in_bytes = 4096,
  };

  update.SetPartitionStatistics(stats1).SetPartitionStatistics(stats2);

  // Verify through public API
  auto result = update.Apply();
  ASSERT_THAT(result, IsOk());

  const auto& files = result.value();
  EXPECT_EQ(files.size(), 2);
  EXPECT_EQ(files[0].snapshot_id, 100);
  EXPECT_EQ(files[1].snapshot_id, 200);
}

TEST(UpdatePartitionStatisticsTest, RemovePartitionStatistics) {
  MockUpdatePartitionStatistics update;
  update.RemovePartitionStatistics(100);

  // Just verify it doesn't error
  auto result = update.Apply();
  EXPECT_THAT(result, IsOk());
}

TEST(UpdatePartitionStatisticsTest, RemoveMultiplePartitionStatistics) {
  MockUpdatePartitionStatistics update;
  update.RemovePartitionStatistics(100)
      .RemovePartitionStatistics(200)
      .RemovePartitionStatistics(300);

  // Verify it doesn't error
  auto result = update.Apply();
  EXPECT_THAT(result, IsOk());
}

TEST(UpdatePartitionStatisticsTest, MethodChaining) {
  MockUpdatePartitionStatistics update;
  PartitionStatisticsFile stats{
      .snapshot_id = 100,
      .path = "s3://bucket/metadata/partition-stats-100.parquet",
      .file_size_in_bytes = 2048,
  };

  // Test that all methods return the reference for chaining
  update.SetPartitionStatistics(stats)
      .RemovePartitionStatistics(50)
      .RemovePartitionStatistics(75);

  // Verify through public API
  auto result = update.Apply();
  ASSERT_THAT(result, IsOk());

  const auto& files = result.value();
  EXPECT_EQ(files.size(), 1);
  EXPECT_EQ(files[0].snapshot_id, 100);
}

TEST(UpdatePartitionStatisticsTest, ApplySuccess) {
  MockUpdatePartitionStatistics update;
  auto result = update.Apply();
  EXPECT_THAT(result, IsOk());
  EXPECT_TRUE(update.ApplyCalled());
}

TEST(UpdatePartitionStatisticsTest, ApplyValidationFailed) {
  MockUpdatePartitionStatistics update;
  update.SetShouldFail(true);
  auto result = update.Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Mock validation failed"));
}

TEST(UpdatePartitionStatisticsTest, CommitSuccess) {
  MockUpdatePartitionStatistics update;
  auto status = update.Commit();
  EXPECT_THAT(status, IsOk());
  EXPECT_TRUE(update.CommitCalled());
}

TEST(UpdatePartitionStatisticsTest, CommitFailed) {
  MockUpdatePartitionStatistics update;
  update.SetShouldFailCommit(true);
  auto status = update.Commit();
  EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(status, HasErrorMessage("Mock commit failed"));
}

TEST(UpdatePartitionStatisticsTest, InheritanceFromPendingUpdate) {
  std::unique_ptr<PendingUpdate> base_ptr =
      std::make_unique<MockUpdatePartitionStatistics>();
  auto status = base_ptr->Commit();
  EXPECT_THAT(status, IsOk());
}

TEST(UpdatePartitionStatisticsTest, InheritanceFromPendingUpdateTyped) {
  std::unique_ptr<PendingUpdateTyped<std::vector<PartitionStatisticsFile>>> typed_ptr =
      std::make_unique<MockUpdatePartitionStatistics>();
  auto status = typed_ptr->Commit();
  EXPECT_THAT(status, IsOk());

  auto result = typed_ptr->Apply();
  EXPECT_THAT(result, IsOk());
}

}  // namespace iceberg
