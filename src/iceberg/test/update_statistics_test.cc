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

#include "iceberg/update_statistics.h"

#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/result.h"
#include "iceberg/statistics_file.h"
#include "iceberg/test/matchers.h"

namespace iceberg {

// Mock implementation of UpdateStatistics for testing
// This mock tracks which methods were called to verify behavior
class MockUpdateStatistics : public UpdateStatistics {
 public:
  MockUpdateStatistics() = default;

  Result<std::vector<StatisticsFile>> Apply() override {
    if (should_fail_) {
      return ValidationFailed("Mock validation failed");
    }
    apply_called_ = true;

    // Return a vector of statistics files that reflects the configuration
    // In a real implementation, this would return the statistics files
    // that were set or modified
    return statistics_files_;
  }

  Status Commit() override {
    if (should_fail_commit_) {
      return CommitFailed("Mock commit failed");
    }
    commit_called_ = true;
    return {};
  }

  UpdateStatistics& SetStatistics(const StatisticsFile& statistics_file) override {
    statistics_files_.push_back(statistics_file);
    return *this;
  }

  UpdateStatistics& RemoveStatistics(int64_t snapshot_id) override {
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

  std::vector<StatisticsFile> statistics_files_;
  std::vector<int64_t> removed_snapshot_ids_;
};

TEST(UpdateStatisticsTest, SetStatistics) {
  MockUpdateStatistics update;
  StatisticsFile stats{
      .snapshot_id = 100,
      .path = "s3://bucket/metadata/stats-100.puffin",
      .file_size_in_bytes = 1024,
      .file_footer_size_in_bytes = 128,
      .blob_metadata = {},
  };

  update.SetStatistics(stats);

  // Verify through public API: Apply() should return statistics files
  auto result = update.Apply();
  ASSERT_THAT(result, IsOk());

  const auto& files = result.value();
  EXPECT_EQ(files.size(), 1);
  EXPECT_EQ(files[0].snapshot_id, 100);
  EXPECT_EQ(files[0].path, "s3://bucket/metadata/stats-100.puffin");
}

TEST(UpdateStatisticsTest, SetMultipleStatistics) {
  MockUpdateStatistics update;
  StatisticsFile stats1{
      .snapshot_id = 100,
      .path = "s3://bucket/metadata/stats-100.puffin",
      .file_size_in_bytes = 1024,
      .file_footer_size_in_bytes = 128,
      .blob_metadata = {},
  };
  StatisticsFile stats2{
      .snapshot_id = 200,
      .path = "s3://bucket/metadata/stats-200.puffin",
      .file_size_in_bytes = 2048,
      .file_footer_size_in_bytes = 256,
      .blob_metadata = {},
  };

  update.SetStatistics(stats1).SetStatistics(stats2);

  // Verify through public API
  auto result = update.Apply();
  ASSERT_THAT(result, IsOk());

  const auto& files = result.value();
  EXPECT_EQ(files.size(), 2);
  EXPECT_EQ(files[0].snapshot_id, 100);
  EXPECT_EQ(files[1].snapshot_id, 200);
}

TEST(UpdateStatisticsTest, RemoveStatistics) {
  MockUpdateStatistics update;
  update.RemoveStatistics(100);

  // Just verify it doesn't error
  auto result = update.Apply();
  EXPECT_THAT(result, IsOk());
}

TEST(UpdateStatisticsTest, RemoveMultipleStatistics) {
  MockUpdateStatistics update;
  update.RemoveStatistics(100).RemoveStatistics(200).RemoveStatistics(300);

  // Verify it doesn't error
  auto result = update.Apply();
  EXPECT_THAT(result, IsOk());
}

TEST(UpdateStatisticsTest, MethodChaining) {
  MockUpdateStatistics update;
  StatisticsFile stats{
      .snapshot_id = 100,
      .path = "s3://bucket/metadata/stats-100.puffin",
      .file_size_in_bytes = 1024,
      .file_footer_size_in_bytes = 128,
      .blob_metadata = {},
  };

  // Test that all methods return the reference for chaining
  update.SetStatistics(stats).RemoveStatistics(50).RemoveStatistics(75);

  // Verify through public API
  auto result = update.Apply();
  ASSERT_THAT(result, IsOk());

  const auto& files = result.value();
  EXPECT_EQ(files.size(), 1);
  EXPECT_EQ(files[0].snapshot_id, 100);
}

TEST(UpdateStatisticsTest, SetStatisticsWithBlobMetadata) {
  MockUpdateStatistics update;
  BlobMetadata blob{
      .type = "ndv",
      .source_snapshot_id = 100,
      .source_snapshot_sequence_number = 1,
      .fields = {1, 2, 3},
      .properties = {{"key1", "value1"}, {"key2", "value2"}},
  };

  StatisticsFile stats{
      .snapshot_id = 100,
      .path = "s3://bucket/metadata/stats-100.puffin",
      .file_size_in_bytes = 1024,
      .file_footer_size_in_bytes = 128,
      .blob_metadata = {blob},
  };

  update.SetStatistics(stats);

  auto result = update.Apply();
  ASSERT_THAT(result, IsOk());

  const auto& files = result.value();
  EXPECT_EQ(files.size(), 1);
  EXPECT_EQ(files[0].blob_metadata.size(), 1);
  EXPECT_EQ(files[0].blob_metadata[0].type, "ndv");
  EXPECT_EQ(files[0].blob_metadata[0].fields.size(), 3);
}

TEST(UpdateStatisticsTest, ApplySuccess) {
  MockUpdateStatistics update;
  auto result = update.Apply();
  EXPECT_THAT(result, IsOk());
  EXPECT_TRUE(update.ApplyCalled());
}

TEST(UpdateStatisticsTest, ApplyValidationFailed) {
  MockUpdateStatistics update;
  update.SetShouldFail(true);
  auto result = update.Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Mock validation failed"));
}

TEST(UpdateStatisticsTest, CommitSuccess) {
  MockUpdateStatistics update;
  auto status = update.Commit();
  EXPECT_THAT(status, IsOk());
  EXPECT_TRUE(update.CommitCalled());
}

TEST(UpdateStatisticsTest, CommitFailed) {
  MockUpdateStatistics update;
  update.SetShouldFailCommit(true);
  auto status = update.Commit();
  EXPECT_THAT(status, IsError(ErrorKind::kCommitFailed));
  EXPECT_THAT(status, HasErrorMessage("Mock commit failed"));
}

TEST(UpdateStatisticsTest, InheritanceFromPendingUpdate) {
  std::unique_ptr<PendingUpdate> base_ptr = std::make_unique<MockUpdateStatistics>();
  auto status = base_ptr->Commit();
  EXPECT_THAT(status, IsOk());
}

TEST(UpdateStatisticsTest, InheritanceFromPendingUpdateTyped) {
  std::unique_ptr<PendingUpdateTyped<std::vector<StatisticsFile>>> typed_ptr =
      std::make_unique<MockUpdateStatistics>();
  auto status = typed_ptr->Commit();
  EXPECT_THAT(status, IsOk());

  auto result = typed_ptr->Apply();
  EXPECT_THAT(result, IsOk());
}

}  // namespace iceberg
