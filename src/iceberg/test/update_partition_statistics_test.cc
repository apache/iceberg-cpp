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

#include "iceberg/update/update_partition_statistics.h"

#include <algorithm>
#include <memory>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/result.h"
#include "iceberg/statistics_file.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/update_test_base.h"

namespace iceberg {

class UpdatePartitionStatisticsTest : public UpdateTestBase {
 protected:
  // Helper function to create a partition statistics file
  std::shared_ptr<PartitionStatisticsFile> MakePartitionStatisticsFile(
      int64_t snapshot_id, const std::string& path, int64_t file_size = 2048) {
    auto stats_file = std::make_shared<PartitionStatisticsFile>();
    stats_file->snapshot_id = snapshot_id;
    stats_file->path = path;
    stats_file->file_size_in_bytes = file_size;
    return stats_file;
  }

  // Helper to find partition statistics file by snapshot_id in the result vector
  std::shared_ptr<PartitionStatisticsFile> FindPartitionStatistics(
      const std::vector<std::pair<int64_t, std::shared_ptr<PartitionStatisticsFile>>>&
          to_set,
      int64_t snapshot_id) {
    auto it = std::ranges::find_if(
        to_set, [snapshot_id](const auto& p) { return p.first == snapshot_id; });
    return it != to_set.end() ? it->second : nullptr;
  }
};

TEST_F(UpdatePartitionStatisticsTest, EmptyUpdate) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdatePartitionStatistics());
  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_TRUE(result.to_set.empty());
  EXPECT_TRUE(result.to_remove.empty());
}

TEST_F(UpdatePartitionStatisticsTest, SetPartitionStatistics) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdatePartitionStatistics());
  auto partition_stats_file = MakePartitionStatisticsFile(
      1, "/warehouse/test_table/metadata/partition-stats-1.parquet");
  update->SetPartitionStatistics(partition_stats_file);

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_EQ(result.to_set.size(), 1);
  EXPECT_TRUE(result.to_remove.empty());

  auto found = FindPartitionStatistics(result.to_set, 1);
  ASSERT_NE(found, nullptr);
  EXPECT_EQ(found->snapshot_id, 1);
  EXPECT_EQ(found->path, "/warehouse/test_table/metadata/partition-stats-1.parquet");
  EXPECT_EQ(found->file_size_in_bytes, 2048);
}

TEST_F(UpdatePartitionStatisticsTest, SetMultiplePartitionStatistics) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdatePartitionStatistics());

  auto partition_stats_file1 = MakePartitionStatisticsFile(
      1, "/warehouse/test_table/metadata/partition-stats-1.parquet");
  auto partition_stats_file2 = MakePartitionStatisticsFile(
      2, "/warehouse/test_table/metadata/partition-stats-2.parquet", 4096);

  update->SetPartitionStatistics(partition_stats_file1);
  update->SetPartitionStatistics(partition_stats_file2);

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_EQ(result.to_set.size(), 2);
  EXPECT_TRUE(result.to_remove.empty());

  auto found1 = FindPartitionStatistics(result.to_set, 1);
  ASSERT_NE(found1, nullptr);
  EXPECT_EQ(found1->snapshot_id, 1);

  auto found2 = FindPartitionStatistics(result.to_set, 2);
  ASSERT_NE(found2, nullptr);
  EXPECT_EQ(found2->snapshot_id, 2);
  EXPECT_EQ(found2->file_size_in_bytes, 4096);
}

TEST_F(UpdatePartitionStatisticsTest, ReplacePartitionStatistics) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdatePartitionStatistics());

  auto partition_stats_file1 = MakePartitionStatisticsFile(
      1, "/warehouse/test_table/metadata/partition-stats-1.parquet");
  auto partition_stats_file2 = MakePartitionStatisticsFile(
      1, "/warehouse/test_table/metadata/partition-stats-1-updated.parquet", 8192);

  update->SetPartitionStatistics(partition_stats_file1);
  update->SetPartitionStatistics(partition_stats_file2);

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_EQ(result.to_set.size(), 1);
  EXPECT_TRUE(result.to_remove.empty());

  auto found = FindPartitionStatistics(result.to_set, 1);
  ASSERT_NE(found, nullptr);
  EXPECT_EQ(found->path,
            "/warehouse/test_table/metadata/partition-stats-1-updated.parquet");
  EXPECT_EQ(found->file_size_in_bytes, 8192);
}

TEST_F(UpdatePartitionStatisticsTest, RemovePartitionStatistics) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdatePartitionStatistics());
  update->RemovePartitionStatistics(1);

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_TRUE(result.to_set.empty());
  EXPECT_EQ(result.to_remove.size(), 1);
  EXPECT_EQ(result.to_remove[0], 1);
}

TEST_F(UpdatePartitionStatisticsTest, SetThenRemovePartitionStatistics) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdatePartitionStatistics());

  auto partition_stats_file = MakePartitionStatisticsFile(
      1, "/warehouse/test_table/metadata/partition-stats-1.parquet");
  update->SetPartitionStatistics(partition_stats_file);
  update->RemovePartitionStatistics(1);

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_TRUE(result.to_set.empty());
  EXPECT_EQ(result.to_remove.size(), 1);
  EXPECT_EQ(result.to_remove[0], 1);
}

TEST_F(UpdatePartitionStatisticsTest, SetNullPartitionStatistics) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdatePartitionStatistics());

  update->SetPartitionStatistics(nullptr);

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Statistics file cannot be null"));
}

TEST_F(UpdatePartitionStatisticsTest, SetAndRemoveMixed) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdatePartitionStatistics());

  auto partition_stats_file1 = MakePartitionStatisticsFile(
      1, "/warehouse/test_table/metadata/partition-stats-1.parquet");
  auto partition_stats_file2 = MakePartitionStatisticsFile(
      2, "/warehouse/test_table/metadata/partition-stats-2.parquet");

  update->SetPartitionStatistics(partition_stats_file1);
  update->SetPartitionStatistics(partition_stats_file2);
  update->RemovePartitionStatistics(3);

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_EQ(result.to_set.size(), 2);
  EXPECT_EQ(result.to_remove.size(), 1);
  EXPECT_EQ(result.to_remove[0], 3);
}

}  // namespace iceberg
