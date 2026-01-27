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

#include "iceberg/update/update_statistics.h"

#include <algorithm>
#include <memory>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/result.h"
#include "iceberg/statistics_file.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/update_test_base.h"

namespace iceberg {

class UpdateStatisticsTest : public UpdateTestBase {
 protected:
  // Helper function to create a statistics file
  std::shared_ptr<StatisticsFile> MakeStatisticsFile(int64_t snapshot_id,
                                                     const std::string& path,
                                                     int64_t file_size = 1024,
                                                     int64_t footer_size = 128) {
    auto stats_file = std::make_shared<StatisticsFile>();
    stats_file->snapshot_id = snapshot_id;
    stats_file->path = path;
    stats_file->file_size_in_bytes = file_size;
    stats_file->file_footer_size_in_bytes = footer_size;

    BlobMetadata blob;
    blob.type = "apache-datasketches-theta-v1";
    blob.source_snapshot_id = snapshot_id;
    blob.source_snapshot_sequence_number = 1;
    blob.fields = {1, 2};
    blob.properties = {{"ndv", "100"}};
    stats_file->blob_metadata.push_back(blob);

    return stats_file;
  }

  // Helper to find statistics file by snapshot_id in the result vector
  std::shared_ptr<StatisticsFile> FindStatistics(
      const std::vector<std::pair<int64_t, std::shared_ptr<StatisticsFile>>>& to_set,
      int64_t snapshot_id) {
    auto it = std::ranges::find_if(
        to_set, [snapshot_id](const auto& p) { return p.first == snapshot_id; });
    return it != to_set.end() ? it->second : nullptr;
  }
};

TEST_F(UpdateStatisticsTest, EmptyUpdate) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateStatistics());
  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_TRUE(result.to_set.empty());
  EXPECT_TRUE(result.to_remove.empty());
}

TEST_F(UpdateStatisticsTest, SetStatistics) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateStatistics());
  auto stats_file =
      MakeStatisticsFile(1, "/warehouse/test_table/metadata/stats-1.puffin");
  update->SetStatistics(stats_file);

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_EQ(result.to_set.size(), 1);
  EXPECT_TRUE(result.to_remove.empty());
  EXPECT_EQ(FindStatistics(result.to_set, 1), stats_file);
}

TEST_F(UpdateStatisticsTest, SetMultipleStatistics) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateStatistics());
  auto stats_file_1 =
      MakeStatisticsFile(1, "/warehouse/test_table/metadata/stats-1.puffin");
  auto stats_file_2 =
      MakeStatisticsFile(2, "/warehouse/test_table/metadata/stats-2.puffin");

  update->SetStatistics(stats_file_1).SetStatistics(stats_file_2);

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_EQ(result.to_set.size(), 2);
  EXPECT_TRUE(result.to_remove.empty());
  EXPECT_EQ(FindStatistics(result.to_set, 1), stats_file_1);
  EXPECT_EQ(FindStatistics(result.to_set, 2), stats_file_2);
}

TEST_F(UpdateStatisticsTest, RemoveStatistics) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateStatistics());
  update->RemoveStatistics(1);

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_TRUE(result.to_set.empty());
  EXPECT_EQ(result.to_remove.size(), 1);
  EXPECT_THAT(result.to_remove, ::testing::Contains(1));
}

TEST_F(UpdateStatisticsTest, RemoveMultipleStatistics) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateStatistics());
  update->RemoveStatistics(1).RemoveStatistics(2).RemoveStatistics(3);

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_TRUE(result.to_set.empty());
  EXPECT_EQ(result.to_remove.size(), 3);
  EXPECT_THAT(result.to_remove, ::testing::UnorderedElementsAre(1, 2, 3));
}

TEST_F(UpdateStatisticsTest, SetAndRemoveDifferentSnapshots) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateStatistics());
  auto stats_file =
      MakeStatisticsFile(1, "/warehouse/test_table/metadata/stats-1.puffin");

  update->SetStatistics(stats_file).RemoveStatistics(2);

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_EQ(result.to_set.size(), 1);
  EXPECT_EQ(FindStatistics(result.to_set, 1), stats_file);
  EXPECT_EQ(result.to_remove.size(), 1);
  EXPECT_THAT(result.to_remove, ::testing::Contains(2));
}

TEST_F(UpdateStatisticsTest, ReplaceStatistics) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateStatistics());
  auto stats_file_1 =
      MakeStatisticsFile(1, "/warehouse/test_table/metadata/stats-1a.puffin");
  auto stats_file_2 =
      MakeStatisticsFile(1, "/warehouse/test_table/metadata/stats-1b.puffin", 2048, 256);

  // Set statistics for snapshot 1, then replace it
  update->SetStatistics(stats_file_1).SetStatistics(stats_file_2);

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_EQ(result.to_set.size(), 1);
  EXPECT_TRUE(result.to_remove.empty());
  // Should have the second one (replacement)
  EXPECT_EQ(FindStatistics(result.to_set, 1), stats_file_2);
  EXPECT_NE(FindStatistics(result.to_set, 1), stats_file_1);
}

TEST_F(UpdateStatisticsTest, SetThenRemoveSameSnapshot) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateStatistics());
  auto stats_file =
      MakeStatisticsFile(1, "/warehouse/test_table/metadata/stats-1.puffin");

  // Set statistics for snapshot 1, then remove it
  update->SetStatistics(stats_file).RemoveStatistics(1);

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_TRUE(result.to_set.empty());
  EXPECT_EQ(result.to_remove.size(), 1);
  EXPECT_THAT(result.to_remove, ::testing::Contains(1));
}

TEST_F(UpdateStatisticsTest, RemoveThenSetSameSnapshot) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateStatistics());
  auto stats_file =
      MakeStatisticsFile(1, "/warehouse/test_table/metadata/stats-1.puffin");

  // Remove statistics for snapshot 1, then set new ones
  update->RemoveStatistics(1).SetStatistics(stats_file);

  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_EQ(result.to_set.size(), 1);
  EXPECT_TRUE(result.to_remove.empty());
  EXPECT_EQ(FindStatistics(result.to_set, 1), stats_file);
}

TEST_F(UpdateStatisticsTest, SetNullStatistics) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateStatistics());

  update->SetStatistics(nullptr);

  auto result = update->Apply();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("Statistics file cannot be null"));
}

TEST_F(UpdateStatisticsTest, CommitSuccess) {
  // Test empty commit
  ICEBERG_UNWRAP_OR_FAIL(auto empty_update, table_->NewUpdateStatistics());
  EXPECT_THAT(empty_update->Commit(), IsOk());

  // Reload table after first commit
  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));

  // Get a valid snapshot ID from the table
  ICEBERG_UNWRAP_OR_FAIL(auto current_snapshot, reloaded->current_snapshot());
  int64_t snapshot_id = current_snapshot->snapshot_id;

  // Test commit with statistics changes
  ICEBERG_UNWRAP_OR_FAIL(auto update, reloaded->NewUpdateStatistics());
  auto stats_file =
      MakeStatisticsFile(snapshot_id, "/warehouse/test_table/metadata/stats-1.puffin");
  update->SetStatistics(stats_file);

  EXPECT_THAT(update->Commit(), IsOk());

  // Verify the statistics were committed and persisted
  ICEBERG_UNWRAP_OR_FAIL(auto final_table, catalog_->LoadTable(table_ident_));
  const auto& statistics = final_table->metadata()->statistics;
  EXPECT_EQ(statistics.size(), 1);
  EXPECT_EQ(*statistics[0], *stats_file);
}

}  // namespace iceberg
