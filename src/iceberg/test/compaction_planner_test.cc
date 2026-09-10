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

#include "iceberg/compaction_planner.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/expression/literal.h"
#include "iceberg/file_format.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/table_scan.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"

namespace iceberg {
namespace {

using ::testing::ElementsAre;
using ::testing::IsEmpty;
using ::testing::SizeIs;

class CompactionPlannerTest : public testing::Test {
 protected:
  static constexpr int64_t kSnapshotId = 1234;

  static std::shared_ptr<DataFile> Data(std::string path, PartitionValues partition,
                                        int64_t size, std::optional<int32_t> spec_id = 1,
                                        int64_t records = 100) {
    return std::make_shared<DataFile>(DataFile{
        .file_path = std::move(path),
        .partition = std::move(partition),
        .record_count = records,
        .file_size_in_bytes = size,
        .partition_spec_id = spec_id,
    });
  }

  static std::shared_ptr<DataFile> Data(std::string path, int32_t partition, int64_t size,
                                        std::optional<int32_t> spec_id = 1,
                                        int64_t records = 100) {
    return Data(std::move(path), PartitionValues(Literal::Int(partition)), size, spec_id,
                records);
  }

  static std::shared_ptr<DataFile> PositionDelete(std::string path,
                                                  const std::string& referenced_file,
                                                  int64_t records,
                                                  bool file_scoped = true) {
    return std::make_shared<DataFile>(DataFile{
        .content = DataFile::Content::kPositionDeletes,
        .file_path = std::move(path),
        .record_count = records,
        .referenced_data_file =
            file_scoped ? std::make_optional(referenced_file) : std::nullopt,
    });
  }

  static std::shared_ptr<DataFile> DeletionVector(std::string path,
                                                  const std::string& referenced_file,
                                                  int64_t records, int64_t offset = 0,
                                                  int64_t size = 10) {
    return std::make_shared<DataFile>(DataFile{
        .content = DataFile::Content::kPositionDeletes,
        .file_path = std::move(path),
        .file_format = FileFormatType::kPuffin,
        .record_count = records,
        .referenced_data_file = referenced_file,
        .content_offset = offset,
        .content_size_in_bytes = size,
    });
  }

  static std::shared_ptr<DataFile> EqualityDelete(std::string path, int64_t records) {
    return std::make_shared<DataFile>(DataFile{
        .content = DataFile::Content::kEqualityDeletes,
        .file_path = std::move(path),
        .record_count = records,
    });
  }

  static std::shared_ptr<FileScanTask> Task(
      std::shared_ptr<DataFile> data_file,
      std::vector<std::shared_ptr<DataFile>> deletes = {}) {
    return std::make_shared<FileScanTask>(std::move(data_file), std::move(deletes));
  }

  static std::vector<std::string> Paths(const CompactionGroup& group) {
    std::vector<std::string> paths;
    for (const auto& file : group.files) {
      paths.push_back(file.scan_task->data_file()->file_path);
    }
    return paths;
  }

  static CompactionPlannerConfig Config() {
    return CompactionPlannerConfig{
        .target_file_size_bytes = 100,
        .min_file_size_ratio = 0.5,
        .max_file_size_ratio = 2.0,
        .min_input_files = 2,
        .delete_file_threshold = 2,
        .delete_ratio_threshold = 0.2,
    };
  }
};

TEST_F(CompactionPlannerTest, ValidatesConfig) {
  std::vector<CompactionPlannerConfig> invalid;

  auto config = Config();
  config.target_file_size_bytes = 0;
  invalid.push_back(config);
  config = Config();
  config.min_file_size_ratio = -0.1;
  invalid.push_back(config);
  config = Config();
  config.min_file_size_ratio = 1.1;
  invalid.push_back(config);
  config = Config();
  config.min_file_size_ratio = std::numeric_limits<double>::quiet_NaN();
  invalid.push_back(config);
  config = Config();
  config.max_file_size_ratio = 0.9;
  invalid.push_back(config);
  config = Config();
  config.max_file_size_ratio = std::numeric_limits<double>::infinity();
  invalid.push_back(config);
  config = Config();
  config.min_input_files = 0;
  invalid.push_back(config);
  config = Config();
  config.delete_file_threshold = -1;
  invalid.push_back(config);
  config = Config();
  config.delete_ratio_threshold = -0.1;
  invalid.push_back(config);
  config = Config();
  config.delete_ratio_threshold = 1.1;
  invalid.push_back(config);
  config = Config();
  config.delete_ratio_threshold = std::numeric_limits<double>::quiet_NaN();
  invalid.push_back(config);

  for (const auto& invalid_config : invalid) {
    EXPECT_THAT(CompactionPlanner::Plan(kSnapshotId, {}, invalid_config),
                IsError(ErrorKind::kInvalidArgument));
  }
}

TEST_F(CompactionPlannerTest, ZeroDeleteThresholdsDisableDeleteSelection) {
  auto config = Config();
  config.delete_file_threshold = 0;
  config.delete_ratio_threshold = 0;
  std::vector<std::shared_ptr<FileScanTask>> tasks{
      Task(Data("data", 1, 100), {PositionDelete("delete-a", "data", 100),
                                  PositionDelete("delete-b", "data", 100)})};

  ICEBERG_UNWRAP_OR_FAIL(auto plan, CompactionPlanner::Plan(kSnapshotId, tasks, config));
  EXPECT_THAT(plan.groups, IsEmpty());
}

TEST_F(CompactionPlannerTest, BindsPlanToSourceSnapshot) {
  ICEBERG_UNWRAP_OR_FAIL(auto plan, CompactionPlanner::Plan(kSnapshotId, {}, Config()));

  EXPECT_EQ(plan.source_snapshot_id, kSnapshotId);
  EXPECT_THAT(CompactionPlanner::Plan(-1, {}, Config()),
              IsError(ErrorKind::kInvalidArgument));
}

TEST_F(CompactionPlannerTest, SelectsThresholdBoundariesAndIgnoresOtherDeletes) {
  std::vector<std::shared_ptr<FileScanTask>> tasks{
      Task(Data("small-a", 1, 49)),
      Task(Data("small-b", 1, 49)),
      Task(Data("at-min-size", 1, 50)),
      Task(Data("below-ratio", 2, 100),
           {DeletionVector("below-ratio.dv", "below-ratio", 19)}),
      Task(Data("at-ratio", 2, 100), {DeletionVector("at-ratio.dv", "at-ratio", 20)}),
      Task(Data("at-count", 3, 100), {PositionDelete("count-a", "at-count", 0),
                                      PositionDelete("count-b", "at-count", 0)}),
      Task(Data("ignored", 4, 100),
           {EqualityDelete("equality", 100),
            PositionDelete("partition-scoped", "ignored", 100, false)})};

  ICEBERG_UNWRAP_OR_FAIL(auto plan,
                         CompactionPlanner::Plan(kSnapshotId, tasks, Config()));
  ASSERT_THAT(plan.groups, SizeIs(3));
  EXPECT_THAT(Paths(plan.groups[0]), ElementsAre("small-a", "small-b"));
  EXPECT_THAT(Paths(plan.groups[1]), ElementsAre("at-ratio"));
  EXPECT_EQ(plan.groups[1].file_scoped_delete_record_count, 20);
  EXPECT_THAT(Paths(plan.groups[2]), ElementsAre("at-count"));
}

TEST_F(CompactionPlannerTest, GroupsNullPartitionsTogether) {
  auto null_partition = [] { return PartitionValues(Literal::Null(int32())); };
  std::vector<std::shared_ptr<FileScanTask>> tasks{
      Task(Data("null-b", null_partition(), 10)), Task(Data("value-a", 1, 10)),
      Task(Data("null-a", null_partition(), 10)), Task(Data("value-b", 1, 10))};

  ICEBERG_UNWRAP_OR_FAIL(auto plan,
                         CompactionPlanner::Plan(kSnapshotId, tasks, Config()));
  ASSERT_THAT(plan.groups, SizeIs(2));
  EXPECT_THAT(Paths(plan.groups[0]), ElementsAre("null-a", "null-b"));
  EXPECT_THAT(Paths(plan.groups[1]), ElementsAre("value-a", "value-b"));
}

TEST_F(CompactionPlannerTest, GroupsCanonicalNaNPartitionsTogether) {
  auto quiet_nan =
      PartitionValues(Literal::Double(std::numeric_limits<double>::quiet_NaN()));
  auto signaling_nan =
      PartitionValues(Literal::Double(std::numeric_limits<double>::signaling_NaN()));
  std::vector<std::shared_ptr<FileScanTask>> tasks{
      Task(Data("nan-b", std::move(signaling_nan), 10)),
      Task(Data("nan-a", std::move(quiet_nan), 10))};

  ICEBERG_UNWRAP_OR_FAIL(auto plan,
                         CompactionPlanner::Plan(kSnapshotId, tasks, Config()));
  ASSERT_THAT(plan.groups, SizeIs(1));
  EXPECT_THAT(Paths(plan.groups[0]), ElementsAre("nan-a", "nan-b"));
}

TEST_F(CompactionPlannerTest, RejectsMissingPartitionSpecId) {
  std::vector<std::shared_ptr<FileScanTask>> tasks{
      Task(Data("missing-spec", 1, 10, std::nullopt))};

  EXPECT_THAT(CompactionPlanner::Plan(kSnapshotId, tasks, Config()),
              IsError(ErrorKind::kInvalidArgument));
}

TEST_F(CompactionPlannerTest, RejectsDuplicateDataFileTasks) {
  std::vector<std::shared_ptr<FileScanTask>> tasks{Task(Data("duplicate", 1, 10)),
                                                   Task(Data("duplicate", 1, 10))};

  EXPECT_THAT(CompactionPlanner::Plan(kSnapshotId, tasks, Config()),
              IsError(ErrorKind::kInvalidArgument));
}

TEST_F(CompactionPlannerTest, ProducesCanonicalPartitionAndFileOrder) {
  std::vector<std::shared_ptr<FileScanTask>> tasks{
      Task(Data("s2-z", 1, 10, 2)), Task(Data("p2-z", 2, 10)), Task(Data("p1-z", 1, 10)),
      Task(Data("s2-a", 1, 10, 2)), Task(Data("p2-a", 2, 10)), Task(Data("p1-a", 1, 10))};

  ICEBERG_UNWRAP_OR_FAIL(auto first,
                         CompactionPlanner::Plan(kSnapshotId, tasks, Config()));
  std::ranges::reverse(tasks);
  ICEBERG_UNWRAP_OR_FAIL(auto second,
                         CompactionPlanner::Plan(kSnapshotId, tasks, Config()));

  ASSERT_THAT(first.groups, SizeIs(3));
  ASSERT_THAT(second.groups, SizeIs(3));
  for (size_t i = 0; i < first.groups.size(); ++i) {
    EXPECT_EQ(first.groups[i].partition_spec_id, second.groups[i].partition_spec_id);
    EXPECT_EQ(Paths(first.groups[i]), Paths(second.groups[i]));
  }
  EXPECT_EQ(first.groups[0].partition_spec_id, 1);
  EXPECT_THAT(Paths(first.groups[0]), ElementsAre("p1-a", "p1-z"));
  EXPECT_THAT(Paths(first.groups[1]), ElementsAre("p2-a", "p2-z"));
  EXPECT_EQ(first.groups[2].partition_spec_id, 2);
  EXPECT_THAT(Paths(first.groups[2]), ElementsAre("s2-a", "s2-z"));
}

TEST_F(CompactionPlannerTest, ComparesLargeFileSizeRatiosExactly) {
  constexpr int64_t kMax = std::numeric_limits<int64_t>::max();
  auto config = Config();
  config.target_file_size_bytes = kMax;
  config.min_file_size_ratio = std::nextafter(1.0, 0.0);
  config.max_file_size_ratio = 1.0;
  config.min_input_files = 1;
  config.delete_file_threshold = 0;
  config.delete_ratio_threshold = 0;
  std::vector<std::shared_ptr<FileScanTask>> tasks{
      Task(Data("below-boundary", 1, kMax - 1024)),
      Task(Data("above-boundary", 2, kMax - 1023))};

  ICEBERG_UNWRAP_OR_FAIL(auto plan, CompactionPlanner::Plan(kSnapshotId, tasks, config));
  ASSERT_THAT(plan.groups, SizeIs(1));
  EXPECT_THAT(Paths(plan.groups[0]), ElementsAre("below-boundary"));
}

TEST_F(CompactionPlannerTest, ComparesLargeDeleteRatiosExactly) {
  constexpr int64_t kMax = std::numeric_limits<int64_t>::max();
  auto config = Config();
  config.min_file_size_ratio = 0;
  config.delete_file_threshold = 0;
  config.delete_ratio_threshold = std::nextafter(1.0, 0.0);
  std::vector<std::shared_ptr<FileScanTask>> tasks{
      Task(Data("below-boundary", 1, 100, 1, kMax),
           {PositionDelete("below.delete", "below-boundary", kMax - 1024)}),
      Task(Data("at-boundary", 2, 100, 1, kMax),
           {PositionDelete("at.delete", "at-boundary", kMax - 1023)})};

  ICEBERG_UNWRAP_OR_FAIL(auto plan, CompactionPlanner::Plan(kSnapshotId, tasks, config));
  ASSERT_THAT(plan.groups, SizeIs(1));
  EXPECT_THAT(Paths(plan.groups[0]), ElementsAre("at-boundary"));
}

TEST_F(CompactionPlannerTest, DeduplicatesDeleteReferencesAndDistinguishesDvRanges) {
  std::vector<std::shared_ptr<FileScanTask>> tasks{
      Task(Data("data", 1, 100),
           {PositionDelete("deletes", "data", 30), PositionDelete("deletes", "data", 30),
            DeletionVector("deletes", "data", 20, 0, 10),
            DeletionVector("deletes", "data", 20, 0, 10),
            DeletionVector("deletes", "data", 5, 10, 10)})};
  auto config = Config();
  config.delete_file_threshold = 3;
  config.delete_ratio_threshold = 0;

  ICEBERG_UNWRAP_OR_FAIL(auto plan, CompactionPlanner::Plan(kSnapshotId, tasks, config));
  ASSERT_THAT(plan.groups, SizeIs(1));
  ASSERT_THAT(plan.groups[0].files, SizeIs(1));
  EXPECT_EQ(plan.groups[0].files[0].file_scoped_delete_count, 3);
  EXPECT_EQ(plan.groups[0].files[0].file_scoped_delete_record_count, 55);
}

TEST_F(CompactionPlannerTest, CapsPositionDeleteCardinalityAtDataRows) {
  std::vector<std::shared_ptr<FileScanTask>> tasks{Task(
      Data("data", 1, 100),
      {PositionDelete("delete-a", "data", 80), PositionDelete("delete-b", "data", 40)})};

  ICEBERG_UNWRAP_OR_FAIL(auto plan,
                         CompactionPlanner::Plan(kSnapshotId, tasks, Config()));
  ASSERT_THAT(plan.groups, SizeIs(1));
  EXPECT_EQ(plan.groups[0].files[0].file_scoped_delete_record_count, 100);
  EXPECT_EQ(plan.groups[0].file_scoped_delete_record_count, 100);
}

TEST_F(CompactionPlannerTest, RejectsDvCardinalityAboveDataRows) {
  std::vector<std::shared_ptr<FileScanTask>> tasks{
      Task(Data("data", 1, 100), {DeletionVector("dv", "data", 101)})};

  EXPECT_THAT(CompactionPlanner::Plan(kSnapshotId, tasks, Config()),
              IsError(ErrorKind::kInvalidArgument));
}

TEST_F(CompactionPlannerTest, ReturnsErrorsForAggregateOverflow) {
  constexpr int64_t kMax = std::numeric_limits<int64_t>::max();
  std::vector<std::shared_ptr<FileScanTask>> file_overflow{
      Task(Data("data", 1, 100, 1, kMax), {PositionDelete("delete-a", "data", kMax),
                                           PositionDelete("delete-b", "data", kMax)})};
  EXPECT_THAT(CompactionPlanner::Plan(kSnapshotId, file_overflow, Config()),
              IsError(ErrorKind::kInvalidArgument));

  std::vector<std::shared_ptr<FileScanTask>> group_overflow{
      Task(Data("a", 1, 0, 1, kMax), {PositionDelete("delete-a", "a", kMax)}),
      Task(Data("b", 1, 0, 1, kMax), {PositionDelete("delete-b", "b", kMax)})};
  EXPECT_THAT(CompactionPlanner::Plan(kSnapshotId, group_overflow, Config()),
              IsError(ErrorKind::kInvalidArgument));
}

TEST_F(CompactionPlannerTest, RequiresDeletePressureForOversizedFiles) {
  std::vector<std::shared_ptr<FileScanTask>> tasks{
      Task(Data("no-deletes", 1, 201)),
      Task(Data("below-ratio", 1, 201), {DeletionVector("below.dv", "below-ratio", 19)}),
      Task(Data("at-ratio", 1, 201), {DeletionVector("at.dv", "at-ratio", 20)})};

  ICEBERG_UNWRAP_OR_FAIL(auto plan,
                         CompactionPlanner::Plan(kSnapshotId, tasks, Config()));
  ASSERT_THAT(plan.groups, SizeIs(1));
  EXPECT_THAT(Paths(plan.groups[0]), ElementsAre("at-ratio"));
}

TEST_F(CompactionPlannerTest, SplitsPartitionIntoTargetSizedGroups) {
  std::vector<std::shared_ptr<FileScanTask>> tasks;
  for (const auto* path : {"f", "e", "d", "c", "b", "a"}) {
    tasks.push_back(Task(Data(path, 1, 40)));
  }

  ICEBERG_UNWRAP_OR_FAIL(auto plan,
                         CompactionPlanner::Plan(kSnapshotId, tasks, Config()));
  ASSERT_THAT(plan.groups, SizeIs(3));
  EXPECT_THAT(Paths(plan.groups[0]), ElementsAre("a", "b"));
  EXPECT_THAT(Paths(plan.groups[1]), ElementsAre("c", "d"));
  EXPECT_THAT(Paths(plan.groups[2]), ElementsAre("e", "f"));
  for (const auto& group : plan.groups) {
    EXPECT_EQ(group.data_file_size_bytes, 80);
  }
}

TEST_F(CompactionPlannerTest, BestFitAvoidsDroppingCompatibleSmallFiles) {
  auto config = Config();
  config.min_file_size_ratio = 1.0;
  std::vector<std::shared_ptr<FileScanTask>> tasks{
      Task(Data("d40", 1, 40)), Task(Data("b60", 1, 60)), Task(Data("c40", 1, 40)),
      Task(Data("a60", 1, 60))};

  ICEBERG_UNWRAP_OR_FAIL(auto plan, CompactionPlanner::Plan(kSnapshotId, tasks, config));
  ASSERT_THAT(plan.groups, SizeIs(2));
  EXPECT_THAT(Paths(plan.groups[0]), ElementsAre("a60", "c40"));
  EXPECT_THAT(Paths(plan.groups[1]), ElementsAre("b60", "d40"));
  EXPECT_EQ(plan.groups[0].data_file_size_bytes, 100);
  EXPECT_EQ(plan.groups[1].data_file_size_bytes, 100);
}

}  // namespace
}  // namespace iceberg
