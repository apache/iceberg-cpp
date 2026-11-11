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

#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/expression/expressions.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/result.h"
#include "iceberg/scan_task_planning_internal.h"
#include "iceberg/table_properties.h"
#include "iceberg/table_scan.h"
#include "iceberg/test/matchers.h"
#include "iceberg/util/stream.h"

namespace iceberg::internal {
namespace {

using testing::ElementsAre;
using testing::Eq;

const int64_t kDefaultTargetSize = TableProperties::kSplitSize.value();
const int32_t kDefaultLookback = TableProperties::kSplitLookback.value();
const int64_t kDefaultOpenFileCost = TableProperties::kSplitOpenFileCost.value();

std::shared_ptr<DataFile> MakeDataFile(const std::string& path, int64_t file_size,
                                       std::vector<int64_t> split_offsets = {}) {
  auto file = std::make_shared<DataFile>();
  file->content = DataFile::Content::kData;
  file->file_path = path;
  file->file_format = FileFormatType::kParquet;
  file->record_count = 10;
  file->file_size_in_bytes = file_size;
  file->split_offsets = std::move(split_offsets);
  return file;
}

std::shared_ptr<DataFile> MakeDeleteFile(int64_t file_size,
                                         std::optional<int64_t> content_size = {},
                                         bool puffin = false) {
  auto file = std::make_shared<DataFile>();
  file->content = DataFile::Content::kPositionDeletes;
  file->file_path = "delete-file";
  file->file_format = puffin ? FileFormatType::kPuffin : FileFormatType::kParquet;
  file->record_count = 1;
  file->file_size_in_bytes = file_size;
  file->content_size_in_bytes = content_size;
  return file;
}

std::shared_ptr<FileScanTask> MakeTask(
    std::shared_ptr<DataFile> file, std::vector<std::shared_ptr<DataFile>> deletes = {}) {
  return std::make_shared<FileScanTask>(std::move(file), std::move(deletes));
}

FileScanTaskStreamPtr TaskStream(std::vector<std::shared_ptr<FileScanTask>> tasks) {
  return FileScanTaskStream::FromVector(std::move(tasks));
}

TaskGroupPlanningOptions Options(int64_t target_size, int64_t lookback = 10,
                                 int64_t open_file_cost = 0) {
  return TaskGroupPlanningOptions{
      .target_size = target_size, .lookback = lookback, .open_file_cost = open_file_cost};
}

Result<std::vector<std::shared_ptr<FileScanTaskGroup>>> PlanAll(
    std::vector<std::shared_ptr<FileScanTask>> tasks,
    const TaskGroupPlanningOptions& options) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto stream, PlanTaskGroupStream(TaskStream(std::move(tasks)), options, nullptr));
  std::vector<std::shared_ptr<FileScanTaskGroup>> groups;
  while (true) {
    ICEBERG_ASSIGN_OR_RAISE(auto group, stream->Next());
    if (!group.has_value()) {
      return groups;
    }
    groups.push_back(std::move(group).value());
  }
}

// (start, length) of each task in a group, in order.
std::vector<std::pair<int64_t, int64_t>> RangesOf(const FileScanTaskGroup& group) {
  std::vector<std::pair<int64_t, int64_t>> ranges;
  for (const auto& task : group.tasks()) {
    ranges.emplace_back(task->start(), task->length());
  }
  return ranges;
}

// A split policy with fixed boundaries that counts how often it is consulted.
class CountingSplitPolicy final : public SplitPolicy {
 public:
  explicit CountingSplitPolicy(std::vector<int64_t> boundaries)
      : boundaries_(std::move(boundaries)) {}

  Result<std::vector<int64_t>> SplitBoundaries(const std::shared_ptr<FileIO>&,
                                               const DataFile&) override {
    ++calls_;
    return boundaries_;
  }

  int calls() const { return calls_; }

 private:
  std::vector<int64_t> boundaries_;
  int calls_ = 0;
};

// Registers a policy for a format and unregisters it when the test ends.
class ScopedSplitPolicy {
 public:
  ScopedSplitPolicy(FileFormatType format, std::unique_ptr<SplitPolicy> policy)
      : format_(format) {
    SplitPolicyRegistry::Register(format_, std::move(policy));
  }

  ~ScopedSplitPolicy() { SplitPolicyRegistry::Unregister(format_); }

 private:
  FileFormatType format_;
};

class ScanTaskPlanningTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Core-only builds have no registered policies; make bundled builds match by
    // removing anything registered before the test.
    SplitPolicyRegistry::Unregister(FileFormatType::kParquet);
  }

  ~ScanTaskPlanningTest() override {
    SplitPolicyRegistry::Unregister(FileFormatType::kParquet);
  }
};

TEST_F(ScanTaskPlanningTest, ResolveOptionsUsesDefaults) {
  const std::unordered_map<std::string, std::string> empty;
  ICEBERG_UNWRAP_OR_FAIL(auto options, ResolveTaskGroupPlanningOptions(empty, empty));
  EXPECT_THAT(options.target_size, Eq(kDefaultTargetSize));
  EXPECT_THAT(options.lookback, Eq(kDefaultLookback));
  EXPECT_THAT(options.open_file_cost, Eq(kDefaultOpenFileCost));
}

TEST_F(ScanTaskPlanningTest, ResolveOptionsPrefersScanOptionOverTableProperty) {
  const std::unordered_map<std::string, std::string> scan_options{
      {"read.split.target-size", "1000"}};
  const std::unordered_map<std::string, std::string> table_properties{
      {"read.split.target-size", "999"}};
  ICEBERG_UNWRAP_OR_FAIL(auto options,
                         ResolveTaskGroupPlanningOptions(scan_options, table_properties));
  EXPECT_THAT(options.target_size, Eq(1000));
}

TEST_F(ScanTaskPlanningTest, ResolveOptionsPrefersTablePropertyOverDefault) {
  const std::unordered_map<std::string, std::string> scan_options;
  const std::unordered_map<std::string, std::string> table_properties{
      {"read.split.planning-lookback", "3"}};
  ICEBERG_UNWRAP_OR_FAIL(auto options,
                         ResolveTaskGroupPlanningOptions(scan_options, table_properties));
  EXPECT_THAT(options.lookback, Eq(3));
}

TEST_F(ScanTaskPlanningTest, ResolveOptionsRejectsInvalidValues) {
  const std::unordered_map<std::string, std::string> empty;
  const std::unordered_map<std::string, std::string> malformed{
      {"read.split.target-size", "not-a-number"}};
  EXPECT_THAT(ResolveTaskGroupPlanningOptions(malformed, empty),
              IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(ResolveTaskGroupPlanningOptions(empty, malformed),
              IsError(ErrorKind::kInvalidArgument));

  for (const std::string& target : {"0", "-1"}) {
    EXPECT_THAT(
        ResolveTaskGroupPlanningOptions({{"read.split.target-size", target}}, empty),
        IsError(ErrorKind::kInvalid));
  }
  EXPECT_THAT(ResolveTaskGroupPlanningOptions(
                  {{"read.split.planning-lookback", "3000000000"}}, empty),
              IsError(ErrorKind::kInvalidArgument));

  for (const std::string& lookback : {"0", "-3"}) {
    EXPECT_THAT(ResolveTaskGroupPlanningOptions(
                    {{"read.split.planning-lookback", lookback}}, empty),
                IsError(ErrorKind::kInvalid));
  }
  EXPECT_THAT(
      ResolveTaskGroupPlanningOptions({{"read.split.open-file-cost", "-1"}}, empty),
      IsError(ErrorKind::kInvalid));
}

TEST_F(ScanTaskPlanningTest, WeightMatchesJavaFormula) {
  auto task =
      MakeTask(MakeDataFile("file", 100), {MakeDeleteFile(50), MakeDeleteFile(30)});
  ICEBERG_UNWRAP_OR_FAIL(auto weight, TaskGroupWeight(*task, 10));
  // max(100 + 50 + 30, (1 + 2) * 10) = 180
  EXPECT_THAT(weight, Eq(180));
}

TEST_F(ScanTaskPlanningTest, WeightChargesOpenFileCost) {
  auto task = MakeTask(MakeDataFile("file", 10));
  ICEBERG_UNWRAP_OR_FAIL(auto weight, TaskGroupWeight(*task, 1000));
  // max(10, (1 + 0) * 1000) = 1000
  EXPECT_THAT(weight, Eq(1000));
}

TEST_F(ScanTaskPlanningTest, WeightUsesDeletionVectorContentSize) {
  auto task = MakeTask(MakeDataFile("file", 10),
                       {MakeDeleteFile(1000, /*content_size=*/40, /*puffin=*/true)});
  ICEBERG_UNWRAP_OR_FAIL(auto weight, TaskGroupWeight(*task, 0));
  // The deletion vector blob counts, not its 1000-byte container.
  EXPECT_THAT(weight, Eq(50));
}

TEST_F(ScanTaskPlanningTest, WeightRejectsCostOverflow) {
  const int64_t kMax = std::numeric_limits<int64_t>::max();
  auto task = MakeTask(MakeDataFile("file", kMax), {MakeDeleteFile(kMax)});
  EXPECT_THAT(TaskGroupWeight(*task, 0), IsError(ErrorKind::kInvalid));
}

TEST_F(ScanTaskPlanningTest, NoPolicyKeepsLargeTaskWhole) {
  auto task = MakeTask(MakeDataFile("file", 1000));
  ICEBERG_UNWRAP_OR_FAIL(auto groups, PlanAll({task}, Options(100)));
  ASSERT_THAT(groups.size(), Eq(1));
  ASSERT_THAT(groups[0]->tasks().size(), Eq(1));
  EXPECT_THAT(groups[0]->tasks()[0], Eq(task));
  EXPECT_FALSE(groups[0]->tasks()[0]->is_split());
}

TEST_F(ScanTaskPlanningTest, UnsplittableFormatKeepsLargeTaskWhole) {
  auto avro = MakeDataFile("avro-file", 1000);
  avro->file_format = FileFormatType::kAvro;
  auto task = MakeTask(std::move(avro));
  ScopedSplitPolicy scoped(
      FileFormatType::kParquet,
      std::make_unique<CountingSplitPolicy>(std::vector<int64_t>{100, 500}));
  ICEBERG_UNWRAP_OR_FAIL(auto groups, PlanAll({task}, Options(100)));
  ASSERT_THAT(groups.size(), Eq(1));
  ASSERT_THAT(groups[0]->tasks().size(), Eq(1));
  EXPECT_THAT(groups[0]->tasks()[0], Eq(task));
  EXPECT_FALSE(groups[0]->tasks()[0]->is_split());
}

TEST_F(ScanTaskPlanningTest, SmallTaskSkipsPolicy) {
  auto policy = std::make_unique<CountingSplitPolicy>(std::vector<int64_t>{});
  const CountingSplitPolicy* policy_view = policy.get();
  ScopedSplitPolicy scoped(FileFormatType::kParquet, std::move(policy));

  auto small = MakeTask(MakeDataFile("small", 500));
  ICEBERG_UNWRAP_OR_FAIL(auto groups, PlanAll({small}, Options(1000)));
  ASSERT_THAT(groups.size(), Eq(1));
  EXPECT_THAT(policy_view->calls(), Eq(0));
}

TEST_F(ScanTaskPlanningTest, SplitCoversFileExactlyAndCoalescesTowardTarget) {
  auto policy =
      std::make_unique<CountingSplitPolicy>(std::vector<int64_t>{100, 250, 300});
  ScopedSplitPolicy scoped(FileFormatType::kParquet, std::move(policy));

  auto file = MakeDataFile("file", 1000);
  file->record_count = 100;
  auto deletes = std::vector<std::shared_ptr<DataFile>>{MakeDeleteFile(7)};
  const auto residual = Expressions::GreaterThan("id", Literal::Int(1));
  auto task = std::make_shared<FileScanTask>(file, deletes, residual);

  ICEBERG_UNWRAP_OR_FAIL(auto groups, PlanAll({task}, Options(120)));
  // [0, 100) and [0, 250) coalesce to [0, 250); [250, 300) joins the tail
  // [250, 1000). The ranges cover [0, 1000) exactly, including the bytes before
  // the first boundary, and every range reaches the target size, so each packs
  // as a singleton group.
  ASSERT_THAT(groups.size(), Eq(2));
  EXPECT_THAT(RangesOf(*groups[0]), ElementsAre(std::pair<int64_t, int64_t>{0, 250}));
  EXPECT_THAT(RangesOf(*groups[1]), ElementsAre(std::pair<int64_t, int64_t>{250, 750}));

  for (const auto& group : groups) {
    for (const auto& split : group->tasks()) {
      EXPECT_THAT(split->data_file(), Eq(file));
      EXPECT_THAT(split->delete_files(), ElementsAre(deletes[0]));
      EXPECT_THAT(split->residual_filter(), Eq(residual));
    }
  }
  EXPECT_THAT(groups[0]->estimated_row_count() + groups[1]->estimated_row_count(),
              Eq(100));
}

TEST_F(ScanTaskPlanningTest, BoundariesBelowTargetKeepFileWhole) {
  ScopedSplitPolicy scoped(
      FileFormatType::kParquet,
      std::make_unique<CountingSplitPolicy>(std::vector<int64_t>{10}));

  auto task = MakeTask(MakeDataFile("file", 1000));
  ICEBERG_UNWRAP_OR_FAIL(auto groups, PlanAll({task}, Options(100)));
  ASSERT_THAT(groups.size(), Eq(1));
  ASSERT_THAT(groups[0]->tasks().size(), Eq(1));
  EXPECT_THAT(groups[0]->tasks()[0], Eq(task));
  EXPECT_FALSE(task->is_split());
}

TEST_F(ScanTaskPlanningTest, MixedFormatTasksShareOneGroup) {
  auto avro_file = MakeDataFile("avro-file", 400);
  avro_file->file_format = FileFormatType::kAvro;
  auto avro = MakeTask(std::move(avro_file));
  auto parquet = MakeTask(MakeDataFile("parquet-file", 500));

  // No split policy is registered: both files stay whole and pack together.
  ICEBERG_UNWRAP_OR_FAIL(auto groups, PlanAll({avro, parquet}, Options(1000)));
  ASSERT_THAT(groups.size(), Eq(1));
  EXPECT_THAT(groups[0]->tasks(), ElementsAre(avro, parquet));
}

TEST_F(ScanTaskPlanningTest, EmptyFileStaysWhole) {
  auto task = MakeTask(MakeDataFile("empty", 0));
  ICEBERG_UNWRAP_OR_FAIL(auto groups, PlanAll({task}, Options(100)));
  ASSERT_THAT(groups.size(), Eq(1));
  EXPECT_THAT(groups[0]->tasks(), ElementsAre(task));
}

TEST_F(ScanTaskPlanningTest, AlreadySplitTaskPassesThrough) {
  ScopedSplitPolicy scoped(
      FileFormatType::kParquet,
      std::make_unique<CountingSplitPolicy>(std::vector<int64_t>{100, 500}));

  ICEBERG_UNWRAP_OR_FAIL(auto split,
                         FileScanTask::MakeSplit(MakeDataFile("file", 1000), 100, 50));
  ICEBERG_UNWRAP_OR_FAIL(auto groups, PlanAll({split}, Options(10)));
  ASSERT_THAT(groups.size(), Eq(1));
  EXPECT_THAT(groups[0]->tasks(), ElementsAre(split));
}

TEST_F(ScanTaskPlanningTest, PackingFillsFirstGroupThatFits) {
  auto a = MakeTask(MakeDataFile("a", 100));
  auto b = MakeTask(MakeDataFile("b", 40));
  auto c = MakeTask(MakeDataFile("c", 40));
  ICEBERG_UNWRAP_OR_FAIL(auto groups, PlanAll({a, b, c}, Options(100)));
  ASSERT_THAT(groups.size(), Eq(2));
  EXPECT_THAT(groups[0]->tasks(), ElementsAre(a));
  EXPECT_THAT(groups[1]->tasks(), ElementsAre(b, c));
}

TEST_F(ScanTaskPlanningTest, PackingClosesLargestOpenGroupFirst) {
  // With lookback 2, opening a third group closes the largest open group.
  auto a = MakeTask(MakeDataFile("a", 100));
  auto b = MakeTask(MakeDataFile("b", 60));
  auto c = MakeTask(MakeDataFile("c", 60));
  ICEBERG_UNWRAP_OR_FAIL(auto groups, PlanAll({a, b, c}, Options(100, /*lookback=*/2)));
  ASSERT_THAT(groups.size(), Eq(3));
  EXPECT_THAT(groups[0]->tasks(), ElementsAre(a));
  EXPECT_THAT(groups[1]->tasks(), ElementsAre(b));
  EXPECT_THAT(groups[2]->tasks(), ElementsAre(c));
}

TEST_F(ScanTaskPlanningTest, OversizedTaskFormsSingletonGroup) {
  auto big = MakeTask(MakeDataFile("big", 1000));
  auto small = MakeTask(MakeDataFile("small", 20));
  ICEBERG_UNWRAP_OR_FAIL(auto groups,
                         PlanAll({big, small}, Options(100, /*lookback=*/1)));
  ASSERT_THAT(groups.size(), Eq(2));
  EXPECT_THAT(groups[0]->tasks(), ElementsAre(big));
  EXPECT_THAT(groups[1]->tasks(), ElementsAre(small));
}

TEST_F(ScanTaskPlanningTest, GroupHoldsAtMost1024Tasks) {
  std::vector<std::shared_ptr<FileScanTask>> tasks;
  tasks.reserve(1025);
  for (int i = 0; i < 1025; ++i) {
    tasks.push_back(MakeTask(MakeDataFile("file-" + std::to_string(i), 1)));
  }
  ICEBERG_UNWRAP_OR_FAIL(auto groups, PlanAll(std::move(tasks), Options(2000)));
  ASSERT_THAT(groups.size(), Eq(2));
  EXPECT_THAT(groups[0]->tasks().size(), Eq(1024));
  EXPECT_THAT(groups[1]->tasks().size(), Eq(1));
}

TEST_F(ScanTaskPlanningTest, GroupSumsTaskMetrics) {
  auto first = MakeTask(MakeDataFile("first", 30));
  auto second = MakeTask(MakeDataFile("second", 20, {10}));
  ICEBERG_UNWRAP_OR_FAIL(auto groups, PlanAll({first, second}, Options(100)));
  ASSERT_THAT(groups.size(), Eq(1));
  const auto& group = *groups[0];
  EXPECT_THAT(group.kind(), Eq(ScanTask::Kind::kFileScanTaskGroup));
  EXPECT_THAT(group.size_bytes(), Eq(50));
  // FileScanTask::files_count() counts one file per task in this version.
  EXPECT_THAT(group.files_count(), Eq(2));
  EXPECT_THAT(group.estimated_row_count(), Eq(20));
}

TEST_F(ScanTaskPlanningTest, CostOverflowSurfacesPlanningError) {
  const int64_t kMax = std::numeric_limits<int64_t>::max();
  auto task = MakeTask(MakeDataFile("file", kMax), {MakeDeleteFile(kMax)});
  ICEBERG_UNWRAP_OR_FAIL(auto stream,
                         PlanTaskGroupStream(TaskStream({task}), Options(100), nullptr));
  auto group = stream->Next();
  ASSERT_FALSE(group.has_value());
  EXPECT_THAT(group, IsError(ErrorKind::kInvalid));
}

TEST_F(ScanTaskPlanningTest, EmptyTaskStreamProducesNoGroups) {
  ICEBERG_UNWRAP_OR_FAIL(auto stream,
                         PlanTaskGroupStream(TaskStream({}), Options(100), nullptr));
  ICEBERG_UNWRAP_OR_FAIL(auto group, stream->Next());
  EXPECT_FALSE(group.has_value());
}

TEST_F(ScanTaskPlanningTest, PlanRejectsInvalidOptions) {
  auto tasks = TaskStream({});
  EXPECT_THAT(PlanTaskGroupStream(std::move(tasks), Options(0), nullptr),
              IsError(ErrorKind::kInvalid));

  tasks = TaskStream({});
  EXPECT_THAT(
      PlanTaskGroupStream(std::move(tasks), Options(100, /*lookback=*/0), nullptr),
      IsError(ErrorKind::kInvalid));

  tasks = TaskStream({});
  EXPECT_THAT(PlanTaskGroupStream(std::move(tasks),
                                  Options(100, 10, /*open_file_cost=*/-1), nullptr),
              IsError(ErrorKind::kInvalid));
}

TEST_F(ScanTaskPlanningTest, StreamStopsEarlyWithoutConsumingAllGroups) {
  auto a = MakeTask(MakeDataFile("a", 100));
  auto b = MakeTask(MakeDataFile("b", 60));
  auto c = MakeTask(MakeDataFile("c", 60));
  ICEBERG_UNWRAP_OR_FAIL(
      auto stream, PlanTaskGroupStream(TaskStream({a, b, c}), Options(100, 2), nullptr));
  {
    ICEBERG_UNWRAP_OR_FAIL(auto group, stream->Next());
    ASSERT_TRUE(group.has_value());
    EXPECT_THAT((*group)->tasks(), ElementsAre(a));
  }
  // Destroying the stream mid-consumption releases the task stream.
}

}  // namespace
}  // namespace iceberg::internal
