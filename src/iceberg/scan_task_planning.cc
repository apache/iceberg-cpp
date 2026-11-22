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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <unordered_map>
#include <utility>
#include <vector>

#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/result.h"
#include "iceberg/scan_task_planning_internal.h"
#include "iceberg/table_properties.h"
#include "iceberg/util/content_file_util.h"
#include "iceberg/util/int128.h"
#include "iceberg/util/string_util.h"

namespace iceberg::internal {

namespace {

template <typename T>
Result<T> ResolveValue(
    const std::string& key, T default_value,
    const std::unordered_map<std::string, std::string>& scan_options,
    const std::unordered_map<std::string, std::string>& table_properties) {
  for (const auto* source : {&scan_options, &table_properties}) {
    auto value = source->find(key);
    if (value != source->end()) {
      return StringUtils::ParseNumber<T>(value->second);
    }
  }
  return default_value;
}

int128_t DeleteContentBytes(const FileScanTask& task) {
  int128_t total = 0;
  for (const auto& delete_file : task.delete_files()) {
    total += ContentFileUtil::ContentSizeInBytes(*delete_file);
  }
  return total;
}

// One group that is still open while packing. Mirrors Java's BinPacking.Bin.
struct OpenGroup {
  std::vector<std::shared_ptr<FileScanTask>> tasks;
  int128_t weight = 0;
  int64_t count = 0;
};

}  // namespace

Result<TaskGroupPlanningOptions> ResolveTaskGroupPlanningOptions(
    const std::unordered_map<std::string, std::string>& scan_options,
    const std::unordered_map<std::string, std::string>& table_properties) {
  using TableProps = TableProperties;
  ICEBERG_ASSIGN_OR_RAISE(
      auto target_size,
      ResolveValue<int64_t>(TableProps::kSplitSize.key(), TableProps::kSplitSize.value(),
                            scan_options, table_properties));
  ICEBERG_ASSIGN_OR_RAISE(auto lookback,
                          ResolveValue<int32_t>(TableProps::kSplitLookback.key(),
                                                TableProps::kSplitLookback.value(),
                                                scan_options, table_properties));
  ICEBERG_ASSIGN_OR_RAISE(auto open_file_cost,
                          ResolveValue<int64_t>(TableProps::kSplitOpenFileCost.key(),
                                                TableProps::kSplitOpenFileCost.value(),
                                                scan_options, table_properties));
  if (target_size <= 0) {
    return Invalid("read.split.target-size must be positive, but is {}", target_size);
  }
  if (lookback <= 0) {
    return Invalid("read.split.planning-lookback must be positive, but is {}", lookback);
  }
  if (open_file_cost < 0) {
    return Invalid("read.split.open-file-cost must not be negative, but is {}",
                   open_file_cost);
  }
  return TaskGroupPlanningOptions{
      .target_size = target_size, .lookback = lookback, .open_file_cost = open_file_cost};
}

Result<int64_t> TaskGroupWeight(const FileScanTask& task, int64_t open_file_cost) {
  const int128_t content_cost =
      static_cast<int128_t>(task.length()) + DeleteContentBytes(task);
  const int128_t open_cost =
      (static_cast<int128_t>(task.delete_files().size()) + 1) * open_file_cost;
  const auto weight = std::max(content_cost, open_cost);
  if (weight > std::numeric_limits<int64_t>::max()) {
    return Invalid(
        "Task group cost overflows 64 bits for data file {} (length {}, delete files {})",
        task.data_file()->file_path, task.length(), task.delete_files().size());
  }
  return static_cast<int64_t>(weight);
}

namespace {

std::span<const int64_t> ValidSplitOffsets(const DataFile& file) {
  int64_t previous = 0;
  for (int64_t offset : file.split_offsets) {
    if (offset <= previous || offset >= file.file_size_in_bytes) {
      return {};
    }
    previous = offset;
  }
  return file.split_offsets;
}

// Generates ranges on demand so a small target size does not materialize every
// split of a large file before the first group can be returned.
class FileScanTaskSplitCursor {
 public:
  FileScanTaskSplitCursor(std::shared_ptr<FileScanTask> task, int64_t target_size)
      : task_(std::move(task)),
        target_size_(target_size),
        boundaries_(ValidSplitOffsets(*task_->data_file())) {}

  bool HasNext() const { return next_start_ < task_->length(); }

  Result<std::shared_ptr<FileScanTask>> Next() {
    const int64_t file_size = task_->length();
    int64_t end = file_size;
    if (boundaries_.empty()) {
      end = next_start_ + std::min(target_size_, file_size - next_start_);
    } else {
      // Coalesce toward the target while covering the prefix and tail of the file.
      while (next_boundary_ < boundaries_.size()) {
        const int64_t boundary = boundaries_[next_boundary_++];
        if (boundary - next_start_ >= target_size_) {
          end = boundary;
          break;
        }
      }
    }
    const int64_t start = std::exchange(next_start_, end);
    if (start == 0 && end == file_size) {
      return task_;
    }
    return FileScanTask::MakeSplit(task_->data_file(), start, end - start,
                                   task_->delete_files(), task_->residual_filter());
  }

 private:
  std::shared_ptr<FileScanTask> task_;
  int64_t target_size_;
  std::span<const int64_t> boundaries_;
  int64_t next_start_ = 0;
  size_t next_boundary_ = 0;
};

// Packs tasks into groups, mirroring Java's BinPacking.PackingIterator with
// largestBinFirst=true: at most `lookback` groups stay open, a task joins the first
// open group that fits, and the largest open group is closed first when a new group
// opens.
class TaskGroupPacker {
 public:
  explicit TaskGroupPacker(const TaskGroupPlanningOptions& options) : options_(options) {}

  /// Adds a task and returns a closed group when packing closes one.
  Result<std::optional<std::shared_ptr<FileScanTaskGroup>>> Add(
      std::shared_ptr<FileScanTask> task) {
    ICEBERG_ASSIGN_OR_RAISE(auto weight, TaskGroupWeight(*task, options_.open_file_cost));
    for (auto& group : open_groups_) {
      if (group.weight + weight <= options_.target_size &&
          group.count < kMaxTasksPerTaskGroup) {
        group.tasks.push_back(std::move(task));
        group.weight += weight;
        ++group.count;
        return std::nullopt;
      }
    }
    OpenGroup group;
    group.tasks.push_back(std::move(task));
    group.weight = weight;
    group.count = 1;
    open_groups_.push_back(std::move(group));
    if (std::cmp_greater(open_groups_.size(), options_.lookback)) {
      return std::optional<std::shared_ptr<FileScanTaskGroup>>{CloseLargestGroup()};
    }
    return std::nullopt;
  }

  /// Returns the next remaining group after the input ends, or std::nullopt when all
  /// groups have been returned.
  std::optional<std::shared_ptr<FileScanTaskGroup>> NextRemainingGroup() {
    if (open_groups_.empty()) {
      return std::nullopt;
    }
    std::vector<std::shared_ptr<FileScanTask>> tasks =
        std::move(open_groups_.front().tasks);
    open_groups_.pop_front();
    return std::make_shared<FileScanTaskGroup>(std::move(tasks));
  }

 private:
  std::shared_ptr<FileScanTaskGroup> CloseLargestGroup() {
    // Like Java's Collections.max, the first open group with the largest weight is
    // closed.
    auto largest = open_groups_.begin();
    for (auto group = std::next(largest); group != open_groups_.end(); ++group) {
      if (group->weight > largest->weight) {
        largest = group;
      }
    }
    std::vector<std::shared_ptr<FileScanTask>> tasks = std::move(largest->tasks);
    open_groups_.erase(largest);
    return std::make_shared<FileScanTaskGroup>(std::move(tasks));
  }

  TaskGroupPlanningOptions options_;
  std::deque<OpenGroup> open_groups_;
};

// Splits and packs one task stream into groups on demand. The stream owns the task
// stream, so destroying it releases the underlying planning resources, including a
// borrowed planning executor's work.
class TaskGroupPlanningStream final : public FileScanTaskGroupStream {
 public:
  TaskGroupPlanningStream(FileScanTaskStreamPtr tasks,
                          const TaskGroupPlanningOptions& options)
      : tasks_(std::move(tasks)), target_size_(options.target_size), packer_(options) {}

  Result<std::optional<std::shared_ptr<FileScanTaskGroup>>> NextImpl() override {
    while (true) {
      ICEBERG_ASSIGN_OR_RAISE(auto task, NextTask());
      if (!task.has_value()) {
        return packer_.NextRemainingGroup();
      }
      ICEBERG_ASSIGN_OR_RAISE(auto group, packer_.Add(std::move(*task)));
      if (group.has_value()) {
        return group;
      }
    }
  }

 private:
  Result<std::optional<std::shared_ptr<FileScanTask>>> NextTask() {
    if (!splits_ || !splits_->HasNext()) {
      splits_.reset();
      ICEBERG_ASSIGN_OR_RAISE(auto task, tasks_->Next());
      if (!task || (*task)->is_split() || (*task)->length() <= target_size_) {
        return task;
      }
      // FileScanTaskReader supports partial tasks for Parquet.
      if ((*task)->data_file()->file_format != FileFormatType::kParquet) {
        return task;
      }
      splits_.emplace(std::move(*task), target_size_);
    }
    ICEBERG_ASSIGN_OR_RAISE(auto split, splits_->Next());
    return std::optional{std::move(split)};
  }

  FileScanTaskStreamPtr tasks_;
  int64_t target_size_;
  TaskGroupPacker packer_;
  std::optional<FileScanTaskSplitCursor> splits_;
};

}  // namespace

Result<FileScanTaskGroupStreamPtr> PlanTaskGroupStream(
    FileScanTaskStreamPtr tasks, const TaskGroupPlanningOptions& options) {
  if (tasks == nullptr) {
    return Invalid("Task stream must not be null");
  }
  if (options.target_size <= 0) {
    return Invalid("read.split.target-size must be positive, but is {}",
                   options.target_size);
  }
  if (options.lookback <= 0) {
    return Invalid("read.split.planning-lookback must be positive, but is {}",
                   options.lookback);
  }
  if (options.open_file_cost < 0) {
    return Invalid("read.split.open-file-cost must not be negative, but is {}",
                   options.open_file_cost);
  }
  return std::make_unique<TaskGroupPlanningStream>(std::move(tasks), options);
}

}  // namespace iceberg::internal
