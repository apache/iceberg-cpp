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
#include <cstdint>
#include <deque>
#include <limits>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>

#include "iceberg/result.h"
#include "iceberg/scan_task_planning_internal.h"
#include "iceberg/table_properties.h"
#include "iceberg/util/int128.h"
#include "iceberg/util/string_util.h"

namespace iceberg::internal {

namespace {

std::mutex& SplitPolicyMutex() {
  static std::mutex mutex;
  return mutex;
}

std::unordered_map<FileFormatType, std::unique_ptr<SplitPolicy>>& SplitPolicies() {
  static std::unordered_map<FileFormatType, std::unique_ptr<SplitPolicy>> policies;
  return policies;
}

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
    int64_t size = delete_file->file_size_in_bytes;
    // A deletion vector stores a blob inside a larger container; charge the blob,
    // like Java's ScanTaskUtil.contentSizeInBytes.
    if (delete_file->file_format == FileFormatType::kPuffin &&
        delete_file->content_size_in_bytes.has_value()) {
      size = *delete_file->content_size_in_bytes;
    }
    total += size;
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

void SplitPolicyRegistry::Register(FileFormatType format,
                                   std::unique_ptr<SplitPolicy> policy) {
  std::lock_guard lock(SplitPolicyMutex());
  SplitPolicies()[format] = std::move(policy);
}

void SplitPolicyRegistry::Unregister(FileFormatType format) {
  std::lock_guard lock(SplitPolicyMutex());
  SplitPolicies().erase(format);
}

SplitPolicy* SplitPolicyRegistry::Find(FileFormatType format) {
  std::lock_guard lock(SplitPolicyMutex());
  auto policy = SplitPolicies().find(format);
  return policy == SplitPolicies().end() ? nullptr : policy->second.get();
}

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

Result<std::vector<std::shared_ptr<FileScanTask>>> SplitFileScanTask(
    const std::shared_ptr<FileScanTask>& task, int64_t target_size,
    const std::shared_ptr<FileIO>& io) {
  const int64_t length = task->length();
  if (task->is_split() || length <= target_size) {
    return std::vector<std::shared_ptr<FileScanTask>>{task};
  }

  SplitPolicy* policy = SplitPolicyRegistry::Find(task->data_file()->file_format);
  if (policy == nullptr) {
    return std::vector<std::shared_ptr<FileScanTask>>{task};
  }
  ICEBERG_ASSIGN_OR_RAISE(auto boundaries,
                          policy->SplitBoundaries(io, *task->data_file()));
  auto make_split = [&](int64_t start, int64_t split_length) {
    return FileScanTask::MakeSplit(task->data_file(), start, split_length,
                                   task->delete_files(), task->residual_filter());
  };

  // Coalesce adjacent boundary intervals toward the target size. The first range
  // starts at zero, so the ranges cover [0, file size) exactly, including the bytes
  // before the first boundary.
  std::vector<std::shared_ptr<FileScanTask>> splits;
  int64_t split_start = 0;
  for (int64_t boundary : boundaries) {
    if (boundary - split_start >= target_size) {
      ICEBERG_ASSIGN_OR_RAISE(auto split,
                              make_split(split_start, boundary - split_start));
      splits.push_back(std::move(split));
      split_start = boundary;
    }
  }
  if (splits.empty()) {
    // No boundary is far enough from the start to reach the target size; the file
    // stays whole, like a file with a single huge row group.
    return std::vector<std::shared_ptr<FileScanTask>>{task};
  }
  if (split_start < length) {
    ICEBERG_ASSIGN_OR_RAISE(auto tail, make_split(split_start, length - split_start));
    splits.push_back(std::move(tail));
  }
  return splits;
}

namespace {

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
                          const TaskGroupPlanningOptions& options,
                          std::shared_ptr<FileIO> io)
      : tasks_(std::move(tasks)),
        options_(options),
        packer_(options),
        io_(std::move(io)) {}

  Result<std::optional<std::shared_ptr<FileScanTaskGroup>>> NextImpl() override {
    while (true) {
      if (input_exhausted_) {
        return packer_.NextRemainingGroup();
      }
      ICEBERG_ASSIGN_OR_RAISE(auto task, tasks_->Next());
      if (!task.has_value()) {
        input_exhausted_ = true;
        continue;
      }
      ICEBERG_ASSIGN_OR_RAISE(auto splits,
                              SplitFileScanTask(*task, options_.target_size, io_));
      for (auto& split : splits) {
        ICEBERG_ASSIGN_OR_RAISE(auto group, packer_.Add(std::move(split)));
        if (group.has_value()) {
          return group;
        }
      }
    }
  }

 private:
  FileScanTaskStreamPtr tasks_;
  TaskGroupPlanningOptions options_;
  TaskGroupPacker packer_;
  std::shared_ptr<FileIO> io_;
  bool input_exhausted_ = false;
};

}  // namespace

Result<FileScanTaskGroupStreamPtr> PlanTaskGroupStream(
    FileScanTaskStreamPtr tasks, const TaskGroupPlanningOptions& options,
    std::shared_ptr<FileIO> io) {
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
  return FileScanTaskGroupStreamPtr(
      new TaskGroupPlanningStream(std::move(tasks), options, std::move(io)));
}

}  // namespace iceberg::internal
