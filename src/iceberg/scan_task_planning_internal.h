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

#pragma once

/// \file iceberg/scan_task_planning_internal.h
/// \brief Internal helpers that split and group file scan tasks for parallel reads.

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/result.h"
#include "iceberg/table_scan.h"
#include "iceberg/type_fwd.h"

namespace iceberg::internal {

/// \brief Maximum number of tasks in one planned group in this version.
inline constexpr int64_t kMaxTasksPerTaskGroup = 1024;

/// \brief Format-specific policy that supplies byte boundaries for splitting a data
/// file.
///
/// Policies are registered with SplitPolicyRegistry, usually by the format bundle
/// before planning.
class ICEBERG_EXPORT SplitPolicy {
 public:
  virtual ~SplitPolicy() = default;

  /// \brief Return strictly ascending split boundaries inside (0, file size), or an
  /// empty list when the file has no usable boundaries.
  ///
  /// Implementations decide whether the manifest's split offsets suffice or the file
  /// metadata must be inspected once. Boundaries are planning hints: readers assign
  /// row groups by midpoint, so boundaries that differ from read offsets cost balance,
  /// not correctness.
  ///
  /// \param io FileIO, used only when the policy must read file metadata.
  /// \param file The data file to split.
  virtual Result<std::vector<int64_t>> SplitBoundaries(const std::shared_ptr<FileIO>& io,
                                                       const DataFile& file) = 0;
};

/// \brief Registry of split policies by file format.
class ICEBERG_EXPORT SplitPolicyRegistry {
 public:
  /// \brief Register or replace the split policy for a file format.
  ///
  /// Policies must be registered before planning begins and must not be replaced while
  /// a planning stream that uses them is alive.
  static void Register(FileFormatType format, std::unique_ptr<SplitPolicy> policy);

  /// \brief Remove the split policy for a file format.
  static void Unregister(FileFormatType format);

  /// \brief Find the split policy for a file format, or nullptr when none is
  /// registered.
  ///
  /// The returned policy is borrowed from the registry and stays valid until the
  /// registry is mutated.
  static SplitPolicy* Find(FileFormatType format);
};

/// \brief Split-planning settings resolved from read.split.* properties.
struct TaskGroupPlanningOptions {
  /// Target combined task weight per group; read.split.target-size.
  int64_t target_size = 0;
  /// Maximum number of open groups while packing; read.split.planning-lookback.
  int64_t lookback = 0;
  /// Cost charged for every file a group opens; read.split.open-file-cost.
  int64_t open_file_cost = 0;
};

/// \brief Resolve split-planning settings by scan option, then table property, then
/// the default value.
///
/// \return An error for malformed values, a nonpositive target size or lookback, or a
/// negative open-file cost.
ICEBERG_EXPORT Result<TaskGroupPlanningOptions> ResolveTaskGroupPlanningOptions(
    const std::unordered_map<std::string, std::string>& scan_options,
    const std::unordered_map<std::string, std::string>& table_properties);

/// \brief The group-packing weight of a task, matching Java Iceberg's TableScanUtil:
/// max(range length + delete content bytes, (1 + delete file count) * open file cost).
///
/// Deletion vectors count their content size rather than their container file size,
/// matching Java's ScanTaskUtil.contentSizeInBytes.
/// \return An error when the cost overflows a signed 64-bit integer.
ICEBERG_EXPORT Result<int64_t> TaskGroupWeight(const FileScanTask& task,
                                               int64_t open_file_cost);

/// \brief Split one whole-file task into tasks whose ranges coalesce policy
/// boundaries toward the target size and cover the file exactly.
///
/// A task at or below the target size, a task whose format has no registered split
/// policy, or a task whose boundaries never reach the target size stays whole. An
/// already-split task is returned unchanged.
ICEBERG_EXPORT Result<std::vector<std::shared_ptr<FileScanTask>>> SplitFileScanTask(
    const std::shared_ptr<FileScanTask>& task, int64_t target_size,
    const std::shared_ptr<FileIO>& io);

/// \brief Lazily split and pack the tasks of \p tasks into task groups.
///
/// The returned single-pass stream owns \p tasks and passes \p io to split policies.
/// Groups are packed like Java Iceberg's bin packing: at most \p lookback groups stay
/// open, the largest open group is closed first, a group holds at most
/// kMaxTasksPerTaskGroup tasks, and an oversized task forms a singleton group.
ICEBERG_EXPORT Result<FileScanTaskGroupStreamPtr> PlanTaskGroupStream(
    FileScanTaskStreamPtr tasks, const TaskGroupPlanningOptions& options,
    std::shared_ptr<FileIO> io);

}  // namespace iceberg::internal
