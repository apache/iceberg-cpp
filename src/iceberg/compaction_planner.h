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

/// \file iceberg/compaction_planner.h
/// Plan data-file compaction without executing rewrites.

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Thresholds used to select data files for compaction.
struct ICEBERG_EXPORT CompactionPlannerConfig {
  /// Desired aggregate data-file size for each compaction group.
  ///
  /// A single selected file may exceed this size.
  int64_t target_file_size_bytes = int64_t{512} * 1024 * 1024;

  /// Files smaller than this ratio of the target size are small-file candidates.
  double min_file_size_ratio = 0.75;

  /// Files larger than this ratio of the target size are oversized.
  ///
  /// Oversized files are selected only when they also meet an enabled delete-pressure
  /// threshold.
  double max_file_size_ratio = 1.8;

  /// Minimum number of files required for a group containing only small-file candidates.
  size_t min_input_files = 5;

  /// Minimum number of applicable file-scoped position delete files.
  ///
  /// A value of zero disables this selection criterion.
  int64_t delete_file_threshold = 2;

  /// Minimum ratio of deleted records to data-file records.
  ///
  /// A value of zero disables this selection criterion.
  double delete_ratio_threshold = 0.3;
};

/// \brief A selected data file and its file-scoped delete pressure.
struct ICEBERG_EXPORT CompactionFile {
  /// Scan task for the selected data file and its applicable delete files.
  std::shared_ptr<FileScanTask> scan_task;

  /// Number of distinct applicable file-scoped position delete files.
  int64_t file_scoped_delete_count = 0;

  /// Number of deleted records, capped at the data file's record count.
  int64_t file_scoped_delete_record_count = 0;
};

/// \brief Selected files from one partition that can be rewritten together.
struct ICEBERG_EXPORT CompactionGroup {
  /// Partition spec ID shared by every file in this group.
  int32_t partition_spec_id;

  /// Partition tuple shared by every file in this group.
  PartitionValues partition;

  /// Selected files in canonical file-path order.
  std::vector<CompactionFile> files;

  /// Aggregate size of the selected data files.
  int64_t data_file_size_bytes = 0;

  /// Aggregate file-scoped deleted-record count.
  int64_t file_scoped_delete_record_count = 0;
};

/// \brief Result of compaction planning.
struct ICEBERG_EXPORT CompactionPlan {
  /// Snapshot whose scan tasks were used to produce this plan.
  ///
  /// An executor must verify this snapshot is still valid before rewriting files.
  int64_t source_snapshot_id;

  /// Compaction groups in canonical partition and file order.
  std::vector<CompactionGroup> groups;
};

/// \brief Select and group scan tasks for data-file compaction.
class ICEBERG_EXPORT CompactionPlanner {
 public:
  /// \brief Plan deterministic, partition-isolated compaction groups.
  ///
  /// \param source_snapshot_id Non-negative snapshot whose scan tasks are being planned.
  /// \param scan_tasks Data-file scan tasks with applicable delete files.
  /// \param config Candidate selection and group sizing thresholds.
  /// \return A metadata-only compaction plan, or an error for invalid configuration,
  /// snapshot ID, duplicate data-file tasks, metadata, partition keys, delete
  /// cardinality, or aggregate overflow.
  static Result<CompactionPlan> Plan(
      int64_t source_snapshot_id,
      std::span<const std::shared_ptr<FileScanTask>> scan_tasks,
      const CompactionPlannerConfig& config = {});
};

}  // namespace iceberg
