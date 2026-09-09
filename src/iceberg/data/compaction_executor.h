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

/// \file iceberg/data/compaction_executor.h
/// Execute planned data-file compaction.

#include <memory>

#include "iceberg/iceberg_data_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

struct CompactionPlan;

/// \brief Executes a snapshot-bound data-file compaction plan.
///
/// The executor reads every planned source file through the delete-aware scan reader,
/// writes replacement data files, and commits all data and file-scoped position-delete
/// replacements in one table update. Generated files remain owned by the executor until
/// commit succeeds, commit state becomes unknown, or cleanup deletes them.
class ICEBERG_DATA_EXPORT CompactionExecutor {
 public:
  /// \brief Destroy the executor.
  ~CompactionExecutor();

  /// \brief Create an executor for a table.
  ///
  /// \param table Table whose files and metadata will be rewritten.
  /// \return A new executor, or an error if table is null.
  static Result<std::unique_ptr<CompactionExecutor>> Make(std::shared_ptr<Table> table);

  /// \brief Rewrite and atomically commit a snapshot-bound compaction plan.
  ///
  /// Execution refreshes the table and rejects plans whose snapshot is no longer
  /// current before writing. Commit validation also rejects deletes added after the
  /// plan snapshot. A definite failure attempts to clean generated files; cleanup
  /// failures are appended to the original error and undeleted files remain owned by
  /// this executor.
  ///
  /// \param plan Planner output containing the source snapshot and compaction groups.
  /// \return Success after commit, or the original planning, IO, validation, or commit
  /// error, including cleanup details when cleanup also fails.
  Status Execute(const CompactionPlan& plan);

  /// \brief Retry deletion of uncommitted output files owned by this executor.
  ///
  /// Successfully deleted or absent paths are released. Paths whose deletion fails
  /// remain owned and may be retried by calling Cleanup again.
  ///
  /// \return Success when every owned output is deleted, otherwise the first cleanup
  /// error.
  Status Cleanup();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;

  explicit CompactionExecutor(std::unique_ptr<Impl> impl);
};

}  // namespace iceberg
