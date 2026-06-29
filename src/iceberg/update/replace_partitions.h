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

/// \file iceberg/update/replace_partitions.h

#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/update/merging_snapshot_update.h"
#include "iceberg/util/partition_value_util.h"

namespace iceberg {

/// \brief Replaces partitions in a table with new data files.
///
/// ReplacePartitions dynamically identifies which partitions to overwrite based
/// on the data files added via AddFile(). All existing data files in each
/// touched partition are marked DELETED, and the new files are written as the
/// sole data in those partitions. Partitions not referenced by any added file
/// are left unchanged.
///
/// This operation produces a snapshot with operation="overwrite" and
/// "replace-partitions"="true" in the summary. For unpartitioned tables, all
/// existing files are replaced.
///
/// When committing, these changes are applied to the latest table snapshot.
/// Commit conflicts are resolved by re-applying to the new latest snapshot
/// and reattempting the commit.
///
/// \note This is provided to implement SQL compatible with Hive table
/// operations but is not recommended. Instead, use OverwriteFiles to
/// explicitly overwrite data.
class ICEBERG_EXPORT ReplacePartitions : public MergingSnapshotUpdate {
 public:
  /// \brief Create a new ReplacePartitions instance.
  ///
  /// \param table_name The name of the table
  /// \param ctx The transaction context
  /// \return A Result containing the ReplacePartitions instance or an error
  static Result<std::unique_ptr<ReplacePartitions>> Make(
      std::string table_name, std::shared_ptr<TransactionContext> ctx);

  /// \brief Add a data file and mark its partition for replacement.
  ///
  /// Each call registers the file's partition so all existing data files in
  /// that partition are replaced. Duplicate files (same path) are ignored.
  ///
  /// \param file The data file to add (must have partition_spec_id set)
  /// \return Reference to this for method chaining
  ReplacePartitions& AddFile(const std::shared_ptr<DataFile>& file);

  /// \brief Fail the commit if any existing data file would be deleted.
  ///
  /// This validation is useful to ensure the operation is only applied to
  /// tables where no data currently exists in the affected partitions.
  ///
  /// \return Reference to this for method chaining
  ReplacePartitions& ValidateAppendOnly();

  /// \brief Set the snapshot ID used as the baseline for conflict validation.
  ///
  /// Validations check changes that occurred after this snapshot ID. If not
  /// set, all ancestor snapshots through the initial snapshot are validated.
  ///
  /// \param snapshot_id A snapshot ID
  /// \return Reference to this for method chaining
  ReplacePartitions& ValidateFromSnapshot(int64_t snapshot_id);

  /// \brief Enable validation that no conflicting data files were added concurrently.
  ///
  /// Fails the commit if a concurrent operation added a data file in any of
  /// the partitions being replaced after the snapshot set by
  /// ValidateFromSnapshot().
  ///
  /// \return Reference to this for method chaining
  ReplacePartitions& ValidateNoConflictingData();

  /// \brief Enable validation that no conflicting delete files were added concurrently.
  ///
  /// Fails the commit if a concurrent operation added a delete file covering
  /// any of the partitions being replaced after the snapshot set by
  /// ValidateFromSnapshot().
  ///
  /// \return Reference to this for method chaining
  ReplacePartitions& ValidateNoConflictingDeletes();

  std::string operation() override;

 protected:
  Status Validate(const TableMetadata& current_metadata,
                  const std::shared_ptr<Snapshot>& snapshot) override;

 private:
  explicit ReplacePartitions(std::string table_name,
                             std::shared_ptr<TransactionContext> ctx);

  std::optional<int64_t> starting_snapshot_id_;
  bool validate_conflicting_data_{false};
  bool validate_conflicting_deletes_{false};
  // True once an AddFile() call has staged a file whose partition spec is
  // unpartitioned. Java's BaseReplacePartitions treats this case as a
  // table-wide replace (DeleteByRowFilter(AlwaysTrue())) and runs conflict
  // validation against AlwaysTrue rather than a partition set — mirror that.
  bool replace_by_row_filter_{false};
  // Partitions touched by AddFile() in partitioned specs. Used to scope
  // conflict validation to the overwritten partitions in the partitioned
  // case; ignored when replace_by_row_filter_ is true.
  PartitionSet replaced_partitions_;
};

}  // namespace iceberg
