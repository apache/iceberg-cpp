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

/// \file iceberg/update/row_delta.h

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <unordered_set>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/update/merging_snapshot_update.h"
#include "iceberg/util/data_file_set.h"

namespace iceberg {

/// \brief API for encoding row-level changes to a table.
///
/// This API accumulates data and delete file changes, produces a new Snapshot
/// of the table, and commits that snapshot as the current.
///
/// When committing, these changes are applied to the latest table snapshot.
/// Commit conflicts are resolved by applying the changes to the new latest
/// snapshot and reattempting the commit.
class ICEBERG_EXPORT RowDelta : public MergingSnapshotUpdate {
 public:
  /// \brief Create a new RowDelta instance.
  static Result<std::unique_ptr<RowDelta>> Make(std::string table_name,
                                                std::shared_ptr<TransactionContext> ctx);

  /// \brief Add a DataFile to the table.
  ///
  /// \param inserts A data file of rows to insert.
  /// \return This RowDelta for method chaining.
  RowDelta& AddRows(const std::shared_ptr<DataFile>& inserts);

  /// \brief Add a DeleteFile to the table.
  ///
  /// \param deletes A delete file of rows to delete.
  /// \return This RowDelta for method chaining.
  RowDelta& AddDeletes(const std::shared_ptr<DataFile>& deletes);

  /// \brief Remove a DataFile from the table.
  ///
  /// \param file A data file.
  /// \return This RowDelta for method chaining.
  RowDelta& RemoveRows(const std::shared_ptr<DataFile>& file);

  /// \brief Remove a rewritten DeleteFile from the table.
  ///
  /// \param deletes A delete file that can be removed from the table.
  /// \return This RowDelta for method chaining.
  RowDelta& RemoveDeletes(const std::shared_ptr<DataFile>& deletes);

  /// \brief Set the snapshot ID used in any reads for this operation.
  ///
  /// Validations check changes after this snapshot ID. If the from snapshot is
  /// not set, all ancestor snapshots through the table's initial snapshot are
  /// validated.
  ///
  /// \param snapshot_id A snapshot ID.
  /// \return This RowDelta for method chaining.
  RowDelta& ValidateFromSnapshot(int64_t snapshot_id);

  /// \brief Enable or disable case-sensitive expression binding for validations.
  ///
  /// \param case_sensitive Whether expression binding should be case sensitive.
  /// \return This RowDelta for method chaining.
  RowDelta& CaseSensitive(bool case_sensitive);

  /// \brief Add data file paths that must not be removed by conflicting commits.
  ///
  /// If any path has been removed by a conflicting commit in the table since
  /// the snapshot passed to ValidateFromSnapshot(), the operation fails.
  ///
  /// By default, this validation checks only rewrite and overwrite commits. To
  /// apply validation to delete commits, call ValidateDeletedFiles().
  ///
  /// \param referenced_files File paths that are referenced by a position
  /// delete file.
  /// \return This RowDelta for method chaining.
  RowDelta& ValidateDataFilesExist(std::span<const std::string> referenced_files);

  /// \brief Enable validation that referenced data files were not deleted.
  ///
  /// If a data file has a row deleted using a position delete file, rewriting
  /// or overwriting the data file concurrently would un-delete the row. Deleting
  /// the data file is normally allowed, but a delete may be part of a
  /// transaction that reads and re-appends a row. This method is used to
  /// validate deletes for the transaction case.
  ///
  /// \return This RowDelta for method chaining.
  RowDelta& ValidateDeletedFiles();

  /// \brief Set a conflict detection filter used to validate concurrently added
  /// data and delete files.
  ///
  /// If not called, a true literal is used as the conflict detection filter.
  ///
  /// \param filter An expression on rows in the table.
  /// \return This RowDelta for method chaining.
  RowDelta& ConflictDetectionFilter(std::shared_ptr<Expression> filter);

  /// \brief Enable validation that concurrent data files do not conflict.
  ///
  /// This method should be called when the table is queried to determine which
  /// files to delete or append. If a concurrent operation commits a new file
  /// after the data was read and that file might contain rows matching the
  /// conflict detection filter, this operation detects that during retries and
  /// fails.
  ///
  /// Calling this method is required to maintain serializable isolation for
  /// update/delete operations. Otherwise, the isolation level is snapshot
  /// isolation.
  ///
  /// Validation uses the filter passed to ConflictDetectionFilter() and applies
  /// to operations after the snapshot passed to ValidateFromSnapshot().
  ///
  /// \return This RowDelta for method chaining.
  RowDelta& ValidateNoConflictingDataFiles();

  /// \brief Enable validation that concurrent delete files do not conflict.
  ///
  /// This method must be called when the table is queried to produce a row
  /// delta for UPDATE and MERGE operations independently of the isolation level.
  /// Calling this method is not required for DELETE operations because it is OK
  /// to delete a record that is also deleted concurrently.
  ///
  /// Validation uses the filter passed to ConflictDetectionFilter() and applies
  /// to operations after the snapshot passed to ValidateFromSnapshot().
  ///
  /// \return This RowDelta for method chaining.
  RowDelta& ValidateNoConflictingDeleteFiles();

  std::string operation() override;

 protected:
  Status Validate(const TableMetadata& current_metadata,
                  const std::shared_ptr<Snapshot>& snapshot) override;

 private:
  explicit RowDelta(std::string table_name, std::shared_ptr<TransactionContext> ctx);

  Status ValidateNoConflictingFileAndPositionDeletes() const;

  std::optional<int64_t> starting_snapshot_id_;
  std::unordered_set<std::string> referenced_data_files_;
  DataFileSet removed_data_files_;
  bool validate_deletes_ = false;
  std::shared_ptr<Expression> conflict_detection_filter_;
  bool validate_new_data_files_ = false;
  bool validate_new_delete_files_ = false;
};

}  // namespace iceberg
