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

/// \file iceberg/update/overwrite_files.h

#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/update/merging_snapshot_update.h"
#include "iceberg/util/data_file_set.h"

namespace iceberg {

/// \brief API for overwriting files in a table.
///
/// This API accumulates file additions and produces a new Snapshot of the table by
/// replacing all deleted files with the set of additions. This operation is used
/// to implement idempotent writes that always replace a section of a table with
/// new data or update/delete operations that eagerly overwrite files.
///
/// Overwrites can be validated. The default validation mode is idempotent,
/// meaning the overwrite is correct and should be committed regardless of other
/// concurrent changes to the table. For example, this can be used for replacing
/// all data for day D with query results. Alternatively, this API can be
/// configured for overwriting certain files with their filtered versions while
/// ensuring no new data that would need to be filtered has been added.
///
/// When committing, these changes are applied to the latest table snapshot.
/// Commit conflicts are resolved by applying the changes to the new latest
/// snapshot and reattempting the commit.
class ICEBERG_EXPORT OverwriteFiles : public MergingSnapshotUpdate {
 public:
  /// \brief Create a new OverwriteFiles instance.
  ///
  /// \param table_name The name of the table
  /// \param ctx The transaction context to use for this update
  /// \return A Result containing the OverwriteFiles instance or an error
  static Result<std::shared_ptr<OverwriteFiles>> Make(
      std::string table_name, std::shared_ptr<TransactionContext> ctx);

  ~OverwriteFiles() override;

  /// \brief Add a DataFile to the table.
  ///
  /// \param file A data file.
  /// \return This OverwriteFiles for method chaining.
  OverwriteFiles& AddFile(const std::shared_ptr<DataFile>& file);

  /// \brief Delete a DataFile from the table.
  ///
  /// \param file A data file.
  /// \return This OverwriteFiles for method chaining.
  OverwriteFiles& DeleteFile(const std::shared_ptr<DataFile>& file);

  /// \brief Delete a set of data files from the table with their respective
  /// delete files.
  ///
  /// \param data_files_to_delete The data files to be deleted from the table.
  /// \param delete_files_to_delete The delete files corresponding to the data
  /// files to be deleted from the table.
  /// \return This OverwriteFiles for method chaining.
  OverwriteFiles& DeleteFiles(const DataFileSet& data_files_to_delete,
                              const DeleteFileSet& delete_files_to_delete);

  /// \brief Delete files that match an expression on data rows from the table.
  ///
  /// A file is selected to be deleted by the expression if it could contain any
  /// rows that match the expression. Candidate files are selected using an
  /// inclusive partition projection. These candidate files are deleted if all of
  /// the rows in the file must match the expression, determined by the
  /// expression's strict partition projection. This guarantees that files are
  /// deleted if and only if all rows in the file must match the expression.
  ///
  /// Files that may contain some rows that match the expression and some rows
  /// that do not will result in a validation error.
  ///
  /// \param expr An expression on rows in the table.
  /// \return This OverwriteFiles for method chaining.
  OverwriteFiles& OverwriteByRowFilter(std::shared_ptr<Expression> expr);

  /// \brief Set the snapshot ID used in any reads for this operation.
  ///
  /// Validations check changes after this snapshot ID. If the from snapshot is
  /// not set, all ancestor snapshots through the table's initial snapshot are
  /// validated.
  ///
  /// \param snapshot_id A snapshot ID.
  /// \return This OverwriteFiles for method chaining.
  OverwriteFiles& ValidateFromSnapshot(int64_t snapshot_id);

  /// \brief Set a conflict detection filter used to validate concurrently added
  /// data and delete files.
  ///
  /// \param expr An expression on rows in the table.
  /// \return This OverwriteFiles for method chaining.
  OverwriteFiles& ConflictDetectionFilter(std::shared_ptr<Expression> expr);

  /// \brief Enable validation that data added concurrently does not conflict
  /// with this commit's operation.
  ///
  /// This method should be called while committing non-idempotent overwrite
  /// operations. If a concurrent operation commits a new file after the data was
  /// read and that file might contain rows matching the specified conflict
  /// detection filter, the overwrite operation detects this and fails.
  ///
  /// Calling this method with a correct conflict detection filter is required to
  /// maintain isolation for non-idempotent overwrite operations.
  ///
  /// Validation uses the conflict detection filter passed to
  /// ConflictDetectionFilter() and applies to operations that happened after the
  /// snapshot passed to ValidateFromSnapshot(). If the conflict detection filter
  /// is not set, any new data added concurrently will fail this overwrite
  /// operation.
  ///
  /// \return This OverwriteFiles for method chaining.
  OverwriteFiles& ValidateNoConflictingData();

  /// \brief Enable validation that deletes that happened concurrently do not
  /// conflict with this commit's operation.
  ///
  /// Validating concurrent deletes is required during non-idempotent overwrite
  /// operations. If a concurrent operation deletes data in one of the files being
  /// overwritten, the overwrite operation must be aborted as it may undelete rows
  /// that were removed concurrently.
  ///
  /// Calling this method with a correct conflict detection filter is required to
  /// maintain isolation for non-idempotent overwrite operations.
  ///
  /// Validation uses the conflict detection filter passed to
  /// ConflictDetectionFilter() and applies to operations that happened after the
  /// snapshot passed to ValidateFromSnapshot(). If the conflict detection filter
  /// is not set, this operation will use the row filter provided in
  /// OverwriteByRowFilter() to check for new delete files and will ensure there
  /// are no conflicting deletes for data files removed via DeleteFile().
  ///
  /// \return This OverwriteFiles for method chaining.
  OverwriteFiles& ValidateNoConflictingDeletes();

  /// \brief Signal that each file added to the table must match the overwrite
  /// expression.
  ///
  /// If this method is called, each added file is validated on commit to ensure
  /// that it matches the overwrite row filter. This is used to ensure that
  /// writes are idempotent: files cannot be added during a commit that would not
  /// be removed if the operation were run a second time.
  ///
  /// \return This OverwriteFiles for method chaining.
  OverwriteFiles& ValidateAddedFilesMatchOverwriteFilter();

  /// \brief Enable or disable case-sensitive expression binding for validations
  /// that accept expressions.
  ///
  /// \param case_sensitive Whether expression binding should be case sensitive.
  /// \return This OverwriteFiles for method chaining.
  OverwriteFiles& CaseSensitive(bool case_sensitive);

  std::string operation() override;

 protected:
  Status Validate(const TableMetadata& current_metadata,
                  const std::shared_ptr<Snapshot>& snapshot) override;

 private:
  explicit OverwriteFiles(std::string table_name,
                          std::shared_ptr<TransactionContext> ctx);

  /// \brief Select the conflict-detection filter from the configured state.
  std::shared_ptr<Expression> DataConflictDetectionFilter() const;

  DataFileSet deleted_data_files_;
  bool validate_added_files_match_overwrite_filter_ = false;
  std::optional<int64_t> starting_snapshot_id_;
  std::shared_ptr<Expression> conflict_detection_filter_;
  bool validate_new_data_files_ = false;
  bool validate_new_deletes_ = false;
};

}  // namespace iceberg
