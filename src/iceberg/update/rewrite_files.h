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

/// \file iceberg/update/rewrite_files.h
/// RewriteFiles operation for replacing files in a table.

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/update/merging_snapshot_update.h"
#include "iceberg/util/data_file_set.h"

namespace iceberg {

/// \brief API for replacing files in a table.
///
/// This operation accumulates file additions and deletions, produces a new
/// Snapshot of the changes, and commits that snapshot as the current.
///
/// When committing, these changes will be applied to the latest table snapshot.
/// Commit conflicts will be resolved by applying the changes to the new latest
/// snapshot and reattempting the commit. If any of the deleted files are no
/// longer in the latest snapshot when reattempting, the commit will throw a
/// ValidationException.
///
/// Note that the new state of the table after each rewrite must be logically
/// equivalent to the original table state.
class ICEBERG_EXPORT RewriteFiles : public MergingSnapshotUpdate {
 public:
  /// \brief Create a new RewriteFiles operation.
  ///
  /// \param table_name The name of the table
  /// \param ctx The transaction context
  /// \return A unique pointer to the new RewriteFiles operation
  static Result<std::unique_ptr<RewriteFiles>> Make(
      std::string table_name, std::shared_ptr<TransactionContext> ctx);

  ~RewriteFiles() override = default;

  /// \brief Remove a data file from the current table state.
  ///
  /// This rewrite operation may change the size or layout of the data files.
  /// The set of live data records must never change.
  ///
  /// \param data_file a rewritten data file
  /// \return this for method chaining
  RewriteFiles& DeleteDataFile(const std::shared_ptr<DataFile>& data_file);

  /// \brief Remove a delete file from the table state.
  ///
  /// This rewrite operation may change the size or layout of the delete files.
  /// The set of applicable delete records must never change.
  ///
  /// \param delete_file a rewritten delete file
  /// \return this for method chaining
  RewriteFiles& DeleteDeleteFile(const std::shared_ptr<DataFile>& delete_file);

  /// \brief Add a new data file or delete file.
  ///
  /// This rewrite operation may change the size or layout of the files.
  /// The set of live data records must never change.
  ///
  /// \param file a new file (data or delete)
  /// \return this for method chaining
  RewriteFiles& AddDataFile(const std::shared_ptr<DataFile>& file);

  /// \brief Add a new delete file.
  ///
  /// \param delete_file a new delete file
  /// \return this for method chaining
  RewriteFiles& AddDeleteFile(const std::shared_ptr<DataFile>& delete_file);

  /// \brief Add a new delete file with the given data sequence number.
  ///
  /// To ensure equivalence in the set of applicable delete records, the
  /// sequence number of the delete file must be the max sequence number of
  /// the delete files that it is replacing.
  ///
  /// \param delete_file a new delete file
  /// \param data_sequence_number data sequence number to append on the file
  /// \return this for method chaining
  RewriteFiles& AddDeleteFile(const std::shared_ptr<DataFile>& delete_file,
                              int64_t data_sequence_number);

  /// \brief Configure the data sequence number for this rewrite operation.
  ///
  /// This data sequence number will be used for all new data files that are
  /// added in this rewrite. This is helpful to avoid commit conflicts between
  /// data compaction and adding equality deletes.
  ///
  /// \param sequence_number a data sequence number
  /// \return this for method chaining
  RewriteFiles& SetDataSequenceNumber(int64_t sequence_number);

  /// \brief Add a rewrite that replaces one set of data files with another set
  /// that contains the same data. The sequence number provided will be used for
  /// all the data files added.
  ///
  /// \param files_to_delete files that will be replaced (deleted)
  /// \param files_to_add files that will be added
  /// \param sequence_number sequence number to use for all data files added
  /// \return this for method chaining
  RewriteFiles& RewriteDataFiles(
      const std::vector<std::shared_ptr<DataFile>>& files_to_delete,
      const std::vector<std::shared_ptr<DataFile>>& files_to_add,
      int64_t sequence_number);

  /// \brief Add a rewrite that replaces one set of files with another set that
  /// contains the same data.
  ///
  /// \param data_files_to_replace data files that will be replaced (deleted)
  /// \param delete_files_to_replace delete files that will be replaced (deleted)
  /// \param data_files_to_add data files that will be added
  /// \param delete_files_to_add delete files that will be added
  /// \return this for method chaining
  RewriteFiles& Rewrite(
      const std::vector<std::shared_ptr<DataFile>>& data_files_to_replace,
      const std::vector<std::shared_ptr<DataFile>>& delete_files_to_replace,
      const std::vector<std::shared_ptr<DataFile>>& data_files_to_add,
      const std::vector<std::shared_ptr<DataFile>>& delete_files_to_add);

  /// \brief Set the snapshot ID used in any reads for this operation.
  ///
  /// Validations will check changes after this snapshot ID. If this is not
  /// called, all ancestor snapshots through the table's initial snapshot are
  /// validated.
  ///
  /// \param snapshot_id a snapshot ID
  /// \return this for method chaining
  RewriteFiles& ValidateFromSnapshot(int64_t snapshot_id);

  /// \brief Perform operations on a particular branch.
  ///
  /// \param branch name of a SnapshotRef of type branch
  /// \return this for method chaining
  RewriteFiles& ToBranch(const std::string& branch);

  std::string operation() override;

 protected:
  Status Validate(const TableMetadata& current_metadata,
                  const std::shared_ptr<Snapshot>& snapshot) override;

  explicit RewriteFiles(std::string table_name, std::shared_ptr<TransactionContext> ctx);

 private:
  /// \brief Validate the replaced and added files invariants.
  ///
  /// Ensures that:
  /// - Files to delete cannot be empty
  /// - Data files to add must be empty if there's no data file to be rewritten
  /// - Delete files to add must be empty if there's no delete file to be rewritten
  void ValidateReplacedAndAddedFiles();

  /// \brief Tracks which data files are being replaced, for conflict detection.
  DataFileSet replaced_data_files_;

  /// \brief Optional snapshot ID boundary for validation scope.
  std::optional<int64_t> starting_snapshot_id_;
};

}  // namespace iceberg
