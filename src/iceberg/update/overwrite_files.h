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

/// \brief Overwrite data files in a table.
///
/// Supports explicit file replacement and range-based replacement by row filter.
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

  /// \brief Add a new data file to the overwrite.
  ///
  /// \param file The data file to add
  /// \return Reference to this for method chaining
  OverwriteFiles& AddFile(const std::shared_ptr<DataFile>& file);

  /// \brief Delete an existing data file as part of the overwrite.
  ///
  /// \param file The data file to delete
  /// \return Reference to this for method chaining
  OverwriteFiles& DeleteFile(const std::shared_ptr<DataFile>& file);

  /// \brief Remove data files and delete files as part of the overwrite.
  ///
  /// \param data_files_to_delete The data files to delete
  /// \param delete_files_to_delete The delete files to remove
  /// \return Reference to this for method chaining
  OverwriteFiles& DeleteFiles(const DataFileSet& data_files_to_delete,
                              const DeleteFileSet& delete_files_to_delete);

  /// \brief Overwrite all rows matching the given expression.
  ///
  /// \param expr The row filter expression defining the overwrite range
  /// \return Reference to this for method chaining
  OverwriteFiles& OverwriteByRowFilter(std::shared_ptr<Expression> expr);

  /// \brief Set the lower bound snapshot id for concurrency scans.
  ///
  /// \param snapshot_id The starting snapshot id
  /// \return Reference to this for method chaining
  OverwriteFiles& ValidateFromSnapshot(int64_t snapshot_id);

  /// \brief Set an explicit conflict-detection filter.
  ///
  /// \param expr The conflict-detection filter expression
  /// \return Reference to this for method chaining
  OverwriteFiles& ConflictDetectionFilter(std::shared_ptr<Expression> expr);

  /// \brief Enable validation that no concurrent commit added conflicting data files.
  ///
  /// \return Reference to this for method chaining
  OverwriteFiles& ValidateNoConflictingData();

  /// \brief Enable validation that no concurrent commit added conflicting deletes.
  ///
  /// \return Reference to this for method chaining
  OverwriteFiles& ValidateNoConflictingDeletes();

  /// \brief Enable strict validation that every added file is fully within the
  /// overwrite range.
  ///
  /// \return Reference to this for method chaining
  OverwriteFiles& ValidateAddedFilesMatchOverwriteFilter();

  /// \brief Set case sensitivity for binding, projection, and metrics evaluation.
  ///
  /// \param case_sensitive Whether matching should be case-sensitive
  /// \return Reference to this for method chaining
  OverwriteFiles& WithCaseSensitivity(bool case_sensitive);

  std::string operation() override;

  Status Validate(const TableMetadata& current_metadata,
                  const std::shared_ptr<Snapshot>& snapshot) override;

 private:
  explicit OverwriteFiles(std::string table_name,
                          std::shared_ptr<TransactionContext> ctx);

  /// \brief Select the conflict-detection filter from the configured state.
  std::shared_ptr<Expression> DataConflictDetectionFilter() const;

  /// \brief Verify every added data file is fully contained in the row filter.
  Status ValidateAddedFilesMatchOverwriteFilterImpl(const TableMetadata& metadata);

  std::optional<int64_t> starting_snapshot_id_;
  std::shared_ptr<Expression> conflict_detection_filter_;
  DataFileSet deleted_data_files_;
  bool validate_no_conflicting_data_ = false;
  bool validate_no_conflicting_deletes_ = false;
  bool validate_added_files_match_overwrite_filter_ = false;
};

}  // namespace iceberg
