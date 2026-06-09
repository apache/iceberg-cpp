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

/// \brief Row-level delta operation for adding rows and delete files.
///
/// RowDelta is the C++ counterpart of Java BaseRowDelta. It can add data files,
/// add delete files, remove data/delete files, and validate conflicts against
/// snapshots committed after a configured starting snapshot.
class ICEBERG_EXPORT RowDelta : public MergingSnapshotUpdate {
 public:
  /// \brief Create a new RowDelta instance.
  static Result<std::unique_ptr<RowDelta>> Make(std::string table_name,
                                                std::shared_ptr<TransactionContext> ctx);

  /// \brief Add a data file containing inserted rows.
  RowDelta& AddRows(const std::shared_ptr<DataFile>& inserts);

  /// \brief Add a delete file.
  RowDelta& AddDeletes(const std::shared_ptr<DataFile>& deletes);

  /// \brief Remove a data file from the table.
  RowDelta& RemoveRows(const std::shared_ptr<DataFile>& file);

  /// \brief Remove a delete file from the table.
  RowDelta& RemoveDeletes(const std::shared_ptr<DataFile>& deletes);

  /// \brief Validate against snapshots committed after snapshot_id.
  RowDelta& ValidateFromSnapshot(int64_t snapshot_id);

  /// \brief Set case sensitivity for conflict detection.
  RowDelta& CaseSensitive(bool case_sensitive);

  /// \brief Validate that referenced data files still exist.
  RowDelta& ValidateDataFilesExist(std::span<const std::string> referenced_files);

  /// \brief Fail if any requested data/delete-file removal is missing from
  /// manifests when the table has a current snapshot.
  RowDelta& ValidateDeletedFiles();

  /// \brief Set the conflict detection filter used by validation methods.
  RowDelta& ConflictDetectionFilter(std::shared_ptr<Expression> filter);

  /// \brief Validate that no matching data files were concurrently added.
  RowDelta& ValidateNoConflictingDataFiles();

  /// \brief Validate that no matching delete files were concurrently added.
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
