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

#include "iceberg/update/row_delta.h"

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "iceberg/expression/expressions.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/transaction.h"
#include "iceberg/util/error_collector.h"
#include "iceberg/util/formatter_internal.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/snapshot_util_internal.h"

namespace iceberg {

Result<std::unique_ptr<RowDelta>> RowDelta::Make(
    std::string table_name, std::shared_ptr<TransactionContext> ctx) {
  ICEBERG_PRECHECK(!table_name.empty(), "Table name cannot be empty");
  ICEBERG_PRECHECK(ctx != nullptr, "Cannot create RowDelta without a context");
  return std::unique_ptr<RowDelta>(new RowDelta(std::move(table_name), std::move(ctx)));
}

RowDelta::RowDelta(std::string table_name, std::shared_ptr<TransactionContext> ctx)
    : MergingSnapshotUpdate(std::move(table_name), std::move(ctx)),
      conflict_detection_filter_(Expressions::AlwaysTrue()) {}

RowDelta& RowDelta::AddRows(const std::shared_ptr<DataFile>& inserts) {
  ICEBERG_BUILDER_RETURN_IF_ERROR(AddDataFile(inserts));
  return *this;
}

RowDelta& RowDelta::AddDeletes(const std::shared_ptr<DataFile>& deletes) {
  ICEBERG_BUILDER_RETURN_IF_ERROR(AddDeleteFile(deletes));
  return *this;
}

RowDelta& RowDelta::RemoveRows(const std::shared_ptr<DataFile>& file) {
  ICEBERG_BUILDER_RETURN_IF_ERROR(DeleteDataFile(file));
  removed_data_files_.insert(file);
  return *this;
}

RowDelta& RowDelta::RemoveDeletes(const std::shared_ptr<DataFile>& deletes) {
  ICEBERG_BUILDER_RETURN_IF_ERROR(DeleteDeleteFile(deletes));
  return *this;
}

RowDelta& RowDelta::ValidateFromSnapshot(int64_t snapshot_id) {
  starting_snapshot_id_ = snapshot_id;
  return *this;
}

RowDelta& RowDelta::CaseSensitive(bool case_sensitive) {
  MergingSnapshotUpdate::CaseSensitive(case_sensitive);
  return *this;
}

RowDelta& RowDelta::ValidateDataFilesExist(
    std::span<const std::string> referenced_files) {
  for (const auto& file : referenced_files) {
    referenced_data_files_.insert(file);
  }
  return *this;
}

RowDelta& RowDelta::ValidateDeletedFiles() {
  validate_deletes_ = true;
  return *this;
}

RowDelta& RowDelta::ConflictDetectionFilter(std::shared_ptr<Expression> filter) {
  ICEBERG_BUILDER_CHECK(filter != nullptr, "Conflict detection filter cannot be null");
  conflict_detection_filter_ = std::move(filter);
  return *this;
}

RowDelta& RowDelta::ValidateNoConflictingDataFiles() {
  validate_new_data_files_ = true;
  return *this;
}

RowDelta& RowDelta::ValidateNoConflictingDeleteFiles() {
  validate_new_delete_files_ = true;
  return *this;
}

std::string RowDelta::operation() {
  if (AddsDataFiles() && !AddsDeleteFiles() && !DeletesDataFiles()) {
    return DataOperation::kAppend;
  }

  if (AddsDeleteFiles() && !AddsDataFiles()) {
    return DataOperation::kDelete;
  }

  return DataOperation::kOverwrite;
}

Status RowDelta::Validate(const TableMetadata& current_metadata,
                          const std::shared_ptr<Snapshot>& snapshot) {
  if (snapshot == nullptr) {
    return {};
  }

  if (validate_deletes_) {
    FailMissingDeletePaths();
  }

  if (starting_snapshot_id_.has_value()) {
    ICEBERG_ASSIGN_OR_RAISE(bool is_ancestor, SnapshotUtil::IsAncestorOf(
                                                  current_metadata, snapshot->snapshot_id,
                                                  starting_snapshot_id_.value()));
    ICEBERG_CHECK(is_ancestor, "Snapshot {} is not an ancestor of {}",
                  starting_snapshot_id_.value(), snapshot->snapshot_id);
  }

  auto io = ctx_->table->io();
  if (!referenced_data_files_.empty()) {
    ICEBERG_RETURN_UNEXPECTED(MergingSnapshotUpdate::ValidateDataFilesExist(
        current_metadata, starting_snapshot_id_, referenced_data_files_,
        /*skip_deletes=*/!validate_deletes_, conflict_detection_filter_, snapshot, io,
        IsCaseSensitive()));
  }

  if (validate_new_data_files_) {
    ICEBERG_RETURN_UNEXPECTED(MergingSnapshotUpdate::ValidateAddedDataFiles(
        current_metadata, starting_snapshot_id_, conflict_detection_filter_, snapshot, io,
        IsCaseSensitive()));
  }

  if (validate_new_delete_files_) {
    if (!removed_data_files_.empty()) {
      ICEBERG_RETURN_UNEXPECTED(MergingSnapshotUpdate::ValidateNoNewDeletesForDataFiles(
          current_metadata, starting_snapshot_id_, conflict_detection_filter_,
          removed_data_files_, snapshot, io, IsCaseSensitive()));
    }

    ICEBERG_RETURN_UNEXPECTED(MergingSnapshotUpdate::ValidateNoNewDeleteFiles(
        current_metadata, starting_snapshot_id_, conflict_detection_filter_, snapshot, io,
        IsCaseSensitive()));
  }

  ICEBERG_RETURN_UNEXPECTED(ValidateNoConflictingFileAndPositionDeletes());

  return MergingSnapshotUpdate::ValidateAddedDVs(
      current_metadata, starting_snapshot_id_, conflict_detection_filter_, snapshot, io);
}

Status RowDelta::ValidateNoConflictingFileAndPositionDeletes() const {
  std::vector<std::string_view> conflicting_files;
  for (const auto& file : removed_data_files_) {
    if (file != nullptr && referenced_data_files_.contains(file->file_path)) {
      conflicting_files.push_back(file->file_path);
    }
  }

  if (!conflicting_files.empty()) {
    return ValidationFailed(
        "Cannot delete data files {} that are referenced by new delete files",
        FormatRange(conflicting_files, ", ", "[", "]"));
  }

  return {};
}

}  // namespace iceberg
