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

#include "iceberg/update/rewrite_files.h"

#include <memory>
#include <string>
#include <vector>

#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/result.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/transaction.h"
#include "iceberg/util/macros.h"

namespace iceberg {

RewriteFiles::RewriteFiles(std::string table_name,
                           std::shared_ptr<TransactionContext> ctx)
    : MergingSnapshotUpdate(std::move(table_name), std::move(ctx)) {
  // Replace files must fail if any of the deleted paths is missing and cannot be deleted
  FailMissingDeletePaths();
}

Result<std::unique_ptr<RewriteFiles>> RewriteFiles::Make(
    std::string table_name, std::shared_ptr<TransactionContext> ctx) {
  ICEBERG_PRECHECK(!table_name.empty(), "Table name cannot be empty");
  ICEBERG_PRECHECK(ctx != nullptr, "Cannot create RewriteFiles without a context");
  return std::unique_ptr<RewriteFiles>(
      new RewriteFiles(std::move(table_name), std::move(ctx)));
}

RewriteFiles& RewriteFiles::DeleteDataFile(const std::shared_ptr<DataFile>& data_file) {
  ICEBERG_BUILDER_CHECK(data_file != nullptr, "Invalid data file: null");
  ICEBERG_BUILDER_CHECK(data_file->content == DataFile::Content::kData,
                        "Invalid data file to delete: {} has delete-file content",
                        data_file->file_path);
  ICEBERG_BUILDER_RETURN_IF_ERROR(MergingSnapshotUpdate::DeleteDataFile(data_file));
  replaced_data_files_.insert(std::make_shared<DataFile>(*data_file));
  return *this;
}

RewriteFiles& RewriteFiles::DeleteDeleteFile(
    const std::shared_ptr<DataFile>& delete_file) {
  ICEBERG_BUILDER_CHECK(delete_file != nullptr, "Invalid delete file: null");
  ICEBERG_BUILDER_CHECK(delete_file->content != DataFile::Content::kData,
                        "Invalid delete file to delete: {} has data-file content",
                        delete_file->file_path);
  ICEBERG_BUILDER_RETURN_IF_ERROR(MergingSnapshotUpdate::DeleteDeleteFile(delete_file));
  return *this;
}

RewriteFiles& RewriteFiles::AddDataFile(const std::shared_ptr<DataFile>& file) {
  ICEBERG_BUILDER_CHECK(file != nullptr, "Invalid data file: null");
  ICEBERG_BUILDER_CHECK(file->content == DataFile::Content::kData,
                        "Invalid data file to add: {} has delete-file content",
                        file->file_path);
  ICEBERG_BUILDER_RETURN_IF_ERROR(MergingSnapshotUpdate::AddDataFile(file));
  return *this;
}

RewriteFiles& RewriteFiles::AddDeleteFile(const std::shared_ptr<DataFile>& delete_file) {
  ICEBERG_BUILDER_CHECK(delete_file != nullptr, "Invalid delete file: null");
  ICEBERG_BUILDER_CHECK(delete_file->content != DataFile::Content::kData,
                        "Invalid delete file to add: {} has data-file content",
                        delete_file->file_path);
  ICEBERG_BUILDER_RETURN_IF_ERROR(MergingSnapshotUpdate::AddDeleteFile(delete_file));
  return *this;
}

RewriteFiles& RewriteFiles::AddDeleteFile(const std::shared_ptr<DataFile>& delete_file,
                                          int64_t data_sequence_number) {
  ICEBERG_BUILDER_CHECK(delete_file != nullptr, "Invalid delete file: null");
  ICEBERG_BUILDER_CHECK(delete_file->content != DataFile::Content::kData,
                        "Invalid delete file to add: {} has data-file content",
                        delete_file->file_path);
  ICEBERG_BUILDER_RETURN_IF_ERROR(
      MergingSnapshotUpdate::AddDeleteFile(delete_file, data_sequence_number));
  return *this;
}

RewriteFiles& RewriteFiles::SetDataSequenceNumber(int64_t sequence_number) {
  SetNewDataFilesDataSequenceNumber(sequence_number);
  return *this;
}

RewriteFiles& RewriteFiles::RewriteDataFiles(
    const std::vector<std::shared_ptr<DataFile>>& files_to_delete,
    const std::vector<std::shared_ptr<DataFile>>& files_to_add, int64_t sequence_number) {
  SetNewDataFilesDataSequenceNumber(sequence_number);
  Rewrite(files_to_delete, {}, files_to_add, {});
  return *this;
}

RewriteFiles& RewriteFiles::Rewrite(
    const std::vector<std::shared_ptr<DataFile>>& data_files_to_replace,
    const std::vector<std::shared_ptr<DataFile>>& delete_files_to_replace,
    const std::vector<std::shared_ptr<DataFile>>& data_files_to_add,
    const std::vector<std::shared_ptr<DataFile>>& delete_files_to_add) {
  for (const auto& data_file : data_files_to_replace) {
    DeleteDataFile(data_file);
  }

  for (const auto& delete_file : delete_files_to_replace) {
    DeleteDeleteFile(delete_file);
  }

  for (const auto& data_file : data_files_to_add) {
    AddDataFile(data_file);
  }

  for (const auto& delete_file : delete_files_to_add) {
    AddDeleteFile(delete_file);
  }

  return *this;
}

RewriteFiles& RewriteFiles::ValidateFromSnapshot(int64_t snapshot_id) {
  starting_snapshot_id_ = snapshot_id;
  return *this;
}

std::string RewriteFiles::operation() { return DataOperation::kReplace; }

void RewriteFiles::ValidateReplacedAndAddedFiles() {
  // 1. Files to delete cannot be empty
  if (!DeletesDataFiles() && !DeletesDeleteFiles()) {
    AddError(ErrorKind::kValidationFailed, "Files to delete cannot be empty");
    return;
  }

  // 2. Data files to add must be empty because there's no data file to be rewritten
  if (!DeletesDataFiles() && AddsDataFiles()) {
    AddError(ErrorKind::kValidationFailed,
             "Data files to add must be empty because there's no data file to be "
             "rewritten");
    return;
  }

  // 3. Delete files to add must be empty because there's no delete file to be rewritten
  if (!DeletesDeleteFiles() && AddsDeleteFiles()) {
    AddError(ErrorKind::kValidationFailed,
             "Delete files to add must be empty because there's no delete file to be "
             "rewritten");
    return;
  }
}

Status RewriteFiles::Validate(const TableMetadata& current_metadata,
                              const std::shared_ptr<Snapshot>& snapshot) {
  ValidateReplacedAndAddedFiles();
  ICEBERG_RETURN_UNEXPECTED(CheckErrors());

  if (!replaced_data_files_.empty()) {
    // If there are replaced data files, there cannot be any new row-level deletes
    // for those data files.
    auto io = ctx_->table->io();
    ICEBERG_RETURN_UNEXPECTED(MergingSnapshotUpdate::ValidateNoNewDeletesForDataFiles(
        current_metadata, starting_snapshot_id_, replaced_data_files_, snapshot,
        std::move(io), HasDataSequenceNumber()));
  }

  return {};
}

}  // namespace iceberg
