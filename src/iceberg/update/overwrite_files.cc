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

#include "iceberg/update/overwrite_files.h"

#include <vector>

#include "iceberg/expression/evaluator.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/expression/projections.h"
#include "iceberg/expression/strict_metrics_evaluator.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/transaction.h"
#include "iceberg/type.h"
#include "iceberg/util/error_collector.h"
#include "iceberg/util/macros.h"

namespace iceberg {

Result<std::shared_ptr<OverwriteFiles>> OverwriteFiles::Make(
    std::string table_name, std::shared_ptr<TransactionContext> ctx) {
  ICEBERG_PRECHECK(!table_name.empty(), "Table name cannot be empty");
  ICEBERG_PRECHECK(ctx != nullptr, "Cannot create OverwriteFiles without a context");
  return std::shared_ptr<OverwriteFiles>(
      new OverwriteFiles(std::move(table_name), std::move(ctx)));
}

OverwriteFiles::OverwriteFiles(std::string table_name,
                               std::shared_ptr<TransactionContext> ctx)
    : MergingSnapshotUpdate(std::move(table_name), std::move(ctx)) {}

OverwriteFiles::~OverwriteFiles() = default;

OverwriteFiles& OverwriteFiles::AddFile(const std::shared_ptr<DataFile>& file) {
  ICEBERG_BUILDER_CHECK(file != nullptr, "Invalid data file: null");
  ICEBERG_BUILDER_CHECK(file->content == DataFile::Content::kData,
                        "Invalid data file to add: {} has delete-file content",
                        file->file_path);
  ICEBERG_BUILDER_RETURN_IF_ERROR(AddDataFile(file));
  return *this;
}

OverwriteFiles& OverwriteFiles::DeleteFile(const std::shared_ptr<DataFile>& file) {
  ICEBERG_BUILDER_CHECK(file != nullptr, "Invalid data file: null");
  ICEBERG_BUILDER_CHECK(file->content == DataFile::Content::kData,
                        "Invalid data file to delete: {} has delete-file content",
                        file->file_path);
  deleted_data_files_.insert(file);
  ICEBERG_BUILDER_RETURN_IF_ERROR(DeleteDataFile(file));
  return *this;
}

OverwriteFiles& OverwriteFiles::DeleteFiles(const DataFileSet& data_files_to_delete,
                                            const DeleteFileSet& delete_files_to_delete) {
  // Both sets use DataFile pointers, so validate content before forwarding to the
  // data-file and delete-file removal paths.
  for (const auto& file : data_files_to_delete) {
    ICEBERG_BUILDER_CHECK(file != nullptr, "Invalid data file: null");
    ICEBERG_BUILDER_CHECK(file->content == DataFile::Content::kData,
                          "Invalid data file to delete: {} has delete-file content",
                          file->file_path);
    deleted_data_files_.insert(file);
    ICEBERG_BUILDER_RETURN_IF_ERROR(DeleteDataFile(file));
  }
  for (const auto& file : delete_files_to_delete) {
    ICEBERG_BUILDER_CHECK(file != nullptr, "Invalid delete file: null");
    ICEBERG_BUILDER_CHECK(file->content != DataFile::Content::kData,
                          "Invalid delete file to delete: {} has data-file content",
                          file->file_path);
    ICEBERG_BUILDER_RETURN_IF_ERROR(DeleteDeleteFile(file));
  }
  return *this;
}

OverwriteFiles& OverwriteFiles::OverwriteByRowFilter(std::shared_ptr<Expression> expr) {
  ICEBERG_BUILDER_CHECK(expr != nullptr, "Invalid row filter expression: null");
  ICEBERG_BUILDER_RETURN_IF_ERROR(DeleteByRowFilter(std::move(expr)));
  return *this;
}

OverwriteFiles& OverwriteFiles::ValidateFromSnapshot(int64_t snapshot_id) {
  ICEBERG_BUILDER_CHECK(snapshot_id >= 0, "Invalid snapshot id: {}", snapshot_id);
  starting_snapshot_id_ = snapshot_id;
  return *this;
}

OverwriteFiles& OverwriteFiles::ConflictDetectionFilter(
    std::shared_ptr<Expression> expr) {
  ICEBERG_BUILDER_CHECK(expr != nullptr, "Invalid conflict detection filter: null");
  conflict_detection_filter_ = std::move(expr);
  return *this;
}

OverwriteFiles& OverwriteFiles::CaseSensitive(bool case_sensitive) {
  MergingSnapshotUpdate::CaseSensitive(case_sensitive);
  return *this;
}

OverwriteFiles& OverwriteFiles::ValidateNoConflictingData() {
  validate_new_data_files_ = true;
  FailMissingDeletePaths();
  return *this;
}

OverwriteFiles& OverwriteFiles::ValidateNoConflictingDeletes() {
  validate_new_deletes_ = true;
  FailMissingDeletePaths();
  return *this;
}

OverwriteFiles& OverwriteFiles::ValidateAddedFilesMatchOverwriteFilter() {
  validate_added_files_match_overwrite_filter_ = true;
  return *this;
}

std::string OverwriteFiles::operation() {
  if (DeletesDataFiles() && !AddsDataFiles()) {
    return DataOperation::kDelete;
  }
  if (AddsDataFiles() && !DeletesDataFiles()) {
    return DataOperation::kAppend;
  }
  return DataOperation::kOverwrite;
}

std::shared_ptr<Expression> OverwriteFiles::DataConflictDetectionFilter() const {
  if (conflict_detection_filter_ != nullptr) {
    return conflict_detection_filter_;
  }
  if (auto filter = RowFilter(); filter != nullptr &&
                                 filter != Expressions::AlwaysFalse() &&
                                 deleted_data_files_.empty()) {
    return filter;
  }
  return Expressions::AlwaysTrue();
}

Status OverwriteFiles::Validate(const TableMetadata& current_metadata,
                                const std::shared_ptr<Snapshot>& snapshot) {
  auto row_filter = RowFilter();

  if (validate_added_files_match_overwrite_filter_) {
    ICEBERG_ASSIGN_OR_RAISE(auto spec, DataSpec());
    ICEBERG_ASSIGN_OR_RAISE(auto schema, current_metadata.Schema());
    ICEBERG_ASSIGN_OR_RAISE(auto partition_type, spec->PartitionType(*schema));
    auto partition_schema = partition_type->ToSchema();

    ICEBERG_ASSIGN_OR_RAISE(
        auto inclusive_expr,
        Projections::Inclusive(*spec, *schema, IsCaseSensitive())->Project(row_filter));
    ICEBERG_ASSIGN_OR_RAISE(
        auto inclusive_evaluator,
        Evaluator::Make(*partition_schema, inclusive_expr, IsCaseSensitive()));

    ICEBERG_ASSIGN_OR_RAISE(
        auto strict_expr,
        Projections::Strict(*spec, *schema, IsCaseSensitive())->Project(row_filter));
    ICEBERG_ASSIGN_OR_RAISE(
        auto strict_evaluator,
        Evaluator::Make(*partition_schema, strict_expr, IsCaseSensitive()));

    ICEBERG_ASSIGN_OR_RAISE(
        auto metrics_evaluator,
        StrictMetricsEvaluator::Make(row_filter, schema, IsCaseSensitive()));

    // the real test is that the strict or metrics test matches the file, indicating that
    // all records in the file match the filter. inclusive is used to avoid testing the
    // metrics, which is more complicated
    const auto file_test = [&](const DataFile& file) -> Result<bool> {
      ICEBERG_ASSIGN_OR_RAISE(bool inclusive_match,
                              inclusive_evaluator->Evaluate(file.partition));
      if (!inclusive_match) {
        return false;
      }
      ICEBERG_ASSIGN_OR_RAISE(bool strict_match,
                              strict_evaluator->Evaluate(file.partition));
      if (strict_match) {
        return true;
      }
      return metrics_evaluator->Evaluate(file);
    };

    for (const auto& file : AddedDataFiles()) {
      ICEBERG_ASSIGN_OR_RAISE(bool matches_filter, file_test(*file));
      if (!matches_filter) {
        return ValidationFailed(
            "Cannot append file with rows that do not match filter: {}: {}",
            row_filter->ToString(), file->file_path);
      }
    }
  }

  if (validate_new_data_files_) {
    ICEBERG_RETURN_UNEXPECTED(ValidateAddedDataFiles(
        current_metadata, starting_snapshot_id_, DataConflictDetectionFilter(), snapshot,
        ctx_->table->io(), IsCaseSensitive()));
  }

  if (validate_new_deletes_) {
    if (row_filter != nullptr && row_filter != Expressions::AlwaysFalse()) {
      auto filter =
          conflict_detection_filter_ != nullptr ? conflict_detection_filter_ : row_filter;
      ICEBERG_RETURN_UNEXPECTED(
          ValidateNoNewDeleteFiles(current_metadata, starting_snapshot_id_, filter,
                                   snapshot, ctx_->table->io(), IsCaseSensitive()));
      ICEBERG_RETURN_UNEXPECTED(
          ValidateDeletedDataFiles(current_metadata, starting_snapshot_id_, filter,
                                   snapshot, ctx_->table->io(), IsCaseSensitive()));
    }

    if (!deleted_data_files_.empty()) {
      ICEBERG_RETURN_UNEXPECTED(ValidateNoNewDeletesForDataFiles(
          current_metadata, starting_snapshot_id_, conflict_detection_filter_,
          deleted_data_files_, snapshot, ctx_->table->io(), IsCaseSensitive()));
    }
  }

  return {};
}

}  // namespace iceberg
