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

#include "iceberg/expression/binder.h"
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
  ICEBERG_BUILDER_RETURN_IF_ERROR(AddDataFile(file));
  return *this;
}

OverwriteFiles& OverwriteFiles::DeleteFile(const std::shared_ptr<DataFile>& file) {
  ICEBERG_BUILDER_CHECK(file != nullptr, "Invalid data file: null");
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

OverwriteFiles& OverwriteFiles::WithCaseSensitivity(bool case_sensitive) {
  CaseSensitive(case_sensitive);
  return *this;
}

OverwriteFiles& OverwriteFiles::ValidateNoConflictingData() {
  validate_no_conflicting_data_ = true;
  FailMissingDeletePaths();
  return *this;
}

OverwriteFiles& OverwriteFiles::ValidateNoConflictingDeletes() {
  validate_no_conflicting_deletes_ = true;
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

// Pure row-filter overwrites use the row filter as the data conflict filter. Explicit
// file replacement is more conservative unless the caller supplies a narrower filter.
std::shared_ptr<Expression> OverwriteFiles::DataConflictDetectionFilter() const {
  if (conflict_detection_filter_ != nullptr) {
    return conflict_detection_filter_;
  }
  if (RowFilter() != nullptr && RowFilter() != Expressions::AlwaysFalse() &&
      deleted_data_files_.empty()) {
    return RowFilter();
  }
  return Expressions::AlwaysTrue();
}

Status OverwriteFiles::Validate(const TableMetadata& current_metadata,
                                const std::shared_ptr<Snapshot>& snapshot) {
  if (validate_added_files_match_overwrite_filter_) {
    if (RowFilter() == nullptr || RowFilter() == Expressions::AlwaysFalse()) {
      return ValidationFailed(
          "Cannot validate added files match overwrite filter: row filter is not set");
    }
    ICEBERG_RETURN_UNEXPECTED(
        ValidateAddedFilesMatchOverwriteFilterImpl(current_metadata));
  }

  if (validate_no_conflicting_data_) {
    ICEBERG_RETURN_UNEXPECTED(ValidateAddedDataFiles(
        current_metadata, starting_snapshot_id_, DataConflictDetectionFilter(), snapshot,
        ctx_->table->io(), IsCaseSensitive()));
  }

  if (validate_no_conflicting_deletes_) {
    if (RowFilter() != nullptr && RowFilter() != Expressions::AlwaysFalse()) {
      auto delete_filter = conflict_detection_filter_ != nullptr
                               ? conflict_detection_filter_
                               : RowFilter();
      ICEBERG_RETURN_UNEXPECTED(
          ValidateNoNewDeleteFiles(current_metadata, starting_snapshot_id_, delete_filter,
                                   snapshot, ctx_->table->io(), IsCaseSensitive()));
      ICEBERG_RETURN_UNEXPECTED(
          ValidateDeletedDataFiles(current_metadata, starting_snapshot_id_, delete_filter,
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

// Every added data file must be fully contained in the overwrite range defined by
// RowFilter(). Partition projection handles whole-partition proofs; strict metrics are
// used only when the partition alone is not enough.
Status OverwriteFiles::ValidateAddedFilesMatchOverwriteFilterImpl(
    const TableMetadata& metadata) {
  ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());

  ICEBERG_ASSIGN_OR_RAISE(auto spec, DataSpec());

  // Project requires a bound expression; StrictMetricsEvaluator binds internally.
  ICEBERG_ASSIGN_OR_RAISE(auto bound_filter,
                          Binder::Bind(*schema, RowFilter(), IsCaseSensitive()));
  ICEBERG_ASSIGN_OR_RAISE(
      auto strict_metrics_evaluator,
      StrictMetricsEvaluator::Make(RowFilter(), schema, IsCaseSensitive()));

  ICEBERG_ASSIGN_OR_RAISE(auto partition_type, spec->PartitionType(*schema));
  auto partition_fields = partition_type->fields();
  Schema partition_schema(
      std::vector<SchemaField>(partition_fields.begin(), partition_fields.end()));

  auto inclusive_projection = Projections::Inclusive(*spec, *schema, IsCaseSensitive());
  ICEBERG_ASSIGN_OR_RAISE(auto inclusive_expr,
                          inclusive_projection->Project(bound_filter));
  ICEBERG_ASSIGN_OR_RAISE(
      auto inclusive_evaluator,
      Evaluator::Make(partition_schema, inclusive_expr, IsCaseSensitive()));

  auto strict_projection = Projections::Strict(*spec, *schema, IsCaseSensitive());
  ICEBERG_ASSIGN_OR_RAISE(auto strict_expr, strict_projection->Project(bound_filter));
  ICEBERG_ASSIGN_OR_RAISE(
      auto strict_evaluator,
      Evaluator::Make(partition_schema, strict_expr, IsCaseSensitive()));

  for (const auto& file : AddedDataFiles()) {
    if (file == nullptr) {
      return ValidationFailed(
          "Cannot validate added files match overwrite filter: null data file");
    }

    ICEBERG_ASSIGN_OR_RAISE(bool inclusive_match,
                            inclusive_evaluator->Evaluate(file->partition));
    if (!inclusive_match) {
      return ValidationFailed(
          "Cannot commit file {}: added file does not match overwrite filter (outside "
          "the overwrite range)",
          file->file_path);
    }

    ICEBERG_ASSIGN_OR_RAISE(bool strict_match,
                            strict_evaluator->Evaluate(file->partition));
    if (strict_match) {
      continue;
    }

    ICEBERG_ASSIGN_OR_RAISE(bool metrics_match,
                            strict_metrics_evaluator->Evaluate(*file));
    if (!metrics_match) {
      return ValidationFailed(
          "Cannot commit file {}: added file is not fully contained in the overwrite "
          "filter",
          file->file_path);
    }
  }

  return {};
}

}  // namespace iceberg
