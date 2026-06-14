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
  // Forward to the inherited staging path. Any error (e.g. missing partition spec
  // ID) is captured by the ErrorCollector and surfaced at Commit() rather than
  // dropped. RowFilter() and deleted_data_files_ are intentionally left unchanged.
  ICEBERG_BUILDER_RETURN_IF_ERROR(AddDataFile(file));
  return *this;
}

OverwriteFiles& OverwriteFiles::DeleteFile(const std::shared_ptr<DataFile>& file) {
  ICEBERG_BUILDER_CHECK(file != nullptr, "Invalid data file: null");
  // Dual-track: record the file for delete-conflict validation AND register it for
  // removal via the inherited pipeline.
  deleted_data_files_.insert(file);
  ICEBERG_BUILDER_RETURN_IF_ERROR(DeleteDataFile(file));
  return *this;
}

OverwriteFiles& OverwriteFiles::DeleteFiles(const DataFileSet& data_files_to_delete,
                                            const DeleteFileSet& delete_files_to_delete) {
  // Bulk equivalent of repeated DeleteFile(...) plus explicit delete-file removal. Empty
  // sets are no-ops; the set types handle deduplication of repeated entries.
  for (const auto& file : data_files_to_delete) {
    ICEBERG_BUILDER_CHECK(file != nullptr, "Invalid data file: null");
    // Dual-track: record for delete-conflict validation AND register for removal.
    deleted_data_files_.insert(file);
    ICEBERG_BUILDER_RETURN_IF_ERROR(DeleteDataFile(file));
  }
  for (const auto& file : delete_files_to_delete) {
    ICEBERG_BUILDER_CHECK(file != nullptr, "Invalid delete file: null");
    ICEBERG_BUILDER_RETURN_IF_ERROR(DeleteDeleteFile(file));
  }
  return *this;
}

OverwriteFiles& OverwriteFiles::OverwriteByRowFilter(std::shared_ptr<Expression> expr) {
  ICEBERG_BUILDER_CHECK(expr != nullptr, "Invalid row filter expression: null");
  // Forward to the inherited filter path: this sets RowFilter() and forwards the
  // expression to both the data and delete filter managers. Any error is captured by the
  // ErrorCollector and surfaced at Commit().
  ICEBERG_BUILDER_RETURN_IF_ERROR(DeleteByRowFilter(std::move(expr)));
  return *this;
}

OverwriteFiles& OverwriteFiles::ValidateFromSnapshot(int64_t snapshot_id) {
  // Snapshot ids are non-negative: SnapshotUtil::GenerateSnapshotId() masks with
  // int64_t::max() and so yields a value in [0, INT64_MAX], while kInvalidSnapshotId is
  // -1. A negative id therefore can never identify a real snapshot and is rejected via
  // the deferred ErrorCollector (surfaced at Commit()); 0 is accepted because it is in
  // the generator's range. A negative id is always a misuse, so we keep an early,
  // clearer guard against the obviously-invalid sentinel/negative values.
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
  // Forward to the protected MergingSnapshotUpdate::CaseSensitive(bool) setter (which
  // returns void); the public name differs to avoid the public/protected name clash and
  // keep a fluent return type.
  CaseSensitive(case_sensitive);
  return *this;
}

OverwriteFiles& OverwriteFiles::ValidateNoConflictingData() {
  // Enable concurrent-data validation and require every explicitly-deleted old file to
  // be hit during manifest filtering.
  validate_no_conflicting_data_ = true;
  FailMissingDeletePaths();
  return *this;
}

OverwriteFiles& OverwriteFiles::ValidateNoConflictingDeletes() {
  // Enable concurrent-delete validation and require every explicitly-deleted old file to
  // be hit during manifest filtering.
  validate_no_conflicting_deletes_ = true;
  FailMissingDeletePaths();
  return *this;
}

OverwriteFiles& OverwriteFiles::ValidateAddedFilesMatchOverwriteFilter() {
  // Enable strict validation that every added data file is fully contained in the
  // overwrite range. The precondition (a row filter must be set) is enforced in
  // Validate(...).
  validate_added_files_match_overwrite_filter_ = true;
  return *this;
}

// Classify the snapshot operation from the current builder content. DeletesDataFiles()
// is true for both an explicit DeleteFile(...) and a pure OverwriteByRowFilter(...) (the
// latter via ManifestFilterManager::ContainsDeletes()), so OverwriteByRowFilter + AddFile
// correctly classifies as overwrite without any RowFilter()-based fallback.
std::string OverwriteFiles::operation() {
  if (DeletesDataFiles() && !AddsDataFiles()) {
    return DataOperation::kDelete;
  }
  if (AddsDataFiles() && !DeletesDataFiles()) {
    return DataOperation::kAppend;
  }
  return DataOperation::kOverwrite;
}

// Select the conflict-detection filter. Precedence: (1) the explicitly-set
// conflict_detection_filter_; otherwise
// (2) the row filter when it is set and not AlwaysFalse and no explicit data files were
// registered for deletion (the pure overwriteByRowFilter case, where the row filter
// already describes the conflicting range); otherwise (3) the conservative AlwaysTrue,
// under which any newly-added data file counts as a conflict (file-replacement or mixed
// mode). RowFilter() defaults to the AlwaysFalse singleton when unset, so comparing
// against the Expressions::AlwaysFalse() singleton by pointer identity (consistent with
// how the filter managers track delete_expr_) correctly treats the unset case as "no
// row filter".
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

// Run the enabled overwrite-specific concurrency checks before commit. The branches are
// sequential and independent. The first failing check aborts the commit; on success we
// return OK and let the inherited Apply(...) build the snapshot.
Status OverwriteFiles::Validate(const TableMetadata& current_metadata,
                                const std::shared_ptr<Snapshot>& snapshot) {
  // 0. Strict added-file range precondition: when
  // ValidateAddedFilesMatchOverwriteFilter()
  //    is enabled, a row filter is required to define the overwrite range. RowFilter()
  //    defaults to the AlwaysFalse singleton when unset, so an unset or AlwaysFalse
  //    filter means there is no range to validate against and the commit must fail.
  if (validate_added_files_match_overwrite_filter_) {
    if (RowFilter() == nullptr || RowFilter() == Expressions::AlwaysFalse()) {
      return ValidationFailed(
          "Cannot validate added files match overwrite filter: row filter is not set");
    }
    ICEBERG_RETURN_UNEXPECTED(
        ValidateAddedFilesMatchOverwriteFilterImpl(current_metadata));
  }

  // 1. Concurrent newly-added data files: fail if any snapshot after the starting point
  //    added a data file matching the resolved conflict-detection filter.
  if (validate_no_conflicting_data_) {
    ICEBERG_RETURN_UNEXPECTED(ValidateAddedDataFiles(
        current_metadata, starting_snapshot_id_, DataConflictDetectionFilter(), snapshot,
        ctx_->table->io(), IsCaseSensitive()));
  }

  // 2. Concurrent deletes: the two sub-checks are independent and both may fire.
  if (validate_no_conflicting_deletes_) {
    // Path A: a row filter is in play (set and not the AlwaysFalse singleton). Fail if a
    // new delete file matches the range, or if a data file in the range was concurrently
    // removed. Prefer the explicit conflict_detection_filter_ when set, else the row
    // filter that describes the overwrite range.
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

    // Path B: explicit old data files were registered for replacement. Fail if a new
    // delete file matching the conflict-detection filter covers any of them. Use the
    // STATIC data_filter overload, passing the raw conflict_detection_filter_ (which may
    // be nullptr — a nullptr filter means no filter, so all delete files are considered).
    // The 3rd argument is an Expression, so this overload is unambiguous and needs no
    // member-function-pointer cast.
    if (!deleted_data_files_.empty()) {
      ICEBERG_RETURN_UNEXPECTED(ValidateNoNewDeletesForDataFiles(
          current_metadata, starting_snapshot_id_, conflict_detection_filter_,
          deleted_data_files_, snapshot, ctx_->table->io(), IsCaseSensitive()));
    }
  }

  return {};
}

// Every added data file must be fully contained in the overwrite range defined by
// RowFilter(). The validation resolves a single partition spec via DataSpec() and then,
// for each added file, an inclusive partition projection provides a fast negative (reject
// when the file's partition cannot possibly fall in range), a strict partition projection
// provides a fast positive (accept when the whole partition is guaranteed in range), and
// a file-level StrictMetricsEvaluator provides the fallback proof. Metrics that are
// missing or insufficient yield a non-matching result and therefore fail validation
// (conservative: never silently accept an unprovable file).
//
// DataSpec() returns InvalidArgument when zero data files were added (empty-added-files
// is rejected) and when more than one partition spec is represented among the added files
// (multi-spec is rejected). DataSpec() reads the spec from the producer's base metadata;
// the projections use the table schema from metadata.Schema().
//
// Implementation notes:
//   * `Evaluator::Make` takes a `Schema`, and the partition type is obtained via
//     `PartitionSpec::PartitionType(schema)` (which needs the table schema and returns a
//     `StructType`). We therefore wrap the partition `StructType`'s fields in a `Schema`
//     and bind the projected expression against it.
//   * `StrictMetricsEvaluator::Make` binds its expression internally, and `Binder::Bind`
//     rejects an already-bound predicate. So the bound filter is used only for the
//     projections (`ProjectionEvaluator::Project` requires a bound expression), while the
//     UNBOUND `RowFilter()` is handed to `StrictMetricsEvaluator::Make`.
Status OverwriteFiles::ValidateAddedFilesMatchOverwriteFilterImpl(
    const TableMetadata& metadata) {
  ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());

  // Single spec for all added files. This rejects both the empty-added-files case and
  // the multi-spec case via InvalidArgument.
  ICEBERG_ASSIGN_OR_RAISE(auto spec, DataSpec());

  // Bind the row filter once for projection (Project requires a bound expression); build
  // the strict metrics evaluator once over the UNBOUND row filter (it binds internally).
  ICEBERG_ASSIGN_OR_RAISE(auto bound_filter,
                          Binder::Bind(*schema, RowFilter(), IsCaseSensitive()));
  ICEBERG_ASSIGN_OR_RAISE(
      auto strict_metrics_evaluator,
      StrictMetricsEvaluator::Make(RowFilter(), schema, IsCaseSensitive()));

  // Build ONE inclusive and ONE strict partition-value evaluator from the single spec.
  // The Evaluators bind on construction and do not retain the partition schema, so the
  // local schema below may safely go out of scope.
  ICEBERG_ASSIGN_OR_RAISE(auto partition_type, spec->PartitionType(*schema));
  auto partition_fields = partition_type->fields();
  Schema partition_schema(
      std::vector<SchemaField>(partition_fields.begin(), partition_fields.end()));

  // Inclusive projection (fast negative): project the bound filter into the partition
  // space and build an evaluator over the spec's partition values.
  auto inclusive_projection = Projections::Inclusive(*spec, *schema, IsCaseSensitive());
  ICEBERG_ASSIGN_OR_RAISE(auto inclusive_expr,
                          inclusive_projection->Project(bound_filter));
  ICEBERG_ASSIGN_OR_RAISE(
      auto inclusive_evaluator,
      Evaluator::Make(partition_schema, inclusive_expr, IsCaseSensitive()));

  // Strict projection (fast positive).
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

    // Step 1: inclusive projection — fast negative. If the file's partition cannot match
    // the (inclusively projected) filter, the file is definitively outside the range.
    ICEBERG_ASSIGN_OR_RAISE(bool inclusive_match,
                            inclusive_evaluator->Evaluate(file->partition));
    if (!inclusive_match) {
      return ValidationFailed(
          "Cannot commit file {}: added file does not match overwrite filter (outside "
          "the overwrite range)",
          file->file_path);
    }

    // Step 2: strict projection — fast positive. If the whole partition is guaranteed in
    // range, the file is accepted without inspecting metrics.
    ICEBERG_ASSIGN_OR_RAISE(bool strict_match,
                            strict_evaluator->Evaluate(file->partition));
    if (strict_match) {
      continue;
    }

    // Step 3: file-level proof via strict metrics. A false result — including the case
    // where metrics are missing or insufficient — fails validation conservatively rather
    // than silently accepting a file that cannot be proved fully in range.
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
