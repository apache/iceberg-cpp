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

#include "iceberg/update/replace_partitions.h"

#include <algorithm>
#include <string_view>

#include "iceberg/expression/expressions.h"
#include "iceberg/partition_spec.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"  // IWYU pragma: keep
#include "iceberg/table_metadata.h"
#include "iceberg/transaction.h"
#include "iceberg/transform.h"
#include "iceberg/util/error_collector.h"
#include "iceberg/util/macros.h"

namespace iceberg {

Result<std::unique_ptr<ReplacePartitions>> ReplacePartitions::Make(
    std::string table_name, std::shared_ptr<TransactionContext> ctx) {
  ICEBERG_PRECHECK(!table_name.empty(), "Table name cannot be empty");
  ICEBERG_PRECHECK(ctx != nullptr, "Cannot create ReplacePartitions without a context");
  return std::unique_ptr<ReplacePartitions>(
      new ReplacePartitions(std::move(table_name), std::move(ctx)));
}

ReplacePartitions::ReplacePartitions(std::string table_name,
                                     std::shared_ptr<TransactionContext> ctx)
    : MergingSnapshotUpdate(std::move(table_name), std::move(ctx)) {
  SetSummaryProperty(SnapshotSummaryFields::kReplacePartitions, "true");
}

ReplacePartitions& ReplacePartitions::AddFile(const std::shared_ptr<DataFile>& file) {
  ICEBERG_BUILDER_CHECK(file != nullptr, "Invalid data file: null");
  ICEBERG_BUILDER_CHECK(file->partition_spec_id.has_value(),
                        "Data file must have partition spec ID");

  int32_t spec_id = file->partition_spec_id.value();
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto spec, base().PartitionSpecById(spec_id));

  ICEBERG_BUILDER_RETURN_IF_ERROR(AddDataFile(file));
  // Specs with no non-void transforms are unpartitioned.
  auto is_unpartitioned = [](const PartitionSpec& s) {
    return s.fields().empty() ||
           std::all_of(s.fields().begin(), s.fields().end(), [](const PartitionField& f) {
             return f.transform()->transform_type() == TransformType::kVoid;
           });
  };
  if (is_unpartitioned(*spec)) {
    // Unpartitioned: replace the whole table and validate conflicts against an
    // AlwaysTrue row filter instead of a spec-scoped partition set.
    ICEBERG_BUILDER_RETURN_IF_ERROR(DeleteByRowFilter(Expressions::AlwaysTrue()));
    replace_by_row_filter_ = true;
  } else {
    ICEBERG_BUILDER_RETURN_IF_ERROR(DropPartition(spec_id, file->partition));
    replaced_partitions_.add(spec_id, file->partition);
  }
  return *this;
}

ReplacePartitions& ReplacePartitions::ValidateAppendOnly() {
  FailAnyDelete();
  return *this;
}

ReplacePartitions& ReplacePartitions::ValidateFromSnapshot(int64_t snapshot_id) {
  starting_snapshot_id_ = snapshot_id;
  return *this;
}

ReplacePartitions& ReplacePartitions::ValidateNoConflictingData() {
  validate_conflicting_data_ = true;
  return *this;
}

ReplacePartitions& ReplacePartitions::ValidateNoConflictingDeletes() {
  validate_conflicting_deletes_ = true;
  return *this;
}

std::string ReplacePartitions::operation() { return DataOperation::kOverwrite; }

Result<std::vector<ManifestFile>> ReplacePartitions::Apply(
    const TableMetadata& metadata_to_update, const std::shared_ptr<Snapshot>& snapshot) {
  auto result = MergingSnapshotUpdate::Apply(metadata_to_update, snapshot);
  if (result.has_value()) {
    return result;
  }
  // Translate the fail-any-delete error raised when ValidateAppendOnly() is set,
  // matching Java BaseReplacePartitions.apply(). The base reports "Operation
  // would delete existing data: <partition>"; surface the partition-conflict
  // wording Java callers expect.
  constexpr std::string_view kFailAnyDeletePrefix =
      "Operation would delete existing data: ";
  const Error& error = result.error();
  if (error.kind == ErrorKind::kValidationFailed &&
      error.message.starts_with(kFailAnyDeletePrefix)) {
    std::string_view partition_path{error.message};
    partition_path.remove_prefix(kFailAnyDeletePrefix.size());
    return ValidationFailed(
        "Cannot commit file that conflicts with existing partition: {}", partition_path);
  }
  return result;
}

Status ReplacePartitions::Validate(const TableMetadata& current_metadata,
                                   const std::shared_ptr<Snapshot>& snapshot) {
  // Require at least one staged data file, all sharing exactly one partition
  // spec. DataSpec() enforces both invariants. Call it unconditionally, even on
  // the row-filter path: if an unpartitioned/all-void file is staged first and a
  // later file comes from a different spec, this rejects the mixed-spec replace
  // instead of committing a table-wide replace. The returned spec is unused
  // because replace_by_row_filter_ / replaced_partitions_ already scope
  // validation.
  ICEBERG_RETURN_UNEXPECTED(DataSpec());

  if (snapshot == nullptr) {
    return {};
  }

  auto io = ctx_->table->io();
  if (validate_conflicting_data_) {
    if (replace_by_row_filter_) {
      ICEBERG_RETURN_UNEXPECTED(ValidateAddedDataFiles(
          current_metadata, starting_snapshot_id_, Expressions::AlwaysTrue(), snapshot,
          io, IsCaseSensitive()));
    } else {
      ICEBERG_RETURN_UNEXPECTED(ValidateAddedDataFiles(
          current_metadata, starting_snapshot_id_, replaced_partitions_, snapshot, io));
    }
  }
  if (validate_conflicting_deletes_) {
    // Run ValidateDeletedDataFiles before ValidateNoNewDeleteFiles, matching
    // Java, so the reported failure is deterministic when both conflict types
    // are present. The first rejects concurrent removals of data in the
    // replaced partitions (which would otherwise resurrect deleted rows); the
    // second rejects delete files added concurrently in those partitions.
    if (replace_by_row_filter_) {
      ICEBERG_RETURN_UNEXPECTED(ValidateDeletedDataFiles(
          current_metadata, starting_snapshot_id_, Expressions::AlwaysTrue(), snapshot,
          io, IsCaseSensitive()));
      ICEBERG_RETURN_UNEXPECTED(ValidateNoNewDeleteFiles(
          current_metadata, starting_snapshot_id_, Expressions::AlwaysTrue(), snapshot,
          io, IsCaseSensitive()));
    } else {
      ICEBERG_RETURN_UNEXPECTED(ValidateDeletedDataFiles(
          current_metadata, starting_snapshot_id_, replaced_partitions_, snapshot, io));
      ICEBERG_RETURN_UNEXPECTED(ValidateNoNewDeleteFiles(
          current_metadata, starting_snapshot_id_, replaced_partitions_, snapshot, io));
    }
  }
  return {};
}

}  // namespace iceberg
