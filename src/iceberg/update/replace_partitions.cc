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

#include <string_view>

#include "iceberg/expression/expressions.h"
#include "iceberg/partition_spec.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"  // IWYU pragma: keep
#include "iceberg/table_metadata.h"
#include "iceberg/transaction.h"
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
  Set(SnapshotSummaryFields::kReplacePartitions, "true");
}

ReplacePartitions& ReplacePartitions::AddFile(const std::shared_ptr<DataFile>& file) {
  ICEBERG_BUILDER_CHECK(file != nullptr, "Invalid data file: null");
  ICEBERG_BUILDER_CHECK(file->partition_spec_id.has_value(),
                        "Data file must have partition spec ID");

  int32_t spec_id = file->partition_spec_id.value();
  ICEBERG_BUILDER_RETURN_IF_ERROR(DropPartition(spec_id, file->partition));
  replaced_partitions_.add(spec_id, file->partition);
  ICEBERG_BUILDER_RETURN_IF_ERROR(AddDataFile(file));
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
  ICEBERG_ASSIGN_OR_RAISE(auto data_spec, DataSpec());
  if (data_spec->IsUnpartitioned()) {
    ICEBERG_RETURN_UNEXPECTED(DeleteByRowFilter(Expressions::AlwaysTrue()));
  }

  auto result = MergingSnapshotUpdate::Apply(metadata_to_update, snapshot);
  if (result.has_value()) {
    return result;
  }
  // Translate append-only conflicts to the ReplacePartitions error.
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
  ICEBERG_ASSIGN_OR_RAISE(auto data_spec, DataSpec());

  if (snapshot == nullptr) {
    return {};
  }

  auto io = ctx_->table->io();
  if (validate_conflicting_data_) {
    if (data_spec->IsUnpartitioned()) {
      ICEBERG_RETURN_UNEXPECTED(ValidateAddedDataFiles(
          current_metadata, starting_snapshot_id_, Expressions::AlwaysTrue(), snapshot,
          io, IsCaseSensitive()));
    } else {
      ICEBERG_RETURN_UNEXPECTED(ValidateAddedDataFiles(
          current_metadata, starting_snapshot_id_, replaced_partitions_, snapshot, io));
    }
  }
  if (validate_conflicting_deletes_) {
    // Check deleted data files before newly added delete files.
    if (data_spec->IsUnpartitioned()) {
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
