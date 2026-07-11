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
  // A spec is effectively unpartitioned if it has no fields, or every field
  // uses the void transform. Java's PartitionSpec.isUnpartitioned() covers
  // both cases; mirror that so an all-void spec triggers the table-wide
  // replace path instead of a DropPartition with void-valued partition keys.
  auto is_unpartitioned = [](const PartitionSpec& s) {
    return s.fields().empty() ||
           std::all_of(s.fields().begin(), s.fields().end(), [](const PartitionField& f) {
             return f.transform()->transform_type() == TransformType::kVoid;
           });
  };
  if (is_unpartitioned(*spec)) {
    // Unpartitioned spec: Java's BaseReplacePartitions treats this as a
    // table-wide replace rather than a spec-scoped DropPartition with empty
    // partition values. Mirror that so every existing data file is dropped
    // and conflict validation runs against AlwaysTrue.
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

Status ReplacePartitions::Validate(const TableMetadata& current_metadata,
                                   const std::shared_ptr<Snapshot>& snapshot) {
  // Match Java BaseReplacePartitions.validate: require at least one staged data
  // file and that all staged files share exactly one partition spec.
  // DataSpec() enforces both invariants; ignore the returned spec here since
  // replace_by_row_filter_ / replaced_partitions_ already scope validation.
  if (!replace_by_row_filter_) {
    ICEBERG_RETURN_UNEXPECTED(DataSpec());
  }

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
    // Java's BaseReplacePartitions.validate gates both ValidateNoNewDeleteFiles
    // and ValidateDeletedDataFiles on the same validateNewDeletes flag. The
    // second check rejects concurrent overwrite/delete commits in the replaced
    // partitions; without it a concurrent delete in a replaced partition would
    // commit silently.
    if (replace_by_row_filter_) {
      ICEBERG_RETURN_UNEXPECTED(ValidateNoNewDeleteFiles(
          current_metadata, starting_snapshot_id_, Expressions::AlwaysTrue(), snapshot,
          io, IsCaseSensitive()));
      ICEBERG_RETURN_UNEXPECTED(ValidateDeletedDataFiles(
          current_metadata, starting_snapshot_id_, Expressions::AlwaysTrue(), snapshot,
          io, IsCaseSensitive()));
    } else {
      ICEBERG_RETURN_UNEXPECTED(ValidateNoNewDeleteFiles(
          current_metadata, starting_snapshot_id_, replaced_partitions_, snapshot, io));
      ICEBERG_RETURN_UNEXPECTED(ValidateDeletedDataFiles(
          current_metadata, starting_snapshot_id_, replaced_partitions_, snapshot, io));
    }
  }
  return {};
}

}  // namespace iceberg
