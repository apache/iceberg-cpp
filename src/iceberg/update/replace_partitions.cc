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
  SetSummaryProperty(SnapshotSummaryFields::kReplacePartitions, "true");
}

ReplacePartitions& ReplacePartitions::AddFile(const std::shared_ptr<DataFile>& file) {
  ICEBERG_BUILDER_CHECK(file != nullptr, "Invalid data file: null");
  ICEBERG_BUILDER_CHECK(file->partition_spec_id.has_value(),
                        "Data file must have partition spec ID");

  int32_t spec_id = file->partition_spec_id.value();
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto spec, base().PartitionSpecById(spec_id));

  ICEBERG_BUILDER_RETURN_IF_ERROR(AddDataFile(file));
  // DropPartition(spec_id, partition) registers the (spec_id, partition_values)
  // tuple with both data and delete filter managers. For an unpartitioned spec
  // the partition values are empty and naturally match every file under that
  // spec — no separate AlwaysTrue path is needed, and validation stays scoped
  // to the spec rather than the whole table.
  ICEBERG_BUILDER_RETURN_IF_ERROR(DropPartition(spec_id, file->partition));
  replaced_partitions_.add(spec_id, file->partition);
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
  if (snapshot == nullptr) {
    return {};
  }
  // No-op update: no partitions were staged, so there is nothing to conflict
  // with. Calling the validators with AlwaysTrue here would turn an empty
  // builder into a full-table conflict check.
  if (replaced_partitions_.empty()) {
    return {};
  }

  auto io = ctx_->table->io();
  if (validate_conflicting_data_) {
    ICEBERG_RETURN_UNEXPECTED(ValidateAddedDataFiles(
        current_metadata, starting_snapshot_id_, replaced_partitions_, snapshot, io));
  }
  if (validate_conflicting_deletes_) {
    ICEBERG_RETURN_UNEXPECTED(ValidateNoNewDeleteFiles(
        current_metadata, starting_snapshot_id_, replaced_partitions_, snapshot, io));
  }
  return {};
}

}  // namespace iceberg
