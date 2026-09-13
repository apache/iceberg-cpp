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

/// \file iceberg/update/replace_partitions.h

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/update/merging_snapshot_update.h"
#include "iceberg/util/partition_value_util.h"

namespace iceberg {

/// \brief API for overwriting files in a table by partition.
///
/// \note This is provided to implement SQL compatible with Hive table
/// operations but is not recommended. Instead, use OverwriteFiles to
/// explicitly overwrite data.
///
/// The default validation mode is idempotent, meaning the overwrite is correct
/// and should be committed regardless of other concurrent changes to the table.
/// Alternatively, call ValidateNoConflictingDeletes() and
/// ValidateNoConflictingData() to ensure that no conflicting delete files or
/// data files, respectively, have been written since the snapshot passed to
/// ValidateFromSnapshot().
///
/// This API accumulates file additions and produces a new Snapshot by replacing
/// all files in partitions containing new data with the new additions. This
/// operation implements dynamic partition replacement.
///
/// When committing, these changes are applied to the latest table snapshot.
/// Commit conflicts are resolved by applying the changes to the new latest
/// snapshot and reattempting the commit.
class ICEBERG_EXPORT ReplacePartitions : public MergingSnapshotUpdate {
 public:
  /// \brief Create a new ReplacePartitions instance.
  ///
  /// \param table_name The name of the table
  /// \param ctx The transaction context
  /// \return A Result containing the ReplacePartitions instance or an error
  static Result<std::shared_ptr<ReplacePartitions>> Make(
      std::string table_name, std::shared_ptr<TransactionContext> ctx);

  /// \brief Add a data file to the table.
  ///
  /// \param file A data file with partition_spec_id set
  /// \return Reference to this for method chaining
  ReplacePartitions& AddFile(const std::shared_ptr<DataFile>& file);

  /// \brief Validate that no partitions will be replaced and the operation is
  /// append-only.
  ///
  /// \return Reference to this for method chaining
  ReplacePartitions& ValidateAppendOnly();

  /// \brief Set the snapshot ID used in validations for this operation.
  ///
  /// All validations check changes after this snapshot ID. If this is not
  /// called, validation occurs from the beginning of the table's history.
  ///
  /// This method should be called before this operation is committed. If a
  /// concurrent operation committed a data or delete file or removed a data
  /// file after the given snapshot ID that might contain rows matching a
  /// partition marked for deletion, validation detects the conflict and fails.
  ///
  /// \param snapshot_id A snapshot ID set to when this operation started to
  /// read the table
  /// \return Reference to this for method chaining
  ReplacePartitions& ValidateFromSnapshot(int64_t snapshot_id);

  /// \brief Enable validation that data added concurrently does not conflict
  /// with this operation.
  ///
  /// Validating concurrent data files is required during non-idempotent replace
  /// partition operations. This checks whether a concurrent operation inserted
  /// data in any partition being overwritten and aborts the replace to avoid
  /// removing rows added concurrently.
  ///
  /// \return Reference to this for method chaining
  ReplacePartitions& ValidateNoConflictingData();

  /// \brief Enable validation that deletes that happened concurrently do not
  /// conflict with this operation.
  ///
  /// Validating concurrent deletes is required during non-idempotent replace
  /// partition operations. This checks whether a concurrent operation deleted
  /// data in any partition being overwritten and aborts the replace to avoid
  /// undeleting rows removed concurrently.
  ///
  /// \return Reference to this for method chaining
  ReplacePartitions& ValidateNoConflictingDeletes();

  std::string operation() override;

 protected:
  Status Validate(const TableMetadata& current_metadata,
                  const std::shared_ptr<Snapshot>& snapshot) override;

  /// \brief Apply changes, translating the fail-any-delete error.
  ///
  /// Rewrites the delete error raised when ValidateAppendOnly() is set into the
  /// partition-conflict message.
  Result<std::vector<ManifestFile>> Apply(
      const TableMetadata& current_metadata,
      const std::shared_ptr<Snapshot>& snapshot) override;

 private:
  explicit ReplacePartitions(std::string table_name,
                             std::shared_ptr<TransactionContext> ctx);

  std::optional<int64_t> starting_snapshot_id_;
  bool validate_conflicting_data_{false};
  bool validate_conflicting_deletes_{false};
  PartitionSet replaced_partitions_;
};

}  // namespace iceberg
