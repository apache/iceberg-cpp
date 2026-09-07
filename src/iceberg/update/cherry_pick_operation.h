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

/// \file iceberg/update/cherry_pick_operation.h

#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/update/merging_snapshot_update.h"
#include "iceberg/util/partition_value_util.h"

namespace iceberg {

/// \brief Cherry-picks the changes of a snapshot onto the current state.
///
/// This update is not exposed through the Table API. It is part of the
/// Transaction API intended for use in SnapshotManager.
///
/// Three kinds of snapshot can be picked. An append snapshot has its added
/// data files re-applied on top of the current state. An overwrite snapshot
/// carrying "replace-partitions"="true" has both its added and its removed
/// data files re-applied, and can only be picked while the partitions it
/// replaced are unchanged. Any other snapshot can only be fast-forwarded.
///
/// A fast-forward moves the current state to the picked snapshot without
/// producing a new one, which this operation cannot express because Apply()
/// always builds a new snapshot. Callers detect that case with
/// SnapshotUtil::CanFastForward() and set the current snapshot instead of
/// committing this operation; ValidateFastForward() applies the checks this
/// operation would otherwise have run.
///
/// The new snapshot records the picked snapshot in "source-snapshot-id". When
/// the picked snapshot carries a "wap.id", that id is recorded in
/// "published-wap-id" and the pick fails if the id was already published.
class ICEBERG_EXPORT CherryPickOperation : public MergingSnapshotUpdate {
 public:
  /// \brief Create a new CherryPickOperation instance.
  ///
  /// \param table_name The name of the table
  /// \param ctx The transaction context
  /// \return A new CherryPickOperation instance
  static Result<std::unique_ptr<CherryPickOperation>> Make(
      std::string table_name, std::shared_ptr<TransactionContext> ctx);

  /// \brief Apply the changes of the given snapshot to the current state.
  ///
  /// \param snapshot_id The ID of the snapshot whose changes to apply
  /// \return Reference to this for method chaining
  CherryPickOperation& Cherrypick(int64_t snapshot_id);

  /// \brief Run the checks that apply when the given snapshot is fast-forwarded
  /// to rather than picked.
  ///
  /// A fast-forward publishes the picked snapshot itself, so the WAP id staged
  /// on it must not already have been published. Mirrors Java, where this check
  /// runs in cherrypick() for append and dynamic overwrite snapshots, before
  /// apply() elects to fast-forward.
  ///
  /// \param metadata The table metadata to fast-forward
  /// \param snapshot The snapshot to fast-forward to
  static Status ValidateFastForward(const TableMetadata& metadata,
                                    const Snapshot& snapshot);

  std::string operation() override;

 protected:
  Status Validate(const TableMetadata& current_metadata,
                  const std::shared_ptr<Snapshot>& snapshot) override;

 private:
  explicit CherryPickOperation(std::string table_name,
                               std::shared_ptr<TransactionContext> ctx);

  /// \brief Whether the snapshot passed to Cherrypick() can be fast-forwarded.
  bool IsFastForward() const;

  /// \brief Fail if the picked snapshot is already part of the current history,
  /// either directly or as the source of an earlier pick.
  Status ValidateNonAncestor(const TableMetadata& metadata, int64_t snapshot_id) const;

  /// \brief Fail if any partition replaced by the picked snapshot received new
  /// files after the picked snapshot's parent.
  Status ValidateReplacedPartitions(const TableMetadata& metadata) const;

  std::shared_ptr<Snapshot> cherrypick_snapshot_;
  // Set only when the picked snapshot is a dynamic partition overwrite.
  std::optional<PartitionSet> replaced_partitions_;
};

}  // namespace iceberg
