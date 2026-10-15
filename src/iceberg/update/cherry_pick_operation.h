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

/// \brief Cherry-picks or fast-forwards the current state to a snapshot.
///
/// This update is not exposed through the Table API. It is part of the
/// Transaction API intended for use in SnapshotManager.
class ICEBERG_EXPORT CherryPickOperation : public MergingSnapshotUpdate {
 public:
  /// \brief Create a new CherryPickOperation instance.
  ///
  /// \param table_name The name of the table
  /// \param ctx The transaction context
  /// \return A new CherryPickOperation instance
  static Result<std::unique_ptr<CherryPickOperation>> Make(
      std::string table_name, std::shared_ptr<TransactionContext> ctx);

  /// \brief Cherry-pick the changes of the given snapshot, or fast-forward to it.
  ///
  /// \param snapshot_id The ID of the snapshot whose changes to apply
  /// \return Reference to this for method chaining
  CherryPickOperation& Cherrypick(int64_t snapshot_id);

 protected:
  Result<ApplyResult> Apply() override;

  std::string operation() override;

  Status Validate(const TableMetadata& base,
                  const std::shared_ptr<Snapshot>& snapshot) override;

 private:
  explicit CherryPickOperation(std::string table_name,
                               std::shared_ptr<TransactionContext> ctx);

  /// \brief Whether the snapshot passed to Cherrypick() can be fast-forwarded.
  bool IsFastForward(const TableMetadata& base) const;

  /// \brief Fail if the picked snapshot is already part of the current history,
  /// either directly or as the source of an earlier pick.
  Status ValidateNonAncestor(const TableMetadata& meta, int64_t snapshot_id) const;

  /// \brief If there is a current snapshot, require an ancestral parent (if any)
  /// and reject files added to replaced partitions since that parent.
  Status ValidateReplacedPartitions(const TableMetadata& meta) const;

  std::shared_ptr<Snapshot> cherrypick_snapshot_;
  bool require_fast_forward_ = false;
  std::optional<PartitionSet> replaced_partitions_;
};

}  // namespace iceberg
