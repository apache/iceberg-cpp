
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

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Whether a transaction creates a new table or updates an existing one.
enum class TransactionKind : uint8_t { kCreate, kUpdate };

/// \brief A transaction for performing multiple updates to a table
class ICEBERG_EXPORT Transaction : public std::enable_shared_from_this<Transaction> {
 public:
  ~Transaction();

  /// \brief Create a new transaction
  static Result<std::shared_ptr<Transaction>> Make(std::shared_ptr<Table> table,
                                                   TransactionKind kind);

  /// \brief Create a transaction from an existing context (used by PendingUpdate::Commit)
  static Result<std::shared_ptr<Transaction>> Make(
      std::shared_ptr<TransactionContext> ctx);

  /// \brief Return the Table that this transaction will update
  const std::shared_ptr<Table>& table() const;

  /// \brief Returns the base metadata without any changes
  const TableMetadata* base() const;

  /// \brief Return the current metadata with staged changes applied
  const TableMetadata& current() const;

  /// \brief Return the location of the metadata file with the given filename
  ///
  /// \param filename the name of the metadata file
  /// \return the location of the metadata file
  std::string MetadataFileLocation(std::string_view filename) const;

  /// \brief Apply the pending changes from all actions and commit.
  ///
  /// \return Updated table if the transaction was committed successfully, or an error:
  /// - ValidationFailed: if any update cannot be applied to the current table metadata.
  /// - CommitFailed: if the updates cannot be committed due to conflicts.
  Result<std::shared_ptr<Table>> Commit();

  /// \brief Create a new UpdatePartitionSpec to update the partition spec of this table
  /// and commit the changes.
  Result<std::shared_ptr<UpdatePartitionSpec>> NewUpdatePartitionSpec();

  /// \brief Create a new UpdateProperties to update table properties and commit the
  /// changes.
  Result<std::shared_ptr<UpdateProperties>> NewUpdateProperties();

  /// \brief Create a new UpdateSortOrder to update the table sort order and commit the
  /// changes.
  Result<std::shared_ptr<UpdateSortOrder>> NewUpdateSortOrder();

  /// \brief Create a new UpdateSchema to alter the columns of this table and commit the
  /// changes.
  Result<std::shared_ptr<UpdateSchema>> NewUpdateSchema();

  /// \brief Create a new ExpireSnapshots to remove expired snapshots and commit the
  /// changes.
  Result<std::shared_ptr<ExpireSnapshots>> NewExpireSnapshots();

  /// \brief Create a new UpdateStatistics to update table statistics and commit the
  /// changes.
  Result<std::shared_ptr<UpdateStatistics>> NewUpdateStatistics();

  /// \brief Create a new UpdatePartitionStatistics to update partition statistics and
  /// commit the changes.
  Result<std::shared_ptr<UpdatePartitionStatistics>> NewUpdatePartitionStatistics();

  /// \brief Create a new UpdateLocation to update the table location and commit the
  /// changes.
  Result<std::shared_ptr<UpdateLocation>> NewUpdateLocation();

  /// \brief Create a new FastAppend to append data files and commit the changes.
  Result<std::shared_ptr<FastAppend>> NewFastAppend();

  /// \brief Create a new SnapshotManager to manage snapshots.
  Result<std::shared_ptr<SnapshotManager>> NewSnapshotManager();

  /// \brief Create a new SetSnapshot to set the current snapshot or rollback to a
  /// previous snapshot and commit the changes.
  Result<std::shared_ptr<SetSnapshot>> NewSetSnapshot();

  /// \brief Create a new UpdateSnapshotReference to update snapshot references (branches
  /// and tags) and commit the changes.
  Result<std::shared_ptr<UpdateSnapshotReference>> NewUpdateSnapshotReference();

 private:
  explicit Transaction(std::shared_ptr<TransactionContext> ctx);

  Status AddUpdate(const std::shared_ptr<PendingUpdate>& update);

  /// \brief Apply the pending changes to current table.
  Status Apply(PendingUpdate& updates);

  // Helper methods for applying different types of updates
  Status ApplyExpireSnapshots(ExpireSnapshots& update);
  Status ApplySetSnapshot(SetSnapshot& update);
  Status ApplyUpdateLocation(UpdateLocation& update);
  Status ApplyUpdatePartitionSpec(UpdatePartitionSpec& update);
  Status ApplyUpdatePartitionStatistics(UpdatePartitionStatistics& update);
  Status ApplyUpdateProperties(UpdateProperties& update);
  Status ApplyUpdateSchema(UpdateSchema& update);
  Status ApplyUpdateSnapshot(SnapshotUpdate& update);
  Status ApplyUpdateSnapshotReference(UpdateSnapshotReference& update);
  Status ApplyUpdateSortOrder(UpdateSortOrder& update);
  Status ApplyUpdateStatistics(UpdateStatistics& update);

  /// \brief Perform a single commit attempt for UPDATE transactions
  Result<std::shared_ptr<Table>> CommitOnce();

  /// \brief Whether this transaction can retry after a commit conflict.
  bool CanRetry() const;

 private:
  friend class PendingUpdate;

  // Shared context owning the table, metadata builder, and kind.
  std::shared_ptr<TransactionContext> ctx_;
  // Keep track of all created pending updates.
  std::vector<std::shared_ptr<PendingUpdate>> pending_updates_;
  // To make the state simple, we require updates are added and committed in order.
  bool last_update_committed_ = true;
  // Tracks if transaction has been committed to prevent double-commit
  bool committed_ = false;
};

/// \brief Shared context between Transaction and PendingUpdate instances.
class ICEBERG_EXPORT TransactionContext {
 public:
  TransactionContext();
  ~TransactionContext();

  static Result<std::shared_ptr<TransactionContext>> Make(std::shared_ptr<Table> table,
                                                          TransactionKind kind);

  const TableMetadata* base() const;
  const TableMetadata& current() const;
  std::string MetadataFileLocation(std::string_view filename) const;

  std::shared_ptr<Table> table;
  std::unique_ptr<TableMetadataBuilder> metadata_builder;
  TransactionKind kind;
  // If PendingUpdate is created directly from Table, this is nullopt;
  // otherwise, it holds a weak pointer to the Transaction that created it.
  std::optional<std::weak_ptr<Transaction>> transaction;
};

}  // namespace iceberg
