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

/// \file iceberg/update/pending_update.h
/// API for table changes using builder pattern

#include <memory>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/error_collector.h"

namespace iceberg {

enum class TransactionState : uint8_t;

/// \brief Base class for all kinds of table metadata updates.
///
/// Any created `PendingUpdate` instance is tracked by the `Transaction` instance
/// and commit is also delegated to the `Transaction` instance.
///
/// Lifecycle: an update is configured through its fluent mutators, then committed
/// exactly once. `Commit()` freezes the configuration before the first Apply so a
/// commit retry replays the same intent against refreshed metadata. After that point
/// the update is either frozen (explicit transaction, awaiting `Transaction::Commit()`)
/// or terminal (standalone commit finished, or the owning transaction reached a
/// terminal state).
///
/// \note Implementations are expected to use builder pattern and errors
/// should be handled by the ErrorCollector base class. Configuration errors are
/// collected and surfaced by `Commit()`. Lifecycle misuse is different: calling a
/// mutator on a frozen or terminal update, or while an operation is in progress,
/// throws `IcebergError` rather than adding a collected error, so the frozen replay
/// intent can never be poisoned.
class ICEBERG_EXPORT PendingUpdate : protected ErrorCollector,
                                     public std::enable_shared_from_this<PendingUpdate> {
 public:
  using ErrorCollector::CheckErrors;
  using ErrorCollector::error_count;
  using ErrorCollector::errors;
  using ErrorCollector::has_errors;

  enum class Kind : uint8_t {
    kExpireSnapshots,
    kSetSnapshot,
    kUpdateLocation,
    kUpdatePartitionSpec,
    kUpdatePartitionStatistics,
    kUpdateProperties,
    kUpdateSchema,
    kUpdateSnapshot,
    kUpdateSnapshotReference,
    kUpdateSortOrder,
    kUpdateStatistics,
  };

  /// \brief Return the kind of this pending update.
  virtual Kind kind() const = 0;

  /// \brief Whether this update can be retried after a commit conflict.
  virtual bool IsRetryable() const = 0;

  /// \brief Apply the pending changes and commit.
  ///
  /// \return An OK status if the commit was successful, or an error:
  ///         - ValidationFailed: if it cannot be applied to the current table metadata.
  ///         - CommitFailed: if it cannot be committed due to conflicts.
  ///         - CommitStateUnknown: unknown status, no cleanup should be done.
  /// \note The update must be owned by a `std::shared_ptr` before calling Commit().
  /// Commit freezes its configuration. Later mutators throw IcebergError; an Apply
  /// failure is terminal and cannot be corrected within the same transaction.
  virtual Status Commit();

  PendingUpdate(const PendingUpdate&) = delete;
  PendingUpdate& operator=(const PendingUpdate&) = delete;
  PendingUpdate(PendingUpdate&&) = delete;
  PendingUpdate& operator=(PendingUpdate&&) = delete;

  ~PendingUpdate() override;

 protected:
  explicit PendingUpdate(std::shared_ptr<TransactionContext> ctx);

  const TableMetadata& base() const;

  /// \brief Reject lifecycle misuse without poisoning the builder's collected errors.
  /// Fluent mutators throw IcebergError when frozen, terminal, or reentered.
  void EnsureMutable() const;
  Status CheckCommitAllowed() const;

  /// \brief Capture mutable input values before the first Apply.
  virtual Status Freeze() { return {}; }
  /// \brief Discard this generation's staging, retaining frozen operation intent.
  virtual Status CleanStaged() { return {}; }
  /// \brief Complete a known successful commit. Called only by Transaction with the
  /// final committed table metadata.
  virtual Status Finalize(const TableMetadata& committed);
  virtual Status ReportCommitted() { return {}; }
  virtual bool MayAddFileReferences() const { return true; }

  using ErrorCollector::AddError;
  using ErrorCollector::ClearErrors;

  std::shared_ptr<TransactionContext> ctx_;

 private:
  friend class Transaction;
  // ErrorCollector's explicit-object helpers access the derived builder's errors.
  friend class ErrorCollector;

  void Cleanup() noexcept;
  void FinalizeOnce(const TableMetadata& committed) noexcept;

  /// \brief Lifecycle of this update, advanced only by Transaction.
  ///
  /// kMutable: configurable, not yet applied.
  /// kFrozen: public configuration is closed. Entered before Freeze and the first
  /// Apply; reaching this phase does not imply either step has succeeded.
  /// kTerminal: the owning transaction reached a terminal state (an update that was
  /// never applied can also become terminal when its transaction is aborted).
  enum class Phase : uint8_t { kMutable, kFrozen, kTerminal };
  Phase phase_ = Phase::kMutable;

  // True while the current Apply generation has not been consumed by Cleanup or
  // FinalizeOnce. Set before Freeze/Apply, even before any output exists, so a
  // failure can clean partially staged resources. Cleared before cleanup or
  // finalization hooks run to make them idempotent per generation and to skip
  // finalization/reporting for a snapshot generation already cleaned as a no-op.
  bool staged_ = false;
};

}  // namespace iceberg
