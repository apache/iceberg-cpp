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

/// \brief Base class for all kinds of table metadata updates.
///
/// Any created `PendingUpdate` instance is tracked by the `Transaction` instance
/// and commit is also delegated to the `Transaction` instance.
///
/// Lifecycle: an update is configured through its fluent mutators, then committed
/// exactly once. After that point the update is owned by its transaction until the
/// transaction reaches a terminal state.
///
/// \note Implementations are expected to use builder pattern and errors
/// should be handled by the ErrorCollector base class. Configuration errors are
/// collected and surfaced by `Commit()`. Callers must not mutate input objects or
/// reconfigure an update after its first commit.
class ICEBERG_EXPORT PendingUpdate : public ErrorCollector,
                                     public std::enable_shared_from_this<PendingUpdate> {
 public:
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
  /// An Apply failure is terminal and cannot be corrected within the same transaction.
  virtual Status Commit();

  PendingUpdate(const PendingUpdate&) = delete;
  PendingUpdate& operator=(const PendingUpdate&) = delete;
  PendingUpdate(PendingUpdate&&) = delete;
  PendingUpdate& operator=(PendingUpdate&&) = delete;

  ~PendingUpdate() override;

 protected:
  explicit PendingUpdate(std::shared_ptr<TransactionContext> ctx);

  const TableMetadata& base() const;

  Status CheckCommitAllowed() const;

  /// \brief Discard this generation's staging, retaining applied operation intent.
  virtual Status CleanStaged() { return {}; }
  /// \brief Complete a known successful commit. Called only by Transaction with the
  /// final committed table metadata.
  virtual Status Finalize(const TableMetadata& committed);

  std::shared_ptr<TransactionContext> ctx_;

 private:
  friend class Transaction;

  bool commit_called_ = false;
};

}  // namespace iceberg
