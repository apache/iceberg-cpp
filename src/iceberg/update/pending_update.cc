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

#include "iceberg/update/pending_update.h"

#include "iceberg/result.h"
#include "iceberg/table.h"
#include "iceberg/transaction.h"
#include "iceberg/util/macros.h"

namespace iceberg {
namespace {

class ScopedTransactionBinding {
 public:
  ScopedTransactionBinding(TransactionContext& ctx,
                           const std::shared_ptr<Transaction>& txn)
      : ctx_(ctx) {
    ctx_.transaction = txn;
  }

  ~ScopedTransactionBinding() { ctx_.transaction.reset(); }

 private:
  TransactionContext& ctx_;
};

}  // namespace

PendingUpdate::PendingUpdate(std::shared_ptr<TransactionContext> ctx)
    : ctx_(std::move(ctx)) {}

PendingUpdate::~PendingUpdate() = default;

Status PendingUpdate::Commit() {
  if (!ctx_->transaction) {
    // Table-created path: no transaction exists yet, create a temporary one.
    ICEBERG_ASSIGN_OR_RAISE(auto txn, Transaction::Make(ctx_));
    auto self = weak_from_this().lock();
    ICEBERG_PRECHECK(self != nullptr, "PendingUpdate must be owned by std::shared_ptr");
    ICEBERG_RETURN_UNEXPECTED(txn->AddUpdate(self));
    // Keep Transaction::Make(ctx_) detached, but expose this live transaction while
    // Commit() runs so an internal retry can reapply through update->Commit().
    ScopedTransactionBinding binding(*ctx_, txn);

    auto apply_status = txn->Apply(*this);
    if (!apply_status.has_value()) {
      txn->FinalizeUpdates(std::unexpected(apply_status.error()));
      return apply_status;
    }

    auto commit_result = txn->Commit();
    ICEBERG_RETURN_UNEXPECTED(commit_result);
    return {};
  }

  auto txn = ctx_->transaction->lock();
  if (!txn) {
    return CommitFailed("Transaction has been destroyed");
  }

  auto apply_status = txn->Apply(*this);
  if (!apply_status.has_value() && !txn->committing_) {
    // Finalize eagerly so a failed update cleans up its staged files even if the
    // caller never commits the transaction. When the transaction is mid-commit,
    // leave finalization to Transaction::Commit(): the failure may be retryable
    // (e.g. RetryableValidationFailed from a stale sequence number), and
    // finalizing here would destroy staged state before the retry runs.
    txn->FinalizeUpdates(std::unexpected(apply_status.error()));
  }
  return apply_status;
}

Status PendingUpdate::Finalize(
    [[maybe_unused]] Result<const TableMetadata*> commit_result) {
  return {};
}

const TableMetadata& PendingUpdate::base() const { return ctx_->current(); }

}  // namespace iceberg
