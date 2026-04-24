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

PendingUpdate::PendingUpdate(std::shared_ptr<TransactionContext> ctx)
    : ctx_(std::move(ctx)) {}

PendingUpdate::~PendingUpdate() = default;

Status PendingUpdate::Commit() {
  if (!ctx_->transaction) {
    // Table-created path: no transaction exists yet, create a temporary one.
    ICEBERG_ASSIGN_OR_RAISE(auto txn, Transaction::Make(ctx_));
    auto apply_status = txn->Apply(*this);
    if (!apply_status.has_value()) {
      std::ignore = Finalize(std::unexpected(apply_status.error()));
      return apply_status;
    }

    auto commit_result = txn->Commit();
    if (!commit_result.has_value()) {
      std::ignore = Finalize(std::unexpected(commit_result.error()));
      return std::unexpected(commit_result.error());
    }

    std::ignore = Finalize(commit_result.value()->metadata().get());
    return {};
  }
  auto txn = ctx_->transaction->lock();
  if (!txn) {
    return CommitFailed("Transaction has been destroyed");
  }
  return txn->Apply(*this);
}

Status PendingUpdate::Finalize(
    [[maybe_unused]] Result<const TableMetadata*> commit_result) {
  return {};
}

const TableMetadata& PendingUpdate::base() const { return ctx_->current(); }

}  // namespace iceberg
