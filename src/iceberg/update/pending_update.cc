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

Status PendingUpdate::CheckCommitAllowed() const {
  ICEBERG_CHECK(!commit_called_, "Update has already been committed");
  return {};
}

Status PendingUpdate::Commit() {
  ICEBERG_RETURN_UNEXPECTED(CheckCommitAllowed());
  if (ctx_->transaction) {
    auto txn = ctx_->transaction->lock();
    ICEBERG_CHECK(txn != nullptr, "Transaction has been destroyed");
    return txn->CommitUpdate(*this);
  }

  auto self = weak_from_this().lock();
  ICEBERG_PRECHECK(self != nullptr, "PendingUpdate must be owned by std::shared_ptr");
  ICEBERG_ASSIGN_OR_RAISE(auto txn, Transaction::Make(ctx_));
  ICEBERG_RETURN_UNEXPECTED(txn->AddUpdate(self));
  ICEBERG_RETURN_UNEXPECTED(txn->CommitUpdate(*this));
  ICEBERG_RETURN_UNEXPECTED(txn->Commit());
  return {};
}

Status PendingUpdate::Finalize([[maybe_unused]] const TableMetadata& committed) {
  return {};
}

const TableMetadata& PendingUpdate::base() const { return ctx_->current(); }

}  // namespace iceberg
