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

#include "iceberg/exception.h"
#include "iceberg/logging/log_macros.h"
#include "iceberg/result.h"
#include "iceberg/table.h"
#include "iceberg/transaction.h"
#include "iceberg/util/macros.h"

namespace iceberg {
namespace {

template <typename Hook>
void BestEffort(std::string_view name, Hook&& hook) noexcept {
  try {
    if (auto result = hook(); !result) {
      ICEBERG_LOG_WARN("Update {} failed: {}", name, result.error().message);
    }
  } catch (const std::exception& e) {
    ICEBERG_LOG_WARN("Update {} threw: {}", name, e.what());
  } catch (...) {
    ICEBERG_LOG_WARN("Update {} threw an unknown exception", name);
  }
}

}  // namespace

PendingUpdate::PendingUpdate(std::shared_ptr<TransactionContext> ctx)
    : ctx_(std::move(ctx)) {}

PendingUpdate::~PendingUpdate() = default;

void PendingUpdate::EnsureMutable() const {
  ICEBERG_CHECK_OR_DIE(phase_ == Phase::kMutable,
                       "Update configuration is frozen or terminal");
  ICEBERG_CHECK_OR_DIE(!ctx_->in_progress_,
                       "Cannot mutate an update during an operation");
  if (ctx_->transaction) {
    auto txn = ctx_->transaction->lock();
    ICEBERG_CHECK_OR_DIE(txn != nullptr, "Transaction has been destroyed");
    ICEBERG_CHECK_OR_DIE(txn->state() == TransactionState::kReady ||
                             txn->state() == TransactionState::kUpdatePending,
                         "Transaction is terminal");
  }
}

Status PendingUpdate::CheckCommitAllowed() const {
  ICEBERG_CHECK(phase_ != Phase::kTerminal, "Update is terminal");
  ICEBERG_CHECK(!ctx_->in_progress_, "Cannot reenter an update or transaction operation");
  return {};
}

Status PendingUpdate::Commit() {
  ICEBERG_RETURN_UNEXPECTED(CheckCommitAllowed());
  if (ctx_->transaction) {
    auto txn = ctx_->transaction->lock();
    ICEBERG_CHECK(txn != nullptr, "Transaction has been destroyed");
    return txn->Apply(*this);
  }

  auto self = weak_from_this().lock();
  ICEBERG_PRECHECK(self != nullptr, "PendingUpdate must be owned by std::shared_ptr");
  ICEBERG_ASSIGN_OR_RAISE(auto txn, Transaction::Make(ctx_));
  ICEBERG_RETURN_UNEXPECTED(txn->AddUpdate(self));
  ICEBERG_RETURN_UNEXPECTED(txn->Apply(*this));
  ICEBERG_RETURN_UNEXPECTED(txn->Commit());
  return {};
}

Status PendingUpdate::Finalize([[maybe_unused]] const TableMetadata& committed) {
  return {};
}

void PendingUpdate::Cleanup() noexcept {
  if (!staged_) {
    return;
  }
  // Consume the generation before any callback can throw or reenter.
  staged_ = false;
  BestEffort("staging cleanup", [this] { return CleanStaged(); });
}

void PendingUpdate::FinalizeOnce(const TableMetadata& committed) noexcept {
  // A generation already consumed by Cleanup (a staged snapshot that produced no
  // metadata change) has nothing to finalize or report.
  if (!staged_) {
    return;
  }
  staged_ = false;
  BestEffort("finalization", [this, &committed] { return Finalize(committed); });
  BestEffort("reporting", [this] { return ReportCommitted(); });
}

const TableMetadata& PendingUpdate::base() const { return ctx_->current(); }

}  // namespace iceberg
