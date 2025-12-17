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

#include "iceberg/table_update.h"
#include "iceberg/transaction.h"
#include "iceberg/util/macros.h"

namespace iceberg {

PendingUpdate::PendingUpdate(std::shared_ptr<Transaction> transaction)
    : transaction_(std::move(transaction)) {}

PendingUpdate::~PendingUpdate() = default;

Status PendingUpdate::Commit() {
  ICEBERG_ASSIGN_OR_RAISE(auto apply_result, Apply());
  return transaction_->Apply(std::move(apply_result.updates));
}

}  // namespace iceberg
