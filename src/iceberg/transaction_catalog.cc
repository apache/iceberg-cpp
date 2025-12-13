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

#include "iceberg/transaction_catalog.h"

#include "iceberg/base_transaction.h"
#include "iceberg/table_metadata.h"

namespace iceberg {

Result<std::unique_ptr<Table>> TransactionCatalog::UpdateTable(
    const TableIdentifier& identifier,
    const std::vector<std::shared_ptr<const TableRequirement>>& requirements,
    const std::vector<std::shared_ptr<const TableUpdate>>& updates) {
  if (!owner_) {
    return InvalidState("Transaction state is unavailable");
  }

  return owner_->StageUpdates(identifier, requirements, updates);
}

Result<std::unique_ptr<Table>> TransactionCatalog::LoadTable(
    const TableIdentifier& identifier) {
  if (!owner_) {
    return InvalidState("Transaction state is unavailable");
  }

  auto metadata = std::make_shared<TableMetadata>(*owner_->context_.current_metadata);
  return std::make_unique<Table>(identifier, std::move(metadata),
                                 owner_->table()->metadata_location(),
                                 owner_->table()->io(), catalog_impl_);
}

void TransactionCatalog::SetLastOperationCommitted(bool committed) {
  if (owner_) {
    owner_->SetLastOperationCommitted(committed);
  }
}

}  // namespace iceberg
