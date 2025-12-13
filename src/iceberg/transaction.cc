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

#include "iceberg/transaction.h"

#include "iceberg/catalog.h"
#include "iceberg/pending_update.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/transaction_catalog.h"
#include "iceberg/update/update_properties.h"
#include "iceberg/util/macros.h"

namespace iceberg {

Result<std::unique_ptr<Transaction>> Transaction::Make(std::shared_ptr<const Table> table,
                                                       std::shared_ptr<Catalog> catalog) {
  return BaseTransaction::Make(std::move(table), std::move(catalog));
}

Result<std::unique_ptr<Transaction>> BaseTransaction::Make(
    std::shared_ptr<const Table> table, std::shared_ptr<Catalog> catalog) {
  if (!table) {
    return InvalidArgument("Transaction::Make requires a table");
  }
  if (!catalog) {
    return InvalidArgument("Transaction::Make requires a catalog");
  }

  return std::unique_ptr<Transaction>(
      new BaseTransaction(std::move(table), std::move(catalog)));
}

BaseTransaction::BaseTransaction(std::shared_ptr<const Table> table,
                                 std::shared_ptr<Catalog> catalog)
    : table_(std::move(table)) {
  context_.identifier = table_->name();
  context_.current_metadata = table_->metadata();
  catalog_ = std::make_shared<TransactionCatalog>(std::move(catalog), this);
}

const std::shared_ptr<const Table>& BaseTransaction::table() const { return table_; }

Result<std::unique_ptr<UpdateProperties>> BaseTransaction::NewUpdateProperties() {
  if (!HasLastOperationCommitted()) {
    return InvalidState(
        "Cannot create new update: last operation in transaction has not committed");
  }
  SetLastOperationCommitted(false);

  auto metadata = std::make_shared<TableMetadata>(*context_.current_metadata);
  return std::make_unique<UpdateProperties>(table_->name(), catalog_,
                                            std::move(metadata));
}

Result<std::unique_ptr<AppendFiles>> BaseTransaction::NewAppend() {
  throw NotImplemented("BaseTransaction::NewAppend not implemented");
}

Status BaseTransaction::CommitTransaction() {
  if (!HasLastOperationCommitted()) {
    return InvalidState("Cannot commit transaction: last operation has not committed");
  }

  if (context_.pending_updates.empty()) {
    return {};
  }

  ICEBERG_ASSIGN_OR_RAISE(
      auto updated_table,
      catalog_->catalog_impl()->UpdateTable(
          context_.identifier, context_.pending_requirements, context_.pending_updates));

  context_.pending_requirements.clear();
  context_.pending_updates.clear();

  return {};
}

Result<std::unique_ptr<Table>> BaseTransaction::StageUpdates(
    const TableIdentifier& identifier,
    const std::vector<std::shared_ptr<const TableRequirement>>& requirements,
    const std::vector<std::shared_ptr<const TableUpdate>>& updates) {
  if (identifier != context_.identifier) {
    return InvalidArgument("Transaction only supports table '{}'",
                           context_.identifier.name);
  }

  if (!context_.current_metadata) {
    return InvalidState("Transaction metadata is not initialized");
  }

  if (updates.empty()) {
    return std::make_unique<Table>(
        context_.identifier, std::make_shared<TableMetadata>(*context_.current_metadata),
        table_->metadata_location(), table_->io(), catalog_->catalog_impl());
  }

  ICEBERG_RETURN_UNEXPECTED(ApplyUpdates(updates));
  context_.pending_requirements.insert(context_.pending_requirements.end(),
                                       requirements.begin(), requirements.end());
  context_.pending_updates.insert(context_.pending_updates.end(), updates.begin(),
                                  updates.end());

  return std::make_unique<Table>(
      context_.identifier, std::make_shared<TableMetadata>(*context_.current_metadata),
      table_->metadata_location(), table_->io(), catalog_->catalog_impl());
}

Status BaseTransaction::ApplyUpdates(
    const std::vector<std::shared_ptr<const TableUpdate>>& updates) {
  if (updates.empty()) {
    return {};
  }

  auto builder = TableMetadataBuilder::BuildFrom(context_.current_metadata.get());
  for (const auto& update : updates) {
    if (!update) {
      continue;
    }
    update->ApplyTo(*builder);
  }

  ICEBERG_ASSIGN_OR_RAISE(auto new_metadata, builder->Build());
  context_.current_metadata = std::shared_ptr<TableMetadata>(std::move(new_metadata));
  return {};
}

}  // namespace iceberg
