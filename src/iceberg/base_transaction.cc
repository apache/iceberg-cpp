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

#include "iceberg/base_transaction.h"

#include <utility>

#include "iceberg/catalog.h"
#include "iceberg/pending_update.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/transaction_catalog.h"
#include "iceberg/update/update_properties.h"
#include "iceberg/util/macros.h"

namespace iceberg {

BaseTransaction::BaseTransaction(std::shared_ptr<const Table> table,
                                 std::shared_ptr<Catalog> catalog)
    : table_(std::move(table)) {
  ICEBERG_DCHECK(table_ != nullptr, "table must not be null");
  ICEBERG_DCHECK(catalog != nullptr, "catalog must not be null");
  context_.identifier = table_->name();
  context_.current_metadata = table_->metadata();
  catalog_ = std::make_shared<TransactionCatalog>(std::move(catalog), this);
}

const std::shared_ptr<const Table>& BaseTransaction::table() const { return table_; }

std::unique_ptr<UpdateProperties> BaseTransaction::UpdateProperties() {
  auto update = CheckAndCreateUpdate<::iceberg::UpdateProperties>(
      table_->name(), catalog_, CurrentMetadata());
  if (!update.has_value()) {
    ERROR_TO_EXCEPTION(update.error());
  }

  return std::move(update).value();
}

std::unique_ptr<AppendFiles> BaseTransaction::NewAppend() {
  throw NotImplemented("BaseTransaction::NewAppend not implemented");
}

Status BaseTransaction::CommitTransaction() {
  if (!HasLastOperationCommitted()) {
    return InvalidState("Cannot commit transaction: last operation has not committed");
  }

  auto pending_updates = ConsumePendingUpdates();
  if (pending_updates.empty()) {
    return {};
  }

  auto pending_requirements = ConsumePendingRequirements();

  ICEBERG_ASSIGN_OR_RAISE(
      auto updated_table,
      catalog_->catalog_impl()->UpdateTable(
          table_->name(), std::move(pending_requirements), std::move(pending_updates)));

  // update table to the new version
  if (updated_table) {
    table_ = std::shared_ptr<Table>(std::move(updated_table));
  }

  return {};
}

Result<std::unique_ptr<Table>> BaseTransaction::StageUpdates(
    const TableIdentifier& identifier,
    std::vector<std::unique_ptr<TableRequirement>> requirements,
    std::vector<std::unique_ptr<TableUpdate>> updates) {
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
        table_->location(), table_->io(), catalog_->catalog_impl());
  }

  ICEBERG_RETURN_UNEXPECTED(ApplyUpdates(updates));

  for (auto& requirement : requirements) {
    context_.pending_requirements.emplace_back(std::move(requirement));
  }
  for (auto& update : updates) {
    context_.pending_updates.emplace_back(std::move(update));
  }

  return std::make_unique<Table>(
      context_.identifier, std::make_shared<TableMetadata>(*context_.current_metadata),
      table_->location(), table_->io(), catalog_->catalog_impl());
}

Status BaseTransaction::ApplyUpdates(
    const std::vector<std::unique_ptr<TableUpdate>>& updates) {
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

std::vector<std::unique_ptr<TableRequirement>>
BaseTransaction::ConsumePendingRequirements() {
  return std::exchange(context_.pending_requirements, {});
}

std::vector<std::unique_ptr<TableUpdate>> BaseTransaction::ConsumePendingUpdates() {
  return std::exchange(context_.pending_updates, {});
}

}  // namespace iceberg
