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

#include "iceberg/catalog.h"
#include "iceberg/pending_update.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_requirements.h"
#include "iceberg/table_update.h"

namespace iceberg {

BaseTransaction::BaseTransaction(std::shared_ptr<const Table> table,
                                 std::shared_ptr<Catalog> catalog)
    : table_(std::move(table)), catalog_(std::move(catalog)) {
  ICEBERG_DCHECK(table_ != nullptr, "table must not be null");
  ICEBERG_DCHECK(catalog_ != nullptr, "catalog must not be null");
}

const std::shared_ptr<const Table>& BaseTransaction::table() const { return table_; }

std::shared_ptr<PropertiesUpdate> BaseTransaction::UpdateProperties() {
  return RegisterUpdate<PropertiesUpdate>();
}

std::shared_ptr<AppendFiles> BaseTransaction::NewAppend() {
  throw NotImplemented("BaseTransaction::NewAppend not implemented");
}

Status BaseTransaction::CommitTransaction() {
  const auto& metadata = table_->metadata();
  if (!metadata) {
    return InvalidArgument("Table metadata is null");
  }

  auto builder = TableMetadataBuilder::BuildFrom(metadata.get());
  for (const auto& pending_update : pending_updates_) {
    if (!pending_update) {
      continue;
    }
    ICEBERG_RETURN_UNEXPECTED(pending_update->Apply(*builder));
  }

  auto table_updates = builder->GetChanges();
  TableUpdateContext context(metadata.get(), /*is_replace=*/false);
  for (const auto& update : table_updates) {
    ICEBERG_RETURN_UNEXPECTED(update->GenerateRequirements(context));
  }
  ICEBERG_ASSIGN_OR_RAISE(auto table_requirements, context.Build());

  ICEBERG_ASSIGN_OR_RAISE(
      auto updated_table,
      catalog_->UpdateTable(table_->name(), table_requirements, table_updates));

  if (updated_table) {
    table_ = std::shared_ptr<Table>(std::move(updated_table));
  }

  pending_updates_.clear();
  return {};
}

}  // namespace iceberg
