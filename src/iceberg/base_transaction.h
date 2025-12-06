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

#include <memory>
#include <vector>

#include "iceberg/table_identifier.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_requirement.h"
#include "iceberg/table_update.h"
#include "iceberg/transaction.h"
#include "iceberg/transaction_catalog.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Base class for transaction implementations
class ICEBERG_EXPORT BaseTransaction : public Transaction {
 public:
  BaseTransaction(std::shared_ptr<const Table> table, std::shared_ptr<Catalog> catalog);
  ~BaseTransaction() override = default;

  const std::shared_ptr<const Table>& table() const override;

  std::unique_ptr<::iceberg::UpdateProperties> UpdateProperties() override;

  std::unique_ptr<AppendFiles> NewAppend() override;

  Status CommitTransaction() override;

  Result<std::unique_ptr<Table>> StageUpdates(
      const TableIdentifier& identifier,
      std::vector<std::unique_ptr<TableRequirement>> requirements,
      std::vector<std::unique_ptr<TableUpdate>> updates);

  bool HasLastOperationCommitted() const { return context_.last_operation_committed; }

  void SetLastOperationCommitted(bool committed) {
    context_.last_operation_committed = committed;
  }

  const std::shared_ptr<TableMetadata>& CurrentMetadata() const {
    return context_.current_metadata;
  }

  Status ApplyUpdates(const std::vector<std::unique_ptr<TableUpdate>>& updates);

  std::vector<std::unique_ptr<TableRequirement>> ConsumePendingRequirements();

  std::vector<std::unique_ptr<TableUpdate>> ConsumePendingUpdates();

 protected:
  template <typename UpdateType, typename... Args>
  Result<std::unique_ptr<UpdateType>> CheckAndCreateUpdate(Args&&... args) {
    if (!HasLastOperationCommitted()) {
      return InvalidState(
          "Cannot create new update: last operation in transaction has not committed");
    }
    SetLastOperationCommitted(false);
    return std::make_unique<UpdateType>(std::forward<Args>(args)...);
  }

 private:
  struct TransactionContext {
    TransactionContext() = default;
    TransactionContext(const TableIdentifier& identifier,
                       std::shared_ptr<TableMetadata> metadata)
        : identifier(identifier), current_metadata(std::move(metadata)) {}

    bool last_operation_committed = true;
    TableIdentifier identifier;
    std::shared_ptr<TableMetadata> current_metadata;
    std::vector<std::unique_ptr<TableRequirement>> pending_requirements;
    std::vector<std::unique_ptr<TableUpdate>> pending_updates;
  };

  std::shared_ptr<const Table> table_;
  std::shared_ptr<TransactionCatalog> catalog_;
  TransactionContext context_;
};

}  // namespace iceberg
