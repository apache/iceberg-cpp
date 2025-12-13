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
#include "iceberg/table_requirement.h"
#include "iceberg/table_update.h"
#include "iceberg/transaction.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Base class for transaction implementations
class ICEBERG_EXPORT BaseTransaction : public Transaction {
 public:
  BaseTransaction(std::shared_ptr<const Table> table, std::shared_ptr<Catalog> catalog);
  ~BaseTransaction() override = default;

  const std::shared_ptr<const Table>& table() const override;

  Result<std::unique_ptr<UpdateProperties>> NewUpdateProperties() override;

  Result<std::unique_ptr<AppendFiles>> NewAppend() override;

  Status CommitTransaction() override;

  /// \brief Stage updates to be applied upon commit
  ///
  /// \param identifier the table identifier
  /// \param requirements the list of table requirements to validate
  /// \param updates the list of table updates to apply
  /// \return a new Table instance with staged updates applied
  Result<std::unique_ptr<Table>> StageUpdates(
      const TableIdentifier& identifier,
      const std::vector<std::shared_ptr<const TableRequirement>>& requirements,
      const std::vector<std::shared_ptr<const TableUpdate>>& updates);

  /// \brief Whether the last operation has been committed
  ///
  /// \return true if the last operation was committed, false otherwise
  bool HasLastOperationCommitted() const { return context_.last_operation_committed; }

  /// \brief Mark the last operation as committed or not
  ///
  /// \param committed true if the last operation was committed, false otherwise
  void SetLastOperationCommitted(bool committed) {
    context_.last_operation_committed = committed;
  }

 protected:
  /// \brief Apply a list of table updates to the current metadata
  ///
  /// \param updates the list of table updates to apply
  /// \return Status::OK if the updates were applied successfully, or an error status
  Status ApplyUpdates(const std::vector<std::shared_ptr<const TableUpdate>>& updates);

 private:
  /// \brief Context for transaction
  struct TransactionContext {
    TransactionContext() = default;
    TransactionContext(TableIdentifier identifier,
                       std::shared_ptr<TableMetadata> metadata)
        : identifier(std::move(identifier)), current_metadata(std::move(metadata)) {}

    // Non-copyable, movable
    TransactionContext(const TransactionContext&) = delete;
    TransactionContext& operator=(const TransactionContext&) = delete;
    TransactionContext(TransactionContext&&) noexcept = default;
    TransactionContext& operator=(TransactionContext&&) noexcept = default;

    bool last_operation_committed = true;
    TableIdentifier identifier;
    std::shared_ptr<TableMetadata> current_metadata;
    std::vector<std::shared_ptr<const TableRequirement>> pending_requirements;
    std::vector<std::shared_ptr<const TableUpdate>> pending_updates;
  };

  std::shared_ptr<const Table> table_;
  std::shared_ptr<TransactionCatalog> catalog_;
  TransactionContext context_;

  friend class TransactionCatalog;
};

}  // namespace iceberg
