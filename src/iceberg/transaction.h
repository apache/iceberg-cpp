
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

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief A transaction for performing multiple updates to a table
class ICEBERG_EXPORT Transaction {
 public:
  virtual ~Transaction() = default;

  /// \brief Return the Table that this transaction will update
  ///
  /// \return this transaction's table
  virtual const std::shared_ptr<Table>& table() const = 0;

  /// \brief Create a new schema addition operation
  ///
  /// \return a new AddSchema
  virtual std::shared_ptr<AddSchema> AddSchema() = 0;

  /// \brief Create a new set current schema operation
  ///
  /// \param schema_id the schema id to set as current
  /// \return a new SetCurrentSchema
  virtual std::shared_ptr<SetCurrentSchema> SetCurrentSchema(int32_t schema_id) = 0;

  /// \brief Create a new remove schemas operation
  ///
  /// \param schema_ids the schema ids to remove
  /// \return a new RemoveSchemas
  virtual std::shared_ptr<RemoveSchemas> RemoveSchemas(
      const std::vector<int32_t>& schema_ids) = 0;

  /// \brief Create a new partition spec addition operation
  ///
  /// \return a new AddPartitionSpec
  virtual std::shared_ptr<AddPartitionSpec> AddPartitionSpec() = 0;

  /// \brief Create a new set default partition spec operation
  ///
  /// \param spec_id the partition spec id to set as default
  /// \return a new SetDefaultPartitionSpec
  virtual std::shared_ptr<SetDefaultPartitionSpec> SetDefaultPartitionSpec(
      int32_t spec_id) = 0;

  /// \brief Create a new remove partition specs operation
  ///
  /// \param spec_ids the partition spec ids to remove
  /// \return a new RemovePartitionSpecs
  virtual std::shared_ptr<RemovePartitionSpecs> RemovePartitionSpecs(
      const std::vector<int32_t>& spec_ids) = 0;

  /// \brief Create a new sort order addition operation
  ///
  /// \return a new AddSortOrder
  virtual std::shared_ptr<AddSortOrder> AddSortOrder() = 0;

  /// \brief Create a new set default sort order operation
  ///
  /// \param order_id the sort order id to set as default
  /// \return a new SetDefaultSortOrder
  virtual std::shared_ptr<SetDefaultSortOrder> SetDefaultSortOrder(int32_t order_id) = 0;

  /// \brief Create a new remove sort orders operation
  ///
  /// \param order_ids the sort order ids to remove
  /// \return a new RemoveSortOrders
  virtual std::shared_ptr<RemoveSortOrders> RemoveSortOrders(
      const std::vector<int32_t>& order_ids) = 0;

  /// \brief Create a new set properties operation
  ///
  /// \return a new SetProperties
  virtual std::shared_ptr<SetProperties> SetProperties() = 0;

  /// \brief Create a new remove properties operation
  ///
  /// \return a new RemoveProperties
  virtual std::shared_ptr<RemoveProperties> RemoveProperties() = 0;

  /// \brief Create a new set location operation
  ///
  /// \return a new SetLocation
  virtual std::shared_ptr<SetLocation> SetLocation() = 0;

  /// \brief Create a new append API to add files to this table
  ///
  /// \return a new AppendFiles
  virtual std::shared_ptr<AppendFiles> NewAppend() = 0;

  /// \brief Apply the pending changes from all actions and commit
  ///
  /// This method applies all pending data operations and metadata updates in the
  /// transaction and commits them to the table in a single atomic operation.
  ///
  /// \return Status::OK if the transaction was committed successfully, or an error
  ///         status if validation failed or the commit encountered conflicts
  virtual Status CommitTransaction() = 0;
};

}  // namespace iceberg
