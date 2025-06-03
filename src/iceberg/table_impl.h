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

#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/location_provider.h"
#include "iceberg/result.h"
#include "iceberg/table.h"
#include "iceberg/transaction.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief An abstract base implementation of the Iceberg Table interface.
///
/// BaseTable provides common functionality for Iceberg table implementations,
/// including lazy initialization of schema, partition specs, sort orders,
/// and snapshot metadata.
///
/// This class is not intended to be used directly by users, but serves as a foundation
/// for concrete implementations such as StaticTable or CatalogTable.
class ICEBERG_EXPORT BaseTable : public Table {
 public:
  ~BaseTable() override = default;

  BaseTable(std::string name, std::shared_ptr<TableMetadata> metadata);

  const std::string& name() const override { return name_; }

  const std::string& uuid() const override;

  Result<std::shared_ptr<Schema>> schema() const override;

  const std::unordered_map<int32_t, std::shared_ptr<Schema>>& schemas() const override;

  Result<std::shared_ptr<PartitionSpec>> spec() const override;

  const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>& specs()
      const override;

  Result<std::shared_ptr<SortOrder>> sort_order() const override;

  const std::unordered_map<int32_t, std::shared_ptr<SortOrder>>& sort_orders()
      const override;

  const std::unordered_map<std::string, std::string>& properties() const override;

  const std::string& location() const override;

  Result<std::shared_ptr<Snapshot>> current_snapshot() const override;

  Result<std::shared_ptr<Snapshot>> snapshot(int64_t snapshot_id) const override;

  const std::vector<std::shared_ptr<Snapshot>>& snapshots() const override;

  const std::vector<std::shared_ptr<HistoryEntry>>& history() const override;

 private:
  void InitSchema() const;
  void InitPartitionSpec() const;
  void InitSortOrder() const;
  void InitSnapshot() const;

  const std::string name_;

  mutable std::unordered_map<int32_t, std::shared_ptr<Schema>> schemas_map_;

  mutable std::shared_ptr<PartitionSpec> partition_spec_;
  mutable std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> partition_spec_map_;

  mutable std::shared_ptr<SortOrder> sort_order_;
  mutable std::unordered_map<int32_t, std::shared_ptr<SortOrder>> sort_orders_map_;

  mutable std::shared_ptr<Snapshot> current_snapshot_;
  mutable std::unordered_map<int64_t, std::shared_ptr<Snapshot>> snapshots_map_;

  mutable std::vector<std::shared_ptr<HistoryEntry>> history_;

  std::shared_ptr<TableMetadata> metadata_;

  // once_flags
  mutable std::once_flag init_schema_once_;
  mutable std::once_flag init_partition_spec_once_;
  mutable std::once_flag init_sort_order_once_;
  mutable std::once_flag init_snapshot_once_;
};

/// \brief A read-only implementation of an Iceberg table.
///
/// StaticTable represents a snapshot of a table that does not support mutation.
class ICEBERG_EXPORT StaticTable : public BaseTable {
 public:
  ~StaticTable() override = default;

  StaticTable(std::string name, std::shared_ptr<TableMetadata> metadata)
      : BaseTable(std::move(name), std::move(metadata)) {}

  Status Refresh() override;

  Result<std::shared_ptr<AppendFiles>> NewAppend() override;

  Result<std::unique_ptr<Transaction>> NewTransaction() override;

  Result<std::unique_ptr<LocationProvider>> location_provider() const override;
};

}  // namespace iceberg
