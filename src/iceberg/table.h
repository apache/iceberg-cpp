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
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Represents an Iceberg table
class ICEBERG_EXPORT Table {
 public:
  virtual ~Table() = default;

  /// \brief Return the full name for this table
  virtual const std::string& name() const = 0;

  /// \brief Returns the UUID of the table
  virtual const std::string& uuid() const = 0;

  /// \brief Refresh the current table metadata
  virtual Status Refresh() = 0;

  /// \brief Return the schema for this table
  virtual const std::shared_ptr<Schema>& schema() const = 0;

  /// \brief Return a map of schema for this table
  virtual const std::unordered_map<int32_t, std::shared_ptr<Schema>>& schemas() const = 0;

  /// \brief Return the partition spec for this table
  virtual const std::shared_ptr<PartitionSpec>& spec() const = 0;

  /// \brief Return a map of partition specs for this table
  virtual const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>& specs()
      const = 0;

  /// \brief Return the sort order for this table
  virtual const std::shared_ptr<SortOrder>& sort_order() const = 0;

  /// \brief Return a map of sort order IDs to sort orders for this table
  virtual const std::unordered_map<int32_t, std::shared_ptr<SortOrder>>& sort_orders()
      const = 0;

  /// \brief Return a map of string properties for this table
  virtual const std::unordered_map<std::string, std::string>& properties() const = 0;

  /// \brief Return the table's base location
  virtual const std::string& location() const = 0;

  /// \brief Return the table's current snapshot
  virtual const std::shared_ptr<Snapshot>& current_snapshot() const = 0;

  /// \brief Get the snapshot of this table with the given id, or null if there is no
  /// matching snapshot
  ///
  /// \param snapshot_id the ID of the snapshot to get
  /// \return the Snapshot with the given id
  virtual Result<std::shared_ptr<Snapshot>> snapshot(int64_t snapshot_id) const = 0;

  /// \brief Get the snapshots of this table
  virtual const std::vector<std::shared_ptr<Snapshot>>& snapshots() const = 0;

  /// \brief Get the snapshot history of this table
  ///
  /// \return a vector of history entries
  virtual const std::vector<std::shared_ptr<HistoryEntry>>& history() const = 0;

  /// \brief Create a new table scan for this table
  ///
  /// Once a table scan is created, it can be refined to project columns and filter data.
  virtual std::unique_ptr<TableScan> NewScan() const = 0;

  /// \brief Create a new append API to add files to this table and commit
  virtual std::shared_ptr<AppendFiles> NewAppend() = 0;

  /// \brief Create a new transaction API to commit multiple table operations at once
  virtual std::unique_ptr<Transaction> NewTransaction() = 0;

  /// TODO(wgtmac): design of FileIO is not finalized yet. We intend to use an
  /// IO-less design in the core library.
  // /// \brief Returns a FileIO to read and write table data and metadata files
  // virtual std::shared_ptr<FileIO> io() const = 0;

  /// \brief Returns a LocationProvider to provide locations for new data files
  virtual std::unique_ptr<LocationProvider> location_provider() const = 0;
};

class ICEBERG_EXPORT BaseTable : public Table {
 public:
  ~BaseTable() override = default;
  BaseTable(std::string name, std::shared_ptr<TableMetadata> metadata);

  const std::string& name() const override { return name_; }

  const std::string& uuid() const override;

  const std::shared_ptr<Schema>& schema() const override;

  const std::unordered_map<int32_t, std::shared_ptr<Schema>>& schemas() const override;

  const std::shared_ptr<PartitionSpec>& spec() const override;

  const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>& specs()
      const override;

  const std::shared_ptr<SortOrder>& sort_order() const override;

  const std::unordered_map<int32_t, std::shared_ptr<SortOrder>>& sort_orders()
      const override;

  const std::unordered_map<std::string, std::string>& properties() const override;

  const std::string& location() const override;

  const std::shared_ptr<Snapshot>& current_snapshot() const override;

  Result<std::shared_ptr<Snapshot>> snapshot(int64_t snapshot_id) const override;

  const std::vector<std::shared_ptr<Snapshot>>& snapshots() const override;

  const std::vector<std::shared_ptr<HistoryEntry>>& history() const override;

 private:
  void InitSchema() const;
  void InitPartitionSpec() const;
  void InitSortOrder() const;
  void InitSnapshot() const;

  const std::string name_;

  mutable std::shared_ptr<Schema> schema_;
  mutable std::unordered_map<int32_t, std::shared_ptr<Schema>> schemas_map_;

  mutable std::shared_ptr<PartitionSpec> partition_spec_;
  mutable std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> partition_spec_map_;

  mutable std::shared_ptr<SortOrder> sort_order_;
  mutable std::unordered_map<int32_t, std::shared_ptr<SortOrder>> sort_orders_map_;

  mutable std::shared_ptr<Snapshot> current_snapshot_;
  mutable std::unordered_map<int64_t, std::shared_ptr<Snapshot>> snapshots_map_;

  std::shared_ptr<TableMetadata> metadata_;

  // once_flags
  mutable std::once_flag init_schema_once_;
  mutable std::once_flag init_partition_spec_once_;
  mutable std::once_flag init_sort_order_once_;
  mutable std::once_flag init_snapshot_once_;
};

class ICEBERG_EXPORT StaticTable : public BaseTable {
 public:
  ~StaticTable() override = default;
  StaticTable(std::string name, std::shared_ptr<TableMetadata> metadata)
      : BaseTable(std::move(name), std::move(metadata)) {}

  Status Refresh() override;

  std::unique_ptr<TableScan> NewScan() const override;

  std::shared_ptr<AppendFiles> NewAppend() override;

  std::unique_ptr<Transaction> NewTransaction() override;

  std::unique_ptr<LocationProvider> location_provider() const override;
};

}  // namespace iceberg
