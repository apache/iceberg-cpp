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

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_identifier.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/timepoint.h"

namespace iceberg {

/// \brief Represents an Iceberg table
class ICEBERG_EXPORT Table : public std::enable_shared_from_this<Table> {
 public:
  /// \brief Construct a table.
  /// \param[in] identifier The identifier of the table.
  /// \param[in] metadata The metadata for the table.
  /// \param[in] metadata_location The location of the table metadata file.
  /// \param[in] io The FileIO to read and write table data and metadata files.
  /// \param[in] catalog The catalog that this table belongs to.
  static Result<std::shared_ptr<Table>> Make(TableIdentifier identifier,
                                             std::shared_ptr<TableMetadata> metadata,
                                             std::string metadata_location,
                                             std::shared_ptr<FileIO> io,
                                             std::shared_ptr<Catalog> catalog);

  virtual ~Table();

  /// \brief Return the identifier of this table
  const TableIdentifier& name() const { return identifier_; }

  /// \brief Returns the UUID of the table
  const std::string& uuid() const;

  /// \brief Return the schema for this table, return NotFoundError if not found
  Result<std::shared_ptr<Schema>> schema() const;

  /// \brief Return a map of schema for this table
  Result<
      std::reference_wrapper<const std::unordered_map<int32_t, std::shared_ptr<Schema>>>>
  schemas() const;

  /// \brief Return the partition spec for this table, return NotFoundError if not found
  Result<std::shared_ptr<PartitionSpec>> spec() const;

  /// \brief Return a map of partition specs for this table
  Result<std::reference_wrapper<
      const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>>>
  specs() const;

  /// \brief Return the sort order for this table, return NotFoundError if not found
  Result<std::shared_ptr<SortOrder>> sort_order() const;

  /// \brief Return a map of sort order IDs to sort orders for this table
  Result<std::reference_wrapper<
      const std::unordered_map<int32_t, std::shared_ptr<SortOrder>>>>
  sort_orders() const;

  /// \brief Return a map of string properties for this table
  const TableProperties& properties() const;

  /// \brief Return the table's metadata file location
  std::string_view metadata_file_location() const;

  /// \brief Return the table's base location
  std::string_view location() const;

  /// \brief Returns the time when this table was last updated
  TimePointMs last_updated_ms() const;

  /// \brief Return the table's current snapshot, return NotFoundError if not found
  Result<std::shared_ptr<Snapshot>> current_snapshot() const;

  /// \brief Get the snapshot of this table with the given id
  ///
  /// \param snapshot_id the ID of the snapshot to get
  /// \return the Snapshot with the given id, return NotFoundError if not found
  Result<std::shared_ptr<Snapshot>> SnapshotById(int64_t snapshot_id) const;

  /// \brief Get the snapshots of this table
  const std::vector<std::shared_ptr<Snapshot>>& snapshots() const;

  /// \brief Get the snapshot history of this table
  const std::vector<SnapshotLogEntry>& history() const;

  /// \brief Returns a FileIO to read and write table data and metadata files
  const std::shared_ptr<FileIO>& io() const;

  /// \brief Returns the current metadata for this table
  const std::shared_ptr<TableMetadata>& metadata() const;

  /// \brief Returns the catalog that this table belongs to
  const std::shared_ptr<Catalog>& catalog() const;

  /// \brief Refresh the current table metadata
  virtual Status Refresh();

  /// \brief Create a new table scan builder for this table
  ///
  /// Once a table scan builder is created, it can be refined to project columns and
  /// filter data.
  virtual Result<std::unique_ptr<TableScanBuilder>> NewScan() const;

  /// \brief Create a new Transaction to commit multiple table operations at once.
  virtual Result<std::shared_ptr<Transaction>> NewTransaction();

  /// \brief Create a new UpdateProperties to update table properties and commit the
  /// changes.
  virtual Result<std::shared_ptr<UpdateProperties>> NewUpdateProperties();

  /// \brief Create a new UpdateSortOrder to update the table sort order and commit the
  /// changes.
  virtual Result<std::shared_ptr<UpdateSortOrder>> NewUpdateSortOrder();

 protected:
  Table(TableIdentifier identifier, std::shared_ptr<TableMetadata> metadata,
        std::string metadata_location, std::shared_ptr<FileIO> io,
        std::shared_ptr<Catalog> catalog);

  const TableIdentifier identifier_;
  std::shared_ptr<TableMetadata> metadata_;
  std::string metadata_location_;
  std::shared_ptr<FileIO> io_;
  std::shared_ptr<Catalog> catalog_;
  std::unique_ptr<class TableMetadataCache> metadata_cache_;
};

/// \brief A table created by stage-create and not yet committed.
class ICEBERG_EXPORT StagedTable final : public Table {
 public:
  static Result<std::shared_ptr<StagedTable>> Make(
      TableIdentifier identifier, std::shared_ptr<TableMetadata> metadata,
      std::string metadata_location, std::shared_ptr<FileIO> io,
      std::shared_ptr<Catalog> catalog);

  ~StagedTable() override;

  Status Refresh() override { return {}; }

  Result<std::unique_ptr<TableScanBuilder>> NewScan() const override;

 private:
  using Table::Table;
};

/// \brief A read-only table.

class ICEBERG_EXPORT StaticTable final : public Table {
 public:
  static Result<std::shared_ptr<StaticTable>> Make(
      TableIdentifier identifier, std::shared_ptr<TableMetadata> metadata,
      std::string metadata_location, std::shared_ptr<FileIO> io);

  ~StaticTable() override;

  Status Refresh() override;

  Result<std::shared_ptr<Transaction>> NewTransaction() override;

  Result<std::shared_ptr<UpdateProperties>> NewUpdateProperties() override;

 private:
  using Table::Table;
};

}  // namespace iceberg
