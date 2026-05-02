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
#include <string>

#include "iceberg/iceberg_export.h"
#include "iceberg/location_provider.h"
#include "iceberg/result.h"
#include "iceberg/sort_order.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_scan.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief The type of metadata table
enum class MetadataTableType {
  kSnapshots,
  kHistory,
};

/// \brief Base class for Iceberg metadata tables
///
/// Metadata tables expose table metadata as queryable tables with schemas and scan
/// support. They provide read-only access to metadata.
class ICEBERG_EXPORT MetadataTable : public StaticTable {
 public:
  /// \brief Returns the identifier of this table
  const TableIdentifier& name() const override { return identifier_; }

  virtual MetadataTableType type() const noexcept = 0;

  /// \brief Returns the UUID of the table
  const std::string& uuid() const { return uuid_; }

  /// \brief Returns the schema for this table, return NotFoundError if not found
  Result<std::shared_ptr<Schema>> schema() const { return schema_; }

  /// \brief Returns a map of schema for this table
  Result<
      std::reference_wrapper<const std::unordered_map<int32_t, std::shared_ptr<Schema>>>>
  schemas() const {
    return schemas_;
  }

  /// \brief Returns the partition spec for this table, return NotFoundError if not found
  Result<std::shared_ptr<PartitionSpec>> spec() const { return partition_spec; };

  /// \brief Returns a map of partition specs for this table
  Result<std::reference_wrapper<
      const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>>>
  specs() const {
    return partition_specs_;
  }

  /// \brief Returns the sort order for this table, return NotFoundError if not found
  Result<std::shared_ptr<SortOrder>> sort_order() const { return sort_order_; }

  /// \brief Returns a map of sort order IDs to sort orders for this table
  Result<std::reference_wrapper<
      const std::unordered_map<int32_t, std::shared_ptr<SortOrder>>>>
  sort_orders() const {
    return sort_orders_;
  }

  /// \brief Returns the properties of this table
  const TableProperties& properties() const { return properties_; }

  /// \brief Returns the table's metadata file location
  std::string_view metadata_file_location() const {
    return source_table_->metadata_file_location();
  }

  /// \brief Returns the table's base location
  std::string_view location() const { return source_table_->location(); }

  /// \brief Returns the time when this table was last updated
  TimePointMs last_updated_ms() const { return source_table_->last_updated_ms(); }

  /// \brief Returns the table's current snapshot, return NotFoundError if not found
  Result<std::shared_ptr<Snapshot>> current_snapshot() const {
    return source_table_->current_snapshot();
  }

  /// \brief Get the snapshot of this table with the given id
  ///
  /// \param snapshot_id the ID of the snapshot to get
  /// \return the Snapshot with the given id, return NotFoundError if not found
  Result<std::shared_ptr<Snapshot>> SnapshotById(int64_t snapshot_id) const {
    return source_table_->SnapshotById(snapshot_id);
  }

  /// \brief Get the snapshots of this table
  const std::vector<std::shared_ptr<Snapshot>>& snapshots() const {
    return source_table_->snapshots();
  }

  /// \brief Get the snapshot history of this table
  const std::vector<SnapshotLogEntry>& history() const {
    return source_table_->history();
  }

  /// \brief Returns the current metadata for this table
  const std::shared_ptr<TableMetadata>& metadata() const {
    // TODO: or should we return an empty TableMetadata?
    return source_table_->metadata();
  }

  /// \brief Returns the catalog that this table belongs to
  const std::shared_ptr<Catalog>& catalog() const { return source_table_->catalog(); }

  /// \brief Returns a LocationProvider for this table
  Result<std::unique_ptr<LocationProvider>> location_provider() const {
    return source_table_->location_provider();
  }

  /// \brief Refreshing is not supported in metadata tables.
  Status Refresh() override;

  /// \brief Create a new table scan builder for this table
  ///
  /// Once a table scan builder is created, it can be refined to project columns and
  /// filter data.
  Result<std::unique_ptr<TableScanBuilder>> NewScan() const;

 protected:
  explicit MetadataTable(std::shared_ptr<Table> source_table, TableIdentifier identifier);

  ~MetadataTable();

  std::shared_ptr<Table> source_table_;
  std::string uuid_;
  std::shared_ptr<Schema> schema_;
  std::unordered_map<int32_t, std::shared_ptr<Schema>> schemas_;
  TableProperties properties_ = TableProperties();
  const std::shared_ptr<SortOrder> sort_order_ = SortOrder::Unsorted();
  const std::unordered_map<int32_t, std::shared_ptr<SortOrder>> sort_orders_ = {
      {sort_order_->order_id(), sort_order_}};
  const std::shared_ptr<PartitionSpec> partition_spec = PartitionSpec::Unpartitioned();
  const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> partition_specs_ = {
      {partition_spec->spec_id(), partition_spec}};
};

/// \brief Metadata table factory and inspector
///
/// MetadataTable provides factory methods to create specific metadata tables for
/// inspecting table metadata. Each metadata table exposes a different aspect of the
/// table's metadata as a scannable Iceberg table.
///
/// Usage:
///   auto snapshots = ICEBERG_TRY(MetadataTable::GetSnapshotsTable(table));
///   auto scan = ICEBERG_TRY(snapshots->NewScan());
///   // ... scan and read snapshot data
class ICEBERG_EXPORT MetadataTableFactory {
 public:
  /// \brief Create a SnapshotsTable from a table
  ///
  /// \param table The source table
  /// \return A SnapshotsTable exposing all snapshots or error status
  static Result<std::unique_ptr<SnapshotsTable>> GetSnapshotsTable(
      std::shared_ptr<Table> table);

  /// \brief Create a HistoryTable from a table
  ///
  /// \param table The source table
  /// \return A HistoryTable exposing snapshot history or error status
  static Result<std::unique_ptr<HistoryTable>> GetHistoryTable(
      std::shared_ptr<Table> table);
};

}  // namespace iceberg
