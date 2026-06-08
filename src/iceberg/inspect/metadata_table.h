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
  virtual MetadataTableType type() const noexcept = 0;

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
  Result<std::unique_ptr<DataTableScanBuilder>> NewScan() const override;

  ~MetadataTable();

 protected:
  explicit MetadataTable(std::shared_ptr<Table> source_table, TableIdentifier identifier);

  /// \brief Returns the schema for this metadata table
  ///
  /// Subclasses override this method to provide their custom schema during
  /// MetadataTable construction. The returned schema is used to initialize
  /// the underlying TableMetadata.
  ///
  /// \return The schema for this metadata table, or nullptr for default schema
  virtual std::shared_ptr<Schema> GetSchema() const { return nullptr; }

  std::shared_ptr<Table> source_table_;
};

/// \brief Metadata table factory and inspector
///
/// MetadataTable provides factory methods to create specific metadata tables for
/// inspecting table metadata. Each metadata table exposes a different aspect of the
/// table's metadata as a scannable Iceberg table.
///
/// Usage:
///   auto metadata_table = ICEBERG_TRY(
///       MetadataTableFactory::CreateMetadataTable(
///           table, MetadataTableType::kSnapshots));
///   auto scan = ICEBERG_TRY(metadata_table->NewScan());
///   // ... scan and read metadata table data
class ICEBERG_EXPORT MetadataTableFactory {
 public:
  /// \brief Create a metadata table from a table
  ///
  /// \param table The source table
  /// \param type The metadata table type to create
  /// \return A MetadataTable instance or error status
  static Result<std::unique_ptr<MetadataTable>> CreateMetadataTable(
      std::shared_ptr<Table> table, MetadataTableType type);
};

}  // namespace iceberg
