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

#include <string>
#include <unordered_map>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_identifier.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Represents an Iceberg table
class ICEBERG_EXPORT Table {
 public:
  virtual ~Table() = default;

  /// \brief Construct a table.
  /// \param[in] identifier The identifier of the table.
  /// \param[in] metadata The metadata for the table.
  /// \param[in] metadata_location The location of the table metadata file.
  /// \param[in] io The FileIO to read and write table data and metadata files.
  /// \param[in] catalog The catalog that this table belongs to. If null, the table will
  /// be read-only.
  Table(TableIdentifier identifier, std::shared_ptr<TableMetadata> metadata,
        std::string metadata_location, std::shared_ptr<FileIO> io,
        std::shared_ptr<Catalog> catalog)
      : identifier_(std::move(identifier)),
        metadata_(std::move(metadata)),
        metadata_location_(std::move(metadata_location)),
        io_(std::move(io)),
        catalog_(std::move(catalog)) {};

  /// \brief Return the identifier of this table
  const TableIdentifier& name() const { return identifier_; }

  /// \brief Returns the UUID of the table
  const std::string& uuid() const;

  /// \brief Return the schema for this table, return NotFoundError if not found
  Result<std::shared_ptr<Schema>> schema() const;

  /// \brief Return a map of schema for this table
  /// \note This method is **not** thread-safe in the current implementation.
  const std::shared_ptr<std::unordered_map<int32_t, std::shared_ptr<Schema>>>& schemas()
      const;

  /// \brief Return the partition spec for this table, return NotFoundError if not found
  Result<std::shared_ptr<PartitionSpec>> spec() const;

  /// \brief Return a map of partition specs for this table
  /// \note This method is **not** thread-safe in the current implementation.
  const std::shared_ptr<std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>>&
  specs() const;

  /// \brief Return the sort order for this table, return NotFoundError if not found
  Result<std::shared_ptr<SortOrder>> sort_order() const;

  /// \brief Return a map of sort order IDs to sort orders for this table
  /// \note This method is **not** thread-safe in the current implementation.
  const std::shared_ptr<std::unordered_map<int32_t, std::shared_ptr<SortOrder>>>&
  sort_orders() const;

  /// \brief Return a map of string properties for this table
  const std::unordered_map<std::string, std::string>& properties() const;

  /// \brief Return the table's base location
  const std::string& location() const;

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
  ///
  /// \return a vector of history entries
  const std::vector<SnapshotLogEntry>& history() const;

  /// \brief Returns a FileIO to read and write table data and metadata files
  const std::shared_ptr<FileIO>& io() const;

 private:
  const TableIdentifier identifier_;
  std::shared_ptr<TableMetadata> metadata_;
  const std::string metadata_location_;
  std::shared_ptr<FileIO> io_;
  std::shared_ptr<Catalog> catalog_;

  // Cache lazy-initialized maps.
  mutable std::shared_ptr<std::unordered_map<int32_t, std::shared_ptr<Schema>>>
      schemas_map_;
  mutable std::shared_ptr<std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>>
      partition_spec_map_;
  mutable std::shared_ptr<std::unordered_map<int32_t, std::shared_ptr<SortOrder>>>
      sort_orders_map_;
};

}  // namespace iceberg
