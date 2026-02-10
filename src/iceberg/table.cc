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

#include "iceberg/table.h"

#include <memory>

#include "iceberg/catalog.h"
#include "iceberg/location_provider.h"
#include "iceberg/partition_spec.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/sort_order.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"
#include "iceberg/table_scan.h"
#include "iceberg/transaction.h"
#include "iceberg/update/expire_snapshots.h"
#include "iceberg/update/snapshot_manager.h"
#include "iceberg/update/update_partition_spec.h"
#include "iceberg/update/update_partition_statistics.h"
#include "iceberg/update/update_properties.h"
#include "iceberg/update/update_schema.h"
#include "iceberg/update/update_statistics.h"
#include "iceberg/util/macros.h"

namespace iceberg {

Result<std::shared_ptr<Table>> Table::Make(TableIdentifier identifier,
                                           std::shared_ptr<TableMetadata> metadata,
                                           std::string metadata_location,
                                           std::shared_ptr<FileIO> io,
                                           std::shared_ptr<Catalog> catalog) {
  if (metadata == nullptr) [[unlikely]] {
    return InvalidArgument("Metadata cannot be null");
  }
  if (metadata_location.empty()) [[unlikely]] {
    return InvalidArgument("Metadata location cannot be empty");
  }
  if (io == nullptr) [[unlikely]] {
    return InvalidArgument("FileIO cannot be null");
  }
  if (catalog == nullptr) [[unlikely]] {
    return InvalidArgument("Catalog cannot be null");
  }
  return std::shared_ptr<Table>(new Table(std::move(identifier), std::move(metadata),
                                          std::move(metadata_location), std::move(io),
                                          std::move(catalog)));
}

Table::~Table() = default;

Table::Table(TableIdentifier identifier, std::shared_ptr<TableMetadata> metadata,
             std::string metadata_location, std::shared_ptr<FileIO> io,
             std::shared_ptr<Catalog> catalog)
    : identifier_(std::move(identifier)),
      metadata_(std::move(metadata)),
      metadata_location_(std::move(metadata_location)),
      io_(std::move(io)),
      catalog_(std::move(catalog)),
      metadata_cache_(std::make_unique<TableMetadataCache>(metadata_.get())) {}

const std::string& Table::uuid() const { return metadata_->table_uuid; }

Status Table::Refresh() {
  ICEBERG_ASSIGN_OR_RAISE(auto refreshed_table, catalog_->LoadTable(identifier_));
  if (metadata_location_ != refreshed_table->metadata_file_location()) {
    metadata_ = std::move(refreshed_table->metadata_);
    io_ = std::move(refreshed_table->io_);
    metadata_cache_ = std::make_unique<TableMetadataCache>(metadata_.get());
  }
  return {};
}

Result<std::shared_ptr<Schema>> Table::schema() const { return metadata_->Schema(); }

Result<std::reference_wrapper<const std::unordered_map<int32_t, std::shared_ptr<Schema>>>>
Table::schemas() const {
  return metadata_cache_->GetSchemasById();
}

Result<std::shared_ptr<PartitionSpec>> Table::spec() const {
  return metadata_->PartitionSpec();
}

Result<std::reference_wrapper<
    const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>>>
Table::specs() const {
  return metadata_cache_->GetPartitionSpecsById();
}

Result<std::shared_ptr<SortOrder>> Table::sort_order() const {
  return metadata_->SortOrder();
}

Result<
    std::reference_wrapper<const std::unordered_map<int32_t, std::shared_ptr<SortOrder>>>>
Table::sort_orders() const {
  return metadata_cache_->GetSortOrdersById();
}

const TableProperties& Table::properties() const { return metadata_->properties; }

std::string_view Table::metadata_file_location() const { return metadata_location_; }

std::string_view Table::location() const { return metadata_->location; }

TimePointMs Table::last_updated_ms() const { return metadata_->last_updated_ms; }

Result<std::shared_ptr<Snapshot>> Table::current_snapshot() const {
  return metadata_->Snapshot();
}

Result<std::shared_ptr<Snapshot>> Table::SnapshotById(int64_t snapshot_id) const {
  return metadata_->SnapshotById(snapshot_id);
}

const std::vector<std::shared_ptr<Snapshot>>& Table::snapshots() const {
  return metadata_->snapshots;
}

const std::vector<SnapshotLogEntry>& Table::history() const {
  return metadata_->snapshot_log;
}

const std::shared_ptr<FileIO>& Table::io() const { return io_; }

const std::shared_ptr<TableMetadata>& Table::metadata() const { return metadata_; }

const std::shared_ptr<Catalog>& Table::catalog() const { return catalog_; }

Result<std::unique_ptr<LocationProvider>> Table::location_provider() const {
  return LocationProvider::Make(metadata_->location, metadata_->properties);
}

Result<std::unique_ptr<TableScanBuilder>> Table::NewScan() const {
  return TableScanBuilder::Make(metadata_, io_);
}

Result<std::shared_ptr<Transaction>> Table::NewTransaction() {
  // Create a brand new transaction object for the table. Users are expected to commit the
  // transaction manually.
  return Transaction::Make(shared_from_this(), Transaction::Kind::kUpdate,
                           /*auto_commit=*/false);
}

Result<std::shared_ptr<UpdatePartitionSpec>> Table::NewUpdatePartitionSpec() {
  ICEBERG_ASSIGN_OR_RAISE(
      auto transaction, Transaction::Make(shared_from_this(), Transaction::Kind::kUpdate,
                                          /*auto_commit=*/true));
  return transaction->NewUpdatePartitionSpec();
}

Result<std::shared_ptr<UpdateProperties>> Table::NewUpdateProperties() {
  ICEBERG_ASSIGN_OR_RAISE(
      auto transaction, Transaction::Make(shared_from_this(), Transaction::Kind::kUpdate,
                                          /*auto_commit=*/true));
  return transaction->NewUpdateProperties();
}

Result<std::shared_ptr<UpdateSortOrder>> Table::NewUpdateSortOrder() {
  ICEBERG_ASSIGN_OR_RAISE(
      auto transaction, Transaction::Make(shared_from_this(), Transaction::Kind::kUpdate,
                                          /*auto_commit=*/true));
  return transaction->NewUpdateSortOrder();
}

Result<std::shared_ptr<UpdateSchema>> Table::NewUpdateSchema() {
  ICEBERG_ASSIGN_OR_RAISE(
      auto transaction, Transaction::Make(shared_from_this(), Transaction::Kind::kUpdate,
                                          /*auto_commit=*/true));
  return transaction->NewUpdateSchema();
}

Result<std::shared_ptr<ExpireSnapshots>> Table::NewExpireSnapshots() {
  ICEBERG_ASSIGN_OR_RAISE(
      auto transaction, Transaction::Make(shared_from_this(), Transaction::Kind::kUpdate,
                                          /*auto_commit=*/true));
  return transaction->NewExpireSnapshots();
}

Result<std::shared_ptr<UpdateLocation>> Table::NewUpdateLocation() {
  ICEBERG_ASSIGN_OR_RAISE(
      auto transaction, Transaction::Make(shared_from_this(), Transaction::Kind::kUpdate,
                                          /*auto_commit=*/true));
  return transaction->NewUpdateLocation();
}

Result<std::shared_ptr<FastAppend>> Table::NewFastAppend() {
  ICEBERG_ASSIGN_OR_RAISE(
      auto transaction, Transaction::Make(shared_from_this(), Transaction::Kind::kUpdate,
                                          /*auto_commit=*/true));
  return transaction->NewFastAppend();
}

Result<std::shared_ptr<UpdateStatistics>> Table::NewUpdateStatistics() {
  ICEBERG_ASSIGN_OR_RAISE(
      auto transaction, Transaction::Make(shared_from_this(), Transaction::Kind::kUpdate,
                                          /*auto_commit=*/true));
  return transaction->NewUpdateStatistics();
}

Result<std::shared_ptr<UpdatePartitionStatistics>> Table::NewUpdatePartitionStatistics() {
  ICEBERG_ASSIGN_OR_RAISE(
      auto transaction, Transaction::Make(shared_from_this(), Transaction::Kind::kUpdate,
                                          /*auto_commit=*/true));
  return transaction->NewUpdatePartitionStatistics();
}

Result<std::shared_ptr<SnapshotManager>> Table::NewSnapshotManager() {
  ICEBERG_ASSIGN_OR_RAISE(
      auto transaction, Transaction::Make(shared_from_this(), Transaction::Kind::kUpdate,
                                          /*auto_commit=*/false));
  return SnapshotManager::Make(std::move(transaction));
}

Result<std::shared_ptr<StagedTable>> StagedTable::Make(
    TableIdentifier identifier, std::shared_ptr<TableMetadata> metadata,
    std::string metadata_location, std::shared_ptr<FileIO> io,
    std::shared_ptr<Catalog> catalog) {
  if (metadata == nullptr) [[unlikely]] {
    return InvalidArgument("Metadata cannot be null");
  }
  if (io == nullptr) [[unlikely]] {
    return InvalidArgument("FileIO cannot be null");
  }
  if (catalog == nullptr) [[unlikely]] {
    return InvalidArgument("Catalog cannot be null");
  }
  return std::shared_ptr<StagedTable>(
      new StagedTable(std::move(identifier), std::move(metadata),
                      std::move(metadata_location), std::move(io), std::move(catalog)));
}

StagedTable::~StagedTable() = default;

Result<std::unique_ptr<TableScanBuilder>> StagedTable::NewScan() const {
  return NotSupported("Cannot scan a staged table");
}

Result<std::shared_ptr<StaticTable>> StaticTable::Make(
    TableIdentifier identifier, std::shared_ptr<TableMetadata> metadata,
    std::string metadata_location, std::shared_ptr<FileIO> io) {
  if (metadata == nullptr) [[unlikely]] {
    return InvalidArgument("Metadata cannot be null");
  }
  if (io == nullptr) [[unlikely]] {
    return InvalidArgument("FileIO cannot be null");
  }
  return std::shared_ptr<StaticTable>(
      new StaticTable(std::move(identifier), std::move(metadata),
                      std::move(metadata_location), std::move(io), /*catalog=*/nullptr));
}

StaticTable::~StaticTable() = default;

Status StaticTable::Refresh() { return NotSupported("Cannot refresh a static table"); }

Result<std::shared_ptr<Transaction>> StaticTable::NewTransaction() {
  return NotSupported("Cannot create a transaction for a static table");
}

Result<std::shared_ptr<UpdateProperties>> StaticTable::NewUpdateProperties() {
  return NotSupported("Cannot create an update properties for a static table");
}

Result<std::shared_ptr<UpdateSchema>> StaticTable::NewUpdateSchema() {
  return NotSupported("Cannot create an update schema for a static table");
}

}  // namespace iceberg
