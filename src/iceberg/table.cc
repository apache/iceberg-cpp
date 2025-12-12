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

#include "iceberg/catalog.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/sort_order.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"
#include "iceberg/table_scan.h"
#include "iceberg/update/update_properties.h"
#include "iceberg/util/macros.h"

namespace iceberg {

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
  if (!catalog_) {
    return NotSupported("Refresh is not supported for table without a catalog");
  }

  ICEBERG_ASSIGN_OR_RAISE(auto refreshed_table, catalog_->LoadTable(identifier_));
  if (metadata_location_ != refreshed_table->metadata_location_) {
    metadata_ = std::move(refreshed_table->metadata_);
    metadata_location_ = std::move(refreshed_table->metadata_location_);
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

const std::shared_ptr<TableProperties>& Table::properties() const {
  return metadata_->properties;
}

const std::string& Table::location() const { return metadata_->location; }

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

std::unique_ptr<UpdateProperties> Table::UpdateProperties() const {
  return std::make_unique<iceberg::UpdateProperties>(identifier_, catalog_, metadata_);
}

std::unique_ptr<Transaction> Table::NewTransaction() const {
  throw NotImplemented("Table::NewTransaction is not implemented");
}

const std::shared_ptr<FileIO>& Table::io() const { return io_; }

std::unique_ptr<TableScanBuilder> Table::NewScan() const {
  return std::make_unique<TableScanBuilder>(metadata_, io_);
}

}  // namespace iceberg
