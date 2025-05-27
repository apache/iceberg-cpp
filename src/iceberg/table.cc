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

#include <iostream>

#include "iceberg/exception.h"
#include "iceberg/partition_spec.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/sort_order.h"
#include "iceberg/table_metadata.h"

namespace iceberg {

BaseTable::BaseTable(std::string name, std::shared_ptr<TableMetadata> metadata)
    : name_(std::move(name)), metadata_(std::move(metadata)) {
  if (!metadata_) {
    throw std::invalid_argument("Table metadata cannot be null");
  }
}

void BaseTable::InitSchema() const {
  for (const auto& schema : metadata_->schemas) {
    if (schema->schema_id() != std::nullopt) {
      schemas_map_.emplace(schema->schema_id().value(), schema);
      if (schema->schema_id().value() == metadata_->current_schema_id) {
        schema_ = schema;
      }
    }
  }
  // compatible with V1 table schema
  if (!schema_ && metadata_->schemas.size() == 1UL) {
    schema_ = metadata_->schemas.front();
  }
}

void BaseTable::InitPartitionSpec() const {
  for (const auto& spec : metadata_->partition_specs) {
    partition_spec_map_[spec->spec_id()] = spec;
    if (spec->spec_id() == metadata_->default_spec_id) {
      partition_spec_ = spec;
    }
  }
}

void BaseTable::InitSortOrder() const {
  for (const auto& order : metadata_->sort_orders) {
    sort_orders_map_[order->order_id()] = order;
    if (order->order_id() == metadata_->default_sort_order_id) {
      sort_order_ = order;
    }
  }
}

void BaseTable::InitSnapshot() const {
  auto snapshots = metadata_->snapshots;
  for (const auto& snapshot : snapshots) {
    if (snapshot->snapshot_id == metadata_->current_snapshot_id) {
      current_snapshot_ = snapshot;
    }
    snapshots_map_[snapshot->snapshot_id] = snapshot;
  }
}

const std::string& BaseTable::uuid() const { return metadata_->table_uuid; }

const std::shared_ptr<Schema>& BaseTable::schema() const {
  std::call_once(init_schema_once_, [this]() { InitSchema(); });
  if (!schema_) {
    throw IcebergError("Current schema is not defined for this table");
  }
  return schema_;
}

const std::unordered_map<int32_t, std::shared_ptr<Schema>>& BaseTable::schemas() const {
  std::call_once(init_schema_once_, [this]() { InitSchema(); });
  return schemas_map_;
}

const std::shared_ptr<PartitionSpec>& BaseTable::spec() const {
  std::call_once(init_partition_spec_once_, [this]() { InitPartitionSpec(); });
  return partition_spec_;
}

const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>& BaseTable::specs()
    const {
  std::call_once(init_partition_spec_once_, [this]() { InitPartitionSpec(); });
  return partition_spec_map_;
}

const std::shared_ptr<SortOrder>& BaseTable::sort_order() const {
  std::call_once(init_sort_order_once_, [this]() { InitSortOrder(); });
  return sort_order_;
}

const std::unordered_map<int32_t, std::shared_ptr<SortOrder>>& BaseTable::sort_orders()
    const {
  std::call_once(init_sort_order_once_, [this]() { InitSortOrder(); });
  return sort_orders_map_;
}

const std::unordered_map<std::string, std::string>& BaseTable::properties() const {
  return metadata_->properties;
}

const std::string& BaseTable::location() const { return metadata_->location; }

const std::shared_ptr<Snapshot>& BaseTable::current_snapshot() const {
  std::call_once(init_snapshot_once_, [this]() { InitSnapshot(); });
  if (!current_snapshot_) {
    throw IcebergError("Current snapshot is not defined for this table");
  }
  return current_snapshot_;
}

Result<std::shared_ptr<Snapshot>> BaseTable::snapshot(int64_t snapshot_id) const {
  std::call_once(init_snapshot_once_, [this]() { InitSnapshot(); });
  auto iter = snapshots_map_.find(snapshot_id);
  if (iter == snapshots_map_.end()) {
    return NotFound("Snapshot with ID {} not found", snapshot_id);
  }
  return iter->second;
}

const std::vector<std::shared_ptr<Snapshot>>& BaseTable::snapshots() const {
  return metadata_->snapshots;
}

const std::vector<std::shared_ptr<HistoryEntry>>& BaseTable::history() const {
  // TODO(lishuxu): Implement history retrieval
  throw IcebergError("history is not supported for BaseTable now");
}

Status StaticTable::Refresh() {
  throw IcebergError("Refresh is not supported for StaticTable");
}

std::unique_ptr<TableScan> StaticTable::NewScan() const {
  throw IcebergError("NewScan is not supported for StaticTable");
}

std::shared_ptr<AppendFiles> StaticTable::NewAppend() {
  throw IcebergError("NewAppend is not supported for StaticTable");
}

std::unique_ptr<Transaction> StaticTable::NewTransaction() {
  throw IcebergError("NewTransaction is not supported for StaticTable");
}

std::unique_ptr<LocationProvider> StaticTable::location_provider() const {
  throw IcebergError("location_provider is not supported for StaticTable");
}

}  // namespace iceberg
