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

#include <algorithm>

#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/sort_order.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_scan.h"

namespace iceberg {

const std::string& Table::uuid() const { return metadata_->table_uuid; }

Result<std::shared_ptr<Schema>> Table::schema() const { return metadata_->Schema(); }

const std::shared_ptr<std::unordered_map<int32_t, std::shared_ptr<Schema>>>&
Table::schemas() const {
  if (!schemas_map_) {
    schemas_map_ =
        std::make_shared<std::unordered_map<int32_t, std::shared_ptr<Schema>>>();
    for (const auto& schema : metadata_->schemas) {
      if (schema->schema_id()) {
        schemas_map_->emplace(schema->schema_id().value(), schema);
      }
    }
  }
  return schemas_map_;
}

Result<std::shared_ptr<PartitionSpec>> Table::spec() const {
  return metadata_->PartitionSpec();
}

const std::shared_ptr<std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>>&
Table::specs() const {
  if (!partition_spec_map_) {
    partition_spec_map_ =
        std::make_shared<std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>>();
    for (const auto& spec : metadata_->partition_specs) {
      partition_spec_map_->emplace(spec->spec_id(), spec);
    }
  }
  return partition_spec_map_;
}

Result<std::shared_ptr<SortOrder>> Table::sort_order() const {
  return metadata_->SortOrder();
}

const std::shared_ptr<std::unordered_map<int32_t, std::shared_ptr<SortOrder>>>&
Table::sort_orders() const {
  if (!sort_orders_map_) {
    sort_orders_map_ =
        std::make_shared<std::unordered_map<int32_t, std::shared_ptr<SortOrder>>>();
    for (const auto& order : metadata_->sort_orders) {
      sort_orders_map_->emplace(order->order_id(), order);
    }
  }
  return sort_orders_map_;
}

const std::unordered_map<std::string, std::string>& Table::properties() const {
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

const std::shared_ptr<FileIO>& Table::io() const { return io_; }

std::unique_ptr<TableScanBuilder> Table::NewScan() const {
  return std::make_unique<TableScanBuilder>(metadata_, io_);
}

}  // namespace iceberg
