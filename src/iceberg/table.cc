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

#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/sort_order.h"
#include "iceberg/table_metadata.h"

namespace iceberg {

const std::string& Table::uuid() const { return metadata_->table_uuid; }

const std::shared_ptr<Schema>& Table::schema() const {
  if (!schema_) {
    const static std::shared_ptr<Schema> kEmptySchema =
        std::make_shared<Schema>(std::vector<SchemaField>{});
    auto schema = metadata_->Schema();
    if (schema.has_value()) {
      schema_ = schema.value();
    } else {
      schema_ = kEmptySchema;
    }
  }
  return schema_;
}

const std::unordered_map<int32_t, std::shared_ptr<Schema>>& Table::schemas() const {
  std::call_once(init_schemas_once_, [this]() {
    for (const auto& schema : metadata_->schemas) {
      if (schema->schema_id()) {
        schemas_map_.emplace(schema->schema_id().value(), schema);
      }
    }
  });
  return schemas_map_;
}

const std::shared_ptr<PartitionSpec>& Table::spec() const {
  std::call_once(init_partition_spec_once_, [this]() {
    auto partition_spec = metadata_->PartitionSpec();
    if (partition_spec.has_value()) {
      partition_spec_ = partition_spec.value();
    }
  });
  return partition_spec_;
}

const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>& Table::specs() const {
  std::call_once(init_partition_specs_once_, [this]() {
    for (const auto& spec : metadata_->partition_specs) {
      partition_spec_map_[spec->spec_id()] = spec;
    }
  });
  return partition_spec_map_;
}

const std::shared_ptr<SortOrder>& Table::sort_order() const {
  std::call_once(init_sort_order_once_, [this]() {
    auto sort_order = metadata_->SortOrder();
    if (sort_order.has_value()) {
      sort_order_ = sort_order.value();
    }
  });
  return sort_order_;
}

const std::unordered_map<int32_t, std::shared_ptr<SortOrder>>& Table::sort_orders()
    const {
  std::call_once(init_sort_orders_once_, [this]() {
    for (const auto& order : metadata_->sort_orders) {
      sort_orders_map_[order->order_id()] = order;
    }
  });
  return sort_orders_map_;
}

const std::unordered_map<std::string, std::string>& Table::properties() const {
  return metadata_->properties;
}

const std::string& Table::location() const { return metadata_->location; }

std::shared_ptr<Snapshot> Table::CurrentSnapshot() const {
  std::call_once(init_snapshot_once_, [this]() {
    auto snapshot = metadata_->Snapshot();
    if (snapshot.has_value()) {
      current_snapshot_ = snapshot.value();
    }
  });
  return current_snapshot_;
}

std::shared_ptr<Snapshot> Table::SnapshotById(int64_t snapshot_id) const {
  auto iter = std::ranges::find_if(metadata_->snapshots,
                                   [this, &snapshot_id](const auto& snapshot) {
                                     return snapshot->snapshot_id == snapshot_id;
                                   });
  if (iter == metadata_->snapshots.end()) {
    return nullptr;
  }
  return *iter;
}

const std::vector<std::shared_ptr<Snapshot>>& Table::snapshots() const {
  return metadata_->snapshots;
}

const std::vector<SnapshotLogEntry>& Table::history() const {
  return metadata_->snapshot_log;
}

}  // namespace iceberg
