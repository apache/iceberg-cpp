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

#include "iceberg/table_metadata.h"

#include <format>
#include <ranges>
#include <string>

#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/sort_order.h"
namespace iceberg {

std::string ToString(const SnapshotLogEntry& entry) {
  return std::format("SnapshotLogEntry[timestampMillis={},snapshotId={}]",
                     entry.timestamp_ms, entry.snapshot_id);
}

std::string ToString(const MetadataLogEntry& entry) {
  return std::format("MetadataLogEntry[timestampMillis={},file={}]", entry.timestamp_ms,
                     entry.metadata_file);
}

const std::shared_ptr<Schema>& TableMetadata::Schema() const {
  static const std::shared_ptr<::iceberg::Schema> empty_schema = nullptr;

  auto iter = std::ranges::find_if(schemas, [this](const auto& schema) {
    return schema->schema_id() == current_schema_id;
  });
  return iter == schemas.end() ? empty_schema : *iter;
}

const std::shared_ptr<PartitionSpec>& TableMetadata::PartitionSpec() const {
  static const std::shared_ptr<iceberg::PartitionSpec> empty_spec = nullptr;

  auto iter = std::ranges::find_if(partition_specs, [this](const auto& spec) {
    return spec->spec_id() == default_spec_id;
  });
  return iter == partition_specs.end() ? empty_spec : *iter;
}

const std::shared_ptr<SortOrder>& TableMetadata::SortOrder() const {
  static const std::shared_ptr<iceberg::SortOrder> empty_order = nullptr;

  auto iter = std::ranges::find_if(sort_orders, [this](const auto& order) {
    return order->order_id() == default_sort_order_id;
  });
  return iter == sort_orders.end() ? empty_order : *iter;
}

Result<TimePointMs> TimePointMsFromUnixMs(int64_t unix_ms) {
  return TimePointMs{std::chrono::milliseconds(unix_ms)};
}

int64_t UnixMsFromTimePointMs(const TimePointMs& time_point_ms) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             time_point_ms.time_since_epoch())
      .count();
}

}  // namespace iceberg
