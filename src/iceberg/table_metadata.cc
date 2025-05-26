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
#include <string>

#include <nlohmann/json.hpp>

#include "iceberg/file_io.h"
#include "iceberg/json_internal.h"
#include "iceberg/partition_spec.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/sort_order.h"
#include "iceberg/util/gzip_internal.h"
#include "iceberg/util/macros.h"

namespace iceberg {

std::string ToString(const SnapshotLogEntry& entry) {
  return std::format("SnapshotLogEntry[timestampMillis={},snapshotId={}]",
                     entry.timestamp_ms, entry.snapshot_id);
}

std::string ToString(const MetadataLogEntry& entry) {
  return std::format("MetadataLogEntry[timestampMillis={},file={}]", entry.timestamp_ms,
                     entry.metadata_file);
}

Result<std::shared_ptr<Schema>> TableMetadata::Schema() const {
  auto iter = std::ranges::find_if(schemas, [this](const auto& schema) {
    return schema->schema_id() == current_schema_id;
  });
  if (iter == schemas.end()) {
    return NotFound("Current schema is not found");
  }
  return *iter;
}

Result<std::shared_ptr<PartitionSpec>> TableMetadata::PartitionSpec() const {
  auto iter = std::ranges::find_if(partition_specs, [this](const auto& spec) {
    return spec->spec_id() == default_spec_id;
  });
  if (iter == partition_specs.end()) {
    return NotFound("Default partition spec is not found");
  }
  return *iter;
}

Result<std::shared_ptr<SortOrder>> TableMetadata::SortOrder() const {
  auto iter = std::ranges::find_if(sort_orders, [this](const auto& order) {
    return order->order_id() == default_sort_order_id;
  });
  if (iter == sort_orders.end()) {
    return NotFound("Default sort order is not found");
  }
  return *iter;
}

namespace {

template <typename T>
bool SharedPtrVectorEquals(const std::vector<std::shared_ptr<T>>& lhs,
                           const std::vector<std::shared_ptr<T>>& rhs) {
  if (lhs.size() != rhs.size()) {
    return false;
  }
  for (size_t i = 0; i < lhs.size(); ++i) {
    if (*lhs[i] != *rhs[i]) {
      return false;
    }
  }
  return true;
}

bool SnapshotRefEquals(
    const std::unordered_map<std::string, std::shared_ptr<SnapshotRef>>& lhs,
    const std::unordered_map<std::string, std::shared_ptr<SnapshotRef>>& rhs) {
  if (lhs.size() != rhs.size()) {
    return false;
  }
  for (const auto& [key, value] : lhs) {
    auto iter = rhs.find(key);
    if (iter == rhs.end()) {
      return false;
    }
    if (*iter->second != *value) {
      return false;
    }
  }
  return true;
}

}  // namespace

bool operator==(const TableMetadata& lhs, const TableMetadata& rhs) {
  return lhs.format_version == rhs.format_version && lhs.table_uuid == rhs.table_uuid &&
         lhs.location == rhs.location &&
         lhs.last_sequence_number == rhs.last_sequence_number &&
         lhs.last_updated_ms == rhs.last_updated_ms &&
         lhs.last_column_id == rhs.last_column_id &&
         lhs.current_schema_id == rhs.current_schema_id &&
         SharedPtrVectorEquals(lhs.schemas, rhs.schemas) &&
         lhs.default_spec_id == rhs.default_spec_id &&
         lhs.last_partition_id == rhs.last_partition_id &&
         lhs.properties == rhs.properties &&
         lhs.current_snapshot_id == rhs.current_snapshot_id &&
         SharedPtrVectorEquals(lhs.snapshots, rhs.snapshots) &&
         lhs.snapshot_log == rhs.snapshot_log && lhs.metadata_log == rhs.metadata_log &&
         SharedPtrVectorEquals(lhs.sort_orders, rhs.sort_orders) &&
         lhs.default_sort_order_id == rhs.default_sort_order_id &&
         SnapshotRefEquals(lhs.refs, rhs.refs) &&
         SharedPtrVectorEquals(lhs.statistics, rhs.statistics) &&
         SharedPtrVectorEquals(lhs.partition_statistics, rhs.partition_statistics) &&
         lhs.next_row_id == rhs.next_row_id;
}

Result<MetadataFileCodecType> TableMetadataUtil::CodecFromFileName(
    std::string_view file_name) {
  if (file_name.find(".metadata.json") == std::string::npos) {
    return InvalidArgument("{} is not a valid metadata file", file_name);
  }

  // We have to be backward-compatible with .metadata.json.gz files
  if (file_name.ends_with(".metadata.json.gz")) {
    return MetadataFileCodecType::kGzip;
  }

  std::string_view file_name_without_suffix =
      file_name.substr(0, file_name.find_last_of(".metadata.json"));
  if (file_name_without_suffix.ends_with(".gz")) {
    return MetadataFileCodecType::kGzip;
  }
  return MetadataFileCodecType::kNone;
}

Result<std::unique_ptr<TableMetadata>> TableMetadataUtil::Read(
    FileIO& io, const std::string& location, std::optional<size_t> length) {
  ICEBERG_ASSIGN_OR_RAISE(auto codec_type, CodecFromFileName(location));

  ICEBERG_ASSIGN_OR_RAISE(auto content, io.ReadFile(location, length));
  if (codec_type == MetadataFileCodecType::kGzip) {
    auto gzip_decompressor = std::make_unique<GZipDecompressor>();
    ICEBERG_RETURN_UNEXPECTED(gzip_decompressor->Init());
    auto result = gzip_decompressor->Decompress(content);
    ICEBERG_RETURN_UNEXPECTED(result);
    content = result.value();
  }

  ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(content));
  return TableMetadataFromJson(json);
}

Status TableMetadataUtil::Write(FileIO& io, const std::string& location,
                                const TableMetadata& metadata) {
  auto json = ToJson(metadata);
  ICEBERG_ASSIGN_OR_RAISE(auto json_string, ToJsonString(json));
  return io.WriteFile(location, json_string);
}

}  // namespace iceberg
