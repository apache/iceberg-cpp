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

#include <algorithm>
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
  return SchemaById(current_schema_id);
}

Result<std::shared_ptr<Schema>> TableMetadata::SchemaById(
    const std::optional<int32_t>& schema_id) const {
  auto iter = std::ranges::find_if(schemas, [schema_id](const auto& schema) {
    return schema->schema_id() == schema_id;
  });
  if (iter == schemas.end()) {
    return NotFound("Schema with ID {} is not found", schema_id.value_or(-1));
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

Result<std::shared_ptr<Snapshot>> TableMetadata::Snapshot() const {
  return SnapshotById(current_snapshot_id);
}

Result<std::shared_ptr<Snapshot>> TableMetadata::SnapshotById(int64_t snapshot_id) const {
  auto iter = std::ranges::find_if(snapshots, [snapshot_id](const auto& snapshot) {
    return snapshot->snapshot_id == snapshot_id;
  });
  if (iter == snapshots.end()) {
    return NotFound("Snapshot with ID {} is not found", snapshot_id);
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

// TableMetadataBuilder implementation

struct TableMetadataBuilder::Impl {
  // Base metadata (if building from existing metadata)
  std::shared_ptr<const TableMetadata> base;

  // Mutable fields that will be used to build the final TableMetadata
  int8_t format_version;
  std::string table_uuid;
  std::string location;
  int64_t last_sequence_number;
  TimePointMs last_updated_ms;
  int32_t last_column_id;
  std::vector<std::shared_ptr<Schema>> schemas;
  std::optional<int32_t> current_schema_id;
  std::vector<std::shared_ptr<PartitionSpec>> partition_specs;
  int32_t default_spec_id;
  int32_t last_partition_id;
  std::unordered_map<std::string, std::string> properties;
  int64_t current_snapshot_id;
  std::vector<std::shared_ptr<Snapshot>> snapshots;
  std::vector<SnapshotLogEntry> snapshot_log;
  std::vector<MetadataLogEntry> metadata_log;
  std::vector<std::shared_ptr<SortOrder>> sort_orders;
  int32_t default_sort_order_id;
  std::unordered_map<std::string, std::shared_ptr<SnapshotRef>> refs;
  std::vector<std::shared_ptr<StatisticsFile>> statistics;
  std::vector<std::shared_ptr<PartitionStatisticsFile>> partition_statistics;
  int64_t next_row_id;

  // List of changes (MetadataUpdate objects)
  std::vector<std::unique_ptr<MetadataUpdate>> changes;

  explicit Impl(int8_t fmt_version)
      : format_version(fmt_version),
        last_sequence_number(TableMetadata::kInitialSequenceNumber),
        last_updated_ms(std::chrono::milliseconds(0)),
        last_column_id(0),
        default_spec_id(0),
        last_partition_id(0),
        current_snapshot_id(-1),
        default_sort_order_id(0),
        next_row_id(TableMetadata::kInitialRowId) {}

  explicit Impl(const std::shared_ptr<const TableMetadata>& base_metadata)
      : base(base_metadata),
        format_version(base_metadata->format_version),
        table_uuid(base_metadata->table_uuid),
        location(base_metadata->location),
        last_sequence_number(base_metadata->last_sequence_number),
        last_updated_ms(base_metadata->last_updated_ms),
        last_column_id(base_metadata->last_column_id),
        schemas(base_metadata->schemas),
        current_schema_id(base_metadata->current_schema_id),
        partition_specs(base_metadata->partition_specs),
        default_spec_id(base_metadata->default_spec_id),
        last_partition_id(base_metadata->last_partition_id),
        properties(base_metadata->properties),
        current_snapshot_id(base_metadata->current_snapshot_id),
        snapshots(base_metadata->snapshots),
        snapshot_log(base_metadata->snapshot_log),
        metadata_log(base_metadata->metadata_log),
        sort_orders(base_metadata->sort_orders),
        default_sort_order_id(base_metadata->default_sort_order_id),
        refs(base_metadata->refs),
        statistics(base_metadata->statistics),
        partition_statistics(base_metadata->partition_statistics),
        next_row_id(base_metadata->next_row_id) {}
};

TableMetadataBuilder::TableMetadataBuilder(int8_t format_version)
    : impl_(std::make_unique<Impl>(format_version)) {}

TableMetadataBuilder::TableMetadataBuilder(std::shared_ptr<const TableMetadata> base)
    : impl_(std::make_unique<Impl>(base)) {}

TableMetadataBuilder::~TableMetadataBuilder() = default;

TableMetadataBuilder::TableMetadataBuilder(TableMetadataBuilder&&) noexcept = default;

TableMetadataBuilder& TableMetadataBuilder::operator=(TableMetadataBuilder&&) noexcept =
    default;

TableMetadataBuilder TableMetadataBuilder::BuildFromEmpty(int8_t format_version) {
  return TableMetadataBuilder(format_version);
}

TableMetadataBuilder TableMetadataBuilder::BuildFrom(
    const std::shared_ptr<const TableMetadata>& base) {
  return TableMetadataBuilder(base);
}

TableMetadataBuilder& TableMetadataBuilder::AssignUUID() {
  // TODO(gty404): Implement
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::AssignUUID(const std::string& uuid) {
  // TODO(gty404): Implement
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::UpgradeFormatVersion(
    int8_t new_format_version) {
  // TODO(gty404): Implement
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::SetCurrentSchema(
    std::shared_ptr<Schema> schema, int32_t new_last_column_id) {
  // TODO(gty404): Implement
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::SetCurrentSchema(int32_t schema_id) {
  // TODO(gty404): Implement
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::AddSchema(std::shared_ptr<Schema> schema) {
  // TODO(gty404): Implement
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::SetDefaultPartitionSpec(
    std::shared_ptr<PartitionSpec> spec) {
  // TODO(gty404): Implement
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::SetDefaultPartitionSpec(int32_t spec_id) {
  // TODO(gty404): Implement
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::AddPartitionSpec(
    std::shared_ptr<PartitionSpec> spec) {
  // TODO(gty404): Implement
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::RemovePartitionSpecs(
    const std::vector<int32_t>& spec_ids) {
  // TODO(gty404): Implement
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::RemoveSchemas(
    const std::vector<int32_t>& schema_ids) {
  // TODO(gty404): Implement
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::SetDefaultSortOrder(
    std::shared_ptr<SortOrder> order) {
  // TODO(gty404): Implement
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::SetDefaultSortOrder(int32_t order_id) {
  // TODO(gty404): Implement
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::AddSortOrder(
    std::shared_ptr<SortOrder> order) {
  // TODO(gty404): Implement
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::AddSnapshot(
    std::shared_ptr<Snapshot> snapshot) {
  // TODO(gty404): Implement
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::SetBranchSnapshot(int64_t snapshot_id,
                                                              const std::string& branch) {
  // TODO(gty404): Implement
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::SetRef(const std::string& name,
                                                   std::shared_ptr<SnapshotRef> ref) {
  // TODO(gty404): Implement
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::RemoveRef(const std::string& name) {
  // TODO(gty404): Implement
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::RemoveSnapshots(
    const std::vector<int64_t>& snapshot_ids) {
  // TODO(gty404): Implement
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::SetProperties(
    const std::unordered_map<std::string, std::string>& updated) {
  // TODO(gty404): Implement
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::RemoveProperties(
    const std::vector<std::string>& removed) {
  // TODO(gty404): Implement
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::SetLocation(const std::string& location) {
  // TODO(gty404): Implement
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::DiscardChanges() {
  // TODO(gty404): Implement
  return *this;
}

Result<std::unique_ptr<TableMetadata>> TableMetadataBuilder::Build() {
  return NotImplemented("TableMetadataBuilder::Build is not implemented");
}

}  // namespace iceberg
