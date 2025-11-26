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
#include <chrono>
#include <format>
#include <string>

#include <nlohmann/json.hpp>

#include "iceberg/exception.h"
#include "iceberg/file_io.h"
#include "iceberg/json_internal.h"
#include "iceberg/partition_spec.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/sort_order.h"
#include "iceberg/table_update.h"
#include "iceberg/util/gzip_internal.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/uuid.h"

namespace iceberg {

namespace {
const TimePointMs kInvalidLastUpdatedMs = TimePointMs::min();
}

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

// TableMetadataCache implementation

Result<TableMetadataCache::SchemasMapRef> TableMetadataCache::GetSchemasById() const {
  return schemas_map_.Get(metadata_);
}

Result<TableMetadataCache::PartitionSpecsMapRef>
TableMetadataCache::GetPartitionSpecsById() const {
  return partition_specs_map_.Get(metadata_);
}

Result<TableMetadataCache::SortOrdersMapRef> TableMetadataCache::GetSortOrdersById()
    const {
  return sort_orders_map_.Get(metadata_);
}

Result<TableMetadataCache::SnapshotsMapRef> TableMetadataCache::GetSnapshotsById() const {
  return snapshot_map_.Get(metadata_);
}

Result<TableMetadataCache::SchemasMap> TableMetadataCache::InitSchemasMap(
    const TableMetadata* metadata) {
  SchemasMap schemas_map;
  schemas_map.reserve(metadata->schemas.size());
  for (const auto& schema : metadata->schemas) {
    if (schema->schema_id()) {
      schemas_map.emplace(schema->schema_id().value(), schema);
    }
  }
  return schemas_map;
}

Result<TableMetadataCache::PartitionSpecsMap> TableMetadataCache::InitPartitionSpecsMap(
    const TableMetadata* metadata) {
  PartitionSpecsMap partition_specs_map;
  partition_specs_map.reserve(metadata->partition_specs.size());
  for (const auto& spec : metadata->partition_specs) {
    partition_specs_map.emplace(spec->spec_id(), spec);
  }
  return partition_specs_map;
}

Result<TableMetadataCache::SortOrdersMap> TableMetadataCache::InitSortOrdersMap(
    const TableMetadata* metadata) {
  SortOrdersMap sort_orders_map;
  sort_orders_map.reserve(metadata->sort_orders.size());
  for (const auto& order : metadata->sort_orders) {
    sort_orders_map.emplace(order->order_id(), order);
  }
  return sort_orders_map;
}

Result<TableMetadataCache::SnapshotsMap> TableMetadataCache::InitSnapshotMap(
    const TableMetadata* metadata) {
  SnapshotsMap snapshots_map;
  snapshots_map.reserve(metadata->snapshots.size());
  for (const auto& snapshot : metadata->snapshots) {
    snapshots_map.emplace(snapshot->snapshot_id, snapshot);
  }
  return snapshots_map;
}

// TableMetadataUtil implementation

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
  // Base metadata (nullptr for new tables)
  const TableMetadata* base;

  // Working metadata copy
  TableMetadata metadata;

  // Change tracking
  std::vector<std::unique_ptr<TableUpdate>> changes;

  // Error collection (since methods return *this and cannot throw)
  std::vector<Error> errors;

  // Metadata location tracking
  std::optional<std::string> metadata_location;
  std::optional<std::string> previous_metadata_location;

  // Constructor for new table
  explicit Impl(int8_t format_version) : base(nullptr), metadata{} {
    metadata.format_version = format_version;
    metadata.last_sequence_number = TableMetadata::kInitialSequenceNumber;
    metadata.last_updated_ms = kInvalidLastUpdatedMs;
    metadata.last_column_id = Schema::kInvalidColumnId;
    metadata.default_spec_id = PartitionSpec::kInitialSpecId;
    metadata.last_partition_id = PartitionSpec::kInvalidPartitionFieldId;
    metadata.current_snapshot_id = Snapshot::kInvalidSnapshotId;
    metadata.default_sort_order_id = SortOrder::kInitialSortOrderId;
    metadata.next_row_id = TableMetadata::kInitialRowId;
  }

  // Constructor from existing metadata
  explicit Impl(const TableMetadata* base_metadata)
      : base(base_metadata), metadata(*base_metadata) {}
};

TableMetadataBuilder::TableMetadataBuilder(int8_t format_version)
    : impl_(std::make_unique<Impl>(format_version)) {}

TableMetadataBuilder::TableMetadataBuilder(const TableMetadata* base)
    : impl_(std::make_unique<Impl>(base)) {}

TableMetadataBuilder::~TableMetadataBuilder() = default;

TableMetadataBuilder::TableMetadataBuilder(TableMetadataBuilder&&) noexcept = default;

TableMetadataBuilder& TableMetadataBuilder::operator=(TableMetadataBuilder&&) noexcept =
    default;

std::unique_ptr<TableMetadataBuilder> TableMetadataBuilder::BuildFromEmpty(
    int8_t format_version) {
  return std::unique_ptr<TableMetadataBuilder>(
      new TableMetadataBuilder(format_version));  // NOLINT
}

std::unique_ptr<TableMetadataBuilder> TableMetadataBuilder::BuildFrom(
    const TableMetadata* base) {
  return std::unique_ptr<TableMetadataBuilder>(new TableMetadataBuilder(base));  // NOLINT
}

TableMetadataBuilder& TableMetadataBuilder::SetMetadataLocation(
    std::string_view metadata_location) {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

TableMetadataBuilder& TableMetadataBuilder::SetPreviousMetadataLocation(
    std::string_view previous_metadata_location) {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

TableMetadataBuilder& TableMetadataBuilder::AssignUUID() {
  if (impl_->metadata.table_uuid.empty()) {
    // Generate a random UUID
    return AssignUUID(Uuid::GenerateV4().ToString());
  }

  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::AssignUUID(std::string_view uuid) {
  std::string uuid_str(uuid);

  // Validation: UUID cannot be empty
  if (uuid_str.empty()) {
    impl_->errors.emplace_back(ErrorKind::kInvalidArgument, "Cannot assign empty UUID");
    return *this;
  }

  // Check if UUID is already set to the same value (no-op)
  if (StringUtils::EqualsIgnoreCase(impl_->metadata.table_uuid, uuid_str)) {
    return *this;
  }

  // Update the metadata
  impl_->metadata.table_uuid = uuid_str;

  // Record the change
  impl_->changes.push_back(std::make_unique<table::AssignUUID>(std::move(uuid_str)));

  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::UpgradeFormatVersion(
    int8_t new_format_version) {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

TableMetadataBuilder& TableMetadataBuilder::SetCurrentSchema(
    std::shared_ptr<Schema> schema, int32_t new_last_column_id) {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

TableMetadataBuilder& TableMetadataBuilder::SetCurrentSchema(int32_t schema_id) {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

TableMetadataBuilder& TableMetadataBuilder::AddSchema(std::shared_ptr<Schema> schema) {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

TableMetadataBuilder& TableMetadataBuilder::SetDefaultPartitionSpec(
    std::shared_ptr<PartitionSpec> spec) {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

TableMetadataBuilder& TableMetadataBuilder::SetDefaultPartitionSpec(int32_t spec_id) {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

TableMetadataBuilder& TableMetadataBuilder::AddPartitionSpec(
    std::shared_ptr<PartitionSpec> spec) {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

TableMetadataBuilder& TableMetadataBuilder::RemovePartitionSpecs(
    const std::vector<int32_t>& spec_ids) {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

TableMetadataBuilder& TableMetadataBuilder::RemoveSchemas(
    const std::vector<int32_t>& schema_ids) {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

TableMetadataBuilder& TableMetadataBuilder::SetDefaultSortOrder(
    std::shared_ptr<SortOrder> order) {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

TableMetadataBuilder& TableMetadataBuilder::SetDefaultSortOrder(int32_t order_id) {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

TableMetadataBuilder& TableMetadataBuilder::AddSortOrder(
    std::shared_ptr<SortOrder> order) {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

TableMetadataBuilder& TableMetadataBuilder::AddSnapshot(
    std::shared_ptr<Snapshot> snapshot) {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

TableMetadataBuilder& TableMetadataBuilder::SetBranchSnapshot(int64_t snapshot_id,
                                                              const std::string& branch) {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

TableMetadataBuilder& TableMetadataBuilder::SetRef(const std::string& name,
                                                   std::shared_ptr<SnapshotRef> ref) {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

TableMetadataBuilder& TableMetadataBuilder::RemoveRef(const std::string& name) {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

TableMetadataBuilder& TableMetadataBuilder::RemoveSnapshots(
    const std::vector<std::shared_ptr<Snapshot>>& snapshots_to_remove) {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

TableMetadataBuilder& TableMetadataBuilder::RemoveSnapshots(
    const std::vector<int64_t>& snapshot_ids) {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

TableMetadataBuilder& TableMetadataBuilder::suppressHistoricalSnapshots() {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

TableMetadataBuilder& TableMetadataBuilder::SetStatistics(
    const std::shared_ptr<StatisticsFile>& statistics_file) {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

TableMetadataBuilder& TableMetadataBuilder::RemoveStatistics(int64_t snapshot_id) {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

TableMetadataBuilder& TableMetadataBuilder::SetPartitionStatistics(
    const std::shared_ptr<PartitionStatisticsFile>& partition_statistics_file) {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

TableMetadataBuilder& TableMetadataBuilder::RemovePartitionStatistics(
    int64_t snapshot_id) {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

TableMetadataBuilder& TableMetadataBuilder::SetProperties(
    const std::unordered_map<std::string, std::string>& updated) {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

TableMetadataBuilder& TableMetadataBuilder::RemoveProperties(
    const std::vector<std::string>& removed) {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

TableMetadataBuilder& TableMetadataBuilder::SetLocation(std::string_view location) {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

TableMetadataBuilder& TableMetadataBuilder::AddEncryptionKey(
    std::shared_ptr<EncryptedKey> key) {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

TableMetadataBuilder& TableMetadataBuilder::RemoveEncryptionKey(std::string_view key_id) {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

Result<std::unique_ptr<TableMetadata>> TableMetadataBuilder::Build() {
  // 1. Check for accumulated errors
  if (!impl_->errors.empty()) {
    std::string error_msg = "Failed to build TableMetadata due to validation errors:\n";
    for (const auto& [kind, message] : impl_->errors) {
      error_msg += "  - " + message + "\n";
    }
    return CommitFailed("{}", error_msg);
  }

  // 2. Validate metadata consistency through TableMetadata#Validate

  // 3. Update last_updated_ms if there are changes
  if (impl_->metadata.last_updated_ms == kInvalidLastUpdatedMs) {
    impl_->metadata.last_updated_ms =
        TimePointMs{std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())};
  }

  // 4. Create and return the TableMetadata
  auto result = std::make_unique<TableMetadata>(std::move(impl_->metadata));

  return result;
}

}  // namespace iceberg
