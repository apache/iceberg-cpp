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
#include <charconv>
#include <chrono>
#include <cstdint>
#include <format>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <unordered_map>
#include <utility>

#include <nlohmann/json.hpp>

#include "iceberg/exception.h"
#include "iceberg/file_io.h"
#include "iceberg/json_internal.h"
#include "iceberg/partition_spec.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/sort_order.h"
#include "iceberg/table_properties.h"
#include "iceberg/table_update.h"
#include "iceberg/util/gzip_internal.h"
#include "iceberg/util/location_util.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/uuid.h"
namespace iceberg {
namespace {
const TimePointMs kInvalidLastUpdatedMs = TimePointMs::min();
constexpr int32_t kLastAdded = -1;
constexpr std::string_view kMetadataFolderName = "metadata";
}  // namespace

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
         SharedPtrVectorEquals(lhs.partition_specs, rhs.partition_specs) &&
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
  return metadata->schemas | std::views::filter([](const auto& schema) {
           return schema->schema_id().has_value();
         }) |
         std::views::transform([](const auto& schema) {
           return std::make_pair(schema->schema_id().value(), schema);
         }) |
         std::ranges::to<SchemasMap>();
}

Result<TableMetadataCache::PartitionSpecsMap> TableMetadataCache::InitPartitionSpecsMap(
    const TableMetadata* metadata) {
  return metadata->partition_specs | std::views::transform([](const auto& spec) {
           return std::make_pair(spec->spec_id(), spec);
         }) |
         std::ranges::to<PartitionSpecsMap>();
}

Result<TableMetadataCache::SortOrdersMap> TableMetadataCache::InitSortOrdersMap(
    const TableMetadata* metadata) {
  return metadata->sort_orders | std::views::transform([](const auto& order) {
           return std::make_pair(order->order_id(), order);
         }) |
         std::ranges::to<SortOrdersMap>();
}

Result<TableMetadataCache::SnapshotsMap> TableMetadataCache::InitSnapshotMap(
    const TableMetadata* metadata) {
  return metadata->snapshots | std::views::transform([](const auto& snapshot) {
           return std::make_pair(snapshot->snapshot_id, snapshot);
         }) |
         std::ranges::to<SnapshotsMap>();
}

Result<MetadataFileCodecType> TableMetadataUtil::Codec::FromString(
    std::string_view name) {
  std::string name_upper = StringUtils::ToUpper(name);
  if (name_upper == kCodecTypeGzip) {
    return MetadataFileCodecType::kGzip;
  } else if (name_upper == kCodecTypeNone) {
    return MetadataFileCodecType::kNone;
  }
  return InvalidArgument("Invalid codec name: {}", name);
}

Result<MetadataFileCodecType> TableMetadataUtil::Codec::FromFileName(
    std::string_view file_name) {
  auto pos = file_name.find_last_of(kTableMetadataFileSuffix);
  if (pos == std::string::npos) {
    return InvalidArgument("{} is not a valid metadata file", file_name);
  }

  // We have to be backward-compatible with .metadata.json.gz files
  if (file_name.ends_with(kCompGzipTableMetadataFileSuffix)) {
    return MetadataFileCodecType::kGzip;
  }

  std::string_view file_name_without_suffix = file_name.substr(0, pos);
  if (file_name_without_suffix.ends_with(kGzipTableMetadataFileExtension)) {
    return MetadataFileCodecType::kGzip;
  }
  return MetadataFileCodecType::kNone;
}

Result<std::string> TableMetadataUtil::Codec::NameToFileExtension(
    std::string_view codec) {
  ICEBERG_ASSIGN_OR_RAISE(MetadataFileCodecType codec_type, FromString(codec));
  return TypeToFileExtension(codec_type);
}

std::string TableMetadataUtil::Codec::TypeToFileExtension(MetadataFileCodecType codec) {
  switch (codec) {
    case MetadataFileCodecType::kGzip:
      return std::string(kGzipTableMetadataFileSuffix);
    case MetadataFileCodecType::kNone:
      return std::string(kTableMetadataFileSuffix);
  }
  std::unreachable();
}

// TableMetadataUtil implementation

Result<std::unique_ptr<TableMetadata>> TableMetadataUtil::Read(
    FileIO& io, const std::string& location, std::optional<size_t> length) {
  ICEBERG_ASSIGN_OR_RAISE(auto codec_type, Codec::FromFileName(location));

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

Result<std::string> TableMetadataUtil::Write(FileIO& io, const TableMetadata* base,
                                             const std::string& base_metadata_location,
                                             TableMetadata& metadata) {
  int32_t version = ParseVersionFromLocation(base_metadata_location);
  ICEBERG_ASSIGN_OR_RAISE(auto new_file_location,
                          NewTableMetadataFilePath(metadata, version + 1));
  ICEBERG_RETURN_UNEXPECTED(Write(io, new_file_location, metadata));
  return new_file_location;
}

Status TableMetadataUtil::Write(FileIO& io, const std::string& location,
                                const TableMetadata& metadata) {
  auto json = ToJson(metadata);
  ICEBERG_ASSIGN_OR_RAISE(auto json_string, ToJsonString(json));
  return io.WriteFile(location, json_string);
}

void TableMetadataUtil::DeleteRemovedMetadataFiles(FileIO& io, const TableMetadata* base,
                                                   const TableMetadata& metadata) {
  if (!base) {
    return;
  }

  bool delete_after_commit =
      metadata.properties.Get(TableProperties::kMetadataDeleteAfterCommitEnabled);
  if (delete_after_commit) {
    auto current_files =
        metadata.metadata_log | std::ranges::to<std::unordered_set<MetadataLogEntry>>();
    std::ranges::for_each(
        base->metadata_log | std::views::filter([&current_files](const auto& entry) {
          return !current_files.contains(entry);
        }),
        [&io](const auto& entry) { std::ignore = io.DeleteFile(entry.metadata_file); });
  }
}

int32_t TableMetadataUtil::ParseVersionFromLocation(std::string_view metadata_location) {
  size_t version_start = metadata_location.find_last_of('/') + 1;
  size_t version_end = metadata_location.find('-', version_start);

  int32_t version = -1;
  if (version_end != std::string::npos) {
    std::from_chars(metadata_location.data() + version_start,
                    metadata_location.data() + version_end, version);
  }
  return version;
}

Result<std::string> TableMetadataUtil::NewTableMetadataFilePath(const TableMetadata& meta,
                                                                int32_t new_version) {
  auto codec_name =
      meta.properties.Get<std::string>(TableProperties::kMetadataCompression);
  ICEBERG_ASSIGN_OR_RAISE(std::string file_extension,
                          Codec::NameToFileExtension(codec_name));

  std::string uuid = Uuid::GenerateV7().ToString();
  std::string filename = std::format("{:05d}-{}{}", new_version, uuid, file_extension);

  auto metadata_location =
      meta.properties.Get<std::string>(TableProperties::kWriteMetadataLocation);
  if (!metadata_location.empty()) {
    return std::format("{}/{}", LocationUtil::StripTrailingSlash(metadata_location),
                       filename);
  } else {
    return std::format("{}/{}/{}", meta.location, kMetadataFolderName, filename);
  }
}

// TableMetadataBuilder implementation

struct TableMetadataBuilder::Impl {
  // Base metadata (nullptr for new tables)
  const TableMetadata* base;

  // Working metadata copy
  TableMetadata metadata;

  // Change tracking
  std::vector<std::unique_ptr<TableUpdate>> changes;
  std::optional<int32_t> last_added_schema_id;
  std::optional<int32_t> last_added_order_id;
  std::optional<int32_t> last_added_spec_id;

  // Metadata location tracking
  std::string metadata_location;
  std::string previous_metadata_location;

  // indexes for convenience
  std::unordered_map<int32_t, std::shared_ptr<Schema>> schemas_by_id;
  std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> specs_by_id;
  std::unordered_map<int32_t, std::shared_ptr<SortOrder>> sort_orders_by_id;

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
  explicit Impl(const TableMetadata* base_metadata,
                std::string base_metadata_location = "")
      : base(base_metadata), metadata(*base_metadata) {
    // Initialize index maps from base metadata
    for (const auto& schema : metadata.schemas) {
      if (schema->schema_id().has_value()) {
        schemas_by_id.emplace(schema->schema_id().value(), schema);
      }
    }

    for (const auto& spec : metadata.partition_specs) {
      specs_by_id.emplace(spec->spec_id(), spec);
    }

    for (const auto& order : metadata.sort_orders) {
      sort_orders_by_id.emplace(order->order_id(), order);
    }

    metadata.last_updated_ms = kInvalidLastUpdatedMs;
  }
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
  impl_->metadata_location = std::string(metadata_location);
  if (impl_->base != nullptr) {
    // Carry over lastUpdatedMillis from base and set previousFileLocation to null to
    // avoid writing a new metadata log entry.
    // This is safe since setting metadata location doesn't cause any changes and no other
    // changes can be added when metadata location is configured
    impl_->previous_metadata_location = std::string();
    impl_->metadata.last_updated_ms = impl_->base->last_updated_ms;
  }
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::SetPreviousMetadataLocation(
    std::string_view previous_metadata_location) {
  impl_->previous_metadata_location = std::string(previous_metadata_location);
  return *this;
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
    return AddError(ErrorKind::kInvalidArgument, "Cannot assign empty UUID");
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
  // Check that the new format version is supported
  if (new_format_version > TableMetadata::kSupportedTableFormatVersion) {
    return AddError(
        ErrorKind::kInvalidArgument,
        std::format(
            "Cannot upgrade table to unsupported format version: v{} (supported: v{})",
            new_format_version, TableMetadata::kSupportedTableFormatVersion));
  }

  // Check that we're not downgrading
  if (new_format_version < impl_->metadata.format_version) {
    return AddError(ErrorKind::kInvalidArgument,
                    std::format("Cannot downgrade v{} table to v{}",
                                impl_->metadata.format_version, new_format_version));
  }

  // No-op if the version is the same
  if (new_format_version == impl_->metadata.format_version) {
    return *this;
  }

  // Update the format version
  impl_->metadata.format_version = new_format_version;

  // Record the change
  impl_->changes.push_back(
      std::make_unique<table::UpgradeFormatVersion>(new_format_version));

  return *this;
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
  BUILDER_ASSIGN_OR_RETURN(auto order_id, AddSortOrderInternal(*order));
  return SetDefaultSortOrder(order_id);
}

TableMetadataBuilder& TableMetadataBuilder::SetDefaultSortOrder(int32_t order_id) {
  if (order_id == -1) {
    if (!impl_->last_added_order_id.has_value()) {
      return AddError(ErrorKind::kInvalidArgument,
                      "Cannot set last added sort order: no sort order has been added");
    }
    return SetDefaultSortOrder(impl_->last_added_order_id.value());
  }

  if (order_id == impl_->metadata.default_sort_order_id) {
    return *this;
  }

  impl_->metadata.default_sort_order_id = order_id;

  if (impl_->last_added_order_id == std::make_optional(order_id)) {
    impl_->changes.push_back(std::make_unique<table::SetDefaultSortOrder>(kLastAdded));
  } else {
    impl_->changes.push_back(std::make_unique<table::SetDefaultSortOrder>(order_id));
  }
  return *this;
}

Result<int32_t> TableMetadataBuilder::AddSortOrderInternal(const SortOrder& order) {
  int32_t new_order_id = ReuseOrCreateNewSortOrderId(order);

  if (impl_->sort_orders_by_id.find(new_order_id) != impl_->sort_orders_by_id.end()) {
    // update last_added_order_id if the order was added in this set of changes (since it
    // is now the last)
    bool is_new_order =
        impl_->last_added_order_id.has_value() &&
        std::ranges::find_if(impl_->changes, [new_order_id](const auto& change) {
          auto* add_sort_order = dynamic_cast<table::AddSortOrder*>(change.get());
          return add_sort_order &&
                 add_sort_order->sort_order()->order_id() == new_order_id;
        }) != impl_->changes.cend();
    impl_->last_added_order_id =
        is_new_order ? std::make_optional(new_order_id) : std::nullopt;
    return new_order_id;
  }

  // Get current schema and validate the sort order against it
  ICEBERG_ASSIGN_OR_RAISE(auto schema, impl_->metadata.Schema());
  ICEBERG_RETURN_UNEXPECTED(order.Validate(*schema));

  std::shared_ptr<SortOrder> new_order;
  if (order.is_unsorted()) {
    new_order = SortOrder::Unsorted();
  } else {
    // Unlike freshSortOrder from Java impl, we don't use field name from old bound
    // schema to rebuild the sort order.
    ICEBERG_ASSIGN_OR_RAISE(
        new_order,
        SortOrder::Make(new_order_id, std::vector<SortField>(order.fields().begin(),
                                                             order.fields().end())));
  }

  impl_->metadata.sort_orders.push_back(new_order);
  impl_->sort_orders_by_id.emplace(new_order_id, new_order);

  impl_->changes.push_back(std::make_unique<table::AddSortOrder>(new_order));
  impl_->last_added_order_id = new_order_id;
  return new_order_id;
}

TableMetadataBuilder& TableMetadataBuilder::AddSortOrder(
    std::shared_ptr<SortOrder> order) {
  BUILDER_ASSIGN_OR_RETURN(auto order_id, AddSortOrderInternal(*order));
  return *this;
}

int32_t TableMetadataBuilder::ReuseOrCreateNewSortOrderId(const SortOrder& new_order) {
  if (new_order.is_unsorted()) {
    return SortOrder::kUnsortedOrderId;
  }
  // determine the next order id
  int32_t new_order_id = SortOrder::kInitialSortOrderId;
  for (const auto& order : impl_->metadata.sort_orders) {
    if (order->SameOrder(new_order)) {
      return order->order_id();
    } else if (new_order_id <= order->order_id()) {
      new_order_id = order->order_id() + 1;
    }
  }
  return new_order_id;
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

TableMetadataBuilder& TableMetadataBuilder::SuppressHistoricalSnapshots() {
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
  // If updated is empty, return early (no-op)
  if (updated.empty()) {
    return *this;
  }

  // Add all updated properties to the metadata properties
  for (const auto& [key, value] : updated) {
    impl_->metadata.properties.mutable_configs()[key] = value;
  }

  // Record the change
  impl_->changes.push_back(std::make_unique<table::SetProperties>(updated));

  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::RemoveProperties(
    const std::vector<std::string>& removed) {
  // If removed is empty, return early (no-op)
  if (removed.empty()) {
    return *this;
  }

  // Remove each property from the metadata properties
  for (const auto& key : removed) {
    impl_->metadata.properties.mutable_configs().erase(key);
  }

  // Record the change
  impl_->changes.push_back(std::make_unique<table::RemoveProperties>(removed));

  return *this;
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
  ICEBERG_RETURN_UNEXPECTED(CheckErrors());

  // 2. Validate metadata consistency through TableMetadata#Validate

  // 3. Update last_updated_ms if there are changes
  if (impl_->metadata.last_updated_ms == kInvalidLastUpdatedMs) {
    impl_->metadata.last_updated_ms =
        TimePointMs{std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())};
  }

  // 4. Buildup metadata_log from base metadata
  int32_t max_metadata_log_size =
      impl_->metadata.properties.Get(TableProperties::kMetadataPreviousVersionsMax);
  if (impl_->base != nullptr && !impl_->previous_metadata_location.empty()) {
    impl_->metadata.metadata_log.emplace_back(impl_->base->last_updated_ms,
                                              impl_->previous_metadata_location);
  }
  if (impl_->metadata.metadata_log.size() > max_metadata_log_size) {
    impl_->metadata.metadata_log.erase(
        impl_->metadata.metadata_log.begin(),
        impl_->metadata.metadata_log.end() - max_metadata_log_size);
  }

  // TODO(anyone): 5. update snapshot_log

  // 6. Create and return the TableMetadata
  return std::make_unique<TableMetadata>(std::move(impl_->metadata));
}

const std::vector<std::unique_ptr<TableUpdate>>& TableMetadataBuilder::changes() const {
  return impl_->changes;
}

const TableMetadata* TableMetadataBuilder::base() const { return impl_->base; }

const TableMetadata& TableMetadataBuilder::current() const { return impl_->metadata; }

}  // namespace iceberg
