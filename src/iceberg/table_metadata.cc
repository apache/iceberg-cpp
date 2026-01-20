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
#include <unordered_set>
#include <utility>

#include <nlohmann/json.hpp>

#include "iceberg/constants.h"
#include "iceberg/exception.h"
#include "iceberg/file_io.h"
#include "iceberg/json_internal.h"
#include "iceberg/metrics_config.h"
#include "iceberg/partition_field.h"
#include "iceberg/partition_spec.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/sort_order.h"
#include "iceberg/statistics_file.h"
#include "iceberg/table_properties.h"
#include "iceberg/table_update.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/error_collector.h"
#include "iceberg/util/gzip_internal.h"
#include "iceberg/util/location_util.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/property_util.h"
#include "iceberg/util/timepoint.h"
#include "iceberg/util/type_util.h"
#include "iceberg/util/uuid.h"

namespace iceberg {
namespace {
const TimePointMs kInvalidLastUpdatedMs = TimePointMs::min();
constexpr int32_t kLastAdded = -1;
constexpr std::string_view kMetadataFolderName = "metadata";

// TableMetadata private static methods
Result<std::unique_ptr<PartitionSpec>> FreshPartitionSpec(int32_t spec_id,
                                                          const PartitionSpec& spec,
                                                          const Schema& base_schema,
                                                          const Schema& fresh_schema) {
  std::vector<PartitionField> partition_fields;
  partition_fields.reserve(spec.fields().size());
  int32_t last_partition_field_id = PartitionSpec::kInvalidPartitionFieldId;
  for (auto& field : spec.fields()) {
    ICEBERG_ASSIGN_OR_RAISE(auto source_name,
                            base_schema.FindColumnNameById(field.source_id()));
    if (!source_name.has_value()) [[unlikely]] {
      return InvalidSchema(
          "Cannot find source partition field with ID {} in the old schema",
          field.source_id());
    }
    ICEBERG_ASSIGN_OR_RAISE(auto fresh_field,
                            fresh_schema.FindFieldByName(source_name.value()));
    if (!fresh_field.has_value()) [[unlikely]] {
      return InvalidSchema("Partition field {} does not exist in the schema",
                           source_name.value());
    }
    partition_fields.emplace_back(fresh_field.value().get().field_id(),
                                  ++last_partition_field_id, std::string(field.name()),
                                  field.transform());
  }
  return PartitionSpec::Make(fresh_schema, spec_id, std::move(partition_fields), false);
}

Result<std::shared_ptr<SortOrder>> FreshSortOrder(int32_t order_id,
                                                  const SortOrder& order,
                                                  const Schema& base_schema,
                                                  const Schema& fresh_schema) {
  if (order.is_unsorted()) {
    return SortOrder::Unsorted();
  }

  std::vector<SortField> sort_fields;
  sort_fields.reserve(order.fields().size());
  for (const auto& field : order.fields()) {
    ICEBERG_ASSIGN_OR_RAISE(auto source_name,
                            base_schema.FindColumnNameById(field.source_id()));
    if (!source_name.has_value()) {
      return InvalidSchema("Cannot find source sort field with ID {} in the old schema",
                           field.source_id());
    }
    ICEBERG_ASSIGN_OR_RAISE(auto fresh_field,
                            fresh_schema.FindFieldByName(source_name.value()));
    if (!fresh_field.has_value()) {
      return InvalidSchema("Cannot find field '{}' in the new schema",
                           source_name.value());
    }
    sort_fields.emplace_back(fresh_field.value().get().field_id(), field.transform(),
                             field.direction(), field.null_order());
  }
  return SortOrder::Make(order_id, std::move(sort_fields));
}

std::vector<std::unique_ptr<TableUpdate>> ChangesForCreate(
    const TableMetadata& metadata) {
  std::vector<std::unique_ptr<TableUpdate>> changes;

  // Add UUID assignment
  changes.push_back(std::make_unique<table::AssignUUID>(metadata.table_uuid));

  // Add format version upgrade
  changes.push_back(
      std::make_unique<table::UpgradeFormatVersion>(metadata.format_version));

  // Add schema
  if (auto current_schema_result = metadata.Schema(); current_schema_result.has_value()) {
    auto current_schema = current_schema_result.value();
    changes.push_back(
        std::make_unique<table::AddSchema>(current_schema, metadata.last_column_id));
    changes.push_back(std::make_unique<table::SetCurrentSchema>(kLastAdded));
  }

  // Add partition spec
  if (auto partition_spec_result = metadata.PartitionSpec();
      partition_spec_result.has_value()) {
    auto spec = partition_spec_result.value();
    if (spec && spec->spec_id() != PartitionSpec::kInitialSpecId) {
      changes.push_back(std::make_unique<table::AddPartitionSpec>(spec));
    } else {
      changes.push_back(
          std::make_unique<table::AddPartitionSpec>(PartitionSpec::Unpartitioned()));
    }
    changes.push_back(std::make_unique<table::SetDefaultPartitionSpec>(kLastAdded));
  }

  // Add sort order
  if (auto sort_order_result = metadata.SortOrder(); sort_order_result.has_value()) {
    auto order = sort_order_result.value();
    if (order && order->is_sorted()) {
      changes.push_back(std::make_unique<table::AddSortOrder>(order));
    } else {
      changes.push_back(std::make_unique<table::AddSortOrder>(SortOrder::Unsorted()));
    }
    changes.push_back(std::make_unique<table::SetDefaultSortOrder>(kLastAdded));
  }

  // Set location if not empty
  if (!metadata.location.empty()) {
    changes.push_back(std::make_unique<table::SetLocation>(metadata.location));
  }

  // Set properties if not empty
  if (!metadata.properties.configs().empty()) {
    changes.push_back(
        std::make_unique<table::SetProperties>(metadata.properties.configs()));
  }

  return changes;
}
}  // namespace

std::string ToString(const SnapshotLogEntry& entry) {
  return std::format("SnapshotLogEntry[timestampMillis={},snapshotId={}]",
                     entry.timestamp_ms, entry.snapshot_id);
}

std::string ToString(const MetadataLogEntry& entry) {
  return std::format("MetadataLogEntry[timestampMillis={},file={}]", entry.timestamp_ms,
                     entry.metadata_file);
}

Result<std::unique_ptr<TableMetadata>> TableMetadata::Make(
    const iceberg::Schema& schema, const iceberg::PartitionSpec& spec,
    const iceberg::SortOrder& sort_order, const std::string& location,
    const std::unordered_map<std::string, std::string>& properties, int format_version) {
  for (const auto& [key, _] : properties) {
    if (TableProperties::reserved_properties().contains(key)) {
      return InvalidArgument(
          "Table properties should not contain reserved properties, but got {}", key);
    }
  }

  // Reassign all column ids to ensure consistency
  int32_t last_column_id = 0;
  auto next_id = [&last_column_id]() -> int32_t { return ++last_column_id; };
  ICEBERG_ASSIGN_OR_RAISE(auto fresh_schema,
                          AssignFreshIds(Schema::kInitialSchemaId, schema, next_id));

  // Rebuild the partition spec using the new column ids
  ICEBERG_ASSIGN_OR_RAISE(
      auto fresh_spec,
      FreshPartitionSpec(PartitionSpec::kInitialSpecId, spec, schema, *fresh_schema));

  // rebuild the sort order using the new column ids
  ICEBERG_ASSIGN_OR_RAISE(
      auto fresh_order,
      FreshSortOrder(SortOrder::kInitialSortOrderId, sort_order, schema, *fresh_schema))

  // Validata the metrics configuration.
  ICEBERG_RETURN_UNEXPECTED(
      MetricsConfig::VerifyReferencedColumns(properties, *fresh_schema));

  ICEBERG_RETURN_UNEXPECTED(PropertyUtil::ValidateCommitProperties(properties));

  return TableMetadataBuilder::BuildFromEmpty(format_version)
      ->SetLocation(location)
      .SetCurrentSchema(std::move(fresh_schema), last_column_id)
      .SetDefaultPartitionSpec(std::move(fresh_spec))
      .SetDefaultSortOrder(std::move(fresh_order))
      .SetProperties(properties)
      .Build();
}

Result<std::shared_ptr<Schema>> TableMetadata::Schema() const {
  return SchemaById(current_schema_id);
}

Result<std::shared_ptr<Schema>> TableMetadata::SchemaById(int32_t schema_id) const {
  auto iter = std::ranges::find_if(schemas, [schema_id](const auto& schema) {
    return schema != nullptr && schema->schema_id() == schema_id;
  });
  if (iter == schemas.end()) {
    return NotFound("Schema with ID {} is not found", schema_id);
  }
  return *iter;
}

Result<std::shared_ptr<PartitionSpec>> TableMetadata::PartitionSpec() const {
  return PartitionSpecById(default_spec_id);
}

Result<std::shared_ptr<PartitionSpec>> TableMetadata::PartitionSpecById(
    int32_t spec_id) const {
  auto iter = std::ranges::find_if(partition_specs, [spec_id](const auto& spec) {
    return spec != nullptr && spec->spec_id() == spec_id;
  });
  if (iter == partition_specs.end()) {
    return NotFound("Partition spec with ID {} is not found", spec_id);
  }
  return *iter;
}

Result<std::shared_ptr<SortOrder>> TableMetadata::SortOrder() const {
  return SortOrderById(default_sort_order_id);
}

Result<std::shared_ptr<SortOrder>> TableMetadata::SortOrderById(int32_t order_id) const {
  auto iter = std::ranges::find_if(sort_orders, [order_id](const auto& order) {
    return order != nullptr && order->order_id() == order_id;
  });
  if (iter == sort_orders.end()) {
    return NotFound("Sort order with ID {} is not found", order_id);
  }
  return *iter;
}

Result<std::shared_ptr<Snapshot>> TableMetadata::Snapshot() const {
  if (current_snapshot_id == kInvalidSnapshotId) {
    return NotFound("No current snapshot");
  }
  return SnapshotById(current_snapshot_id);
}

Result<std::shared_ptr<Snapshot>> TableMetadata::SnapshotById(int64_t snapshot_id) const {
  auto iter = std::ranges::find_if(snapshots, [snapshot_id](const auto& snapshot) {
    return snapshot != nullptr && snapshot->snapshot_id == snapshot_id;
  });
  if (iter == snapshots.end()) {
    return NotFound("Snapshot with ID {} is not found", snapshot_id);
  }
  return *iter;
}

int64_t TableMetadata::NextSequenceNumber() const {
  return format_version > 1 ? last_sequence_number + 1 : kInitialSequenceNumber;
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
  return metadata->schemas | std::views::transform([](const auto& schema) {
           return std::make_pair(schema->schema_id(), schema);
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

class TableMetadataBuilder::Impl {
 public:
  // Constructor for new table
  explicit Impl(int8_t format_version) : base_(nullptr), metadata_{} {
    metadata_.format_version = format_version;
    metadata_.last_sequence_number = TableMetadata::kInitialSequenceNumber;
    metadata_.last_updated_ms = kInvalidLastUpdatedMs;
    metadata_.last_column_id = Schema::kInvalidColumnId;
    metadata_.default_spec_id = PartitionSpec::kInitialSpecId;
    metadata_.last_partition_id = PartitionSpec::kInvalidPartitionFieldId;
    metadata_.current_snapshot_id = kInvalidSnapshotId;
    metadata_.default_sort_order_id = SortOrder::kInitialSortOrderId;
    metadata_.next_row_id = TableMetadata::kInitialRowId;
  }

  // Constructor from existing metadata
  explicit Impl(const TableMetadata* base_metadata,
                std::string base_metadata_location = "")
      : base_(base_metadata), metadata_(*base_metadata) {
    // Initialize index maps from base metadata
    for (const auto& schema : metadata_.schemas) {
      schemas_by_id_.emplace(schema->schema_id(), schema);
    }

    for (const auto& spec : metadata_.partition_specs) {
      specs_by_id_.emplace(spec->spec_id(), spec);
    }

    for (const auto& order : metadata_.sort_orders) {
      sort_orders_by_id_.emplace(order->order_id(), order);
    }

    for (const auto& snapshot : metadata_.snapshots) {
      snapshots_by_id_.emplace(snapshot->snapshot_id, snapshot);
    }

    metadata_.last_updated_ms = kInvalidLastUpdatedMs;
  }

  bool UUIDSet() const { return !metadata_.table_uuid.empty(); }
  const std::vector<std::unique_ptr<TableUpdate>>& changes() const { return changes_; }
  const TableMetadata* base() const { return base_; }
  const TableMetadata& metadata() const { return metadata_; }

  void SetMetadataLocation(std::string_view metadata_location) {
    metadata_location_ = std::string(metadata_location);
    if (base_ != nullptr) {
      // Carry over lastUpdatedMillis from base and set previousFileLocation to null to
      // avoid writing a new metadata log entry.
      // This is safe since setting metadata location doesn't cause any changes and no
      // other changes can be added when metadata location is configured
      previous_metadata_location_ = std::string();
      metadata_.last_updated_ms = base_->last_updated_ms;
    }
  }

  void SetPreviousMetadataLocation(std::string_view previous_metadata_location) {
    previous_metadata_location_ = std::string(previous_metadata_location);
  }

  Status AssignUUID(std::string_view uuid);
  Status UpgradeFormatVersion(int8_t new_format_version);
  Status SetDefaultSortOrder(int32_t order_id);
  Result<int32_t> AddSortOrder(const SortOrder& order);
  Status SetProperties(const std::unordered_map<std::string, std::string>& updated);
  Status RemoveProperties(const std::unordered_set<std::string>& removed);
  Status SetDefaultPartitionSpec(int32_t spec_id);
  Result<int32_t> AddPartitionSpec(const PartitionSpec& spec);
  Status SetCurrentSchema(int32_t schema_id);
  Status RemoveSchemas(const std::unordered_set<int32_t>& schema_ids);
  Result<int32_t> AddSchema(const Schema& schema, int32_t new_last_column_id);
  void SetLocation(std::string_view location);
  Status AddSnapshot(std::shared_ptr<Snapshot> snapshot);
  Status SetBranchSnapshot(int64_t snapshot_id, const std::string& branch);
  Status SetBranchSnapshot(std::shared_ptr<Snapshot> snapshot, const std::string& branch);
  Status SetRef(const std::string& name, std::shared_ptr<SnapshotRef> ref);
  Status RemoveRef(const std::string& name);
  Status RemoveSnapshots(const std::vector<int64_t>& snapshot_ids);
  Status RemovePartitionSpecs(const std::vector<int32_t>& spec_ids);
  Status SetStatistics(std::shared_ptr<StatisticsFile> statistics_file);
  Status RemoveStatistics(int64_t snapshot_id);

  Result<std::unique_ptr<TableMetadata>> Build();

 private:
  /// \brief Internal method to check for existing sort order and reuse its ID or create a
  /// new one
  /// \param new_order The sort order to check
  /// \return The ID to use for this sort order (reused if exists, new otherwise)
  int32_t ReuseOrCreateNewSortOrderId(const SortOrder& new_order);

  /// \brief Internal method to check for existing partition spec and reuse its ID or
  /// create a new one
  /// \param new_spec The partition spec to check
  /// \return The ID to use for this partition spec (reused if exists, new otherwise)
  int32_t ReuseOrCreateNewPartitionSpecId(const PartitionSpec& new_spec);

  /// \brief Internal method to check for existing schema and reuse its ID or create a new
  /// one
  /// \param new_schema The schema to check
  /// \return The ID to use for this schema (reused if exists, new otherwise
  int32_t ReuseOrCreateNewSchemaId(const Schema& new_schema) const;

  /// \brief Finds intermediate snapshots that have not been committed as the current
  /// snapshot.
  ///
  /// Transactions can create snapshots that are never the current snapshot because
  /// several changes are combined by the transaction into one table metadata update. When
  /// each intermediate snapshot is added to table metadata, it is added to the snapshot
  /// log, assuming that it will be the current snapshot. When there are multiple snapshot
  /// updates, the log must be corrected by suppressing the intermediate snapshot entries.
  ///
  /// A snapshot is an intermediate snapshot if it was added but is not the current
  /// snapshot.
  ///
  /// \param current_snapshot_id The current snapshot ID
  /// \return A set of snapshot IDs for all added snapshots that were later replaced as
  /// the current snapshot in changes
  std::unordered_set<int64_t> IntermediateSnapshotIdSet(
      int64_t current_snapshot_id) const;

  /// \brief Updates the snapshot log by removing intermediate snapshots and handling
  /// removed snapshots.
  ///
  /// \param current_snapshot_id The current snapshot ID
  /// \return Updated snapshot log or error
  Result<std::vector<SnapshotLogEntry>> UpdateSnapshotLog(
      int64_t current_snapshot_id) const;

  Status SetBranchSnapshotInternal(const Snapshot& snapshot, const std::string& branch);

 private:
  // Base metadata (nullptr for new tables)
  const TableMetadata* base_;

  // Working metadata copy
  TableMetadata metadata_;

  // Change tracking
  std::vector<std::unique_ptr<TableUpdate>> changes_;
  std::optional<int32_t> last_added_schema_id_;
  std::optional<int32_t> last_added_order_id_;
  std::optional<int32_t> last_added_spec_id_;

  // Metadata location tracking
  std::string metadata_location_;
  std::string previous_metadata_location_;

  // indexes for convenience
  std::unordered_map<int32_t, std::shared_ptr<Schema>> schemas_by_id_;
  std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> specs_by_id_;
  std::unordered_map<int32_t, std::shared_ptr<SortOrder>> sort_orders_by_id_;
  std::unordered_map<int64_t, std::shared_ptr<Snapshot>> snapshots_by_id_;
};

Status TableMetadataBuilder::Impl::AssignUUID(std::string_view uuid) {
  if (uuid.empty()) {
    return InvalidArgument("Cannot assign empty UUID");
  }

  std::string uuid_str = std::string(uuid);
  // Check if UUID is already set to the same value (no-op)
  if (StringUtils::EqualsIgnoreCase(metadata_.table_uuid, uuid_str)) {
    return {};
  }

  // Update the metadata
  metadata_.table_uuid = uuid_str;

  // Record the change
  changes_.push_back(std::make_unique<table::AssignUUID>(std::move(uuid_str)));
  return {};
}

Status TableMetadataBuilder::Impl::UpgradeFormatVersion(int8_t new_format_version) {
  // Check that the new format version is supported
  if (new_format_version > TableMetadata::kSupportedTableFormatVersion) {
    return InvalidArgument(
        "Cannot upgrade table to unsupported format version: v{} (supported: v{})",
        new_format_version, TableMetadata::kSupportedTableFormatVersion);
  }

  // Check that we're not downgrading
  if (new_format_version < metadata_.format_version) {
    return InvalidArgument("Cannot downgrade v{} table to v{}", metadata_.format_version,
                           new_format_version);
  }

  // No-op if the version is the same
  if (new_format_version == metadata_.format_version) {
    return {};
  }

  // Update the format version
  metadata_.format_version = new_format_version;

  // Record the change
  changes_.push_back(std::make_unique<table::UpgradeFormatVersion>(new_format_version));

  return {};
}

Status TableMetadataBuilder::Impl::SetDefaultSortOrder(int32_t order_id) {
  if (order_id == kLastAdded) {
    if (!last_added_order_id_.has_value()) {
      return InvalidArgument(
          "Cannot set last added sort order: no sort order has been added");
    }
    return SetDefaultSortOrder(last_added_order_id_.value());
  }

  if (order_id == metadata_.default_sort_order_id) {
    return {};
  }

  metadata_.default_sort_order_id = order_id;

  if (last_added_order_id_ == std::make_optional(order_id)) {
    changes_.push_back(std::make_unique<table::SetDefaultSortOrder>(kLastAdded));
  } else {
    changes_.push_back(std::make_unique<table::SetDefaultSortOrder>(order_id));
  }
  return {};
}

Result<int32_t> TableMetadataBuilder::Impl::AddSortOrder(const SortOrder& order) {
  int32_t new_order_id = ReuseOrCreateNewSortOrderId(order);

  if (sort_orders_by_id_.find(new_order_id) != sort_orders_by_id_.end()) {
    // update last_added_order_id if the order was added in this set of changes (since it
    // is now the last)
    bool is_new_order =
        last_added_order_id_.has_value() &&
        std::ranges::any_of(changes_, [new_order_id](const auto& change) {
          return change->kind() == TableUpdate::Kind::kAddSortOrder &&
                 internal::checked_cast<const table::AddSortOrder&>(*change)
                         .sort_order()
                         ->order_id() == new_order_id;
        });
    last_added_order_id_ = is_new_order ? std::make_optional(new_order_id) : std::nullopt;
    return new_order_id;
  }

  // Get current schema and validate the sort order against it
  ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata_.Schema());
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

  metadata_.sort_orders.push_back(new_order);
  sort_orders_by_id_.emplace(new_order_id, new_order);

  changes_.push_back(std::make_unique<table::AddSortOrder>(new_order));
  last_added_order_id_ = new_order_id;
  return new_order_id;
}

Status TableMetadataBuilder::Impl::SetDefaultPartitionSpec(int32_t spec_id) {
  if (spec_id == kLastAdded) {
    if (!last_added_spec_id_.has_value()) {
      return ValidationFailed(
          "Cannot set last added partition spec: no partition spec has been added");
    }
    return SetDefaultPartitionSpec(last_added_spec_id_.value());
  }

  if (spec_id == metadata_.default_spec_id) {
    // the new spec is already current and no change is needed
    return {};
  }

  metadata_.default_spec_id = spec_id;
  if (last_added_spec_id_ == std::make_optional(spec_id)) {
    changes_.push_back(std::make_unique<table::SetDefaultPartitionSpec>(kLastAdded));
  } else {
    changes_.push_back(std::make_unique<table::SetDefaultPartitionSpec>(spec_id));
  }
  return {};
}

Result<int32_t> TableMetadataBuilder::Impl::AddPartitionSpec(const PartitionSpec& spec) {
  int32_t new_spec_id = ReuseOrCreateNewPartitionSpecId(spec);

  if (specs_by_id_.contains(new_spec_id)) {
    // update last_added_spec_id if the spec was added in this set of changes (since it
    // is now the last)
    bool is_new_spec =
        last_added_spec_id_.has_value() &&
        std::ranges::any_of(changes_, [new_spec_id](const auto& change) {
          return change->kind() == TableUpdate::Kind::kAddPartitionSpec &&
                 internal::checked_cast<const table::AddPartitionSpec&>(*change)
                         .spec()
                         ->spec_id() == new_spec_id;
        });
    last_added_spec_id_ = is_new_spec ? std::make_optional(new_spec_id) : std::nullopt;
    return new_spec_id;
  }

  // Get current schema and validate the partition spec against it
  ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata_.Schema());
  ICEBERG_RETURN_UNEXPECTED(spec.Validate(*schema, /*allow_missing_fields=*/false));
  ICEBERG_CHECK(
      metadata_.format_version > 1 || PartitionSpec::HasSequentialFieldIds(spec),
      "Spec does not use sequential IDs that are required in v1: {}", spec.ToString());

  ICEBERG_ASSIGN_OR_RAISE(
      std::shared_ptr<PartitionSpec> new_spec,
      PartitionSpec::Make(new_spec_id, spec.fields() | std::ranges::to<std::vector>()));
  metadata_.last_partition_id =
      std::max(metadata_.last_partition_id, new_spec->last_assigned_field_id());
  metadata_.partition_specs.push_back(new_spec);
  specs_by_id_.emplace(new_spec_id, new_spec);

  changes_.push_back(std::make_unique<table::AddPartitionSpec>(new_spec));
  last_added_spec_id_ = new_spec_id;

  return new_spec_id;
}

Status TableMetadataBuilder::Impl::SetProperties(
    const std::unordered_map<std::string, std::string>& updated) {
  // If updated is empty, return early (no-op)
  if (updated.empty()) {
    return {};
  }

  // Add all updated properties to the metadata properties
  for (const auto& [key, value] : updated) {
    metadata_.properties.mutable_configs()[key] = value;
  }

  // Record the change
  changes_.push_back(std::make_unique<table::SetProperties>(updated));

  return {};
}

Status TableMetadataBuilder::Impl::RemoveProperties(
    const std::unordered_set<std::string>& removed) {
  // If removed is empty, return early (no-op)
  if (removed.empty()) {
    return {};
  }

  // Remove each property from the metadata properties
  for (const auto& key : removed) {
    metadata_.properties.mutable_configs().erase(key);
  }

  // Record the change
  changes_.push_back(std::make_unique<table::RemoveProperties>(removed));

  return {};
}

Status TableMetadataBuilder::Impl::SetCurrentSchema(int32_t schema_id) {
  if (schema_id == kLastAdded) {
    if (!last_added_schema_id_.has_value()) {
      return ValidationFailed("Cannot set last added schema: no schema has been added");
    }
    return SetCurrentSchema(last_added_schema_id_.value());
  }

  if (metadata_.current_schema_id == schema_id) {
    return {};
  }

  auto it = schemas_by_id_.find(schema_id);
  ICEBERG_PRECHECK(it != schemas_by_id_.end(),
                   "Cannot set current schema to unknown schema: {}", schema_id);
  const auto& schema = it->second;

  // Rebuild all partition specs for the new current schema
  std::vector<std::shared_ptr<PartitionSpec>> updated_specs;
  for (const auto& partition_spec : metadata_.partition_specs) {
    ICEBERG_ASSIGN_OR_RAISE(
        auto updated_spec,
        PartitionSpec::Make(partition_spec->spec_id(),
                            partition_spec->fields() | std::ranges::to<std::vector>()));

    ICEBERG_RETURN_UNEXPECTED(
        PartitionSpec::ValidatePartitionName(*schema, *updated_spec));

    updated_specs.push_back(std::move(updated_spec));
  }
  metadata_.partition_specs = std::move(updated_specs);

  specs_by_id_.clear();
  for (const auto& spec : metadata_.partition_specs) {
    specs_by_id_.emplace(spec->spec_id(), spec);
  }

  // Rebuild all sort orders for the new current schema
  std::vector<std::shared_ptr<SortOrder>> updated_orders;
  for (const auto& sort_order : metadata_.sort_orders) {
    ICEBERG_ASSIGN_OR_RAISE(
        auto updated_order,
        SortOrder::Make(sort_order->order_id(),
                        sort_order->fields() | std::ranges::to<std::vector>()));
    updated_orders.push_back(std::move(updated_order));
  }
  metadata_.sort_orders = std::move(updated_orders);

  sort_orders_by_id_.clear();
  for (const auto& order : metadata_.sort_orders) {
    sort_orders_by_id_.emplace(order->order_id(), order);
  }

  // Set the current schema ID
  metadata_.current_schema_id = schema_id;

  // Record the change
  if (last_added_schema_id_.has_value() && last_added_schema_id_.value() == schema_id) {
    changes_.push_back(std::make_unique<table::SetCurrentSchema>(kLastAdded));
  } else {
    changes_.push_back(std::make_unique<table::SetCurrentSchema>(schema_id));
  }

  return {};
}

Status TableMetadataBuilder::Impl::RemoveSchemas(
    const std::unordered_set<int32_t>& schema_ids) {
  auto current_schema_id = metadata_.current_schema_id;
  ICEBERG_PRECHECK(!schema_ids.contains(current_schema_id),
                   "Cannot remove current schema: {}", current_schema_id);

  if (!schema_ids.empty()) {
    metadata_.schemas = metadata_.schemas | std::views::filter([&](const auto& schema) {
                          return !schema_ids.contains(schema->schema_id());
                        }) |
                        std::ranges::to<std::vector<std::shared_ptr<Schema>>>();
    changes_.push_back(std::make_unique<table::RemoveSchemas>(schema_ids));
  }

  return {};
}

Result<int32_t> TableMetadataBuilder::Impl::AddSchema(const Schema& schema,
                                                      int32_t new_last_column_id) {
  ICEBERG_PRECHECK(new_last_column_id >= metadata_.last_column_id,
                   "Invalid last column ID: {} < {} (previous last column ID)",
                   new_last_column_id, metadata_.last_column_id);

  ICEBERG_RETURN_UNEXPECTED(schema.Validate(metadata_.format_version));

  auto new_schema_id = ReuseOrCreateNewSchemaId(schema);
  auto schema_found = schemas_by_id_.contains(new_schema_id);
  if (schema_found && new_last_column_id == metadata_.last_column_id) {
    // update last_added_schema_id if the schema was added in this set of changes (since
    // it is now the last)
    bool is_new_schema =
        last_added_schema_id_.has_value() &&
        std::ranges::any_of(changes_, [new_schema_id](const auto& change) {
          if (change->kind() != TableUpdate::Kind::kAddSchema) {
            return false;
          }
          auto* add_schema = internal::checked_cast<table::AddSchema*>(change.get());
          return add_schema->schema()->schema_id() == std::make_optional(new_schema_id);
        });
    last_added_schema_id_ =
        is_new_schema ? std::make_optional(new_schema_id) : std::nullopt;
    return new_schema_id;
  }

  metadata_.last_column_id = new_last_column_id;

  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<Schema> new_schema,
                          Schema::Make(schema.fields() | std::ranges::to<std::vector>(),
                                       new_schema_id, schema.IdentifierFieldIds()))

  if (!schema_found) {
    metadata_.schemas.push_back(new_schema);
    schemas_by_id_.emplace(new_schema_id, new_schema);
  }

  changes_.push_back(std::make_unique<table::AddSchema>(new_schema, new_last_column_id));

  last_added_schema_id_ = new_schema_id;

  return new_schema_id;
}

void TableMetadataBuilder::Impl::SetLocation(std::string_view location) {
  if (location == metadata_.location) {
    return;
  }
  metadata_.location = std::string(location);
  changes_.push_back(std::make_unique<table::SetLocation>(std::string(location)));
}

Status TableMetadataBuilder::Impl::AddSnapshot(std::shared_ptr<Snapshot> snapshot) {
  if (snapshot == nullptr) {
    // change is a noop
    return {};
  }
  ICEBERG_CHECK(!metadata_.schemas.empty(),
                "Attempting to add a snapshot before a schema is added");
  ICEBERG_CHECK(!metadata_.partition_specs.empty(),
                "Attempting to add a snapshot before a partition spec is added");
  ICEBERG_CHECK(!metadata_.sort_orders.empty(),
                "Attempting to add a snapshot before a sort order is added");
  ICEBERG_CHECK(!snapshots_by_id_.contains(snapshot->snapshot_id),
                "Snapshot already exists for id: {}", snapshot->snapshot_id);
  ICEBERG_CHECK(
      metadata_.format_version == 1 ||
          snapshot->sequence_number > metadata_.last_sequence_number ||
          !snapshot->parent_snapshot_id.has_value(),
      "Cannot add snapshot with sequence number {} older than last sequence number {}",
      snapshot->sequence_number, metadata_.last_sequence_number);

  metadata_.last_updated_ms = snapshot->timestamp_ms;
  metadata_.last_sequence_number = snapshot->sequence_number;
  metadata_.snapshots.push_back(snapshot);
  snapshots_by_id_.emplace(snapshot->snapshot_id, snapshot);
  changes_.push_back(std::make_unique<table::AddSnapshot>(snapshot));

  if (metadata_.format_version >= TableMetadata::kMinFormatVersionRowLineage) {
    ICEBERG_ASSIGN_OR_RAISE(auto first_row_id, snapshot->FirstRowId());
    ICEBERG_CHECK(first_row_id.has_value(),
                  "Cannot add a snapshot: first-row-id is null");
    ICEBERG_CHECK(
        first_row_id.value() >= metadata_.next_row_id,
        "Cannot add a snapshot, first-row-id is behind table next-row-id: {} < {}",
        first_row_id.value(), metadata_.next_row_id);

    ICEBERG_ASSIGN_OR_RAISE(auto add_rows, snapshot->AddedRows());
    ICEBERG_CHECK(add_rows.has_value(), "Cannot add a snapshot: added-rows is null");
    metadata_.next_row_id += add_rows.value();
  }

  return {};
}

Status TableMetadataBuilder::Impl::SetBranchSnapshot(int64_t snapshot_id,
                                                     const std::string& branch) {
  auto ref_it = metadata_.refs.find(branch);
  if (ref_it != metadata_.refs.end() && ref_it->second->snapshot_id == snapshot_id) {
    // change is a noop
    return {};
  }

  auto snapshot_it = snapshots_by_id_.find(snapshot_id);
  ICEBERG_CHECK(snapshot_it != snapshots_by_id_.end(),
                "Cannot set {} to unknown snapshot: {}", branch, snapshot_id);
  return SetBranchSnapshotInternal(*snapshot_it->second, branch);
}

Status TableMetadataBuilder::Impl::SetBranchSnapshot(std::shared_ptr<Snapshot> snapshot,
                                                     const std::string& branch) {
  if (snapshot == nullptr) {
    // change is a noop
    return {};
  }
  const Snapshot& snapshot_ref = *snapshot;
  ICEBERG_RETURN_UNEXPECTED(AddSnapshot(std::move(snapshot)));
  return SetBranchSnapshotInternal(snapshot_ref, branch);
}

Status TableMetadataBuilder::Impl::SetBranchSnapshotInternal(const Snapshot& snapshot,
                                                             const std::string& branch) {
  const int64_t replacement_snapshot_id = snapshot.snapshot_id;
  auto ref_it = metadata_.refs.find(branch);
  if (ref_it != metadata_.refs.end()) {
    ICEBERG_CHECK(ref_it->second->type() == SnapshotRefType::kBranch,
                  "Cannot update branch: {} is a tag", branch);
    if (ref_it->second->snapshot_id == replacement_snapshot_id) {
      return {};
    }
  }

  ICEBERG_CHECK(
      metadata_.format_version == 1 ||
          snapshot.sequence_number <= metadata_.last_sequence_number,
      "Last sequence number {} is less than existing snapshot sequence number {}",
      metadata_.last_sequence_number, snapshot.sequence_number);

  std::shared_ptr<SnapshotRef> new_ref;
  if (ref_it != metadata_.refs.end()) {
    new_ref = ref_it->second->Clone(replacement_snapshot_id);
  } else {
    ICEBERG_ASSIGN_OR_RAISE(new_ref, SnapshotRef::MakeBranch(replacement_snapshot_id));
  }

  return SetRef(branch, std::move(new_ref));
}

Status TableMetadataBuilder::Impl::SetRef(const std::string& name,
                                          std::shared_ptr<SnapshotRef> ref) {
  auto existing_ref_it = metadata_.refs.find(name);
  if (existing_ref_it != metadata_.refs.end() && *existing_ref_it->second == *ref) {
    return {};
  }

  int64_t snapshot_id = ref->snapshot_id;
  auto snapshot_it = snapshots_by_id_.find(snapshot_id);
  ICEBERG_CHECK(snapshot_it != snapshots_by_id_.end(),
                "Cannot set {} to unknown snapshot: {}", name, snapshot_id);
  const auto& snapshot = snapshot_it->second;

  // If snapshot was added in this set of changes, update last_updated_ms
  if (std::ranges::any_of(changes_, [snapshot_id](const auto& change) {
        return change->kind() == TableUpdate::Kind::kAddSnapshot &&
               internal::checked_cast<const table::AddSnapshot&>(*change)
                       .snapshot()
                       ->snapshot_id == snapshot_id;
      })) {
    metadata_.last_updated_ms = snapshot->timestamp_ms;
  }

  if (name == SnapshotRef::kMainBranch) {
    metadata_.current_snapshot_id = ref->snapshot_id;
    if (metadata_.last_updated_ms == kInvalidLastUpdatedMs) {
      metadata_.last_updated_ms = CurrentTimePointMs();
    }
    metadata_.snapshot_log.emplace_back(metadata_.last_updated_ms, ref->snapshot_id);
  }

  changes_.push_back(std::make_unique<table::SetSnapshotRef>(name, *ref));
  metadata_.refs[name] = std::move(ref);

  return {};
}

Status TableMetadataBuilder::Impl::SetStatistics(
    std::shared_ptr<StatisticsFile> statistics_file) {
  ICEBERG_PRECHECK(statistics_file != nullptr, "Cannot set null statistics file");

  // Find and replace existing statistics for the same snapshot_id, or add new one
  auto it = std::ranges::find_if(
      metadata_.statistics,
      [snapshot_id = statistics_file->snapshot_id](const auto& stat) {
        return stat && stat->snapshot_id == snapshot_id;
      });

  if (it != metadata_.statistics.end()) {
    *it = statistics_file;
  } else {
    metadata_.statistics.push_back(statistics_file);
  }

  changes_.push_back(std::make_unique<table::SetStatistics>(std::move(statistics_file)));
  return {};
}

Status TableMetadataBuilder::Impl::RemoveStatistics(int64_t snapshot_id) {
  auto removed_count =
      std::erase_if(metadata_.statistics, [snapshot_id](const auto& stat) {
        return stat && stat->snapshot_id == snapshot_id;
      });
  if (removed_count != 0) {
    changes_.push_back(std::make_unique<table::RemoveStatistics>(snapshot_id));
  }
  return {};
}

std::unordered_set<int64_t> TableMetadataBuilder::Impl::IntermediateSnapshotIdSet(
    int64_t current_snapshot_id) const {
  std::unordered_set<int64_t> added_snapshot_ids;
  std::unordered_set<int64_t> intermediate_snapshot_ids;

  std::ranges::for_each(changes_, [&](const auto& change) {
    if (change->kind() == TableUpdate::Kind::kAddSnapshot) {
      // Adds must always come before set current snapshot
      const auto& added_snapshot =
          internal::checked_cast<const table::AddSnapshot&>(*change);
      added_snapshot_ids.insert(added_snapshot.snapshot()->snapshot_id);
    } else if (change->kind() == TableUpdate::Kind::kSetSnapshotRef) {
      const auto& set_ref = internal::checked_cast<const table::SetSnapshotRef&>(*change);
      int64_t snapshot_id = set_ref.snapshot_id();
      if (added_snapshot_ids.contains(snapshot_id) &&
          set_ref.ref_name() == SnapshotRef::kMainBranch &&
          snapshot_id != current_snapshot_id) {
        intermediate_snapshot_ids.insert(snapshot_id);
      }
    }
  });

  return intermediate_snapshot_ids;
}

Result<std::vector<SnapshotLogEntry>> TableMetadataBuilder::Impl::UpdateSnapshotLog(
    int64_t current_snapshot_id) const {
  std::unordered_set<int64_t> intermediate_snapshot_ids =
      IntermediateSnapshotIdSet(current_snapshot_id);
  const bool has_removed_snapshots =
      std::ranges::any_of(changes_, [](const auto& change) {
        return change->kind() == TableUpdate::Kind::kRemoveSnapshots;
      });
  if (intermediate_snapshot_ids.empty() && !has_removed_snapshots) {
    return metadata_.snapshot_log;
  }

  // Update the snapshot log
  std::vector<SnapshotLogEntry> new_snapshot_log;
  for (const auto& log_entry : metadata_.snapshot_log) {
    int64_t snapshot_id = log_entry.snapshot_id;
    if (snapshots_by_id_.contains(snapshot_id)) {
      if (!intermediate_snapshot_ids.contains(snapshot_id)) {
        // Copy the log entries that are still valid
        new_snapshot_log.push_back(log_entry);
      }
    } else if (has_removed_snapshots) {
      // Any invalid entry causes the history before it to be removed. Otherwise, there
      // could be history gaps that cause time-travel queries to produce incorrect
      // results. For example, if history is [(t1, s1), (t2, s2), (t3, s3)] and s2 is
      // removed, the history cannot be [(t1, s1), (t3, s3)] because it appears that s3
      // was current during the time between t2 and t3 when in fact s2 was the current
      // snapshot.
      new_snapshot_log.clear();
    }
  }

  if (snapshots_by_id_.contains(current_snapshot_id)) {
    ICEBERG_CHECK(
        !new_snapshot_log.empty() &&
            new_snapshot_log.back().snapshot_id == current_snapshot_id,
        "Cannot set invalid snapshot log: latest entry is not the current snapshot");
  }

  return new_snapshot_log;
}

Result<std::unique_ptr<TableMetadata>> TableMetadataBuilder::Impl::Build() {
  // 1. Validate metadata consistency through TableMetadata#Validate

  // 2. Update last_updated_ms if there are changes
  if (metadata_.last_updated_ms == kInvalidLastUpdatedMs) {
    metadata_.last_updated_ms =
        TimePointMs{std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())};
  }

  auto current_schema_id = metadata_.current_schema_id;
  auto schema_it = schemas_by_id_.find(current_schema_id);
  ICEBERG_PRECHECK(schema_it != schemas_by_id_.end(),
                   "Current schema ID {} not found in schemas", current_schema_id);
  {
    const auto& current_schema = schema_it->second;

    auto spec_it = specs_by_id_.find(metadata_.default_spec_id);
    ICEBERG_PRECHECK(spec_it != specs_by_id_.end(),
                     "Default spec ID {} not found in partition specs",
                     metadata_.default_spec_id);
    ICEBERG_RETURN_UNEXPECTED(
        spec_it->second->Validate(*current_schema, /*allow_missing_fields=*/false));

    auto sort_order_it = sort_orders_by_id_.find(metadata_.default_sort_order_id);
    ICEBERG_PRECHECK(sort_order_it != sort_orders_by_id_.end(),
                     "Default sort order ID {} not found in sort orders",
                     metadata_.default_sort_order_id);
    ICEBERG_RETURN_UNEXPECTED(sort_order_it->second->Validate(*current_schema));
  }

  // 3. Buildup metadata_log from base metadata
  int32_t max_metadata_log_size =
      metadata_.properties.Get(TableProperties::kMetadataPreviousVersionsMax);
  if (base_ != nullptr && !previous_metadata_location_.empty()) {
    metadata_.metadata_log.emplace_back(base_->last_updated_ms,
                                        previous_metadata_location_);
  }
  if (metadata_.metadata_log.size() > max_metadata_log_size) {
    metadata_.metadata_log.erase(metadata_.metadata_log.begin(),
                                 metadata_.metadata_log.end() - max_metadata_log_size);
  }

  // 4. Update snapshot_log
  ICEBERG_ASSIGN_OR_RAISE(metadata_.snapshot_log,
                          UpdateSnapshotLog(metadata_.current_snapshot_id));

  // 5. Create and return the TableMetadata
  return std::make_unique<TableMetadata>(std::move(metadata_));
}

int32_t TableMetadataBuilder::Impl::ReuseOrCreateNewSortOrderId(
    const SortOrder& new_order) {
  if (new_order.is_unsorted()) {
    return SortOrder::kUnsortedOrderId;
  }
  // determine the next order id
  int32_t new_order_id = SortOrder::kInitialSortOrderId;
  for (const auto& order : metadata_.sort_orders) {
    if (order->SameOrder(new_order)) {
      return order->order_id();
    } else if (new_order_id <= order->order_id()) {
      new_order_id = order->order_id() + 1;
    }
  }
  return new_order_id;
}

int32_t TableMetadataBuilder::Impl::ReuseOrCreateNewPartitionSpecId(
    const PartitionSpec& new_spec) {
  // if the spec already exists, use the same ID. otherwise, use the highest ID + 1.
  int32_t new_spec_id = PartitionSpec::kInitialSpecId;
  for (const auto& spec : metadata_.partition_specs) {
    if (new_spec.CompatibleWith(*spec)) {
      return spec->spec_id();
    } else if (new_spec_id <= spec->spec_id()) {
      new_spec_id = spec->spec_id() + 1;
    }
  }
  return new_spec_id;
}

int32_t TableMetadataBuilder::Impl::ReuseOrCreateNewSchemaId(
    const Schema& new_schema) const {
  // if the schema already exists, use its id; otherwise use the highest id + 1
  auto new_schema_id = metadata_.current_schema_id;
  for (auto& schema : metadata_.schemas) {
    auto schema_id = schema->schema_id();
    if (schema->SameSchema(new_schema)) {
      return schema_id;
    } else if (new_schema_id <= schema_id) {
      new_schema_id = schema_id + 1;
    }
  }
  return new_schema_id;
}

Status TableMetadataBuilder::Impl::RemoveRef(const std::string& name) {
  if (name == SnapshotRef::kMainBranch) {
    metadata_.current_snapshot_id = kInvalidSnapshotId;
  }

  if (metadata_.refs.erase(name) != 0) {
    changes_.push_back(std::make_unique<table::RemoveSnapshotRef>(name));
  }

  return {};
}

Status TableMetadataBuilder::Impl::RemoveSnapshots(
    const std::vector<int64_t>& snapshot_ids) {
  if (snapshot_ids.empty()) {
    return {};
  }

  std::unordered_set<int64_t> ids_to_remove(snapshot_ids.begin(), snapshot_ids.end());
  std::vector<std::shared_ptr<Snapshot>> retained_snapshots;
  retained_snapshots.reserve(metadata_.snapshots.size() - snapshot_ids.size());
  std::vector<int64_t> snapshot_ids_to_remove;
  snapshot_ids_to_remove.reserve(snapshot_ids.size());

  for (auto& snapshot : metadata_.snapshots) {
    ICEBERG_CHECK(snapshot != nullptr, "Encountered null snapshot in metadata");
    const int64_t snapshot_id = snapshot->snapshot_id;
    if (ids_to_remove.contains(snapshot_id)) {
      snapshots_by_id_.erase(snapshot_id);
      snapshot_ids_to_remove.push_back(snapshot_id);
      // FIXME: implement statistics removal and uncomment below
      // ICEBERG_RETURN_UNEXPECTED(RemoveStatistics(snapshot_id));
      // ICEBERG_RETURN_UNEXPECTED(RemovePartitionStatistics(snapshot_id));
    } else {
      retained_snapshots.push_back(std::move(snapshot));
    }
  }

  if (!snapshot_ids_to_remove.empty()) {
    changes_.push_back(std::make_unique<table::RemoveSnapshots>(snapshot_ids_to_remove));
  }

  metadata_.snapshots = std::move(retained_snapshots);

  // Remove any refs that are no longer valid (dangling refs)
  std::vector<std::string> dangling_refs;
  for (const auto& [ref_name, ref] : metadata_.refs) {
    if (!snapshots_by_id_.contains(ref->snapshot_id)) {
      dangling_refs.push_back(ref_name);
    }
  }
  for (const auto& ref_name : dangling_refs) {
    ICEBERG_RETURN_UNEXPECTED(RemoveRef(ref_name));
  }

  return {};
}

Status TableMetadataBuilder::Impl::RemovePartitionSpecs(
    const std::vector<int32_t>& spec_ids) {
  if (spec_ids.empty()) {
    return {};
  }

  std::unordered_set<int32_t> spec_ids_to_remove(spec_ids.begin(), spec_ids.end());
  ICEBERG_PRECHECK(!spec_ids_to_remove.contains(metadata_.default_spec_id),
                   "Cannot remove the default partition spec");

  metadata_.partition_specs =
      metadata_.partition_specs | std::views::filter([&](const auto& spec) {
        return !spec_ids_to_remove.contains(spec->spec_id());
      }) |
      std::ranges::to<std::vector<std::shared_ptr<PartitionSpec>>>();
  changes_.push_back(std::make_unique<table::RemovePartitionSpecs>(spec_ids));

  return {};
}

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

TableMetadataBuilder& TableMetadataBuilder::ApplyChangesForCreate(
    const TableMetadata& base) {
  for (auto& change : ChangesForCreate(base)) {
    change->ApplyTo(*this);
  }
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::SetMetadataLocation(
    std::string_view metadata_location) {
  impl_->SetMetadataLocation(metadata_location);
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::SetPreviousMetadataLocation(
    std::string_view previous_metadata_location) {
  impl_->SetPreviousMetadataLocation(previous_metadata_location);
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::AssignUUID() {
  if (!impl_->UUIDSet()) {
    // Generate a random UUID
    return AssignUUID(Uuid::GenerateV4().ToString());
  }
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::AssignUUID(std::string_view uuid) {
  ICEBERG_BUILDER_RETURN_IF_ERROR(impl_->AssignUUID(uuid));
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::UpgradeFormatVersion(
    int8_t new_format_version) {
  ICEBERG_BUILDER_RETURN_IF_ERROR(impl_->UpgradeFormatVersion(new_format_version));
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::SetCurrentSchema(
    std::shared_ptr<Schema> const& schema, int32_t new_last_column_id) {
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto schema_id,
                                   impl_->AddSchema(*schema, new_last_column_id));
  return SetCurrentSchema(schema_id);
}

TableMetadataBuilder& TableMetadataBuilder::SetCurrentSchema(int32_t schema_id) {
  ICEBERG_BUILDER_RETURN_IF_ERROR(impl_->SetCurrentSchema(schema_id));
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::AddSchema(
    std::shared_ptr<Schema> const& schema) {
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto highest_field_id, schema->HighestFieldId());
  auto new_last_column_id = std::max(impl_->metadata().last_column_id, highest_field_id);
  ICEBERG_BUILDER_RETURN_IF_ERROR(impl_->AddSchema(*schema, new_last_column_id));
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::SetDefaultPartitionSpec(
    std::shared_ptr<PartitionSpec> spec) {
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto spec_id, impl_->AddPartitionSpec(*spec));
  return SetDefaultPartitionSpec(spec_id);
}

TableMetadataBuilder& TableMetadataBuilder::SetDefaultPartitionSpec(int32_t spec_id) {
  ICEBERG_BUILDER_RETURN_IF_ERROR(impl_->SetDefaultPartitionSpec(spec_id));
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::AddPartitionSpec(
    std::shared_ptr<PartitionSpec> spec) {
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto spec_id, impl_->AddPartitionSpec(*spec));
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::RemovePartitionSpecs(
    const std::vector<int32_t>& spec_ids) {
  ICEBERG_BUILDER_RETURN_IF_ERROR(impl_->RemovePartitionSpecs(spec_ids));
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::RemoveSchemas(
    const std::unordered_set<int32_t>& schema_ids) {
  ICEBERG_BUILDER_RETURN_IF_ERROR(impl_->RemoveSchemas(schema_ids));
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::SetDefaultSortOrder(
    std::shared_ptr<SortOrder> order) {
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto order_id, impl_->AddSortOrder(*order));
  return SetDefaultSortOrder(order_id);
}

TableMetadataBuilder& TableMetadataBuilder::SetDefaultSortOrder(int32_t order_id) {
  ICEBERG_BUILDER_RETURN_IF_ERROR(impl_->SetDefaultSortOrder(order_id));
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::AddSortOrder(
    std::shared_ptr<SortOrder> order) {
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto order_id, impl_->AddSortOrder(*order));
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::AddSnapshot(
    std::shared_ptr<Snapshot> snapshot) {
  ICEBERG_BUILDER_RETURN_IF_ERROR(impl_->AddSnapshot(std::move(snapshot)));
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::SetBranchSnapshot(int64_t snapshot_id,
                                                              const std::string& branch) {
  ICEBERG_BUILDER_RETURN_IF_ERROR(impl_->SetBranchSnapshot(snapshot_id, branch));
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::SetBranchSnapshot(
    std::shared_ptr<Snapshot> snapshot, const std::string& branch) {
  ICEBERG_BUILDER_RETURN_IF_ERROR(impl_->SetBranchSnapshot(std::move(snapshot), branch));
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::SetRef(const std::string& name,
                                                   std::shared_ptr<SnapshotRef> ref) {
  ICEBERG_BUILDER_RETURN_IF_ERROR(impl_->SetRef(name, std::move(ref)));
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::RemoveRef(const std::string& name) {
  ICEBERG_BUILDER_RETURN_IF_ERROR(impl_->RemoveRef(name));
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::RemoveSnapshots(
    const std::vector<std::shared_ptr<Snapshot>>& snapshots_to_remove) {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

TableMetadataBuilder& TableMetadataBuilder::RemoveSnapshots(
    const std::vector<int64_t>& snapshot_ids) {
  ICEBERG_BUILDER_RETURN_IF_ERROR(impl_->RemoveSnapshots(snapshot_ids));
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::SuppressHistoricalSnapshots() {
  throw IcebergError(std::format("{} not implemented", __FUNCTION__));
}

TableMetadataBuilder& TableMetadataBuilder::SetStatistics(
    std::shared_ptr<StatisticsFile> statistics_file) {
  ICEBERG_BUILDER_RETURN_IF_ERROR(impl_->SetStatistics(std::move(statistics_file)));
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::RemoveStatistics(int64_t snapshot_id) {
  ICEBERG_BUILDER_RETURN_IF_ERROR(impl_->RemoveStatistics(snapshot_id));
  return *this;
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
  ICEBERG_BUILDER_RETURN_IF_ERROR(impl_->SetProperties(updated));
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::RemoveProperties(
    const std::unordered_set<std::string>& removed) {
  ICEBERG_BUILDER_RETURN_IF_ERROR(impl_->RemoveProperties(removed));
  return *this;
}

TableMetadataBuilder& TableMetadataBuilder::SetLocation(std::string_view location) {
  impl_->SetLocation(location);
  return *this;
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
  return impl_->Build();
}

const std::vector<std::unique_ptr<TableUpdate>>& TableMetadataBuilder::changes() const {
  return impl_->changes();
}

const TableMetadata* TableMetadataBuilder::base() const { return impl_->base(); }

const TableMetadata& TableMetadataBuilder::current() const { return impl_->metadata(); }

}  // namespace iceberg
