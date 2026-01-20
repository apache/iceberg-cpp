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

#pragma once

/// \file iceberg/table_metadata.h
/// Table metadata for Iceberg tables.

#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/table_properties.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/error_collector.h"
#include "iceberg/util/lazy.h"
#include "iceberg/util/timepoint.h"

namespace iceberg {

/// \brief Represents a snapshot log entry
struct ICEBERG_EXPORT SnapshotLogEntry {
  /// The timestamp in milliseconds of the change
  TimePointMs timestamp_ms;
  /// ID of the snapshot
  int64_t snapshot_id;

  friend bool operator==(const SnapshotLogEntry& lhs, const SnapshotLogEntry& rhs) {
    return lhs.timestamp_ms == rhs.timestamp_ms && lhs.snapshot_id == rhs.snapshot_id;
  }
};

/// \brief Represents a metadata log entry
struct ICEBERG_EXPORT MetadataLogEntry {
  /// The timestamp in milliseconds of the change
  TimePointMs timestamp_ms;
  /// Metadata file location
  std::string metadata_file;

  friend bool operator==(const MetadataLogEntry& lhs, const MetadataLogEntry& rhs) {
    return lhs.timestamp_ms == rhs.timestamp_ms && lhs.metadata_file == rhs.metadata_file;
  }
};

/// \brief Represents the metadata for an Iceberg table
///
/// Note that it only contains table metadata from the spec.  Compared to the Java
/// implementation, missing pieces including:
/// (1) Map<Integer, Schema|PartitionSpec|SortOrder>
/// (2) List<MetadataUpdate>
/// (3) Map<Long, Snapshot>
/// (4) Map<String, SnapshotRef>
struct ICEBERG_EXPORT TableMetadata {
  static constexpr int8_t kDefaultTableFormatVersion = 2;
  static constexpr int8_t kSupportedTableFormatVersion = 3;
  static constexpr int8_t kMinFormatVersionRowLineage = 3;
  static constexpr int8_t kMinFormatVersionDefaultValues = 3;
  static constexpr int64_t kInitialSequenceNumber = 0;
  static constexpr int64_t kInitialRowId = 0;

  static inline const std::unordered_map<TypeId, int8_t> kMinFormatVersions = {};

  /// An integer version number for the format
  int8_t format_version;
  /// A UUID that identifies the table
  std::string table_uuid;
  /// The table's base location
  std::string location;
  /// The table's highest assigned sequence number
  int64_t last_sequence_number;
  /// Timestamp in milliseconds from the unix epoch when the table was last updated.
  TimePointMs last_updated_ms;
  /// The highest assigned column ID for the table
  int32_t last_column_id;
  /// A list of schemas
  std::vector<std::shared_ptr<iceberg::Schema>> schemas;
  /// ID of the table's current schema
  int32_t current_schema_id;
  /// A list of partition specs
  std::vector<std::shared_ptr<iceberg::PartitionSpec>> partition_specs;
  /// ID of the current partition spec that writers should use by default
  int32_t default_spec_id;
  /// The highest assigned partition field ID across all partition specs for the table
  int32_t last_partition_id;
  /// A string to string map of table properties
  TableProperties properties;
  /// ID of the current table snapshot
  int64_t current_snapshot_id;
  /// A list of valid snapshots
  std::vector<std::shared_ptr<iceberg::Snapshot>> snapshots;
  /// A list of timestamp and snapshot ID pairs that encodes changes to the current
  /// snapshot for the table
  std::vector<SnapshotLogEntry> snapshot_log;
  /// A list of timestamp and metadata file location pairs that encodes changes to the
  /// previous metadata files for the table
  std::vector<MetadataLogEntry> metadata_log;
  /// A list of sort orders
  std::vector<std::shared_ptr<iceberg::SortOrder>> sort_orders;
  /// Default sort order id of the table
  int32_t default_sort_order_id;
  /// A map of snapshot references
  std::unordered_map<std::string, std::shared_ptr<SnapshotRef>> refs;
  /// A list of table statistics
  std::vector<std::shared_ptr<struct StatisticsFile>> statistics;
  /// A list of partition statistics
  std::vector<std::shared_ptr<struct PartitionStatisticsFile>> partition_statistics;
  /// A `long` higher than all assigned row IDs
  int64_t next_row_id;

  static Result<std::unique_ptr<TableMetadata>> Make(
      const iceberg::Schema& schema, const iceberg::PartitionSpec& spec,
      const iceberg::SortOrder& sort_order, const std::string& location,
      const std::unordered_map<std::string, std::string>& properties,
      int format_version = kDefaultTableFormatVersion);

  /// \brief Get the current schema, return NotFoundError if not found
  /// \note The returned schema is guaranteed to be not null
  Result<std::shared_ptr<iceberg::Schema>> Schema() const;
  /// \brief Get the current schema by ID, return NotFoundError if not found
  /// \note The returned schema is guaranteed to be not null
  Result<std::shared_ptr<iceberg::Schema>> SchemaById(int32_t schema_id) const;
  /// \brief Get the current partition spec, return NotFoundError if not found
  /// \note The returned partition spec is guaranteed to be not null
  Result<std::shared_ptr<iceberg::PartitionSpec>> PartitionSpec() const;
  /// \brief Get the current partition spec by ID, return NotFoundError if not found
  /// \note The returned partition spec is guaranteed to be not null
  Result<std::shared_ptr<iceberg::PartitionSpec>> PartitionSpecById(
      int32_t spec_id) const;
  /// \brief Get the current sort order, return NotFoundError if not found
  /// \note The returned sort order is guaranteed to be not null
  Result<std::shared_ptr<iceberg::SortOrder>> SortOrder() const;
  /// \brief Get the current sort order by ID, return NotFoundError if not found
  /// \note The returned sort order is guaranteed to be not null
  Result<std::shared_ptr<iceberg::SortOrder>> SortOrderById(int32_t sort_order_id) const;
  /// \brief Get the current snapshot, return NotFoundError if not found
  /// \note The returned snapshot is guaranteed to be not null
  Result<std::shared_ptr<iceberg::Snapshot>> Snapshot() const;
  /// \brief Get the snapshot by ID, return NotFoundError if not found
  /// \note The returned snapshot is guaranteed to be not null
  Result<std::shared_ptr<iceberg::Snapshot>> SnapshotById(int64_t snapshot_id) const;
  /// \brief Get the next sequence number
  int64_t NextSequenceNumber() const;

  ICEBERG_EXPORT friend bool operator==(const TableMetadata& lhs,
                                        const TableMetadata& rhs);
};

// Cache for table metadata mappings to facilitate fast lookups.
class ICEBERG_EXPORT TableMetadataCache {
 public:
  explicit TableMetadataCache(const TableMetadata* metadata) : metadata_(metadata) {}

  template <typename T>
  using ByIdMap = std::unordered_map<int32_t, std::shared_ptr<T>>;
  using SchemasMap = ByIdMap<Schema>;
  using PartitionSpecsMap = ByIdMap<PartitionSpec>;
  using SortOrdersMap = ByIdMap<SortOrder>;
  using SnapshotsMap = std::unordered_map<int64_t, std::shared_ptr<Snapshot>>;
  using SchemasMapRef = std::reference_wrapper<const SchemasMap>;
  using PartitionSpecsMapRef = std::reference_wrapper<const PartitionSpecsMap>;
  using SortOrdersMapRef = std::reference_wrapper<const SortOrdersMap>;
  using SnapshotsMapRef = std::reference_wrapper<const SnapshotsMap>;

  Result<SchemasMapRef> GetSchemasById() const;
  Result<PartitionSpecsMapRef> GetPartitionSpecsById() const;
  Result<SortOrdersMapRef> GetSortOrdersById() const;
  Result<SnapshotsMapRef> GetSnapshotsById() const;

 private:
  static Result<SchemasMap> InitSchemasMap(const TableMetadata* metadata);
  static Result<PartitionSpecsMap> InitPartitionSpecsMap(const TableMetadata* metadata);
  static Result<SortOrdersMap> InitSortOrdersMap(const TableMetadata* metadata);
  static Result<SnapshotsMap> InitSnapshotMap(const TableMetadata* metadata);

  const TableMetadata* metadata_;
  Lazy<InitSchemasMap> schemas_map_;
  Lazy<InitPartitionSpecsMap> partition_specs_map_;
  Lazy<InitSortOrdersMap> sort_orders_map_;
  Lazy<InitSnapshotMap> snapshot_map_;
};

/// \brief Returns a string representation of a SnapshotLogEntry
ICEBERG_EXPORT std::string ToString(const SnapshotLogEntry& entry);

/// \brief Returns a string representation of a MetadataLogEntry
ICEBERG_EXPORT std::string ToString(const MetadataLogEntry& entry);

/// \brief Builder class for constructing TableMetadata objects
///
/// This builder provides a fluent interface for creating and modifying table metadata.
/// It supports both creating new tables and building from existing metadata.
///
/// Each modification method generates a corresponding TableUpdate that is tracked
/// in a changes list. This allows the builder to maintain a complete history of all
/// modifications made to the table metadata, which is important for tracking table
/// evolution and for serialization purposes.
///
/// If a modification violates Iceberg table constraints (e.g., setting a current
/// schema ID that does not exist), an error will be recorded and returned when
/// Build() is called.
class ICEBERG_EXPORT TableMetadataBuilder : public ErrorCollector {
 public:
  /// \brief Create a builder for a new table
  ///
  /// \param format_version The format version for the table
  /// \return A new TableMetadataBuilder instance
  static std::unique_ptr<TableMetadataBuilder> BuildFromEmpty(
      int8_t format_version = TableMetadata::kDefaultTableFormatVersion);

  /// \brief Create a builder from existing table metadata
  ///
  /// \param base The base table metadata to build from
  /// \return A new TableMetadataBuilder instance initialized
  /// with base metadata
  static std::unique_ptr<TableMetadataBuilder> BuildFrom(const TableMetadata* base);

  /// \brief Apply changes required to create this table metadata
  ///
  /// \param base The table metadata to build from
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& ApplyChangesForCreate(const TableMetadata& base);

  /// \brief Set the metadata location of the table
  ///
  /// \param metadata_location The new metadata location
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& SetMetadataLocation(std::string_view metadata_location);

  /// \brief Set the previous metadata location of the table
  ///
  /// \param previous_metadata_location The previous metadata location
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& SetPreviousMetadataLocation(
      std::string_view previous_metadata_location);

  /// \brief Assign a UUID to the table
  ///
  /// If no UUID is provided, a random UUID will be generated.
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& AssignUUID();

  /// \brief Assign a specific UUID to the table
  ///
  /// \param uuid The UUID string to assign
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& AssignUUID(std::string_view uuid);

  /// \brief Upgrade the format version of the table
  ///
  /// \param new_format_version The new format version (must be >= current version)
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& UpgradeFormatVersion(int8_t new_format_version);

  /// \brief Set the current schema for the table
  ///
  /// \param schema The schema to set as current
  /// \param new_last_column_id The highest column ID in the schema
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& SetCurrentSchema(const std::shared_ptr<Schema>& schema,
                                         int32_t new_last_column_id);

  /// \brief Set the current schema by schema ID
  ///
  /// \param schema_id The ID of the schema to set as current
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& SetCurrentSchema(int32_t schema_id);

  /// \brief Add a schema to the table
  ///
  /// \param schema The schema to add
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& AddSchema(const std::shared_ptr<Schema>& schema);

  /// \brief Set the default partition spec for the table
  ///
  /// \param spec The partition spec to set as default
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& SetDefaultPartitionSpec(std::shared_ptr<PartitionSpec> spec);

  /// \brief Set the default partition spec by spec ID
  ///
  /// \param spec_id The ID of the partition spec to set as default
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& SetDefaultPartitionSpec(int32_t spec_id);

  /// \brief Add a partition spec to the table
  ///
  /// \param spec The partition spec to add
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& AddPartitionSpec(std::shared_ptr<PartitionSpec> spec);

  /// \brief Remove partition specs from the table
  ///
  /// \param spec_ids The IDs of partition specs to remove
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& RemovePartitionSpecs(const std::vector<int32_t>& spec_ids);

  /// \brief Remove schemas from the table
  ///
  /// \param schema_ids The IDs of schemas to remove
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& RemoveSchemas(const std::unordered_set<int32_t>& schema_ids);

  /// \brief Set the default sort order for the table
  ///
  /// \param order The sort order to set as default
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& SetDefaultSortOrder(std::shared_ptr<SortOrder> order);

  /// \brief Set the default sort order by order ID
  ///
  /// \param order_id The ID of the sort order to set as default
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& SetDefaultSortOrder(int32_t order_id);

  /// \brief Add a sort order to the table
  ///
  /// \param order The sort order to add
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& AddSortOrder(std::shared_ptr<SortOrder> order);

  /// \brief Add a snapshot to the table
  ///
  /// \param snapshot The snapshot to add
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& AddSnapshot(std::shared_ptr<Snapshot> snapshot);

  /// \brief Set a branch to point to a specific snapshot
  ///
  /// \param snapshot_id The snapshot ID the branch should reference
  /// \param branch The name of the branch
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& SetBranchSnapshot(int64_t snapshot_id, const std::string& branch);

  /// \brief Set a branch to point to a specific snapshot
  ///
  /// \param snapshot The snapshot the branch should reference
  /// \param branch The name of the branch
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& SetBranchSnapshot(std::shared_ptr<Snapshot> snapshot,
                                          const std::string& branch);

  /// \brief Set a snapshot reference
  ///
  /// \param name The name of the reference
  /// \param ref The snapshot reference to set
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& SetRef(const std::string& name, std::shared_ptr<SnapshotRef> ref);

  /// \brief Remove a snapshot reference
  ///
  /// \param name The name of the reference to remove
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& RemoveRef(const std::string& name);

  /// \brief Remove snapshots from the table
  ///
  /// \param snapshots_to_remove The snapshots to remove
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& RemoveSnapshots(
      const std::vector<std::shared_ptr<Snapshot>>& snapshots_to_remove);

  /// \brief Remove snapshots from the table
  ///
  /// \param snapshot_ids The IDs of snapshots to remove
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& RemoveSnapshots(const std::vector<int64_t>& snapshot_ids);

  /// \brief  Suppresses snapshots that are historical, removing the metadata for lazy
  /// snapshot loading.
  ///
  /// Note that the snapshots are not considered removed from metadata and no
  /// RemoveSnapshot changes are created. A snapshot is historical if no ref directly
  /// references its ID.
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& SuppressHistoricalSnapshots();

  /// \brief Set table statistics
  ///
  /// \param statistics_file The statistics file to set
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& SetStatistics(
      const std::shared_ptr<StatisticsFile>& statistics_file);

  /// \brief Remove table statistics by snapshot ID
  ///
  /// \param snapshot_id The snapshot ID whose statistics to remove
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& RemoveStatistics(int64_t snapshot_id);

  /// \brief Set partition statistics
  ///
  /// \param partition_statistics_file The partition statistics file to set
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& SetPartitionStatistics(
      const std::shared_ptr<PartitionStatisticsFile>& partition_statistics_file);

  /// \brief Remove partition statistics by snapshot ID
  ///
  /// \param snapshot_id The snapshot ID whose partition statistics to remove
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& RemovePartitionStatistics(int64_t snapshot_id);

  /// \brief Set table properties
  ///
  /// \param updated Map of properties to set or update
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& SetProperties(
      const std::unordered_map<std::string, std::string>& updated);

  /// \brief Remove table properties
  ///
  /// \param removed Set of property keys to remove
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& RemoveProperties(const std::unordered_set<std::string>& removed);

  /// \brief Set the table location
  ///
  /// \param location The table base location
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& SetLocation(std::string_view location);

  /// \brief Add an encryption key to the table
  ///
  /// \param key The encryption key to add
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& AddEncryptionKey(std::shared_ptr<EncryptedKey> key);

  /// \brief Remove an encryption key from the table by key ID
  ///
  /// \param key_id The ID of the encryption key to remove
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& RemoveEncryptionKey(std::string_view key_id);

  /// \brief Build the TableMetadata object
  ///
  /// \return A Result containing the constructed TableMetadata or an error
  Result<std::unique_ptr<TableMetadata>> Build();

  /// \brief Returns the changes made to the table metadata
  const std::vector<std::unique_ptr<TableUpdate>>& changes() const;

  /// \brief Returns the base metadata without any changes
  const TableMetadata* base() const;

  /// \brief Returns the current metadata with staged changes applied
  const TableMetadata& current() const;

  /// \brief Destructor
  ~TableMetadataBuilder() override;

  // Delete copy operations (use BuildFrom to create a new builder)
  TableMetadataBuilder(const TableMetadataBuilder&) = delete;
  TableMetadataBuilder& operator=(const TableMetadataBuilder&) = delete;

  // Enable move operations
  TableMetadataBuilder(TableMetadataBuilder&&) noexcept;
  TableMetadataBuilder& operator=(TableMetadataBuilder&&) noexcept;

 private:
  /// \brief Private constructor for building from empty state
  explicit TableMetadataBuilder(int8_t format_version);

  /// \brief Private constructor for building from existing metadata
  explicit TableMetadataBuilder(const TableMetadata* base);

  /// Internal state members
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

/// \brief The codec type of the table metadata file.
enum class ICEBERG_EXPORT MetadataFileCodecType {
  kNone,
  kGzip,
};

/// \brief Utility class for table metadata
struct ICEBERG_EXPORT TableMetadataUtil {
  struct ICEBERG_EXPORT Codec {
    /// \brief Returns the MetadataFileCodecType corresponding to the given string.
    ///
    /// \param name The string to parse.
    /// \return The MetadataFileCodecType corresponding to the given string.
    static Result<MetadataFileCodecType> FromString(std::string_view name);

    /// \brief Get the codec type from the table metadata file name.
    ///
    /// \param file_name The name of the table metadata file.
    /// \return The codec type of the table metadata file.
    static Result<MetadataFileCodecType> FromFileName(std::string_view file_name);

    /// \brief Get the file extension from the codec type.
    /// \param codec The codec name.
    /// \return The file extension of the codec.
    static Result<std::string> NameToFileExtension(std::string_view codec);

    /// \brief Get the file extension from the codec type.
    /// \param codec The codec type.
    /// \return The file extension of the codec.
    static std::string TypeToFileExtension(MetadataFileCodecType codec);

    static constexpr std::string_view kTableMetadataFileSuffix = ".metadata.json";
    static constexpr std::string_view kCompGzipTableMetadataFileSuffix =
        ".metadata.json.gz";
    static constexpr std::string_view kGzipTableMetadataFileSuffix = ".gz.metadata.json";
    static constexpr std::string_view kGzipTableMetadataFileExtension = ".gz";
    static constexpr std::string_view kCodecTypeGzip = "GZIP";
    static constexpr std::string_view kCodecTypeNone = "NONE";
  };

  /// \brief Read the table metadata file.
  ///
  /// \param io The file IO to use to read the table metadata.
  /// \param location The location of the table metadata file.
  /// \param length The optional length of the table metadata file.
  /// \return The table metadata.
  static Result<std::unique_ptr<TableMetadata>> Read(
      class FileIO& io, const std::string& location,
      std::optional<size_t> length = std::nullopt);

  /// \brief Write a new metadata file to storage.
  ///
  /// Serializes the table metadata to JSON and writes it to a new metadata
  /// file. If no location is specified in the metadata, generates a new
  /// file path based on the version number.
  ///
  /// \param io The FileIO instance for writing files
  /// \param base The base metadata (can be null for new tables)
  /// \param metadata The metadata to write, which will be updated with the new location
  /// \return The new metadata location
  static Result<std::string> Write(FileIO& io, const TableMetadata* base,
                                   const std::string& base_metadata_location,
                                   TableMetadata& metadata);

  /// \brief Delete removed metadata files based on retention policy.
  ///
  /// Removes obsolete metadata files that are no longer referenced in the
  /// current metadata log, based on the metadata.delete-after-commit.enabled
  /// property.
  ///
  /// \param io The FileIO instance for deleting files
  /// \param base The previous metadata version
  /// \param metadata The current metadata containing the updated log
  static void DeleteRemovedMetadataFiles(FileIO& io, const TableMetadata* base,
                                         const TableMetadata& metadata);

  /// \brief Write the table metadata to a file.
  ///
  /// \param io The file IO to use to write the table metadata.
  /// \param location The location of the table metadata file.
  /// \param metadata The table metadata to write.
  static Status Write(FileIO& io, const std::string& location,
                      const TableMetadata& metadata);

 private:
  /// \brief Parse the version number from a metadata file location.
  ///
  /// Extracts the version number from a metadata file path which follows
  /// the format: vvvvv-uuid.metadata.json where vvvvv is the zero-padded
  /// version number.
  ///
  /// \param metadata_location The metadata file location string
  /// \return The parsed version number, or -1 if parsing fails or the
  ///         location doesn't contain a version
  static int32_t ParseVersionFromLocation(std::string_view metadata_location);

  /// \brief Generate a new metadata file path for a table.
  ///
  /// \param metadata The table metadata.
  /// \param version The version number for the new metadata file.
  /// \return The generated metadata file path.
  static Result<std::string> NewTableMetadataFilePath(const TableMetadata& metadata,
                                                      int32_t version);
};

}  // namespace iceberg

namespace std {
template <>
struct hash<iceberg::MetadataLogEntry> {
  size_t operator()(const iceberg::MetadataLogEntry& m) const noexcept {
    return std::hash<std::string>{}(m.metadata_file);
  }
};

}  // namespace std
