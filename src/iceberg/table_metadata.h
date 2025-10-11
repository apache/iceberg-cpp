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
#include "iceberg/metadata_update.h"
#include "iceberg/type_fwd.h"
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
  static constexpr int64_t kInitialSequenceNumber = 0;
  static constexpr int64_t kInvalidSequenceNumber = -1;
  static constexpr int64_t kInitialRowId = 0;

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
  std::optional<int32_t> current_schema_id;
  /// A list of partition specs
  std::vector<std::shared_ptr<iceberg::PartitionSpec>> partition_specs;
  /// ID of the current partition spec that writers should use by default
  int32_t default_spec_id;
  /// The highest assigned partition field ID across all partition specs for the table
  int32_t last_partition_id;
  /// A string to string map of table properties
  std::unordered_map<std::string, std::string> properties;
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

  /// \brief Get the current schema, return NotFoundError if not found
  Result<std::shared_ptr<iceberg::Schema>> Schema() const;
  /// \brief Get the current schema by ID, return NotFoundError if not found
  Result<std::shared_ptr<iceberg::Schema>> SchemaById(
      const std::optional<int32_t>& schema_id) const;
  /// \brief Get the current partition spec, return NotFoundError if not found
  Result<std::shared_ptr<iceberg::PartitionSpec>> PartitionSpec() const;
  /// \brief Get the current sort order, return NotFoundError if not found
  Result<std::shared_ptr<iceberg::SortOrder>> SortOrder() const;
  /// \brief Get the current snapshot, return NotFoundError if not found
  Result<std::shared_ptr<iceberg::Snapshot>> Snapshot() const;
  /// \brief Get the snapshot of this table with the given id
  Result<std::shared_ptr<iceberg::Snapshot>> SnapshotById(int64_t snapshot_id) const;

  friend bool operator==(const TableMetadata& lhs, const TableMetadata& rhs);
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
/// Each modification method generates a corresponding MetadataUpdate that is tracked
/// in a changes list. This allows the builder to maintain a complete history of all
/// modifications made to the table metadata, which is important for tracking table
/// evolution and for serialization purposes.
class ICEBERG_EXPORT TableMetadataBuilder {
 public:
  /// \brief Create a builder for a new table
  ///
  /// \param format_version The format version for the table
  /// \return A new TableMetadataBuilder instance
  static TableMetadataBuilder BuildFromEmpty(
      int8_t format_version = TableMetadata::kDefaultTableFormatVersion);

  /// \brief Create a builder from existing table metadata
  ///
  /// \param base The base table metadata to build from
  /// \return A new TableMetadataBuilder instance initialized with base metadata
  static TableMetadataBuilder BuildFrom(const std::shared_ptr<const TableMetadata>& base);

  /// \brief Assign a UUID to the table
  ///
  /// If no UUID is provided, a random UUID will be generated.
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& AssignUUID();

  /// \brief Assign a specific UUID to the table
  ///
  /// \param uuid The UUID string to assign
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& AssignUUID(const std::string& uuid);

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
  TableMetadataBuilder& SetCurrentSchema(std::shared_ptr<iceberg::Schema> schema,
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
  TableMetadataBuilder& AddSchema(std::shared_ptr<iceberg::Schema> schema);

  /// \brief Set the default partition spec for the table
  ///
  /// \param spec The partition spec to set as default
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& SetDefaultPartitionSpec(
      std::shared_ptr<iceberg::PartitionSpec> spec);

  /// \brief Set the default partition spec by spec ID
  ///
  /// \param spec_id The ID of the partition spec to set as default
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& SetDefaultPartitionSpec(int32_t spec_id);

  /// \brief Add a partition spec to the table
  ///
  /// \param spec The partition spec to add
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& AddPartitionSpec(std::shared_ptr<iceberg::PartitionSpec> spec);

  /// \brief Remove partition specs from the table
  ///
  /// \param spec_ids The IDs of partition specs to remove
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& RemovePartitionSpecs(const std::vector<int32_t>& spec_ids);

  /// \brief Remove schemas from the table
  ///
  /// \param schema_ids The IDs of schemas to remove
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& RemoveSchemas(const std::vector<int32_t>& schema_ids);

  /// \brief Set the default sort order for the table
  ///
  /// \param order The sort order to set as default
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& SetDefaultSortOrder(std::shared_ptr<iceberg::SortOrder> order);

  /// \brief Set the default sort order by order ID
  ///
  /// \param order_id The ID of the sort order to set as default
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& SetDefaultSortOrder(int32_t order_id);

  /// \brief Add a sort order to the table
  ///
  /// \param order The sort order to add
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& AddSortOrder(std::shared_ptr<iceberg::SortOrder> order);

  /// \brief Add a snapshot to the table
  ///
  /// \param snapshot The snapshot to add
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& AddSnapshot(std::shared_ptr<iceberg::Snapshot> snapshot);

  /// \brief Set a branch to point to a specific snapshot
  ///
  /// \param snapshot_id The snapshot ID the branch should reference
  /// \param branch The name of the branch
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& SetBranchSnapshot(int64_t snapshot_id, const std::string& branch);

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
  /// \param snapshot_ids The IDs of snapshots to remove
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& RemoveSnapshots(const std::vector<int64_t>& snapshot_ids);

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
  TableMetadataBuilder& RemoveProperties(const std::vector<std::string>& removed);

  /// \brief Set the table location
  ///
  /// \param location The table base location
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& SetLocation(const std::string& location);

  /// \brief Discard all accumulated changes
  ///
  /// This is useful when you want to reset the builder state without
  /// creating a new builder instance.
  /// \return Reference to this builder for method chaining
  TableMetadataBuilder& DiscardChanges();

  /// \brief Build the TableMetadata object
  ///
  /// \return A Result containing the constructed TableMetadata or an error
  Result<std::unique_ptr<TableMetadata>> Build();

  /// \brief Destructor
  ~TableMetadataBuilder();

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
  explicit TableMetadataBuilder(std::shared_ptr<const TableMetadata> base);

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
  /// \brief Get the codec type from the table metadata file name.
  ///
  /// \param file_name The name of the table metadata file.
  /// \return The codec type of the table metadata file.
  static Result<MetadataFileCodecType> CodecFromFileName(std::string_view file_name);

  /// \brief Read the table metadata file.
  ///
  /// \param io The file IO to use to read the table metadata.
  /// \param location The location of the table metadata file.
  /// \param length The optional length of the table metadata file.
  /// \return The table metadata.
  static Result<std::unique_ptr<TableMetadata>> Read(
      class FileIO& io, const std::string& location,
      std::optional<size_t> length = std::nullopt);

  /// \brief Write the table metadata to a file.
  ///
  /// \param io The file IO to use to write the table metadata.
  /// \param location The location of the table metadata file.
  /// \param metadata The table metadata to write.
  static Status Write(FileIO& io, const std::string& location,
                      const TableMetadata& metadata);
};

}  // namespace iceberg
