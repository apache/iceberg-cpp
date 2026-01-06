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

/// \file iceberg/table_update.h
/// Table metadata update operations for Iceberg tables.

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/snapshot.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Base class for metadata update operations
///
/// Represents a change to table metadata. Each concrete subclass
/// represents a specific type of update operation.
class ICEBERG_EXPORT TableUpdate {
 public:
  enum class Kind : uint8_t {
    kAssignUUID,
    kUpgradeFormatVersion,
    kAddSchema,
    kSetCurrentSchema,
    kAddPartitionSpec,
    kSetDefaultPartitionSpec,
    kRemovePartitionSpecs,
    kRemoveSchemas,
    kAddSortOrder,
    kSetDefaultSortOrder,
    kAddSnapshot,
    kRemoveSnapshots,
    kRemoveSnapshotRef,
    kSetSnapshotRef,
    kSetProperties,
    kRemoveProperties,
    kSetLocation,
  };

  virtual ~TableUpdate();

  /// \brief Return the kind of this update.
  virtual Kind kind() const = 0;

  /// \brief Apply this update to a TableMetadataBuilder
  ///
  /// This method modifies the builder by applying the update operation
  /// it represents. Each subclass implements this to apply its specific
  /// type of update.
  ///
  /// \param builder The builder to apply this update to
  virtual void ApplyTo(TableMetadataBuilder& builder) const = 0;

  /// \brief Generate update requirements for this metadata update
  ///
  /// This method generates the appropriate UpdateRequirement instances
  /// that must be validated before this update can be applied. The context
  /// provides information about the base metadata and operation mode.
  ///
  /// \param context The context containing base metadata and operation state
  virtual void GenerateRequirements(TableUpdateContext& context) const = 0;

  /// \brief Check equality with another TableUpdate
  ///
  /// \param other The update to compare with
  /// \return true if the updates are equal, false otherwise
  virtual bool Equals(const TableUpdate& other) const = 0;

  /// \brief Create a deep copy of this update
  ///
  /// \return A unique_ptr to a new TableUpdate that is a copy of this one
  virtual std::unique_ptr<TableUpdate> Clone() const = 0;

  /// \brief Compare two TableUpdate instances for equality
  friend bool operator==(const TableUpdate& lhs, const TableUpdate& rhs) {
    return lhs.Equals(rhs);
  }
};

namespace table {

/// \brief Represents an assignment of a UUID to the table
class ICEBERG_EXPORT AssignUUID : public TableUpdate {
 public:
  explicit AssignUUID(std::string uuid) : uuid_(std::move(uuid)) {}

  const std::string& uuid() const { return uuid_; }

  void ApplyTo(TableMetadataBuilder& builder) const override;

  void GenerateRequirements(TableUpdateContext& context) const override;

  Kind kind() const override { return Kind::kAssignUUID; }

  bool Equals(const TableUpdate& other) const override;

  std::unique_ptr<TableUpdate> Clone() const override;

 private:
  std::string uuid_;
};

/// \brief Represents an upgrade of the table format version
class ICEBERG_EXPORT UpgradeFormatVersion : public TableUpdate {
 public:
  explicit UpgradeFormatVersion(int8_t format_version)
      : format_version_(format_version) {}

  int8_t format_version() const { return format_version_; }

  void ApplyTo(TableMetadataBuilder& builder) const override;

  void GenerateRequirements(TableUpdateContext& context) const override;

  Kind kind() const override { return Kind::kUpgradeFormatVersion; }

  bool Equals(const TableUpdate& other) const override;

  std::unique_ptr<TableUpdate> Clone() const override;

 private:
  int8_t format_version_;
};

/// \brief Represents adding a new schema to the table
class ICEBERG_EXPORT AddSchema : public TableUpdate {
 public:
  explicit AddSchema(std::shared_ptr<Schema> schema, int32_t last_column_id)
      : schema_(std::move(schema)), last_column_id_(last_column_id) {}

  const std::shared_ptr<Schema>& schema() const { return schema_; }

  int32_t last_column_id() const { return last_column_id_; }

  void ApplyTo(TableMetadataBuilder& builder) const override;

  void GenerateRequirements(TableUpdateContext& context) const override;

  Kind kind() const override { return Kind::kAddSchema; }

  bool Equals(const TableUpdate& other) const override;

  std::unique_ptr<TableUpdate> Clone() const override;

 private:
  std::shared_ptr<Schema> schema_;
  int32_t last_column_id_;
};

/// \brief Represents setting the current schema
class ICEBERG_EXPORT SetCurrentSchema : public TableUpdate {
 public:
  explicit SetCurrentSchema(int32_t schema_id) : schema_id_(schema_id) {}

  int32_t schema_id() const { return schema_id_; }

  void ApplyTo(TableMetadataBuilder& builder) const override;

  void GenerateRequirements(TableUpdateContext& context) const override;

  Kind kind() const override { return Kind::kSetCurrentSchema; }

  bool Equals(const TableUpdate& other) const override;

  std::unique_ptr<TableUpdate> Clone() const override;

 private:
  int32_t schema_id_;
};

/// \brief Represents adding a new partition spec to the table
class ICEBERG_EXPORT AddPartitionSpec : public TableUpdate {
 public:
  explicit AddPartitionSpec(std::shared_ptr<PartitionSpec> spec)
      : spec_(std::move(spec)) {}

  const std::shared_ptr<PartitionSpec>& spec() const { return spec_; }

  void ApplyTo(TableMetadataBuilder& builder) const override;

  void GenerateRequirements(TableUpdateContext& context) const override;

  Kind kind() const override { return Kind::kAddPartitionSpec; }

  bool Equals(const TableUpdate& other) const override;

  std::unique_ptr<TableUpdate> Clone() const override;

 private:
  std::shared_ptr<PartitionSpec> spec_;
};

/// \brief Represents setting the default partition spec
class ICEBERG_EXPORT SetDefaultPartitionSpec : public TableUpdate {
 public:
  explicit SetDefaultPartitionSpec(int32_t spec_id) : spec_id_(spec_id) {}

  int32_t spec_id() const { return spec_id_; }

  void ApplyTo(TableMetadataBuilder& builder) const override;

  void GenerateRequirements(TableUpdateContext& context) const override;

  Kind kind() const override { return Kind::kSetDefaultPartitionSpec; }

  bool Equals(const TableUpdate& other) const override;

  std::unique_ptr<TableUpdate> Clone() const override;

 private:
  int32_t spec_id_;
};

/// \brief Represents removing partition specs from the table
class ICEBERG_EXPORT RemovePartitionSpecs : public TableUpdate {
 public:
  explicit RemovePartitionSpecs(std::vector<int32_t> spec_ids)
      : spec_ids_(std::move(spec_ids)) {}

  const std::vector<int32_t>& spec_ids() const { return spec_ids_; }

  void ApplyTo(TableMetadataBuilder& builder) const override;

  void GenerateRequirements(TableUpdateContext& context) const override;

  Kind kind() const override { return Kind::kRemovePartitionSpecs; }

  bool Equals(const TableUpdate& other) const override;

  std::unique_ptr<TableUpdate> Clone() const override;

 private:
  std::vector<int32_t> spec_ids_;
};

/// \brief Represents removing schemas from the table
class ICEBERG_EXPORT RemoveSchemas : public TableUpdate {
 public:
  explicit RemoveSchemas(std::unordered_set<int32_t> schema_ids)
      : schema_ids_(std::move(schema_ids)) {}

  const std::unordered_set<int32_t>& schema_ids() const { return schema_ids_; }

  void ApplyTo(TableMetadataBuilder& builder) const override;

  void GenerateRequirements(TableUpdateContext& context) const override;

  Kind kind() const override { return Kind::kRemoveSchemas; }

  bool Equals(const TableUpdate& other) const override;

  std::unique_ptr<TableUpdate> Clone() const override;

 private:
  std::unordered_set<int32_t> schema_ids_;
};

/// \brief Represents adding a new sort order to the table
class ICEBERG_EXPORT AddSortOrder : public TableUpdate {
 public:
  explicit AddSortOrder(std::shared_ptr<SortOrder> sort_order)
      : sort_order_(std::move(sort_order)) {}

  const std::shared_ptr<SortOrder>& sort_order() const { return sort_order_; }

  void ApplyTo(TableMetadataBuilder& builder) const override;

  void GenerateRequirements(TableUpdateContext& context) const override;

  Kind kind() const override { return Kind::kAddSortOrder; }

  bool Equals(const TableUpdate& other) const override;

  std::unique_ptr<TableUpdate> Clone() const override;

 private:
  std::shared_ptr<SortOrder> sort_order_;
};

/// \brief Represents setting the default sort order
class ICEBERG_EXPORT SetDefaultSortOrder : public TableUpdate {
 public:
  explicit SetDefaultSortOrder(int32_t sort_order_id) : sort_order_id_(sort_order_id) {}

  int32_t sort_order_id() const { return sort_order_id_; }

  void ApplyTo(TableMetadataBuilder& builder) const override;

  void GenerateRequirements(TableUpdateContext& context) const override;

  Kind kind() const override { return Kind::kSetDefaultSortOrder; }

  bool Equals(const TableUpdate& other) const override;

  std::unique_ptr<TableUpdate> Clone() const override;

 private:
  int32_t sort_order_id_;
};

/// \brief Represents adding a snapshot to the table
class ICEBERG_EXPORT AddSnapshot : public TableUpdate {
 public:
  explicit AddSnapshot(std::shared_ptr<Snapshot> snapshot)
      : snapshot_(std::move(snapshot)) {}

  const std::shared_ptr<Snapshot>& snapshot() const { return snapshot_; }

  void ApplyTo(TableMetadataBuilder& builder) const override;

  void GenerateRequirements(TableUpdateContext& context) const override;

  Kind kind() const override { return Kind::kAddSnapshot; }

  bool Equals(const TableUpdate& other) const override;

  std::unique_ptr<TableUpdate> Clone() const override;

 private:
  std::shared_ptr<Snapshot> snapshot_;
};

/// \brief Represents removing snapshots from the table
class ICEBERG_EXPORT RemoveSnapshots : public TableUpdate {
 public:
  explicit RemoveSnapshots(std::vector<int64_t> snapshot_ids)
      : snapshot_ids_(std::move(snapshot_ids)) {}

  const std::vector<int64_t>& snapshot_ids() const { return snapshot_ids_; }

  void ApplyTo(TableMetadataBuilder& builder) const override;

  void GenerateRequirements(TableUpdateContext& context) const override;

  Kind kind() const override { return Kind::kRemoveSnapshots; }

  bool Equals(const TableUpdate& other) const override;

  std::unique_ptr<TableUpdate> Clone() const override;

 private:
  std::vector<int64_t> snapshot_ids_;
};

/// \brief Represents removing a snapshot reference
class ICEBERG_EXPORT RemoveSnapshotRef : public TableUpdate {
 public:
  explicit RemoveSnapshotRef(std::string ref_name) : ref_name_(std::move(ref_name)) {}

  const std::string& ref_name() const { return ref_name_; }

  void ApplyTo(TableMetadataBuilder& builder) const override;

  void GenerateRequirements(TableUpdateContext& context) const override;

  Kind kind() const override { return Kind::kRemoveSnapshotRef; }

  bool Equals(const TableUpdate& other) const override;

  std::unique_ptr<TableUpdate> Clone() const override;

 private:
  std::string ref_name_;
};

/// \brief Represents setting a snapshot reference
class ICEBERG_EXPORT SetSnapshotRef : public TableUpdate {
 public:
  SetSnapshotRef(std::string ref_name, int64_t snapshot_id, SnapshotRefType type,
                 std::optional<int32_t> min_snapshots_to_keep = std::nullopt,
                 std::optional<int64_t> max_snapshot_age_ms = std::nullopt,
                 std::optional<int64_t> max_ref_age_ms = std::nullopt)
      : ref_name_(std::move(ref_name)),
        snapshot_id_(snapshot_id),
        type_(type),
        min_snapshots_to_keep_(min_snapshots_to_keep),
        max_snapshot_age_ms_(max_snapshot_age_ms),
        max_ref_age_ms_(max_ref_age_ms) {}

  const std::string& ref_name() const { return ref_name_; }
  int64_t snapshot_id() const { return snapshot_id_; }
  SnapshotRefType type() const { return type_; }
  const std::optional<int32_t>& min_snapshots_to_keep() const {
    return min_snapshots_to_keep_;
  }
  const std::optional<int64_t>& max_snapshot_age_ms() const {
    return max_snapshot_age_ms_;
  }
  const std::optional<int64_t>& max_ref_age_ms() const { return max_ref_age_ms_; }

  void ApplyTo(TableMetadataBuilder& builder) const override;

  void GenerateRequirements(TableUpdateContext& context) const override;

  Kind kind() const override { return Kind::kSetSnapshotRef; }

  bool Equals(const TableUpdate& other) const override;

  std::unique_ptr<TableUpdate> Clone() const override;

 private:
  std::string ref_name_;
  int64_t snapshot_id_;
  SnapshotRefType type_;
  std::optional<int32_t> min_snapshots_to_keep_;
  std::optional<int64_t> max_snapshot_age_ms_;
  std::optional<int64_t> max_ref_age_ms_;
};

/// \brief Represents setting table properties
class ICEBERG_EXPORT SetProperties : public TableUpdate {
 public:
  explicit SetProperties(std::unordered_map<std::string, std::string> updated)
      : updated_(std::move(updated)) {}

  const std::unordered_map<std::string, std::string>& updated() const { return updated_; }

  void ApplyTo(TableMetadataBuilder& builder) const override;

  void GenerateRequirements(TableUpdateContext& context) const override;

  Kind kind() const override { return Kind::kSetProperties; }

  bool Equals(const TableUpdate& other) const override;

  std::unique_ptr<TableUpdate> Clone() const override;

 private:
  std::unordered_map<std::string, std::string> updated_;
};

/// \brief Represents removing table properties
class ICEBERG_EXPORT RemoveProperties : public TableUpdate {
 public:
  explicit RemoveProperties(std::unordered_set<std::string> removed)
      : removed_(std::move(removed)) {}

  const std::unordered_set<std::string>& removed() const { return removed_; }

  void ApplyTo(TableMetadataBuilder& builder) const override;

  void GenerateRequirements(TableUpdateContext& context) const override;

  Kind kind() const override { return Kind::kRemoveProperties; }

  bool Equals(const TableUpdate& other) const override;

  std::unique_ptr<TableUpdate> Clone() const override;

 private:
  std::unordered_set<std::string> removed_;
};

/// \brief Represents setting the table location
class ICEBERG_EXPORT SetLocation : public TableUpdate {
 public:
  explicit SetLocation(std::string location) : location_(std::move(location)) {}

  const std::string& location() const { return location_; }

  void ApplyTo(TableMetadataBuilder& builder) const override;

  void GenerateRequirements(TableUpdateContext& context) const override;

  Kind kind() const override { return Kind::kSetLocation; }

  bool Equals(const TableUpdate& other) const override;

  std::unique_ptr<TableUpdate> Clone() const override;

 private:
  std::string location_;
};

}  // namespace table

}  // namespace iceberg
