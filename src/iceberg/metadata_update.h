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

/// \file iceberg/metadata_update.h
/// Metadata update operations for Iceberg tables.

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/snapshot.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

class TableMetadataBuilder;
class UpdateRequirementsContext;

/// \brief Base class for metadata update operations
///
/// Represents a change to table metadata. Each concrete subclass
/// represents a specific type of update operation.
class ICEBERG_EXPORT MetadataUpdate {
 public:
  virtual ~MetadataUpdate() = default;

  /// \brief Clone this metadata update
  virtual std::unique_ptr<MetadataUpdate> Clone() const = 0;

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
  virtual void GenerateRequirements(UpdateRequirementsContext& context) const = 0;
};

/// \brief Represents an assignment of a UUID to the table
class ICEBERG_EXPORT AssignUUID : public MetadataUpdate {
 public:
  explicit AssignUUID(std::string uuid) : uuid_(std::move(uuid)) {}

  const std::string& uuid() const { return uuid_; }

  std::unique_ptr<MetadataUpdate> Clone() const override {
    return std::make_unique<AssignUUID>(uuid_);
  }

  void ApplyTo(TableMetadataBuilder& builder) const override;

  void GenerateRequirements(UpdateRequirementsContext& context) const override;

 private:
  std::string uuid_;
};

/// \brief Represents an upgrade of the table format version
class ICEBERG_EXPORT UpgradeFormatVersion : public MetadataUpdate {
 public:
  explicit UpgradeFormatVersion(int8_t format_version)
      : format_version_(format_version) {}

  int8_t format_version() const { return format_version_; }

  std::unique_ptr<MetadataUpdate> Clone() const override {
    return std::make_unique<UpgradeFormatVersion>(format_version_);
  }

  void ApplyTo(TableMetadataBuilder& builder) const override;

  void GenerateRequirements(UpdateRequirementsContext& context) const override;

 private:
  int8_t format_version_;
};

/// \brief Represents adding a new schema to the table
class ICEBERG_EXPORT AddSchema : public MetadataUpdate {
 public:
  explicit AddSchema(std::shared_ptr<iceberg::Schema> schema, int32_t last_column_id)
      : schema_(std::move(schema)), last_column_id_(last_column_id) {}

  const std::shared_ptr<iceberg::Schema>& schema() const { return schema_; }

  int32_t last_column_id() const { return last_column_id_; }

  std::unique_ptr<MetadataUpdate> Clone() const override {
    return std::make_unique<AddSchema>(schema_, last_column_id_);
  }

  void ApplyTo(TableMetadataBuilder& builder) const override;

  void GenerateRequirements(UpdateRequirementsContext& context) const override;

 private:
  std::shared_ptr<iceberg::Schema> schema_;
  int32_t last_column_id_;
};

/// \brief Represents setting the current schema
class ICEBERG_EXPORT SetCurrentSchema : public MetadataUpdate {
 public:
  explicit SetCurrentSchema(int32_t schema_id) : schema_id_(schema_id) {}

  int32_t schema_id() const { return schema_id_; }

  std::unique_ptr<MetadataUpdate> Clone() const override {
    return std::make_unique<SetCurrentSchema>(schema_id_);
  }

  void ApplyTo(TableMetadataBuilder& builder) const override;

  void GenerateRequirements(UpdateRequirementsContext& context) const override;

 private:
  int32_t schema_id_;
};

/// \brief Represents adding a new partition spec to the table
class ICEBERG_EXPORT AddPartitionSpec : public MetadataUpdate {
 public:
  explicit AddPartitionSpec(std::shared_ptr<iceberg::PartitionSpec> spec)
      : spec_(std::move(spec)) {}

  const std::shared_ptr<iceberg::PartitionSpec>& spec() const { return spec_; }

  std::unique_ptr<MetadataUpdate> Clone() const override {
    return std::make_unique<AddPartitionSpec>(spec_);
  }

  void ApplyTo(TableMetadataBuilder& builder) const override;

  void GenerateRequirements(UpdateRequirementsContext& context) const override;

 private:
  std::shared_ptr<iceberg::PartitionSpec> spec_;
};

/// \brief Represents setting the default partition spec
class ICEBERG_EXPORT SetDefaultPartitionSpec : public MetadataUpdate {
 public:
  explicit SetDefaultPartitionSpec(int32_t spec_id) : spec_id_(spec_id) {}

  int32_t spec_id() const { return spec_id_; }

  std::unique_ptr<MetadataUpdate> Clone() const override {
    return std::make_unique<SetDefaultPartitionSpec>(spec_id_);
  }

  void ApplyTo(TableMetadataBuilder& builder) const override;

  void GenerateRequirements(UpdateRequirementsContext& context) const override;

 private:
  int32_t spec_id_;
};

/// \brief Represents removing partition specs from the table
class ICEBERG_EXPORT RemovePartitionSpecs : public MetadataUpdate {
 public:
  explicit RemovePartitionSpecs(std::vector<int32_t> spec_ids)
      : spec_ids_(std::move(spec_ids)) {}

  const std::vector<int32_t>& spec_ids() const { return spec_ids_; }

  std::unique_ptr<MetadataUpdate> Clone() const override {
    return std::make_unique<RemovePartitionSpecs>(spec_ids_);
  }

  void ApplyTo(TableMetadataBuilder& builder) const override;

  void GenerateRequirements(UpdateRequirementsContext& context) const override;

 private:
  std::vector<int32_t> spec_ids_;
};

/// \brief Represents removing schemas from the table
class ICEBERG_EXPORT RemoveSchemas : public MetadataUpdate {
 public:
  explicit RemoveSchemas(std::vector<int32_t> schema_ids)
      : schema_ids_(std::move(schema_ids)) {}

  const std::vector<int32_t>& schema_ids() const { return schema_ids_; }

  std::unique_ptr<MetadataUpdate> Clone() const override {
    return std::make_unique<RemoveSchemas>(schema_ids_);
  }

  void ApplyTo(TableMetadataBuilder& builder) const override;

  void GenerateRequirements(UpdateRequirementsContext& context) const override;

 private:
  std::vector<int32_t> schema_ids_;
};

/// \brief Represents adding a new sort order to the table
class ICEBERG_EXPORT AddSortOrder : public MetadataUpdate {
 public:
  explicit AddSortOrder(std::shared_ptr<iceberg::SortOrder> sort_order)
      : sort_order_(std::move(sort_order)) {}

  const std::shared_ptr<iceberg::SortOrder>& sort_order() const { return sort_order_; }

  std::unique_ptr<MetadataUpdate> Clone() const override {
    return std::make_unique<AddSortOrder>(sort_order_);
  }

  void ApplyTo(TableMetadataBuilder& builder) const override;

  void GenerateRequirements(UpdateRequirementsContext& context) const override;

 private:
  std::shared_ptr<iceberg::SortOrder> sort_order_;
};

/// \brief Represents setting the default sort order
class ICEBERG_EXPORT SetDefaultSortOrder : public MetadataUpdate {
 public:
  explicit SetDefaultSortOrder(int32_t sort_order_id) : sort_order_id_(sort_order_id) {}

  int32_t sort_order_id() const { return sort_order_id_; }

  std::unique_ptr<MetadataUpdate> Clone() const override {
    return std::make_unique<SetDefaultSortOrder>(sort_order_id_);
  }

  void ApplyTo(TableMetadataBuilder& builder) const override;

  void GenerateRequirements(UpdateRequirementsContext& context) const override;

 private:
  int32_t sort_order_id_;
};

/// \brief Represents adding a snapshot to the table
class ICEBERG_EXPORT AddSnapshot : public MetadataUpdate {
 public:
  explicit AddSnapshot(std::shared_ptr<iceberg::Snapshot> snapshot)
      : snapshot_(std::move(snapshot)) {}

  const std::shared_ptr<iceberg::Snapshot>& snapshot() const { return snapshot_; }

  std::unique_ptr<MetadataUpdate> Clone() const override {
    return std::make_unique<AddSnapshot>(snapshot_);
  }

  void ApplyTo(TableMetadataBuilder& builder) const override;

  void GenerateRequirements(UpdateRequirementsContext& context) const override;

 private:
  std::shared_ptr<iceberg::Snapshot> snapshot_;
};

/// \brief Represents removing snapshots from the table
class ICEBERG_EXPORT RemoveSnapshots : public MetadataUpdate {
 public:
  explicit RemoveSnapshots(std::vector<int64_t> snapshot_ids)
      : snapshot_ids_(std::move(snapshot_ids)) {}

  const std::vector<int64_t>& snapshot_ids() const { return snapshot_ids_; }

  std::unique_ptr<MetadataUpdate> Clone() const override {
    return std::make_unique<RemoveSnapshots>(snapshot_ids_);
  }

  void ApplyTo(TableMetadataBuilder& builder) const override;

  void GenerateRequirements(UpdateRequirementsContext& context) const override;

 private:
  std::vector<int64_t> snapshot_ids_;
};

/// \brief Represents removing a snapshot reference
class ICEBERG_EXPORT RemoveSnapshotRef : public MetadataUpdate {
 public:
  explicit RemoveSnapshotRef(std::string ref_name) : ref_name_(std::move(ref_name)) {}

  const std::string& ref_name() const { return ref_name_; }

  std::unique_ptr<MetadataUpdate> Clone() const override {
    return std::make_unique<RemoveSnapshotRef>(ref_name_);
  }

  void ApplyTo(TableMetadataBuilder& builder) const override;

  void GenerateRequirements(UpdateRequirementsContext& context) const override;

 private:
  std::string ref_name_;
};

/// \brief Represents setting a snapshot reference
class ICEBERG_EXPORT SetSnapshotRef : public MetadataUpdate {
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

  std::unique_ptr<MetadataUpdate> Clone() const override {
    return std::make_unique<SetSnapshotRef>(ref_name_, snapshot_id_, type_,
                                            min_snapshots_to_keep_, max_snapshot_age_ms_,
                                            max_ref_age_ms_);
  }

  void ApplyTo(TableMetadataBuilder& builder) const override;

  void GenerateRequirements(UpdateRequirementsContext& context) const override;

 private:
  std::string ref_name_;
  int64_t snapshot_id_;
  SnapshotRefType type_;
  std::optional<int32_t> min_snapshots_to_keep_;
  std::optional<int64_t> max_snapshot_age_ms_;
  std::optional<int64_t> max_ref_age_ms_;
};

/// \brief Represents setting table properties
class ICEBERG_EXPORT SetProperties : public MetadataUpdate {
 public:
  explicit SetProperties(std::unordered_map<std::string, std::string> updated)
      : updated_(std::move(updated)) {}

  const std::unordered_map<std::string, std::string>& updated() const { return updated_; }

  std::unique_ptr<MetadataUpdate> Clone() const override {
    return std::make_unique<SetProperties>(updated_);
  }

  void ApplyTo(TableMetadataBuilder& builder) const override;

  void GenerateRequirements(UpdateRequirementsContext& context) const override;

 private:
  std::unordered_map<std::string, std::string> updated_;
};

/// \brief Represents removing table properties
class ICEBERG_EXPORT RemoveProperties : public MetadataUpdate {
 public:
  explicit RemoveProperties(std::vector<std::string> removed)
      : removed_(std::move(removed)) {}

  const std::vector<std::string>& removed() const { return removed_; }

  std::unique_ptr<MetadataUpdate> Clone() const override {
    return std::make_unique<RemoveProperties>(removed_);
  }

  void ApplyTo(TableMetadataBuilder& builder) const override;

  void GenerateRequirements(UpdateRequirementsContext& context) const override;

 private:
  std::vector<std::string> removed_;
};

/// \brief Represents setting the table location
class ICEBERG_EXPORT SetLocation : public MetadataUpdate {
 public:
  explicit SetLocation(std::string location) : location_(std::move(location)) {}

  const std::string& location() const { return location_; }

  std::unique_ptr<MetadataUpdate> Clone() const override {
    return std::make_unique<SetLocation>(location_);
  }

  void ApplyTo(TableMetadataBuilder& builder) const override;

  void GenerateRequirements(UpdateRequirementsContext& context) const override;

 private:
  std::string location_;
};

}  // namespace iceberg
