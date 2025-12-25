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

/// \file iceberg/table_requirement.h
/// Update requirements for Iceberg table operations.
///
/// Table requirements are conditions that must be satisfied before
/// applying metadata updates to a table. They are used for optimistic
/// concurrency control in table operations.

#include <optional>
#include <string>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Base class for update requirement operations
///
/// Represents a requirement that must be validated before applying
/// metadata updates to a table. Each concrete subclass represents
/// a specific type of requirement check.
class ICEBERG_EXPORT TableRequirement {
 public:
  enum class Kind : uint8_t {
    kAssertDoesNotExist,
    kAssertUUID,
    kAssertRefSnapshotID,
    kAssertLastAssignedFieldId,
    kAssertCurrentSchemaID,
    kAssertLastAssignedPartitionId,
    kAssertDefaultSpecID,
    kAssertDefaultSortOrderID,
  };

  virtual ~TableRequirement() = default;

  /// \brief Return the kind of requirement
  virtual Kind kind() const = 0;

  /// \brief Validate this requirement against table metadata
  ///
  /// \param base The base table metadata to validate against (may be nullptr)
  /// \return Status indicating success or failure with error details
  virtual Status Validate(const TableMetadata* base) const = 0;
};

namespace table {

/// \brief Requirement that the table does not exist
///
/// This requirement is used when creating a new table to ensure
/// it doesn't already exist.
class ICEBERG_EXPORT AssertDoesNotExist : public TableRequirement {
 public:
  AssertDoesNotExist() = default;

  Kind kind() const override { return Kind::kAssertDoesNotExist; }

  Status Validate(const TableMetadata* base) const override;
};

/// \brief Requirement that the table UUID matches the expected value
///
/// This ensures the table hasn't been replaced or recreated between
/// reading the metadata and attempting to update it.
class ICEBERG_EXPORT AssertUUID : public TableRequirement {
 public:
  explicit AssertUUID(std::string uuid) : uuid_(std::move(uuid)) {}

  const std::string& uuid() const { return uuid_; }

  Kind kind() const override { return Kind::kAssertUUID; }

  Status Validate(const TableMetadata* base) const override;

 private:
  std::string uuid_;
};

/// \brief Requirement that a reference (branch or tag) points to a specific snapshot
///
/// This requirement validates that a named reference (branch or tag) either:
/// - Points to the expected snapshot ID
/// - Does not exist (if snapshot_id is nullopt)
class ICEBERG_EXPORT AssertRefSnapshotID : public TableRequirement {
 public:
  AssertRefSnapshotID(std::string ref_name, std::optional<int64_t> snapshot_id)
      : ref_name_(std::move(ref_name)), snapshot_id_(snapshot_id) {}

  const std::string& ref_name() const { return ref_name_; }

  const std::optional<int64_t>& snapshot_id() const { return snapshot_id_; }

  Kind kind() const override { return Kind::kAssertRefSnapshotID; }

  Status Validate(const TableMetadata* base) const override;

 private:
  std::string ref_name_;
  std::optional<int64_t> snapshot_id_;
};

/// \brief Requirement that the last assigned field ID matches
///
/// This ensures the schema hasn't been modified (by adding fields)
/// since the metadata was read.
class ICEBERG_EXPORT AssertLastAssignedFieldId : public TableRequirement {
 public:
  explicit AssertLastAssignedFieldId(int32_t last_assigned_field_id)
      : last_assigned_field_id_(last_assigned_field_id) {}

  int32_t last_assigned_field_id() const { return last_assigned_field_id_; }

  Kind kind() const override { return Kind::kAssertLastAssignedFieldId; }

  Status Validate(const TableMetadata* base) const override;

 private:
  int32_t last_assigned_field_id_;
};

/// \brief Requirement that the current schema ID matches
///
/// This ensures the active schema hasn't changed since the
/// metadata was read.
class ICEBERG_EXPORT AssertCurrentSchemaID : public TableRequirement {
 public:
  explicit AssertCurrentSchemaID(int32_t schema_id) : schema_id_(schema_id) {}

  int32_t schema_id() const { return schema_id_; }

  Kind kind() const override { return Kind::kAssertCurrentSchemaID; }

  Status Validate(const TableMetadata* base) const override;

 private:
  int32_t schema_id_;
};

/// \brief Requirement that the last assigned partition ID matches
///
/// This ensures partition specs haven't been modified since the
/// metadata was read.
class ICEBERG_EXPORT AssertLastAssignedPartitionId : public TableRequirement {
 public:
  explicit AssertLastAssignedPartitionId(int32_t last_assigned_partition_id)
      : last_assigned_partition_id_(last_assigned_partition_id) {}

  int32_t last_assigned_partition_id() const { return last_assigned_partition_id_; }

  Kind kind() const override { return Kind::kAssertLastAssignedPartitionId; }

  Status Validate(const TableMetadata* base) const override;

 private:
  int32_t last_assigned_partition_id_;
};

/// \brief Requirement that the default partition spec ID matches
///
/// This ensures the default partition spec hasn't changed since
/// the metadata was read.
class ICEBERG_EXPORT AssertDefaultSpecID : public TableRequirement {
 public:
  explicit AssertDefaultSpecID(int32_t spec_id) : spec_id_(spec_id) {}

  int32_t spec_id() const { return spec_id_; }

  Kind kind() const override { return Kind::kAssertDefaultSpecID; }

  Status Validate(const TableMetadata* base) const override;

 private:
  int32_t spec_id_;
};

/// \brief Requirement that the default sort order ID matches
///
/// This ensures the default sort order hasn't changed since
/// the metadata was read.
class ICEBERG_EXPORT AssertDefaultSortOrderID : public TableRequirement {
 public:
  explicit AssertDefaultSortOrderID(int32_t sort_order_id)
      : sort_order_id_(sort_order_id) {}

  int32_t sort_order_id() const { return sort_order_id_; }

  Kind kind() const override { return Kind::kAssertDefaultSortOrderID; }

  Status Validate(const TableMetadata* base) const override;

 private:
  int32_t sort_order_id_;
};

}  // namespace table

}  // namespace iceberg
