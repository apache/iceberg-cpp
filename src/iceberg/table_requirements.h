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

/// \file iceberg/table_requirements.h
/// Factory for generating table requirements from metadata updates.
///
/// This utility class generates the appropriate TableRequirement instances
/// based on a list of TableUpdate operations. The requirements are used
/// for optimistic concurrency control when committing table changes.

#include <memory>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/table_requirement.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Context for generating table requirements
///
/// This context is passed to each TableUpdate's GenerateRequirements method
/// and maintains state about what requirements have already been added to avoid
/// duplicates.
class ICEBERG_EXPORT TableUpdateContext {
 public:
  /// \brief Construct a context for requirement generation
  ///
  /// \param base The base table metadata (maybe nullptr for table creation)
  /// \param is_replace Whether this is a replace operation (more permissive)
  TableUpdateContext(const TableMetadata* base, bool is_replace)
      : base_(base), is_replace_(is_replace) {}

  // Delete copy operations (contains unique_ptr members)
  TableUpdateContext(const TableUpdateContext&) = delete;
  TableUpdateContext& operator=(const TableUpdateContext&) = delete;

  // Enable move construction only (assignment deleted due to const members)
  TableUpdateContext(TableUpdateContext&&) noexcept = default;

  /// \brief Add a requirement to the list
  void AddRequirement(std::unique_ptr<TableRequirement> requirement);

  /// \brief Get the base table metadata
  const TableMetadata* base() const { return base_; }

  /// \brief Check if this is a replace operation
  bool is_replace() const { return is_replace_; }

  /// \brief Build and return the list of requirements
  Result<std::vector<std::unique_ptr<TableRequirement>>> Build();

  // Getters for deduplication flags
  bool added_last_assigned_field_id() const { return added_last_assigned_field_id_; }
  bool added_current_schema_id() const { return added_current_schema_id_; }
  bool added_last_assigned_partition_id() const {
    return added_last_assigned_partition_id_;
  }
  bool added_default_spec_id() const { return added_default_spec_id_; }
  bool added_default_sort_order_id() const { return added_default_sort_order_id_; }

  // Setters for deduplication flags
  void set_added_last_assigned_field_id(bool value) {
    added_last_assigned_field_id_ = value;
  }
  void set_added_current_schema_id(bool value) { added_current_schema_id_ = value; }
  void set_added_last_assigned_partition_id(bool value) {
    added_last_assigned_partition_id_ = value;
  }
  void set_added_default_spec_id(bool value) { added_default_spec_id_ = value; }
  void set_added_default_sort_order_id(bool value) {
    added_default_sort_order_id_ = value;
  }

 private:
  const TableMetadata* base_;
  const bool is_replace_;

  std::vector<std::unique_ptr<TableRequirement>> requirements_;

  // flags to avoid adding duplicate requirements
  bool added_last_assigned_field_id_ = false;
  bool added_current_schema_id_ = false;
  bool added_last_assigned_partition_id_ = false;
  bool added_default_spec_id_ = false;
  bool added_default_sort_order_id_ = false;
};

/// \brief Factory class for generating table requirements
///
/// This class analyzes a sequence of table updates and generates the
/// appropriate table requirements to ensure safe concurrent modifications.
class ICEBERG_EXPORT TableRequirements {
 public:
  /// \brief Generate requirements for creating a new table
  ///
  /// For table creation, this requires that the table does not already exist.
  ///
  /// \param table_updates The list of table updates for table creation
  /// \return A list of table requirements to validate before creation
  static Result<std::vector<std::unique_ptr<TableRequirement>>> ForCreateTable(
      const std::vector<std::unique_ptr<TableUpdate>>& table_updates);

  /// \brief Generate requirements for replacing an existing table
  ///
  /// For table replacement, this requires that the table UUID matches but
  /// allows more aggressive changes than a regular update.
  ///
  /// \param base The base table metadata
  /// \param table_updates The list of table updates for replacement
  /// \return A list of table requirements to validate before replacement
  static Result<std::vector<std::unique_ptr<TableRequirement>>> ForReplaceTable(
      const TableMetadata& base,
      const std::vector<std::unique_ptr<TableUpdate>>& table_updates);

  /// \brief Generate requirements for updating an existing table
  ///
  /// For table updates, this generates requirements to ensure that key
  /// metadata properties haven't changed concurrently.
  ///
  /// \param base The base table metadata
  /// \param table_updates The list of table updates
  /// \return A list of table requirements to validate before update
  static Result<std::vector<std::unique_ptr<TableRequirement>>> ForUpdateTable(
      const TableMetadata& base,
      const std::vector<std::unique_ptr<TableUpdate>>& table_updates);
};

}  // namespace iceberg
