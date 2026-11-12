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

/// \file iceberg/view_metadata.h
/// View metadata for Iceberg views.

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/timepoint.h"
#include "iceberg/view_version.h"

namespace iceberg {

/// \brief Represents a view history entry, which records a change to the
/// current version of a view.
struct ICEBERG_EXPORT ViewHistoryEntry {
  /// The timestamp in milliseconds of the change to the current version.
  TimePointMs timestamp_ms;
  /// ID of the view version that was made current.
  int32_t version_id;

  /// \brief Compare two view history entries for equality.
  friend bool operator==(const ViewHistoryEntry& lhs, const ViewHistoryEntry& rhs) {
    return lhs.timestamp_ms == rhs.timestamp_ms && lhs.version_id == rhs.version_id;
  }
};

/// \brief Represents the metadata for an Iceberg view.
///
/// View metadata follows view format version 1. Table-format schema
/// restrictions are deliberately not applied to the view's schemas: view
/// schemas are projections of the referenced tables' columns and may use
/// types or nullability that table data would reject.
struct ICEBERG_EXPORT ViewMetadata {
  static constexpr int8_t kDefaultViewFormatVersion = 1;
  static constexpr int8_t kSupportedViewFormatVersion = 1;

  /// An integer version number for the view format; must be 1.
  int8_t format_version;
  /// A UUID that identifies the view, generated when the view is created.
  std::string view_uuid;
  /// The view's base location; used to create metadata file locations.
  std::string location;
  /// A list of known schemas.
  std::vector<std::shared_ptr<iceberg::Schema>> schemas;
  /// ID of the current version of the view.
  int32_t current_version_id;
  /// A list of known versions of the view.
  std::vector<std::shared_ptr<ViewVersion>> versions;
  /// A list of timestamp and version ID pairs that encodes changes to the
  /// current version.
  std::vector<ViewHistoryEntry> version_log;
  /// A string to string map of view properties.
  std::unordered_map<std::string, std::string> properties;

  /// \brief Validate view format v1 invariants.
  ///
  /// Checks referential integrity, required fields, and per-dialect
  /// uniqueness of SQL representations, without applying table-format schema
  /// restrictions. Never modifies the metadata.
  ///
  /// \return Status indicating success, or ErrorKind::kInvalid with the first
  /// violated invariant.
  Status Validate() const;

  /// \brief Get the current view version.
  /// \return the current version or ErrorKind::kNotFound if it is not listed.
  /// \note The returned version is guaranteed to be not null.
  Result<std::shared_ptr<ViewVersion>> CurrentVersion() const;

  /// \brief Get a view version by ID.
  /// \param version_id ID of the view version to look up
  /// \return the version or ErrorKind::kNotFound if it is not listed.
  /// \note The returned version is guaranteed to be not null.
  Result<std::shared_ptr<ViewVersion>> VersionById(int32_t version_id) const;

  /// \brief Get the schema of the current view version.
  /// \return the current schema or ErrorKind::kNotFound if the current
  /// version or its schema is not listed.
  /// \note The returned schema is guaranteed to be not null.
  Result<std::shared_ptr<iceberg::Schema>> CurrentSchema() const;

  /// \brief Get a view schema by ID.
  /// \param schema_id ID of the schema to look up
  /// \return the schema or ErrorKind::kNotFound if it is not listed.
  /// \note The returned schema is guaranteed to be not null.
  Result<std::shared_ptr<iceberg::Schema>> SchemaById(int32_t schema_id) const;

  /// \brief Compare two view metadata for equality.
  friend bool operator==(const ViewMetadata& lhs, const ViewMetadata& rhs) {
    return lhs.Equals(rhs);
  }

 private:
  /// \brief Compare two view metadata for equality.
  bool Equals(const ViewMetadata& other) const;
};

}  // namespace iceberg
