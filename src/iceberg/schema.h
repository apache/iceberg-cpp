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

/// \file iceberg/schema.h
/// Schemas for Iceberg tables.  This header contains the definition of Schema
/// and any utility functions.  See iceberg/type.h and iceberg/field.h as well.

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/schema_field.h"
#include "iceberg/type.h"
#include "iceberg/util/string_util.h"

namespace iceberg {

/// \brief A schema for a Table.
///
/// A schema is a list of typed columns, along with a unique integer ID.  A
/// Table may have different schemas over its lifetime due to schema
/// evolution.
class ICEBERG_EXPORT Schema : public StructType {
 public:
  static constexpr int32_t kInitialSchemaId = 0;

  explicit Schema(std::vector<SchemaField> fields,
                  std::optional<int32_t> schema_id = std::nullopt);

  /// \brief Get the schema ID.
  ///
  /// A schema is identified by a unique ID for the purposes of schema
  /// evolution.
  std::optional<int32_t> schema_id() const;

  std::string ToString() const override;

  /// \brief Find the SchemaField by field name.
  ///
  /// Short names for maps and lists are included for any name that does not conflict with
  /// a canonical name. For example, a list, 'l', of structs with field 'x' will produce
  /// short name 'l.x' in addition to canonical name 'l.element.x'. a map 'm', if its
  /// value include a structs with field 'x' wil produce short name 'm.x' in addition to
  /// canonical name 'm.value.x'
  /// FIXME: Currently only handles ASCII lowercase conversion; extend to support
  /// non-ASCII characters (e.g., using std::towlower or ICU)
  Result<std::optional<std::reference_wrapper<const SchemaField>>> FindFieldByName(
      std::string_view name, bool case_sensitive = true) const;

  /// \brief Find the SchemaField by field id.
  Result<std::optional<std::reference_wrapper<const SchemaField>>> FindFieldById(
      int32_t field_id) const;

  /// \brief Creates a projected schema from selected field names.
  ///
  /// Selects fields by their names using dot notation for nested fields.
  /// Supports both canonical names (e.g., "user.address.street") and short names
  /// (e.g., "user.street" for map values, "list.element" for list elements).
  ///
  /// \param names Field names to select (supports nested field paths)
  /// \param case_sensitive Whether name matching is case-sensitive (default: true)
  /// \return Projected schema containing only the specified fields
  ///
  /// \example
  /// \code
  /// // Original schema: struct {
  /// //   id: int,
  /// //   user: struct {
  /// //     name: string,
  /// //     address: struct { street: string, city: string }
  /// //   }
  /// // }
  ///
  /// // Select by names - you must specify the exact path
  /// auto result1 = schema->Select({"id", "user.name"});
  /// // Result: struct { id: int, user: struct { name: string } }
  ///
  /// auto result2 = schema->Select({"user.address.street"});
  /// // Result: struct { user: struct { address: struct { street: string } } }
  ///
  /// auto result3 = schema->Select({"user.name"});
  /// Result: struct { user: struct { name: string } }
  /// \endcode
  Result<std::unique_ptr<const Schema>> Select(std::span<const std::string> names,
                                               bool case_sensitive = true) const;

  /// \brief Creates a projected schema from selected field names.
  Result<std::unique_ptr<const Schema>> Select(
      const std::initializer_list<std::string>& names, bool case_sensitive = true) const;

  /// \brief Creates a projected schema from selected field names.
  template <typename... Args>
  Result<std::unique_ptr<const Schema>> Select(Args&&... names,
                                               bool case_sensitive = true) const {
    static_assert(((std::is_convertible_v<Args, std::string> ||
                    std::is_convertible_v<Args, std::string>) &&
                   ...),
                  "All arguments must be convertible to std::string");
    return select({(names)...}, case_sensitive);
  }

  /// \brief Creates a projected schema from selected field IDs.
  ///
  /// Selects fields by their numeric IDs. More efficient than Select() when you
  /// already know the field IDs. Handles recursive projection of nested structs.
  ///
  /// \param field_ids Set of field IDs to select
  /// \return Projected schema containing only the specified fields
  ///
  /// \note When a struct field ID is specified:
  ///       - If nested field IDs are also in field_ids, they are recursively projected
  ///       - If no nested field IDs are in field_ids, an empty struct is included
  ///       - List/Map types cannot be explicitly projected (returns error)
  ///
  /// \example
  /// \code
  /// // Original schema with field IDs:
  /// // struct {
  /// //   1: id: int,
  /// //   2: user: struct {
  /// //     3: name: string,
  /// //     4: address: struct { 5: street: string, 6: city: string }
  /// //   }
  /// // }
  ///
  /// // Project by IDs - recursive behavior for structs
  /// std::unordered_set<int32_t> ids1 = {1, 2, 3};  // id, user, user.name
  /// auto result1 = schema->Project(ids1);
  /// // Result: struct { id: int, user: struct { name: string } }
  ///
  /// std::unordered_set<int32_t> ids2 = {2};  // Only user struct
  /// auto result2 = schema->Project(ids2);
  /// // Result: struct { user: struct {} }  // Empty struct because no nested fields
  /// selected
  ///
  /// std::unordered_set<int32_t> ids3 = {2, 5};  // user struct + street field
  /// auto result3 = schema->Project(ids3);
  /// // Result: struct { user: struct { address: struct { street: string } } }
  /// \endcode
  Result<std::unique_ptr<const Schema>> Project(
      std::unordered_set<int32_t>& field_ids) const;

  friend bool operator==(const Schema& lhs, const Schema& rhs) { return lhs.Equals(rhs); }

 private:
  /// \brief Compare two schemas for equality.
  bool Equals(const Schema& other) const;

  Result<std::unique_ptr<const Schema>> SelectInternal(std::span<const std::string> names,
                                                       bool case_sensitive) const;

  // TODO(nullccxsy): Address potential concurrency issues in lazy initialization (e.g.,
  // use std::call_once)
  Status InitIdToFieldMap() const;
  Status InitNameToIdMap() const;
  Status InitLowerCaseNameToIdMap() const;

  const std::optional<int32_t> schema_id_;
  /// Mapping from field id to field.
  mutable std::unordered_map<int32_t, std::reference_wrapper<const SchemaField>>
      id_to_field_;
  /// Mapping from field name to field id.
  mutable std::unordered_map<std::string, int32_t, StringHash, std::equal_to<>>
      name_to_id_;
  /// Mapping from lowercased field name to field id
  mutable std::unordered_map<std::string, int32_t, StringHash, std::equal_to<>>
      lowercase_name_to_id_;
};

}  // namespace iceberg
