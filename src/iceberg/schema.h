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
#include "iceberg/util/lazy.h"
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
  /// \param names Selected field names and nested names are dot-concatenated.
  /// \param case_sensitive Whether name matching is case-sensitive (default: true).
  /// \return Projected schema containing only selected fields.
  /// \note If the field name of a nested type has been selected, all of its
  /// sub-fields will be selected.
  Result<std::unique_ptr<Schema>> Select(std::span<const std::string> names,
                                         bool case_sensitive = true) const;

  /// \brief Creates a projected schema from selected field IDs.
  ///
  /// \param field_ids Set of field IDs to select
  /// \return Projected schema containing only the specified fields.
  /// \note Field ID of a nested field may not be projected unless at least
  /// one of its sub-fields has been projected.
  Result<std::unique_ptr<Schema>> Project(
      const std::unordered_set<int32_t>& field_ids) const;

  friend bool operator==(const Schema& lhs, const Schema& rhs) { return lhs.Equals(rhs); }

 private:
  /// \brief Compare two schemas for equality.
  bool Equals(const Schema& other) const;

  static Result<std::unordered_map<int32_t, std::reference_wrapper<const SchemaField>>>
  InitIdToFieldMap(const Schema&);
  static Result<std::unordered_map<std::string, int32_t, StringHash, std::equal_to<>>>
  InitNameToIdMap(const Schema&);
  static Result<std::unordered_map<std::string, int32_t, StringHash, std::equal_to<>>>
  InitLowerCaseNameToIdMap(const Schema&);

  const std::optional<int32_t> schema_id_;
  /// Mapping from field id to field.
  Lazy<InitIdToFieldMap> id_to_field_;
  /// Mapping from field name to field id.
  Lazy<InitNameToIdMap> name_to_id_;
  /// Mapping from lowercased field name to field id
  Lazy<InitLowerCaseNameToIdMap> lowercase_name_to_id_;
};

}  // namespace iceberg
