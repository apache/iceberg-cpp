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
  static constexpr int32_t kInitialColumnId = 0;
  static constexpr int32_t kInvalidColumnId = -1;

  /// \brief Special value to select all columns from manifest files.
  static constexpr std::string_view kAllColumns = "*";

  explicit Schema(std::vector<SchemaField> fields,
                  std::optional<int32_t> schema_id = std::nullopt,
                  std::vector<int32_t> identifier_field_ids = {});

  /// \brief Create a schema.
  ///
  /// \param fields The fields that make up the schema.
  /// \param schema_id The unique identifier for this schema (default: kInitialSchemaId).
  /// \param identifier_field_names Canonical names of fields that uniquely identify rows
  /// in the table (default: empty). \return A new Schema instance or Status if failed.
  static Result<std::unique_ptr<Schema>> Make(
      std::vector<SchemaField> fields, std::optional<int32_t> schema_id = std::nullopt,
      const std::vector<std::string>& identifier_field_names = {});

  /// \brief Get the schema ID.
  ///
  /// A schema is identified by a unique ID for the purposes of schema
  /// evolution.
  std::optional<int32_t> schema_id() const;

  std::string ToString() const override;

  /// \brief Recursively find the SchemaField by field name.
  ///
  /// Short names for maps and lists are included for any name that does not conflict with
  /// a canonical name. For example, a list, 'l', of structs with field 'x' will produce
  /// short name 'l.x' in addition to canonical name 'l.element.x'. A map 'm', if its
  /// value includes a struct with field 'x' will produce short name 'm.x' in addition to
  /// canonical name 'm.value.x'.
  /// FIXME: Currently only handles ASCII lowercase conversion; extend to support
  /// non-ASCII characters (e.g., using std::towlower or ICU)
  Result<std::optional<std::reference_wrapper<const SchemaField>>> FindFieldByName(
      std::string_view name, bool case_sensitive = true) const;

  /// \brief Recursively find the SchemaField by field id.
  ///
  /// \param field_id The id of the field to get the accessor for.
  /// \return The field with the given id, or std::nullopt if not found.
  Result<std::optional<std::reference_wrapper<const SchemaField>>> FindFieldById(
      int32_t field_id) const;

  /// \brief Returns the canonical field name for the given id.
  ///
  /// \param field_id The id of the field to get the canonical name for.
  /// \return The canocinal column name of the field with the given id, or std::nullopt if
  /// not found.
  Result<std::optional<std::string_view>> FindColumnNameById(int32_t field_id) const;

  /// \brief Get the accessor to access the field by field id.
  ///
  /// \param field_id The id of the field to get the accessor for.
  /// \return The accessor to access the field, or NotFound if the field is not found.
  Result<std::unique_ptr<StructLikeAccessor>> GetAccessorById(int32_t field_id) const;

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

  /// \brief Return the field IDs of the identifier fields.
  const std::vector<int32_t>& IdentifierFieldIds() const;

  /// \brief Return the canonical field names of the identifier fields.
  Result<std::vector<std::string>> IdentifierFieldNames() const;

  /// \brief Get the highest field ID in the schema.
  /// \return The highest field ID.
  Result<int32_t> HighestFieldId() const;

  /// \brief Checks whether this schema is equivalent to another schema while ignoring the
  /// schema id.
  bool SameSchema(const Schema& other) const;

  /// \brief Validate the schema for a given format version.
  ///
  /// This validates that the schema does not contain types that were released in later
  /// format versions.
  ///
  /// \param format_version The format version to validate against.
  /// \return Error status if the schema is invalid.
  Status Validate(int32_t format_version) const;

  friend bool operator==(const Schema& lhs, const Schema& rhs) { return lhs.Equals(rhs); }

 private:
  /// \brief Compare two schemas for equality.
  bool Equals(const Schema& other) const;

  struct NameIdMap {
    /// \brief Mapping from canonical field name to ID
    ///
    /// \note Short names for maps and lists are included for any name that does not
    /// conflict with a canonical name. For example, a list, 'l', of structs with field
    /// 'x' will produce short name 'l.x' in addition to canonical name 'l.element.x'.
    std::unordered_map<std::string, int32_t, StringHash, std::equal_to<>> name_to_id;

    /// \brief Mapping from field ID to canonical name
    ///
    /// \note Canonical names, but not short names are set, for example
    /// 'list.element.field' instead of 'list.field'.
    std::unordered_map<int32_t, std::string> id_to_name;
  };

  static Result<std::unordered_map<int32_t, std::reference_wrapper<const SchemaField>>>
  InitIdToFieldMap(const Schema&);
  static Result<NameIdMap> InitNameIdMap(const Schema&);
  static Result<std::unordered_map<std::string, int32_t, StringHash, std::equal_to<>>>
  InitLowerCaseNameToIdMap(const Schema&);
  static Result<std::unordered_map<int32_t, std::vector<size_t>>> InitIdToPositionPath(
      const Schema&);
  static Result<int32_t> InitHighestFieldId(const Schema&);

  const std::optional<int32_t> schema_id_;
  /// Field IDs that uniquely identify rows in the table.
  std::vector<int32_t> identifier_field_ids_;
  /// Mapping from field id to field.
  Lazy<InitIdToFieldMap> id_to_field_;
  /// Mapping from field name to field id.
  Lazy<InitNameIdMap> name_id_map_;
  /// Mapping from lowercased field name to field id
  Lazy<InitLowerCaseNameToIdMap> lowercase_name_to_id_;
  /// Mapping from field id to (nested) position path to access the field.
  Lazy<InitIdToPositionPath> id_to_position_path_;
  /// Highest field ID in the schema.
  Lazy<InitHighestFieldId> highest_field_id_;
};

}  // namespace iceberg
