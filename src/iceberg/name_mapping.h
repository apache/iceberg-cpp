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

#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"

namespace iceberg {

/// \brief An immutable mapping between a field ID and a set of names.
///
/// This class is trivial enough that we don't need any function.
struct ICEBERG_EXPORT MappedField {
  /// \brief A required list of 0 or more names for a field.
  std::unordered_set<std::string> names;
  /// \brief An optional Iceberg field ID used when a field's name is present in `names`.
  std::optional<int32_t> field_id;
  /// \brief An optional list of field mappings for child field of structs, maps, and
  /// lists.
  std::shared_ptr<class MappedFields> nested_mapping;

  friend bool operator==(const MappedField& lhs, const MappedField& rhs);
};

using MappedFieldConstRef = std::reference_wrapper<const MappedField>;

/// \brief A list of field mappings for child field of structs, maps, and lists.
class ICEBERG_EXPORT MappedFields {
 public:
  /// \brief Create a new MappedFields instance.
  /// \param[in] fields The list of field mappings.
  /// \return A new MappedFields instance.
  static std::unique_ptr<MappedFields> Make(std::vector<MappedField> fields);

  /// \brief Get the field for a given field ID.
  /// \param[in] id The ID of the field.
  /// \return The field for the given field ID.
  std::optional<MappedFieldConstRef> Field(int32_t id) const;

  /// \brief Get the field ID for a given field name.
  /// \param[in] name The name of the field.
  /// \return The field ID of the field.
  std::optional<int32_t> Id(std::string_view name) const;

  /// \brief Get the number of field mappings.
  size_t Size() const;

  /// \brief Get the list of field mappings.
  std::span<const MappedField> fields() const;

  ICEBERG_EXPORT friend bool operator==(const MappedFields& lhs, const MappedFields& rhs);

 private:
  explicit MappedFields(std::vector<MappedField> fields);

  const std::unordered_map<std::string_view, int32_t>& LazyNameToId() const;
  const std::unordered_map<int32_t, MappedFieldConstRef>& LazyIdToField() const;

 private:
  std::vector<MappedField> fields_;

  // Lazy-initialized mappings
  mutable std::unordered_map<std::string_view, int32_t> name_to_id_;
  mutable std::unordered_map<int32_t, MappedFieldConstRef> id_to_field_;
};

/// \brief Represents a mapping from external schema names to Iceberg type IDs.
class ICEBERG_EXPORT NameMapping {
 public:
  /// \brief Create a new NameMapping instance.
  static std::unique_ptr<NameMapping> Make(std::unique_ptr<MappedFields> fields);

  /// \brief Create a new NameMapping instance.
  static std::unique_ptr<NameMapping> Make(std::vector<MappedField> fields);

  /// \brief Create an empty NameMapping instance.
  static std::unique_ptr<NameMapping> MakeEmpty();

  /// \brief Find a field by its ID.
  std::optional<MappedFieldConstRef> Find(int32_t id) const;

  /// \brief Find a field by its unconcatenated names.
  std::optional<MappedFieldConstRef> Find(std::span<const std::string> names) const;

  /// \brief Find a field by its (concatenated) name.
  std::optional<MappedFieldConstRef> Find(const std::string& name) const;

  /// \brief Get the underlying MappedFields instance.
  const MappedFields& AsMappedFields() const;

  ICEBERG_EXPORT friend bool operator==(const NameMapping& lhs, const NameMapping& rhs);

 private:
  explicit NameMapping(std::unique_ptr<MappedFields> mapping);

  const std::unordered_map<int32_t, MappedFieldConstRef>& LazyFieldsById() const;
  const std::unordered_map<std::string, MappedFieldConstRef>& LazyFieldsByName() const;

 private:
  std::unique_ptr<MappedFields> mapping_;

  // Lazy-initialized mappings
  mutable std::unordered_map<int32_t, MappedFieldConstRef> fields_by_id_;
  mutable std::unordered_map<std::string, MappedFieldConstRef> fields_by_name_;
};

ICEBERG_EXPORT std::string ToString(const MappedField& field);
ICEBERG_EXPORT std::string ToString(const MappedFields& fields);
ICEBERG_EXPORT std::string ToString(const NameMapping& mapping);

/// \brief Create a name-based mapping for a schema.
///
/// The mapping returned by this method will use the schema's name for each field.
///
/// \param schema The schema to create the mapping for.
/// \return A new NameMapping instance initialized with the schema's fields and names.
ICEBERG_EXPORT Result<std::unique_ptr<NameMapping>> CreateMapping(const Schema& schema);

/// TODO(gangwu): implement this function once SchemaUpdate is supported
///
/// \brief Update a name-based mapping using changes to a schema.
/// \param mapping a name-based mapping
/// \param updates a map from field ID to updated field definitions
/// \param adds a map from parent field ID to nested fields to be added
/// \return an updated mapping with names added to renamed fields and the mapping extended
/// for new fields
// ICEBERG_EXPORT Result<std::unique_ptr<NameMapping>> UpdateMapping(
//     const NameMapping& mapping, const std::map<int32_t, SchemaField>& updates,
//     const std::multimap<int32_t, int32_t>& adds);

}  // namespace iceberg
