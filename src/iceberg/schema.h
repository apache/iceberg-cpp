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

#include <algorithm>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/schema_field.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/visit_type.h"

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
  [[nodiscard]] std::optional<int32_t> schema_id() const;

  [[nodiscard]] std::string ToString() const override;

  ///\brief Get thd SchemaField By Name
  ///
  /// Short names for maps and lists are included for any name that does not conflict with
  /// a canonical name. For example, a list, 'l', of structs with field 'x' will produce
  /// short name 'l.x' in addition to canonical name 'l.element.x'. a map 'm', if its
  /// value include a structs with field 'x' wil produce short name 'm.x' in addition to
  /// canonical name 'm.value.x'
  [[nodiscard]] Result<std::optional<std::reference_wrapper<const SchemaField>>>
  FindFieldByName(std::string_view name, bool case_sensitive) const;

  [[nodiscard]] Result<std::optional<std::reference_wrapper<const SchemaField>>>
  FindFieldByName(std::string_view name) const;

  [[nodiscard]] Result<std::optional<std::reference_wrapper<const SchemaField>>>
  FindFieldById(int32_t field_id) const;

  friend bool operator==(const Schema& lhs, const Schema& rhs) { return lhs.Equals(rhs); }

  /// Mapping from field id to index of `full_schemafield_`.
  mutable std::unordered_map<int, size_t> id_to_index_;
  /// Mapping from field name to index of `full_schemafield_`.
  mutable std::unordered_map<std::string, size_t> name_to_index_;
  /// Mapping from field lowercase_name(suppoert case_insensitive query) to index of
  /// `full_schemafield_`.
  mutable std::unordered_map<std::string, size_t> lowercase_name_to_index_;
  mutable std::vector<std::reference_wrapper<const SchemaField>> full_schemafield_;

 private:
  /// \brief Compare two schemas for equality.
  [[nodiscard]] bool Equals(const Schema& other) const;

  const std::optional<int32_t> schema_id_;

  Result<Status> InitIdToIndexMap() const;
  Result<Status> InitNameToIndexMap() const;
  Result<Status> InitLowerCaseNameToIndexMap() const;
};
}  // namespace iceberg
