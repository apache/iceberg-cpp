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
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/schema_field.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/string_util.h"

/// \file iceberg/util/type_util.h
/// Utility functions and visitors for Iceberg types.

namespace iceberg {

/// \brief Visitor for building a map from field ID to SchemaField reference.
class IdToFieldVisitor {
 public:
  explicit IdToFieldVisitor(
      std::unordered_map<int32_t, std::reference_wrapper<const SchemaField>>&
          id_to_field);
  Status Visit(const PrimitiveType& type);
  Status Visit(const NestedType& type);

 private:
  std::unordered_map<int32_t, std::reference_wrapper<const SchemaField>>& id_to_field_;
};

/// \brief Visitor for building maps from field name to field ID and field ID to field
/// name.
class NameToIdVisitor {
 public:
  explicit NameToIdVisitor(
      std::unordered_map<std::string, int32_t, StringHash, std::equal_to<>>& name_to_id,
      std::unordered_map<int32_t, std::string>* id_to_name, bool case_sensitive = true,
      std::function<std::string(std::string_view)> quoting_func = {});
  Status Visit(const ListType& type, const std::string& path,
               const std::string& short_path);
  Status Visit(const MapType& type, const std::string& path,
               const std::string& short_path);
  Status Visit(const StructType& type, const std::string& path,
               const std::string& short_path);
  Status Visit(const PrimitiveType& type, const std::string& path,
               const std::string& short_path);
  void Finish();

 private:
  std::string BuildPath(std::string_view prefix, std::string_view field_name,
                        bool case_sensitive);

 private:
  bool case_sensitive_;
  std::unordered_map<std::string, int32_t, StringHash, std::equal_to<>>& name_to_id_;
  std::unordered_map<int32_t, std::string>* id_to_name_;
  std::unordered_map<std::string, int32_t, StringHash, std::equal_to<>> short_name_to_id_;
  std::function<std::string(std::string_view)> quoting_func_;
};

/// \brief Visitor for building a map from field ID to position path.
class PositionPathVisitor {
 public:
  Status Visit(const PrimitiveType& type);
  Status Visit(const StructType& type);
  Status Visit(const ListType& type);
  Status Visit(const MapType& type);
  std::unordered_map<int32_t, std::vector<size_t>> Finish();

 private:
  constexpr static int32_t kUnassignedFieldId = -1;
  int32_t current_field_id_ = kUnassignedFieldId;
  std::vector<size_t> current_path_;
  std::unordered_map<int32_t, std::vector<size_t>> position_path_;
};

/// \brief Visitor for pruning columns based on selected field IDs.
///
/// This visitor traverses a schema and creates a projected version containing only
/// the specified fields. When `select_full_types` is true, a field with all its
/// sub-fields are selected if its field-id has been selected; otherwise, only leaf
/// fields of selected field-ids are selected.
///
/// \note It returns an error when projection is not successful.
class PruneColumnVisitor {
 public:
  PruneColumnVisitor(const std::unordered_set<int32_t>& selected_ids,
                     bool select_full_types);

  Result<std::shared_ptr<Type>> Visit(const std::shared_ptr<Type>& type) const;
  Result<std::shared_ptr<Type>> Visit(const SchemaField& field) const;
  static SchemaField MakeField(const SchemaField& field, std::shared_ptr<Type> type);
  Result<std::shared_ptr<Type>> Visit(const std::shared_ptr<StructType>& type) const;
  Result<std::shared_ptr<Type>> Visit(const std::shared_ptr<ListType>& type) const;
  Result<std::shared_ptr<Type>> Visit(const std::shared_ptr<MapType>& type) const;

 private:
  const std::unordered_set<int32_t>& selected_ids_;
  const bool select_full_types_;
};

/// \brief Visitor for getting projected field IDs.
class GetProjectedIdsVisitor {
 public:
  explicit GetProjectedIdsVisitor(bool include_struct_ids = false);

  Status Visit(const Type& type);
  Status VisitNested(const NestedType& type);
  Status VisitPrimitive(const PrimitiveType& type);
  std::unordered_set<int32_t> Finish() const;

 private:
  const bool include_struct_ids_;
  std::unordered_set<int32_t> ids_;
};

/// \brief Index parent field IDs for all fields in a struct hierarchy.
/// \param root_struct The root struct type to analyze
/// \return A map from field ID to its parent struct field ID
/// \note This function assumes the input StructType has already been validated:
///       - All field IDs must be non-negative
///       - All field IDs must be unique across the entire schema hierarchy
///       If the struct is part of a Schema, these invariants are enforced by
///       StructType::InitFieldById which checks for duplicate field IDs.
ICEBERG_EXPORT std::unordered_map<int32_t, int32_t> IndexParents(
    const StructType& root_struct);

/// \brief Assigns fresh IDs to all fields in the schema.
class AssignFreshIdVisitor {
 public:
  explicit AssignFreshIdVisitor(std::function<int32_t()> next_id);

  std::shared_ptr<Type> Visit(const std::shared_ptr<Type>& type) const;
  std::shared_ptr<StructType> Visit(const StructType& type) const;
  std::shared_ptr<ListType> Visit(const ListType& type) const;
  std::shared_ptr<MapType> Visit(const MapType& type) const;

 private:
  std::function<int32_t()> next_id_;
};

/// \brief Assigns fresh IDs to all fields in a schema.
///
/// \param schema_id An ID assigned to this schema
/// \param schema The schema to assign IDs to.
/// \param next_id An id assignment function, which returns the next ID to assign.
/// \return A schema with new ids assigned by the next_id function.
ICEBERG_EXPORT Result<std::shared_ptr<Schema>> AssignFreshIds(
    int32_t schema_id, const Schema& schema, std::function<int32_t()> next_id);

/// \brief Check if type promotion from one type to another is allowed.
///
/// Type promotion rules:
/// - int -> long
/// - float -> double
/// - decimal(P,S) -> decimal(P',S) where P' > P
///
/// \param from_type The original type
/// \param to_type The target type
/// \return true if promotion is allowed, false otherwise
ICEBERG_EXPORT bool IsPromotionAllowed(const std::shared_ptr<Type>& from_type,
                                       const std::shared_ptr<Type>& to_type);

}  // namespace iceberg
