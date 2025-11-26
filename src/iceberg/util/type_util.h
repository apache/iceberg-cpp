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
#include <span>
#include <stack>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "iceberg/result.h"
#include "iceberg/schema_field.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/string_util.h"
#include "iceberg/util/visit_type.h"

/// \file iceberg/util/type_util.h
/// Utility functions and visitors for Iceberg types.

namespace iceberg {

/// \brief Visitor for building a map from field ID to SchemaField reference.
/// Corresponds to Java's IndexById visitor.
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

/// \brief Visitor for building a map from field name to field ID.
/// Corresponds to Java's IndexByName visitor.
class NameToIdVisitor {
 public:
  explicit NameToIdVisitor(
      std::unordered_map<std::string, int32_t, StringHash, std::equal_to<>>& name_to_id,
      bool case_sensitive = true,
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
  std::unordered_map<std::string, int32_t, StringHash, std::equal_to<>> short_name_to_id_;
  std::function<std::string(std::string_view)> quoting_func_;
};

/// \brief Visitor for building a map from field ID to position path.
/// Used for efficient field access in StructLike.
class PositionPathVisitor {
 public:
  Status Visit(const PrimitiveType& type) {
    if (current_field_id_ == kUnassignedFieldId) {
      return InvalidSchema("Current field id is not assigned, type: {}", type.ToString());
    }

    if (auto ret = position_path_.try_emplace(current_field_id_, current_path_);
        !ret.second) {
      return InvalidSchema("Duplicate field id found: {}, prev path: {}, curr path: {}",
                           current_field_id_, ret.first->second, current_path_);
    }

    return {};
  }

  Status Visit(const StructType& type) {
    for (size_t i = 0; i < type.fields().size(); ++i) {
      const auto& field = type.fields()[i];
      current_field_id_ = field.field_id();
      current_path_.push_back(i);
      ICEBERG_RETURN_UNEXPECTED(VisitTypeInline(*field.type(), this));
      current_path_.pop_back();
    }
    return {};
  }

  // Non-struct types are not supported yet, but it is not an error.
  Status Visit(const ListType& type) { return {}; }
  Status Visit(const MapType& type) { return {}; }

  std::unordered_map<int32_t, std::vector<size_t>> Finish() {
    return std::move(position_path_);
  }

 private:
  constexpr static int32_t kUnassignedFieldId = -1;
  int32_t current_field_id_ = kUnassignedFieldId;
  std::vector<size_t> current_path_;
  std::unordered_map<int32_t, std::vector<size_t>> position_path_;
};

/// \brief Visitor for pruning columns based on selected field IDs.
/// Corresponds to Java's PruneColumns visitor.
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
                     bool select_full_types)
      : selected_ids_(selected_ids), select_full_types_(select_full_types) {}

  Result<std::shared_ptr<Type>> Visit(const std::shared_ptr<Type>& type) const {
    switch (type->type_id()) {
      case TypeId::kStruct:
        return Visit(internal::checked_pointer_cast<StructType>(type));
      case TypeId::kList:
        return Visit(internal::checked_pointer_cast<ListType>(type));
      case TypeId::kMap:
        return Visit(internal::checked_pointer_cast<MapType>(type));
      default:
        return nullptr;
    }
  }

  Result<std::shared_ptr<Type>> Visit(const SchemaField& field) const {
    if (selected_ids_.contains(field.field_id())) {
      return (select_full_types_ || field.type()->is_primitive()) ? field.type()
                                                                  : Visit(field.type());
    }
    return Visit(field.type());
  }

  static SchemaField MakeField(const SchemaField& field, std::shared_ptr<Type> type) {
    return {field.field_id(), std::string(field.name()), std::move(type),
            field.optional(), std::string(field.doc())};
  }

  Result<std::shared_ptr<Type>> Visit(const std::shared_ptr<StructType>& type) const {
    bool same_types = true;
    std::vector<SchemaField> selected_fields;
    for (const auto& field : type->fields()) {
      ICEBERG_ASSIGN_OR_RAISE(auto child_type, Visit(field));
      if (child_type) {
        same_types = same_types && (child_type == field.type());
        selected_fields.emplace_back(MakeField(field, std::move(child_type)));
      }
    }

    if (selected_fields.empty()) {
      return nullptr;
    } else if (same_types && selected_fields.size() == type->fields().size()) {
      return type;
    }
    return std::make_shared<StructType>(std::move(selected_fields));
  }

  Result<std::shared_ptr<Type>> Visit(const std::shared_ptr<ListType>& type) const {
    const auto& elem_field = type->fields()[0];
    ICEBERG_ASSIGN_OR_RAISE(auto elem_type, Visit(elem_field));
    if (elem_type == nullptr) {
      return nullptr;
    } else if (elem_type == elem_field.type()) {
      return type;
    }
    return std::make_shared<ListType>(MakeField(elem_field, std::move(elem_type)));
  }

  Result<std::shared_ptr<Type>> Visit(const std::shared_ptr<MapType>& type) const {
    const auto& key_field = type->fields()[0];
    const auto& value_field = type->fields()[1];
    ICEBERG_ASSIGN_OR_RAISE(auto key_type, Visit(key_field));
    ICEBERG_ASSIGN_OR_RAISE(auto value_type, Visit(value_field));

    if (key_type == nullptr && value_type == nullptr) {
      return nullptr;
    } else if (value_type == value_field.type() &&
               (key_type == key_field.type() || key_type == nullptr)) {
      return type;
    } else if (value_type == nullptr) {
      return InvalidArgument("Cannot project Map without value field");
    }
    return std::make_shared<MapType>(
        (key_type == nullptr ? key_field : MakeField(key_field, std::move(key_type))),
        MakeField(value_field, std::move(value_type)));
  }

 private:
  const std::unordered_set<int32_t>& selected_ids_;
  const bool select_full_types_;
};

/// \brief Index parent field IDs for all fields in a struct hierarchy.
/// Corresponds to Java's indexParents(Types.StructType struct).
/// \param root_struct The root struct type to analyze
/// \return A map from field ID to its parent struct field ID
/// \note This function assumes the input StructType has already been validated:
///       - All field IDs must be non-negative
///       - All field IDs must be unique across the entire schema hierarchy
///       If the struct is part of a Schema, these invariants are enforced by
///       StructType::InitFieldById which checks for duplicate field IDs.
static std::unordered_map<int32_t, int32_t> indexParents(const StructType& root_struct) {
  std::unordered_map<int32_t, int32_t> id_to_parent;
  std::stack<int32_t> parent_id_stack;

  // Recursive function to visit and build parent relationships
  std::function<void(const Type&)> visit = [&](const Type& type) -> void {
    switch (type.type_id()) {
      case TypeId::kStruct:
      case TypeId::kList:
      case TypeId::kMap: {
        const auto& nested_type = static_cast<const NestedType&>(type);
        for (const auto& field : nested_type.fields()) {
          if (!parent_id_stack.empty()) {
            id_to_parent[field.field_id()] = parent_id_stack.top();
          }
          parent_id_stack.push(field.field_id());
          visit(*field.type());
          parent_id_stack.pop();
        }
        break;
      }

      default:
        break;
    }
  };

  visit(root_struct);
  return id_to_parent;
}

}  // namespace iceberg
