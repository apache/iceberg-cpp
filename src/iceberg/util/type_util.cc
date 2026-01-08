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

#include "iceberg/util/type_util.h"

#include <stack>

#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/formatter_internal.h"
#include "iceberg/util/string_util.h"
#include "iceberg/util/visit_type.h"

namespace iceberg {

IdToFieldVisitor::IdToFieldVisitor(
    std::unordered_map<int32_t, std::reference_wrapper<const SchemaField>>& id_to_field)
    : id_to_field_(id_to_field) {}

Status IdToFieldVisitor::Visit(const PrimitiveType& type) { return {}; }

Status IdToFieldVisitor::Visit(const NestedType& type) {
  const auto& nested = internal::checked_cast<const NestedType&>(type);
  const auto& fields = nested.fields();
  for (const auto& field : fields) {
    auto it = id_to_field_.try_emplace(field.field_id(), std::cref(field));
    if (!it.second) {
      return InvalidSchema("Duplicate field id found: {}", field.field_id());
    }
    ICEBERG_RETURN_UNEXPECTED(VisitTypeInline(*field.type(), this));
  }
  return {};
}

NameToIdVisitor::NameToIdVisitor(
    std::unordered_map<std::string, int32_t, StringHash, std::equal_to<>>& name_to_id,
    std::unordered_map<int32_t, std::string>* id_to_name, bool case_sensitive,
    std::function<std::string(std::string_view)> quoting_func)
    : case_sensitive_(case_sensitive),
      name_to_id_(name_to_id),
      id_to_name_(id_to_name),
      quoting_func_(std::move(quoting_func)) {}

Status NameToIdVisitor::Visit(const ListType& type, const std::string& path,
                              const std::string& short_path) {
  const auto& field = type.fields()[0];
  std::string new_path = BuildPath(path, field.name(), case_sensitive_);
  std::string new_short_path;
  if (field.type()->type_id() == TypeId::kStruct) {
    new_short_path = short_path;
  } else {
    new_short_path = BuildPath(short_path, field.name(), case_sensitive_);
  }
  auto it = name_to_id_.try_emplace(new_path, field.field_id());
  if (!it.second) {
    return InvalidSchema("Duplicate path found: {}, prev id: {}, curr id: {}",
                         it.first->first, it.first->second, field.field_id());
  }
  short_name_to_id_.try_emplace(new_short_path, field.field_id());
  ICEBERG_RETURN_UNEXPECTED(
      VisitTypeInline(*field.type(), this, new_path, new_short_path));
  return {};
}

Status NameToIdVisitor::Visit(const MapType& type, const std::string& path,
                              const std::string& short_path) {
  std::string new_path, new_short_path;
  const auto& fields = type.fields();
  for (const auto& field : fields) {
    new_path = BuildPath(path, field.name(), case_sensitive_);
    if (field.name() == MapType::kValueName &&
        field.type()->type_id() == TypeId::kStruct) {
      new_short_path = short_path;
    } else {
      new_short_path = BuildPath(short_path, field.name(), case_sensitive_);
    }
    auto it = name_to_id_.try_emplace(new_path, field.field_id());
    if (!it.second) {
      return InvalidSchema("Duplicate path found: {}, prev id: {}, curr id: {}",
                           it.first->first, it.first->second, field.field_id());
    }
    short_name_to_id_.try_emplace(new_short_path, field.field_id());
    ICEBERG_RETURN_UNEXPECTED(
        VisitTypeInline(*field.type(), this, new_path, new_short_path));
  }
  return {};
}

Status NameToIdVisitor::Visit(const StructType& type, const std::string& path,
                              const std::string& short_path) {
  const auto& fields = type.fields();
  std::string new_path, new_short_path;
  for (const auto& field : fields) {
    new_path = BuildPath(path, field.name(), case_sensitive_);
    new_short_path = BuildPath(short_path, field.name(), case_sensitive_);
    auto it = name_to_id_.try_emplace(new_path, field.field_id());
    if (!it.second) {
      return InvalidSchema("Duplicate path found: {}, prev id: {}, curr id: {}",
                           it.first->first, it.first->second, field.field_id());
    }
    short_name_to_id_.try_emplace(new_short_path, field.field_id());
    ICEBERG_RETURN_UNEXPECTED(
        VisitTypeInline(*field.type(), this, new_path, new_short_path));
  }
  return {};
}

Status NameToIdVisitor::Visit(const PrimitiveType& type, const std::string& path,
                              const std::string& short_path) {
  return {};
}

std::string NameToIdVisitor::BuildPath(std::string_view prefix,
                                       std::string_view field_name, bool case_sensitive) {
  std::string quoted_name;
  if (!quoting_func_) {
    quoted_name = std::string(field_name);
  } else {
    quoted_name = quoting_func_(field_name);
  }
  if (case_sensitive) {
    return prefix.empty() ? quoted_name : std::string(prefix) + "." + quoted_name;
  }
  return prefix.empty() ? StringUtils::ToLower(quoted_name)
                        : std::string(prefix) + "." + StringUtils::ToLower(quoted_name);
}

void NameToIdVisitor::Finish() {
  if (id_to_name_) {
    for (auto& [name, id] : name_to_id_) {
      id_to_name_->try_emplace(id, name);
    }
  }
  for (auto&& it : short_name_to_id_) {
    name_to_id_.try_emplace(it.first, it.second);
  }
}

Status PositionPathVisitor::Visit(const PrimitiveType& type) {
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

Status PositionPathVisitor::Visit(const StructType& type) {
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
Status PositionPathVisitor::Visit(const ListType& type) { return {}; }

Status PositionPathVisitor::Visit(const MapType& type) { return {}; }

std::unordered_map<int32_t, std::vector<size_t>> PositionPathVisitor::Finish() {
  return std::move(position_path_);
}

PruneColumnVisitor::PruneColumnVisitor(const std::unordered_set<int32_t>& selected_ids,
                                       bool select_full_types)
    : selected_ids_(selected_ids), select_full_types_(select_full_types) {}

Result<std::shared_ptr<Type>> PruneColumnVisitor::Visit(
    const std::shared_ptr<Type>& type) const {
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

Result<std::shared_ptr<Type>> PruneColumnVisitor::Visit(const SchemaField& field) const {
  if (selected_ids_.contains(field.field_id())) {
    return (select_full_types_ || field.type()->is_primitive()) ? field.type()
                                                                : Visit(field.type());
  }
  return Visit(field.type());
}

SchemaField PruneColumnVisitor::MakeField(const SchemaField& field,
                                          std::shared_ptr<Type> type) {
  return {field.field_id(), std::string(field.name()), std::move(type), field.optional(),
          std::string(field.doc())};
}

Result<std::shared_ptr<Type>> PruneColumnVisitor::Visit(
    const std::shared_ptr<StructType>& type) const {
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

Result<std::shared_ptr<Type>> PruneColumnVisitor::Visit(
    const std::shared_ptr<ListType>& type) const {
  const auto& elem_field = type->fields()[0];
  ICEBERG_ASSIGN_OR_RAISE(auto elem_type, Visit(elem_field));
  if (elem_type == nullptr) {
    return nullptr;
  } else if (elem_type == elem_field.type()) {
    return type;
  }
  return std::make_shared<ListType>(MakeField(elem_field, std::move(elem_type)));
}

Result<std::shared_ptr<Type>> PruneColumnVisitor::Visit(
    const std::shared_ptr<MapType>& type) const {
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

GetProjectedIdsVisitor::GetProjectedIdsVisitor(bool include_struct_ids)
    : include_struct_ids_(include_struct_ids) {}

Status GetProjectedIdsVisitor::Visit(const Type& type) {
  if (type.is_nested()) {
    return VisitNested(internal::checked_cast<const NestedType&>(type));
  } else {
    return VisitPrimitive(internal::checked_cast<const PrimitiveType&>(type));
  }
}

Status GetProjectedIdsVisitor::VisitNested(const NestedType& type) {
  for (auto& field : type.fields()) {
    ICEBERG_RETURN_UNEXPECTED(Visit(*field.type()));
  }
  for (auto& field : type.fields()) {
    // TODO(zhuo.wang) or is_variant
    if ((include_struct_ids_ && field.type()->type_id() == TypeId::kStruct) ||
        field.type()->is_primitive()) {
      ids_.insert(field.field_id());
    }
  }
  return {};
}

Status GetProjectedIdsVisitor::VisitPrimitive(const PrimitiveType& type) { return {}; }

std::unordered_set<int32_t> GetProjectedIdsVisitor::Finish() const { return ids_; }

std::unordered_map<int32_t, int32_t> IndexParents(const StructType& root_struct) {
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

AssignFreshIdVisitor::AssignFreshIdVisitor(std::function<int32_t()> next_id)
    : next_id_(std::move(next_id)) {}

std::shared_ptr<Type> AssignFreshIdVisitor::Visit(
    const std::shared_ptr<Type>& type) const {
  switch (type->type_id()) {
    case TypeId::kStruct:
      return Visit(*internal::checked_pointer_cast<StructType>(type));
    case TypeId::kMap:
      return Visit(*internal::checked_pointer_cast<MapType>(type));
    case TypeId::kList:
      return Visit(*internal::checked_pointer_cast<ListType>(type));
    default:
      return type;
  }
}

std::shared_ptr<StructType> AssignFreshIdVisitor::Visit(const StructType& type) const {
  auto fresh_ids =
      type.fields() |
      std::views::transform([&](const auto& /* unused */) { return next_id_(); }) |
      std::ranges::to<std::vector<int32_t>>();
  std::vector<SchemaField> fresh_fields;
  for (size_t i = 0; i < type.fields().size(); ++i) {
    const auto& field = type.fields()[i];
    fresh_fields.emplace_back(fresh_ids[i], std::string(field.name()),
                              Visit(field.type()), field.optional(),
                              std::string(field.doc()));
  }
  return std::make_shared<StructType>(std::move(fresh_fields));
}

std::shared_ptr<ListType> AssignFreshIdVisitor::Visit(const ListType& type) const {
  const auto& elem_field = type.fields()[0];
  int32_t fresh_id = next_id_();
  SchemaField fresh_elem_field(fresh_id, std::string(elem_field.name()),
                               Visit(elem_field.type()), elem_field.optional(),
                               std::string(elem_field.doc()));
  return std::make_shared<ListType>(std::move(fresh_elem_field));
}

std::shared_ptr<MapType> AssignFreshIdVisitor::Visit(const MapType& type) const {
  const auto& key_field = type.fields()[0];
  const auto& value_field = type.fields()[1];

  int32_t fresh_key_id = next_id_();
  int32_t fresh_value_id = next_id_();

  SchemaField fresh_key_field(fresh_key_id, std::string(key_field.name()),
                              Visit(key_field.type()), key_field.optional(),
                              std::string(key_field.doc()));
  SchemaField fresh_value_field(fresh_value_id, std::string(value_field.name()),
                                Visit(value_field.type()), value_field.optional(),
                                std::string(value_field.doc()));
  return std::make_shared<MapType>(std::move(fresh_key_field),
                                   std::move(fresh_value_field));
}

Result<std::shared_ptr<Schema>> AssignFreshIds(int32_t schema_id, const Schema& schema,
                                               std::function<int32_t()> next_id) {
  auto fresh_type = AssignFreshIdVisitor(std::move(next_id))
                        .Visit(internal::checked_cast<const StructType&>(schema));
  std::vector<SchemaField> fields =
      fresh_type->fields() | std::ranges::to<std::vector<SchemaField>>();
  ICEBERG_ASSIGN_OR_RAISE(auto identifier_field_names, schema.IdentifierFieldNames());
  return Schema::Make(std::move(fields), schema_id, identifier_field_names);
}

bool IsPromotionAllowed(const std::shared_ptr<Type>& from_type,
                        const std::shared_ptr<Type>& to_type) {
  if (!from_type || !to_type) {
    return false;
  }

  // Same type is always allowed
  if (*from_type == *to_type) {
    return true;
  }

  // Both must be primitive types for promotion
  if (!from_type->is_primitive() || !to_type->is_primitive()) {
    return false;
  }

  TypeId from_id = from_type->type_id();
  TypeId to_id = to_type->type_id();

  // int -> long
  if (from_id == TypeId::kInt && to_id == TypeId::kLong) {
    return true;
  }

  // float -> double
  if (from_id == TypeId::kFloat && to_id == TypeId::kDouble) {
    return true;
  }

  // decimal(P,S) -> decimal(P',S) where P' > P
  if (from_id == TypeId::kDecimal && to_id == TypeId::kDecimal) {
    const auto& from_decimal = internal::checked_cast<const DecimalType&>(*from_type);
    const auto& to_decimal = internal::checked_cast<const DecimalType&>(*to_type);
    // Scale must be the same, precision can only increase
    return from_decimal.scale() == to_decimal.scale() &&
           from_decimal.precision() < to_decimal.precision();
  }

  return false;
}

}  // namespace iceberg
