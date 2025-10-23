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

#include "iceberg/schema.h"

#include <format>
#include <functional>

#include "iceberg/schema_internal.h"
#include "iceberg/type.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep
#include "iceberg/util/macros.h"
#include "iceberg/util/visit_type.h"

namespace iceberg {

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

Schema::Schema(std::vector<SchemaField> fields, std::optional<int32_t> schema_id)
    : StructType(std::move(fields)), schema_id_(schema_id) {}

std::optional<int32_t> Schema::schema_id() const { return schema_id_; }

std::string Schema::ToString() const {
  std::string repr = "schema<";
  for (const auto& field : fields_) {
    std::format_to(std::back_inserter(repr), "  {}\n", field);
  }
  repr += ">";
  return repr;
}

bool Schema::Equals(const Schema& other) const {
  return schema_id_ == other.schema_id_ && fields_ == other.fields_;
}

Result<std::optional<std::reference_wrapper<const SchemaField>>> Schema::FindFieldByName(
    std::string_view name, bool case_sensitive) const {
  if (case_sensitive) {
    ICEBERG_ASSIGN_OR_RAISE(auto name_to_id, name_to_id_.Get(*this));
    auto it = name_to_id.get().find(name);
    if (it == name_to_id.get().end()) {
      return std::nullopt;
    };
    return FindFieldById(it->second);
  }
  ICEBERG_ASSIGN_OR_RAISE(auto lowercase_name_to_id, lowercase_name_to_id_.Get(*this));
  auto it = lowercase_name_to_id.get().find(StringUtils::ToLower(name));
  if (it == lowercase_name_to_id.get().end()) {
    return std::nullopt;
  }
  return FindFieldById(it->second);
}

Result<std::unordered_map<int32_t, std::reference_wrapper<const SchemaField>>>
Schema::InitIdToFieldMap(const Schema& self) {
  std::unordered_map<int32_t, std::reference_wrapper<const SchemaField>> id_to_field;
  IdToFieldVisitor visitor(id_to_field);
  ICEBERG_RETURN_UNEXPECTED(VisitTypeInline(self, &visitor));
  return id_to_field;
}

Result<std::unordered_map<std::string, int32_t, StringHash, std::equal_to<>>>
Schema::InitNameToIdMap(const Schema& self) {
  std::unordered_map<std::string, int32_t, StringHash, std::equal_to<>> name_to_id;
  NameToIdVisitor visitor(name_to_id, /*case_sensitive=*/true);
  ICEBERG_RETURN_UNEXPECTED(
      VisitTypeInline(self, &visitor, /*path=*/"", /*short_path=*/""));
  visitor.Finish();
  return name_to_id;
}

Result<std::unordered_map<std::string, int32_t, StringHash, std::equal_to<>>>
Schema::InitLowerCaseNameToIdMap(const Schema& self) {
  std::unordered_map<std::string, int32_t, StringHash, std::equal_to<>>
      lowercase_name_to_id;
  NameToIdVisitor visitor(lowercase_name_to_id, /*case_sensitive=*/false);
  ICEBERG_RETURN_UNEXPECTED(
      VisitTypeInline(self, &visitor, /*path=*/"", /*short_path=*/""));
  visitor.Finish();
  return lowercase_name_to_id;
}

Result<std::optional<std::reference_wrapper<const SchemaField>>> Schema::FindFieldById(
    int32_t field_id) const {
  ICEBERG_ASSIGN_OR_RAISE(auto id_to_field, id_to_field_.Get(*this));
  auto it = id_to_field.get().find(field_id);
  if (it == id_to_field.get().end()) {
    return std::nullopt;
  }
  return it->second;
}

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
    bool case_sensitive, std::function<std::string(std::string_view)> quoting_func)
    : case_sensitive_(case_sensitive),
      name_to_id_(name_to_id),
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
  for (auto&& it : short_name_to_id_) {
    name_to_id_.try_emplace(it.first, it.second);
  }
}

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

Result<std::unique_ptr<Schema>> Schema::Select(std::span<const std::string> names,
                                               bool case_sensitive) const {
  const std::string kAllColumns = "*";
  if (std::ranges::find(names, kAllColumns) != names.end()) {
    auto struct_type = ToStructType(*this);
    return FromStructType(std::move(*struct_type), std::nullopt);
  }

  std::unordered_set<int32_t> selected_ids;
  for (const auto& name : names) {
    ICEBERG_ASSIGN_OR_RAISE(auto result, FindFieldByName(name, case_sensitive));
    if (result.has_value()) {
      selected_ids.insert(result.value().get().field_id());
    }
  }

  PruneColumnVisitor visitor(selected_ids, /*select_full_types=*/true);
  ICEBERG_ASSIGN_OR_RAISE(
      auto pruned_type, visitor.Visit(std::shared_ptr<StructType>(ToStructType(*this))));

  if (!pruned_type) {
    return std::make_unique<Schema>(std::vector<SchemaField>{}, std::nullopt);
  }

  if (pruned_type->type_id() != TypeId::kStruct) {
    return InvalidSchema("Projected type must be a struct type");
  }

  return FromStructType(std::move(internal::checked_cast<StructType&>(*pruned_type)),
                        std::nullopt);
}

Result<std::unique_ptr<Schema>> Schema::Project(
    const std::unordered_set<int32_t>& field_ids) const {
  PruneColumnVisitor visitor(field_ids, /*select_full_types=*/false);
  ICEBERG_ASSIGN_OR_RAISE(
      auto project_type, visitor.Visit(std::shared_ptr<StructType>(ToStructType(*this))));

  if (!project_type) {
    return std::make_unique<Schema>(std::vector<SchemaField>{}, std::nullopt);
  }

  if (project_type->type_id() != TypeId::kStruct) {
    return InvalidSchema("Projected type must be a struct type");
  }

  return FromStructType(std::move(internal::checked_cast<StructType&>(*project_type)),
                        std::nullopt);
}

}  // namespace iceberg
