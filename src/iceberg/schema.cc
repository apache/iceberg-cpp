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
    ICEBERG_RETURN_UNEXPECTED(
        LazyInitWithCallOnce(name_to_id_flag_, [this]() { return InitNameToIdMap(); }));
    auto it = name_to_id_.find(name);
    if (it == name_to_id_.end()) return std::nullopt;
    return FindFieldById(it->second);
  }
  ICEBERG_RETURN_UNEXPECTED(LazyInitWithCallOnce(
      lowercase_name_to_id_flag_, [this]() { return InitLowerCaseNameToIdMap(); }));
  auto it = lowercase_name_to_id_.find(StringUtils::ToLower(name));
  if (it == lowercase_name_to_id_.end()) return std::nullopt;
  return FindFieldById(it->second);
}

Status Schema::InitIdToFieldMap() const {
  if (!id_to_field_.empty()) {
    return {};
  }
  IdToFieldVisitor visitor(id_to_field_);
  ICEBERG_RETURN_UNEXPECTED(VisitTypeInline(*this, &visitor));
  return {};
}

Status Schema::InitNameToIdMap() const {
  if (!name_to_id_.empty()) {
    return {};
  }
  NameToIdVisitor visitor(name_to_id_, /*case_sensitive=*/true);
  ICEBERG_RETURN_UNEXPECTED(
      VisitTypeInline(*this, &visitor, /*path=*/"", /*short_path=*/""));
  visitor.Finish();
  return {};
}

Status Schema::InitLowerCaseNameToIdMap() const {
  if (!lowercase_name_to_id_.empty()) {
    return {};
  }
  NameToIdVisitor visitor(lowercase_name_to_id_, /*case_sensitive=*/false);
  ICEBERG_RETURN_UNEXPECTED(
      VisitTypeInline(*this, &visitor, /*path=*/"", /*short_path=*/""));
  visitor.Finish();
  return {};
}

Result<std::optional<std::reference_wrapper<const SchemaField>>> Schema::FindFieldById(
    int32_t field_id) const {
  ICEBERG_RETURN_UNEXPECTED(
      LazyInitWithCallOnce(id_to_field_flag_, [this]() { return InitIdToFieldMap(); }));
  auto it = id_to_field_.find(field_id);
  if (it == id_to_field_.end()) {
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
    : name_to_id_(name_to_id),
      case_sensitive_(case_sensitive),
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

class PruneColumnVisitor {
 public:
  explicit PruneColumnVisitor(const std::unordered_set<int32_t>& selected_ids,
                              bool select_full_types = false);
  Status Visit(const ListType& type, std::shared_ptr<const Type>& result);
  Status Visit(const MapType& type, std::shared_ptr<const Type>& result);
  Status Visit(const StructType& type, std::shared_ptr<const Type>& result);
  Status Visit(const PrimitiveType& type, std::shared_ptr<const Type>& result);
  Status ProjectList(const SchemaField& element,
                     std::shared_ptr<const Type>& child_result,
                     std::shared_ptr<const Type>& result);
  Status ProjectMap(const SchemaField& key_field, const SchemaField& value_field,
                    std::shared_ptr<const Type>& value_result,
                    std::shared_ptr<const Type>& result);

 private:
  const std::unordered_set<int32_t>& selected_ids_;
  bool select_full_types_;
};

Result<std::shared_ptr<const Schema>> Schema::select(
    const std::vector<std::string>& names, bool case_sensitive) const {
  return internalSelect(names, case_sensitive);
}

Result<std::shared_ptr<const Schema>> Schema::select(
    const std::initializer_list<std::string>& names, bool case_sensitive) const {
  return internalSelect(std::vector<std::string>(names), case_sensitive);
}

Result<std::shared_ptr<const Schema>> Schema::internalSelect(
    const std::vector<std::string>& names, bool case_sensitive) const {
  const std::string ALL_COLUMNS = "*";
  if (std::ranges::find(names, ALL_COLUMNS) != names.end()) {
    return shared_from_this();
  }

  std::unordered_set<int32_t> selected_ids;
  for (const auto& name : names) {
    ICEBERG_ASSIGN_OR_RAISE(auto result, FindFieldByName(name, case_sensitive));
    if (result.has_value()) {
      selected_ids.insert(result.value().get().field_id());
    }
  }

  std::shared_ptr<const Type> result;
  PruneColumnVisitor visitor(selected_ids, /*select_full_types=*/true);
  ICEBERG_RETURN_UNEXPECTED(VisitTypeInline(*this, &visitor, result));

  if (!result) {
    return std::make_shared<Schema>(std::vector<SchemaField>{}, schema_id_);
  }

  if (result->type_id() != TypeId::kStruct) {
    return InvalidSchema("Projected type must be a struct type");
  }

  const auto& projected_struct = internal::checked_cast<const StructType&>(*result);

  std::vector<SchemaField> fields_vec(projected_struct.fields().begin(),
                                      projected_struct.fields().end());
  return std::make_shared<Schema>(std::move(fields_vec), schema_id_);
}

Result<std::shared_ptr<const Schema>> Schema::project(
    std::unordered_set<int32_t>& selected_ids) const {
  PruneColumnVisitor visitor(selected_ids, /*select_full_types=*/false);

  std::shared_ptr<const Type> result;
  ICEBERG_RETURN_UNEXPECTED(VisitTypeInline(*this, &visitor, result));

  if (!result) {
    return std::make_shared<Schema>(std::vector<SchemaField>{}, schema_id_);
  }

  if (result->type_id() != TypeId::kStruct) {
    return InvalidSchema("Projected type must be a struct type");
  }

  const auto& projected_struct = internal::checked_cast<const StructType&>(*result);
  std::vector<SchemaField> fields_vec(projected_struct.fields().begin(),
                                      projected_struct.fields().end());
  return std::make_shared<Schema>(std::move(fields_vec), schema_id_);
}

PruneColumnVisitor::PruneColumnVisitor(const std::unordered_set<int32_t>& selected_ids,
                                       bool select_full_types)
    : selected_ids_(selected_ids), select_full_types_(select_full_types) {}

Status PruneColumnVisitor::Visit(const StructType& type,
                                 std::shared_ptr<const Type>& result) {
  std::vector<std::shared_ptr<const Type>> selected_types;
  const auto& fields = type.fields();
  for (const auto& field : fields) {
    std::shared_ptr<const Type> child_result;
    ICEBERG_RETURN_UNEXPECTED(VisitTypeInline(*field.type(), this, child_result));
    if (selected_ids_.contains(field.field_id())) {
      // select
      if (select_full_types_) {
        selected_types.emplace_back(field.type());
      } else if (field.type()->type_id() == TypeId::kStruct) {
        // project(kstruct)
        if (!child_result) {
          child_result = std::make_shared<StructType>(std::vector<SchemaField>{});
        }
        selected_types.emplace_back(std::move(child_result));
      } else {
        // project(list, map, primitive)
        if (!field.type()->is_primitive()) {
          return InvalidArgument(
              "Cannot explicitly project List or Map types, {}:{} of type {} was "
              "selected",
              field.field_id(), field.name(), field.type()->ToString());
        }
        selected_types.emplace_back(field.type());
      }
    } else if (child_result) {
      // project, select
      selected_types.emplace_back(std::move(child_result));
    } else {
      // project, select
      selected_types.emplace_back(nullptr);
    }
  }

  bool same_types = true;
  std::vector<SchemaField> selected_fields;
  for (size_t i = 0; i < fields.size(); i++) {
    if (fields[i].type() == selected_types[i]) {
      selected_fields.emplace_back(fields[i]);
    } else if (selected_types[i]) {
      same_types = false;
      selected_fields.emplace_back(fields[i].field_id(), std::string(fields[i].name()),
                                   std::const_pointer_cast<Type>(selected_types[i]),
                                   fields[i].optional(), std::string(fields[i].doc()));
    }
  }

  if (!selected_fields.empty()) {
    if (same_types && selected_fields.size() == fields.size()) {
      result = std::make_shared<StructType>(type);
    } else {
      result = std::make_shared<StructType>(std::move(selected_fields));
    }
  }

  return {};
}

Status PruneColumnVisitor::Visit(const ListType& type,
                                 std::shared_ptr<const Type>& result) {
  const auto& element_field = type.fields()[0];

  if (select_full_types_ and selected_ids_.contains(element_field.field_id())) {
    result = std::make_shared<ListType>(type);
    return {};
  }

  std::shared_ptr<const Type> child_result;
  ICEBERG_RETURN_UNEXPECTED(VisitTypeInline(*element_field.type(), this, child_result));

  if (selected_ids_.contains(element_field.field_id())) {
    if (element_field.type()->type_id() == TypeId::kStruct) {
      ICEBERG_RETURN_UNEXPECTED(ProjectList(element_field, child_result, result));
    } else {
      if (!element_field.type()->is_primitive()) {
        return InvalidArgument(
            "Cannot explicitly project List or Map types, List element {} of type {} was "
            "selected",
            element_field.field_id(), element_field.name());
      }
      result = std::make_shared<ListType>(element_field);
    }
  } else if (child_result) {
    ICEBERG_RETURN_UNEXPECTED(ProjectList(element_field, child_result, result));
  }

  return {};
}

Status PruneColumnVisitor::Visit(const MapType& type,
                                 std::shared_ptr<const Type>& result) {
  const auto& key_field = type.fields()[0];
  const auto& value_field = type.fields()[1];

  if (select_full_types_ and selected_ids_.contains(value_field.field_id())) {
    result = std::make_shared<MapType>(type);
    return {};
  }

  std::shared_ptr<const Type> key_result;
  ICEBERG_RETURN_UNEXPECTED(VisitTypeInline(*key_field.type(), this, key_result));

  std::shared_ptr<const Type> value_result;
  ICEBERG_RETURN_UNEXPECTED(VisitTypeInline(*value_field.type(), this, value_result));

  if (selected_ids_.contains(value_field.field_id())) {
    if (value_field.type()->type_id() == TypeId::kStruct) {
      ICEBERG_RETURN_UNEXPECTED(ProjectMap(key_field, value_field, value_result, result));
    } else {
      if (!value_field.type()->is_primitive()) {
        return InvalidArgument(
            "Cannot explicitly project List or Map types, Map value {} of type {} was "
            "selected",
            value_field.field_id(), type.ToString());
      }
      result = std::make_shared<MapType>(type);
    }
  } else if (value_result) {
    ICEBERG_RETURN_UNEXPECTED(ProjectMap(key_field, value_field, value_result, result));
  } else if (selected_ids_.contains(key_field.field_id())) {
    result = std::make_shared<MapType>(type);
  }

  return {};
}

Status PruneColumnVisitor::Visit(const PrimitiveType& type,
                                 std::shared_ptr<const Type>& result) {
  return {};
}

Status PruneColumnVisitor::ProjectList(const SchemaField& element_field,
                                       std::shared_ptr<const Type>& child_result,
                                       std::shared_ptr<const Type>& result) {
  if (!child_result) {
    return InvalidArgument("Cannot project a list when the element result is null");
  }
  if (element_field.type() == child_result) {
    result = std::make_shared<ListType>(element_field);
  } else {
    result = std::make_shared<ListType>(element_field.field_id(),
                                        std::const_pointer_cast<Type>(child_result),
                                        element_field.optional());
  }
  return {};
}

Status PruneColumnVisitor::ProjectMap(const SchemaField& key_field,
                                      const SchemaField& value_field,
                                      std::shared_ptr<const Type>& value_result,
                                      std::shared_ptr<const Type>& result) {
  if (!value_result) {
    return InvalidArgument("Attempted to project a map without a defined map value type");
  }
  if (value_field.type() == value_result) {
    result = std::make_shared<MapType>(key_field, value_field);
  } else {
    result = std::make_shared<MapType>(
        key_field,
        SchemaField(value_field.field_id(), std::string(value_field.name()),
                    std::const_pointer_cast<Type>(value_result), value_field.optional()));
  }
  return {};
}

}  // namespace iceberg
