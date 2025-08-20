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

#include "iceberg/exception.h"
#include "iceberg/type.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep
namespace iceberg {

Schema::Schema(std::vector<SchemaField> fields, std::optional<int32_t> schema_id)
    : StructType(std::move(fields)), schema_id_(schema_id) {
  InitIdToIndexMap();
}

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

std::optional<std::reference_wrapper<const SchemaField>> Schema::GetFieldByName(
    std::string_view name, bool case_sensitive) const {
  if (case_sensitive) {
    InitNameToIndexMap();
    auto it = name_to_index_.find(std::string(name));
    if (it == name_to_index_.end()) return std::nullopt;
    return full_schemafield_[it->second];
  }
  InitLowerCaseNameToIndexMap();
  std::string lower_name(name);
  std::ranges::transform(lower_name, lower_name.begin(), ::tolower);
  auto it = lowercase_name_to_index_.find(lower_name);
  if (it == lowercase_name_to_index_.end()) return std::nullopt;
  return full_schemafield_[it->second];
}

std::optional<std::reference_wrapper<const SchemaField>> Schema::GetFieldByName(
    std::string_view name) const {
  return GetFieldByName(name, true);
}

void Schema::InitIdToIndexMap() const {
  if (!id_to_index_.empty()) {
    return;
  }
  SchemaFieldVisitor visitor;
  auto result = VisitTypeInline(*this, &visitor, id_to_index_, full_schemafield_);
}

void Schema::InitNameToIndexMap() const {
  if (!name_to_index_.empty()) {
    return;
  }
  int index = 0;
  std::string_view path, short_path;
  SchemaFieldVisitor visitor;
  std::unordered_map<std::string, size_t> shortname_to_index;
  auto tmp = VisitTypeInline(*this, &visitor, name_to_index_, path, shortname_to_index,
                             short_path, index, true);
  if (!tmp.has_value()) {
    throw IcebergError("Failed to perform InitNameToIndexMap");
  }
  for (const auto& pair : shortname_to_index) {
    if (!name_to_index_.count(pair.first)) {
      name_to_index_.emplace(pair.first, pair.second);
    }
  }
}

void Schema::InitLowerCaseNameToIndexMap() const {
  if (!lowercase_name_to_index_.empty()) {
    return;
  }
  int index = 0;
  std::string_view path, short_path;
  SchemaFieldVisitor visitor;
  std::unordered_map<std::string, size_t> shortlowercasename_to_index;
  auto tmp = VisitTypeInline(*this, &visitor, lowercase_name_to_index_, path,
                             shortlowercasename_to_index, short_path, index, false);
  if (!tmp.has_value()) {
    throw IcebergError("Failed to perform InitLowerCaseNameToIndexMap");
  }
  for (const auto& pair : shortlowercasename_to_index) {
    if (!lowercase_name_to_index_.count(pair.first)) {
      lowercase_name_to_index_.emplace(pair.first, pair.second);
    }
  }
}

std::optional<std::reference_wrapper<const SchemaField>> Schema::GetFieldById(
    int32_t field_id) const {
  InitIdToIndexMap();
  auto it = id_to_index_.find(field_id);
  if (it == id_to_index_.end()) {
    return std::nullopt;
  }
  return full_schemafield_[it->second];
}

Status SchemaFieldVisitor::Visit(const Type& type,
                                 std::unordered_map<int, size_t>& id_to_index,
                                 std::vector<SchemaField>& full_schemafield) {
  const auto& nested = iceberg::internal::checked_cast<const NestedType&>(type);
  for (const auto& field : nested.fields()) {
    id_to_index[field.field_id()] = full_schemafield.size();
    full_schemafield.emplace_back(field);
    if (field.type()->is_nested()) {
      auto tmp = Visit(*field.type(), id_to_index, full_schemafield);
      if (!tmp.has_value()) {
        throw IcebergError("Failed to perform visit(id_to_index)");
      }
    }
  }
  return {};
}
std::string SchemaFieldVisitor::GetPath(const std::string& last_path,
                                        const std::string& field_name,
                                        bool case_sensitive) {
  if (case_sensitive) {
    return last_path.empty() ? field_name : last_path + "." + field_name;
  }
  std::string lower_name(field_name);
  std::ranges::transform(lower_name, lower_name.begin(), ::tolower);
  return last_path.empty() ? lower_name : last_path + "." + lower_name;
}

Status SchemaFieldVisitor::Visit(
    const Type& type, std::unordered_map<std::string, size_t>& name_to_index,
    std::string_view path, std::unordered_map<std::string, size_t>& shortname_to_index,
    std::string_view short_path, int& index, bool case_sensitive) {
  const char dot = '.';
  const auto& nested = iceberg::internal::checked_cast<const NestedType&>(type);
  for (const auto& field : nested.fields()) {
    std::string full_path, short_full_path;
    full_path = GetPath(std::string(path), std::string(field.name()), case_sensitive);
    name_to_index[full_path] = index;

    if (type.type_id() == TypeId::kList and field.type()->type_id() == TypeId::kStruct) {
      short_full_path = short_path;
    } else if (type.type_id() == TypeId::kMap and field.name() == "value" and
               field.type()->type_id() == TypeId::kStruct) {
      short_full_path = short_path;
    } else {
      short_full_path =
          GetPath(std::string(short_path), std::string(field.name()), case_sensitive);
    }
    shortname_to_index[short_full_path] = index++;
    if (field.type()->is_nested()) {
      auto tmp = Visit(*field.type(), name_to_index, full_path, shortname_to_index,
                       short_full_path, index, case_sensitive);
      if (!tmp.has_value()) {
        throw IcebergError("Failed to perform visit(name_to_index)");
      }
    }
  }
  return {};
}

}  // namespace iceberg
