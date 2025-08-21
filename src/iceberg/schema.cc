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

#include "iceberg/type.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep
namespace iceberg {
class IdVisitor {
 public:
  explicit IdVisitor(bool has_init_ = false);
  Status Visit(const Type& type);

  bool has_init;
  int index = 0;
  std::unordered_map<int, size_t> id_to_index;
  std::vector<std::reference_wrapper<const SchemaField>> full_schemafield;
};

std::string GetPath(const std::string& last_path, const std::string& field_name,
                    bool case_sensitive) {
  if (case_sensitive) {
    return last_path.empty() ? field_name : last_path + "." + field_name;
  }
  std::string lower_name(field_name);
  std::ranges::transform(lower_name, lower_name.begin(), ::tolower);
  return last_path.empty() ? lower_name : last_path + "." + lower_name;
}
class NameVisitor {
 public:
  explicit NameVisitor(bool case_sensitive_ = true, bool has_init_ = false);
  Status Visit(const ListType& type, const std::string& path,
               const std::string& short_path);
  Status Visit(const MapType& type, const std::string& path,
               const std::string& short_path);
  Status Visit(const StructType& type, const std::string& path,
               const std::string& short_path);
  Status Visit(const PrimitiveType& type, const std::string& path,
               const std::string& short_path);

  int index = 0;
  bool case_sensitive;
  bool has_init;
  std::unordered_map<std::string, size_t> name_to_index;
  std::vector<std::reference_wrapper<const SchemaField>> full_schemafield;
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
    ICEBERG_RETURN_UNEXPECTED(InitNameToIndexMap());
    auto it = name_to_index_.find(std::string(name));
    if (it == name_to_index_.end()) return std::nullopt;
    return full_schemafield_[it->second];
  }
  ICEBERG_RETURN_UNEXPECTED(InitLowerCaseNameToIndexMap());
  std::string lower_name(name);
  std::ranges::transform(lower_name, lower_name.begin(), ::tolower);
  auto it = lowercase_name_to_index_.find(lower_name);
  if (it == lowercase_name_to_index_.end()) return std::nullopt;
  return full_schemafield_[it->second];
}

Result<std::optional<std::reference_wrapper<const SchemaField>>> Schema::FindFieldByName(
    std::string_view name) const {
  return FindFieldByName(name, /*case_sensitive*/ true);
}

Result<Status> Schema::InitIdToIndexMap() const {
  if (!id_to_index_.empty()) {
    return {};
  }
  bool has_init = !full_schemafield_.empty();
  IdVisitor visitor(has_init);
  ICEBERG_RETURN_UNEXPECTED(VisitTypeInline(*this, &visitor));
  id_to_index_ = std::move(visitor.id_to_index);
  if (!has_init) {
    full_schemafield_ = std::move(visitor.full_schemafield);
  }
  return {};
}

Result<Status> Schema::InitNameToIndexMap() const {
  if (!name_to_index_.empty()) {
    return {};
  }
  bool has_init = !full_schemafield_.empty();
  std::string path, short_path;
  NameVisitor visitor(true, has_init);
  ICEBERG_RETURN_UNEXPECTED(VisitTypeInline(*this, &visitor, path, short_path));
  name_to_index_ = std::move(visitor.name_to_index);
  if (!has_init) {
    full_schemafield_ = std::move(visitor.full_schemafield);
  }
  return {};
}

Result<Status> Schema::InitLowerCaseNameToIndexMap() const {
  if (!lowercase_name_to_index_.empty()) {
    return {};
  }
  bool has_init = !full_schemafield_.empty();
  std::string path, short_path;
  NameVisitor visitor(false, has_init);
  ICEBERG_RETURN_UNEXPECTED(VisitTypeInline(*this, &visitor, path, short_path));
  lowercase_name_to_index_ = std::move(visitor.name_to_index);
  if (!has_init) {
    full_schemafield_ = std::move(visitor.full_schemafield);
  }
  return {};
}

Result<std::optional<std::reference_wrapper<const SchemaField>>> Schema::FindFieldById(
    int32_t field_id) const {
  ICEBERG_RETURN_UNEXPECTED(InitIdToIndexMap());
  auto it = id_to_index_.find(field_id);
  if (it == id_to_index_.end()) {
    return std::nullopt;
  }
  return full_schemafield_[it->second];
}

IdVisitor::IdVisitor(bool has_init_) : has_init(has_init_) {}

Status IdVisitor::Visit(const Type& type) {
  const auto& nested = iceberg::internal::checked_cast<const NestedType&>(type);
  const auto& fields = nested.fields();
  for (const auto& field : fields) {
    id_to_index[field.field_id()] = index++;
    if (!has_init) {
      full_schemafield.emplace_back(field);
    }
    if (field.type()->is_nested()) {
      ICEBERG_RETURN_UNEXPECTED(Visit(*field.type()));
    }
  }
  return {};
}

NameVisitor::NameVisitor(bool case_sensitive_, bool has_init_)
    : case_sensitive(case_sensitive_), has_init(has_init_) {}

Status NameVisitor::Visit(const ListType& type, const std::string& path,
                          const std::string& short_path) {
  const auto& field = type.fields()[0];
  std::string full_path =
      iceberg::GetPath(path, std::string(field.name()), case_sensitive);
  std::string short_full_path;
  if (field.type()->type_id() == TypeId::kStruct) {
    short_full_path = short_path;
  } else {
    short_full_path =
        iceberg::GetPath(short_path, std::string(field.name()), case_sensitive);
  }
  name_to_index[full_path] = index++;
  if (!has_init) {
    full_schemafield.emplace_back(field);
  }
  name_to_index.emplace(short_full_path, index - 1);
  if (field.type()->is_nested()) {
    ICEBERG_RETURN_UNEXPECTED(
        VisitTypeInline(*field.type(), this, full_path, short_full_path));
  }
  return {};
}

Status NameVisitor::Visit(const MapType& type, const std::string& path,
                          const std::string& short_path) {
  std::string full_path, short_full_path;
  for (const auto& field : type.fields()) {
    full_path = iceberg::GetPath(path, std::string(field.name()), case_sensitive);
    if (field.name() == MapType::kValueName &&
        field.type()->type_id() == TypeId::kStruct) {
      short_full_path = short_path;
    } else {
      short_full_path = iceberg::GetPath(path, std::string(field.name()), case_sensitive);
    }
    name_to_index[full_path] = index++;
    if (!has_init) {
      full_schemafield.emplace_back(field);
    }
    name_to_index.emplace(short_full_path, index - 1);
    if (field.type()->is_nested()) {
      ICEBERG_RETURN_UNEXPECTED(
          VisitTypeInline(*field.type(), this, full_path, short_full_path));
    }
  }
  return {};
}

Status NameVisitor::Visit(const StructType& type, const std::string& path,
                          const std::string& short_path) {
  const auto& fields = type.fields();
  std::string full_path, short_full_path;
  for (const auto& field : fields) {
    full_path = iceberg::GetPath(path, std::string(field.name()), case_sensitive);
    short_full_path =
        iceberg::GetPath(short_path, std::string(field.name()), case_sensitive);
    name_to_index[full_path] = index++;
    if (!has_init) {
      full_schemafield.emplace_back(field);
    }
    name_to_index.emplace(short_full_path, index - 1);
    if (field.type()->is_nested()) {
      ICEBERG_RETURN_UNEXPECTED(
          VisitTypeInline(*field.type(), this, full_path, short_full_path));
    }
  }
  return {};
}

Status NameVisitor::Visit(const PrimitiveType& type, const std::string& path,
                          const std::string& short_path) {
  return {};
}
}  // namespace iceberg
