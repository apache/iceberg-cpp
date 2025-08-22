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

#include <algorithm>
#include <format>

#include "iceberg/type.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep
#include "iceberg/util/macros.h"
#include "iceberg/util/visit_type.h"
namespace iceberg {
class IdVisitor {
 public:
  explicit IdVisitor(
      std::unordered_map<int32_t, std::reference_wrapper<const SchemaField>>&
          id_to_field);
  Status Visit(const Type& type);
  Status VisitNestedType(const Type& type);

 private:
  std::unordered_map<int32_t, std::reference_wrapper<const SchemaField>>& id_to_field_;
};
class NametoIdVisitor {
 public:
  explicit NametoIdVisitor(std::unordered_map<std::string, size_t>& name_to_id,
                           bool case_sensitive_ = true);
  Status Visit(const ListType& type, const std::string& path,
               const std::string& short_path);
  Status Visit(const MapType& type, const std::string& path,
               const std::string& short_path);
  Status Visit(const StructType& type, const std::string& path,
               const std::string& short_path);
  Status Visit(const PrimitiveType& type, const std::string& path,
               const std::string& short_path);
  static std::string GetPath(const std::string& last_path, const std::string& field_name,
                             bool case_sensitive = true);

 private:
  bool case_sensitive_;
  std::unordered_map<std::string, size_t>& name_to_id_;
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
    auto it = name_to_id_.find(std::string(name));
    if (it == name_to_id_.end()) return std::nullopt;
    return FindFieldById(it->second);
  }
  ICEBERG_RETURN_UNEXPECTED(InitLowerCaseNameToIndexMap());
  std::string lower_name(name);
  std::ranges::transform(lower_name, lower_name.begin(), ::tolower);
  auto it = lowercase_name_to_id_.find(lower_name);
  if (it == lowercase_name_to_id_.end()) return std::nullopt;
  return FindFieldById(it->second);
}

Result<std::optional<std::reference_wrapper<const SchemaField>>> Schema::FindFieldByName(
    std::string_view name) const {
  return FindFieldByName(name, /*case_sensitive*/ true);
}

Result<Status> Schema::InitIdToIndexMap() const {
  if (!id_to_field_.empty()) {
    return {};
  }
  IdVisitor visitor(id_to_field_);
  ICEBERG_RETURN_UNEXPECTED(VisitTypeInline(*this, &visitor));
  return {};
}

Result<Status> Schema::InitNameToIndexMap() const {
  if (!name_to_id_.empty()) {
    return {};
  }
  std::string path, short_path;
  NametoIdVisitor visitor(name_to_id_, true);
  ICEBERG_RETURN_UNEXPECTED(VisitTypeInline(*this, &visitor, path, short_path));
  return {};
}

Result<Status> Schema::InitLowerCaseNameToIndexMap() const {
  if (!lowercase_name_to_id_.empty()) {
    return {};
  }
  std::string path, short_path;
  NametoIdVisitor visitor(lowercase_name_to_id_, false);
  ICEBERG_RETURN_UNEXPECTED(VisitTypeInline(*this, &visitor, path, short_path));
  return {};
}

Result<std::optional<std::reference_wrapper<const SchemaField>>> Schema::FindFieldById(
    int32_t field_id) const {
  ICEBERG_RETURN_UNEXPECTED(InitIdToIndexMap());
  auto it = id_to_field_.find(field_id);
  if (it == id_to_field_.end()) {
    return std::nullopt;
  }
  return it->second;
}

IdVisitor::IdVisitor(
    std::unordered_map<int32_t, std::reference_wrapper<const SchemaField>>& id_to_field)
    : id_to_field_(id_to_field) {}

Status IdVisitor::Visit(const Type& type) {
  if (type.is_nested()) {
    ICEBERG_RETURN_UNEXPECTED(VisitNestedType(type));
  }
  return {};
}

Status IdVisitor::VisitNestedType(const Type& type) {
  const auto& nested = iceberg::internal::checked_cast<const NestedType&>(type);
  const auto& fields = nested.fields();
  for (const auto& field : fields) {
    id_to_field_.emplace(field.field_id(), std::cref(field));
    ICEBERG_RETURN_UNEXPECTED(Visit(*field.type()));
  }
  return {};
}

NametoIdVisitor::NametoIdVisitor(std::unordered_map<std::string, size_t>& name_to_id,
                                 bool case_sensitive)
    : name_to_id_(name_to_id), case_sensitive_(case_sensitive) {}

Status NametoIdVisitor::Visit(const ListType& type, const std::string& path,
                              const std::string& short_path) {
  const auto& field = type.fields()[0];
  std::string full_path = GetPath(path, std::string(field.name()), case_sensitive_);
  std::string short_full_path;
  if (field.type()->type_id() == TypeId::kStruct) {
    short_full_path = short_path;
  } else {
    short_full_path = GetPath(short_path, std::string(field.name()), case_sensitive_);
  }
  name_to_id_[full_path] = field.field_id();
  name_to_id_.emplace(short_full_path, field.field_id());
  ICEBERG_RETURN_UNEXPECTED(
      VisitTypeInline(*field.type(), this, full_path, short_full_path));
  return {};
}

Status NametoIdVisitor::Visit(const MapType& type, const std::string& path,
                              const std::string& short_path) {
  std::string full_path, short_full_path;
  const auto& fields = type.fields();
  for (const auto& field : fields) {
    full_path = GetPath(path, std::string(field.name()), case_sensitive_);
    if (field.name() == MapType::kValueName &&
        field.type()->type_id() == TypeId::kStruct) {
      short_full_path = short_path;
    } else {
      short_full_path = GetPath(path, std::string(field.name()), case_sensitive_);
    }
    name_to_id_[full_path] = field.field_id();
    name_to_id_.emplace(short_full_path, field.field_id());
    ICEBERG_RETURN_UNEXPECTED(
        VisitTypeInline(*field.type(), this, full_path, short_full_path));
  }
  return {};
}

Status NametoIdVisitor::Visit(const StructType& type, const std::string& path,
                              const std::string& short_path) {
  const auto& fields = type.fields();
  std::string full_path, short_full_path;
  for (const auto& field : fields) {
    full_path =
        NametoIdVisitor::GetPath(path, std::string(field.name()), case_sensitive_);
    short_full_path = GetPath(short_path, std::string(field.name()), case_sensitive_);
    name_to_id_[full_path] = field.field_id();
    name_to_id_.emplace(short_full_path, field.field_id());
    ICEBERG_RETURN_UNEXPECTED(
        VisitTypeInline(*field.type(), this, full_path, short_full_path));
  }
  return {};
}

Status NametoIdVisitor::Visit(const PrimitiveType& type, const std::string& path,
                              const std::string& short_path) {
  return {};
}

std::string NametoIdVisitor::GetPath(const std::string& last_path,
                                     const std::string& field_name, bool case_sensitive) {
  if (case_sensitive) {
    return last_path.empty() ? field_name : last_path + "." + field_name;
  }
  std::string lower_name(field_name);
  std::ranges::transform(lower_name, lower_name.begin(), ::tolower);
  return last_path.empty() ? lower_name : last_path + "." + lower_name;
}
}  // namespace iceberg
