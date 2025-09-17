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

}  // namespace iceberg
