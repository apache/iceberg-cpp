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

#include "iceberg/name_mapping.h"

#include <format>
#include <sstream>

#include "iceberg/util/formatter_internal.h"

namespace iceberg {

namespace {

// Helper function to join a list of field names with a dot
std::string JoinByDot(std::span<const std::string> parts) {
  std::stringstream ss;
  for (size_t i = 0; i < parts.size(); ++i) {
    if (i > 0) {
      ss << ".";
    }
    ss << parts[i];
  }
  return ss.str();
}

// Helper class to recursively index MappedField by field id
struct IndexByIdVisitor {
  std::unordered_map<int32_t, MappedFieldConstRef> field_by_id;

  void Visit(const MappedField& field) {
    if (field.field_id.has_value()) {
      field_by_id.emplace(field.field_id.value(), std::cref(field));
    }
    if (field.nested_mapping != nullptr) {
      Visit(*field.nested_mapping);
    }
  }

  void Visit(const MappedFields& fields) {
    for (const auto& field : fields.fields()) {
      Visit(field);
    }
  }

  void Visit(const NameMapping& name_mapping) { Visit(name_mapping.AsMappedFields()); }
};

// Helper class to recursively index MappedField by field name
struct IndexByNameVisitor {
  std::unordered_map<std::string, MappedFieldConstRef> field_by_name;

  void Visit(const MappedField& field) {
    for (const auto& name : field.names) {
      field_by_name.emplace(name, std::cref(field));
    }

    if (field.nested_mapping != nullptr) {
      IndexByNameVisitor nested_visitor;
      nested_visitor.Visit(*field.nested_mapping);

      for (const auto& [name, mapped_field] : nested_visitor.field_by_name) {
        for (const auto& prefix : field.names) {
          std::vector<std::string> parts = {prefix, name};
          field_by_name.emplace(JoinByDot(parts), std::cref(mapped_field));
        }
      }
    }
  }

  void Visit(const MappedFields& fields) {
    for (const auto& field : fields.fields()) {
      Visit(field);
    }
  }

  void Visit(const NameMapping& name_mapping) { Visit(name_mapping.AsMappedFields()); }
};

}  // namespace

MappedFields::MappedFields(std::vector<MappedField> fields)
    : fields_(std::move(fields)) {}

std::unique_ptr<MappedFields> MappedFields::Make(std::vector<MappedField> fields) {
  return std::unique_ptr<MappedFields>(new MappedFields(std::move(fields)));
}

std::optional<MappedFieldConstRef> MappedFields::Field(int32_t id) const {
  const auto& id_to_field = LazyIdToField();
  if (auto it = id_to_field.find(id); it != id_to_field.cend()) {
    return it->second;
  }
  return std::nullopt;
}

std::optional<int32_t> MappedFields::Id(std::string_view name) const {
  const auto& name_to_id = LazyNameToId();
  if (auto it = name_to_id.find(name); it != name_to_id.cend()) {
    return it->second;
  }
  return std::nullopt;
}

size_t MappedFields::Size() const { return fields_.size(); }

std::span<const MappedField> MappedFields::fields() const { return fields_; }

const std::unordered_map<std::string_view, int32_t>& MappedFields::LazyNameToId() const {
  if (name_to_id_.empty() && !fields_.empty()) {
    for (const auto& field : fields_) {
      for (const auto& name : field.names) {
        if (field.field_id.has_value()) {
          name_to_id_.emplace(name, field.field_id.value());
        }
      }
    }
  }
  return name_to_id_;
}

const std::unordered_map<int32_t, MappedFieldConstRef>& MappedFields::LazyIdToField()
    const {
  if (id_to_field_.empty() && !fields_.empty()) {
    for (const auto& field : fields_) {
      if (field.field_id.has_value()) {
        id_to_field_.emplace(field.field_id.value(), std::cref(field));
      }
    }
  }
  return id_to_field_;
}

NameMapping::NameMapping(std::unique_ptr<MappedFields> mapping)
    : mapping_(std::move(mapping)) {}

std::optional<MappedFieldConstRef> NameMapping::Find(int32_t id) {
  const auto& fields_by_id = LazyFieldsById();
  if (auto iter = fields_by_id.find(id); iter != fields_by_id.cend()) {
    return iter->second;
  }
  return std::nullopt;
}

std::optional<MappedFieldConstRef> NameMapping::Find(std::span<const std::string> names) {
  if (names.empty()) {
    return std::nullopt;
  }
  return Find(JoinByDot(names));
}

std::optional<MappedFieldConstRef> NameMapping::Find(const std::string& name) {
  const auto& fields_by_name = LazyFieldsByName();
  if (auto iter = fields_by_name.find(name); iter != fields_by_name.cend()) {
    return iter->second;
  }
  return std::nullopt;
}

const MappedFields& NameMapping::AsMappedFields() const {
  if (mapping_ == nullptr) {
    const static std::unique_ptr<MappedFields> kEmptyFields = MappedFields::Make({});
    return *kEmptyFields;
  }
  return *mapping_;
}

const std::unordered_map<int32_t, MappedFieldConstRef>& NameMapping::LazyFieldsById()
    const {
  if (fields_by_id_.empty()) {
    IndexByIdVisitor visitor;
    visitor.Visit(AsMappedFields());
    fields_by_id_ = std::move(visitor.field_by_id);
  }
  return fields_by_id_;
}

const std::unordered_map<std::string, MappedFieldConstRef>&
NameMapping::LazyFieldsByName() const {
  if (fields_by_name_.empty()) {
    IndexByNameVisitor visitor;
    visitor.Visit(AsMappedFields());
    fields_by_name_ = std::move(visitor.field_by_name);
  }
  return fields_by_name_;
}

std::unique_ptr<NameMapping> NameMapping::MakeEmpty() {
  return std::unique_ptr<NameMapping>(new NameMapping(MappedFields::Make({})));
}

std::unique_ptr<NameMapping> NameMapping::Make(std::unique_ptr<MappedFields> fields) {
  return std::unique_ptr<NameMapping>(new NameMapping(std::move(fields)));
}

std::unique_ptr<NameMapping> NameMapping::Make(std::vector<MappedField> fields) {
  return Make(MappedFields::Make(std::move(fields)));
}

bool operator==(const MappedField& lhs, const MappedField& rhs) {
  if (lhs.field_id != rhs.field_id) {
    return false;
  }
  if (lhs.names != rhs.names) {
    return false;
  }
  if (lhs.nested_mapping == nullptr && rhs.nested_mapping == nullptr) {
    return true;
  }
  if (lhs.nested_mapping == nullptr || rhs.nested_mapping == nullptr) {
    return false;
  }
  return *lhs.nested_mapping == *rhs.nested_mapping;
}

bool operator==(const MappedFields& lhs, const MappedFields& rhs) {
  if (lhs.Size() != rhs.Size()) {
    return false;
  }
  auto lhs_fields = lhs.fields();
  auto rhs_fields = rhs.fields();
  for (size_t i = 0; i < lhs.Size(); ++i) {
    if (lhs_fields[i] != rhs_fields[i]) {
      return false;
    }
  }
  return true;
}

bool operator==(const NameMapping& lhs, const NameMapping& rhs) {
  return lhs.AsMappedFields() == rhs.AsMappedFields();
}

std::string ToString(const MappedField& field) {
  return std::format(
      "({} -> {}{})", field.names,
      field.field_id.has_value() ? std::to_string(field.field_id.value()) : "null",
      field.nested_mapping ? std::format(", {}", ToString(*field.nested_mapping)) : "");
}

std::string ToString(const MappedFields& fields) {
  return std::format("{}", fields.fields());
}

std::string ToString(const NameMapping& name_mapping) {
  const auto& fields = name_mapping.AsMappedFields();
  if (fields.Size() == 0) {
    return "[]";
  }
  std::string repr = "[\n";
  for (const auto& field : fields.fields()) {
    std::format_to(std::back_inserter(repr), "  {}\n", ToString(field));
  }
  repr += "]";
  return repr;
}

}  // namespace iceberg
