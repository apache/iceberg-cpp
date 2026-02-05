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
#include "iceberg/util/macros.h"
#include "iceberg/util/visit_type.h"

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

std::optional<MappedFieldConstRef> NameMapping::Find(int32_t id) const {
  const auto& fields_by_id = LazyFieldsById();
  if (auto iter = fields_by_id.find(id); iter != fields_by_id.cend()) {
    return iter->second;
  }
  return std::nullopt;
}

std::optional<MappedFieldConstRef> NameMapping::Find(
    std::span<const std::string> names) const {
  if (names.empty()) {
    return std::nullopt;
  }
  return Find(JoinByDot(names));
}

std::optional<MappedFieldConstRef> NameMapping::Find(const std::string& name) const {
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

namespace {

// Visitor class for creating name mappings from schema types
class CreateMappingVisitor {
 public:
  Result<std::unique_ptr<MappedFields>> Visit(const StructType& type) const {
    std::vector<MappedField> fields;
    fields.reserve(type.fields().size());
    for (const auto& field : type.fields()) {
      ICEBERG_RETURN_UNEXPECTED(AddMappedField(fields, std::string(field.name()), field));
    }
    return MappedFields::Make(std::move(fields));
  }

  Result<std::unique_ptr<MappedFields>> Visit(const ListType& type) const {
    std::vector<MappedField> fields;
    ICEBERG_RETURN_UNEXPECTED(AddMappedField(fields, "element", type.fields().back()));
    return MappedFields::Make(std::move(fields));
  }

  Result<std::unique_ptr<MappedFields>> Visit(const MapType& type) const {
    std::vector<MappedField> fields;
    fields.reserve(2);
    ICEBERG_RETURN_UNEXPECTED(AddMappedField(fields, "key", type.key()));
    ICEBERG_RETURN_UNEXPECTED(AddMappedField(fields, "value", type.value()));
    return MappedFields::Make(std::move(fields));
  }

  template <typename T>
  Result<std::unique_ptr<MappedFields>> Visit(const T& type) const {
    return nullptr;
  }

 private:
  Status AddMappedField(std::vector<MappedField>& fields, const std::string& name,
                        const SchemaField& field) const {
    auto visit_result =
        VisitType(*field.type(), [this](const auto& type) { return this->Visit(type); });
    ICEBERG_RETURN_UNEXPECTED(visit_result);

    fields.emplace_back(MappedField{
        .names = {name},
        .field_id = field.field_id(),
        .nested_mapping = std::move(visit_result.value()),
    });
    return {};
  }
};

// Visitor class for updating name mappings with schema changes
class UpdateMappingVisitor {
 public:
  UpdateMappingVisitor(const std::map<int32_t, SchemaField>& updates,
                       const std::multimap<int32_t, int32_t>& adds)
      : updates_(updates), adds_(adds) {}

  Result<std::unique_ptr<MappedFields>> VisitMapping(const NameMapping& mapping) {
    auto fields_result = VisitFields(mapping.AsMappedFields());
    ICEBERG_RETURN_UNEXPECTED(fields_result);
    return AddNewFields(std::move(*fields_result),
                        -1 /* parent ID for top-level fields */);
  }

 private:
  Result<std::unique_ptr<MappedFields>> VisitFields(const MappedFields& fields) {
    // Recursively visit all fields
    std::vector<MappedField> field_results;
    field_results.reserve(fields.Size());

    for (const auto& field : fields.fields()) {
      auto field_result = VisitField(field);
      ICEBERG_RETURN_UNEXPECTED(field_result);
      field_results.push_back(std::move(*field_result));
    }

    // Build update assignments map for removing reassigned names
    std::unordered_map<std::string, int32_t> update_assignments;
    std::ranges::for_each(field_results, [&](const auto& field) {
      if (field.field_id.has_value()) {
        auto update_it = updates_.find(field.field_id.value());
        if (update_it != updates_.end()) {
          update_assignments.emplace(std::string(update_it->second.name()),
                                     field.field_id.value());
        }
      }
    });

    // Remove reassigned names from all fields
    for (auto& field : field_results) {
      field = RemoveReassignedNames(field, update_assignments);
    }

    return MappedFields::Make(std::move(field_results));
  }

  Result<MappedField> VisitField(const MappedField& field) {
    // Update this field's names
    std::unordered_set<std::string> field_names = field.names;
    if (field.field_id.has_value()) {
      auto update_it = updates_.find(field.field_id.value());
      if (update_it != updates_.end()) {
        field_names.insert(std::string(update_it->second.name()));
      }
    }

    std::unique_ptr<MappedFields> nested_mapping = nullptr;
    if (field.nested_mapping != nullptr) {
      auto nested_result = VisitFields(*field.nested_mapping);
      ICEBERG_RETURN_UNEXPECTED(nested_result);
      nested_mapping = std::move(*nested_result);
    }

    // Add a new mapping for any new nested fields
    if (field.field_id.has_value()) {
      auto nested_result =
          AddNewFields(std::move(nested_mapping), field.field_id.value());
      ICEBERG_RETURN_UNEXPECTED(nested_result);
      nested_mapping = std::move(*nested_result);
    }

    return MappedField{
        .names = std::move(field_names),
        .field_id = field.field_id,
        .nested_mapping = std::move(nested_mapping),
    };
  }

  Result<std::unique_ptr<MappedFields>> AddNewFields(
      std::unique_ptr<MappedFields> mapping, int32_t parent_id) {
    auto range = adds_.equal_range(parent_id);
    std::vector<const SchemaField*> fields_to_add;
    for (auto it = range.first; it != range.second; ++it) {
      auto update_it = updates_.find(it->second);
      if (update_it != updates_.end()) {
        fields_to_add.push_back(&update_it->second);
      }
    }

    if (fields_to_add.empty()) {
      return std::move(mapping);
    }

    std::vector<MappedField> new_fields;
    CreateMappingVisitor create_visitor;
    for (const auto* field_to_add : fields_to_add) {
      auto nested_result = VisitType(
          *field_to_add->type(),
          [&create_visitor](const auto& type) { return create_visitor.Visit(type); });
      ICEBERG_RETURN_UNEXPECTED(nested_result);

      new_fields.emplace_back(MappedField{
          .names = {std::string(field_to_add->name())},
          .field_id = field_to_add->field_id(),
          .nested_mapping = std::move(*nested_result),
      });
    }

    if (mapping == nullptr || mapping->Size() == 0) {
      return MappedFields::Make(std::move(new_fields));
    }

    // Build assignments map for removing reassigned names
    std::unordered_map<std::string, int32_t> assignments;
    for (const auto* field_to_add : fields_to_add) {
      assignments.emplace(std::string(field_to_add->name()), field_to_add->field_id());
    }

    // create a copy of fields that can be updated (append new fields, replace existing
    // for reassignment)
    std::vector<MappedField> fields;
    fields.reserve(mapping->Size() + new_fields.size());
    for (const auto& field : mapping->fields()) {
      fields.push_back(RemoveReassignedNames(field, assignments));
    }

    fields.insert(fields.end(), std::make_move_iterator(new_fields.begin()),
                  std::make_move_iterator(new_fields.end()));

    return MappedFields::Make(std::move(fields));
  }

  static MappedField RemoveReassignedNames(
      const MappedField& field,
      const std::unordered_map<std::string, int32_t>& assignments) {
    std::unordered_set<std::string> updated_names = field.names;
    std::erase_if(updated_names, [&](const std::string& name) {
      auto assign_it = assignments.find(name);
      return assign_it != assignments.end() &&
             (!field.field_id.has_value() || assign_it->second != field.field_id.value());
    });
    return MappedField{
        .names = std::move(updated_names),
        .field_id = field.field_id,
        .nested_mapping = field.nested_mapping,
    };
  }

  const std::map<int32_t, SchemaField>& updates_;
  const std::multimap<int32_t, int32_t>& adds_;
};

}  // namespace

Result<std::unique_ptr<NameMapping>> CreateMapping(const Schema& schema) {
  CreateMappingVisitor visitor;
  auto result = VisitType(
      schema, [&visitor](const auto& type) -> Result<std::unique_ptr<MappedFields>> {
        return visitor.Visit(type);
      });
  ICEBERG_RETURN_UNEXPECTED(result);
  return NameMapping::Make(std::move(*result));
}

Result<std::unique_ptr<NameMapping>> UpdateMapping(
    const NameMapping& mapping, const std::map<int32_t, SchemaField>& updates,
    const std::multimap<int32_t, int32_t>& adds) {
  UpdateMappingVisitor visitor(updates, adds);
  auto result = visitor.VisitMapping(mapping);
  ICEBERG_RETURN_UNEXPECTED(result);
  return NameMapping::Make(std::move(*result));
}

}  // namespace iceberg
