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
#include <stack>

#include "iceberg/result.h"
#include "iceberg/row/struct_like.h"
#include "iceberg/schema_internal.h"
#include "iceberg/table_metadata.h"
#include "iceberg/type.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep
#include "iceberg/util/macros.h"
#include "iceberg/util/type_util.h"
#include "iceberg/util/visit_type.h"

namespace iceberg {

Schema::Schema(std::vector<SchemaField> fields, int32_t schema_id)
    : StructType(std::move(fields)), schema_id_(schema_id) {}

Result<std::unique_ptr<Schema>> Schema::Make(std::vector<SchemaField> fields,
                                             int32_t schema_id,
                                             std::vector<int32_t> identifier_field_ids) {
  auto schema = std::make_unique<Schema>(std::move(fields), schema_id);

  if (!identifier_field_ids.empty()) {
    auto id_to_parent = IndexParents(*schema);
    for (auto field_id : identifier_field_ids) {
      ICEBERG_RETURN_UNEXPECTED(
          ValidateIdentifierFields(field_id, *schema, id_to_parent));
    }
  }

  schema->identifier_field_ids_ = std::move(identifier_field_ids);
  return schema;
}

Result<std::unique_ptr<Schema>> Schema::Make(
    std::vector<SchemaField> fields, int32_t schema_id,
    const std::vector<std::string>& identifier_field_names) {
  auto schema = std::make_unique<Schema>(std::move(fields), schema_id);

  std::vector<int32_t> fresh_identifier_ids;
  for (const auto& name : identifier_field_names) {
    ICEBERG_ASSIGN_OR_RAISE(auto field, schema->FindFieldByName(name));
    if (!field) {
      return InvalidSchema("Cannot find identifier field: {}", name);
    }
    fresh_identifier_ids.push_back(field.value().get().field_id());
  }

  if (!fresh_identifier_ids.empty()) {
    auto id_to_parent = IndexParents(*schema);
    for (auto field_id : fresh_identifier_ids) {
      ICEBERG_RETURN_UNEXPECTED(
          ValidateIdentifierFields(field_id, *schema, id_to_parent));
    }
  }

  schema->identifier_field_ids_ = std::move(fresh_identifier_ids);
  return schema;
}

Status Schema::ValidateIdentifierFields(
    int32_t field_id, const Schema& schema,
    const std::unordered_map<int32_t, int32_t>& id_to_parent) {
  ICEBERG_ASSIGN_OR_RAISE(auto field_opt, schema.FindFieldById(field_id));
  ICEBERG_PRECHECK(field_opt.has_value(),
                   "Cannot add field {} as an identifier field: field does not exist",
                   field_id);

  const SchemaField& field = field_opt.value().get();
  ICEBERG_PRECHECK(
      field.type()->is_primitive(),
      "Cannot add field {} as an identifier field: not a primitive type field", field_id);
  ICEBERG_PRECHECK(!field.optional(),
                   "Cannot add field {} as an identifier field: not a required field",
                   field_id);
  ICEBERG_PRECHECK(
      field.type()->type_id() != TypeId::kDouble &&
          field.type()->type_id() != TypeId::kFloat,
      "Cannot add field {} as an identifier field: must not be float or double field",
      field_id);

  // check whether the nested field is in a chain of required struct fields
  // exploring from root for better error message for list and map types
  std::stack<int32_t> ancestors;
  auto parent_it = id_to_parent.find(field.field_id());
  while (parent_it != id_to_parent.end()) {
    ancestors.push(parent_it->second);
    parent_it = id_to_parent.find(parent_it->second);
  }

  while (!ancestors.empty()) {
    ICEBERG_ASSIGN_OR_RAISE(auto parent_opt, schema.FindFieldById(ancestors.top()));
    ICEBERG_PRECHECK(
        parent_opt.has_value(),
        "Cannot add field {} as an identifier field: parent field id {} does not exist",
        field_id, ancestors.top());
    const SchemaField& parent = parent_opt.value().get();
    ICEBERG_PRECHECK(
        parent.type()->type_id() == TypeId::kStruct,
        "Cannot add field {} as an identifier field: must not be nested in {}", field_id,
        *parent.type());
    ICEBERG_PRECHECK(!parent.optional(),
                     "Cannot add field {} as an identifier field: must not be nested in "
                     "optional field {}",
                     field_id, parent.field_id());
    ancestors.pop();
  }
  return {};
}

const std::shared_ptr<Schema>& Schema::EmptySchema() {
  static const auto empty_schema =
      std::make_shared<Schema>(std::vector<SchemaField>{}, kInitialSchemaId);
  return empty_schema;
}

int32_t Schema::schema_id() const { return schema_id_; }

std::string Schema::ToString() const {
  std::string repr = "schema<";
  for (const auto& field : fields_) {
    std::format_to(std::back_inserter(repr), "  {}\n", field);
  }
  repr += ">";
  return repr;
}

bool Schema::Equals(const Schema& other) const {
  return schema_id_ == other.schema_id_ && fields_ == other.fields_ &&
         identifier_field_ids_ == other.identifier_field_ids_;
}

Result<std::optional<std::reference_wrapper<const SchemaField>>> Schema::FindFieldByName(
    std::string_view name, bool case_sensitive) const {
  if (case_sensitive) {
    ICEBERG_ASSIGN_OR_RAISE(auto name_id_map, name_id_map_.Get(*this));
    auto it = name_id_map.get().name_to_id.find(name);
    if (it == name_id_map.get().name_to_id.end()) {
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

Result<Schema::NameIdMap> Schema::InitNameIdMap(const Schema& self) {
  NameIdMap name_id_map;
  NameToIdVisitor visitor(name_id_map.name_to_id, &name_id_map.id_to_name,
                          /*case_sensitive=*/true);
  ICEBERG_RETURN_UNEXPECTED(
      VisitTypeInline(self, &visitor, /*path=*/"", /*short_path=*/""));
  visitor.Finish();
  return name_id_map;
}

Result<std::unordered_map<std::string, int32_t, StringHash, std::equal_to<>>>
Schema::InitLowerCaseNameToIdMap(const Schema& self) {
  std::unordered_map<std::string, int32_t, StringHash, std::equal_to<>>
      lowercase_name_to_id;
  NameToIdVisitor visitor(lowercase_name_to_id, /*id_to_name=*/nullptr,
                          /*case_sensitive=*/false);
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

Result<std::optional<std::string_view>> Schema::FindColumnNameById(
    int32_t field_id) const {
  ICEBERG_ASSIGN_OR_RAISE(auto name_id_map, name_id_map_.Get(*this));
  auto it = name_id_map.get().id_to_name.find(field_id);
  if (it == name_id_map.get().id_to_name.end()) {
    return std::nullopt;
  }
  return it->second;
}

Result<std::unordered_map<int32_t, std::vector<size_t>>> Schema::InitIdToPositionPath(
    const Schema& self) {
  PositionPathVisitor visitor;
  ICEBERG_RETURN_UNEXPECTED(VisitTypeInline(self, &visitor));
  return visitor.Finish();
}

Result<int32_t> Schema::InitHighestFieldId(const Schema& self) {
  ICEBERG_ASSIGN_OR_RAISE(auto id_to_field, self.id_to_field_.Get(self));

  if (id_to_field.get().empty()) {
    return kInitialColumnId;
  }

  auto max_it = std::ranges::max_element(
      id_to_field.get(),
      [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });

  return max_it->first;
}

Result<std::unique_ptr<StructLikeAccessor>> Schema::GetAccessorById(
    int32_t field_id) const {
  ICEBERG_ASSIGN_OR_RAISE(auto id_to_position_path, id_to_position_path_.Get(*this));
  if (auto it = id_to_position_path.get().find(field_id);
      it != id_to_position_path.get().cend()) {
    ICEBERG_ASSIGN_OR_RAISE(auto field, FindFieldById(field_id));
    if (!field.has_value()) {
      return NotFound("Cannot get accessor for field id: {}", field_id);
    }
    return std::make_unique<StructLikeAccessor>(field.value().get().type(), it->second);
  }
  return NotFound("Cannot get accessor for field id: {}", field_id);
}

Result<std::unique_ptr<Schema>> Schema::Select(std::span<const std::string> names,
                                               bool case_sensitive) const {
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
    return std::make_unique<Schema>(std::vector<SchemaField>{}, kInitialSchemaId);
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
    return std::make_unique<Schema>(std::vector<SchemaField>{}, kInitialSchemaId);
  }

  if (project_type->type_id() != TypeId::kStruct) {
    return InvalidSchema("Projected type must be a struct type");
  }

  return FromStructType(std::move(internal::checked_cast<StructType&>(*project_type)),
                        std::nullopt);
}

const std::vector<int32_t>& Schema::IdentifierFieldIds() const {
  return identifier_field_ids_;
}

Result<std::vector<std::string>> Schema::IdentifierFieldNames() const {
  std::vector<std::string> names;
  names.reserve(identifier_field_ids_.size());
  for (auto id : identifier_field_ids_) {
    ICEBERG_ASSIGN_OR_RAISE(auto name, FindColumnNameById(id));
    if (!name.has_value()) {
      return InvalidSchema("Cannot find identifier field id: {}", id);
    }
    names.emplace_back(name.value());
  }
  return names;
}

Result<int32_t> Schema::HighestFieldId() const { return highest_field_id_.Get(*this); }

bool Schema::SameSchema(const Schema& other) const {
  return fields_ == other.fields_ && identifier_field_ids_ == other.identifier_field_ids_;
}

Status Schema::Validate(int32_t format_version) const {
  // Get all fields including nested ones
  ICEBERG_ASSIGN_OR_RAISE(auto id_to_field, id_to_field_.Get(*this));

  // Check each field's type and defaults
  for (const auto& [field_id, field_ref] : id_to_field.get()) {
    const auto& field = field_ref.get();

    // Check if the field's type requires a minimum format version
    if (auto it = TableMetadata::kMinFormatVersions.find(field.type()->type_id());
        it != TableMetadata::kMinFormatVersions.end()) {
      if (int32_t min_format_version = it->second; format_version < min_format_version) {
        return InvalidSchema("Invalid type for {}: {} is not supported until v{}",
                             field.name(), *field.type(), min_format_version);
      }
    }

    // TODO(GuoTao.yu): Check default values when they are supported
  }

  return {};
}

}  // namespace iceberg
