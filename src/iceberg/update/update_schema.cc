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

#include "iceberg/update/update_schema.h"

#include <format>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/table_metadata.h"
#include "iceberg/transaction.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/error_collector.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/type_util.h"
#include "iceberg/util/visit_type.h"

namespace iceberg {

namespace {
constexpr int32_t kTableRootId = -1;

/// \brief Visitor for applying schema changes recursively to nested types
class ApplyChangesVisitor {
 public:
  ApplyChangesVisitor(
      const std::unordered_set<int32_t>& deletes,
      const std::unordered_map<int32_t, std::shared_ptr<SchemaField>>& updates,
      const std::unordered_map<int32_t, std::vector<int32_t>>& parent_to_added_ids)
      : deletes_(deletes), updates_(updates), parent_to_added_ids_(parent_to_added_ids) {}

  /// \brief Apply changes to a type using schema visitor pattern
  Result<std::shared_ptr<Type>> ApplyChanges(const std::shared_ptr<Type>& type,
                                             int32_t parent_id) {
    return VisitTypeCategory(*type, this, type, parent_id);
  }

  /// \brief Apply changes to a struct type
  Result<std::shared_ptr<Type>> VisitStruct(const StructType& struct_type,
                                            const std::shared_ptr<Type>& base_type,
                                            int32_t parent_id) {
    std::vector<SchemaField> new_fields;
    bool has_changes = false;

    // Process existing fields
    for (const auto& field : struct_type.fields()) {
      // Recursively process the field's type first
      ICEBERG_ASSIGN_OR_RAISE(auto field_type_result,
                              ApplyChanges(field.type(), field.field_id()));

      // Process field-level changes (deletes, updates, nested additions)
      ICEBERG_ASSIGN_OR_RAISE(auto processed_field,
                              ProcessField(field, field_type_result));

      if (processed_field.has_value()) {
        const auto& new_field = processed_field.value();
        new_fields.push_back(new_field);

        // Check if this field changed
        if (new_field != field) {
          has_changes = true;
        }
      } else {
        // Field was deleted
        has_changes = true;
      }
    }

    // Add new fields for this struct
    auto adds_it = parent_to_added_ids_.find(parent_id);
    if (adds_it != parent_to_added_ids_.end() && !adds_it->second.empty()) {
      has_changes = true;
      for (int32_t added_id : adds_it->second) {
        auto added_field_it = updates_.find(added_id);
        if (added_field_it != updates_.end()) {
          new_fields.push_back(*added_field_it->second);
        }
      }
    }

    // Return original type if nothing changed
    if (!has_changes) {
      return base_type;
    }

    return std::make_shared<StructType>(std::move(new_fields));
  }

  /// \brief Apply changes to a list type
  Result<std::shared_ptr<Type>> VisitList(const ListType& list_type,
                                          const std::shared_ptr<Type>& base_type,
                                          int32_t parent_id) {
    const auto& element = list_type.element();

    // Recursively process element type
    ICEBERG_ASSIGN_OR_RAISE(auto element_type_result,
                            ApplyChanges(element.type(), element.field_id()));

    // Process element field (handles deletes, updates, nested additions)
    ICEBERG_ASSIGN_OR_RAISE(auto processed_element,
                            ProcessField(element, element_type_result));

    ICEBERG_CHECK(processed_element.has_value(),
                  "Cannot delete element field from list: {}", list_type.ToString());

    const auto& new_element = processed_element.value();

    // Return unchanged if element didn't change
    if (element == new_element) {
      return base_type;
    }

    return std::make_shared<ListType>(new_element);
  }

  /// \brief Apply changes to a map type
  Result<std::shared_ptr<Type>> VisitMap(const MapType& map_type,
                                         const std::shared_ptr<Type>& base_type,
                                         int32_t parent_id) {
    const auto& key = map_type.key();
    const auto& value = map_type.value();

    // Check for key modifications (not allowed in Iceberg)
    int32_t key_id = key.field_id();
    ICEBERG_CHECK(!deletes_.contains(key_id), "Cannot delete map keys");
    ICEBERG_CHECK(!updates_.contains(key_id), "Cannot update map keys");
    ICEBERG_CHECK(!parent_to_added_ids_.contains(key_id),
                  "Cannot add fields to map keys");

    // Recursively process key and value types
    ICEBERG_ASSIGN_OR_RAISE(auto key_type_result, ApplyChanges(key.type(), key_id));
    ICEBERG_ASSIGN_OR_RAISE(auto value_type_result,
                            ApplyChanges(value.type(), value.field_id()));

    // Key type must not change
    ICEBERG_CHECK(*key_type_result == *key.type(), "Cannot alter map keys");

    // Process value field (handles deletes, updates, nested additions)
    ICEBERG_ASSIGN_OR_RAISE(auto processed_value, ProcessField(value, value_type_result));

    ICEBERG_CHECK(processed_value.has_value(), "Cannot delete value field from map: {}",
                  map_type.ToString());

    const auto& new_value = processed_value.value();

    // Return unchanged if nothing changed
    if (key == map_type.key() && value == new_value) {
      return base_type;
    }

    return std::make_shared<MapType>(key, new_value);
  }

  /// \brief Handle primitive types - return unchanged
  Result<std::shared_ptr<Type>> VisitPrimitive(const PrimitiveType& primitive_type,
                                               const std::shared_ptr<Type>& base_type,
                                               int32_t parent_id) {
    // Primitive types are returned as-is
    return base_type;
  }

 private:
  /// \brief Process a field: handle deletes, updates, and nested additions
  ///
  /// It processes field-level operations after the field's type has been recursively
  /// processed.
  Result<std::optional<SchemaField>> ProcessField(
      const SchemaField& field, const std::shared_ptr<Type>& field_type_result) {
    int32_t field_id = field.field_id();

    // 1. Handle deletes
    if (deletes_.contains(field_id)) {
      // Field is deleted
      return std::nullopt;
    }

    // 2. Start with the recursively processed type
    std::shared_ptr<Type> result_type = field_type_result;

    // 3. Handle type updates (e.g., type widening)
    // Note: We check the update against the ORIGINAL field type, not the recursively
    // processed type, because we want to preserve nested changes from recursion
    auto update_it = updates_.find(field_id);
    if (update_it != updates_.end()) {
      const auto& update_field = update_it->second;
      // If the update specifies a type change, use the new type
      // Otherwise keep the recursively processed type
      if (update_field->type() != field.type()) {
        result_type = update_field->type();
      }
    }

    // Note: Nested field additions are handled in VisitStruct, not here
    // to avoid duplication

    // 4. Build the result field
    if (update_it != updates_.end()) {
      // Use update field metadata but with the processed type
      const auto& update_field = update_it->second;
      return SchemaField(field_id, update_field->name(), std::move(result_type),
                         update_field->optional(), update_field->doc());
    } else if (result_type != field.type()) {
      // Type changed but no field-level update
      return SchemaField(field_id, field.name(), std::move(result_type), field.optional(),
                         field.doc());
    } else {
      // No changes
      return field;
    }
  }

  const std::unordered_set<int32_t>& deletes_;
  const std::unordered_map<int32_t, std::shared_ptr<SchemaField>>& updates_;
  const std::unordered_map<int32_t, std::vector<int32_t>>& parent_to_added_ids_;
};

}  // namespace

Result<std::shared_ptr<UpdateSchema>> UpdateSchema::Make(
    std::shared_ptr<Transaction> transaction) {
  ICEBERG_PRECHECK(transaction != nullptr,
                   "Cannot create UpdateSchema without transaction");
  return std::shared_ptr<UpdateSchema>(new UpdateSchema(std::move(transaction)));
}

UpdateSchema::UpdateSchema(std::shared_ptr<Transaction> transaction)
    : PendingUpdate(std::move(transaction)) {
  // Get the current schema
  auto schema_result = base().Schema();
  if (!schema_result.has_value()) {
    AddError(schema_result.error());
    return;
  }
  schema_ = std::move(schema_result.value());

  // Initialize last_column_id from base metadata
  last_column_id_ = base().last_column_id;

  // Initialize identifier field names from the current schema
  auto identifier_names_result = schema_->IdentifierFieldNames();
  if (!identifier_names_result.has_value()) {
    AddError(identifier_names_result.error());
    return;
  }
  identifier_field_names_ = std::move(identifier_names_result.value());

  // Initialize id_to_parent map from the schema
  id_to_parent_ = IndexParents(*schema_);
}

UpdateSchema::~UpdateSchema() = default;

UpdateSchema& UpdateSchema::AllowIncompatibleChanges() {
  allow_incompatible_changes_ = true;
  return *this;
}

UpdateSchema& UpdateSchema::CaseSensitive(bool case_sensitive) {
  case_sensitive_ = case_sensitive;
  return *this;
}

UpdateSchema& UpdateSchema::AddColumn(std::string_view name, std::shared_ptr<Type> type,
                                      std::string_view doc) {
  // Check for "." in top-level name
  ICEBERG_BUILDER_CHECK(!name.contains('.'),
                        "Cannot add column with ambiguous name: {}, use "
                        "AddColumn(parent, name, type, doc)",
                        name);
  return AddColumnInternal(std::nullopt, name, /*is_optional=*/true, std::move(type),
                           doc);
}

UpdateSchema& UpdateSchema::AddColumn(std::optional<std::string_view> parent,
                                      std::string_view name, std::shared_ptr<Type> type,
                                      std::string_view doc) {
  return AddColumnInternal(std::move(parent), name, /*is_optional=*/true, std::move(type),
                           doc);
}

UpdateSchema& UpdateSchema::AddRequiredColumn(std::string_view name,
                                              std::shared_ptr<Type> type,
                                              std::string_view doc) {
  // Check for "." in top-level name
  ICEBERG_BUILDER_CHECK(!name.contains('.'),
                        "Cannot add column with ambiguous name: {}, use "
                        "AddRequiredColumn(parent, name, type, doc)",
                        name);
  return AddColumnInternal(std::nullopt, name, /*is_optional=*/false, std::move(type),
                           doc);
}

UpdateSchema& UpdateSchema::AddRequiredColumn(std::optional<std::string_view> parent,
                                              std::string_view name,
                                              std::shared_ptr<Type> type,
                                              std::string_view doc) {
  return AddColumnInternal(std::move(parent), name, /*is_optional=*/false,
                           std::move(type), doc);
}

UpdateSchema& UpdateSchema::UpdateColumn(std::string_view name,
                                         std::shared_ptr<PrimitiveType> new_type) {
  // TODO(Guotao Yu): Implement UpdateColumn
  AddError(NotImplemented("UpdateSchema::UpdateColumn not implemented"));
  return *this;
}

UpdateSchema& UpdateSchema::UpdateColumnDoc(std::string_view name,
                                            std::string_view new_doc) {
  // TODO(Guotao Yu): Implement UpdateColumnDoc
  AddError(NotImplemented("UpdateSchema::UpdateColumnDoc not implemented"));
  return *this;
}

UpdateSchema& UpdateSchema::RenameColumn(std::string_view name,
                                         std::string_view new_name) {
  // TODO(Guotao Yu): Implement RenameColumn
  AddError(NotImplemented("UpdateSchema::RenameColumn not implemented"));
  return *this;
}

UpdateSchema& UpdateSchema::MakeColumnOptional(std::string_view name) {
  // TODO(Guotao Yu): Implement MakeColumnOptional
  AddError(NotImplemented("UpdateSchema::MakeColumnOptional not implemented"));
  return *this;
}

UpdateSchema& UpdateSchema::RequireColumn(std::string_view name) {
  // TODO(Guotao Yu): Implement RequireColumn
  AddError(NotImplemented("UpdateSchema::RequireColumn not implemented"));
  return *this;
}

UpdateSchema& UpdateSchema::DeleteColumn(std::string_view name) {
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto field_opt, FindField(name));
  ICEBERG_BUILDER_CHECK(field_opt.has_value(), "Cannot delete missing column: {}", name);

  const auto& field = field_opt->get();
  int32_t field_id = field.field_id();

  ICEBERG_BUILDER_CHECK(!parent_to_added_ids_.contains(field_id),
                        "Cannot delete a column that has additions: {}", name);
  ICEBERG_BUILDER_CHECK(!updates_.contains(field_id),
                        "Cannot delete a column that has updates: {}", name);

  // Add to deletes set
  deletes_.insert(field_id);

  return *this;
}

UpdateSchema& UpdateSchema::MoveFirst(std::string_view name) {
  // TODO(Guotao Yu): Implement MoveFirst
  AddError(NotImplemented("UpdateSchema::MoveFirst not implemented"));
  return *this;
}

UpdateSchema& UpdateSchema::MoveBefore(std::string_view name,
                                       std::string_view before_name) {
  // TODO(Guotao Yu): Implement MoveBefore
  AddError(NotImplemented("UpdateSchema::MoveBefore not implemented"));
  return *this;
}

UpdateSchema& UpdateSchema::MoveAfter(std::string_view name,
                                      std::string_view after_name) {
  // TODO(Guotao Yu): Implement MoveAfter
  AddError(NotImplemented("UpdateSchema::MoveAfter not implemented"));
  return *this;
}

UpdateSchema& UpdateSchema::UnionByNameWith(std::shared_ptr<Schema> new_schema) {
  // TODO(Guotao Yu): Implement UnionByNameWith
  AddError(NotImplemented("UpdateSchema::UnionByNameWith not implemented"));
  return *this;
}

UpdateSchema& UpdateSchema::SetIdentifierFields(
    const std::span<std::string_view>& names) {
  identifier_field_names_ = names | std::ranges::to<std::vector<std::string>>();
  return *this;
}

Result<UpdateSchema::ApplyResult> UpdateSchema::Apply() {
  ICEBERG_RETURN_UNEXPECTED(CheckErrors());

  // Validate existing identifier fields are not deleted
  for (const auto& name : identifier_field_names_) {
    ICEBERG_ASSIGN_OR_RAISE(auto field_opt, FindField(name));
    if (field_opt.has_value()) {
      const auto& field = field_opt->get();
      auto field_id = field.field_id();

      ICEBERG_CHECK(!deletes_.contains(field_id),
                    "Cannot delete identifier field {}. To force deletion, also call "
                    "SetIdentifierFields to update identifier fields.",
                    name);

      // Check no parent of this field is deleted
      auto parent_it = id_to_parent_.find(field_id);
      while (parent_it != id_to_parent_.end()) {
        int32_t parent_id = parent_it->second;
        ICEBERG_CHECK(
            !deletes_.contains(parent_id),
            "Cannot delete field with id {} as it will delete nested identifier field {}",
            parent_id, name);
        parent_it = id_to_parent_.find(parent_id);
      }
    }
  }

  // Apply changes recursively using the visitor
  ApplyChangesVisitor visitor(deletes_, updates_, parent_to_added_ids_);
  ICEBERG_ASSIGN_OR_RAISE(auto new_type, visitor.ApplyChanges(schema_, kTableRootId));

  // Cast result back to StructType and extract fields
  auto new_struct_type = internal::checked_pointer_cast<StructType>(new_type);

  // Convert identifier field names to IDs
  auto temp_schema = new_struct_type->ToSchema();
  std::vector<int32_t> fresh_identifier_ids;
  for (const auto& name : identifier_field_names_) {
    ICEBERG_ASSIGN_OR_RAISE(auto field_opt,
                            temp_schema->FindFieldByName(name, case_sensitive_));
    ICEBERG_CHECK(field_opt.has_value(),
                  "Cannot add field {} as an identifier field: not found in current "
                  "schema or added columns",
                  name);
    fresh_identifier_ids.push_back(field_opt->get().field_id());
  }

  // Create the new schema
  auto new_fields = temp_schema->fields() | std::ranges::to<std::vector<SchemaField>>();
  ICEBERG_ASSIGN_OR_RAISE(
      auto new_schema,
      Schema::Make(std::move(new_fields), schema_->schema_id(), fresh_identifier_ids));

  return ApplyResult{.schema = std::move(new_schema),
                     .new_last_column_id = last_column_id_};
}

// TODO(Guotao Yu): v3 default value is not yet supported
UpdateSchema& UpdateSchema::AddColumnInternal(std::optional<std::string_view> parent,
                                              std::string_view name, bool is_optional,
                                              std::shared_ptr<Type> type,
                                              std::string_view doc) {
  int32_t parent_id = kTableRootId;
  std::string full_name;

  // Handle parent field
  if (parent.has_value()) {
    ICEBERG_BUILDER_CHECK(!parent->empty(), "Parent name cannot be empty");
    // Find parent field
    ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto parent_field_opt, FindField(*parent));
    ICEBERG_BUILDER_CHECK(parent_field_opt.has_value(), "Cannot find parent struct: {}",
                          *parent);

    const SchemaField& parent_field = parent_field_opt->get();
    const auto& parent_type = parent_field.type();

    // Get the actual field to add to (handle map/list)
    const SchemaField* target_field = &parent_field;

    if (parent_type->type_id() == TypeId::kMap) {
      // For maps, add to value field
      const auto& map_type = internal::checked_cast<const MapType&>(*parent_type);
      target_field = &map_type.value();
    } else if (parent_type->type_id() == TypeId::kList) {
      // For lists, add to element field
      const auto& list_type = internal::checked_cast<const ListType&>(*parent_type);
      target_field = &list_type.element();
    }

    // Validate target is a struct
    ICEBERG_BUILDER_CHECK(target_field->type()->type_id() == TypeId::kStruct,
                          "Cannot add to non-struct column: {}: {}", *parent,
                          target_field->type()->ToString());

    parent_id = target_field->field_id();

    // Check parent is not being deleted
    ICEBERG_BUILDER_CHECK(!deletes_.contains(parent_id),
                          "Cannot add to a column that will be deleted: {}", *parent);

    // Check field doesn't already exist (unless it's being deleted)
    std::string nested_name = std::format("{}.{}", *parent, name);
    ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto current_field, FindField(nested_name));
    ICEBERG_BUILDER_CHECK(
        !current_field.has_value() || deletes_.contains(current_field->get().field_id()),
        "Cannot add column, name already exists: {}.{}", *parent, name);

    // Build full name using canonical name of parent
    ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto parent_name_opt,
                                     schema_->FindColumnNameById(parent_id));
    ICEBERG_BUILDER_CHECK(parent_name_opt.has_value(),
                          "Cannot find column name for parent id: {}", parent_id);

    full_name = std::format("{}.{}", *parent_name_opt, name);
  } else {
    // Top-level field
    ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto current_field, FindField(name));
    ICEBERG_BUILDER_CHECK(
        !current_field.has_value() || deletes_.contains(current_field->get().field_id()),
        "Cannot add column, name already exists: {}", name);

    full_name = std::string(name);
  }

  // V3 supports default values, but this implementation doesn't support them yet
  // Check for incompatible change: adding required column without default
  ICEBERG_BUILDER_CHECK(
      is_optional || allow_incompatible_changes_,
      "Incompatible change: cannot add required column without a default value: {}",
      full_name);

  // Assign new column ID
  int32_t new_id = AssignNewColumnId();

  // Update tracking for moves
  added_name_to_id_[full_name] = new_id;
  if (parent_id != kTableRootId) {
    id_to_parent_[new_id] = parent_id;
  }

  // Assign fresh IDs to nested types
  AssignFreshIdVisitor id_assigner([this]() { return AssignNewColumnId(); });
  auto type_with_fresh_ids = id_assigner.Visit(type);

  // Create new field
  auto new_field = std::make_shared<SchemaField>(new_id, std::string(name),
                                                 std::move(type_with_fresh_ids),
                                                 is_optional, std::string(doc));

  // Record the update
  updates_[new_id] = std::move(new_field);
  parent_to_added_ids_[parent_id].push_back(new_id);

  return *this;
}

int32_t UpdateSchema::AssignNewColumnId() { return ++last_column_id_; }

Result<std::optional<std::reference_wrapper<const SchemaField>>> UpdateSchema::FindField(
    std::string_view name) const {
  return schema_->FindFieldByName(name, case_sensitive_);
}

}  // namespace iceberg
