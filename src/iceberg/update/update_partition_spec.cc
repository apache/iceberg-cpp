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

#include "iceberg/update/update_partition_spec.h"

#include <algorithm>
#include <format>
#include <memory>
#include <string_view>
#include <utility>

#include "iceberg/expression/term.h"
#include "iceberg/partition_field.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/table_metadata.h"
#include "iceberg/transaction.h"
#include "iceberg/transform.h"
#include "iceberg/util/macros.h"

namespace iceberg {

Result<std::shared_ptr<UpdatePartitionSpec>> UpdatePartitionSpec::Make(
    std::shared_ptr<Transaction> transaction) {
  ICEBERG_PRECHECK(transaction != nullptr,
                   "Cannot create UpdatePartitionSpec without transaction");
  return std::shared_ptr<UpdatePartitionSpec>(
      new UpdatePartitionSpec(std::move(transaction)));
}

UpdatePartitionSpec::UpdatePartitionSpec(std::shared_ptr<Transaction> transaction)
    : PendingUpdate(std::move(transaction)) {
  format_version_ = base().format_version;

  // Get the current/default partition spec
  auto spec_result = base().PartitionSpec();
  if (!spec_result.has_value()) {
    AddError(spec_result.error());
    return;
  }
  spec_ = std::move(spec_result.value());

  // Get the current schema
  auto schema_result = base().Schema();
  if (!schema_result.has_value()) {
    AddError(schema_result.error());
    return;
  }
  schema_ = std::move(schema_result.value());

  last_assigned_partition_id_ =
      std::max(base().last_partition_id, PartitionSpec::kLegacyPartitionDataIdStart - 1);
  name_to_field_ = IndexSpecByName(*spec_);
  transform_to_field_ = IndexSpecByTransform(*spec_);

  // Check for unknown transforms
  for (const auto& field : spec_->fields()) {
    if (field.transform()->transform_type() == TransformType::kUnknown) {
      AddError(ErrorKind::kInvalidArgument,
               "Cannot update partition spec with unknown transform: {}",
               field.ToString());
      return;
    }
  }

  // Build index of historical partition fields for efficient recycling (V2+)
  if (format_version_ >= 2) {
    BuildHistoricalFieldsIndex();
  }
}

UpdatePartitionSpec::~UpdatePartitionSpec() = default;

UpdatePartitionSpec& UpdatePartitionSpec::CaseSensitive(bool is_case_sensitive) {
  case_sensitive_ = is_case_sensitive;
  return *this;
}

UpdatePartitionSpec& UpdatePartitionSpec::AddNonDefaultSpec() {
  set_as_default_ = false;
  return *this;
}

UpdatePartitionSpec& UpdatePartitionSpec::AddField(std::string_view source_name) {
  // Find the source field in the schema
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(
      auto field_opt, schema_->FindFieldByName(source_name, case_sensitive_));

  ICEBERG_BUILDER_CHECK(field_opt.has_value(), "Cannot find source field: {}",
                        source_name);
  int32_t source_id = field_opt->get().field_id();
  return AddFieldInternal(source_name, source_id, Transform::Identity());
}

UpdatePartitionSpec& UpdatePartitionSpec::AddField(const std::shared_ptr<Term>& term,
                                                   std::string_view part_name) {
  ICEBERG_BUILDER_CHECK(term->is_unbound(), "Cannot add bound term to partition spec");
  // Bind the term to get the source id
  if (term->kind() == Term::Kind::kReference) {
    const auto& ref = dynamic_cast<const NamedReference&>(*term);
    ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto bound_ref, ref.Bind(*schema_, case_sensitive_));
    int32_t source_id = bound_ref->field().field_id();
    return AddFieldInternal(part_name, source_id, Transform::Identity());
  } else if (term->kind() == Term::Kind::kTransform) {
    const auto& unbound_transform = dynamic_cast<const UnboundTransform&>(*term);
    ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto bound_transform,
                                     unbound_transform.Bind(*schema_, case_sensitive_));
    int32_t source_id = bound_transform->reference()->field().field_id();
    return AddFieldInternal(part_name, source_id, bound_transform->transform());
  }

  return AddError(
      InvalidArgument("Cannot add {} term to partition spec", term->ToString()));
}

UpdatePartitionSpec& UpdatePartitionSpec::AddFieldInternal(
    std::string_view name, int32_t source_id,
    const std::shared_ptr<Transform>& transform) {
  // Check for duplicate name in added fields
  ICEBERG_BUILDER_CHECK(name.empty() || !added_field_names_.contains(name),
                        "Cannot add duplicate partition field: {}", name);

  // Cache transform string to avoid repeated ToString() calls
  const std::string transform_str = transform->ToString();
  TransformKey validation_key{source_id, transform_str};

  // Check if this field already exists in the current spec
  auto existing_it = transform_to_field_.find(validation_key);
  if (existing_it != transform_to_field_.end()) {
    const auto& existing = existing_it->second;
    const bool is_deleted = deletes_.contains(existing->field_id());
    if (is_deleted && *existing->transform() == *transform) {
      // If the field was deleted and we're re-adding the same one, just undo the delete
      return RewriteDeleteAndAddField(*existing, name);
    }

    ICEBERG_BUILDER_CHECK(
        is_deleted,
        "Cannot add duplicate partition field '{}' for source {} with transform {}, "
        "conflicts with {}",
        name, source_id, transform_str, existing->ToString());
  }

  // Check if already being added
  auto added_it = transform_to_added_field_.find(validation_key);
  ICEBERG_BUILDER_CHECK(
      added_it == transform_to_added_field_.end(),
      "Cannot add duplicate partition field '{}' for source {} with transform {}, "
      "already added: {}",
      name, source_id, transform_str, added_it->second);

  // Create or recycle the partition field
  PartitionField new_field = RecycleOrCreatePartitionField(source_id, transform, name);

  // Generate name if not provided
  std::string field_name;
  if (!name.empty()) {
    field_name = name;
  } else {
    ICEBERG_BUILDER_ASSIGN_OR_RETURN(field_name,
                                     GeneratePartitionName(source_id, transform));
  }

  // Create the final field with the name
  new_field = PartitionField(new_field.source_id(), new_field.field_id(), field_name,
                             new_field.transform());

  // Check for redundant time-based partitions
  CheckForRedundantAddedPartitions(new_field);
  transform_to_added_field_.emplace(validation_key, field_name);

  // Handle name conflicts with existing fields
  auto existing_name_it = name_to_field_.find(field_name);
  if (existing_name_it != name_to_field_.end()) {
    const auto& existing_field = existing_name_it->second;
    const bool existing_is_deleted = deletes_.contains(existing_field->field_id());
    std::string renamed =
        std::format("{}_{}", existing_field->name(), existing_field->field_id());
    if (!existing_is_deleted) {
      if (IsVoidTransform(*existing_field)) {
        // Rename the old deleted field
        RenameField(existing_field->name(), std::move(renamed));
      } else {
        return AddError(
            InvalidArgument("Cannot add duplicate partition field name: {}", field_name));
      }
    } else {
      // Field is being deleted, rename it to avoid conflict
      renames_.emplace(existing_field->name(), std::move(renamed));
    }
  }

  adds_.push_back(std::move(new_field));
  added_field_names_.emplace(field_name);

  return *this;
}

UpdatePartitionSpec& UpdatePartitionSpec::RewriteDeleteAndAddField(
    const PartitionField& existing, std::string_view name) {
  deletes_.erase(existing.field_id());
  if (name.empty() || std::string(existing.name()) == name) {
    return *this;
  }
  return RenameField(std::string(existing.name()), std::string{name});
}

UpdatePartitionSpec& UpdatePartitionSpec::RemoveField(std::string_view name) {
  // Cannot delete newly added fields
  ICEBERG_BUILDER_CHECK(!added_field_names_.contains(name),
                        "Cannot delete newly added field: {}", name);

  // Cannot rename and delete
  ICEBERG_BUILDER_CHECK(!renames_.contains(name),
                        "Cannot rename and delete partition field: {}", name);

  auto field_it = name_to_field_.find(name);
  ICEBERG_BUILDER_CHECK(field_it != name_to_field_.end(),
                        "Cannot find partition field to remove: {}", name);

  deletes_.insert(field_it->second->field_id());
  return *this;
}

UpdatePartitionSpec& UpdatePartitionSpec::RemoveField(const std::shared_ptr<Term>& term) {
  ICEBERG_BUILDER_CHECK(term->is_unbound(),
                        "Cannot remove bound term from partition spec");
  // Bind the term to get the source id
  if (term->kind() == Term::Kind::kReference) {
    const auto& ref = dynamic_cast<const NamedReference&>(*term);
    ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto bound_ref, ref.Bind(*schema_, case_sensitive_));
    int32_t source_id = bound_ref->field().field_id();
    // Reference terms use identity transform
    TransformKey key{source_id, Transform::Identity()->ToString()};
    return RemoveFieldByTransform(key, term->ToString());
  } else if (term->kind() == Term::Kind::kTransform) {
    const auto& unbound_transform = dynamic_cast<const UnboundTransform&>(*term);
    ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto bound_transform,
                                     unbound_transform.Bind(*schema_, case_sensitive_));
    int32_t source_id = bound_transform->reference()->field().field_id();
    auto transform = bound_transform->transform();
    TransformKey key{source_id, transform->ToString()};
    return RemoveFieldByTransform(key, term->ToString());
  }

  return AddError(
      InvalidArgument("Cannot remove {} term from partition spec", term->ToString()));
}

UpdatePartitionSpec& UpdatePartitionSpec::RemoveFieldByTransform(
    const TransformKey& key, std::string_view term_str) {
  // Cannot delete newly added fields
  ICEBERG_BUILDER_CHECK(!transform_to_added_field_.contains(key),
                        "Cannot delete newly added field: {}", term_str);

  auto field_it = transform_to_field_.find(key);
  ICEBERG_BUILDER_CHECK(field_it != transform_to_field_.end(),
                        "Cannot find partition field to remove: {}", term_str);

  const auto& field = field_it->second;
  // Cannot rename and delete
  ICEBERG_BUILDER_CHECK(!renames_.contains(std::string(field->name())),
                        "Cannot rename and delete partition field: {}", field->name());

  deletes_.insert(field->field_id());
  return *this;
}

UpdatePartitionSpec& UpdatePartitionSpec::RenameField(std::string_view name,
                                                      std::string new_name) {
  // Handle existing void field with the new name
  auto existing_it = name_to_field_.find(new_name);
  if (existing_it != name_to_field_.end() && IsVoidTransform(*existing_it->second)) {
    std::string renamed = std::format("{}_{}", existing_it->second->name(),
                                      existing_it->second->field_id());
    RenameField(existing_it->second->name(), std::move(renamed));
  }

  // Cannot rename newly added fields
  ICEBERG_BUILDER_CHECK(!added_field_names_.contains(name),
                        "Cannot rename newly added partition field: {}", name);

  auto field_it = name_to_field_.find(name);
  ICEBERG_BUILDER_CHECK(field_it != name_to_field_.end(),
                        "Cannot find partition field to rename: {}", name);

  // Cannot delete and rename
  ICEBERG_BUILDER_CHECK(!deletes_.contains(field_it->second->field_id()),
                        "Cannot delete and rename partition field: {}", name);

  renames_.emplace(name, std::move(new_name));
  return *this;
}

Result<UpdatePartitionSpec::ApplyResult> UpdatePartitionSpec::Apply() {
  ICEBERG_RETURN_UNEXPECTED(CheckErrors());

  std::vector<PartitionField> new_fields;
  new_fields.reserve(spec_->fields().size() + adds_.size());

  // Process existing fields
  for (const auto& field : spec_->fields()) {
    if (!deletes_.contains(field.field_id())) {
      // Field is kept, check for rename
      auto rename_it = renames_.find(field.name());
      if (rename_it != renames_.end()) {
        new_fields.emplace_back(field.source_id(), field.field_id(), rename_it->second,
                                field.transform());
      } else {
        new_fields.push_back(field);
      }
    } else if (format_version_ < 2) {
      // field IDs were not required for v1 and were assigned sequentially in each
      // partition spec starting at 1,000.
      // To maintain consistent field ids across partition specs in v1 tables, any
      // partition field that is removed must be replaced with a null transform. null
      // values are always allowed in partition data.
      auto rename_it = renames_.find(field.name());
      std::string field_name =
          rename_it != renames_.end() ? rename_it->second : std::string(field.name());
      new_fields.emplace_back(field.source_id(), field.field_id(), std::move(field_name),
                              Transform::Void());
    }
    // In V2, deleted fields are simply removed
  }

  // Add new fields
  new_fields.insert(new_fields.end(), adds_.begin(), adds_.end());

  // Use -1 as a placeholder for the spec id, the actual spec id will be assigned by
  // TableMetadataBuilder when the AddPartitionSpec update is applied.
  ICEBERG_ASSIGN_OR_RAISE(auto new_spec,
                          PartitionSpec::Make(/*spec_id=*/-1, std::move(new_fields)));
  ICEBERG_RETURN_UNEXPECTED(new_spec->Validate(*schema_, /*allow_missing_fields=*/false));

  return ApplyResult{.spec = std::move(new_spec), .set_as_default = set_as_default_};
}

int32_t UpdatePartitionSpec::AssignFieldId() { return ++last_assigned_partition_id_; }

PartitionField UpdatePartitionSpec::RecycleOrCreatePartitionField(
    int32_t source_id, std::shared_ptr<Transform> transform, std::string_view name) {
  // In V2+, use pre-built index for O(1) lookup instead of O(n*m) iteration
  if (format_version_ >= 2 && !historical_fields_.empty()) {
    auto it = historical_fields_.find(TransformKey{source_id, transform->ToString()});
    if (it != historical_fields_.end()) {
      const auto& field = it->second;
      // If target name is specified then consider it too, otherwise not
      if (name.empty() || field.name() == name) {
        return field;
      }
    }
  }
  // No matching field found, create a new one
  return {source_id, AssignFieldId(), std::string{name}, std::move(transform)};
}

Result<std::string> UpdatePartitionSpec::GeneratePartitionName(
    int32_t source_id, const std::shared_ptr<Transform>& transform) const {
  // Find the source field name
  ICEBERG_ASSIGN_OR_RAISE(auto field_opt, schema_->FindFieldById(source_id));
  if (!field_opt.has_value()) {
    return Invalid("Cannot find source field for partition field: {}", source_id);
  }
  return transform->GeneratePartitionName(field_opt.value().get().name());
}

bool UpdatePartitionSpec::IsTimeTransform(const std::shared_ptr<Transform>& transform) {
  switch (transform->transform_type()) {
    case TransformType::kYear:
    case TransformType::kMonth:
    case TransformType::kDay:
    case TransformType::kHour:
      return true;
    default:
      return false;
  }
}

bool UpdatePartitionSpec::IsVoidTransform(const PartitionField& field) {
  return field.transform()->transform_type() == TransformType::kVoid;
}

void UpdatePartitionSpec::CheckForRedundantAddedPartitions(const PartitionField& field) {
  if (IsTimeTransform(field.transform())) {
    if (added_time_fields_.contains(field.source_id())) {
      AddError(ErrorKind::kInvalidArgument,
               "Cannot add redundant partition field: {} conflicts with {}",
               field.ToString(), added_time_fields_.at(field.source_id()));
      return;
    }
    added_time_fields_.emplace(field.source_id(), field.ToString());
  }
}

std::unordered_map<std::string, const PartitionField*, StringHash, StringEqual>
UpdatePartitionSpec::IndexSpecByName(const PartitionSpec& spec) {
  std::unordered_map<std::string, const PartitionField*, StringHash, StringEqual> index;
  for (const auto& field : spec.fields()) {
    index.emplace(field.name(), &field);
  }
  return index;
}

std::unordered_map<UpdatePartitionSpec::TransformKey, const PartitionField*,
                   UpdatePartitionSpec::TransformKeyHash>
UpdatePartitionSpec::IndexSpecByTransform(const PartitionSpec& spec) {
  std::unordered_map<TransformKey, const PartitionField*, TransformKeyHash> index;
  index.reserve(spec.fields().size());
  for (const auto& field : spec.fields()) {
    TransformKey key{field.source_id(), field.transform()->ToString()};
    index.emplace(key, &field);
  }
  return index;
}

void UpdatePartitionSpec::BuildHistoricalFieldsIndex() {
  // Count total fields across all specs to reserve capacity
  size_t total_fields = 0;
  for (const auto& partition_spec : base().partition_specs) {
    total_fields += partition_spec->fields().size();
  }
  historical_fields_.reserve(total_fields);

  // Index all fields from all historical partition specs
  // Later specs override earlier ones for the same (source_id, transform) key
  for (const auto& partition_spec : base().partition_specs) {
    for (const auto& field : partition_spec->fields()) {
      TransformKey key{field.source_id(), field.transform()->ToString()};
      historical_fields_.emplace(key, field);
    }
  }
}

}  // namespace iceberg
