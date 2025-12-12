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

#include <format>

#include "iceberg/catalog.h"
#include "iceberg/expression/term.h"
#include "iceberg/partition_field.h"
#include "iceberg/partition_spec.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/table.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_requirements.h"
#include "iceberg/table_update.h"
#include "iceberg/transform.h"
#include "iceberg/util/macros.h"

namespace iceberg {

UpdatePartitionSpec::UpdatePartitionSpec(TableIdentifier identifier,
                                         std::shared_ptr<Catalog> catalog,
                                         std::shared_ptr<TableMetadata> base)
    : identifier_(std::move(identifier)),
      catalog_(std::move(catalog)),
      base_metadata_(std::move(base)) {
  ICEBERG_DCHECK(catalog_, "Catalog is required to construct UpdatePartitionSpec");
  ICEBERG_DCHECK(base_metadata_,
                 "Base table metadata is required to construct UpdatePartitionSpec");
  format_version_ = base_metadata_->format_version;

  // Get the current/default partition spec
  auto spec_result = base_metadata_->PartitionSpec();
  if (!spec_result.has_value()) {
    AddError(spec_result.error());
    return;
  }
  spec_ = std::move(spec_result.value());

  // Get the current schema
  auto schema_result = base_metadata_->Schema();
  if (!schema_result.has_value()) {
    AddError(schema_result.error());
    return;
  }
  schema_ = std::move(schema_result.value());

  last_assigned_partition_id_ = spec_->last_assigned_field_id();
  name_to_field_ = IndexSpecByName(*spec_);
  transform_to_field_ = IndexSpecByTransform(*spec_);

  // Check for unknown transforms
  for (const auto& field : spec_->fields()) {
    if (field.transform()->transform_type() == TransformType::kUnknown) {
      AddError(ErrorKind::kInvalidArgument,
               std::format("Cannot update partition spec with unknown transform: {}",
                           field.ToString()));
      return;
    }
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

UpdatePartitionSpec& UpdatePartitionSpec::AddField(const std::string& source_name) {
  // Find the source field in the schema
  auto field_result = schema_->FindFieldByName(source_name, case_sensitive_);
  if (!field_result.has_value()) {
    AddError(ErrorKind::kInvalidArgument,
             std::format("Cannot find source field: {}", source_name));
    return *this;
  }

  auto field_opt = field_result.value();
  if (!field_opt.has_value()) {
    AddError(ErrorKind::kInvalidArgument,
             std::format("Cannot find source field: {}", source_name));
    return *this;
  }

  int32_t source_id = field_opt.value().get().field_id();
  return AddFieldInternal(nullptr, source_id, Transform::Identity());
}

UpdatePartitionSpec& UpdatePartitionSpec::AddField(
    std::shared_ptr<UnboundTerm<BoundReference>> term) {
  return AddField(std::nullopt, std::move(term));
}

UpdatePartitionSpec& UpdatePartitionSpec::AddField(
    std::shared_ptr<UnboundTerm<BoundTransform>> term) {
  return AddField(std::nullopt, std::move(term));
}

UpdatePartitionSpec& UpdatePartitionSpec::AddField(
    std::optional<std::string> name, std::shared_ptr<UnboundTerm<BoundReference>> term) {
  // Bind the term to get the source field
  auto bound_result = term->Bind(*schema_, case_sensitive_);
  if (!bound_result.has_value()) {
    AddError(ErrorKind::kInvalidArgument,
             std::format("Cannot bind term: {}", term->ToString()));
    return *this;
  }

  auto bound_ref = bound_result.value();
  int32_t source_id = bound_ref->field().field_id();

  // Reference terms use identity transform
  return AddFieldInternal(name ? &name.value() : nullptr, source_id,
                          Transform::Identity());
}

UpdatePartitionSpec& UpdatePartitionSpec::AddField(
    std::optional<std::string> name, std::shared_ptr<UnboundTerm<BoundTransform>> term) {
  // Bind the term to get the source field and transform
  auto bound_result = term->Bind(*schema_, case_sensitive_);
  if (!bound_result.has_value()) {
    AddError(ErrorKind::kInvalidArgument,
             std::format("Cannot bind term: {}", term->ToString()));
    return *this;
  }

  auto bound_transform = bound_result.value();
  int32_t source_id = bound_transform->reference()->field().field_id();
  auto transform = bound_transform->transform();

  return AddFieldInternal(name ? &name.value() : nullptr, source_id, transform);
}

UpdatePartitionSpec& UpdatePartitionSpec::AddFieldInternal(
    const std::string* name, int32_t source_id, std::shared_ptr<Transform> transform) {
  // Check for duplicate name in added fields
  if (name != nullptr) {
    auto it = name_to_added_field_.find(*name);
    if (it != name_to_added_field_.end()) {
      AddError(ErrorKind::kInvalidArgument,
               std::format("Cannot add duplicate partition field: {}", *name));
      return *this;
    }
  }

  TransformKey validation_key{source_id, transform->ToString()};

  // Check if this field already exists in the current spec
  auto existing_it = transform_to_field_.find(validation_key);
  if (existing_it != transform_to_field_.end()) {
    const auto& existing = existing_it->second;
    if (deletes_.contains(existing.field_id()) && *existing.transform() == *transform) {
      // If the field was deleted and we're re-adding the same one, just undo the delete
      return RewriteDeleteAndAddField(existing, name);
    }

    if (deletes_.find(existing.field_id()) == deletes_.end()) {
      AddError(
          ErrorKind::kInvalidArgument,
          std::format(
              "Cannot add duplicate partition field for source {} with transform {}, "
              "conflicts with {}",
              source_id, transform->ToString(), existing.ToString()));
      return *this;
    }
  }

  // Check if already being added
  auto added_it = transform_to_added_field_.find(validation_key);
  if (added_it != transform_to_added_field_.end()) {
    AddError(ErrorKind::kInvalidArgument,
             std::format(
                 "Cannot add duplicate partition field for source {} with transform {}, "
                 "already added: {}",
                 source_id, transform->ToString(), added_it->second.ToString()));
    return *this;
  }

  // Create or recycle the partition field
  PartitionField new_field = RecycleOrCreatePartitionField(source_id, transform, name);

  // Generate name if not provided
  std::string field_name;
  if (name != nullptr) {
    field_name = *name;
  } else {
    field_name = GeneratePartitionName(source_id, transform);
  }

  // Create the final field with the name
  new_field = PartitionField(new_field.source_id(), new_field.field_id(), field_name,
                             new_field.transform());

  // Check for redundant time-based partitions
  CheckForRedundantAddedPartitions(new_field);

  transform_to_added_field_.emplace(validation_key, new_field);

  // Handle name conflicts with existing fields
  auto existing_name_it = name_to_field_.find(field_name);
  if (existing_name_it != name_to_field_.end()) {
    const auto& existing_field = existing_name_it->second;
    if (!deletes_.contains(existing_field.field_id())) {
      if (IsVoidTransform(existing_field)) {
        // Rename the old deleted field
        std::string renamed =
            std::format("{}_{}", existing_field.name(), existing_field.field_id());
        renames_[std::string(existing_field.name())] = renamed;
      } else {
        AddError(
            ErrorKind::kInvalidArgument,
            std::format("Cannot add duplicate partition field name: {}", field_name));
        return *this;
      }
    } else {
      // Field is being deleted, rename it to avoid conflict
      std::string renamed =
          std::format("{}_{}", existing_field.name(), existing_field.field_id());
      renames_[std::string(existing_field.name())] = renamed;
    }
  }

  name_to_added_field_.emplace(field_name, new_field);
  adds_.push_back(new_field);

  return *this;
}

UpdatePartitionSpec& UpdatePartitionSpec::RewriteDeleteAndAddField(
    const PartitionField& existing, const std::string* name) {
  deletes_.erase(existing.field_id());
  if (name == nullptr || std::string(existing.name()) == *name) {
    return *this;
  }
  return RenameField(std::string(existing.name()), *name);
}

UpdatePartitionSpec& UpdatePartitionSpec::RemoveField(const std::string& name) {
  // Cannot delete newly added fields
  auto added_it = name_to_added_field_.find(name);
  if (added_it != name_to_added_field_.end()) {
    AddError(ErrorKind::kInvalidArgument,
             std::format("Cannot delete newly added field: {}", name));
    return *this;
  }

  // Cannot rename and delete
  if (renames_.find(name) != renames_.end()) {
    AddError(ErrorKind::kInvalidArgument,
             std::format("Cannot rename and delete partition field: {}", name));
    return *this;
  }

  auto field_it = name_to_field_.find(name);
  if (field_it == name_to_field_.end()) {
    AddError(ErrorKind::kInvalidArgument,
             std::format("Cannot find partition field to remove: {}", name));
    return *this;
  }

  deletes_.insert(field_it->second.field_id());
  return *this;
}

UpdatePartitionSpec& UpdatePartitionSpec::RemoveField(
    std::shared_ptr<UnboundTerm<BoundReference>> term) {
  // Bind the term to get the source field
  auto bound_result = term->Bind(*schema_, case_sensitive_);
  if (!bound_result.has_value()) {
    AddError(ErrorKind::kInvalidArgument,
             std::format("Cannot bind term: {}", term->ToString()));
    return *this;
  }

  auto bound_ref = bound_result.value();
  int32_t source_id = bound_ref->field().field_id();

  // Reference terms use identity transform
  TransformKey key{source_id, Transform::Identity()->ToString()};
  return RemoveFieldByTransform(key, term->ToString());
}

UpdatePartitionSpec& UpdatePartitionSpec::RemoveField(
    std::shared_ptr<UnboundTerm<BoundTransform>> term) {
  // Bind the term to get the source field and transform
  auto bound_result = term->Bind(*schema_, case_sensitive_);
  if (!bound_result.has_value()) {
    AddError(ErrorKind::kInvalidArgument,
             std::format("Cannot bind term: {}", term->ToString()));
    return *this;
  }

  auto bound_transform = bound_result.value();
  int32_t source_id = bound_transform->reference()->field().field_id();
  auto transform = bound_transform->transform();

  TransformKey key{source_id, transform->ToString()};
  return RemoveFieldByTransform(key, term->ToString());
}

UpdatePartitionSpec& UpdatePartitionSpec::RemoveFieldByTransform(
    const TransformKey& key, const std::string& term_str) {
  // Cannot delete newly added fields
  auto added_it = transform_to_added_field_.find(key);
  if (added_it != transform_to_added_field_.end()) {
    AddError(ErrorKind::kInvalidArgument,
             std::format("Cannot delete newly added field: {}", term_str));
    return *this;
  }

  auto field_it = transform_to_field_.find(key);
  if (field_it == transform_to_field_.end()) {
    AddError(ErrorKind::kInvalidArgument,
             std::format("Cannot find partition field to remove: {}", term_str));
    return *this;
  }

  const auto& field = field_it->second;
  // Cannot rename and delete
  if (renames_.find(std::string(field.name())) != renames_.end()) {
    AddError(ErrorKind::kInvalidArgument,
             std::format("Cannot rename and delete partition field: {}", field.name()));
    return *this;
  }

  deletes_.insert(field.field_id());
  return *this;
}

UpdatePartitionSpec& UpdatePartitionSpec::RenameField(const std::string& name,
                                                      const std::string& new_name) {
  // Handle existing void field with the new name
  auto existing_it = name_to_field_.find(new_name);
  if (existing_it != name_to_field_.end() && IsVoidTransform(existing_it->second)) {
    std::string renamed =
        std::format("{}_{}", existing_it->second.name(), existing_it->second.field_id());
    renames_[new_name] = renamed;
  }

  // Cannot rename newly added fields
  auto added_it = name_to_added_field_.find(name);
  if (added_it != name_to_added_field_.end()) {
    AddError(ErrorKind::kInvalidArgument,
             std::format("Cannot rename newly added partition field: {}", name));
    return *this;
  }

  auto field_it = name_to_field_.find(name);
  if (field_it == name_to_field_.end()) {
    AddError(ErrorKind::kInvalidArgument,
             std::format("Cannot find partition field to rename: {}", name));
    return *this;
  }

  // Cannot delete and rename
  if (deletes_.contains(field_it->second.field_id())) {
    AddError(ErrorKind::kInvalidArgument,
             std::format("Cannot delete and rename partition field: {}", name));
    return *this;
  }

  renames_[name] = new_name;
  return *this;
}

Status UpdatePartitionSpec::Apply() {
  ICEBERG_RETURN_UNEXPECTED(CheckErrors());

  std::vector<PartitionField> new_fields;

  // Process existing fields
  for (const auto& field : spec_->fields()) {
    if (!deletes_.contains(field.field_id())) {
      // Field is kept, check for rename
      auto rename_it = renames_.find(std::string(field.name()));
      if (rename_it != renames_.end()) {
        new_fields.emplace_back(field.source_id(), field.field_id(), rename_it->second,
                                field.transform());
      } else {
        new_fields.push_back(field);
      }
    } else if (format_version_ < 2) {
      // In V1, deleted fields are replaced with void transform
      auto rename_it = renames_.find(std::string(field.name()));
      std::string field_name =
          rename_it != renames_.end() ? rename_it->second : std::string(field.name());
      new_fields.emplace_back(field.source_id(), field.field_id(), field_name,
                              Transform::Void());
    }
    // In V2, deleted fields are simply removed
  }

  // Add new fields
  for (const auto& new_field : adds_) {
    new_fields.push_back(new_field);
  }

  // Determine the new spec ID
  int32_t new_spec_id = spec_ ? spec_->spec_id() + 1 : PartitionSpec::kInitialSpecId;

  // In V2, if all fields are removed, reset last_assigned_partition_id to allow
  // field IDs to restart from 1000 when fields are added again
  int32_t last_assigned_id = last_assigned_partition_id_;
  if (format_version_ >= 2 && new_fields.empty()) {
    last_assigned_id = PartitionSpec::kLegacyPartitionDataIdStart - 1;
  }

  ICEBERG_ASSIGN_OR_RAISE(
      auto spec_result, PartitionSpec::Make(*schema_, new_spec_id, std::move(new_fields),
                                            last_assigned_id));
  applied_spec_ = std::shared_ptr<PartitionSpec>(spec_result.release());
  return {};
}

Result<std::shared_ptr<PartitionSpec>> UpdatePartitionSpec::GetAppliedSpec() const {
  if (!applied_spec_) {
    return InvalidArgument("Apply() must be called successfully before getting the spec");
  }
  return applied_spec_;
}

Status UpdatePartitionSpec::Commit() {
  // Apply the changes first
  ICEBERG_RETURN_UNEXPECTED(Apply());

  ICEBERG_ASSIGN_OR_RAISE(auto spec_result, GetAppliedSpec());
  std::shared_ptr<PartitionSpec> new_spec = spec_result;

  std::vector<std::unique_ptr<TableUpdate>> updates;

  // Add the new partition spec
  updates.emplace_back(std::make_unique<table::AddPartitionSpec>(new_spec));

  // If set_as_default_ is true, set this spec as the default
  if (set_as_default_) {
    updates.emplace_back(
        std::make_unique<table::SetDefaultPartitionSpec>(new_spec->spec_id()));
  }

  ICEBERG_ASSIGN_OR_RAISE(auto requirements,
                          TableRequirements::ForUpdateTable(*base_metadata_, updates));
  ICEBERG_RETURN_UNEXPECTED(catalog_->UpdateTable(identifier_, requirements, updates));

  return {};
}

int32_t UpdatePartitionSpec::AssignFieldId() { return ++last_assigned_partition_id_; }

PartitionField UpdatePartitionSpec::RecycleOrCreatePartitionField(
    int32_t source_id, std::shared_ptr<Transform> transform, const std::string* name) {
  // In V2+, search historical specs for a matching field to recycle
  if (format_version_ >= 2) {
    // Collect all fields from all historical partition specs
    std::vector<PartitionField> all_historical_fields;
    for (const auto& partition_spec : base_metadata_->partition_specs) {
      for (const auto& field : partition_spec->fields()) {
        all_historical_fields.push_back(field);
      }
    }

    // Search for a matching field
    for (const auto& field : all_historical_fields) {
      if (field.source_id() == source_id && *field.transform() == *transform) {
        // If target name is specified then consider it too, otherwise not
        if (name == nullptr || std::string(field.name()) == *name) {
          return field;
        }
      }
    }
  }
  // No matching field found, create a new one
  std::string field_name = name ? *name : "";
  return {source_id, AssignFieldId(), field_name, transform};
}

std::string UpdatePartitionSpec::GeneratePartitionName(
    int32_t source_id, const std::shared_ptr<Transform>& transform) const {
  // Find the source field name
  auto field_result = schema_->FindFieldById(source_id);
  std::string source_name = "unknown";
  if (field_result.has_value() && field_result.value().has_value()) {
    source_name = std::string(field_result.value().value().get().name());
  }

  // Extract parameter from transform string for bucket and truncate
  // Transform::ToString() returns "bucket[16]" or "truncate[4]" format
  std::string transform_str = transform->ToString();

  switch (transform->transform_type()) {
    case TransformType::kIdentity:
      return source_name;
    case TransformType::kBucket: {
      // Parse "bucket[N]" to extract N
      // Format: sourceName_bucket_N (matching Java: sourceName + "_bucket_" + numBuckets)
      size_t open_bracket = transform_str.find('[');
      size_t close_bracket = transform_str.find(']');
      if (open_bracket != std::string::npos && close_bracket != std::string::npos) {
        std::string param_str =
            transform_str.substr(open_bracket + 1, close_bracket - open_bracket - 1);
        return std::format("{}_{}_{}", source_name, "bucket", param_str);
      }
      return std::format("{}_bucket", source_name);
    }
    case TransformType::kTruncate: {
      // Parse "truncate[N]" to extract N
      // Format: sourceName_trunc_N (matching Java: sourceName + "_trunc_" + width)
      size_t open_bracket = transform_str.find('[');
      size_t close_bracket = transform_str.find(']');
      if (open_bracket != std::string::npos && close_bracket != std::string::npos) {
        std::string param_str =
            transform_str.substr(open_bracket + 1, close_bracket - open_bracket - 1);
        return std::format("{}_{}_{}", source_name, "trunc", param_str);
      }
      return std::format("{}_trunc", source_name);
    }
    case TransformType::kYear:
      return std::format("{}_year", source_name);
    case TransformType::kMonth:
      return std::format("{}_month", source_name);
    case TransformType::kDay:
      return std::format("{}_day", source_name);
    case TransformType::kHour:
      return std::format("{}_hour", source_name);
    case TransformType::kVoid:
      return std::format("{}_null", source_name);
    case TransformType::kUnknown:
      return std::format("{}_unknown", source_name);
  }
  std::unreachable();
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
  if (HasErrors()) return;

  if (IsTimeTransform(field.transform())) {
    auto it = added_time_fields_.find(field.source_id());
    if (it != added_time_fields_.end()) {
      AddError(ErrorKind::kInvalidArgument,
               std::format("Cannot add redundant partition field: {} conflicts with {}",
                           field.ToString(), it->second.ToString()));
      return;
    }
    added_time_fields_.emplace(field.source_id(), field);
  }
}

std::unordered_map<std::string, PartitionField> UpdatePartitionSpec::IndexSpecByName(
    const PartitionSpec& spec) {
  std::unordered_map<std::string, PartitionField> index;
  for (const auto& field : spec.fields()) {
    index.emplace(std::string(field.name()), field);
  }
  return index;
}

std::unordered_map<UpdatePartitionSpec::TransformKey, PartitionField,
                   UpdatePartitionSpec::TransformKeyHash>
UpdatePartitionSpec::IndexSpecByTransform(const PartitionSpec& spec) {
  std::unordered_map<TransformKey, PartitionField, TransformKeyHash> index;
  for (const auto& field : spec.fields()) {
    TransformKey key{field.source_id(), field.transform()->ToString()};
    index.emplace(key, field);
  }
  return index;
}

}  // namespace iceberg
