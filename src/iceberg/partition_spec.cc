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

#include "iceberg/partition_spec.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <format>
#include <map>
#include <memory>
#include <ranges>
#include <sstream>
#include <unordered_map>
#include <utility>

#include "iceberg/result.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/transform.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep
#include "iceberg/util/macros.h"
#include "iceberg/util/type_util.h"
#include "iceberg/util/url_encoder.h"

namespace iceberg {

PartitionSpec::PartitionSpec(int32_t spec_id, std::vector<PartitionField> fields,
                             std::optional<int32_t> last_assigned_field_id)
    : spec_id_(spec_id), fields_(std::move(fields)) {
  if (last_assigned_field_id) {
    last_assigned_field_id_ = last_assigned_field_id.value();
  } else if (fields_.empty()) {
    last_assigned_field_id_ = kLegacyPartitionDataIdStart - 1;
  } else {
    last_assigned_field_id_ = std::ranges::max(fields_, {}, [](const auto& field) {
                                return field.field_id();
                              }).field_id();
  }
}

const std::shared_ptr<PartitionSpec>& PartitionSpec::Unpartitioned() {
  static const std::shared_ptr<PartitionSpec> unpartitioned(new PartitionSpec(
      kInitialSpecId, std::vector<PartitionField>{}, kLegacyPartitionDataIdStart - 1));
  return unpartitioned;
}

int32_t PartitionSpec::spec_id() const { return spec_id_; }

std::span<const PartitionField> PartitionSpec::fields() const { return fields_; }

Result<std::unique_ptr<StructType>> PartitionSpec::PartitionType(
    const Schema& schema) const {
  if (fields_.empty()) {
    return std::make_unique<StructType>(std::vector<SchemaField>{});
  }

  std::vector<SchemaField> partition_fields;
  for (const auto& partition_field : fields_) {
    // Get the source field from the original schema by source_id
    ICEBERG_ASSIGN_OR_RAISE(auto source_field,
                            schema.FindFieldById(partition_field.source_id()));
    if (!source_field.has_value()) {
      // TODO(xiao.dong) when source field is missing,
      // should return an error or just use UNKNOWN type
      return InvalidSchema("Cannot find source field for partition field:{}",
                           partition_field.field_id());
    }
    auto source_field_type = source_field.value().get().type();
    // Bind the transform to the source field type to get the result type
    ICEBERG_ASSIGN_OR_RAISE(auto transform_function,
                            partition_field.transform()->Bind(source_field_type));

    auto result_type = transform_function->ResultType();

    // Create the partition field with the transform result type
    // Partition fields are always optional (can be null)
    partition_fields.emplace_back(partition_field.field_id(),
                                  std::string(partition_field.name()),
                                  std::move(result_type),
                                  /*optional=*/true);
  }

  return std::make_unique<StructType>(std::move(partition_fields));
}

Result<std::string> PartitionSpec::PartitionPath(const PartitionValues& data) const {
  ICEBERG_PRECHECK(fields_.size() == data.num_fields(),
                   "Partition spec and data mismatch, expected field num {}, got {}",
                   fields_.size(), data.num_fields());
  std::stringstream ss;
  for (int32_t i = 0; i < fields_.size(); ++i) {
    ICEBERG_ASSIGN_OR_RAISE(auto value, data.ValueAt(i));
    if (i > 0) {
      ss << "/";
    }
    // TODO(zhuo.wang): transform for partition value, will be fixed after transform util
    // is ready
    std::string partition_value = value.get().ToString();
    ss << UrlEncoder::Encode(fields_[i].name()) << "="
       << UrlEncoder::Encode(partition_value);
  }
  return ss.str();
}

bool PartitionSpec::CompatibleWith(const PartitionSpec& other) const {
  if (Equals(other)) {
    return true;
  }

  if (fields_.size() != other.fields_.size()) {
    return false;
  }

  for (const auto& [lhs, rhs] :
       std::ranges::zip_view<std::span<const PartitionField>,
                             std::span<const PartitionField>>{fields_, other.fields_}) {
    if (lhs.source_id() != rhs.source_id() || *lhs.transform() != *rhs.transform() ||
        lhs.name() != rhs.name()) {
      return false;
    }
  }

  return true;
}

std::string PartitionSpec::ToString() const {
  std::string repr = std::format("partition_spec[spec_id<{}>,\n", spec_id_);
  for (const auto& field : fields_) {
    std::format_to(std::back_inserter(repr), "  {}\n", field);
  }
  repr += "]";
  return repr;
}

bool PartitionSpec::Equals(const PartitionSpec& other) const {
  return spec_id_ == other.spec_id_ && fields_ == other.fields_;
}

Status PartitionSpec::Validate(const Schema& schema, bool allow_missing_fields) const {
  ICEBERG_RETURN_UNEXPECTED(ValidatePartitionName(schema, *this));
  ICEBERG_RETURN_UNEXPECTED(ValidateRedundantPartitions(*this));

  std::unordered_map<int32_t, int32_t> parents = IndexParents(schema);
  for (const auto& partition_field : fields_) {
    ICEBERG_ASSIGN_OR_RAISE(auto source_field,
                            schema.FindFieldById(partition_field.source_id()));
    // In the case the underlying field is dropped, we cannot check if they are compatible
    if (allow_missing_fields && !source_field.has_value()) {
      continue;
    }
    const auto& field_transform = partition_field.transform();

    // In the case of a Version 1 partition-spec field gets deleted, it is replaced with a
    // void transform, see: https://iceberg.apache.org/spec/#partition-transforms. We
    // don't care about the source type since a VoidTransform is always compatible and
    // skip the checks
    if (field_transform->transform_type() != TransformType::kVoid) {
      if (!source_field.has_value()) {
        return InvalidArgument("Cannot find source column for partition field: {}",
                               partition_field);
      }
      const auto& source_type = source_field.value().get().type();
      if (!field_transform->CanTransform(*source_type)) {
        return InvalidArgument("Invalid source type {} for transform {}",
                               source_type->ToString(), field_transform->ToString());
      }

      // The only valid parent types for a PartitionField are StructTypes. This must be
      // checked recursively.
      auto parent_id_iter = parents.find(partition_field.source_id());
      while (parent_id_iter != parents.end()) {
        int32_t parent_id = parent_id_iter->second;
        ICEBERG_ASSIGN_OR_RAISE(auto parent_field, schema.FindFieldById(parent_id));
        if (!parent_field.has_value()) {
          return InvalidArgument("Cannot find parent field with ID: {}", parent_id);
        }
        const auto& parent_type = parent_field.value().get().type();
        if (parent_type->type_id() != TypeId::kStruct) {
          return InvalidArgument("Invalid partition field parent type: {}",
                                 parent_type->ToString());
        }
        parent_id_iter = parents.find(parent_id);
      }
    }
  }
  return {};
}

Status PartitionSpec::ValidatePartitionName(const Schema& schema,
                                            const PartitionSpec& spec) {
  std::unordered_set<std::string> partition_names;
  for (const auto& partition_field : spec.fields()) {
    auto name = std::string(partition_field.name());
    ICEBERG_CHECK(!name.empty(), "Cannot use empty partition name: {}", name);

    ICEBERG_CHECK(!partition_names.contains(name),
                  "Cannot use partition name more than once: {}", name);
    partition_names.insert(name);

    ICEBERG_ASSIGN_OR_RAISE(auto schema_field, schema.FindFieldByName(name));
    auto transform_type = partition_field.transform()->transform_type();
    if (transform_type == TransformType::kIdentity ||
        transform_type == TransformType::kVoid) {
      // for identity/nulls transform case we allow conflicts between partition and schema
      // field name as long as they are sourced from the same schema field
      if (schema_field.has_value() &&
          schema_field.value().get().field_id() != partition_field.source_id()) {
        return ValidationFailed(
            "Cannot create identity partition sourced from different field in schema: {}",
            name);
      }
    } else {
      // for all other transforms we don't allow conflicts between partition name and
      // schema field name
      ICEBERG_CHECK(!schema_field.has_value(),
                    "Cannot create partition from name that exists in schema: {}", name);
    }
  }

  return {};
}

Result<std::vector<std::reference_wrapper<const PartitionField>>>
PartitionSpec::GetFieldsBySourceId(int32_t source_id) const {
  ICEBERG_ASSIGN_OR_RAISE(auto source_id_to_fields, source_id_to_fields_.Get(*this));
  if (auto it = source_id_to_fields.get().find(source_id);
      it != source_id_to_fields.get().cend()) {
    return it->second;
  }
  // Note that it is not an error to not find any partition fields for a source id.
  return std::vector<PartitionFieldRef>{};
}

Result<PartitionSpec::SourceIdToFieldsMap> PartitionSpec::InitSourceIdToFieldsMap(
    const PartitionSpec& self) {
  SourceIdToFieldsMap source_id_to_fields;
  for (const auto& field : self.fields_) {
    source_id_to_fields[field.source_id()].emplace_back(std::cref(field));
  }
  return source_id_to_fields;
}

Result<std::unique_ptr<PartitionSpec>> PartitionSpec::Make(
    const Schema& schema, int32_t spec_id, std::vector<PartitionField> fields,
    bool allow_missing_fields, std::optional<int32_t> last_assigned_field_id) {
  auto partition_spec = std::unique_ptr<PartitionSpec>(
      new PartitionSpec(spec_id, std::move(fields), last_assigned_field_id));
  ICEBERG_RETURN_UNEXPECTED(partition_spec->Validate(schema, allow_missing_fields));
  return partition_spec;
}

Result<std::unique_ptr<PartitionSpec>> PartitionSpec::Make(
    int32_t spec_id, std::vector<PartitionField> fields,
    std::optional<int32_t> last_assigned_field_id) {
  return std::unique_ptr<PartitionSpec>(
      new PartitionSpec(spec_id, std::move(fields), last_assigned_field_id));
}

bool PartitionSpec::HasSequentialFieldIds(const PartitionSpec& spec) {
  for (size_t i = 0; i < spec.fields().size(); i += 1) {
    if (spec.fields()[i].field_id() != PartitionSpec::kLegacyPartitionDataIdStart + i) {
      return false;
    }
  }
  return true;
}

Status PartitionSpec::ValidateRedundantPartitions(const PartitionSpec& spec) {
  // Use a map to track deduplication keys (source_id + transform dedup name)
  std::map<std::pair<int32_t, std::string>, const PartitionField*> dedup_fields;

  for (const auto& field : spec.fields()) {
    // The dedup name is provided by the transform's DedupName() method
    // which typically returns the transform's string representation
    auto dedup_key = std::make_pair(field.source_id(), field.transform()->DedupName());

    // Check if this dedup key already exists
    // If it does, we have found a redundant partition field
    auto existing_field_iter = dedup_fields.find(dedup_key);
    ICEBERG_CHECK(existing_field_iter == dedup_fields.end(),
                  "Cannot add redundant partition: {} conflicts with {}",
                  field.ToString(), existing_field_iter->second->ToString());

    // Add this field to the dedup map for future conflict detection
    dedup_fields[dedup_key] = &field;
  }

  return {};
}

}  // namespace iceberg
