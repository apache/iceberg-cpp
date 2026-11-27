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

#include <algorithm>
#include <cstdint>
#include <functional>
#include <map>
#include <ranges>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include <nanoarrow/nanoarrow.h>

#include "iceberg/arrow_row_builder_internal.h"
#include "iceberg/inspect/metadata_partition_util_internal.h"
#include "iceberg/nanoarrow_status_internal.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/table_metadata.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/macros.h"

namespace iceberg::internal {

Status AppendLiteral(ArrowArray* array, const Literal& literal) {
  if (literal.IsNull()) {
    return AppendNull(array);
  }
  if (literal.IsAboveMax() || literal.IsBelowMin()) {
    return InvalidArgument("Cannot append non-value partition literal {}",
                           literal.ToString());
  }

  switch (literal.type()->type_id()) {
    case TypeId::kBoolean:
      return AppendBoolean(array, std::get<bool>(literal.value()));
    case TypeId::kInt:
    case TypeId::kDate:
      return AppendInt(array, std::get<int32_t>(literal.value()));
    case TypeId::kLong:
    case TypeId::kTime:
    case TypeId::kTimestamp:
    case TypeId::kTimestampTz:
    case TypeId::kTimestampNs:
    case TypeId::kTimestampTzNs:
      return AppendInt(array, std::get<int64_t>(literal.value()));
    case TypeId::kFloat:
      return AppendDouble(array, std::get<float>(literal.value()));
    case TypeId::kDouble:
      return AppendDouble(array, std::get<double>(literal.value()));
    case TypeId::kString:
      return AppendString(array, std::get<std::string>(literal.value()));
    case TypeId::kBinary:
    case TypeId::kFixed:
      return AppendBytes(array, std::get<std::vector<uint8_t>>(literal.value()));
    case TypeId::kDecimal:
      return AppendDecimal(array, std::get<Decimal>(literal.value()));
    case TypeId::kUuid:
      return AppendBytes(array, std::get<Uuid>(literal.value()).bytes());
    case TypeId::kUnknown:
    case TypeId::kStruct:
    case TypeId::kList:
    case TypeId::kMap:
    case TypeId::kVariant:
    case TypeId::kGeometry:
    case TypeId::kGeography:
      return NotSupported("Cannot append partition literal of type {}",
                          literal.type()->ToString());
  }
  std::unreachable();
}

Result<std::shared_ptr<StructType>> UnifiedPartitionType(const TableMetadata& metadata) {
  ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());
  auto specs = metadata.partition_specs;
  for (const auto& spec : specs) {
    ICEBERG_PRECHECK(spec != nullptr, "Partition spec cannot be null");
  }
  std::ranges::sort(specs, std::greater{}, &PartitionSpec::spec_id);

  std::unordered_set<int32_t> active_field_ids;
  for (const auto& spec : specs) {
    for (const auto& field : spec->fields()) {
      ICEBERG_PRECHECK(field.transform() != nullptr,
                       "Partition field {} has a null transform", field.field_id());
      ICEBERG_CHECK(field.transform()->transform_type() != TransformType::kUnknown,
                    "Cannot build table partition type with unknown transform '{}'",
                    field.transform()->ToString());
      ICEBERG_ASSIGN_OR_RAISE(auto source_field,
                              schema->FindFieldById(field.source_id()));
      if (source_field.has_value()) {
        active_field_ids.insert(field.field_id());
      }
    }
  }

  struct ProjectedField {
    const PartitionField* definition;
    std::string name;
    std::shared_ptr<Type> type;
  };
  std::map<int32_t, ProjectedField> fields_by_id;
  for (const auto& spec : specs) {
    ICEBERG_ASSIGN_OR_RAISE(auto spec_type, spec->PartitionType(*schema));
    ICEBERG_PRECHECK(spec_type->fields().size() == spec->fields().size(),
                     "Partition spec {} has mismatched field and type counts",
                     spec->spec_id());
    auto active_fields =
        std::views::zip(spec->fields(), spec_type->fields()) |
        std::views::filter([&active_field_ids](const auto& fields) {
          return active_field_ids.contains(std::get<0>(fields).field_id());
        });
    for (const auto& [partition_field, spec_field] : active_fields) {
      auto [iter, inserted] =
          fields_by_id.try_emplace(partition_field.field_id(),
                                   ProjectedField{.definition = &partition_field,
                                                  .name = std::string(spec_field.name()),
                                                  .type = spec_field.type()});
      if (!inserted) {
        const auto& existing = *iter->second.definition;
        const auto current_transform = partition_field.transform()->transform_type();
        const auto existing_transform = existing.transform()->transform_type();
        const bool compatible_transform =
            *partition_field.transform() == *existing.transform() ||
            current_transform == TransformType::kVoid ||
            existing_transform == TransformType::kVoid;
        ICEBERG_CHECK(
            partition_field.source_id() == existing.source_id() && compatible_transform,
            "Conflicting partition fields with ID {}: '{}' and '{}'",
            partition_field.field_id(), partition_field.ToString(), existing.ToString());

        if (existing_transform == TransformType::kVoid &&
            current_transform != TransformType::kVoid) {
          iter->second.definition = &partition_field;
          iter->second.type = spec_field.type();
        }
      }
    }
  }

  std::unordered_map<std::string, size_t> name_counts;
  std::unordered_set<std::string> used_names;
  for (const auto& [_, field] : fields_by_id) {
    ++name_counts[field.name];
    used_names.insert(field.name);
  }

  std::vector<SchemaField> fields;
  fields.reserve(fields_by_id.size());
  for (auto& [field_id, field] : fields_by_id) {
    // Preserve unambiguous names. Suffix every member of a collision so a bare name
    // never silently resolves to one of several partition fields.
    if (name_counts.at(field.name) > 1) {
      field.name += "_" + std::to_string(field_id);
      while (!used_names.insert(field.name).second) {
        field.name += '_';
      }
    }
    fields.emplace_back(field_id, std::move(field.name), std::move(field.type),
                        /*optional=*/true);
  }
  return std::make_shared<StructType>(std::move(fields));
}

Result<PartitionValues> ProjectPartitionValues(const StructType& partition_type,
                                               const PartitionSpec& spec,
                                               const PartitionValues& values) {
  ICEBERG_PRECHECK(values.num_fields() == spec.fields().size(),
                   "Partition has {} values but spec {} has {} fields",
                   values.num_fields(), spec.spec_id(), spec.fields().size());

  std::unordered_map<int32_t, size_t> positions;
  positions.reserve(spec.fields().size());
  for (size_t index = 0; index < spec.fields().size(); ++index) {
    positions.emplace(spec.fields()[index].field_id(), index);
  }

  std::vector<Literal> projected;
  projected.reserve(partition_type.fields().size());
  for (const auto& field : partition_type.fields()) {
    auto target_type = checked_pointer_cast<PrimitiveType>(field.type());
    if (auto position = positions.find(field.field_id()); position == positions.end()) {
      projected.push_back(Literal::Null(std::move(target_type)));
    } else {
      ICEBERG_ASSIGN_OR_RAISE(auto value, values.ValueAt(position->second));
      if (value.get().IsNull()) {
        projected.push_back(Literal::Null(std::move(target_type)));
      } else if (value.get().type()->type_id() == target_type->type_id()) {
        projected.push_back(value.get());
      } else {
        ICEBERG_ASSIGN_OR_RAISE(auto coerced, value.get().CastTo(target_type));
        projected.push_back(std::move(coerced));
      }
    }
  }
  return PartitionValues(std::move(projected));
}

Status AppendPartitionValues(ArrowArray* array, const StructType& partition_type,
                             const PartitionValues& values) {
  ICEBERG_PRECHECK(array != nullptr, "Partition Arrow array cannot be null");
  ICEBERG_PRECHECK(std::cmp_equal(array->n_children, partition_type.fields().size()),
                   "Partition Arrow array has {} fields but schema has {}",
                   array->n_children, partition_type.fields().size());
  ICEBERG_PRECHECK(values.num_fields() == partition_type.fields().size(),
                   "Partition has {} values but schema has {} fields",
                   values.num_fields(), partition_type.fields().size());

  for (size_t index = 0; index < values.num_fields(); ++index) {
    ICEBERG_ASSIGN_OR_RAISE(auto value, values.ValueAt(index));
    ICEBERG_RETURN_UNEXPECTED(AppendLiteral(array->children[index], value.get()));
  }
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(array));
  return {};
}

}  // namespace iceberg::internal
