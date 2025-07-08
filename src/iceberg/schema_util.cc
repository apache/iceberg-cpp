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

#include "iceberg/schema_util.h"

#include <format>
#include <string_view>
#include <unordered_map>
#include <utility>

#include "iceberg/metadata_columns.h"
#include "iceberg/schema.h"
#include "iceberg/schema_util_internal.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/formatter_internal.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

Status ValidateSchemaEvolution(const Type& expected_type, const Type& source_type) {
  if (expected_type.is_nested()) {
    // Nested type requires identical type ids but their sub-fields are checked
    // recursively and individually.
    if (source_type.type_id() != expected_type.type_id()) {
      return NotSupported("Cannot read {} from {}", expected_type, source_type);
    }
    return {};
  }

  // Short cut for same primitive type.
  if (expected_type == source_type) {
    return {};
  }

  switch (expected_type.type_id()) {
    case TypeId::kLong: {
      if (source_type.type_id() == TypeId::kInt) {
        return {};
      }
    } break;
    case TypeId::kDouble: {
      if (source_type.type_id() == TypeId::kFloat) {
        return {};
      }
    } break;
    case TypeId::kDecimal: {
      if (source_type.type_id() == TypeId::kDecimal) {
        const auto& expected_decimal =
            internal::checked_cast<const DecimalType&>(expected_type);
        const auto& source_decimal =
            internal::checked_cast<const DecimalType&>(source_type);
        if (expected_decimal.precision() >= source_decimal.precision() &&
            expected_decimal.scale() == source_decimal.scale()) {
          return {};
        }
      }
    } break;
    default:
      break;
  }
  return NotSupported("Cannot read {} from {}", expected_type, source_type);
}

Result<FieldProjection> ProjectNested(const Type& expected_type, const Type& source_type,
                                      bool prune_source) {
  if (!expected_type.is_nested()) {
    return InvalidSchema("Expected a nested type, but got {}", expected_type);
  }
  if (expected_type.type_id() != source_type.type_id()) {
    return InvalidSchema("Expected {}, but got {}", expected_type, source_type);
  }

  const auto& expected_fields =
      internal::checked_cast<const NestedType&>(expected_type).fields();
  const auto& source_fields =
      internal::checked_cast<const NestedType&>(source_type).fields();

  // Build a map from field id to source field info including its local offset in
  // the current nesting level.
  struct SourceFieldInfo {
    size_t local_index;
    const SchemaField* field;
  };
  std::unordered_map<int32_t, SourceFieldInfo> source_field_map;
  source_field_map.reserve(source_fields.size());
  for (size_t i = 0; i < source_fields.size(); ++i) {
    const auto& field = source_fields[i];
    if (const auto [iter, inserted] = source_field_map.emplace(
            std::piecewise_construct, std::forward_as_tuple(field.field_id()),
            std::forward_as_tuple(i, &field));
        !inserted) [[unlikely]] {
      return InvalidSchema("Duplicate field id found, prev: {}, curr: {}",
                           *iter->second.field, field);
    }
  }

  FieldProjection result;
  result.children.reserve(expected_fields.size());

  for (const auto& expected_field : expected_fields) {
    int32_t field_id = expected_field.field_id();
    FieldProjection child_projection;

    if (auto iter = source_field_map.find(field_id); iter != source_field_map.cend()) {
      if (expected_field.type()->is_nested()) {
        ICEBERG_ASSIGN_OR_RAISE(child_projection,
                                ProjectNested(*expected_field.type(),
                                              *iter->second.field->type(), prune_source));
      } else {
        ICEBERG_RETURN_UNEXPECTED(
            ValidateSchemaEvolution(*expected_field.type(), *iter->second.field->type()));
      }
      // If `prune_source` is false, all fields will be read so the local index
      // is exactly the position to read data. Otherwise, the local index is computed
      // by pruning all non-projected fields
      child_projection.from = iter->second.local_index;
      child_projection.kind = FieldProjection::Kind::kProjected;
    } else if (MetadataColumns::IsMetadataColumn(field_id)) {
      child_projection.kind = FieldProjection::Kind::kMetadata;
    } else if (expected_field.optional()) {
      child_projection.kind = FieldProjection::Kind::kNull;
    } else {
      // TODO(gangwu): support default value for v3 and constant value
      return InvalidSchema("Missing required field: {}", expected_field.ToString());
    }
    result.children.emplace_back(std::move(child_projection));
  }

  if (prune_source) {
    PruneFieldProjection(result);
  }

  return result;
}

}  // namespace

Result<SchemaProjection> Project(const Schema& expected_schema,
                                 const Schema& source_schema, bool prune_source) {
  ICEBERG_ASSIGN_OR_RAISE(auto field_projection,
                          ProjectNested(expected_schema, source_schema, prune_source));
  return SchemaProjection{std::move(field_projection.children)};
}

std::string_view ToString(FieldProjection::Kind kind) {
  switch (kind) {
    case FieldProjection::Kind::kProjected:
      return "projected";
    case FieldProjection::Kind::kMetadata:
      return "metadata";
    case FieldProjection::Kind::kConstant:
      return "constant";
    case FieldProjection::Kind::kDefault:
      return "default";
    case FieldProjection::Kind::kNull:
      return "null";
  }
  std::unreachable();
}

std::string ToString(const FieldProjection& projection) {
  std::string repr = std::format("FieldProjection(kind={}", projection.kind);
  if (projection.kind == FieldProjection::Kind::kProjected) {
    std::format_to(std::back_inserter(repr), ", from={}", std::get<1>(projection.from));
  }
  if (!projection.children.empty()) {
    std::format_to(std::back_inserter(repr), ", children={}",
                   FormatRange(projection.children, ", ", "[", "]"));
  }
  std::format_to(std::back_inserter(repr), ")");
  return repr;
}

std::string ToString(const SchemaProjection& projection) {
  return std::format("{}", FormatRange(projection.fields, "\n", "", ""));
}

}  // namespace iceberg
