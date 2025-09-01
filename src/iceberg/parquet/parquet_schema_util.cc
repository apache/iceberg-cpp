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

#include <charconv>

#include <arrow/extension_type.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/util/key_value_metadata.h>
#include <parquet/arrow/schema.h>
#include <parquet/schema.h>

#include "iceberg/constants.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/parquet/parquet_schema_util_internal.h"
#include "iceberg/result.h"
#include "iceberg/schema_util_internal.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/formatter.h"
#include "iceberg/util/macros.h"

namespace iceberg::parquet {

namespace {

std::optional<int32_t> FieldIdFromMetadata(
    const std::shared_ptr<const ::arrow::KeyValueMetadata>& metadata) {
  if (!metadata) {
    return std::nullopt;
  }
  int key = metadata->FindKey(kParquetFieldIdKey);
  if (key < 0) {
    return std::nullopt;
  }
  std::string field_id_str = metadata->value(key);
  int32_t field_id = -1;
  auto [_, ec] = std::from_chars(field_id_str.data(),
                                 field_id_str.data() + field_id_str.size(), field_id);
  if (ec != std::errc() || field_id < 0) {
    return std::nullopt;
  }
  return field_id;
}

std::optional<int32_t> GetFieldId(const ::parquet::arrow::SchemaField& parquet_field) {
  return FieldIdFromMetadata(parquet_field.field->metadata());
}

// TODO(gangwu): support v3 unknown type
Status ValidateParquetSchemaEvolution(
    const Type& expected_type, const ::parquet::arrow::SchemaField& parquet_field) {
  const auto& arrow_type = parquet_field.field->type();
  switch (expected_type.type_id()) {
    case TypeId::kBoolean:
      if (arrow_type->id() == ::arrow::Type::BOOL) {
        return {};
      }
      break;
    case TypeId::kInt:
      if (arrow_type->id() == ::arrow::Type::INT32) {
        return {};
      }
      break;
    case TypeId::kLong:
      if (arrow_type->id() == ::arrow::Type::INT64 ||
          arrow_type->id() == ::arrow::Type::INT32) {
        return {};
      }
      break;
    case TypeId::kFloat:
      if (arrow_type->id() == ::arrow::Type::FLOAT) {
        return {};
      }
      break;
    case TypeId::kDouble:
      if (arrow_type->id() == ::arrow::Type::DOUBLE ||
          arrow_type->id() == ::arrow::Type::FLOAT) {
        return {};
      }
      break;
    case TypeId::kDate:
      if (arrow_type->id() == ::arrow::Type::DATE32) {
        return {};
      }
      break;
    case TypeId::kTime:
      if (arrow_type->id() == ::arrow::Type::TIME64) {
        const auto& time_type =
            internal::checked_cast<const ::arrow::Time64Type&>(*arrow_type);
        if (time_type.unit() == ::arrow::TimeUnit::MICRO) {
          return {};
        }
      }
      break;
    case TypeId::kTimestamp:
      if (arrow_type->id() == ::arrow::Type::TIMESTAMP) {
        const auto& timestamp_type =
            internal::checked_cast<const ::arrow::TimestampType&>(*arrow_type);
        if (timestamp_type.unit() == ::arrow::TimeUnit::MICRO &&
            timestamp_type.timezone().empty()) {
          return {};
        }
      }
      break;
    case TypeId::kTimestampTz:
      if (arrow_type->id() == ::arrow::Type::TIMESTAMP) {
        const auto& timestamp_type =
            internal::checked_cast<const ::arrow::TimestampType&>(*arrow_type);
        if (timestamp_type.unit() == ::arrow::TimeUnit::MICRO &&
            !timestamp_type.timezone().empty()) {
          return {};
        }
      }
      break;
    case TypeId::kString:
      if (arrow_type->id() == ::arrow::Type::STRING) {
        return {};
      }
      break;
    case TypeId::kBinary:
      if (arrow_type->id() == ::arrow::Type::BINARY) {
        return {};
      }
      break;
    case TypeId::kDecimal:
      if (arrow_type->id() == ::arrow::Type::DECIMAL128) {
        const auto& decimal_type =
            internal::checked_cast<const DecimalType&>(expected_type);
        const auto& arrow_decimal =
            internal::checked_cast<const ::arrow::Decimal128Type&>(*arrow_type);
        if (decimal_type.scale() == arrow_decimal.scale() &&
            decimal_type.precision() >= arrow_decimal.precision()) {
          return {};
        }
      }
      break;
    case TypeId::kUuid:
      if (arrow_type->id() == ::arrow::Type::EXTENSION) {
        const auto& extension_type =
            internal::checked_cast<const ::arrow::ExtensionType&>(*arrow_type);
        if (extension_type.extension_name() == "arrow.uuid") {
          return {};
        }
      }
      break;
    case TypeId::kFixed:
      if (arrow_type->id() == ::arrow::Type::FIXED_SIZE_BINARY) {
        const auto& fixed_binary =
            internal::checked_cast<const ::arrow::FixedSizeBinaryType&>(*arrow_type);
        if (fixed_binary.byte_width() ==
            internal::checked_cast<const FixedType&>(expected_type).length()) {
          return {};
        }
      }
      break;
    case TypeId::kStruct:
      if (arrow_type->id() == ::arrow::Type::STRUCT) {
        return {};
      }
      break;
    case TypeId::kList:
      if (arrow_type->id() == ::arrow::Type::LIST) {
        return {};
      }
      break;
    case TypeId::kMap:
      if (arrow_type->id() == ::arrow::Type::MAP) {
        return {};
      }
      break;
    default:
      break;
  }

  return InvalidSchema("Cannot read Iceberg type: {} from Parquet type: {}",
                       expected_type, arrow_type->ToString());
}

// Forward declaration
Result<FieldProjection> ProjectNested(
    const Type& nested_type,
    const std::vector<::parquet::arrow::SchemaField>& parquet_fields);

Result<FieldProjection> ProjectStruct(
    const StructType& struct_type,
    const std::vector<::parquet::arrow::SchemaField>& parquet_fields) {
  struct FieldContext {
    size_t local_index;
    const ::parquet::arrow::SchemaField& parquet_field;
  };
  std::unordered_map<int32_t, FieldContext> field_context_map;
  field_context_map.reserve(parquet_fields.size());

  for (size_t i = 0; i < parquet_fields.size(); ++i) {
    const ::parquet::arrow::SchemaField& parquet_field = parquet_fields[i];
    auto field_id = GetFieldId(parquet_field);
    if (!field_id) {
      continue;
    }
    if (!field_context_map
             .emplace(field_id.value(),
                      FieldContext{.local_index = i, .parquet_field = parquet_field})
             .second) [[unlikely]] {
      return InvalidSchema("Duplicate field id {} found in Parquet schema",
                           field_id.value());
    }
  }

  FieldProjection result;
  result.children.reserve(struct_type.fields().size());

  for (const auto& field : struct_type.fields()) {
    int32_t field_id = field.field_id();
    FieldProjection child_projection;

    if (auto iter = field_context_map.find(field_id); iter != field_context_map.cend()) {
      const auto& parquet_field = iter->second.parquet_field;
      ICEBERG_RETURN_UNEXPECTED(
          ValidateParquetSchemaEvolution(*field.type(), parquet_field));
      if (field.type()->is_nested()) {
        ICEBERG_ASSIGN_OR_RAISE(child_projection,
                                ProjectNested(*field.type(), parquet_field.children));
      } else {
        child_projection.attributes =
            std::make_shared<ParquetExtraAttributes>(parquet_field.column_index);
      }
      child_projection.from = iter->second.local_index;
      child_projection.kind = FieldProjection::Kind::kProjected;
    } else if (MetadataColumns::IsMetadataColumn(field_id)) {
      child_projection.kind = FieldProjection::Kind::kMetadata;
    } else if (field.optional()) {
      child_projection.kind = FieldProjection::Kind::kNull;
    } else {
      return InvalidSchema("Missing required field with id: {}", field_id);
    }

    result.children.emplace_back(std::move(child_projection));
  }

  PruneFieldProjection(result);
  return result;
}

Result<FieldProjection> ProjectList(
    const ListType& list_type,
    const std::vector<::parquet::arrow::SchemaField>& parquet_fields) {
  if (parquet_fields.size() != 1) {
    return InvalidSchema("List type must have exactly one field, got {}",
                         parquet_fields.size());
  }

  const auto& parquet_field = parquet_fields.back();
  auto element_field_id = GetFieldId(parquet_field);
  if (!element_field_id) {
    return InvalidSchema("List element field missing field id");
  }

  const auto& element_field = list_type.fields().back();
  if (element_field.field_id() != element_field_id.value()) {
    return InvalidSchema("List element field id mismatch, expected {}, got {}",
                         element_field.field_id(), element_field_id.value());
  }

  ICEBERG_RETURN_UNEXPECTED(
      ValidateParquetSchemaEvolution(*element_field.type(), parquet_field));

  FieldProjection element_projection;
  if (element_field.type()->is_nested()) {
    ICEBERG_ASSIGN_OR_RAISE(element_projection,
                            ProjectNested(*element_field.type(), parquet_field.children));
  } else {
    element_projection.attributes =
        std::make_shared<ParquetExtraAttributes>(parquet_field.column_index);
  }

  element_projection.kind = FieldProjection::Kind::kProjected;
  element_projection.from = size_t{0};

  FieldProjection result;
  result.children.emplace_back(std::move(element_projection));
  return result;
}

Result<FieldProjection> ProjectMap(
    const MapType& map_type,
    const std::vector<::parquet::arrow::SchemaField>& parquet_fields) {
  if (parquet_fields.size() != 2) {
    return InvalidSchema("Map type must have exactly two fields, got {}",
                         parquet_fields.size());
  }

  auto key_field_id = GetFieldId(parquet_fields[0]);
  if (!key_field_id) {
    return InvalidSchema("Map key field missing field id");
  }
  auto value_field_id = GetFieldId(parquet_fields[1]);
  if (!value_field_id) {
    return InvalidSchema("Map value field missing field id");
  }

  const auto& key_field = map_type.key();
  const auto& value_field = map_type.value();
  if (key_field.field_id() != key_field_id.value()) {
    return InvalidSchema("Map key field id mismatch, expected {}, got {}",
                         key_field.field_id(), key_field_id.value());
  }
  if (value_field.field_id() != value_field_id.value()) {
    return InvalidSchema("Map value field id mismatch, expected {}, got {}",
                         value_field.field_id(), value_field_id.value());
  }

  FieldProjection result;
  result.children.reserve(2);

  for (size_t i = 0; i < parquet_fields.size(); ++i) {
    FieldProjection sub_projection;
    const auto& sub_node = parquet_fields[i];
    const auto& sub_field = map_type.fields()[i];
    ICEBERG_RETURN_UNEXPECTED(
        ValidateParquetSchemaEvolution(*sub_field.type(), sub_node));
    if (sub_field.type()->is_nested()) {
      ICEBERG_ASSIGN_OR_RAISE(sub_projection,
                              ProjectNested(*sub_field.type(), sub_node.children));
    } else {
      sub_projection.attributes =
          std::make_shared<ParquetExtraAttributes>(sub_node.column_index);
    }
    sub_projection.kind = FieldProjection::Kind::kProjected;
    sub_projection.from = i;
    result.children.emplace_back(std::move(sub_projection));
  }

  return result;
}

Result<FieldProjection> ProjectNested(
    const Type& nested_type,
    const std::vector<::parquet::arrow::SchemaField>& parquet_fields) {
  if (!nested_type.is_nested()) {
    return InvalidSchema("Expected a nested type, but got {}", nested_type);
  }

  switch (nested_type.type_id()) {
    case TypeId::kStruct:
      return ProjectStruct(internal::checked_cast<const StructType&>(nested_type),
                           parquet_fields);
    case TypeId::kList:
      return ProjectList(internal::checked_cast<const ListType&>(nested_type),
                         parquet_fields);
    case TypeId::kMap:
      if (parquet_fields.size() != 1 ||
          parquet_fields[0].field->type()->id() != ::arrow::Type::STRUCT ||
          parquet_fields[0].children.size() != 2) {
        return InvalidSchema(
            "Map type must have exactly one struct field with two children");
      }
      return ProjectMap(internal::checked_cast<const MapType&>(nested_type),
                        parquet_fields[0].children);
    default:
      return InvalidSchema("Unsupported nested type: {}", nested_type);
  }
}

void CollectColumnIds(const FieldProjection& field_projection,
                      std::vector<int32_t>* column_ids) {
  if (field_projection.attributes) {
    const auto& attributes = internal::checked_cast<const ParquetExtraAttributes&>(
        *field_projection.attributes);
    if (attributes.column_id) {
      column_ids->push_back(attributes.column_id.value());
    }
  }
  for (const auto& child : field_projection.children) {
    CollectColumnIds(child, column_ids);
  }
}

}  // namespace

Result<SchemaProjection> Project(const Schema& expected_schema,
                                 const ::parquet::arrow::SchemaManifest& parquet_schema) {
  ICEBERG_ASSIGN_OR_RAISE(auto field_projection,
                          ProjectNested(static_cast<const Type&>(expected_schema),
                                        parquet_schema.schema_fields));
  return SchemaProjection{std::move(field_projection.children)};
}

std::vector<int32_t> SelectedColumnIndices(const SchemaProjection& projection) {
  std::vector<int32_t> column_ids;
  for (const auto& field : projection.fields) {
    CollectColumnIds(field, &column_ids);
  }
  std::ranges::sort(column_ids);
  return column_ids;
}

bool HasFieldIds(const ::parquet::schema::NodePtr& node) {
  if (node->field_id() >= 0) {
    return true;
  }

  if (node->is_group()) {
    auto group_node = internal::checked_pointer_cast<::parquet::schema::GroupNode>(node);
    for (int i = 0; i < group_node->field_count(); i++) {
      if (HasFieldIds(group_node->field(i))) {
        return true;
      }
    }
  }

  return false;
}

}  // namespace iceberg::parquet
