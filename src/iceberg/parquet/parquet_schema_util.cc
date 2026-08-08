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
#include "iceberg/schema_internal.h"
#include "iceberg/schema_util_internal.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep
#include "iceberg/util/macros.h"
#include "iceberg/util/string_util.h"

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
  auto res = StringUtils::ParseNumber<int32_t>(field_id_str);
  if (!res.has_value() || res.value() < 0) {
    return std::nullopt;
  }
  return res.value();
}

std::optional<int32_t> GetFieldId(const ::parquet::arrow::SchemaField& parquet_field) {
  return FieldIdFromMetadata(parquet_field.field->metadata());
}

Result<::parquet::LogicalType::EdgeInterpolationAlgorithm> ToParquetAlgorithm(
    EdgeAlgorithm algorithm) {
  using ParquetAlgorithm = ::parquet::LogicalType::EdgeInterpolationAlgorithm;
  switch (algorithm) {
    case EdgeAlgorithm::kSpherical:
      return ParquetAlgorithm::SPHERICAL;
    case EdgeAlgorithm::kVincenty:
      return ParquetAlgorithm::VINCENTY;
    case EdgeAlgorithm::kThomas:
      return ParquetAlgorithm::THOMAS;
    case EdgeAlgorithm::kAndoyer:
      return ParquetAlgorithm::ANDOYER;
    case EdgeAlgorithm::kKarney:
      return ParquetAlgorithm::KARNEY;
  }
  return InvalidArgument("Unknown Iceberg edge algorithm");
}

Result<::parquet::schema::NodePtr> ToParquetNode(const SchemaField& field);

Result<::parquet::schema::NodePtr> ToParquetPrimitive(
    const PrimitiveType& primitive, std::string name,
    ::parquet::Repetition::type repetition, int32_t field_id) {
  using ParquetType = ::parquet::Type;
  using TimeUnit = ::parquet::LogicalType::TimeUnit;

  auto logical_type = ::parquet::LogicalType::None();
  auto physical_type = ParquetType::UNDEFINED;
  int32_t type_length = -1;

  switch (primitive.type_id()) {
    case TypeId::kBoolean:
      physical_type = ParquetType::BOOLEAN;
      break;
    case TypeId::kInt:
      physical_type = ParquetType::INT32;
      break;
    case TypeId::kLong:
      physical_type = ParquetType::INT64;
      break;
    case TypeId::kFloat:
      physical_type = ParquetType::FLOAT;
      break;
    case TypeId::kDouble:
      physical_type = ParquetType::DOUBLE;
      break;
    case TypeId::kDecimal: {
      const auto& decimal = internal::checked_cast<const DecimalType&>(primitive);
      if (decimal.precision() <= 9) {
        physical_type = ParquetType::INT32;
      } else if (decimal.precision() <= 18) {
        physical_type = ParquetType::INT64;
      } else {
        physical_type = ParquetType::FIXED_LEN_BYTE_ARRAY;
        type_length = ::arrow::DecimalType::DecimalSize(decimal.precision());
      }
      logical_type =
          ::parquet::LogicalType::Decimal(decimal.precision(), decimal.scale());
    } break;
    case TypeId::kDate:
      physical_type = ParquetType::INT32;
      logical_type = ::parquet::LogicalType::Date();
      break;
    case TypeId::kTime:
      physical_type = ParquetType::INT64;
      logical_type =
          ::parquet::LogicalType::Time(/*is_adjusted_to_utc=*/false, TimeUnit::MICROS);
      break;
    case TypeId::kTimestamp:
      physical_type = ParquetType::INT64;
      logical_type = ::parquet::LogicalType::Timestamp(/*is_adjusted_to_utc=*/false,
                                                       TimeUnit::MICROS);
      break;
    case TypeId::kTimestampTz:
      physical_type = ParquetType::INT64;
      logical_type = ::parquet::LogicalType::Timestamp(/*is_adjusted_to_utc=*/true,
                                                       TimeUnit::MICROS);
      break;
    case TypeId::kTimestampNs:
      physical_type = ParquetType::INT64;
      logical_type = ::parquet::LogicalType::Timestamp(/*is_adjusted_to_utc=*/false,
                                                       TimeUnit::NANOS);
      break;
    case TypeId::kTimestampTzNs:
      physical_type = ParquetType::INT64;
      logical_type =
          ::parquet::LogicalType::Timestamp(/*is_adjusted_to_utc=*/true, TimeUnit::NANOS);
      break;
    case TypeId::kString:
      physical_type = ParquetType::BYTE_ARRAY;
      logical_type = ::parquet::LogicalType::String();
      break;
    case TypeId::kUuid:
      physical_type = ParquetType::FIXED_LEN_BYTE_ARRAY;
      type_length = 16;
      logical_type = ::parquet::LogicalType::UUID();
      break;
    case TypeId::kFixed: {
      const auto& fixed = internal::checked_cast<const FixedType&>(primitive);
      physical_type = ParquetType::FIXED_LEN_BYTE_ARRAY;
      type_length = fixed.length();
    } break;
    case TypeId::kBinary:
      physical_type = ParquetType::BYTE_ARRAY;
      break;
    case TypeId::kGeometry: {
      const auto& geometry = internal::checked_cast<const GeometryType&>(primitive);
      physical_type = ParquetType::BYTE_ARRAY;
      logical_type = ::parquet::LogicalType::Geometry(std::string(geometry.crs()));
    } break;
    case TypeId::kGeography: {
      const auto& geography = internal::checked_cast<const GeographyType&>(primitive);
      ICEBERG_ASSIGN_OR_RAISE(auto algorithm, ToParquetAlgorithm(geography.algorithm()));
      physical_type = ParquetType::BYTE_ARRAY;
      logical_type =
          ::parquet::LogicalType::Geography(std::string(geography.crs()), algorithm);
    } break;
    case TypeId::kUnknown:
      if (repetition != ::parquet::Repetition::OPTIONAL) {
        return InvalidSchema("Iceberg unknown type must be optional");
      }
      physical_type = ParquetType::INT32;
      logical_type = ::parquet::LogicalType::Null();
      break;
    case TypeId::kVariant:
    case TypeId::kStruct:
    case TypeId::kList:
    case TypeId::kMap:
      return NotSupported("Cannot write Iceberg type {} to Parquet", primitive);
  }

  return ::parquet::schema::PrimitiveNode::Make(name, repetition, std::move(logical_type),
                                                physical_type, type_length, field_id);
}

Result<::parquet::schema::NodePtr> ToParquetNode(const SchemaField& field) {
  const auto repetition = field.optional() ? ::parquet::Repetition::OPTIONAL
                                           : ::parquet::Repetition::REQUIRED;
  const auto& type = *field.type();
  if (type.is_primitive()) {
    return ToParquetPrimitive(internal::checked_cast<const PrimitiveType&>(type),
                              std::string(field.name()), repetition, field.field_id());
  }

  switch (type.type_id()) {
    case TypeId::kStruct: {
      const auto& struct_type = internal::checked_cast<const StructType&>(type);
      if (struct_type.fields().empty()) {
        return NotImplemented("Cannot write empty struct '{}' to Parquet", field.name());
      }
      ::parquet::schema::NodeVector children;
      children.reserve(struct_type.fields().size());
      for (const auto& child_field : struct_type.fields()) {
        ICEBERG_ASSIGN_OR_RAISE(auto child, ToParquetNode(child_field));
        children.push_back(std::move(child));
      }
      return ::parquet::schema::GroupNode::Make(
          std::string(field.name()), repetition, std::move(children),
          /*logical_type=*/nullptr, field.field_id());
    }
    case TypeId::kList: {
      const auto& list_type = internal::checked_cast<const ListType&>(type);
      ICEBERG_ASSIGN_OR_RAISE(auto element, ToParquetNode(list_type.element()));
      auto repeated = ::parquet::schema::GroupNode::Make(
          "list", ::parquet::Repetition::REPEATED, {std::move(element)});
      return ::parquet::schema::GroupNode::Make(
          std::string(field.name()), repetition, {std::move(repeated)},
          ::parquet::LogicalType::List(), field.field_id());
    }
    case TypeId::kMap: {
      const auto& map_type = internal::checked_cast<const MapType&>(type);
      ICEBERG_ASSIGN_OR_RAISE(auto key, ToParquetNode(map_type.key()));
      ICEBERG_ASSIGN_OR_RAISE(auto value, ToParquetNode(map_type.value()));
      auto repeated =
          ::parquet::schema::GroupNode::Make("key_value", ::parquet::Repetition::REPEATED,
                                             {std::move(key), std::move(value)});
      return ::parquet::schema::GroupNode::Make(
          std::string(field.name()), repetition, {std::move(repeated)},
          ::parquet::LogicalType::Map(), field.field_id());
    }
    case TypeId::kVariant:
      return NotSupported("Cannot write Iceberg variant type to Parquet");
    default:
      return InvalidSchema("Expected nested Iceberg type, got {}", type);
  }
}

bool IsNullPhysicalField(const ::parquet::arrow::SchemaField& parquet_field) {
  return parquet_field.field->type()->id() == ::arrow::Type::NA;
}

bool HasSelectedColumn(const FieldProjection& projection) {
  if (projection.attributes) {
    const auto& attributes =
        internal::checked_cast<const ParquetExtraAttributes&>(*projection.attributes);
    if (attributes.column_id) {
      return true;
    }
  }
  return std::ranges::any_of(projection.children, HasSelectedColumn);
}

std::optional<int32_t> FirstColumnIndex(
    const ::parquet::arrow::SchemaField& parquet_field) {
  if (parquet_field.column_index >= 0) {
    return parquet_field.column_index;
  }
  for (const auto& child : parquet_field.children) {
    if (auto column_index = FirstColumnIndex(child)) {
      return column_index;
    }
  }
  return std::nullopt;
}

// Pick a physical column as an anchor when all children are null-projected,
// so that Parquet readers can still locate row boundaries.
void SelectAnchorColumnIfEmpty(
    FieldProjection* projection,
    const std::vector<::parquet::arrow::SchemaField>& parquet_fields) {
  if (HasSelectedColumn(*projection)) {
    return;
  }
  for (const auto& parquet_field : parquet_fields) {
    if (auto column_index = FirstColumnIndex(parquet_field)) {
      projection->attributes =
          std::make_shared<ParquetExtraAttributes>(column_index.value());
      return;
    }
  }
}

Status ValidateGeospatialParquetType(const Type& expected_type,
                                     const ::parquet::arrow::SchemaField& parquet_field,
                                     const ::parquet::ColumnDescriptor& descr) {
  if (parquet_field.field->type()->id() != ::arrow::Type::BINARY) {
    return InvalidSchema("Cannot read Iceberg type {} from non-binary Parquet field",
                         expected_type);
  }
  if (descr.logical_type() == nullptr) {
    return InvalidSchema("Missing Parquet leaf column metadata for Iceberg type {}",
                         expected_type);
  }
  if (descr.physical_type() != ::parquet::Type::BYTE_ARRAY) {
    return InvalidSchema("Iceberg type {} requires Parquet BYTE_ARRAY", expected_type);
  }
  if (expected_type.type_id() == TypeId::kGeometry) {
    if (!descr.logical_type()->is_geometry()) {
      return InvalidSchema("Iceberg geometry requires Parquet Geometry logical type");
    }
    return {};
  }
  if (!descr.logical_type()->is_geography()) {
    return InvalidSchema("Iceberg geography requires Parquet Geography logical type");
  }
  return {};
}

}  // namespace

namespace {

Status ValidateParquetTypeCompatibility(
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
    case TypeId::kTimestampNs:
      if (arrow_type->id() == ::arrow::Type::TIMESTAMP) {
        const auto& timestamp_type =
            internal::checked_cast<const ::arrow::TimestampType&>(*arrow_type);
        if (timestamp_type.unit() == ::arrow::TimeUnit::NANO &&
            timestamp_type.timezone().empty()) {
          return {};
        }
      }
      break;
    case TypeId::kTimestampTzNs:
      if (arrow_type->id() == ::arrow::Type::TIMESTAMP) {
        const auto& timestamp_type =
            internal::checked_cast<const ::arrow::TimestampType&>(*arrow_type);
        if (timestamp_type.unit() == ::arrow::TimeUnit::NANO &&
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
        if (extension_type.extension_name() == kArrowUuidExtensionName) {
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
    case TypeId::kUnknown:
      return {};
    case TypeId::kVariant:
      return NotSupported("Reading Iceberg type {} from Parquet is not supported",
                          expected_type);
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

}  // namespace

namespace {

class ProjectionBuilder {
 public:
  explicit ProjectionBuilder(const ::parquet::SchemaDescriptor* descr) : descr_(descr) {}

  Result<FieldProjection> Project(
      const Type& nested_type,
      const std::vector<::parquet::arrow::SchemaField>& parquet_fields) {
    return ProjectNested(nested_type, parquet_fields);
  }

 private:
  Result<FieldProjection> ProjectField(const SchemaField& expected_field,
                                       const ::parquet::arrow::SchemaField& parquet_field,
                                       size_t source_index) {
    const Type& expected_type = *expected_field.type();

    FieldProjection projection;
    if (expected_type.type_id() == TypeId::kUnknown ||
        IsNullPhysicalField(parquet_field)) {
      if (!expected_field.optional()) {
        return InvalidSchema("Cannot project required field with id {} as null",
                             expected_field.field_id());
      }
      projection.kind = FieldProjection::Kind::kNull;
      return projection;
    }

    if (expected_type.type_id() == TypeId::kGeometry ||
        expected_type.type_id() == TypeId::kGeography) {
      if (descr_ == nullptr || parquet_field.column_index < 0 ||
          parquet_field.column_index >= descr_->num_columns()) {
        return InvalidSchema("Missing Parquet leaf column metadata for Iceberg type {}",
                             expected_type);
      }
      ICEBERG_RETURN_UNEXPECTED(ValidateGeospatialParquetType(
          expected_type, parquet_field, *descr_->Column(parquet_field.column_index)));
    } else {
      ICEBERG_RETURN_UNEXPECTED(
          ValidateParquetTypeCompatibility(expected_type, parquet_field));
    }

    if (expected_type.is_nested()) {
      ICEBERG_ASSIGN_OR_RAISE(projection,
                              ProjectNested(expected_type, parquet_field.children));
    } else {
      projection.attributes =
          std::make_shared<ParquetExtraAttributes>(parquet_field.column_index);
    }
    projection.from = source_index;
    projection.kind = FieldProjection::Kind::kProjected;
    return projection;
  }

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

      if (auto iter = field_context_map.find(field_id);
          iter != field_context_map.cend()) {
        const auto& parquet_field = iter->second.parquet_field;
        ICEBERG_ASSIGN_OR_RAISE(child_projection, ProjectField(field, parquet_field,
                                                               iter->second.local_index));
      } else if (MetadataColumns::IsMetadataColumn(field_id)) {
        child_projection.kind = FieldProjection::Kind::kMetadata;
      } else if (field.initial_default() != nullptr) {
        // Rows written before the field existed assume its `initial-default` value.
        child_projection.kind = FieldProjection::Kind::kDefault;
        child_projection.from = *field.initial_default();
      } else if (field.optional()) {
        child_projection.kind = FieldProjection::Kind::kNull;
      } else {
        return InvalidSchema("Missing required field with id: {}", field_id);
      }

      result.children.emplace_back(std::move(child_projection));
    }

    SelectAnchorColumnIfEmpty(&result, parquet_fields);
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

    ICEBERG_ASSIGN_OR_RAISE(auto element_projection,
                            ProjectField(element_field, parquet_field, size_t{0}));

    FieldProjection result;
    result.children.emplace_back(std::move(element_projection));
    SelectAnchorColumnIfEmpty(&result, parquet_fields);
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
      const auto& sub_node = parquet_fields[i];
      const auto& sub_field = map_type.fields()[i];
      ICEBERG_ASSIGN_OR_RAISE(auto sub_projection, ProjectField(sub_field, sub_node, i));
      if (sub_projection.kind == FieldProjection::Kind::kNull &&
          !HasSelectedColumn(sub_projection)) {
        if (auto column_index = FirstColumnIndex(sub_node)) {
          sub_projection.attributes =
              std::make_shared<ParquetExtraAttributes>(column_index.value());
        }
      }
      result.children.emplace_back(std::move(sub_projection));
    }

    SelectAnchorColumnIfEmpty(&result, parquet_fields);
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

  const ::parquet::SchemaDescriptor* descr_;
};

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
  ProjectionBuilder builder(parquet_schema.descr);
  ICEBERG_ASSIGN_OR_RAISE(auto field_projection,
                          builder.Project(static_cast<const Type&>(expected_schema),
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

Result<std::shared_ptr<::parquet::SchemaDescriptor>> ToParquetSchema(
    const Schema& iceberg_schema) {
  ::parquet::schema::NodeVector fields;
  fields.reserve(iceberg_schema.fields().size());
  for (const auto& field : iceberg_schema.fields()) {
    ICEBERG_ASSIGN_OR_RAISE(auto parquet_field, ToParquetNode(field));
    fields.push_back(std::move(parquet_field));
  }

  auto root = ::parquet::schema::GroupNode::Make(
      "schema", ::parquet::Repetition::REQUIRED, std::move(fields));
  auto parquet_schema = std::make_shared<::parquet::SchemaDescriptor>();
  parquet_schema->Init(std::move(root));
  return parquet_schema;
}

}  // namespace iceberg::parquet
