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

#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_decimal.h>
#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/json/from_string.h>
#include <arrow/type.h>
#include <arrow/util/decimal.h>
#include <avro/Generic.hh>
#include <avro/Node.hh>
#include <avro/NodeImpl.hh>
#include <avro/Types.hh>

#include "iceberg/arrow/arrow_error_transform_internal.h"
#include "iceberg/avro/avro_data_util_internal.h"
#include "iceberg/avro/avro_schema_util_internal.h"
#include "iceberg/schema.h"
#include "iceberg/schema_util.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/macros.h"

namespace iceberg::avro {

using ::iceberg::arrow::ToErrorKind;

namespace {

/// \brief Forward declaration for mutual recursion.
Status AppendFieldToBuilder(const ::avro::NodePtr& avro_node,
                            const ::avro::GenericDatum& avro_datum,
                            const FieldProjection& projection,
                            const SchemaField& projected_field,
                            ::arrow::ArrayBuilder* array_builder);

/// \brief Append Avro record data to Arrow struct builder.
Status AppendStructToBuilder(const ::avro::NodePtr& avro_node,
                             const ::avro::GenericDatum& avro_datum,
                             const std::span<const FieldProjection>& projections,
                             const StructType& struct_type,
                             ::arrow::ArrayBuilder* array_builder) {
  if (avro_node->type() != ::avro::AVRO_RECORD) {
    return InvalidArgument("Expected Avro record, got type: {}", ToString(avro_node));
  }
  const auto& avro_record = avro_datum.value<::avro::GenericRecord>();

  auto* struct_builder = internal::checked_cast<::arrow::StructBuilder*>(array_builder);
  ICEBERG_ARROW_RETURN_NOT_OK(struct_builder->Append());

  for (size_t i = 0; i < projections.size(); ++i) {
    const auto& field_projection = projections[i];
    const auto& expected_field = struct_type.fields()[i];
    auto* field_builder = struct_builder->field_builder(static_cast<int>(i));

    if (field_projection.kind == FieldProjection::Kind::kProjected) {
      size_t avro_field_index =
          std::get<FieldProjection::SourceFieldIndex>(field_projection.from);
      if (avro_field_index >= avro_record.fieldCount()) {
        return InvalidArgument("Avro field index {} out of bound {}", avro_field_index,
                               avro_record.fieldCount());
      }

      const auto& avro_field_node = avro_node->leafAt(avro_field_index);
      const auto& avro_field_datum = avro_record.fieldAt(avro_field_index);
      ICEBERG_RETURN_UNEXPECTED(AppendFieldToBuilder(avro_field_node, avro_field_datum,
                                                     field_projection, expected_field,
                                                     field_builder));
    } else if (field_projection.kind == FieldProjection::Kind::kNull) {
      ICEBERG_ARROW_RETURN_NOT_OK(field_builder->AppendNull());
    } else {
      return NotImplemented("Unsupported field projection kind: {}",
                            ToString(field_projection.kind));
    }
  }
  return {};
}

/// \brief Append Avro array data to Arrow list builder.
Status AppendListToBuilder(const ::avro::NodePtr& avro_node,
                           const ::avro::GenericDatum& avro_datum,
                           const FieldProjection& element_projection,
                           const ListType& list_type,
                           ::arrow::ArrayBuilder* array_builder) {
  if (avro_node->type() != ::avro::AVRO_ARRAY) {
    return InvalidArgument("Expected Avro array, got type: {}", ToString(avro_node));
  }
  const auto& avro_array = avro_datum.value<::avro::GenericArray>();

  auto* list_builder = internal::checked_cast<::arrow::ListBuilder*>(array_builder);
  ICEBERG_ARROW_RETURN_NOT_OK(list_builder->Append());

  auto* value_builder = list_builder->value_builder();
  const auto& element_node = avro_node->leafAt(0);
  const auto& element_field = list_type.fields().back();

  for (const auto& element : avro_array.value()) {
    ICEBERG_RETURN_UNEXPECTED(AppendFieldToBuilder(
        element_node, element, element_projection, element_field, value_builder));
  }
  return {};
}

/// \brief Append Avro map data to Arrow map builder.
Status AppendMapToBuilder(const ::avro::NodePtr& avro_node,
                          const ::avro::GenericDatum& avro_datum,
                          const FieldProjection& key_projection,
                          const FieldProjection& value_projection,
                          const MapType& map_type, ::arrow::ArrayBuilder* array_builder) {
  auto* map_builder = internal::checked_cast<::arrow::MapBuilder*>(array_builder);

  if (avro_node->type() == ::avro::AVRO_MAP) {
    // Handle regular Avro map: map<string, value>
    const auto& avro_map = avro_datum.value<::avro::GenericMap>();
    const auto& map_entries = avro_map.value();

    const auto& key_node = avro_node->leafAt(0);
    const auto& value_node = avro_node->leafAt(1);

    const auto& key_field = map_type.key();
    const auto& value_field = map_type.value();

    ICEBERG_ARROW_RETURN_NOT_OK(map_builder->Append());
    auto* key_builder = map_builder->key_builder();
    auto* item_builder = map_builder->item_builder();

    for (const auto& entry : map_entries) {
      ICEBERG_RETURN_UNEXPECTED(AppendFieldToBuilder(
          key_node, entry.first, key_projection, key_field, key_builder));
      ICEBERG_RETURN_UNEXPECTED(AppendFieldToBuilder(
          value_node, entry.second, value_projection, value_field, item_builder));
    }

    return {};
  } else if (avro_node->type() == ::avro::AVRO_ARRAY && HasMapLogicalType(avro_node)) {
    // Handle array-based map: list<struct<key, value>>
    const auto& avro_array = avro_datum.value<::avro::GenericArray>();
    const auto& array_entries = avro_array.value();

    const auto& key_field = map_type.key();
    const auto& value_field = map_type.value();

    ICEBERG_ARROW_RETURN_NOT_OK(map_builder->Append());
    auto* key_builder = map_builder->key_builder();
    auto* item_builder = map_builder->item_builder();

    const auto& record_node = avro_node->leafAt(0);
    if (record_node->type() != ::avro::AVRO_RECORD || record_node->leaves() != 2) {
      return InvalidArgument(
          "Array-based map must contain records with exactly 2 fields, got: {}",
          ToString(record_node));
    }
    const auto& key_node = record_node->leafAt(0);
    const auto& value_node = record_node->leafAt(1);

    for (const auto& entry : array_entries) {
      const auto& record = entry.value<::avro::GenericRecord>();
      ICEBERG_RETURN_UNEXPECTED(AppendFieldToBuilder(
          key_node, record.fieldAt(0), key_projection, key_field, key_builder));
      ICEBERG_RETURN_UNEXPECTED(AppendFieldToBuilder(
          value_node, record.fieldAt(1), value_projection, value_field, item_builder));
    }

    return {};
  } else {
    return InvalidArgument("Expected Avro map or array with map logical type, got: {}",
                           ToString(avro_node));
  }
}

/// \brief Append nested Avro data to Arrow array builder based on type.
Status AppendNestedValueToBuilder(const ::avro::NodePtr& avro_node,
                                  const ::avro::GenericDatum& avro_datum,
                                  const std::span<const FieldProjection>& projections,
                                  const NestedType& projected_type,
                                  ::arrow::ArrayBuilder* array_builder) {
  switch (projected_type.type_id()) {
    case TypeId::kStruct: {
      const auto& struct_type = internal::checked_cast<const StructType&>(projected_type);
      return AppendStructToBuilder(avro_node, avro_datum, projections, struct_type,
                                   array_builder);
    }

    case TypeId::kList: {
      if (projections.size() != 1) {
        return InvalidArgument("Expected 1 projection for list, got: {}",
                               projections.size());
      }
      const auto& list_type = internal::checked_cast<const ListType&>(projected_type);
      return AppendListToBuilder(avro_node, avro_datum, projections[0], list_type,
                                 array_builder);
    }

    case TypeId::kMap: {
      if (projections.size() != 2) {
        return InvalidArgument("Expected 2 projections for map, got: {}",
                               projections.size());
      }
      const auto& map_type = internal::checked_cast<const MapType&>(projected_type);
      return AppendMapToBuilder(avro_node, avro_datum, projections[0], projections[1],
                                map_type, array_builder);
    }

    default:
      return InvalidArgument("Unsupported nested type: {}", projected_type.ToString());
  }
}

Status AppendPrimitiveValueToBuilder(const ::avro::NodePtr& avro_node,
                                     const ::avro::GenericDatum& avro_datum,
                                     const SchemaField& projected_field,
                                     ::arrow::ArrayBuilder* array_builder) {
  const auto& projected_type = *projected_field.type();
  if (!projected_type.is_primitive()) {
    return InvalidArgument("Expected primitive type, got: {}", projected_type.ToString());
  }

  switch (projected_type.type_id()) {
    case TypeId::kBoolean: {
      if (avro_node->type() != ::avro::AVRO_BOOL) {
        return InvalidArgument("Expected Avro boolean for boolean field, got: {}",
                               ToString(avro_node));
      }
      auto* builder = internal::checked_cast<::arrow::BooleanBuilder*>(array_builder);
      ICEBERG_ARROW_RETURN_NOT_OK(builder->Append(avro_datum.value<bool>()));
      return {};
    }

    case TypeId::kInt: {
      if (avro_node->type() != ::avro::AVRO_INT) {
        return InvalidArgument("Expected Avro int for int field, got: {}",
                               ToString(avro_node));
      }
      auto* builder = internal::checked_cast<::arrow::Int32Builder*>(array_builder);
      ICEBERG_ARROW_RETURN_NOT_OK(builder->Append(avro_datum.value<int32_t>()));
      return {};
    }

    case TypeId::kLong: {
      auto* builder = internal::checked_cast<::arrow::Int64Builder*>(array_builder);
      if (avro_node->type() == ::avro::AVRO_LONG) {
        ICEBERG_ARROW_RETURN_NOT_OK(builder->Append(avro_datum.value<int64_t>()));
      } else if (avro_node->type() == ::avro::AVRO_INT) {
        ICEBERG_ARROW_RETURN_NOT_OK(
            builder->Append(static_cast<int64_t>(avro_datum.value<int32_t>())));
      } else {
        return InvalidArgument("Expected Avro int/long for long field, got: {}",
                               ToString(avro_node));
      }
      return {};
    }

    case TypeId::kFloat: {
      if (avro_node->type() != ::avro::AVRO_FLOAT) {
        return InvalidArgument("Expected Avro float for float field, got: {}",
                               ToString(avro_node));
      }
      auto* builder = internal::checked_cast<::arrow::FloatBuilder*>(array_builder);
      ICEBERG_ARROW_RETURN_NOT_OK(builder->Append(avro_datum.value<float>()));
      return {};
    }

    case TypeId::kDouble: {
      auto* builder = internal::checked_cast<::arrow::DoubleBuilder*>(array_builder);
      if (avro_node->type() == ::avro::AVRO_DOUBLE) {
        ICEBERG_ARROW_RETURN_NOT_OK(builder->Append(avro_datum.value<double>()));
      } else if (avro_node->type() == ::avro::AVRO_FLOAT) {
        ICEBERG_ARROW_RETURN_NOT_OK(
            builder->Append(static_cast<double>(avro_datum.value<float>())));
      } else {
        return InvalidArgument("Expected Avro float/double for double field, got: {}",
                               ToString(avro_node));
      }
      return {};
    }

    case TypeId::kString: {
      if (avro_node->type() != ::avro::AVRO_STRING) {
        return InvalidArgument("Expected Avro string for string field, got: {}",
                               ToString(avro_node));
      }
      auto* builder = internal::checked_cast<::arrow::StringBuilder*>(array_builder);
      ICEBERG_ARROW_RETURN_NOT_OK(builder->Append(avro_datum.value<std::string>()));
      return {};
    }

    case TypeId::kBinary: {
      if (avro_node->type() != ::avro::AVRO_BYTES) {
        return InvalidArgument("Expected Avro bytes for binary field, got: {}",
                               ToString(avro_node));
      }
      auto* builder = internal::checked_cast<::arrow::BinaryBuilder*>(array_builder);
      const auto& bytes = avro_datum.value<std::vector<uint8_t>>();
      ICEBERG_ARROW_RETURN_NOT_OK(
          builder->Append(bytes.data(), static_cast<int32_t>(bytes.size())));
      return {};
    }

    case TypeId::kFixed: {
      if (avro_node->type() != ::avro::AVRO_FIXED) {
        return InvalidArgument("Expected Avro fixed for fixed field, got: {}",
                               ToString(avro_node));
      }
      const auto& fixed = avro_datum.value<::avro::GenericFixed>();
      const auto& fixed_type = internal::checked_cast<const FixedType&>(projected_type);

      if (static_cast<size_t>(fixed.value().size()) != fixed_type.length()) {
        return InvalidArgument("Expected Avro fixed[{}], got: {}", fixed_type.length(),
                               ToString(avro_node));
      }

      auto* builder =
          internal::checked_cast<::arrow::FixedSizeBinaryBuilder*>(array_builder);
      const auto& value = fixed.value();
      ICEBERG_ARROW_RETURN_NOT_OK(
          builder->Append(reinterpret_cast<const uint8_t*>(value.data())));
      return {};
    }

    case TypeId::kUuid: {
      if (avro_node->type() != ::avro::AVRO_FIXED ||
          avro_node->logicalType().type() != ::avro::LogicalType::UUID) {
        return InvalidArgument("Expected Avro fixed for uuid field, got: {}",
                               ToString(avro_node));
      }

      auto* builder =
          internal::checked_cast<::arrow::FixedSizeBinaryBuilder*>(array_builder);
      const auto& fixed = avro_datum.value<::avro::GenericFixed>();
      if (fixed.value().size() != 16) {
        return InvalidArgument("Expected UUID fixed length 16, got: {}",
                               fixed.value().size());
      }
      const auto& value = fixed.value();
      ICEBERG_ARROW_RETURN_NOT_OK(
          builder->Append(reinterpret_cast<const uint8_t*>(value.data())));
      return {};
    }

    case TypeId::kDecimal: {
      if (avro_node->type() != ::avro::AVRO_FIXED ||
          avro_node->logicalType().type() != ::avro::LogicalType::DECIMAL) {
        return InvalidArgument(
            "Expected Avro fixed with decimal logical type for decimal field, got: {}",
            ToString(avro_node));
      }

      const auto& fixed = avro_datum.value<::avro::GenericFixed>();
      const auto& value = fixed.value();
      ICEBERG_ARROW_ASSIGN_OR_RETURN(
          auto decimal, ::arrow::Decimal128::FromBigEndian(value.data(), value.size()));
      auto* builder = internal::checked_cast<::arrow::Decimal128Builder*>(array_builder);
      ICEBERG_ARROW_RETURN_NOT_OK(builder->Append(decimal));
      return {};
    }

    case TypeId::kDate: {
      if (avro_node->type() != ::avro::AVRO_INT ||
          avro_node->logicalType().type() != ::avro::LogicalType::DATE) {
        return InvalidArgument(
            "Expected Avro int with DATE logical type for date field, got: {}",
            ToString(avro_node));
      }
      auto* builder = internal::checked_cast<::arrow::Date32Builder*>(array_builder);
      ICEBERG_ARROW_RETURN_NOT_OK(builder->Append(avro_datum.value<int32_t>()));
      return {};
    }

    case TypeId::kTime: {
      if (avro_node->type() != ::avro::AVRO_LONG ||
          avro_node->logicalType().type() != ::avro::LogicalType::TIME_MICROS) {
        return InvalidArgument(
            "Expected Avro long with TIME_MICROS for time field, got: {}",
            ToString(avro_node));
      }
      auto* builder = internal::checked_cast<::arrow::Time64Builder*>(array_builder);
      ICEBERG_ARROW_RETURN_NOT_OK(builder->Append(avro_datum.value<int64_t>()));
      return {};
    }

    case TypeId::kTimestamp:
    case TypeId::kTimestampTz: {
      if (avro_node->type() != ::avro::AVRO_LONG ||
          avro_node->logicalType().type() != ::avro::LogicalType::TIMESTAMP_MICROS) {
        return InvalidArgument(
            "Expected Avro long with TIMESTAMP_MICROS for timestamp field, got: {}",
            ToString(avro_node));
      }
      auto* builder = internal::checked_cast<::arrow::TimestampBuilder*>(array_builder);
      ICEBERG_ARROW_RETURN_NOT_OK(builder->Append(avro_datum.value<int64_t>()));
      return {};
    }

    default:
      return InvalidArgument("Unsupported primitive type {} to append avro node {}",
                             projected_field.type()->ToString(), ToString(avro_node));
  }
}

/// \brief Dispatch to appropriate handlers based on the projection kind.
Status AppendFieldToBuilder(const ::avro::NodePtr& avro_node,
                            const ::avro::GenericDatum& avro_datum,
                            const FieldProjection& projection,
                            const SchemaField& projected_field,
                            ::arrow::ArrayBuilder* array_builder) {
  if (avro_node->type() == ::avro::AVRO_UNION) {
    const auto& union_datum = avro_datum.value<::avro::GenericUnion>();
    size_t branch = union_datum.currentBranch();
    if (avro_node->leafAt(branch)->type() == ::avro::AVRO_NULL) {
      ICEBERG_ARROW_RETURN_NOT_OK(array_builder->AppendNull());
      return {};
    } else {
      return AppendFieldToBuilder(avro_node->leafAt(branch), union_datum.datum(),
                                  projection, projected_field, array_builder);
    }
  }

  const auto& projected_type = *projected_field.type();
  if (projected_type.is_primitive()) {
    return AppendPrimitiveValueToBuilder(avro_node, avro_datum, projected_field,
                                         array_builder);
  } else {
    const auto& nested_type = internal::checked_cast<const NestedType&>(projected_type);
    return AppendNestedValueToBuilder(avro_node, avro_datum, projection.children,
                                      nested_type, array_builder);
  }
}

}  // namespace

Status AppendDatumToBuilder(const ::avro::NodePtr& avro_node,
                            const ::avro::GenericDatum& avro_datum,
                            const SchemaProjection& projection,
                            const Schema& projected_schema,
                            ::arrow::ArrayBuilder* array_builder) {
  return AppendNestedValueToBuilder(avro_node, avro_datum, projection.fields,
                                    projected_schema, array_builder);
}

}  // namespace iceberg::avro
