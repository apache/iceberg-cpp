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
#include <bit>
#include <cstring>
#include <span>

#include <avro/Generic.hh>
#include <avro/Node.hh>
#include <avro/NodeImpl.hh>
#include <avro/Types.hh>
#include <nanoarrow/nanoarrow.h>

#include "iceberg/arrow_row_builder_internal.h"
#include "iceberg/avro/avro_data_util_internal.h"
#include "iceberg/avro/avro_schema_util_internal.h"
#include "iceberg/expression/literal.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/nanoarrow_status_internal.h"
#include "iceberg/schema.h"
#include "iceberg/schema_util.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/int128.h"
#include "iceberg/util/macros.h"

namespace iceberg::avro {

namespace {

/// \brief Forward declaration for mutual recursion.
Status AppendFieldToBuilder(const ::avro::NodePtr& avro_node,
                            const ::avro::GenericDatum& avro_datum,
                            const FieldProjection& projection,
                            const SchemaField& projected_field,
                            const MetadataColumnContext& metadata_context,
                            ArrowArray* array_builder);

/// \brief Append Avro record data to a nanoarrow struct builder.
Status AppendStructToBuilder(const ::avro::NodePtr& avro_node,
                             const ::avro::GenericDatum& avro_datum,
                             const std::span<const FieldProjection>& projections,
                             const StructType& struct_type,
                             const MetadataColumnContext& metadata_context,
                             ArrowArray* array_builder) {
  if (avro_node->type() != ::avro::AVRO_RECORD) {
    return InvalidArgument("Expected Avro record, got type: {}", ToString(avro_node));
  }
  const auto& avro_record = avro_datum.value<::avro::GenericRecord>();

  for (size_t i = 0; i < projections.size(); ++i) {
    const auto& field_projection = projections[i];
    const auto& expected_field = struct_type.fields()[i];
    auto* field_builder = array_builder->children[i];

    if (field_projection.kind == FieldProjection::Kind::kProjected) {
      size_t avro_field_index = std::get<size_t>(field_projection.from);
      if (avro_field_index >= avro_record.fieldCount()) {
        return InvalidArgument("Avro field index {} out of bound {}", avro_field_index,
                               avro_record.fieldCount());
      }

      const auto& avro_field_node = avro_node->leafAt(avro_field_index);
      const auto& avro_field_datum = avro_record.fieldAt(avro_field_index);
      ICEBERG_RETURN_UNEXPECTED(AppendFieldToBuilder(avro_field_node, avro_field_datum,
                                                     field_projection, expected_field,
                                                     metadata_context, field_builder));
    } else if (field_projection.kind == FieldProjection::Kind::kNull) {
      ICEBERG_RETURN_UNEXPECTED(AppendNull(field_builder));
    } else if (field_projection.kind == FieldProjection::Kind::kDefault) {
      ICEBERG_RETURN_UNEXPECTED(AppendDefaultToBuilder(field_projection, field_builder));
    } else if (field_projection.kind == FieldProjection::Kind::kMetadata) {
      ICEBERG_RETURN_UNEXPECTED(AppendMetadataToBuilder(expected_field.field_id(),
                                                        metadata_context, field_builder));
    } else {
      return NotImplemented("Unsupported field projection kind: {}",
                            ToString(field_projection.kind));
    }
  }
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(array_builder));
  return {};
}

/// \brief Append Avro array data to a nanoarrow list builder.
Status AppendListToBuilder(const ::avro::NodePtr& avro_node,
                           const ::avro::GenericDatum& avro_datum,
                           const FieldProjection& element_projection,
                           const ListType& list_type,
                           const MetadataColumnContext& metadata_context,
                           ArrowArray* array_builder) {
  if (avro_node->type() != ::avro::AVRO_ARRAY) {
    return InvalidArgument("Expected Avro array, got type: {}", ToString(avro_node));
  }
  const auto& avro_array = avro_datum.value<::avro::GenericArray>();

  auto* value_builder = array_builder->children[0];
  const auto& element_node = avro_node->leafAt(0);
  const auto& element_field = list_type.fields().back();

  for (const auto& element : avro_array.value()) {
    ICEBERG_RETURN_UNEXPECTED(AppendFieldToBuilder(element_node, element,
                                                   element_projection, element_field,
                                                   metadata_context, value_builder));
  }
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(array_builder));
  return {};
}

/// \brief Append Avro map data to a nanoarrow map builder.
Status AppendMapToBuilder(const ::avro::NodePtr& avro_node,
                          const ::avro::GenericDatum& avro_datum,
                          const FieldProjection& key_projection,
                          const FieldProjection& value_projection,
                          const MapType& map_type,
                          const MetadataColumnContext& metadata_context,
                          ArrowArray* array_builder) {
  // A map array holds one struct of key and value per entry.
  auto* entries_builder = array_builder->children[0];
  auto* key_builder = entries_builder->children[0];
  auto* item_builder = entries_builder->children[1];
  const auto& key_field = map_type.key();
  const auto& value_field = map_type.value();

  if (avro_node->type() == ::avro::AVRO_MAP) {
    // Handle regular Avro map: map<string, value>
    const auto& avro_map = avro_datum.value<::avro::GenericMap>();
    const auto& key_node = avro_node->leafAt(0);
    const auto& value_node = avro_node->leafAt(1);

    for (const auto& entry : avro_map.value()) {
      ICEBERG_RETURN_UNEXPECTED(AppendFieldToBuilder(key_node, entry.first,
                                                     key_projection, key_field,
                                                     metadata_context, key_builder));
      ICEBERG_RETURN_UNEXPECTED(AppendFieldToBuilder(value_node, entry.second,
                                                     value_projection, value_field,
                                                     metadata_context, item_builder));
      ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(entries_builder));
    }
  } else if (avro_node->type() == ::avro::AVRO_ARRAY && HasMapLogicalType(avro_node)) {
    // Handle array-based map: list<struct<key, value>>
    const auto& avro_array = avro_datum.value<::avro::GenericArray>();
    const auto& record_node = avro_node->leafAt(0);
    if (record_node->type() != ::avro::AVRO_RECORD || record_node->leaves() != 2) {
      return InvalidArgument(
          "Array-based map must contain records with exactly 2 fields, got: {}",
          ToString(record_node));
    }
    const auto& key_node = record_node->leafAt(0);
    const auto& value_node = record_node->leafAt(1);

    for (const auto& entry : avro_array.value()) {
      const auto& record = entry.value<::avro::GenericRecord>();
      ICEBERG_RETURN_UNEXPECTED(AppendFieldToBuilder(key_node, record.fieldAt(0),
                                                     key_projection, key_field,
                                                     metadata_context, key_builder));
      ICEBERG_RETURN_UNEXPECTED(AppendFieldToBuilder(value_node, record.fieldAt(1),
                                                     value_projection, value_field,
                                                     metadata_context, item_builder));
      ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(entries_builder));
    }
  } else {
    return InvalidArgument("Expected Avro map or array with map logical type, got: {}",
                           ToString(avro_node));
  }
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(array_builder));
  return {};
}

/// \brief Append nested Avro data to a nanoarrow array builder based on type.
Status AppendNestedValueToBuilder(const ::avro::NodePtr& avro_node,
                                  const ::avro::GenericDatum& avro_datum,
                                  const std::span<const FieldProjection>& projections,
                                  const NestedType& projected_type,
                                  const MetadataColumnContext& metadata_context,
                                  ArrowArray* array_builder) {
  switch (projected_type.type_id()) {
    case TypeId::kStruct: {
      const auto& struct_type = internal::checked_cast<const StructType&>(projected_type);
      return AppendStructToBuilder(avro_node, avro_datum, projections, struct_type,
                                   metadata_context, array_builder);
    }

    case TypeId::kList: {
      if (projections.size() != 1) {
        return InvalidArgument("Expected 1 projection for list, got: {}",
                               projections.size());
      }
      const auto& list_type = internal::checked_cast<const ListType&>(projected_type);
      return AppendListToBuilder(avro_node, avro_datum, projections[0], list_type,
                                 metadata_context, array_builder);
    }

    case TypeId::kMap: {
      if (projections.size() != 2) {
        return InvalidArgument("Expected 2 projections for map, got: {}",
                               projections.size());
      }
      const auto& map_type = internal::checked_cast<const MapType&>(projected_type);
      return AppendMapToBuilder(avro_node, avro_datum, projections[0], projections[1],
                                map_type, metadata_context, array_builder);
    }

    default:
      return InvalidArgument("Unsupported nested type: {}", projected_type.ToString());
  }
}

Status AppendPrimitiveValueToBuilder(const ::avro::NodePtr& avro_node,
                                     const ::avro::GenericDatum& avro_datum,
                                     const SchemaField& projected_field,
                                     ArrowArray* array_builder) {
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
      return AppendBoolean(array_builder, avro_datum.value<bool>());
    }

    case TypeId::kInt: {
      if (avro_node->type() != ::avro::AVRO_INT) {
        return InvalidArgument("Expected Avro int for int field, got: {}",
                               ToString(avro_node));
      }
      return AppendInt(array_builder, avro_datum.value<int32_t>());
    }

    case TypeId::kLong: {
      if (avro_node->type() == ::avro::AVRO_LONG) {
        return AppendInt(array_builder, avro_datum.value<int64_t>());
      }
      if (avro_node->type() == ::avro::AVRO_INT) {
        return AppendInt(array_builder, avro_datum.value<int32_t>());
      }
      return InvalidArgument("Expected Avro int/long for long field, got: {}",
                             ToString(avro_node));
    }

    case TypeId::kFloat: {
      if (avro_node->type() != ::avro::AVRO_FLOAT) {
        return InvalidArgument("Expected Avro float for float field, got: {}",
                               ToString(avro_node));
      }
      return AppendDouble(array_builder, avro_datum.value<float>());
    }

    case TypeId::kDouble: {
      if (avro_node->type() == ::avro::AVRO_DOUBLE) {
        return AppendDouble(array_builder, avro_datum.value<double>());
      }
      if (avro_node->type() == ::avro::AVRO_FLOAT) {
        return AppendDouble(array_builder, avro_datum.value<float>());
      }
      return InvalidArgument("Expected Avro float/double for double field, got: {}",
                             ToString(avro_node));
    }

    case TypeId::kString: {
      if (avro_node->type() != ::avro::AVRO_STRING) {
        return InvalidArgument("Expected Avro string for string field, got: {}",
                               ToString(avro_node));
      }
      return AppendString(array_builder, avro_datum.value<std::string>());
    }

    case TypeId::kBinary:
    case TypeId::kGeometry:
    case TypeId::kGeography: {
      if (avro_node->type() != ::avro::AVRO_BYTES) {
        return InvalidArgument("Expected Avro bytes for binary field, got: {}",
                               ToString(avro_node));
      }
      return AppendBytes(array_builder, avro_datum.value<std::vector<uint8_t>>());
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
      return AppendBytes(array_builder, fixed.value());
    }

    case TypeId::kUuid: {
      if (avro_node->type() != ::avro::AVRO_FIXED ||
          avro_node->logicalType().type() != ::avro::LogicalType::UUID) {
        return InvalidArgument("Expected Avro fixed for uuid field, got: {}",
                               ToString(avro_node));
      }

      const auto& fixed = avro_datum.value<::avro::GenericFixed>();
      if (fixed.value().size() != 16) {
        return InvalidArgument("Expected UUID fixed length 16, got: {}",
                               fixed.value().size());
      }
      return AppendBytes(array_builder, fixed.value());
    }

    case TypeId::kDecimal: {
      if (avro_node->type() != ::avro::AVRO_FIXED ||
          avro_node->logicalType().type() != ::avro::LogicalType::DECIMAL) {
        return InvalidArgument(
            "Expected Avro fixed with decimal logical type for decimal field, got: {}",
            ToString(avro_node));
      }

      const auto& value = avro_datum.value<::avro::GenericFixed>().value();
      ICEBERG_ASSIGN_OR_RAISE(
          auto decimal,
          Decimal::FromBigEndian(value.data(), static_cast<int32_t>(value.size())));
      return AppendDecimal(array_builder, decimal);
    }

    case TypeId::kDate: {
      if (!IsAvroDateOrPlainInt(avro_node)) {
        return InvalidArgument(
            "Expected Avro int with DATE logical type or plain int for date field, got: "
            "{}",
            ToString(avro_node));
      }
      return AppendInt(array_builder, avro_datum.value<int32_t>());
    }

    case TypeId::kTime: {
      if (avro_node->type() != ::avro::AVRO_LONG ||
          avro_node->logicalType().type() != ::avro::LogicalType::TIME_MICROS) {
        return InvalidArgument(
            "Expected Avro long with TIME_MICROS for time field, got: {}",
            ToString(avro_node));
      }
      return AppendInt(array_builder, avro_datum.value<int64_t>());
    }

    case TypeId::kTimestamp:
    case TypeId::kTimestampTz: {
      if (avro_node->type() != ::avro::AVRO_LONG ||
          avro_node->logicalType().type() != ::avro::LogicalType::TIMESTAMP_MICROS) {
        return InvalidArgument(
            "Expected Avro long with TIMESTAMP_MICROS for timestamp field, got: {}",
            ToString(avro_node));
      }
      return AppendInt(array_builder, avro_datum.value<int64_t>());
    }

    case TypeId::kTimestampNs:
    case TypeId::kTimestampTzNs: {
      if (avro_node->type() != ::avro::AVRO_LONG ||
          avro_node->logicalType().type() != ::avro::LogicalType::TIMESTAMP_NANOS) {
        return InvalidArgument(
            "Expected Avro long with TIMESTAMP_NANOS for timestamp field, got: {}",
            ToString(avro_node));
      }
      return AppendInt(array_builder, avro_datum.value<int64_t>());
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
                            const MetadataColumnContext& metadata_context,
                            ArrowArray* array_builder) {
  if (projection.kind == FieldProjection::Kind::kNull ||
      ShouldReadRowLineageAsNull(projected_field.field_id(), metadata_context)) {
    return AppendNull(array_builder);
  }

  if (projection.kind == FieldProjection::Kind::kDefault) {
    return AppendDefaultToBuilder(projection, array_builder);
  }

  if (avro_node->type() == ::avro::AVRO_UNION) {
    size_t branch = avro_datum.unionBranch();
    if (avro_node->leafAt(branch)->type() == ::avro::AVRO_NULL) {
      // A null row lineage value is inherited from the file.
      if (MetadataColumns::IsRowLineageColumn(projected_field.field_id())) {
        return AppendMetadataToBuilder(projected_field.field_id(), metadata_context,
                                       array_builder);
      }
      return AppendNull(array_builder);
    }
    return AppendFieldToBuilder(avro_node->leafAt(branch), avro_datum, projection,
                                projected_field, metadata_context, array_builder);
  }

  const auto& projected_type = *projected_field.type();
  if (projected_type.is_primitive()) {
    return AppendPrimitiveValueToBuilder(avro_node, avro_datum, projected_field,
                                         array_builder);
  }
  const auto& nested_type = internal::checked_cast<const NestedType&>(projected_type);
  return AppendNestedValueToBuilder(avro_node, avro_datum, projection.children,
                                    nested_type, metadata_context, array_builder);
}

}  // namespace

Status AppendDefaultToBuilder(const FieldProjection& projection, ArrowArray* builder) {
  return AppendLiteral(builder, std::get<Literal>(projection.from));
}

Status AppendMetadataToBuilder(int32_t field_id,
                               const MetadataColumnContext& metadata_context,
                               ArrowArray* builder) {
  if (field_id == MetadataColumns::kFilePathColumnId) {
    return AppendString(builder, metadata_context.file_path);
  }
  if (field_id == MetadataColumns::kFilePositionColumnId) {
    return AppendInt(builder, metadata_context.next_file_pos);
  }
  if (MetadataColumns::IsRowLineageColumn(field_id)) {
    if (auto value = InheritedRowLineageValue(field_id, metadata_context)) {
      return AppendInt(builder, *value);
    }
    return AppendNull(builder);
  }
  return NotSupported("Unsupported metadata column field id: {}", field_id);
}

Status AppendDatumToBuilder(const ::avro::NodePtr& avro_node,
                            const ::avro::GenericDatum& avro_datum,
                            const SchemaProjection& projection,
                            const Schema& projected_schema,
                            const MetadataColumnContext& metadata_context,
                            ArrowArray* array_builder) {
  return AppendNestedValueToBuilder(avro_node, avro_datum, projection.fields,
                                    projected_schema, metadata_context, array_builder);
}

namespace {

// ToAvroNodeVisitor uses 0 for null branch and 1 for value branch.
constexpr int64_t kNullBranch = 0;
constexpr int64_t kValueBranch = 1;

}  // namespace

Status EncodeDecimalToFixed(const Decimal& value, size_t fixed_size,
                            std::vector<uint8_t>* bytes) {
  ICEBERG_PRECHECK(fixed_size > 0, "Avro decimal fixed size must be positive");
  // Decimal's bytes are native-endian; Avro fixed decimals are big-endian.
  auto big_endian = value.ToBytes();
  if constexpr (std::endian::native == std::endian::little) {
    std::ranges::reverse(big_endian);
  }
  const uint8_t padding = (big_endian.front() & 0x80) == 0 ? 0x00 : 0xff;
  const auto value_size = std::min(fixed_size, big_endian.size());
  const auto first = big_endian.end() - value_size;
  // Discard only sign extension, without changing the retained sign bit.
  ICEBERG_PRECHECK(std::all_of(big_endian.begin(), first,
                               [padding](uint8_t byte) { return byte == padding; }) &&
                       ((*first ^ padding) & 0x80) == 0,
                   "Decimal does not fit Avro fixed size {}", fixed_size);
  bytes->assign(fixed_size - value_size, padding);
  bytes->insert(bytes->end(), first, big_endian.end());
  return {};
}

Decimal GetDecimal(const ArrowArrayView& array_view, int64_t index) {
  ArrowDecimal decimal;
  ArrowDecimalInit(&decimal, Decimal::kBitWidth, Decimal::kMaxPrecision, 0);
  ArrowArrayViewGetDecimalUnsafe(&array_view, index, &decimal);
  // nanoarrow copies the stored bytes as they are, in native byte order.
  int128_t value;
  std::memcpy(&value, decimal.words, sizeof(value));
  return Decimal(value);
}

Status ExtractDatumFromArray(const ArrowArrayView& array_view, int64_t index,
                             ::avro::GenericDatum* datum) {
  if (index < 0 || index >= array_view.length) {
    return InvalidArgument("Cannot extract datum from array at index {} of length {}",
                           index, array_view.length);
  }

  if (ArrowArrayViewIsNull(&array_view, index)) {
    if (datum->type() == ::avro::AVRO_NULL) {
      return {};
    }
    if (!datum->isUnion()) [[unlikely]] {
      return InvalidSchema("Cannot extract null to non-union type: {}",
                           ::avro::toString(datum->type()));
    }
    datum->selectBranch(kNullBranch);
    return {};
  }

  if (datum->isUnion()) {
    datum->selectBranch(kValueBranch);
  }

  switch (array_view.storage_type) {
    case NANOARROW_TYPE_BOOL:
      datum->value<bool>() = ArrowArrayViewGetIntUnsafe(&array_view, index) != 0;
      return {};

    case NANOARROW_TYPE_INT32:
    case NANOARROW_TYPE_DATE32:
      datum->value<int32_t>() =
          static_cast<int32_t>(ArrowArrayViewGetIntUnsafe(&array_view, index));
      return {};

    // For time, and for timestamp and timestamp_tz in either time unit.
    case NANOARROW_TYPE_INT64:
    case NANOARROW_TYPE_TIME64:
    case NANOARROW_TYPE_TIMESTAMP:
      datum->value<int64_t>() = ArrowArrayViewGetIntUnsafe(&array_view, index);
      return {};

    case NANOARROW_TYPE_FLOAT:
      datum->value<float>() =
          static_cast<float>(ArrowArrayViewGetDoubleUnsafe(&array_view, index));
      return {};

    case NANOARROW_TYPE_DOUBLE:
      datum->value<double>() = ArrowArrayViewGetDoubleUnsafe(&array_view, index);
      return {};

    // TODO(gangwu): support LARGE_STRING.
    case NANOARROW_TYPE_STRING: {
      ArrowStringView value = ArrowArrayViewGetStringUnsafe(&array_view, index);
      datum->value<std::string>().assign(value.data,
                                         static_cast<size_t>(value.size_bytes));
      return {};
    }

    // TODO(gangwu): support LARGE_BINARY.
    case NANOARROW_TYPE_BINARY: {
      ArrowBufferView value = ArrowArrayViewGetBytesUnsafe(&array_view, index);
      datum->value<std::vector<uint8_t>>().assign(value.data.as_uint8,
                                                  value.data.as_uint8 + value.size_bytes);
      return {};
    }

    // For fixed, and for uuid stored as fixed_size_binary(16).
    case NANOARROW_TYPE_FIXED_SIZE_BINARY: {
      ArrowBufferView value = ArrowArrayViewGetBytesUnsafe(&array_view, index);
      auto& fixed_datum = datum->value<::avro::GenericFixed>();
      fixed_datum.value().assign(value.data.as_uint8,
                                 value.data.as_uint8 + value.size_bytes);
      return {};
    }

    case NANOARROW_TYPE_DECIMAL128: {
      auto& fixed_datum = datum->value<::avro::GenericFixed>();
      return EncodeDecimalToFixed(GetDecimal(array_view, index),
                                  fixed_datum.schema()->fixedSize(),
                                  &fixed_datum.value());
    }

    case NANOARROW_TYPE_STRUCT: {
      auto& record = datum->value<::avro::GenericRecord>();
      // A struct's offset applies to its children as well.
      const int64_t child_index = array_view.offset + index;
      for (int64_t i = 0; i < array_view.n_children; ++i) {
        ICEBERG_RETURN_UNEXPECTED(ExtractDatumFromArray(*array_view.children[i],
                                                        child_index, &record.fieldAt(i)));
      }
      return {};
    }

    // TODO(gangwu): support LARGE_LIST.
    case NANOARROW_TYPE_LIST: {
      auto& avro_array = datum->value<::avro::GenericArray>();
      auto& elements = avro_array.value();

      const int64_t start = GetCollectionChildOffset(array_view, index);
      const int64_t end = GetCollectionChildOffset(array_view, index + 1);
      const auto& values = *array_view.children[0];
      elements.resize(end - start, ::avro::GenericDatum(avro_array.schema()->leafAt(0)));

      for (int64_t i = start; i < end; ++i) {
        ICEBERG_RETURN_UNEXPECTED(ExtractDatumFromArray(values, i, &elements[i - start]));
      }
      return {};
    }

    case NANOARROW_TYPE_MAP: {
      const int64_t start = GetCollectionChildOffset(array_view, index);
      const int64_t end = GetCollectionChildOffset(array_view, index + 1);
      const int64_t length = end - start;

      // Map offsets index the entries struct, whose offset applies to keys and values.
      const auto& entries = *array_view.children[0];
      const auto& keys = *entries.children[0];
      const auto& items = *entries.children[1];
      const int64_t first_entry = entries.offset + start;

      if (datum->type() == ::avro::AVRO_MAP) {
        // Handle regular Avro map
        auto& avro_map = datum->value<::avro::GenericMap>();
        auto value_node = avro_map.schema()->leafAt(1);

        auto& map_entries = avro_map.value();
        map_entries.resize(
            length, std::make_pair(std::string(), ::avro::GenericDatum(value_node)));

        for (int64_t i = 0; i < length; ++i) {
          auto& map_entry = map_entries[i];
          ArrowStringView key = ArrowArrayViewGetStringUnsafe(&keys, first_entry + i);
          map_entry.first.assign(key.data, static_cast<size_t>(key.size_bytes));
          ICEBERG_RETURN_UNEXPECTED(
              ExtractDatumFromArray(items, first_entry + i, &map_entry.second));
        }
      } else if (datum->type() == ::avro::AVRO_ARRAY) {
        // Handle array-based map (list<struct<key, value>>)
        auto& avro_array = datum->value<::avro::GenericArray>();
        auto record_node = avro_array.schema()->leafAt(0);
        if (record_node->type() != ::avro::AVRO_RECORD || record_node->leaves() != 2) {
          return InvalidArgument(
              "Expected Avro record with 2 fields for map value, got: {}",
              ToString(record_node));
        }

        auto& elements = avro_array.value();
        elements.resize(length, ::avro::GenericDatum(record_node));

        for (int64_t i = 0; i < length; ++i) {
          auto& record = elements[i].value<::avro::GenericRecord>();
          ICEBERG_RETURN_UNEXPECTED(
              ExtractDatumFromArray(keys, first_entry + i, &record.fieldAt(0)));
          ICEBERG_RETURN_UNEXPECTED(
              ExtractDatumFromArray(items, first_entry + i, &record.fieldAt(1)));
        }
      } else {
        return InvalidArgument("Unsupported Avro type for map: {}",
                               static_cast<int>(datum->type()));
      }
      return {};
    }

    default:
      return InvalidArgument("Unsupported Arrow array type: {}",
                             ArrowTypeString(array_view.storage_type));
  }
}

}  // namespace iceberg::avro
