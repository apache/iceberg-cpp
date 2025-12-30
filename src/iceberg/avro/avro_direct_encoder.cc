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
#include <cstring>

#include <arrow/array.h>
#include <arrow/extension_type.h>
#include <arrow/type.h>
#include <avro/Specific.hh>

#include "iceberg/avro/avro_direct_encoder_internal.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/macros.h"

namespace iceberg::avro {

namespace {

// Utility struct for union branch information
struct UnionBranches {
  size_t null_index;
  size_t value_index;
  ::avro::NodePtr value_node;
};

Result<UnionBranches> ValidateUnion(const ::avro::NodePtr& union_node) {
  ICEBERG_PRECHECK(union_node->leaves() == 2,
                   "Union must have exactly 2 branches, got {}", union_node->leaves());

  const auto& branch_0 = union_node->leafAt(0);
  const auto& branch_1 = union_node->leafAt(1);

  if (branch_0->type() == ::avro::AVRO_NULL && branch_1->type() != ::avro::AVRO_NULL) {
    return UnionBranches{.null_index = 0, .value_index = 1, .value_node = branch_1};
  }
  if (branch_1->type() == ::avro::AVRO_NULL && branch_0->type() != ::avro::AVRO_NULL) {
    return UnionBranches{.null_index = 1, .value_index = 0, .value_node = branch_0};
  }
  return InvalidArgument("Union must have exactly one null branch");
}

}  // namespace

Status EncodeArrowToAvro(const ::avro::NodePtr& avro_node, ::avro::Encoder& encoder,
                         const Type& type, const ::arrow::Array& array, int64_t row_index,
                         EncodeContext& ctx) {
  ICEBERG_PRECHECK(row_index >= 0 && row_index < array.length(),
                   "Row index {} out of bounds {}", row_index, array.length());

  const bool is_null = array.IsNull(row_index);

  if (avro_node->type() == ::avro::AVRO_UNION) {
    ICEBERG_ASSIGN_OR_RAISE(auto branches, ValidateUnion(avro_node));

    if (is_null) {
      encoder.encodeUnionIndex(branches.null_index);
      encoder.encodeNull();
      return {};
    }

    encoder.encodeUnionIndex(branches.value_index);
    return EncodeArrowToAvro(branches.value_node, encoder, type, array, row_index, ctx);
  }

  if (is_null) {
    return InvalidArgument("Null value in non-nullable field");
  }

  switch (avro_node->type()) {
    case ::avro::AVRO_NULL:
      encoder.encodeNull();
      return {};

    case ::avro::AVRO_BOOL: {
      const auto& bool_array =
          internal::checked_cast<const ::arrow::BooleanArray&>(array);
      encoder.encodeBool(bool_array.Value(row_index));
      return {};
    }

    case ::avro::AVRO_INT: {
      // AVRO_INT can represent: int32, date (days since epoch)
      switch (array.type()->id()) {
        case ::arrow::Type::INT32: {
          const auto& int32_array =
              internal::checked_cast<const ::arrow::Int32Array&>(array);
          encoder.encodeInt(int32_array.Value(row_index));
          return {};
        }
        case ::arrow::Type::DATE32: {
          const auto& date_array =
              internal::checked_cast<const ::arrow::Date32Array&>(array);
          encoder.encodeInt(date_array.Value(row_index));
          return {};
        }
        default:
          return InvalidArgument("AVRO_INT expects Int32Array or Date32Array, got {}",
                                 array.type()->ToString());
      }
    }

    case ::avro::AVRO_LONG: {
      // AVRO_LONG can represent: int64, time (microseconds), timestamp (microseconds)
      switch (array.type()->id()) {
        case ::arrow::Type::INT64: {
          const auto& int64_array =
              internal::checked_cast<const ::arrow::Int64Array&>(array);
          encoder.encodeLong(int64_array.Value(row_index));
          return {};
        }
        case ::arrow::Type::TIME64: {
          const auto& time_array =
              internal::checked_cast<const ::arrow::Time64Array&>(array);
          encoder.encodeLong(time_array.Value(row_index));
          return {};
        }
        case ::arrow::Type::TIMESTAMP: {
          const auto& timestamp_array =
              internal::checked_cast<const ::arrow::TimestampArray&>(array);
          encoder.encodeLong(timestamp_array.Value(row_index));
          return {};
        }
        default:
          return InvalidArgument(
              "AVRO_LONG expects Int64Array, Time64Array, or TimestampArray, got {}",
              array.type()->ToString());
      }
    }

    case ::avro::AVRO_FLOAT: {
      const auto& float_array = internal::checked_cast<const ::arrow::FloatArray&>(array);
      encoder.encodeFloat(float_array.Value(row_index));
      return {};
    }

    case ::avro::AVRO_DOUBLE: {
      const auto& double_array =
          internal::checked_cast<const ::arrow::DoubleArray&>(array);
      encoder.encodeDouble(double_array.Value(row_index));
      return {};
    }

    case ::avro::AVRO_STRING: {
      const auto& string_array =
          internal::checked_cast<const ::arrow::StringArray&>(array);
      std::string_view value = string_array.GetView(row_index);
      encoder.encodeString(std::string(value));
      return {};
    }

    case ::avro::AVRO_BYTES: {
      const auto& binary_array =
          internal::checked_cast<const ::arrow::BinaryArray&>(array);
      std::string_view value = binary_array.GetView(row_index);
      ctx.bytes_scratch.assign(value.begin(), value.end());
      encoder.encodeBytes(ctx.bytes_scratch);
      return {};
    }

    case ::avro::AVRO_FIXED: {
      // Handle UUID
      if (avro_node->logicalType().type() == ::avro::LogicalType::UUID) {
        const auto& extension_array =
            internal::checked_cast<const ::arrow::ExtensionArray&>(array);
        const auto& fixed_array =
            internal::checked_cast<const ::arrow::FixedSizeBinaryArray&>(
                *extension_array.storage());
        std::string_view value = fixed_array.GetView(row_index);
        ctx.bytes_scratch.assign(value.begin(), value.end());
        encoder.encodeFixed(ctx.bytes_scratch.data(), ctx.bytes_scratch.size());
        return {};
      }

      // Handle DECIMAL
      if (avro_node->logicalType().type() == ::avro::LogicalType::DECIMAL) {
        const auto& decimal_array =
            internal::checked_cast<const ::arrow::Decimal128Array&>(array);
        std::string_view decimal_value = decimal_array.GetView(row_index);
        ctx.bytes_scratch.assign(decimal_value.begin(), decimal_value.end());
        // Arrow Decimal128 bytes are in little-endian order, Avro requires big-endian
        std::ranges::reverse(ctx.bytes_scratch);
        encoder.encodeFixed(ctx.bytes_scratch.data(), ctx.bytes_scratch.size());
        return {};
      }

      // Handle regular FIXED
      const auto& fixed_array =
          internal::checked_cast<const ::arrow::FixedSizeBinaryArray&>(array);
      std::string_view value = fixed_array.GetView(row_index);
      ctx.bytes_scratch.assign(value.begin(), value.end());
      encoder.encodeFixed(ctx.bytes_scratch.data(), ctx.bytes_scratch.size());
      return {};
    }

    case ::avro::AVRO_RECORD: {
      ICEBERG_PRECHECK(array.type()->id() == ::arrow::Type::STRUCT,
                       "AVRO_RECORD expects StructArray, got {}",
                       array.type()->ToString());
      ICEBERG_PRECHECK(type.type_id() == TypeId::kStruct,
                       "AVRO_RECORD expects struct type, got type {}", type.ToString());

      const auto& struct_array =
          internal::checked_cast<const ::arrow::StructArray&>(array);
      const auto& struct_type = internal::checked_cast<const StructType&>(type);
      const size_t num_fields = avro_node->leaves();

      ICEBERG_PRECHECK(
          struct_array.num_fields() == static_cast<int>(num_fields),
          "Field count mismatch: Arrow struct has {} fields, Avro node has {} fields",
          struct_array.num_fields(), num_fields);
      ICEBERG_PRECHECK(
          struct_type.fields().size() == num_fields,
          "Field count mismatch: Iceberg struct has {} fields, Avro node has {} fields",
          struct_type.fields().size(), num_fields);

      for (size_t i = 0; i < num_fields; ++i) {
        const auto& field_node = avro_node->leafAt(i);
        const auto& field_array = struct_array.field(static_cast<int>(i));
        const auto& field_schema = struct_type.fields()[i];

        ICEBERG_RETURN_UNEXPECTED(EncodeArrowToAvro(
            field_node, encoder, *field_schema.type(), *field_array, row_index, ctx));
      }
      return {};
    }

    case ::avro::AVRO_ARRAY: {
      const auto& element_node = avro_node->leafAt(0);

      // Handle ListArray
      if (array.type()->id() == ::arrow::Type::LIST) {
        const auto& list_array = internal::checked_cast<const ::arrow::ListArray&>(array);
        const auto& list_type = internal::checked_cast<const ListType&>(type);

        const auto start = list_array.value_offset(row_index);
        const auto end = list_array.value_offset(row_index + 1);
        const auto length = end - start;

        encoder.arrayStart();
        if (length > 0) {
          encoder.setItemCount(length);
          const auto& values = list_array.values();
          const auto& element_type = *list_type.fields()[0].type();

          for (int64_t i = start; i < end; ++i) {
            encoder.startItem();
            ICEBERG_RETURN_UNEXPECTED(
                EncodeArrowToAvro(element_node, encoder, element_type, *values, i, ctx));
          }
        }
        encoder.arrayEnd();
        return {};
      }

      // Handle MapArray (for Avro maps with non-string keys)
      if (array.type()->id() == ::arrow::Type::MAP) {
        ICEBERG_PRECHECK(
            element_node->type() == ::avro::AVRO_RECORD && element_node->leaves() == 2,
            "Expected AVRO_RECORD for map key-value pair, got {}",
            ::avro::toString(element_node->type()));

        const auto& map_array = internal::checked_cast<const ::arrow::MapArray&>(array);
        const auto& map_type = internal::checked_cast<const MapType&>(type);

        const auto start = map_array.value_offset(row_index);
        const auto end = map_array.value_offset(row_index + 1);
        const auto length = end - start;

        encoder.arrayStart();
        if (length > 0) {
          encoder.setItemCount(length);
          const auto& keys = map_array.keys();
          const auto& values = map_array.items();
          const auto& key_type = *map_type.key().type();
          const auto& value_type = *map_type.value().type();

          // The element_node should be a RECORD with "key" and "value" fields
          for (int64_t i = start; i < end; ++i) {
            const auto& key_node = element_node->leafAt(0);
            const auto& value_node = element_node->leafAt(1);

            encoder.startItem();
            ICEBERG_RETURN_UNEXPECTED(
                EncodeArrowToAvro(key_node, encoder, key_type, *keys, i, ctx));
            ICEBERG_RETURN_UNEXPECTED(
                EncodeArrowToAvro(value_node, encoder, value_type, *values, i, ctx));
          }
        }
        encoder.arrayEnd();
        return {};
      }

      return InvalidArgument("AVRO_ARRAY must map to ListArray or MapArray, got {}",
                             array.type()->ToString());
    }

    case ::avro::AVRO_MAP: {
      ICEBERG_PRECHECK(array.type()->id() == ::arrow::Type::MAP,
                       "AVRO_MAP expects MapArray, got {}", array.type()->ToString());
      ICEBERG_PRECHECK(type.type_id() == TypeId::kMap,
                       "AVRO_MAP expects MapType, got type {}", type.ToString());

      const auto& map_array = internal::checked_cast<const ::arrow::MapArray&>(array);
      const auto& map_type = internal::checked_cast<const MapType&>(type);

      const auto start = map_array.value_offset(row_index);
      const auto end = map_array.value_offset(row_index + 1);
      const auto length = end - start;

      encoder.mapStart();
      if (length > 0) {
        encoder.setItemCount(length);
        const auto& keys = map_array.keys();
        const auto& values = map_array.items();
        const auto& value_type = *map_type.value().type();
        const auto& value_node = avro_node->leafAt(1);

        ICEBERG_PRECHECK(keys->type()->id() == ::arrow::Type::STRING ||
                             keys->type()->id() == ::arrow::Type::LARGE_STRING,
                         "AVRO_MAP keys must be StringArray, got {}",
                         keys->type()->ToString());

        for (int64_t i = start; i < end; ++i) {
          encoder.startItem();

          if (keys->type()->id() == ::arrow::Type::STRING) {
            const auto& string_array =
                internal::checked_cast<const ::arrow::StringArray&>(*keys);
            std::string_view key_value = string_array.GetView(i);
            encoder.encodeString(std::string(key_value));
          } else {
            const auto& large_string_array =
                internal::checked_cast<const ::arrow::LargeStringArray&>(*keys);
            std::string_view key_value = large_string_array.GetView(i);
            encoder.encodeString(std::string(key_value));
          }

          ICEBERG_RETURN_UNEXPECTED(
              EncodeArrowToAvro(value_node, encoder, value_type, *values, i, ctx));
        }
      }
      encoder.mapEnd();
      return {};
    }

    case ::avro::AVRO_ENUM:
      return NotSupported("ENUM type encoding not yet implemented");

    case ::avro::AVRO_UNION:
      // Already handled above
      return Invalid("Unexpected union handling");

    default:
      return NotSupported("Unsupported Avro type: {}",
                          ::avro::toString(avro_node->type()));
  }
}

}  // namespace iceberg::avro
