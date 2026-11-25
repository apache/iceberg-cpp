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

#include <avro/Specific.hh>
#include <nanoarrow/nanoarrow.h>

#include "iceberg/avro/avro_data_util_internal.h"
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
                         const Type& type, const ArrowArrayView& array_view,
                         int64_t row_index, EncodeContext& ctx) {
  ICEBERG_PRECHECK(row_index >= 0 && row_index < array_view.length,
                   "Row index {} out of bounds {}", row_index, array_view.length);

  const bool is_null = ArrowArrayViewIsNull(&array_view, row_index);

  if (avro_node->type() == ::avro::AVRO_UNION) {
    ICEBERG_ASSIGN_OR_RAISE(auto branches, ValidateUnion(avro_node));

    if (is_null) {
      encoder.encodeUnionIndex(branches.null_index);
      encoder.encodeNull();
      return {};
    }

    encoder.encodeUnionIndex(branches.value_index);
    return EncodeArrowToAvro(branches.value_node, encoder, type, array_view, row_index,
                             ctx);
  }

  if (avro_node->type() == ::avro::AVRO_NULL) {
    encoder.encodeNull();
    return {};
  }

  if (is_null) {
    return InvalidArgument("Null value in non-nullable field");
  }

  switch (avro_node->type()) {
    case ::avro::AVRO_BOOL:
      encoder.encodeBool(ArrowArrayViewGetIntUnsafe(&array_view, row_index) != 0);
      return {};

    case ::avro::AVRO_INT: {
      // AVRO_INT can represent: int32, date (days since epoch)
      switch (array_view.storage_type) {
        case NANOARROW_TYPE_INT32:
        case NANOARROW_TYPE_DATE32:
          encoder.encodeInt(
              static_cast<int32_t>(ArrowArrayViewGetIntUnsafe(&array_view, row_index)));
          return {};
        default:
          return InvalidArgument("AVRO_INT expects Int32Array or Date32Array, got {}",
                                 ArrowTypeString(array_view.storage_type));
      }
    }

    case ::avro::AVRO_LONG: {
      // AVRO_LONG can represent: int64, time (microseconds), timestamp (microseconds)
      switch (array_view.storage_type) {
        case NANOARROW_TYPE_INT64:
        case NANOARROW_TYPE_TIME64:
        case NANOARROW_TYPE_TIMESTAMP:
          encoder.encodeLong(ArrowArrayViewGetIntUnsafe(&array_view, row_index));
          return {};
        default:
          return InvalidArgument(
              "AVRO_LONG expects Int64Array, Time64Array, or TimestampArray, got {}",
              ArrowTypeString(array_view.storage_type));
      }
    }

    case ::avro::AVRO_FLOAT:
      encoder.encodeFloat(
          static_cast<float>(ArrowArrayViewGetDoubleUnsafe(&array_view, row_index)));
      return {};

    case ::avro::AVRO_DOUBLE:
      encoder.encodeDouble(ArrowArrayViewGetDoubleUnsafe(&array_view, row_index));
      return {};

    case ::avro::AVRO_STRING: {
      ArrowStringView value = ArrowArrayViewGetStringUnsafe(&array_view, row_index);
      encoder.encodeString(
          std::string(value.data, static_cast<size_t>(value.size_bytes)));
      return {};
    }

    case ::avro::AVRO_BYTES: {
      ArrowBufferView value = ArrowArrayViewGetBytesUnsafe(&array_view, row_index);
      ctx.bytes_scratch.assign(value.data.as_uint8,
                               value.data.as_uint8 + value.size_bytes);
      encoder.encodeBytes(ctx.bytes_scratch);
      return {};
    }

    case ::avro::AVRO_FIXED: {
      // Handle DECIMAL
      if (avro_node->logicalType().type() == ::avro::LogicalType::DECIMAL) {
        ICEBERG_RETURN_UNEXPECTED(EncodeDecimalToFixed(GetDecimal(array_view, row_index),
                                                       avro_node->fixedSize(),
                                                       &ctx.bytes_scratch));
        encoder.encodeFixed(ctx.bytes_scratch.data(), ctx.bytes_scratch.size());
        return {};
      }

      // Handle UUID and regular FIXED, both stored as fixed-size binary
      ArrowBufferView value = ArrowArrayViewGetBytesUnsafe(&array_view, row_index);
      encoder.encodeFixed(value.data.as_uint8, static_cast<size_t>(value.size_bytes));
      return {};
    }

    case ::avro::AVRO_RECORD: {
      ICEBERG_PRECHECK(array_view.storage_type == NANOARROW_TYPE_STRUCT,
                       "AVRO_RECORD expects StructArray, got {}",
                       ArrowTypeString(array_view.storage_type));
      ICEBERG_PRECHECK(type.type_id() == TypeId::kStruct,
                       "AVRO_RECORD expects struct type, got type {}", type.ToString());

      const auto& struct_type = internal::checked_cast<const StructType&>(type);
      const size_t num_fields = avro_node->leaves();

      ICEBERG_PRECHECK(
          array_view.n_children == static_cast<int64_t>(num_fields),
          "Field count mismatch: Arrow struct has {} fields, Avro node has {} fields",
          array_view.n_children, num_fields);
      ICEBERG_PRECHECK(
          struct_type.fields().size() == num_fields,
          "Field count mismatch: Iceberg struct has {} fields, Avro node has {} fields",
          struct_type.fields().size(), num_fields);

      // A struct's offset applies to its children as well.
      const int64_t child_index = array_view.offset + row_index;
      for (size_t i = 0; i < num_fields; ++i) {
        const auto& field_node = avro_node->leafAt(i);
        const auto& field_view = *array_view.children[i];
        const auto& field_schema = struct_type.fields()[i];

        ICEBERG_RETURN_UNEXPECTED(EncodeArrowToAvro(
            field_node, encoder, *field_schema.type(), field_view, child_index, ctx));
      }
      return {};
    }

    case ::avro::AVRO_ARRAY: {
      const auto& element_node = avro_node->leafAt(0);
      const int64_t start = GetCollectionChildOffset(array_view, row_index);
      const int64_t end = GetCollectionChildOffset(array_view, row_index + 1);

      // Handle ListArray
      if (array_view.storage_type == NANOARROW_TYPE_LIST) {
        const auto& list_type = internal::checked_cast<const ListType&>(type);

        encoder.arrayStart();
        if (end > start) {
          encoder.setItemCount(end - start);
          const auto& values = *array_view.children[0];
          const auto& element_type = *list_type.fields()[0].type();

          for (int64_t i = start; i < end; ++i) {
            encoder.startItem();
            ICEBERG_RETURN_UNEXPECTED(
                EncodeArrowToAvro(element_node, encoder, element_type, values, i, ctx));
          }
        }
        encoder.arrayEnd();
        return {};
      }

      // Handle MapArray (for Avro maps with non-string keys)
      if (array_view.storage_type == NANOARROW_TYPE_MAP) {
        ICEBERG_PRECHECK(
            element_node->type() == ::avro::AVRO_RECORD && element_node->leaves() == 2,
            "Expected AVRO_RECORD for map key-value pair, got {}",
            ::avro::toString(element_node->type()));

        const auto& map_type = internal::checked_cast<const MapType&>(type);

        encoder.arrayStart();
        if (end > start) {
          encoder.setItemCount(end - start);
          // Map offsets index the entries struct, whose offset applies to keys and
          // values.
          const auto& entries = *array_view.children[0];
          const auto& keys = *entries.children[0];
          const auto& values = *entries.children[1];
          const auto& key_type = *map_type.key().type();
          const auto& value_type = *map_type.value().type();

          // The element_node should be a RECORD with "key" and "value" fields
          for (int64_t i = start; i < end; ++i) {
            const auto& key_node = element_node->leafAt(0);
            const auto& value_node = element_node->leafAt(1);

            encoder.startItem();
            ICEBERG_RETURN_UNEXPECTED(EncodeArrowToAvro(key_node, encoder, key_type, keys,
                                                        entries.offset + i, ctx));
            ICEBERG_RETURN_UNEXPECTED(EncodeArrowToAvro(value_node, encoder, value_type,
                                                        values, entries.offset + i, ctx));
          }
        }
        encoder.arrayEnd();
        return {};
      }

      return InvalidArgument("AVRO_ARRAY must map to ListArray or MapArray, got {}",
                             ArrowTypeString(array_view.storage_type));
    }

    case ::avro::AVRO_MAP: {
      ICEBERG_PRECHECK(array_view.storage_type == NANOARROW_TYPE_MAP,
                       "AVRO_MAP expects MapArray, got {}",
                       ArrowTypeString(array_view.storage_type));
      ICEBERG_PRECHECK(type.type_id() == TypeId::kMap,
                       "AVRO_MAP expects MapType, got type {}", type.ToString());

      const auto& map_type = internal::checked_cast<const MapType&>(type);
      const int64_t start = GetCollectionChildOffset(array_view, row_index);
      const int64_t end = GetCollectionChildOffset(array_view, row_index + 1);

      encoder.mapStart();
      if (end > start) {
        encoder.setItemCount(end - start);
        // Map offsets index the entries struct, whose offset applies to keys and values.
        const auto& entries = *array_view.children[0];
        const auto& keys = *entries.children[0];
        const auto& values = *entries.children[1];
        const auto& value_type = *map_type.value().type();
        const auto& value_node = avro_node->leafAt(1);

        ICEBERG_PRECHECK(keys.storage_type == NANOARROW_TYPE_STRING ||
                             keys.storage_type == NANOARROW_TYPE_LARGE_STRING,
                         "AVRO_MAP keys must be StringArray, got {}",
                         ArrowTypeString(keys.storage_type));

        for (int64_t i = start; i < end; ++i) {
          encoder.startItem();
          ArrowStringView key = ArrowArrayViewGetStringUnsafe(&keys, entries.offset + i);
          encoder.encodeString(
              std::string(key.data, static_cast<size_t>(key.size_bytes)));
          ICEBERG_RETURN_UNEXPECTED(EncodeArrowToAvro(value_node, encoder, value_type,
                                                      values, entries.offset + i, ctx));
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
