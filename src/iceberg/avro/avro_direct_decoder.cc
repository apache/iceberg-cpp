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
#include <arrow/type.h>
#include <arrow/util/decimal.h>
#include <avro/Decoder.hh>
#include <avro/Node.hh>
#include <avro/NodeImpl.hh>
#include <avro/Types.hh>

#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/avro/avro_direct_decoder_internal.h"
#include "iceberg/avro/avro_schema_util_internal.h"
#include "iceberg/schema.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/macros.h"

namespace iceberg::avro {

using ::iceberg::arrow::ToErrorKind;

namespace {

/// \brief Forward declaration for mutual recursion.
Status DecodeFieldToBuilder(const ::avro::NodePtr& avro_node, ::avro::Decoder& decoder,
                            const FieldProjection& projection,
                            const SchemaField& projected_field,
                            ::arrow::ArrayBuilder* array_builder, DecodeContext* ctx);

/// \brief Skip an Avro value based on its schema without decoding
Status SkipAvroValue(const ::avro::NodePtr& avro_node, ::avro::Decoder& decoder) {
  switch (avro_node->type()) {
    case ::avro::AVRO_NULL:
      decoder.decodeNull();
      return {};

    case ::avro::AVRO_BOOL:
      decoder.decodeBool();
      return {};

    case ::avro::AVRO_INT:
      decoder.decodeInt();
      return {};

    case ::avro::AVRO_LONG:
      decoder.decodeLong();
      return {};

    case ::avro::AVRO_FLOAT:
      decoder.decodeFloat();
      return {};

    case ::avro::AVRO_DOUBLE:
      decoder.decodeDouble();
      return {};

    case ::avro::AVRO_STRING:
      decoder.skipString();
      return {};

    case ::avro::AVRO_BYTES:
      decoder.skipBytes();
      return {};

    case ::avro::AVRO_FIXED:
      decoder.skipFixed(avro_node->fixedSize());
      return {};

    case ::avro::AVRO_RECORD: {
      // Skip all fields in order
      for (size_t i = 0; i < avro_node->leaves(); ++i) {
        ICEBERG_RETURN_UNEXPECTED(SkipAvroValue(avro_node->leafAt(i), decoder));
      }
      return {};
    }

    case ::avro::AVRO_ENUM:
      decoder.decodeEnum();
      return {};

    case ::avro::AVRO_ARRAY: {
      const auto& element_node = avro_node->leafAt(0);
      // skipArray() returns count like arrayStart(), must handle all blocks
      int64_t block_count = decoder.skipArray();
      while (block_count > 0) {
        for (int64_t i = 0; i < block_count; ++i) {
          ICEBERG_RETURN_UNEXPECTED(SkipAvroValue(element_node, decoder));
        }
        block_count = decoder.arrayNext();
      }
      return {};
    }

    case ::avro::AVRO_MAP: {
      const auto& value_node = avro_node->leafAt(1);
      // skipMap() returns count like mapStart(), must handle all blocks
      int64_t block_count = decoder.skipMap();
      while (block_count > 0) {
        for (int64_t i = 0; i < block_count; ++i) {
          decoder.skipString();  // Skip key (always string in Avro maps)
          ICEBERG_RETURN_UNEXPECTED(SkipAvroValue(value_node, decoder));
        }
        block_count = decoder.mapNext();
      }
      return {};
    }

    case ::avro::AVRO_UNION: {
      const size_t branch_index = decoder.decodeUnionIndex();
      // Validate branch index
      const size_t num_branches = avro_node->leaves();
      if (branch_index >= num_branches) {
        return InvalidArgument("Union branch index {} out of range [0, {})", branch_index,
                               num_branches);
      }
      return SkipAvroValue(avro_node->leafAt(branch_index), decoder);
    }

    default:
      return InvalidArgument("Unsupported Avro type for skipping: {}",
                             ToString(avro_node));
  }
}

/// \brief Decode Avro record directly to Arrow struct builder.
Status DecodeStructToBuilder(const ::avro::NodePtr& avro_node, ::avro::Decoder& decoder,
                             const std::span<const FieldProjection>& projections,
                             const StructType& struct_type,
                             ::arrow::ArrayBuilder* array_builder, DecodeContext* ctx) {
  if (avro_node->type() != ::avro::AVRO_RECORD) {
    return InvalidArgument("Expected Avro record, got type: {}", ToString(avro_node));
  }

  auto* struct_builder = internal::checked_cast<::arrow::StructBuilder*>(array_builder);
  ICEBERG_ARROW_RETURN_NOT_OK(struct_builder->Append());

  // Build a map from Avro field index to projection index (cached per struct schema)
  // -1 means the field should be skipped
  const FieldProjection* cache_key = projections.data();
  auto cache_it = ctx->avro_to_projection_cache.find(cache_key);
  std::vector<int>* avro_to_projection;

  if (cache_it != ctx->avro_to_projection_cache.end()) {
    // Use cached mapping
    avro_to_projection = &cache_it->second;
  } else {
    // Build and cache the mapping
    auto [inserted_it, inserted] = ctx->avro_to_projection_cache.emplace(
        cache_key, std::vector<int>(avro_node->leaves(), -1));
    avro_to_projection = &inserted_it->second;

    for (size_t proj_idx = 0; proj_idx < projections.size(); ++proj_idx) {
      const auto& field_projection = projections[proj_idx];
      if (field_projection.kind == FieldProjection::Kind::kProjected) {
        size_t avro_field_index = std::get<size_t>(field_projection.from);
        (*avro_to_projection)[avro_field_index] = static_cast<int>(proj_idx);
      }
    }
  }

  // Read all Avro fields in order (must maintain decoder position)
  for (size_t avro_idx = 0; avro_idx < avro_node->leaves(); ++avro_idx) {
    int proj_idx = (*avro_to_projection)[avro_idx];

    if (proj_idx < 0) {
      // Skip this field - not in projection
      const auto& avro_field_node = avro_node->leafAt(avro_idx);
      ICEBERG_RETURN_UNEXPECTED(SkipAvroValue(avro_field_node, decoder));
    } else {
      // Decode this field
      const auto& field_projection = projections[proj_idx];
      const auto& expected_field = struct_type.fields()[proj_idx];
      const auto& avro_field_node = avro_node->leafAt(avro_idx);
      auto* field_builder = struct_builder->field_builder(proj_idx);

      ICEBERG_RETURN_UNEXPECTED(DecodeFieldToBuilder(avro_field_node, decoder,
                                                     field_projection, expected_field,
                                                     field_builder, ctx));
    }
  }

  // Handle null fields (fields in projection but not in Avro)
  for (size_t proj_idx = 0; proj_idx < projections.size(); ++proj_idx) {
    const auto& field_projection = projections[proj_idx];
    if (field_projection.kind == FieldProjection::Kind::kNull) {
      auto* field_builder = struct_builder->field_builder(static_cast<int>(proj_idx));
      ICEBERG_ARROW_RETURN_NOT_OK(field_builder->AppendNull());
    } else if (field_projection.kind != FieldProjection::Kind::kProjected) {
      return InvalidArgument("Unsupported field projection kind: {}",
                             static_cast<int>(field_projection.kind));
    }
  }
  return {};
}

/// \brief Decode Avro array directly to Arrow list builder.
Status DecodeListToBuilder(const ::avro::NodePtr& avro_node, ::avro::Decoder& decoder,
                           const FieldProjection& element_projection,
                           const ListType& list_type,
                           ::arrow::ArrayBuilder* array_builder, DecodeContext* ctx) {
  if (avro_node->type() != ::avro::AVRO_ARRAY) {
    return InvalidArgument("Expected Avro array, got type: {}", ToString(avro_node));
  }

  auto* list_builder = internal::checked_cast<::arrow::ListBuilder*>(array_builder);
  ICEBERG_ARROW_RETURN_NOT_OK(list_builder->Append());

  auto* value_builder = list_builder->value_builder();
  const auto& element_node = avro_node->leafAt(0);
  const auto& element_field = list_type.fields().back();

  // Read array block count
  int64_t block_count = decoder.arrayStart();
  while (block_count != 0) {
    for (int64_t i = 0; i < block_count; ++i) {
      ICEBERG_RETURN_UNEXPECTED(DecodeFieldToBuilder(
          element_node, decoder, element_projection, element_field, value_builder, ctx));
    }
    block_count = decoder.arrayNext();
  }

  return {};
}

/// \brief Decode Avro map directly to Arrow map builder.
Status DecodeMapToBuilder(const ::avro::NodePtr& avro_node, ::avro::Decoder& decoder,
                          const FieldProjection& key_projection,
                          const FieldProjection& value_projection,
                          const MapType& map_type, ::arrow::ArrayBuilder* array_builder,
                          DecodeContext* ctx) {
  auto* map_builder = internal::checked_cast<::arrow::MapBuilder*>(array_builder);

  if (avro_node->type() == ::avro::AVRO_MAP) {
    // Handle regular Avro map: map<string, value>
    const auto& key_node = avro_node->leafAt(0);
    const auto& value_node = avro_node->leafAt(1);
    const auto& key_field = map_type.key();
    const auto& value_field = map_type.value();

    ICEBERG_ARROW_RETURN_NOT_OK(map_builder->Append());
    auto* key_builder = map_builder->key_builder();
    auto* item_builder = map_builder->item_builder();

    // Read map block count
    int64_t block_count = decoder.mapStart();
    while (block_count != 0) {
      for (int64_t i = 0; i < block_count; ++i) {
        ICEBERG_RETURN_UNEXPECTED(DecodeFieldToBuilder(key_node, decoder, key_projection,
                                                       key_field, key_builder, ctx));
        ICEBERG_RETURN_UNEXPECTED(DecodeFieldToBuilder(
            value_node, decoder, value_projection, value_field, item_builder, ctx));
      }
      block_count = decoder.mapNext();
    }

    return {};
  } else if (avro_node->type() == ::avro::AVRO_ARRAY && HasMapLogicalType(avro_node)) {
    // Handle array-based map: list<struct<key, value>>
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

    // Read array block count
    int64_t block_count = decoder.arrayStart();
    while (block_count != 0) {
      for (int64_t i = 0; i < block_count; ++i) {
        ICEBERG_RETURN_UNEXPECTED(DecodeFieldToBuilder(key_node, decoder, key_projection,
                                                       key_field, key_builder, ctx));
        ICEBERG_RETURN_UNEXPECTED(DecodeFieldToBuilder(
            value_node, decoder, value_projection, value_field, item_builder, ctx));
      }
      block_count = decoder.arrayNext();
    }

    return {};
  } else {
    return InvalidArgument("Expected Avro map or array with map logical type, got: {}",
                           ToString(avro_node));
  }
}

/// \brief Decode nested Avro data directly to Arrow array builder.
Status DecodeNestedValueToBuilder(const ::avro::NodePtr& avro_node,
                                  ::avro::Decoder& decoder,
                                  const std::span<const FieldProjection>& projections,
                                  const NestedType& projected_type,
                                  ::arrow::ArrayBuilder* array_builder,
                                  DecodeContext* ctx) {
  switch (projected_type.type_id()) {
    case TypeId::kStruct: {
      const auto& struct_type = internal::checked_cast<const StructType&>(projected_type);
      return DecodeStructToBuilder(avro_node, decoder, projections, struct_type,
                                   array_builder, ctx);
    }

    case TypeId::kList: {
      if (projections.size() != 1) {
        return InvalidArgument("Expected 1 projection for list, got: {}",
                               projections.size());
      }
      const auto& list_type = internal::checked_cast<const ListType&>(projected_type);
      return DecodeListToBuilder(avro_node, decoder, projections[0], list_type,
                                 array_builder, ctx);
    }

    case TypeId::kMap: {
      if (projections.size() != 2) {
        return InvalidArgument("Expected 2 projections for map, got: {}",
                               projections.size());
      }
      const auto& map_type = internal::checked_cast<const MapType&>(projected_type);
      return DecodeMapToBuilder(avro_node, decoder, projections[0], projections[1],
                                map_type, array_builder, ctx);
    }

    default:
      return InvalidArgument("Unsupported nested type: {}", projected_type.ToString());
  }
}

Status DecodePrimitiveValueToBuilder(const ::avro::NodePtr& avro_node,
                                     ::avro::Decoder& decoder,
                                     const SchemaField& projected_field,
                                     ::arrow::ArrayBuilder* array_builder,
                                     DecodeContext* ctx) {
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
      bool value = decoder.decodeBool();
      ICEBERG_ARROW_RETURN_NOT_OK(builder->Append(value));
      return {};
    }

    case TypeId::kInt: {
      if (avro_node->type() != ::avro::AVRO_INT) {
        return InvalidArgument("Expected Avro int for int field, got: {}",
                               ToString(avro_node));
      }
      auto* builder = internal::checked_cast<::arrow::Int32Builder*>(array_builder);
      int32_t value = decoder.decodeInt();
      ICEBERG_ARROW_RETURN_NOT_OK(builder->Append(value));
      return {};
    }

    case TypeId::kLong: {
      auto* builder = internal::checked_cast<::arrow::Int64Builder*>(array_builder);
      if (avro_node->type() == ::avro::AVRO_LONG) {
        int64_t value = decoder.decodeLong();
        ICEBERG_ARROW_RETURN_NOT_OK(builder->Append(value));
      } else if (avro_node->type() == ::avro::AVRO_INT) {
        int32_t value = decoder.decodeInt();
        ICEBERG_ARROW_RETURN_NOT_OK(builder->Append(static_cast<int64_t>(value)));
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
      float value = decoder.decodeFloat();
      ICEBERG_ARROW_RETURN_NOT_OK(builder->Append(value));
      return {};
    }

    case TypeId::kDouble: {
      auto* builder = internal::checked_cast<::arrow::DoubleBuilder*>(array_builder);
      if (avro_node->type() == ::avro::AVRO_DOUBLE) {
        double value = decoder.decodeDouble();
        ICEBERG_ARROW_RETURN_NOT_OK(builder->Append(value));
      } else if (avro_node->type() == ::avro::AVRO_FLOAT) {
        float value = decoder.decodeFloat();
        ICEBERG_ARROW_RETURN_NOT_OK(builder->Append(static_cast<double>(value)));
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
      decoder.decodeString(ctx->string_scratch);
      ICEBERG_ARROW_RETURN_NOT_OK(builder->Append(ctx->string_scratch));
      return {};
    }

    case TypeId::kBinary: {
      if (avro_node->type() != ::avro::AVRO_BYTES) {
        return InvalidArgument("Expected Avro bytes for binary field, got: {}",
                               ToString(avro_node));
      }
      auto* builder = internal::checked_cast<::arrow::BinaryBuilder*>(array_builder);
      decoder.decodeBytes(ctx->bytes_scratch);
      ICEBERG_ARROW_RETURN_NOT_OK(builder->Append(
          ctx->bytes_scratch.data(), static_cast<int32_t>(ctx->bytes_scratch.size())));
      return {};
    }

    case TypeId::kFixed: {
      if (avro_node->type() != ::avro::AVRO_FIXED) {
        return InvalidArgument("Expected Avro fixed for fixed field, got: {}",
                               ToString(avro_node));
      }
      const auto& fixed_type = internal::checked_cast<const FixedType&>(projected_type);
      auto* builder =
          internal::checked_cast<::arrow::FixedSizeBinaryBuilder*>(array_builder);

      ctx->bytes_scratch.resize(fixed_type.length());
      decoder.decodeFixed(fixed_type.length(), ctx->bytes_scratch);
      ICEBERG_ARROW_RETURN_NOT_OK(builder->Append(ctx->bytes_scratch.data()));
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

      ctx->bytes_scratch.resize(16);
      decoder.decodeFixed(16, ctx->bytes_scratch);
      ICEBERG_ARROW_RETURN_NOT_OK(builder->Append(ctx->bytes_scratch.data()));
      return {};
    }

    case TypeId::kDecimal: {
      if (avro_node->type() != ::avro::AVRO_FIXED ||
          avro_node->logicalType().type() != ::avro::LogicalType::DECIMAL) {
        return InvalidArgument(
            "Expected Avro fixed with DECIMAL logical type for decimal field, got: {}",
            ToString(avro_node));
      }

      size_t byte_width = avro_node->fixedSize();
      auto* builder = internal::checked_cast<::arrow::Decimal128Builder*>(array_builder);

      ctx->bytes_scratch.resize(byte_width);
      decoder.decodeFixed(byte_width, ctx->bytes_scratch);
      ICEBERG_ARROW_ASSIGN_OR_RETURN(
          auto decimal, ::arrow::Decimal128::FromBigEndian(ctx->bytes_scratch.data(),
                                                           ctx->bytes_scratch.size()));
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
      int32_t value = decoder.decodeInt();
      ICEBERG_ARROW_RETURN_NOT_OK(builder->Append(value));
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
      int64_t value = decoder.decodeLong();
      ICEBERG_ARROW_RETURN_NOT_OK(builder->Append(value));
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
      int64_t value = decoder.decodeLong();
      ICEBERG_ARROW_RETURN_NOT_OK(builder->Append(value));
      return {};
    }

    default:
      return InvalidArgument("Unsupported primitive type {} to decode from avro node {}",
                             projected_field.type()->ToString(), ToString(avro_node));
  }
}

/// \brief Dispatch to appropriate handlers based on the projection kind.
Status DecodeFieldToBuilder(const ::avro::NodePtr& avro_node, ::avro::Decoder& decoder,
                            const FieldProjection& projection,
                            const SchemaField& projected_field,
                            ::arrow::ArrayBuilder* array_builder, DecodeContext* ctx) {
  if (avro_node->type() == ::avro::AVRO_UNION) {
    const size_t branch_index = decoder.decodeUnionIndex();

    // Validate branch index
    const size_t num_branches = avro_node->leaves();
    if (branch_index >= num_branches) {
      return InvalidArgument("Union branch index {} out of range [0, {})", branch_index,
                             num_branches);
    }

    const auto& branch_node = avro_node->leafAt(branch_index);
    if (branch_node->type() == ::avro::AVRO_NULL) {
      ICEBERG_ARROW_RETURN_NOT_OK(array_builder->AppendNull());
      return {};
    } else {
      return DecodeFieldToBuilder(branch_node, decoder, projection, projected_field,
                                  array_builder, ctx);
    }
  }

  const auto& projected_type = *projected_field.type();
  if (projected_type.is_primitive()) {
    return DecodePrimitiveValueToBuilder(avro_node, decoder, projected_field,
                                         array_builder, ctx);
  } else {
    const auto& nested_type = internal::checked_cast<const NestedType&>(projected_type);
    return DecodeNestedValueToBuilder(avro_node, decoder, projection.children,
                                      nested_type, array_builder, ctx);
  }
}

}  // namespace

Status DecodeAvroToBuilder(const ::avro::NodePtr& avro_node, ::avro::Decoder& decoder,
                           const SchemaProjection& projection,
                           const Schema& projected_schema,
                           ::arrow::ArrayBuilder* array_builder, DecodeContext* ctx) {
  return DecodeNestedValueToBuilder(avro_node, decoder, projection.fields,
                                    projected_schema, array_builder, ctx);
}

}  // namespace iceberg::avro
