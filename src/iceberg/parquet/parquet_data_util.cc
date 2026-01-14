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

#include <arrow/array.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/compute/api.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/parquet/parquet_data_util_internal.h"
#include "iceberg/schema.h"
#include "iceberg/schema_util.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/macros.h"

namespace iceberg::parquet {

namespace {

// Forward declaration
Result<std::shared_ptr<::arrow::Array>> ProjectNestedArray(
    const std::shared_ptr<::arrow::Array>& array,
    const std::shared_ptr<::arrow::DataType>& output_arrow_type,
    const NestedType& nested_type, std::span<const FieldProjection> projections,
    const arrow::MetadataColumnContext& metadata_context, ::arrow::MemoryPool* pool);

/// \brief Create a null array of the given type and length.
Result<std::shared_ptr<::arrow::Array>> MakeNullArray(
    const std::shared_ptr<::arrow::DataType>& type, int64_t length,
    ::arrow::MemoryPool* pool) {
  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto builder, ::arrow::MakeBuilder(type, pool));
  ICEBERG_ARROW_RETURN_NOT_OK(builder->AppendNulls(length));
  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto array, builder->Finish());
  return array;
}

Result<std::shared_ptr<::arrow::Array>> ProjectPrimitiveArray(
    const std::shared_ptr<::arrow::Array>& array,
    const std::shared_ptr<::arrow::DataType>& output_arrow_type,
    ::arrow::MemoryPool* pool) {
  if (array->type()->Equals(output_arrow_type)) {
    return array;
  }

  // Use Arrow compute cast function for type conversions.
  // Note: We don't check the schema evolution rule again because projecting schemas
  // has checked this.
  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto cast_result,
                                 ::arrow::compute::Cast(array, output_arrow_type));
  return cast_result.make_array();
}

Result<std::shared_ptr<::arrow::Array>> ProjectStructArray(
    const std::shared_ptr<::arrow::StructArray>& struct_array,
    const std::shared_ptr<::arrow::StructType>& output_struct_type,
    const StructType& struct_type, std::span<const FieldProjection> projections,
    const arrow::MetadataColumnContext& metadata_context, ::arrow::MemoryPool* pool) {
  if (struct_type.fields().size() != projections.size()) {
    return InvalidSchema(
        "Inconsistent number of fields ({}) and number of projections ({})",
        struct_type.fields().size(), projections.size());
  }
  if (struct_type.fields().size() != output_struct_type->num_fields()) {
    return InvalidSchema(
        "Inconsistent number of fields ({}) and number of output fields ({})",
        struct_type.fields().size(), output_struct_type->num_fields());
  }

  std::vector<std::shared_ptr<::arrow::Array>> projected_arrays;
  projected_arrays.reserve(projections.size());

  for (size_t i = 0; i < projections.size(); ++i) {
    const auto& projected_field = struct_type.fields()[i];
    const auto& field_projection = projections[i];
    const auto& output_arrow_type = output_struct_type->fields()[i]->type();

    std::shared_ptr<::arrow::Array> projected_array;

    if (field_projection.kind == FieldProjection::Kind::kProjected) {
      auto parquet_field_index =
          static_cast<int>(std::get<size_t>(field_projection.from));
      if (parquet_field_index >= struct_array->num_fields()) {
        return InvalidArgument("Parquet field index {} out of bound {}",
                               parquet_field_index, struct_array->num_fields());
      }
      const auto& parquet_array = struct_array->field(parquet_field_index);
      if (projected_field.type()->is_nested()) {
        const auto& nested_type =
            internal::checked_cast<const NestedType&>(*projected_field.type());
        ICEBERG_ASSIGN_OR_RAISE(
            projected_array,
            ProjectNestedArray(parquet_array, output_arrow_type, nested_type,
                               field_projection.children, metadata_context, pool));
      } else {
        ICEBERG_ASSIGN_OR_RAISE(
            projected_array,
            ProjectPrimitiveArray(parquet_array, output_arrow_type, pool));
      }
    } else if (field_projection.kind == FieldProjection::Kind::kNull) {
      ICEBERG_ASSIGN_OR_RAISE(
          projected_array,
          MakeNullArray(output_arrow_type, struct_array->length(), pool));
    } else if (field_projection.kind == FieldProjection::Kind::kMetadata) {
      int32_t field_id = projected_field.field_id();
      if (field_id == MetadataColumns::kFilePathColumnId) {
        ICEBERG_ASSIGN_OR_RAISE(projected_array,
                                arrow::MakeFilePathArray(metadata_context.file_path,
                                                         struct_array->length(), pool));
      } else if (field_id == MetadataColumns::kFilePositionColumnId) {
        ICEBERG_ASSIGN_OR_RAISE(
            projected_array, arrow::MakeRowPositionArray(metadata_context.next_file_pos,
                                                         struct_array->length(), pool));
      } else {
        return NotSupported("Unsupported metadata field id: {}", field_id);
      }
    } else {
      return NotImplemented("Unsupported field projection kind: {}",
                            ToString(field_projection.kind));
    }

    projected_arrays.emplace_back(std::move(projected_array));
  }

  ICEBERG_ARROW_ASSIGN_OR_RETURN(
      auto output_array,
      ::arrow::StructArray::Make(projected_arrays, output_struct_type->fields(),
                                 struct_array->null_bitmap(), struct_array->null_count(),
                                 struct_array->offset()));
  return output_array;
}

///  Templated implementation for projecting list arrays.
/// Works with both ListArray/ListType (32-bit offsets) and
/// LargeListArray/LargeListType (64-bit offsets).
template <typename ArrowListArrayType, typename ArrowListType>
Result<std::shared_ptr<::arrow::Array>> ProjectListArrayImpl(
    const std::shared_ptr<ArrowListArrayType>& list_array,
    const std::shared_ptr<ArrowListType>& output_list_type, const ListType& list_type,
    std::span<const FieldProjection> projections,
    const arrow::MetadataColumnContext& metadata_context, ::arrow::MemoryPool* pool) {
  if (projections.size() != 1) {
    return InvalidArgument("Expected 1 projection for list, got: {}", projections.size());
  }

  const auto& element_field = list_type.fields().back();
  const auto& element_projection = projections[0];
  const auto& output_element_type = output_list_type->value_type();

  std::shared_ptr<::arrow::Array> projected_values;
  if (element_field.type()->is_nested()) {
    const auto& nested_type =
        internal::checked_cast<const NestedType&>(*element_field.type());
    ICEBERG_ASSIGN_OR_RAISE(
        projected_values,
        ProjectNestedArray(list_array->values(), output_element_type, nested_type,
                           element_projection.children, metadata_context, pool));
  } else {
    ICEBERG_ASSIGN_OR_RAISE(
        projected_values,
        ProjectPrimitiveArray(list_array->values(), output_element_type, pool));
  }

  return std::make_shared<ArrowListArrayType>(
      output_list_type, list_array->length(), list_array->value_offsets(),
      std::move(projected_values), list_array->null_bitmap(), list_array->null_count(),
      list_array->offset());
}

Result<std::shared_ptr<::arrow::Array>> ProjectListArray(
    const std::shared_ptr<::arrow::ListArray>& list_array,
    const std::shared_ptr<::arrow::ListType>& output_list_type, const ListType& list_type,
    std::span<const FieldProjection> projections,
    const arrow::MetadataColumnContext& metadata_context, ::arrow::MemoryPool* pool) {
  return ProjectListArrayImpl(list_array, output_list_type, list_type, projections,
                              metadata_context, pool);
}

Result<std::shared_ptr<::arrow::Array>> ProjectLargeListArray(
    const std::shared_ptr<::arrow::LargeListArray>& list_array,
    const std::shared_ptr<::arrow::LargeListType>& output_list_type,
    const ListType& list_type, std::span<const FieldProjection> projections,
    const arrow::MetadataColumnContext& metadata_context, ::arrow::MemoryPool* pool) {
  return ProjectListArrayImpl(list_array, output_list_type, list_type, projections,
                              metadata_context, pool);
}

Result<std::shared_ptr<::arrow::Array>> ProjectMapArray(
    const std::shared_ptr<::arrow::MapArray>& map_array,
    const std::shared_ptr<::arrow::MapType>& output_map_type, const MapType& map_type,
    std::span<const FieldProjection> projections,
    const arrow::MetadataColumnContext& metadata_context, ::arrow::MemoryPool* pool) {
  if (projections.size() != 2) {
    return InvalidArgument("Expected 2 projections for map, got: {}", projections.size());
  }

  const auto& key_projection = projections[0];
  const auto& value_projection = projections[1];
  const auto& key_type = map_type.key().type();
  const auto& value_type = map_type.value().type();

  // Project keys
  std::shared_ptr<::arrow::Array> projected_keys;
  if (key_type->is_nested()) {
    const auto& nested_type = internal::checked_cast<const NestedType&>(*key_type);
    ICEBERG_ASSIGN_OR_RAISE(
        projected_keys,
        ProjectNestedArray(map_array->keys(), output_map_type->key_type(), nested_type,
                           key_projection.children, metadata_context, pool));
  } else {
    ICEBERG_ASSIGN_OR_RAISE(
        projected_keys,
        ProjectPrimitiveArray(map_array->keys(), output_map_type->key_type(), pool));
  }

  // Project values
  std::shared_ptr<::arrow::Array> projected_items;
  if (value_type->is_nested()) {
    const auto& nested_type = internal::checked_cast<const NestedType&>(*value_type);
    ICEBERG_ASSIGN_OR_RAISE(
        projected_items,
        ProjectNestedArray(map_array->items(), output_map_type->item_type(), nested_type,
                           value_projection.children, metadata_context, pool));
  } else {
    ICEBERG_ASSIGN_OR_RAISE(
        projected_items,
        ProjectPrimitiveArray(map_array->items(), output_map_type->item_type(), pool));
  }

  return std::make_shared<::arrow::MapArray>(
      output_map_type, map_array->length(), map_array->value_offsets(),
      std::move(projected_keys), std::move(projected_items), map_array->null_bitmap(),
      map_array->null_count(), map_array->offset());
}

Result<std::shared_ptr<::arrow::Array>> ProjectNestedArray(
    const std::shared_ptr<::arrow::Array>& array,
    const std::shared_ptr<::arrow::DataType>& output_arrow_type,
    const NestedType& nested_type, std::span<const FieldProjection> projections,
    const arrow::MetadataColumnContext& metadata_context, ::arrow::MemoryPool* pool) {
  switch (nested_type.type_id()) {
    case TypeId::kStruct: {
      if (output_arrow_type->id() != ::arrow::Type::STRUCT) {
        return InvalidSchema("Expected struct type, got: {}",
                             output_arrow_type->ToString());
      }
      auto struct_array = internal::checked_pointer_cast<::arrow::StructArray>(array);
      auto output_struct_type =
          internal::checked_pointer_cast<::arrow::StructType>(output_arrow_type);
      const auto& struct_type = internal::checked_cast<const StructType&>(nested_type);
      return ProjectStructArray(struct_array, output_struct_type, struct_type,
                                projections, metadata_context, pool);
    }
    case TypeId::kList: {
      const auto& list_type = internal::checked_cast<const ListType&>(nested_type);

      if (output_arrow_type->id() == ::arrow::Type::LIST) {
        auto list_array = internal::checked_pointer_cast<::arrow::ListArray>(array);
        auto output_list_type =
            internal::checked_pointer_cast<::arrow::ListType>(output_arrow_type);
        return ProjectListArray(list_array, output_list_type, list_type, projections,
                                metadata_context, pool);
      }

      if (output_arrow_type->id() == ::arrow::Type::LARGE_LIST) {
        auto list_array = internal::checked_pointer_cast<::arrow::LargeListArray>(array);
        auto output_list_type =
            internal::checked_pointer_cast<::arrow::LargeListType>(output_arrow_type);
        return ProjectLargeListArray(list_array, output_list_type, list_type, projections,
                                     metadata_context, pool);
      }

      return InvalidSchema("Expected list or large_list type, got: {}",
                           output_arrow_type->ToString());
    }
    case TypeId::kMap: {
      if (output_arrow_type->id() != ::arrow::Type::MAP) {
        return InvalidSchema("Expected map type, got: {}", output_arrow_type->ToString());
      }

      auto map_array = internal::checked_pointer_cast<::arrow::MapArray>(array);
      auto output_map_type =
          internal::checked_pointer_cast<::arrow::MapType>(output_arrow_type);
      const auto& map_type = internal::checked_cast<const MapType&>(nested_type);
      return ProjectMapArray(map_array, output_map_type, map_type, projections,
                             metadata_context, pool);
    }
    default:
      return InvalidSchema("Cannot project array of unsupported nested type: {}",
                           nested_type.ToString());
  }
}

}  // namespace

Result<std::shared_ptr<::arrow::RecordBatch>> ProjectRecordBatch(
    std::shared_ptr<::arrow::RecordBatch> record_batch,
    const std::shared_ptr<::arrow::Schema>& output_arrow_schema,
    const Schema& projected_schema, const SchemaProjection& projection,
    const arrow::MetadataColumnContext& metadata_context, ::arrow::MemoryPool* pool) {
  auto array = std::make_shared<::arrow::StructArray>(
      ::arrow::struct_(record_batch->schema()->fields()), record_batch->num_rows(),
      record_batch->columns());
  ICEBERG_ASSIGN_OR_RAISE(
      auto output_array,
      ProjectNestedArray(array, ::arrow::struct_(output_arrow_schema->fields()),
                         projected_schema, projection.fields, metadata_context, pool));
  auto* struct_array = internal::checked_cast<::arrow::StructArray*>(output_array.get());
  return ::arrow::RecordBatch::Make(output_arrow_schema, record_batch->num_rows(),
                                    struct_array->fields());
}

}  // namespace iceberg::parquet
