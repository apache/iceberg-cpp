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

#include <cstdint>
#include <memory>
#include <mutex>
#include <span>
#include <utility>
#include <variant>
#include <vector>

#include <arrow/array.h>
#include <arrow/array/array_primitive.h>
#include <arrow/buffer.h>
#include <arrow/c/bridge.h>
#include <arrow/compute/api_vector.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <nanoarrow/nanoarrow.h>

#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/arrow/literal_util_internal.h"
#include "iceberg/arrow_c_data_guard_internal.h"
#include "iceberg/arrow_c_data_util_internal.h"
#include "iceberg/nanoarrow_status_internal.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/schema_internal.h"
#include "iceberg/schema_util.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/formatter.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

struct ArrowProjectBatchState {
  std::shared_ptr<::arrow::Schema> input_schema;
  std::shared_ptr<::arrow::Schema> output_schema;
};

Result<std::shared_ptr<::arrow::Schema>> ImportArrowSchema(
    const ArrowSchema& arrow_schema) {
  ArrowSchema schema_copy;
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowSchemaDeepCopy(&arrow_schema, &schema_copy));
  internal::ArrowSchemaGuard schema_copy_guard(&schema_copy);

  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto schema, ::arrow::ImportSchema(&schema_copy));
  return schema;
}

Result<std::shared_ptr<ArrowProjectBatchState>> GetArrowProjectBatchState(
    ProjectionContext& projection) {
  auto state =
      std::static_pointer_cast<ArrowProjectBatchState>(projection.project_batch_state());
  if (state != nullptr) {
    return state;
  }

  ICEBERG_ASSIGN_OR_RAISE(auto input_schema,
                          ImportArrowSchema(projection.input_arrow_schema()));
  ICEBERG_ASSIGN_OR_RAISE(auto output_schema,
                          ImportArrowSchema(projection.output_arrow_schema()));

  state = std::make_shared<ArrowProjectBatchState>(
      ArrowProjectBatchState{.input_schema = std::move(input_schema),
                             .output_schema = std::move(output_schema)});
  projection.project_batch_state() = state;
  return state;
}

Result<ArrowArray> ProjectBatchArrowCompute(ArrowArray* input_batch,
                                            std::span<const int32_t> row_indices,
                                            ProjectionContext& projection) {
  ICEBERG_PRECHECK(input_batch != nullptr, "input_batch must not be null");
  ICEBERG_ASSIGN_OR_RAISE(auto state, GetArrowProjectBatchState(projection));

  ICEBERG_ARROW_ASSIGN_OR_RETURN(
      auto input_record_batch,
      ::arrow::ImportRecordBatch(input_batch, state->input_schema));

  const int32_t empty_index = 0;
  // Buffer::Wrap needs a valid pointer even when the zero-length buffer is never read.
  const int32_t* row_indices_data =
      row_indices.empty() ? &empty_index : row_indices.data();
  auto index_array = std::make_shared<::arrow::Int32Array>(
      static_cast<int64_t>(row_indices.size()),
      ::arrow::Buffer::Wrap(row_indices_data, row_indices.size()));

  std::vector<std::shared_ptr<::arrow::Array>> output_columns;
  output_columns.reserve(projection.selected_field_indices().size());
  for (int32_t input_index : projection.selected_field_indices()) {
    ICEBERG_PRECHECK(input_index >= 0 && input_index < input_record_batch->num_columns(),
                     "Input field index {} out of range for batch with {} columns",
                     input_index, input_record_batch->num_columns());
    ICEBERG_ARROW_ASSIGN_OR_RETURN(
        auto taken_column,
        ::arrow::compute::Take(*input_record_batch->column(input_index), *index_array));
    output_columns.push_back(std::move(taken_column));
  }

  auto output_record_batch = ::arrow::RecordBatch::Make(
      state->output_schema, static_cast<int64_t>(row_indices.size()),
      std::move(output_columns));

  ArrowArray output_array;
  ICEBERG_ARROW_RETURN_NOT_OK(
      ::arrow::ExportRecordBatch(*output_record_batch, &output_array));
  internal::ArrowArrayGuard output_array_guard(&output_array);

  return std::exchange(output_array, ArrowArray{});
}

}  // namespace

void RegisterArrowProjectBatch() {
  static std::once_flag flag;
  std::call_once(flag, []() {
    ProjectionContext::RegisterProjectBatchFunction(&ProjectBatchArrowCompute);
  });
}

namespace arrow {
namespace {

Result<std::shared_ptr<::arrow::Schema>> ImportIcebergSchema(const Schema& schema) {
  ArrowSchema c_schema;
  ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(schema, &c_schema));
  internal::ArrowSchemaGuard c_schema_guard(&c_schema);
  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto arrow_schema, ::arrow::ImportSchema(&c_schema));
  c_schema_guard.Release();
  return arrow_schema;
}

// Writers must not silently drop data: every input field, at any struct nesting level,
// has to exist in the write schema.
Status RejectUnknownInputFields(const StructType& input_struct,
                                const StructType& write_struct) {
  for (const auto& input_field : input_struct.fields()) {
    ICEBERG_ASSIGN_OR_RAISE(auto write_field,
                            write_struct.GetFieldById(input_field.field_id()));
    if (!write_field.has_value()) {
      return InvalidSchema("Input field id {} is not present in write schema",
                           input_field.field_id());
    }
    if (input_field.type()->type_id() == TypeId::kStruct &&
        write_field->get().type()->type_id() == TypeId::kStruct) {
      ICEBERG_RETURN_UNEXPECTED(RejectUnknownInputFields(
          internal::checked_cast<const StructType&>(*input_field.type()),
          internal::checked_cast<const StructType&>(*write_field->get().type())));
    }
  }
  return {};
}

Result<std::vector<std::shared_ptr<::arrow::Array>>> MaterializeProjection(
    std::span<const FieldProjection> projections,
    const std::vector<std::shared_ptr<::arrow::Array>>& input_arrays,
    const std::vector<std::shared_ptr<::arrow::Field>>& write_arrow_fields, int64_t length,
    ::arrow::MemoryPool* pool) {
  std::vector<std::shared_ptr<::arrow::Array>> result;
  result.reserve(projections.size());
  for (size_t index = 0; index < projections.size(); ++index) {
    const auto& projection = projections[index];
    const auto& write_arrow_type = write_arrow_fields[index]->type();
    switch (projection.kind) {
      case FieldProjection::Kind::kProjected: {
        const auto& input_array = input_arrays[std::get<size_t>(projection.from)];
        if (projection.children.empty()) {
          result.emplace_back(input_array);
          break;
        }
        auto input_struct = std::dynamic_pointer_cast<::arrow::StructArray>(input_array);
        auto write_struct_type =
            std::dynamic_pointer_cast<::arrow::StructType>(write_arrow_type);
        if (input_struct == nullptr || write_struct_type == nullptr) {
          return InvalidArrowData("Expected struct array for projected field {}",
                                  index);
        }
        ICEBERG_ASSIGN_OR_RAISE(
            auto children,
            MaterializeProjection(projection.children, input_struct->fields(),
                                  write_struct_type->fields(), input_struct->length(),
                                  pool));
        if (children.empty()) {
          result.emplace_back(std::make_shared<::arrow::StructArray>(
              write_struct_type, input_struct->length(), std::move(children),
              input_struct->null_bitmap(), input_struct->null_count(),
              input_struct->offset()));
          break;
        }
        ICEBERG_ARROW_ASSIGN_OR_RETURN(
            auto struct_array,
            ::arrow::StructArray::Make(std::move(children), write_struct_type->fields(),
                                       input_struct->null_bitmap(),
                                       input_struct->null_count(),
                                       input_struct->offset()));
        result.emplace_back(std::move(struct_array));
        break;
      }
      case FieldProjection::Kind::kDefault: {
        ICEBERG_ASSIGN_OR_RAISE(
            auto default_array,
            MakeDefaultArray(std::get<Literal>(projection.from), write_arrow_type,
                             length, pool));
        result.emplace_back(std::move(default_array));
        break;
      }
      case FieldProjection::Kind::kNull: {
        ICEBERG_ARROW_ASSIGN_OR_RETURN(
            auto null_array, ::arrow::MakeArrayOfNull(write_arrow_type, length, pool));
        result.emplace_back(std::move(null_array));
        break;
      }
      default:
        return NotSupported("Projection kind {} is not supported for write alignment",
                            ToString(projection.kind));
    }
  }
  return result;
}

}  // namespace

Result<ArrowArray> AlignBatchForWrite(ArrowArray* input_batch, const Schema& input_schema,
                                      const Schema& write_schema,
                                      ::arrow::MemoryPool* pool) {
  ICEBERG_PRECHECK(input_batch != nullptr, "input_batch must not be null");
  ICEBERG_PRECHECK(pool != nullptr, "pool must not be null");
  internal::ArrowArrayGuard input_guard(input_batch);

  ICEBERG_ASSIGN_OR_RAISE(auto input_arrow_schema, ImportIcebergSchema(input_schema));
  ICEBERG_ARROW_ASSIGN_OR_RETURN(
      auto input_record_batch,
      ::arrow::ImportRecordBatch(input_batch, input_arrow_schema));
  input_guard.Release();

  ICEBERG_ASSIGN_OR_RAISE(auto write_arrow_schema, ImportIcebergSchema(write_schema));

  ICEBERG_RETURN_UNEXPECTED(RejectUnknownInputFields(input_schema, write_schema));
  ICEBERG_ASSIGN_OR_RAISE(
      auto projection,
      Project(write_schema, input_schema, /*prune_source=*/false,
              ProjectionOptions{
                  .default_policy = ProjectionOptions::DefaultPolicy::kWrite,
                  .allow_type_promotion = false}));
  ICEBERG_ASSIGN_OR_RAISE(
      auto columns,
      MaterializeProjection(projection.fields, input_record_batch->columns(),
                            write_arrow_schema->fields(),
                            input_record_batch->num_rows(), pool));
  auto output_record_batch = ::arrow::RecordBatch::Make(
      write_arrow_schema, input_record_batch->num_rows(), std::move(columns));

  ArrowArray output{};
  ICEBERG_ARROW_RETURN_NOT_OK(::arrow::ExportRecordBatch(*output_record_batch, &output));
  internal::ArrowArrayGuard output_guard(&output);
  return std::exchange(output, ArrowArray{});
}

}  // namespace arrow

}  // namespace iceberg
