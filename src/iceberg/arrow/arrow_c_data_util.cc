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
#include <vector>

#include <arrow/array/array_primitive.h>
#include <arrow/buffer.h>
#include <arrow/c/bridge.h>
#include <arrow/compute/api_vector.h>
#include <arrow/record_batch.h>
#include <nanoarrow/nanoarrow.h>

#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/arrow/nanoarrow_status_internal.h"
#include "iceberg/arrow_c_data_guard_internal.h"
#include "iceberg/arrow_c_data_util_internal.h"
#include "iceberg/result.h"
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
  const int32_t* row_indices_data =
      row_indices.empty() ? &empty_index : row_indices.data();
  auto index_array = std::make_shared<::arrow::Int32Array>(
      static_cast<int64_t>(row_indices.size()),
      ::arrow::Buffer::Wrap(row_indices_data, row_indices.size()));

  std::vector<std::shared_ptr<::arrow::Array>> output_columns;
  output_columns.reserve(projection.selected_field_indices().size());
  for (int input_index : projection.selected_field_indices()) {
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

}  // namespace iceberg
