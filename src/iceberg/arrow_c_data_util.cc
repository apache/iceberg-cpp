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
#include <limits>
#include <memory>
#include <mutex>
#include <span>
#include <utility>
#include <vector>

#include <nanoarrow/nanoarrow.h>

#include "iceberg/arrow/nanoarrow_status_internal.h"
#include "iceberg/arrow_c_data_guard_internal.h"
#include "iceberg/arrow_c_data_util_internal.h"
#include "iceberg/file_reader.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/schema_internal.h"
#include "iceberg/util/macros.h"

namespace iceberg {

Result<ArrowArrayStream> MakeArrowArrayStream(std::unique_ptr<Reader> reader) {
  return MakeArrowArrayStream<Reader>(std::move(reader));
}

namespace {

Result<size_t> FindFieldIndexById(std::span<const SchemaField> fields, int32_t field_id) {
  for (size_t index = 0; index < fields.size(); ++index) {
    if (fields[index].field_id() == field_id) {
      return index;
    }
  }
  return InvalidArgument("Required schema does not contain projected field id {}",
                         field_id);
}

std::mutex g_project_batch_function_mutex;
ProjectionContext::ProjectBatchFunction g_project_batch_function = nullptr;

ProjectionContext::ProjectBatchFunction GetProjectBatchFunction() {
  std::lock_guard lock(g_project_batch_function_mutex);
  return g_project_batch_function;
}

Result<std::vector<int>> BuildSelectedFieldIndices(
    std::span<const SchemaField> input_fields,
    std::span<const SchemaField> output_fields) {
  std::vector<int> selected_field_indices;
  selected_field_indices.reserve(output_fields.size());

  for (const auto& output_field : output_fields) {
    ICEBERG_ASSIGN_OR_RAISE(auto input_index,
                            FindFieldIndexById(input_fields, output_field.field_id()));
    const auto& input_field = input_fields[input_index];
    if (*input_field.type() != *output_field.type()) {
      return InvalidArgument(
          "ProjectBatch only supports complete top-level fields, but field id "
          "{} changes type from {} to {}",
          output_field.field_id(), input_field.type()->ToString(),
          output_field.type()->ToString());
    }
    ICEBERG_PRECHECK(input_index <= static_cast<size_t>(std::numeric_limits<int>::max()),
                     "Input field index {} exceeds int range", input_index);
    selected_field_indices.push_back(static_cast<int>(input_index));
  }

  return selected_field_indices;
}

Status AppendValue(const ArrowSchema& input_schema, const ArrowArray& input_array,
                   const ArrowArrayView& input_view, int64_t row_index,
                   ArrowArray* output_array);

Status AppendListValues(const ArrowSchema& input_schema, const ArrowArray& input_array,
                        const ArrowArrayView& input_view, int64_t row_index,
                        ArrowArray* output_array) {
  const int64_t begin = ArrowArrayViewListChildOffset(&input_view, row_index);
  const int64_t end = ArrowArrayViewListChildOffset(&input_view, row_index + 1);
  for (int64_t element_index = begin; element_index < end; ++element_index) {
    ICEBERG_RETURN_UNEXPECTED(
        AppendValue(*input_schema.children[0], *input_array.children[0],
                    *input_view.children[0], element_index, output_array->children[0]));
  }
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(output_array));
  return {};
}

Status AppendMapValues(const ArrowSchema& input_schema, const ArrowArray& input_array,
                       const ArrowArrayView& input_view, int64_t row_index,
                       ArrowArray* output_array) {
  const int64_t begin = ArrowArrayViewListChildOffset(&input_view, row_index);
  const int64_t end = ArrowArrayViewListChildOffset(&input_view, row_index + 1);
  for (int64_t entry_index = begin; entry_index < end; ++entry_index) {
    ICEBERG_RETURN_UNEXPECTED(
        AppendValue(*input_schema.children[0], *input_array.children[0],
                    *input_view.children[0], entry_index, output_array->children[0]));
  }
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(output_array));
  return {};
}

Status AppendDecimal(const ArrowSchema& input_schema, const ArrowArrayView& input_view,
                     int64_t row_index, ArrowArray* output_array) {
  ArrowError error;
  ArrowSchemaView schema_view;
  ICEBERG_NANOARROW_RETURN_UNEXPECTED_WITH_ERROR(
      ArrowSchemaViewInit(&schema_view, &input_schema, &error), error);

  ArrowDecimal value;
  ArrowDecimalInit(&value, schema_view.decimal_bitwidth, schema_view.decimal_precision,
                   schema_view.decimal_scale);
  ArrowArrayViewGetDecimalUnsafe(&input_view, row_index, &value);
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendDecimal(output_array, &value));
  return {};
}

Status AppendValue(const ArrowSchema& input_schema, const ArrowArray& input_array,
                   const ArrowArrayView& input_view, int64_t row_index,
                   ArrowArray* output_array) {
  if (ArrowArrayViewIsNull(&input_view, row_index)) {
    ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendNull(output_array, 1));
    return {};
  }

  switch (input_view.storage_type) {
    case NANOARROW_TYPE_NA:
      ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendNull(output_array, 1));
      return {};
    case NANOARROW_TYPE_BOOL:
    case NANOARROW_TYPE_INT8:
    case NANOARROW_TYPE_INT16:
    case NANOARROW_TYPE_INT32:
    case NANOARROW_TYPE_INT64:
    case NANOARROW_TYPE_DATE32:
    case NANOARROW_TYPE_DATE64:
    case NANOARROW_TYPE_TIME32:
    case NANOARROW_TYPE_TIME64:
    case NANOARROW_TYPE_TIMESTAMP:
    case NANOARROW_TYPE_DURATION:
      ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendInt(
          output_array, ArrowArrayViewGetIntUnsafe(&input_view, row_index)));
      return {};
    case NANOARROW_TYPE_UINT8:
    case NANOARROW_TYPE_UINT16:
    case NANOARROW_TYPE_UINT32:
    case NANOARROW_TYPE_UINT64:
      ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendUInt(
          output_array, ArrowArrayViewGetUIntUnsafe(&input_view, row_index)));
      return {};
    case NANOARROW_TYPE_HALF_FLOAT:
    case NANOARROW_TYPE_FLOAT:
    case NANOARROW_TYPE_DOUBLE:
      ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendDouble(
          output_array, ArrowArrayViewGetDoubleUnsafe(&input_view, row_index)));
      return {};
    case NANOARROW_TYPE_STRING:
    case NANOARROW_TYPE_LARGE_STRING:
    case NANOARROW_TYPE_STRING_VIEW: {
      auto value = ArrowArrayViewGetStringUnsafe(&input_view, row_index);
      ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendString(output_array, value));
      return {};
    }
    case NANOARROW_TYPE_BINARY:
    case NANOARROW_TYPE_LARGE_BINARY:
    case NANOARROW_TYPE_FIXED_SIZE_BINARY:
    case NANOARROW_TYPE_BINARY_VIEW: {
      auto value = ArrowArrayViewGetBytesUnsafe(&input_view, row_index);
      ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendBytes(output_array, value));
      return {};
    }
    case NANOARROW_TYPE_DECIMAL128:
    case NANOARROW_TYPE_DECIMAL256:
      return AppendDecimal(input_schema, input_view, row_index, output_array);
    case NANOARROW_TYPE_STRUCT: {
      for (int64_t child_index = 0; child_index < input_schema.n_children;
           ++child_index) {
        ICEBERG_RETURN_UNEXPECTED(AppendValue(
            *input_schema.children[child_index], *input_array.children[child_index],
            *input_view.children[child_index], row_index,
            output_array->children[child_index]));
      }
      ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(output_array));
      return {};
    }
    case NANOARROW_TYPE_LIST:
    case NANOARROW_TYPE_LARGE_LIST:
    case NANOARROW_TYPE_FIXED_SIZE_LIST:
      return AppendListValues(input_schema, input_array, input_view, row_index,
                              output_array);
    case NANOARROW_TYPE_MAP:
      return AppendMapValues(input_schema, input_array, input_view, row_index,
                             output_array);
    default:
      return NotImplemented("Unsupported Arrow type for merge-on-read projection: {}",
                            static_cast<int>(input_view.storage_type));
  }
}

}  // namespace

ProjectionContext::ProjectionContext(ProjectionContext&& other) noexcept
    : input_schema_(std::exchange(other.input_schema_, nullptr)),
      output_schema_(std::exchange(other.output_schema_, nullptr)),
      selected_field_indices_(std::move(other.selected_field_indices_)),
      input_arrow_schema_(other.input_arrow_schema_),
      output_arrow_schema_(other.output_arrow_schema_),
      project_batch_function_(std::exchange(other.project_batch_function_, nullptr)),
      project_batch_state_(std::move(other.project_batch_state_)) {
  other.input_arrow_schema_.release = nullptr;
  other.output_arrow_schema_.release = nullptr;
}

ProjectionContext& ProjectionContext::operator=(ProjectionContext&& other) noexcept {
  if (this == &other) {
    return *this;
  }

  if (input_arrow_schema_.release != nullptr) {
    input_arrow_schema_.release(&input_arrow_schema_);
  }
  if (output_arrow_schema_.release != nullptr) {
    output_arrow_schema_.release(&output_arrow_schema_);
  }

  input_schema_ = std::exchange(other.input_schema_, nullptr);
  output_schema_ = std::exchange(other.output_schema_, nullptr);
  selected_field_indices_ = std::move(other.selected_field_indices_);
  input_arrow_schema_ = other.input_arrow_schema_;
  other.input_arrow_schema_.release = nullptr;
  output_arrow_schema_ = other.output_arrow_schema_;
  other.output_arrow_schema_.release = nullptr;
  project_batch_function_ = std::exchange(other.project_batch_function_, nullptr);
  project_batch_state_ = std::move(other.project_batch_state_);
  return *this;
}

ProjectionContext::~ProjectionContext() {
  if (input_arrow_schema_.release != nullptr) {
    input_arrow_schema_.release(&input_arrow_schema_);
  }
  if (output_arrow_schema_.release != nullptr) {
    output_arrow_schema_.release(&output_arrow_schema_);
  }
}

Result<ProjectionContext> ProjectionContext::Make(
    const Schema& input_schema, const Schema& output_schema,
    ProjectionContext::ProjectBatchFunction project_batch_function) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto selected_field_indices,
      BuildSelectedFieldIndices(input_schema.fields(), output_schema.fields()));

  ProjectionContext context;
  context.input_schema_ = &input_schema;
  context.output_schema_ = &output_schema;
  context.selected_field_indices_ = std::move(selected_field_indices);
  ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(input_schema, &context.input_arrow_schema_));
  ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(output_schema, &context.output_arrow_schema_));
  context.project_batch_function_ = project_batch_function;

  return context;
}

const Schema& ProjectionContext::input_schema() const { return *input_schema_; }

const Schema& ProjectionContext::output_schema() const { return *output_schema_; }

const ArrowSchema& ProjectionContext::input_arrow_schema() const {
  return input_arrow_schema_;
}

const ArrowSchema& ProjectionContext::output_arrow_schema() const {
  return output_arrow_schema_;
}

std::span<const int> ProjectionContext::selected_field_indices() const {
  return selected_field_indices_;
}

ProjectionContext::ProjectBatchFunction ProjectionContext::project_batch_function()
    const {
  return project_batch_function_;
}

ProjectionContext::ProjectBatchState& ProjectionContext::project_batch_state() {
  return project_batch_state_;
}

void ProjectionContext::RegisterProjectBatchFunction(
    ProjectionContext::ProjectBatchFunction project_batch_function) {
  ICEBERG_DCHECK(project_batch_function != nullptr,
                 "ProjectBatch implementation must not be null");
  if (project_batch_function == nullptr) {
    return;
  }
  std::lock_guard lock(g_project_batch_function_mutex);
  g_project_batch_function = project_batch_function;
}

bool ProjectionContext::HasProjectBatchFunction() {
  return GetProjectBatchFunction() != nullptr;
}

auto ProjectionContext::ResolveProjectBatchFunction()
    -> ProjectionContext::ProjectBatchFunction {
  return GetProjectBatchFunction();
}

namespace {

Result<ArrowArray> ProjectBatchNanoarrow(const ArrowSchema& input_arrow_schema,
                                         const ArrowArray& input_batch,
                                         std::span<const int32_t> row_indices,
                                         const ArrowSchema& output_arrow_schema,
                                         std::span<const int> selected_field_indices) {
  ArrowArrayView input_view;
  ArrowError error;
  ICEBERG_NANOARROW_RETURN_UNEXPECTED_WITH_ERROR(
      ArrowArrayViewInitFromSchema(&input_view, &input_arrow_schema, &error), error);
  internal::ArrowArrayViewGuard input_view_guard(&input_view);
  ICEBERG_NANOARROW_RETURN_UNEXPECTED_WITH_ERROR(
      ArrowArrayViewSetArray(&input_view, &input_batch, &error), error);
  ICEBERG_NANOARROW_RETURN_UNEXPECTED_WITH_ERROR(
      ArrowArrayViewValidate(&input_view, NANOARROW_VALIDATION_LEVEL_FULL, &error),
      error);

  ArrowArray output_array;
  ICEBERG_NANOARROW_RETURN_UNEXPECTED_WITH_ERROR(
      ArrowArrayInitFromSchema(&output_array, &output_arrow_schema, &error), error);
  internal::ArrowArrayGuard output_array_guard(&output_array);
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayStartAppending(&output_array));
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(
      ArrowArrayReserve(&output_array, static_cast<int64_t>(row_indices.size())));

  for (int64_t row_index : row_indices) {
    ICEBERG_PRECHECK(row_index >= 0 && row_index < input_batch.length,
                     "Row index {} out of range for batch length {}", row_index,
                     input_batch.length);
    for (size_t output_index = 0; output_index < selected_field_indices.size();
         ++output_index) {
      const int input_index = selected_field_indices[output_index];
      ICEBERG_RETURN_UNEXPECTED(AppendValue(*input_arrow_schema.children[input_index],
                                            *input_batch.children[input_index],
                                            *input_view.children[input_index], row_index,
                                            output_array.children[output_index]));
    }
    ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(&output_array));
  }

  ICEBERG_NANOARROW_RETURN_UNEXPECTED_WITH_ERROR(
      ArrowArrayFinishBuildingDefault(&output_array, &error), error);

  return std::exchange(output_array, ArrowArray{});
}

}  // namespace

Result<ArrowArray> ProjectBatch(ArrowArray* input_batch,
                                std::span<const int32_t> row_indices,
                                ProjectionContext& projection) {
  ICEBERG_PRECHECK(input_batch != nullptr, "input_batch must not be null");
  internal::ArrowArrayGuard input_batch_guard(input_batch);

  auto project_batch_function = projection.project_batch_function();
  if (project_batch_function != nullptr) {
    return project_batch_function(input_batch, row_indices, projection);
  }

  return ProjectBatchNanoarrow(projection.input_arrow_schema(), *input_batch, row_indices,
                               projection.output_arrow_schema(),
                               projection.selected_field_indices());
}

}  // namespace iceberg
