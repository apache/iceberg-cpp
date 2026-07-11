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
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/memory_pool.h>

#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/arrow/metadata_column_util_internal.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/util/checked_cast.h"

namespace iceberg::arrow {

bool HasRowLineageValue(int32_t field_id, const MetadataColumnContext& metadata_context) {
  if (!metadata_context.first_row_id.has_value()) {
    return false;
  }
  return field_id != MetadataColumns::kLastUpdatedSequenceNumberColumnId ||
         metadata_context.data_sequence_number.has_value();
}

Status AppendRowLineageValue(int32_t field_id,
                             const MetadataColumnContext& metadata_context,
                             ::arrow::ArrayBuilder* array_builder) {
  auto* int_builder = internal::checked_cast<::arrow::Int64Builder*>(array_builder);
  if (!HasRowLineageValue(field_id, metadata_context)) {
    ICEBERG_ARROW_RETURN_NOT_OK(int_builder->AppendNull());
  } else if (field_id == MetadataColumns::kRowIdColumnId) {
    ICEBERG_ARROW_RETURN_NOT_OK(int_builder->Append(
        metadata_context.first_row_id.value() + metadata_context.next_file_pos));
  } else {
    ICEBERG_ARROW_RETURN_NOT_OK(
        int_builder->Append(metadata_context.data_sequence_number.value()));
  }
  return {};
}

Result<std::shared_ptr<::arrow::Array>> MakeFilePathArray(const std::string& file_path,
                                                          int64_t num_rows,
                                                          ::arrow::MemoryPool* pool) {
  ::arrow::StringBuilder builder(pool);
  ICEBERG_ARROW_RETURN_NOT_OK(builder.Reserve(num_rows));
  for (int64_t i = 0; i < num_rows; ++i) {
    ICEBERG_ARROW_RETURN_NOT_OK(builder.Append(file_path));
  }
  std::shared_ptr<::arrow::Array> array;
  ICEBERG_ARROW_RETURN_NOT_OK(builder.Finish(&array));
  return array;
}

Result<std::shared_ptr<::arrow::Array>> MakeRowPositionArray(int64_t start_position,
                                                             int64_t num_rows,
                                                             ::arrow::MemoryPool* pool) {
  ::arrow::Int64Builder builder(pool);
  ICEBERG_ARROW_RETURN_NOT_OK(builder.Reserve(num_rows));
  for (int64_t i = 0; i < num_rows; ++i) {
    ICEBERG_ARROW_RETURN_NOT_OK(builder.Append(start_position + i));
  }
  std::shared_ptr<::arrow::Array> array;
  ICEBERG_ARROW_RETURN_NOT_OK(builder.Finish(&array));
  return array;
}

Result<std::shared_ptr<::arrow::Array>> MakeRowIdArray(
    std::optional<int64_t> first_row_id, int64_t start_position, int64_t num_rows,
    ::arrow::MemoryPool* pool) {
  ::arrow::Int64Builder builder(pool);
  ICEBERG_ARROW_RETURN_NOT_OK(builder.Reserve(num_rows));
  if (!first_row_id.has_value()) {
    ICEBERG_ARROW_RETURN_NOT_OK(builder.AppendNulls(num_rows));
  } else {
    for (int64_t row_index = 0; row_index < num_rows; ++row_index) {
      ICEBERG_ARROW_RETURN_NOT_OK(
          builder.Append(first_row_id.value() + start_position + row_index));
    }
  }

  std::shared_ptr<::arrow::Array> array;
  ICEBERG_ARROW_RETURN_NOT_OK(builder.Finish(&array));
  return array;
}

Result<std::shared_ptr<::arrow::Array>> MakeLastUpdatedSequenceNumberArray(
    std::optional<int64_t> first_row_id, std::optional<int64_t> data_sequence_number,
    int64_t num_rows, ::arrow::MemoryPool* pool) {
  ::arrow::Int64Builder builder(pool);
  ICEBERG_ARROW_RETURN_NOT_OK(builder.Reserve(num_rows));
  if (!first_row_id.has_value() || !data_sequence_number.has_value()) {
    ICEBERG_ARROW_RETURN_NOT_OK(builder.AppendNulls(num_rows));
  } else {
    for (int64_t row_index = 0; row_index < num_rows; ++row_index) {
      ICEBERG_ARROW_RETURN_NOT_OK(builder.Append(data_sequence_number.value()));
    }
  }

  std::shared_ptr<::arrow::Array> array;
  ICEBERG_ARROW_RETURN_NOT_OK(builder.Finish(&array));
  return array;
}

}  // namespace iceberg::arrow
