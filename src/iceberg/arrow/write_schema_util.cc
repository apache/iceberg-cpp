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

#include <cstddef>
#include <memory>
#include <span>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/arrow/literal_util_internal.h"
#include "iceberg/arrow/write_schema_util_internal.h"
#include "iceberg/arrow_c_data_guard_internal.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/schema_internal.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/formatter.h"
#include "iceberg/util/macros.h"

namespace iceberg::arrow {
namespace {

using FieldIndex = std::unordered_map<int32_t, size_t>;

Result<FieldIndex> IndexFields(std::span<const SchemaField> fields,
                               std::string_view schema_name) {
  FieldIndex result;
  result.reserve(fields.size());
  for (size_t index = 0; index < fields.size(); ++index) {
    if (!result.emplace(fields[index].field_id(), index).second) {
      return InvalidSchema("Duplicate field id {} in {} schema", fields[index].field_id(),
                           schema_name);
    }
  }
  return result;
}

Result<std::vector<std::shared_ptr<::arrow::Array>>> AlignFields(
    std::span<const SchemaField> input_fields, std::span<const SchemaField> write_fields,
    const std::vector<std::shared_ptr<::arrow::Array>>& input_arrays,
    const std::vector<std::shared_ptr<::arrow::Field>>& write_arrow_fields,
    int64_t length, ::arrow::MemoryPool* pool);

Result<std::shared_ptr<::arrow::Array>> AlignPresentField(
    const SchemaField& input_field, const SchemaField& write_field,
    const std::shared_ptr<::arrow::Array>& input_array,
    const std::shared_ptr<::arrow::DataType>& write_arrow_type,
    ::arrow::MemoryPool* pool) {
  if (input_field.type()->type_id() != write_field.type()->type_id()) {
    return InvalidSchema("Cannot align field id {} from type {} to {}",
                         write_field.field_id(), *input_field.type(),
                         *write_field.type());
  }

  if (write_field.type()->type_id() != TypeId::kStruct) {
    if (*input_field.type() != *write_field.type()) {
      return InvalidSchema("Cannot align field id {} from type {} to {}",
                           write_field.field_id(), *input_field.type(),
                           *write_field.type());
    }
    return input_array;
  }

  auto input_struct = std::dynamic_pointer_cast<::arrow::StructArray>(input_array);
  auto write_arrow_struct =
      std::dynamic_pointer_cast<::arrow::StructType>(write_arrow_type);
  if (input_struct == nullptr || write_arrow_struct == nullptr) {
    return InvalidArrowData("Expected struct array for field id {}",
                            write_field.field_id());
  }

  const auto& input_struct_type =
      internal::checked_cast<const StructType&>(*input_field.type());
  const auto& write_struct_type =
      internal::checked_cast<const StructType&>(*write_field.type());
  ICEBERG_ASSIGN_OR_RAISE(
      auto children, AlignFields(input_struct_type.fields(), write_struct_type.fields(),
                                 input_struct->fields(), write_arrow_struct->fields(),
                                 input_struct->length(), pool));

  if (children.empty()) {
    return std::make_shared<::arrow::StructArray>(
        write_arrow_struct, input_struct->length(), std::move(children),
        input_struct->null_bitmap(), input_struct->null_count(), input_struct->offset());
  }
  ICEBERG_ARROW_ASSIGN_OR_RETURN(
      auto result,
      ::arrow::StructArray::Make(std::move(children), write_arrow_struct->fields(),
                                 input_struct->null_bitmap(), input_struct->null_count(),
                                 input_struct->offset()));
  return result;
}

Result<std::vector<std::shared_ptr<::arrow::Array>>> AlignFields(
    std::span<const SchemaField> input_fields, std::span<const SchemaField> write_fields,
    const std::vector<std::shared_ptr<::arrow::Array>>& input_arrays,
    const std::vector<std::shared_ptr<::arrow::Field>>& write_arrow_fields,
    int64_t length, ::arrow::MemoryPool* pool) {
  if (input_fields.size() != input_arrays.size()) {
    return InvalidArrowData("Input schema has {} fields but batch has {} columns",
                            input_fields.size(), input_arrays.size());
  }
  if (write_fields.size() != write_arrow_fields.size()) {
    return InvalidSchema("Write schema has {} Iceberg fields but {} Arrow fields",
                         write_fields.size(), write_arrow_fields.size());
  }

  ICEBERG_ASSIGN_OR_RAISE(auto input_by_id, IndexFields(input_fields, "input"));
  ICEBERG_ASSIGN_OR_RAISE(auto write_by_id, IndexFields(write_fields, "write"));
  for (const auto& input_field : input_fields) {
    if (!write_by_id.contains(input_field.field_id())) {
      return InvalidSchema("Input field id {} is not present in write schema",
                           input_field.field_id());
    }
  }

  std::vector<std::shared_ptr<::arrow::Array>> result;
  result.reserve(write_fields.size());
  for (size_t write_index = 0; write_index < write_fields.size(); ++write_index) {
    const auto& write_field = write_fields[write_index];
    const auto& write_arrow_type = write_arrow_fields[write_index]->type();
    auto input_iter = input_by_id.find(write_field.field_id());
    if (input_iter != input_by_id.end()) {
      size_t input_index = input_iter->second;
      ICEBERG_ASSIGN_OR_RAISE(
          auto aligned,
          AlignPresentField(input_fields[input_index], write_field,
                            input_arrays[input_index], write_arrow_type, pool));
      result.emplace_back(std::move(aligned));
    } else if (write_field.write_default() != nullptr) {
      ICEBERG_ASSIGN_OR_RAISE(
          auto default_array,
          MakeDefaultArray(*write_field.write_default(), write_arrow_type, length, pool));
      result.emplace_back(std::move(default_array));
    } else if (write_field.optional()) {
      ICEBERG_ARROW_ASSIGN_OR_RETURN(
          auto null_array, ::arrow::MakeArrayOfNull(write_arrow_type, length, pool));
      result.emplace_back(std::move(null_array));
    } else {
      return InvalidSchema("Missing required field id {} ({}) without a write-default",
                           write_field.field_id(), write_field.name());
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

  ArrowSchema input_c_schema;
  ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(input_schema, &input_c_schema));
  internal::ArrowSchemaGuard input_schema_guard(&input_c_schema);
  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto input_arrow_schema,
                                 ::arrow::ImportSchema(&input_c_schema));
  input_schema_guard.Release();

  ICEBERG_ARROW_ASSIGN_OR_RETURN(
      auto input_record_batch,
      ::arrow::ImportRecordBatch(input_batch, input_arrow_schema));
  input_guard.Release();

  ArrowSchema write_c_schema;
  ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(write_schema, &write_c_schema));
  internal::ArrowSchemaGuard write_schema_guard(&write_c_schema);
  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto write_arrow_schema,
                                 ::arrow::ImportSchema(&write_c_schema));
  write_schema_guard.Release();

  ICEBERG_ASSIGN_OR_RAISE(
      auto columns,
      AlignFields(input_schema.fields(), write_schema.fields(),
                  input_record_batch->columns(), write_arrow_schema->fields(),
                  input_record_batch->num_rows(), pool));
  auto output_record_batch = ::arrow::RecordBatch::Make(
      write_arrow_schema, input_record_batch->num_rows(), std::move(columns));

  ArrowArray output{};
  ICEBERG_ARROW_RETURN_NOT_OK(::arrow::ExportRecordBatch(*output_record_batch, &output));
  internal::ArrowArrayGuard output_guard(&output);
  return std::exchange(output, ArrowArray{});
}

}  // namespace iceberg::arrow
