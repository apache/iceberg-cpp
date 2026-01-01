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

namespace iceberg::arrow {

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

}  // namespace iceberg::arrow
