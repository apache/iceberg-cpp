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

#include "iceberg/arrow_c_data_internal.h"

#include <array>
#include <string>
#include <utility>

namespace iceberg::internal {

std::pair<ArrowSchema, ArrowArray> CreateExampleArrowSchemaAndArrayByNanoarrow() {
  ArrowSchema out_schema;

  // Initializes the root struct schema
  NANOARROW_THROW_NOT_OK(ArrowSchemaInitFromType(&out_schema, NANOARROW_TYPE_STRUCT));
  NANOARROW_THROW_NOT_OK(ArrowSchemaAllocateChildren(&out_schema, 2));

  // Set up the non-nullable int64 field
  struct ArrowSchema* int64_field = out_schema.children[0];
  ArrowSchemaInit(int64_field);
  NANOARROW_THROW_NOT_OK(ArrowSchemaInitFromType(int64_field, NANOARROW_TYPE_INT64));
  NANOARROW_THROW_NOT_OK(ArrowSchemaSetName(int64_field, "id"));
  int64_field->flags &= ~ARROW_FLAG_NULLABLE;

  // Set up the nullable string field
  struct ArrowSchema* string_field = out_schema.children[1];
  ArrowSchemaInit(string_field);
  NANOARROW_THROW_NOT_OK(ArrowSchemaInitFromType(string_field, NANOARROW_TYPE_STRING));
  NANOARROW_THROW_NOT_OK(ArrowSchemaSetName(string_field, "name"));
  string_field->flags |= ARROW_FLAG_NULLABLE;

  constexpr int64_t kNumValues = 3;
  std::array<int64_t, kNumValues> int64_values = {1, 2, 3};
  std::array<std::string, kNumValues> string_values = {"a", "b", "c"};

  ArrowArray out_array;
  NANOARROW_THROW_NOT_OK(ArrowArrayInitFromSchema(&out_array, &out_schema, nullptr));
  ArrowArray* int64_array = out_array.children[0];
  ArrowArray* string_array = out_array.children[1];

  NANOARROW_THROW_NOT_OK(ArrowArrayStartAppending(int64_array));
  NANOARROW_THROW_NOT_OK(ArrowArrayStartAppending(string_array));

  for (int64_t i = 0; i < kNumValues; i++) {
    NANOARROW_THROW_NOT_OK(ArrowArrayAppendInt(int64_array, int64_values[i]));
    NANOARROW_THROW_NOT_OK(
        ArrowArrayAppendString(string_array, ArrowCharView(string_values[i].c_str())));
  }

  NANOARROW_THROW_NOT_OK(ArrowArrayFinishBuildingDefault(int64_array, nullptr));
  NANOARROW_THROW_NOT_OK(ArrowArrayFinishBuildingDefault(string_array, nullptr));

  out_array.length = kNumValues;
  out_array.null_count = 0;

  return {out_schema, out_array};
}

}  // namespace iceberg::internal
