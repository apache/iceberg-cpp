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

#include <utility>

#include <nanoarrow/nanoarrow.h>

#include "iceberg/arrow/nanoarrow_status_internal.h"
#include "iceberg/arrow_c_data_guard_internal.h"
#include "iceberg/arrow_row_builder_internal.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"

namespace iceberg {

Result<ArrowRowBuilder> ArrowRowBuilder::Make(const Schema& schema) {
  ArrowSchema arrow_schema;
  ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(schema, &arrow_schema));
  internal::ArrowSchemaGuard schema_guard(&arrow_schema);
  return Make(&arrow_schema);
}

Result<ArrowRowBuilder> ArrowRowBuilder::Make(const ArrowSchema* schema) {
  ArrowRowBuilder builder;
  ArrowError error;
  ICEBERG_NANOARROW_RETURN_UNEXPECTED_WITH_ERROR(
      ArrowArrayInitFromSchema(&builder.array_, schema, &error), error);
  // Guard the array in case StartAppending fails.
  internal::ArrowArrayGuard guard(&builder.array_);
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayStartAppending(&builder.array_));
  // Ownership stays with the builder — disarm the guard.
  guard.Release();
  return builder;
}

ArrowRowBuilder::ArrowRowBuilder(ArrowRowBuilder&& other) noexcept
    : array_(other.array_) {
  other.array_.release = nullptr;
}

ArrowRowBuilder& ArrowRowBuilder::operator=(ArrowRowBuilder&& other) noexcept {
  if (this != &other) {
    if (array_.release != nullptr) {
      ArrowArrayRelease(&array_);
    }
    array_ = other.array_;
    other.array_.release = nullptr;
  }
  return *this;
}

ArrowRowBuilder::~ArrowRowBuilder() {
  if (array_.release != nullptr) {
    ArrowArrayRelease(&array_);
  }
}

int64_t ArrowRowBuilder::num_columns() const { return array_.n_children; }

ArrowArray* ArrowRowBuilder::column(int64_t index) {
  if (index < 0 || index >= array_.n_children) {
    return nullptr;
  }
  return array_.children[index];
}

Status ArrowRowBuilder::FinishRow() {
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(&array_));
  return {};
}

Result<ArrowArray> ArrowRowBuilder::Finish() && {
  ArrowError error;
  ICEBERG_NANOARROW_RETURN_UNEXPECTED_WITH_ERROR(
      ArrowArrayFinishBuildingDefault(&array_, &error), error);
  ArrowArray result = array_;
  array_.release = nullptr;
  return result;
}

Status AppendNull(ArrowArray* array) {
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendNull(array, 1));
  return {};
}

Status AppendBoolean(ArrowArray* array, bool value) {
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendInt(array, value ? 1 : 0));
  return {};
}

Status AppendInt(ArrowArray* array, int64_t value) {
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendInt(array, value));
  return {};
}

Status AppendString(ArrowArray* array, std::string_view value) {
  ArrowStringView view(value.data(), static_cast<int64_t>(value.size()));
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendString(array, view));
  return {};
}

Status AppendStringMap(ArrowArray* array,
                       const std::unordered_map<std::string, std::string>& entries) {
  // A nanoarrow map array is a list of struct<key, value>. children[0] is the
  // entries struct, whose children[0]/children[1] are the key/value builders.
  ArrowArray* struct_array = array->children[0];
  ArrowArray* key_array = struct_array->children[0];
  ArrowArray* value_array = struct_array->children[1];

  for (const auto& [key, value] : entries) {
    ICEBERG_RETURN_UNEXPECTED(AppendString(key_array, key));
    ICEBERG_RETURN_UNEXPECTED(AppendString(value_array, value));
    ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(struct_array));
  }

  // Finish the (possibly empty) map element on the outer list.
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(array));
  return {};
}

}  // namespace iceberg
