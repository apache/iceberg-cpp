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

#pragma once

#include <memory>
#include <string_view>
#include <utility>

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/json/from_string.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/arrow_c_data.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
#include "iceberg/util/macros.h"

namespace iceberg {

/// \brief Consume an Arrow C stream into a table, retaining the stream's schema.
inline Result<std::shared_ptr<::arrow::Table>> ReadArrowTable(ArrowArrayStream&& stream) {
  auto reader = ::arrow::ImportRecordBatchReader(&stream);
  if (!reader.ok()) {
    if (stream.release != nullptr) {
      stream.release(&stream);
    }
    return InvalidArrowData(reader.status().ToString());
  }
  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto table, reader.ValueUnsafe()->ToTable());
  return table;
}

/// \brief Build an expected Arrow table from an Iceberg schema and JSON rows.
inline Result<std::shared_ptr<::arrow::Table>> TableFromJson(const Schema& schema,
                                                             std::string_view rows) {
  ArrowSchema c_schema{};
  ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(schema, &c_schema));
  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto arrow_schema, ::arrow::ImportSchema(&c_schema));
  ICEBERG_ARROW_ASSIGN_OR_RETURN(
      auto array,
      ::arrow::json::ArrayFromJSONString(::arrow::struct_(arrow_schema->fields()), rows));
  const auto& struct_array = static_cast<const ::arrow::StructArray&>(*array);
  auto batch =
      ::arrow::RecordBatch::Make(arrow_schema, array->length(), struct_array.fields());
  ICEBERG_ARROW_ASSIGN_OR_RETURN(
      auto table, ::arrow::Table::FromRecordBatches(arrow_schema, {std::move(batch)}));
  return table;
}

/// \brief Compare schemas and values independently of Arrow chunk boundaries.
inline ::testing::AssertionResult TablesEqual(const ::arrow::Table& expected,
                                              const ::arrow::Table& actual) {
  const auto status = actual.ValidateFull();
  if (!status.ok()) {
    return ::testing::AssertionFailure() << status.ToString();
  }
  if (expected.Equals(actual, /*check_metadata=*/true)) {
    return ::testing::AssertionSuccess();
  }
  return ::testing::AssertionFailure() << "Expected:\n"
                                       << expected.ToString() << "\nActual:\n"
                                       << actual.ToString();
}

}  // namespace iceberg
