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

/// \file iceberg/inspect/metadata_table_columns_internal.h
/// \brief Build only the selected columns of metadata table rows.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "iceberg/arrow_c_data.h"
#include "iceberg/arrow_row_builder_internal.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"

namespace iceberg::internal {

/// \brief Append the value of one metadata table column for a row.
template <typename Row>
using AppendColumnFn = Status (*)(ArrowArray* column, const Row& row);

/// \brief A metadata table column and the function that appends its values.
template <typename Row>
struct MetadataTableColumn {
  int32_t field_id;
  AppendColumnFn<Row> append;
};

/// \brief Return the append functions for the columns of a projected schema.
///
/// \param schema The metadata table schema.
/// \param projected_schema Projection of `schema` with the columns to return.
/// \param columns The columns of `schema` with their append functions.
/// \return One append function per column of `projected_schema`, in the same order,
/// or NotSupported if only some fields of a nested column are selected.
template <typename Row>
Result<std::vector<AppendColumnFn<Row>>> SelectAppendFns(
    const Schema& schema, const Schema& projected_schema,
    std::span<const MetadataTableColumn<Row>> columns) {
  std::vector<AppendColumnFn<Row>> append_fns;
  append_fns.reserve(projected_schema.fields().size());
  for (const auto& projected_field : projected_schema.fields()) {
    ICEBERG_ASSIGN_OR_RAISE(auto field, schema.FindFieldById(projected_field.field_id()));
    ICEBERG_CHECK(field.has_value(), "Metadata table column '{}' is not in the schema",
                  projected_field.name());
    if (*field->get().type() != *projected_field.type()) {
      return NotSupported("Cannot select nested fields of metadata table column '{}'",
                          projected_field.name());
    }
    auto column = std::ranges::find(columns, projected_field.field_id(),
                                    &MetadataTableColumn<Row>::field_id);
    ICEBERG_CHECK(column != columns.end(),
                  "Metadata table column '{}' has no append function",
                  projected_field.name());
    append_fns.push_back(column->append);
  }
  return append_fns;
}

/// \brief Append the selected columns of a row and finish the row.
template <typename Row>
Status AppendRow(ArrowRowBuilder& builder,
                 std::span<const AppendColumnFn<Row>> append_fns, const Row& row) {
  for (size_t index = 0; index < append_fns.size(); ++index) {
    ICEBERG_RETURN_UNEXPECTED(
        append_fns[index](builder.column(static_cast<int64_t>(index)), row));
  }
  return builder.FinishRow();
}

}  // namespace iceberg::internal
