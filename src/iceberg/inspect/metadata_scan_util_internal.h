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

/// \file iceberg/inspect/metadata_scan_util_internal.h
/// \brief Read in-memory metadata as Arrow batches produced on demand.

#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <type_traits>
#include <utility>
#include <vector>

#include "iceberg/arrow_c_data.h"
#include "iceberg/arrow_c_data_util_internal.h"
#include "iceberg/arrow_row_builder_internal.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/schema_internal.h"
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
/// \return One append function per column of `projected_schema`, in the same order.
/// Each projected field must select a whole column of `schema` by field id, with the
/// same type and without tightening its nullability.
template <typename Row>
Result<std::vector<AppendColumnFn<Row>>> SelectAppendFns(
    const Schema& schema, const Schema& projected_schema,
    std::span<const MetadataTableColumn<Row>> columns) {
  std::vector<AppendColumnFn<Row>> append_fns;
  append_fns.reserve(projected_schema.fields().size());
  for (const auto& projected_field : projected_schema.fields()) {
    // Only top-level columns have append functions, so nested field ids are rejected.
    ICEBERG_ASSIGN_OR_RAISE(auto field, schema.GetFieldById(projected_field.field_id()));
    if (!field.has_value()) {
      return InvalidArgument("Unknown metadata column: {}", projected_field.name());
    }
    if (*field->get().type() != *projected_field.type()) {
      return NotSupported("Partial projection of metadata column is not supported: {}",
                          projected_field.name());
    }
    if (field->get().optional() && !projected_field.optional()) {
      return InvalidArgument(
          "Nullable metadata column cannot be projected as required: {}",
          projected_field.name());
    }
    auto column = std::ranges::find(columns, projected_field.field_id(),
                                    &MetadataTableColumn<Row>::field_id);
    if (column == columns.end()) {
      return InvalidArgument("Metadata column has no append function: {}",
                             projected_field.name());
    }
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

/// \brief Build batches lazily, retaining the row producer and its input owners.
template <typename Row, typename GetRow>
class MetadataBatchSource {
 public:
  MetadataBatchSource(std::shared_ptr<iceberg::Schema> schema,
                      std::vector<AppendColumnFn<Row>> append_fns, size_t num_rows,
                      int64_t batch_size, GetRow get_row)
      : schema_(std::move(schema)),
        append_fns_(std::move(append_fns)),
        num_rows_(num_rows),
        batch_size_(static_cast<size_t>(batch_size)),
        get_row_(std::move(get_row)) {}

  Status Close() { return {}; }

  Result<ArrowSchema> Schema() {
    ArrowSchema schema{};
    ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(*schema_, &schema));
    return schema;
  }

  Result<std::optional<ArrowArray>> Next() {
    if (error_) {
      return unexpected(*error_);
    }
    auto result = NextBatch();
    if (!result) {
      error_ = result.error();
    }
    return result;
  }

 private:
  Result<std::optional<ArrowArray>> NextBatch() {
    if (index_ == num_rows_) {
      return std::nullopt;
    }
    const size_t end = index_ + std::min(batch_size_, num_rows_ - index_);
    ICEBERG_ASSIGN_OR_RAISE(auto builder, ArrowRowBuilder::Make(*schema_));
    while (index_ < end) {
      ICEBERG_RETURN_UNEXPECTED(AppendRow<Row>(builder, append_fns_, get_row_(index_)));
      ++index_;
    }
    ICEBERG_ASSIGN_OR_RAISE(auto batch, std::move(builder).Finish());
    return batch;
  }

  const std::shared_ptr<iceberg::Schema> schema_;
  const std::vector<AppendColumnFn<Row>> append_fns_;
  const size_t num_rows_;
  const size_t batch_size_;
  GetRow get_row_;
  size_t index_ = 0;
  std::optional<Error> error_;
};

/// \brief Read only projected columns, constructing at most batch_size rows per batch.
///
/// get_row must retain shared ownership of its inputs. Each stream has independent
/// read state, and rows are accessed only when a batch is requested.
template <typename Row, typename GetRow>
Result<ArrowArrayStream> MakeMetadataBatchStream(
    const std::shared_ptr<Schema>& canonical_schema,
    const std::shared_ptr<Schema>& projected_schema,
    std::span<const MetadataTableColumn<Row>> columns, size_t num_rows,
    int64_t batch_size, GetRow&& get_row) {
  ICEBERG_PRECHECK(batch_size > 0, "Batch size must be positive");
  ICEBERG_ASSIGN_OR_RAISE(
      auto append_fns,
      SelectAppendFns<Row>(*canonical_schema, *projected_schema, columns));
  using Source = MetadataBatchSource<Row, std::decay_t<GetRow>>;
  return MakeArrowArrayStream(
      std::make_unique<Source>(projected_schema, std::move(append_fns), num_rows,
                               batch_size, std::forward<GetRow>(get_row)));
}

}  // namespace iceberg::internal
