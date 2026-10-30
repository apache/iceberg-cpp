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

#include <cstddef>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include <nanoarrow/nanoarrow.h>

#include "iceberg/arrow_c_data_util_internal.h"
#include "iceberg/arrow_row_builder_internal.h"
#include "iceberg/inspect/metadata_table.h"
#include "iceberg/nanoarrow_status_internal.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/stream.h"

namespace iceberg::internal {

/// \brief Arrow stream backed by a metadata-table row iterator.
template <typename Row, typename AppendRow>
class MetadataTableRowsStream : public Stream<ArrowArray> {
 public:
  static Result<std::unique_ptr<MetadataTableRowsStream>> Make(
      const iceberg::Schema& schema, std::unique_ptr<Stream<Row>> rows,
      AppendRow append_row) {
    ICEBERG_PRECHECK(rows != nullptr, "Metadata table row source cannot be null");
    if constexpr (requires { static_cast<bool>(append_row); }) {
      ICEBERG_PRECHECK(static_cast<bool>(append_row),
                       "Metadata table row callback cannot be empty");
    }
    ArrowSchema arrow_schema{};
    ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(schema, &arrow_schema));
    return std::unique_ptr<MetadataTableRowsStream>(new MetadataTableRowsStream(
        std::move(rows), std::move(append_row), std::move(arrow_schema)));
  }

  ~MetadataTableRowsStream() override {
    if (arrow_schema_.release != nullptr) {
      ArrowSchemaRelease(&arrow_schema_);
    }
  }

  Status Close() {
    rows_.reset();
    append_row_.reset();
    if (arrow_schema_.release != nullptr) {
      ArrowSchemaRelease(&arrow_schema_);
    }
    return {};
  }

  Result<ArrowSchema> Schema() {
    ICEBERG_PRECHECK(arrow_schema_.release != nullptr,
                     "Cannot read schema from a closed metadata table stream");
    ArrowSchema schema_copy{};
    ICEBERG_NANOARROW_RETURN_UNEXPECTED(
        ArrowSchemaDeepCopy(&arrow_schema_, &schema_copy));
    return schema_copy;
  }

 protected:
  Result<std::optional<ArrowArray>> NextImpl() override {
    ICEBERG_PRECHECK(rows_ != nullptr, "Cannot read from a closed metadata table stream");
    // Check for exhaustion before allocating a builder, including after a partial batch.
    ICEBERG_ASSIGN_OR_RAISE(auto row, rows_->Next());
    if (!row.has_value()) {
      return std::nullopt;
    }
    ICEBERG_ASSIGN_OR_RAISE(auto builder, ArrowRowBuilder::Make(&arrow_schema_));
    ICEBERG_RETURN_UNEXPECTED((*append_row_)(builder, *row));
    while (builder.num_rows() < MetadataTable::kBatchSize) {
      ICEBERG_ASSIGN_OR_RAISE(auto next_row, rows_->Next());
      if (!next_row.has_value()) {
        break;
      }
      ICEBERG_RETURN_UNEXPECTED((*append_row_)(builder, *next_row));
    }
    ICEBERG_ASSIGN_OR_RAISE(auto array, std::move(builder).Finish());
    return array;
  }

 private:
  MetadataTableRowsStream(std::unique_ptr<Stream<Row>> rows, AppendRow append_row,
                          ArrowSchema arrow_schema)
      : rows_(std::move(rows)),
        append_row_(std::move(append_row)),
        arrow_schema_(std::move(arrow_schema)) {}

  std::unique_ptr<Stream<Row>> rows_;
  std::optional<AppendRow> append_row_;
  ArrowSchema arrow_schema_{};
};

template <typename Row, typename AppendRow>
Result<ArrowArrayStream> MakeMetadataTableStream(const Schema& schema,
                                                 std::unique_ptr<Stream<Row>> rows,
                                                 AppendRow append_row) {
  using RowsStream = MetadataTableRowsStream<Row, AppendRow>;
  ICEBERG_ASSIGN_OR_RAISE(
      auto stream, RowsStream::Make(schema, std::move(rows), std::move(append_row)));
  return MakeArrowArrayStream(std::move(stream));
}

template <typename Row>
class MetadataTableVectorIterator : public Stream<Row> {
 public:
  explicit MetadataTableVectorIterator(std::vector<Row> rows) : rows_(std::move(rows)) {}

 protected:
  Result<std::optional<Row>> NextImpl() override {
    if (next_row_ == rows_.size()) {
      return std::nullopt;
    }
    return std::move(rows_[next_row_++]);
  }

 private:
  std::vector<Row> rows_;
  size_t next_row_ = 0;
};

template <typename Row, typename AppendRow>
Result<ArrowArrayStream> MakeMetadataTableStream(const Schema& schema,
                                                 std::vector<Row> rows,
                                                 AppendRow append_row) {
  std::unique_ptr<Stream<Row>> iterator =
      std::make_unique<MetadataTableVectorIterator<Row>>(std::move(rows));
  return MakeMetadataTableStream(schema, std::move(iterator), std::move(append_row));
}

}  // namespace iceberg::internal
