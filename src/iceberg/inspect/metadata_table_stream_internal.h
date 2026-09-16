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
#include <functional>
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
#include "iceberg/util/iterator.h"
#include "iceberg/util/macros.h"

namespace iceberg::internal {

/// \brief Arrow stream backed by a metadata-table row iterator.
template <typename Row>
class MetadataTableRowsStream {
 public:
  using AppendRow = std::function<Status(ArrowRowBuilder&, const Row&)>;

  static Result<std::unique_ptr<MetadataTableRowsStream>> Make(
      const Schema& schema, std::unique_ptr<Iterator<Row>> rows, AppendRow append_row) {
    ArrowSchema arrow_schema{};
    ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(schema, &arrow_schema));
    return std::unique_ptr<MetadataTableRowsStream>(new MetadataTableRowsStream(
        std::move(rows), std::move(append_row), std::move(arrow_schema)));
  }

  ~MetadataTableRowsStream() {
    auto status = Close();
    static_cast<void>(status);
  }

  Status Close() {
    rows_.reset();
    append_row_ = nullptr;
    if (arrow_schema_.release != nullptr) {
      ArrowSchemaRelease(&arrow_schema_);
    }
    return {};
  }

  Result<std::optional<ArrowArray>> Next() {
    ICEBERG_PRECHECK(arrow_schema_.release != nullptr,
                     "Cannot read from a closed metadata table stream");
    ICEBERG_ASSIGN_OR_RAISE(auto builder, ArrowRowBuilder::Make(&arrow_schema_));
    while (builder.num_rows() < MetadataTable::kBatchSize) {
      ICEBERG_ASSIGN_OR_RAISE(auto row, rows_->Next());
      if (!row.has_value()) {
        break;
      }
      ICEBERG_RETURN_UNEXPECTED(append_row_(builder, *row));
    }
    if (builder.num_rows() == 0) {
      return std::nullopt;
    }

    ICEBERG_ASSIGN_OR_RAISE(auto array, std::move(builder).Finish());
    return array;
  }

  Result<ArrowSchema> Schema() {
    ICEBERG_PRECHECK(arrow_schema_.release != nullptr,
                     "Cannot read schema from a closed metadata table stream");
    ArrowSchema schema_copy{};
    ICEBERG_NANOARROW_RETURN_UNEXPECTED(
        ArrowSchemaDeepCopy(&arrow_schema_, &schema_copy));
    return schema_copy;
  }

 private:
  MetadataTableRowsStream(std::unique_ptr<Iterator<Row>> rows, AppendRow append_row,
                          ArrowSchema arrow_schema)
      : rows_(std::move(rows)),
        append_row_(std::move(append_row)),
        arrow_schema_(std::move(arrow_schema)) {}

  std::unique_ptr<Iterator<Row>> rows_;
  AppendRow append_row_;
  ArrowSchema arrow_schema_{};
};

template <typename Row, typename AppendRow>
Result<ArrowArrayStream> MakeMetadataTableStream(const Schema& schema,
                                                 std::unique_ptr<Iterator<Row>> rows,
                                                 AppendRow append_row) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto stream,
      MetadataTableRowsStream<Row>::Make(
          schema, std::move(rows),
          typename MetadataTableRowsStream<Row>::AppendRow(std::move(append_row))));
  return MakeArrowArrayStream(std::move(stream));
}

template <typename Row>
class MetadataTableVectorIterator : public Iterator<Row> {
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
  std::unique_ptr<Iterator<Row>> iterator =
      std::make_unique<MetadataTableVectorIterator<Row>>(std::move(rows));
  return MakeMetadataTableStream(schema, std::move(iterator), std::move(append_row));
}

}  // namespace iceberg::internal
