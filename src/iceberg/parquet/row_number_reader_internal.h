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

/// \file iceberg/parquet/row_number_reader_internal.h
/// Produces file row positions for the rows emitted by a Parquet record batch reader.

#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include <arrow/type_fwd.h>

#include "iceberg/result.h"

namespace parquet {
class FileMetaData;
}  // namespace parquet

namespace iceberg::parquet {

/// \brief Reader of file row positions ("row numbers") for selected row groups.
///
/// Row numbers are absolute positions in the file: row groups are numbered by prefix
/// sums of `num_rows` in footer order, and selected row groups are emitted in the
/// requested order, which may skip or reorder row groups.
///
/// Like parquet-rs RowNumberReader, this keeps ranges until a batch is requested.
/// Row positions are independent of Iceberg metadata column and lineage semantics.
class RowNumberReader {
 public:
  /// \brief Create a reader for `row_groups`, in the order they will be read.
  static Result<std::unique_ptr<RowNumberReader>> Make(
      const ::parquet::FileMetaData& metadata, const std::vector<int>& row_groups,
      ::arrow::MemoryPool* pool);

  /// \brief Read up to `batch_size` row numbers, returning an empty array at EOF.
  /// If the call fails, no row numbers are consumed and the call can be retried.
  Result<std::shared_ptr<::arrow::Int64Array>> NextBatch(int64_t batch_size);

 private:
  /// \brief Half-open range [start, end) of file row positions.
  struct RowRange {
    int64_t start;
    int64_t end;
  };

  RowNumberReader(std::deque<RowRange> ranges, ::arrow::MemoryPool* pool)
      : ranges_(std::move(ranges)), pool_(pool) {}

  // Take up to count rows from the first range, splitting it when necessary.
  std::optional<RowRange> TakeRange(int64_t count);
  Result<std::shared_ptr<::arrow::Int64Array>> BuildArray(
      const std::vector<RowRange>& ranges, int64_t length);

  std::deque<RowRange> ranges_;
  ::arrow::MemoryPool* pool_;
};

}  // namespace iceberg::parquet
