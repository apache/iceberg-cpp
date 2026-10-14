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

#include <limits>

#include <arrow/array.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/memory_pool.h>
#include <parquet/metadata.h>

#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/parquet/row_number_reader_internal.h"

namespace iceberg::parquet {

Result<std::unique_ptr<RowNumberReader>> RowNumberReader::Make(
    const ::parquet::FileMetaData& metadata, const std::vector<int>& row_groups,
    ::arrow::MemoryPool* pool) {
  // Prefix sums in footer order, including the end of the last row group.
  const int num_row_groups = metadata.num_row_groups();
  std::vector<int64_t> offsets(static_cast<size_t>(num_row_groups) + 1, 0);
  for (int i = 0; i < num_row_groups; ++i) {
    const int64_t num_rows = metadata.RowGroup(i)->num_rows();
    if (num_rows < 0 || num_rows > std::numeric_limits<int64_t>::max() - offsets[i]) {
      return InvalidArgument("Invalid row count for row group {}: {}", i, num_rows);
    }
    offsets[i + 1] = offsets[i] + num_rows;
  }

  // Keep the requested order, including repeated row groups.
  std::deque<RowRange> ranges;
  for (int row_group : row_groups) {
    if (row_group < 0 || row_group >= num_row_groups) {
      return InvalidArgument("Row group index {} out of bounds [0, {})", row_group,
                             num_row_groups);
    }
    if (offsets[row_group] != offsets[row_group + 1]) {
      ranges.push_back({.start = offsets[row_group], .end = offsets[row_group + 1]});
    }
  }

  return std::unique_ptr<RowNumberReader>(new RowNumberReader(std::move(ranges), pool));
}

Result<std::shared_ptr<::arrow::Int64Array>> RowNumberReader::NextBatch(
    int64_t batch_size) {
  if (batch_size < 0) {
    return InvalidArgument("Negative batch size: {}", batch_size);
  }

  std::vector<RowRange> batch_ranges;
  int64_t remaining = batch_size;
  while (remaining > 0) {
    auto range = TakeRange(remaining);
    if (!range) {
      break;
    }
    remaining -= range->end - range->start;
    batch_ranges.push_back(*range);
  }

  auto result = BuildArray(batch_ranges, batch_size - remaining);
  if (!result.has_value()) {
    // Restore the consumed ranges in their original order.
    ranges_.insert(ranges_.begin(), batch_ranges.begin(), batch_ranges.end());
  }
  return result;
}

std::optional<RowNumberReader::RowRange> RowNumberReader::TakeRange(int64_t count) {
  if (ranges_.empty()) {
    return std::nullopt;
  }

  auto range = ranges_.front();
  if (range.end - range.start <= count) {
    ranges_.pop_front();
  } else {
    range.end = range.start + count;
    ranges_.front().start = range.end;
  }
  return range;
}

Result<std::shared_ptr<::arrow::Int64Array>> RowNumberReader::BuildArray(
    const std::vector<RowRange>& ranges, int64_t length) {
  // Arrow's numeric builder converts the element count to bytes internally.
  // Check the actual batch length, not the requested upper bound.
  if (length >
      std::numeric_limits<int64_t>::max() / static_cast<int64_t>(sizeof(int64_t))) {
    return InvalidArgument("Row number batch is too large");
  }

  ::arrow::Int64Builder builder(pool_);
  ICEBERG_ARROW_RETURN_NOT_OK(builder.Reserve(length));
  for (const auto& range : ranges) {
    for (int64_t row = range.start; row < range.end; ++row) {
      builder.UnsafeAppend(row);
    }
  }
  std::shared_ptr<::arrow::Int64Array> array;
  ICEBERG_ARROW_RETURN_NOT_OK(builder.Finish(&array));

  return array;
}

}  // namespace iceberg::parquet
