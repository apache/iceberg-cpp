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

/// \file iceberg/arrow/metadata_column_util_internal.h
/// Utility functions for populating metadata columns (_file and _pos) in readers.

#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include <arrow/type_fwd.h>

#include "iceberg/result.h"

namespace arrow {
class ArrayBuilder;
}  // namespace arrow

namespace iceberg::arrow {

/// \brief Context for populating metadata columns during reading.
struct MetadataColumnContext {
  std::string file_path;  // The file path to populate _file column.
  // The file row position used when appending metadata values.
  int64_t next_file_pos = 0;
  std::optional<int64_t> first_row_id;          // First row ID used to populate _row_id.
  std::optional<int64_t> data_sequence_number;  // Data sequence number for lineage.
};

/// \brief Check if row lineage metadata can be inherited for the column.
bool CanInheritRowLineageValue(int32_t field_id,
                               const MetadataColumnContext& metadata_context);

/// \brief Append one inherited row lineage value, or null if unavailable.
Status AppendInheritedRowLineageValue(int32_t field_id,
                                      const MetadataColumnContext& metadata_context,
                                      ::arrow::ArrayBuilder* array_builder);

/// \brief Create a constant string array for _file column.
///
/// Creates an Arrow StringArray where all values are the same file path string.
///
/// \param file_path The file path value.
/// \param num_rows Number of rows in the batch.
/// \param pool Arrow memory pool.
/// \return Arrow StringArray with constant file_path value.
Result<std::shared_ptr<::arrow::Array>> MakeFilePathArray(const std::string& file_path,
                                                          int64_t num_rows,
                                                          ::arrow::MemoryPool* pool);

/// \brief Create a row ID array for _row_id column.
///
/// \param first_row_id First row ID of the data file.
/// \param row_positions File row positions. May be null when first_row_id is unknown;
/// otherwise must contain num_rows non-null positions.
/// \param num_rows Number of rows in the batch.
/// \param pool Arrow memory pool.
/// \return Arrow Int64Array with `first_row_id + pos`, or all nulls when
/// `first_row_id` is unknown.
Result<std::shared_ptr<::arrow::Array>> MakeRowIdArray(
    std::optional<int64_t> first_row_id,
    const std::shared_ptr<::arrow::Int64Array>& row_positions, int64_t num_rows,
    ::arrow::MemoryPool* pool);

/// \brief Create a sequence number array for _last_updated_sequence_number column.
///
/// \param data_sequence_number Data sequence number of the data file.
/// \param num_rows Number of rows in the batch.
/// \param pool Arrow memory pool.
/// \return Arrow Int64Array for _last_updated_sequence_number.
Result<std::shared_ptr<::arrow::Array>> MakeLastUpdatedSequenceNumberArray(
    std::optional<int64_t> data_sequence_number, int64_t num_rows,
    ::arrow::MemoryPool* pool);

}  // namespace iceberg::arrow
