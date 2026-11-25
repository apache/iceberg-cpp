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

/// \file iceberg/metadata_column_context_internal.h
/// Reader state for populating metadata columns, shared by the file format readers.

#include <cstdint>
#include <optional>
#include <string>

#include "iceberg/metadata_columns.h"

namespace iceberg {

/// \brief Context for populating metadata columns during reading.
struct MetadataColumnContext {
  std::string file_path;  // The file path to populate _file column.
  // The file row position used when appending metadata values.
  int64_t next_file_pos = 0;
  std::optional<int64_t> first_row_id;          // First row ID used to populate _row_id.
  std::optional<int64_t> data_sequence_number;  // Data sequence number for lineage.
};

/// \brief Return whether a row lineage column must be read as null.
inline bool ShouldReadRowLineageAsNull(int32_t field_id,
                                       const MetadataColumnContext& metadata_context) {
  return MetadataColumns::IsRowLineageColumn(field_id) &&
         !metadata_context.first_row_id.has_value();
}

/// \brief Check if row lineage metadata can be inherited for the column.
inline bool CanInheritRowLineageValue(int32_t field_id,
                                      const MetadataColumnContext& metadata_context) {
  if (field_id == MetadataColumns::kRowIdColumnId) {
    return metadata_context.first_row_id.has_value();
  }
  if (field_id == MetadataColumns::kLastUpdatedSequenceNumberColumnId) {
    return metadata_context.first_row_id.has_value() &&
           metadata_context.data_sequence_number.has_value();
  }
  return false;
}

/// \brief The row lineage value the current row inherits for the column, or
/// std::nullopt when it cannot be inherited.
inline std::optional<int64_t> InheritedRowLineageValue(
    int32_t field_id, const MetadataColumnContext& metadata_context) {
  if (!CanInheritRowLineageValue(field_id, metadata_context)) {
    return std::nullopt;
  }
  if (field_id == MetadataColumns::kRowIdColumnId) {
    return metadata_context.first_row_id.value() + metadata_context.next_file_pos;
  }
  return metadata_context.data_sequence_number.value();
}

}  // namespace iceberg
