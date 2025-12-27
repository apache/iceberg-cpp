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

/// \file iceberg/metadata_columns.h
/// Metadata columns for reading Iceberg data files.

#include <limits>
#include <memory>
#include <set>
#include <string_view>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/schema_field.h"
#include "iceberg/type.h"

namespace iceberg {

/// \brief A class containing constants and utility methods for metadata columns
struct ICEBERG_EXPORT MetadataColumns {
  constexpr static int32_t kInt32Max = std::numeric_limits<int32_t>::max();

  // IDs kInt32Max - (1-100) are used for metadata columns
  constexpr static int32_t kFilePathColumnId = kInt32Max - 1;
  inline static const SchemaField kFilePath = SchemaField::MakeRequired(
      kFilePathColumnId, "_file", string(), "Path of the file in which a row is stored");

  constexpr static int32_t kFilePositionColumnId = kInt32Max - 2;
  inline static const SchemaField kRowPosition =
      SchemaField::MakeRequired(kFilePositionColumnId, "_pos", int64(),
                                "Ordinal position of a row in the source data file");

  constexpr static int32_t kIsDeletedColumnId = kInt32Max - 3;
  inline static const SchemaField kIsDeleted = SchemaField::MakeRequired(
      kIsDeletedColumnId, "_deleted", binary(), "Whether the row has been deleted");

  constexpr static int32_t kSpecIdColumnId = kInt32Max - 4;
  inline static const SchemaField kSpecId =
      SchemaField::MakeRequired(kSpecIdColumnId, "_spec_id", int32(),
                                "Spec ID used to track the file containing a row");

  // The partition column type depends on all specs in the table
  constexpr static int32_t kPartitionColumnId = kInt32Max - 5;
  constexpr static std::string_view kPartitionColumnName = "_partition";
  constexpr static std::string_view kPartitionColumnDoc =
      "Partition to which a row belongs to";

  constexpr static int32_t kContentOffsetColumnId = kInt32Max - 6;
  constexpr static int32_t kContentSizeInBytesColumnId = kInt32Max - 7;

  // IDs kInt32Max - (101-200) are used for reserved columns
  constexpr static int32_t kDeleteFilePathColumnId = kInt32Max - 101;
  inline static const SchemaField kDeleteFilePath =
      SchemaField::MakeRequired(kDeleteFilePathColumnId, "file_path", string(),
                                "Path of a file in which a deleted row is stored");

  constexpr static int32_t kDeleteFilePosColumnId = kInt32Max - 102;
  inline static const SchemaField kDeleteFilePos =
      SchemaField::MakeRequired(kDeleteFilePosColumnId, "pos", int64(),
                                "Ordinal position of a deleted row in the data file");

  // The row column type depends on the table schema
  constexpr static int32_t kDeleteFileRowColumnId = kInt32Max - 103;
  constexpr static std::string_view kDeleteFileRowFieldName = "row";
  constexpr static std::string_view kDeleteFileRowDoc = "Deleted row values";

  constexpr static int32_t kChangeTypeColumnId = kInt32Max - 104;
  inline static const SchemaField kChangeType = SchemaField::MakeRequired(
      kChangeTypeColumnId, "_change_type", string(), "Record type in changelog");

  constexpr static int32_t kChangeOrdinalColumnId = kInt32Max - 105;
  inline static const SchemaField kChangeOrdinal = SchemaField::MakeOptional(
      kChangeOrdinalColumnId, "_change_ordinal", int32(), "Change ordinal in changelog");

  constexpr static int32_t kCommitSnapshotIdColumnId = kInt32Max - 106;
  inline static const SchemaField kCommitSnapshotId = SchemaField::MakeOptional(
      kCommitSnapshotIdColumnId, "_commit_snapshot_id", int64(), "Commit snapshot ID");

  constexpr static int32_t kRowIdColumnId = kInt32Max - 107;
  inline static const SchemaField kRowId =
      SchemaField::MakeOptional(kRowIdColumnId, "_row_id", int64(),
                                "Implicit row ID that is automatically assigned");

  constexpr static int32_t kLastUpdatedSequenceNumberColumnId = kInt32Max - 108;
  inline static const SchemaField kLastUpdatedSequenceNumber = SchemaField::MakeOptional(
      kLastUpdatedSequenceNumberColumnId, "_last_updated_sequence_number", int64(),
      "Sequence number when the row was last updated");

  /// \brief Get the set of metadata field IDs.
  static const std::set<int32_t>& MetadataFieldIds();

  /// \brief Check if a column name is a metadata column.
  static bool IsMetadataColumn(std::string_view name);

  /// \brief Check if a column ID is a metadata column.
  static bool IsMetadataColumn(int32_t id);

  /// \brief Get a metadata column by name.
  ///
  /// \param name The name of the metadata column.
  /// \return The metadata column, or an error if the name does not refer to a metadata
  /// column. The returned pointer is guaranteed to be valid.
  static Result<const SchemaField*> MetadataColumn(std::string_view name);

  /// TODO(gangwu): add functions to build partition columns from a table schema
};

}  // namespace iceberg
