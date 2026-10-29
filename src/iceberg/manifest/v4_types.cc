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

#include "iceberg/manifest/v4_types.h"

#include "iceberg/schema_field.h"
#include "iceberg/type.h"

namespace iceberg {

Result<FileTrackingStatus> FileTrackingStatusFromInt(int32_t status) {
  switch (status) {
    case 0:
      return FileTrackingStatus::kExisting;
    case 1:
      return FileTrackingStatus::kAdded;
    case 2:
      return FileTrackingStatus::kDeleted;
    case 3:
      return FileTrackingStatus::kReplaced;
    case 4:
      return FileTrackingStatus::kModified;
    default:
      return InvalidArgument("Invalid file tracking status: {}", status);
  }
}

std::shared_ptr<StructType> FileTracking::Type() {
  return std::make_shared<StructType>(std::vector<SchemaField>{
      SchemaField::MakeRequired(0, "status", int32()),
      SchemaField::MakeOptional(1, "snapshot_id", int64()),
      SchemaField::MakeOptional(5, "modified_snapshot_id", int64()),
      SchemaField::MakeOptional(3, "sequence_number", int64()),
      SchemaField::MakeOptional(4, "file_sequence_number", int64()),
      SchemaField::MakeOptional(142, "first_row_id", int64()),
      SchemaField::MakeOptional(6, "deleted_positions", binary()),
      SchemaField::MakeOptional(7, "replaced_positions", binary())});
}

std::shared_ptr<StructType> DeletionVectorMetadata::Type() {
  return std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField::MakeRequired(155, "location", string()),
                               SchemaField::MakeRequired(144, "offset", int64()),
                               SchemaField::MakeRequired(145, "size_in_bytes", int64()),
                               SchemaField::MakeRequired(156, "cardinality", int64()),
                               SchemaField::MakeOptional(149, "key_metadata", binary())});
}

std::shared_ptr<StructType> ManifestInfo::Type() {
  return std::make_shared<StructType>(std::vector<SchemaField>{
      SchemaField::MakeRequired(521, "format_version", int32()),
      SchemaField::MakeOptional(522, "dv", binary()),
      SchemaField::MakeRequired(504, "added_files_count", int32()),
      SchemaField::MakeRequired(505, "existing_files_count", int32()),
      SchemaField::MakeRequired(525, "modified_files_count", int32()),
      SchemaField::MakeRequired(506, "deleted_files_count", int32()),
      SchemaField::MakeRequired(523, "replaced_files_count", int32()),
      SchemaField::MakeRequired(512, "added_rows_count", int64()),
      SchemaField::MakeRequired(513, "existing_rows_count", int64()),
      SchemaField::MakeRequired(526, "modified_rows_count", int64()),
      SchemaField::MakeRequired(514, "deleted_rows_count", int64()),
      SchemaField::MakeRequired(524, "replaced_rows_count", int64()),
      SchemaField::MakeRequired(516, "min_sequence_number", int64())});
}

}  // namespace iceberg
