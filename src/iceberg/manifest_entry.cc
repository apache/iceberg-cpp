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

#include "iceberg/manifest_entry.h"

#include <memory>
#include <vector>

#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/type.h"

namespace iceberg {

std::shared_ptr<DataFile> DataFile::Clone() const {
  auto copy = std::make_shared<DataFile>();
  copy->content = content;
  copy->file_path = file_path;
  copy->file_format = file_format;
  copy->partition = partition;
  copy->record_count = record_count;
  copy->file_size_in_bytes = file_size_in_bytes;
  copy->column_sizes = column_sizes;
  copy->value_counts = value_counts;
  copy->null_value_counts = null_value_counts;
  copy->nan_value_counts = nan_value_counts;
  copy->lower_bounds = lower_bounds;
  copy->upper_bounds = upper_bounds;
  copy->key_metadata = key_metadata;
  copy->split_offsets = split_offsets;
  copy->equality_ids = equality_ids;
  copy->sort_order_id = sort_order_id;
  copy->first_row_id = first_row_id;
  copy->referenced_data_file = referenced_data_file;
  copy->content_offset = content_offset;
  copy->content_size_in_bytes = content_size_in_bytes;
  return copy;
}

bool ManifestEntry::operator==(const ManifestEntry& other) const {
  return status == other.status && snapshot_id == other.snapshot_id &&
         sequence_number == other.sequence_number &&
         file_sequence_number == other.file_sequence_number &&
         ((data_file && other.data_file && *data_file == *other.data_file) ||
          (!data_file && !other.data_file));
}

std::shared_ptr<StructType> DataFile::Type(std::shared_ptr<StructType> partition_type) {
  if (!partition_type) {
    partition_type = std::make_shared<StructType>(std::vector<SchemaField>{});
  }
  return std::make_shared<StructType>(std::vector<SchemaField>{
      kContent,
      kFilePath,
      kFileFormat,
      SchemaField::MakeRequired(kPartitionFieldId, kPartitionField,
                                std::move(partition_type)),
      kRecordCount,
      kFileSize,
      kColumnSizes,
      kValueCounts,
      kNullValueCounts,
      kNanValueCounts,
      kLowerBounds,
      kUpperBounds,
      kKeyMetadata,
      kSplitOffsets,
      kEqualityIds,
      kSortOrderId,
      kFirstRowId,
      kReferencedDataFile,
      kContentOffset,
      kContentSize});
}

std::shared_ptr<StructType> ManifestEntry::TypeFromPartitionType(
    std::shared_ptr<StructType> partition_type) {
  return TypeFromDataFileType(DataFile::Type(std::move(partition_type)));
}

std::shared_ptr<StructType> ManifestEntry::TypeFromDataFileType(
    std::shared_ptr<StructType> datafile_type) {
  return std::make_shared<StructType>(
      std::vector<SchemaField>{kStatus, kSnapshotId, kSequenceNumber, kFileSequenceNumber,
                               SchemaField::MakeRequired(kDataFileFieldId, kDataFileField,
                                                         std::move(datafile_type))});
}

}  // namespace iceberg
