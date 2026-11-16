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

#include "iceberg/manifest/manifest_entry.h"

#include <memory>
#include <tuple>
#include <vector>

#include "iceberg/schema_field.h"
#include "iceberg/type.h"

namespace iceberg {

bool DataFile::operator==(const DataFile& other) const {
  return std::tie(content, file_path, file_format, partition, record_count,
                  file_size_in_bytes, column_sizes, value_counts, null_value_counts,
                  nan_value_counts, lower_bounds, upper_bounds, key_metadata,
                  split_offsets, equality_ids, sort_order_id, first_row_id,
                  referenced_data_file, content_offset, content_size_in_bytes,
                  partition_spec_id, data_sequence_number) ==
         std::tie(other.content, other.file_path, other.file_format, other.partition,
                  other.record_count, other.file_size_in_bytes, other.column_sizes,
                  other.value_counts, other.null_value_counts, other.nan_value_counts,
                  other.lower_bounds, other.upper_bounds, other.key_metadata,
                  other.split_offsets, other.equality_ids, other.sort_order_id,
                  other.first_row_id, other.referenced_data_file, other.content_offset,
                  other.content_size_in_bytes, other.partition_spec_id,
                  other.data_sequence_number);
}

std::shared_ptr<DataFile> DataFile::CopyWithoutStats() const {
  auto copy = std::make_shared<DataFile>(*this);
  copy->column_sizes.clear();
  copy->value_counts.clear();
  copy->null_value_counts.clear();
  copy->nan_value_counts.clear();
  copy->lower_bounds.clear();
  copy->upper_bounds.clear();
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
