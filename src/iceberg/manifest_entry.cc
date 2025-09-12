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

bool ManifestEntry::operator==(const ManifestEntry& other) const {
  return status == other.status && snapshot_id == other.snapshot_id &&
         sequence_number == other.sequence_number &&
         file_sequence_number == other.file_sequence_number &&
         ((data_file && other.data_file && *data_file == *other.data_file) ||
          (!data_file && !other.data_file));
}

std::shared_ptr<StructType> DataFile::Type(std::shared_ptr<StructType> partition_type) {
  if (!partition_type) {
    partition_type = PartitionSpec::Unpartitioned()->schema();
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
