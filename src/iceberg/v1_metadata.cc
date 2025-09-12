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

#include "iceberg/v1_metadata.h"

#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_list.h"
#include "iceberg/schema.h"

namespace iceberg {

Status ManifestEntryAdapterV1::Init() {
  static std::unordered_set<int32_t> compatible_fields{
      0, 1, 2, 100, 101, 102, 103, 104, 105, 108, 109, 110, 137, 125, 128, 131, 132, 140,
  };
  // TODO(xiao.dong) schema to json
  metadata_["schema"] = "{}";
  // TODO(xiao.dong) partition spec to json
  metadata_["partition-spec"] = "{}";
  if (partition_spec_ != nullptr) {
    metadata_["partition-spec-id"] = std::to_string(partition_spec_->spec_id());
  }
  metadata_["format-version"] = "1";
  return InitSchema(compatible_fields);
}

Status ManifestEntryAdapterV1::Append(const iceberg::ManifestEntry& entry) {
  return AppendInternal(entry);
}

std::shared_ptr<StructType> ManifestEntryAdapterV1::GetManifestEntryStructType() {
  // 'block_size_in_bytes' (ID 105) is a deprecated field that is REQUIRED
  // in the v1 data_file schema for backward compatibility.
  // Deprecated. Always write a default in v1. Do not write in v2 or v3.
  static const SchemaField kBlockSizeInBytes = SchemaField::MakeRequired(
      105, "block_size_in_bytes", iceberg::int64(), "Block size in bytes");
  std::shared_ptr<StructType> partition_type = partition_schema_;
  if (!partition_type) {
    partition_type = PartitionSpec::Unpartitioned()->schema();
  }
  auto datafile_type = std::make_shared<StructType>(std::vector<SchemaField>{
      DataFile::kFilePath, DataFile::kFileFormat,
      SchemaField::MakeRequired(102, DataFile::kPartitionField,
                                std::move(partition_type)),
      DataFile::kRecordCount, DataFile::kFileSize, kBlockSizeInBytes,
      DataFile::kColumnSizes, DataFile::kValueCounts, DataFile::kNullValueCounts,
      DataFile::kNanValueCounts, DataFile::kLowerBounds, DataFile::kUpperBounds,
      DataFile::kKeyMetadata, DataFile::kSplitOffsets, DataFile::kSortOrderId});

  return std::make_shared<StructType>(
      std::vector<SchemaField>{ManifestEntry::kStatus, ManifestEntry::kSnapshotId,
                               SchemaField::MakeRequired(2, ManifestEntry::kDataFileField,
                                                         std::move(datafile_type))});
}

Status ManifestFileAdapterV1::Init() {
  static std::unordered_set<int32_t> compatible_fields{
      500, 501, 502, 503, 504, 505, 506, 512, 513, 514, 507, 519,
  };
  metadata_["snapshot-id"] = std::to_string(snapshot_id_);
  metadata_["parent-snapshot-id"] = parent_snapshot_id_.has_value()
                                        ? std::to_string(parent_snapshot_id_.value())
                                        : "null";
  metadata_["format-version"] = "1";
  return InitSchema(compatible_fields);
}

Status ManifestFileAdapterV1::Append(const iceberg::ManifestFile& file) {
  if (file.content != ManifestFile::Content::kData) {
    return InvalidManifestList("Cannot store delete manifests in a v1 table");
  }
  return AppendInternal(file);
}

}  // namespace iceberg
