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

#include <memory>

#include "iceberg/json_internal.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/v1_metadata_internal.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"

namespace iceberg {

ManifestEntryAdapterV1::ManifestEntryAdapterV1(
    std::optional<int64_t> snapshot_id, std::shared_ptr<PartitionSpec> partition_spec,
    std::shared_ptr<Schema> current_schema)
    : ManifestEntryAdapter(snapshot_id, std::move(partition_spec),
                           std::move(current_schema), ManifestContent::kData) {}

std::shared_ptr<Schema> ManifestEntryAdapterV1::EntrySchema(
    std::shared_ptr<StructType> partition_type) {
  return WrapFileSchema(DataFileSchema(std::move(partition_type)));
}

std::shared_ptr<Schema> ManifestEntryAdapterV1::WrapFileSchema(
    std::shared_ptr<StructType> file_schema) {
  return std::make_shared<Schema>(std::vector<SchemaField>{
      ManifestEntry::kStatus,
      ManifestEntry::kSnapshotId,
      SchemaField::MakeRequired(ManifestEntry::kDataFileFieldId,
                                ManifestEntry::kDataFileField, std::move(file_schema)),
  });
}

std::shared_ptr<StructType> ManifestEntryAdapterV1::DataFileSchema(
    std::shared_ptr<StructType> partition_type) {
  return std::make_shared<StructType>(std::vector<SchemaField>{
      DataFile::kFilePath,
      DataFile::kFileFormat,
      SchemaField::MakeRequired(DataFile::kPartitionFieldId, DataFile::kPartitionField,
                                std::move(partition_type)),
      DataFile::kRecordCount,
      DataFile::kFileSize,
      SchemaField::MakeRequired(105, "block_size_in_bytes", int64(),
                                "Block size in bytes"),
      DataFile::kColumnSizes,
      DataFile::kValueCounts,
      DataFile::kNullValueCounts,
      DataFile::kNanValueCounts,
      DataFile::kLowerBounds,
      DataFile::kUpperBounds,
      DataFile::kKeyMetadata,
      DataFile::kSplitOffsets,
      DataFile::kSortOrderId,
  });
}

Status ManifestEntryAdapterV1::Init() {
  ICEBERG_ASSIGN_OR_RAISE(metadata_["schema"], ToJsonString(*current_schema_))
  ICEBERG_ASSIGN_OR_RAISE(metadata_["partition-spec"], ToJsonString(*partition_spec_));
  metadata_["partition-spec-id"] = std::to_string(partition_spec_->spec_id());
  metadata_["format-version"] = "1";

  ICEBERG_ASSIGN_OR_RAISE(partition_type_,
                          partition_spec_->PartitionType(*current_schema_));
  manifest_schema_ = EntrySchema(partition_type_);
  return ToArrowSchema(*manifest_schema_, &schema_);
}

Status ManifestEntryAdapterV1::Append(const ManifestEntry& entry) {
  return AppendInternal(entry);
}

const std::shared_ptr<Schema> ManifestFileAdapterV1::kManifestListSchema =
    std::make_shared<Schema>(std::vector<SchemaField>{
        ManifestFile::kManifestPath,
        ManifestFile::kManifestLength,
        ManifestFile::kPartitionSpecId,
        ManifestFile::kAddedSnapshotId,
        ManifestFile::kAddedFilesCount,
        ManifestFile::kExistingFilesCount,
        ManifestFile::kDeletedFilesCount,
        ManifestFile::kPartitions,
        ManifestFile::kAddedRowsCount,
        ManifestFile::kExistingRowsCount,
        ManifestFile::kDeletedRowsCount,
        ManifestFile::kKeyMetadata,
    });

Status ManifestFileAdapterV1::Init() {
  metadata_["snapshot-id"] = std::to_string(snapshot_id_);
  metadata_["parent-snapshot-id"] = parent_snapshot_id_.has_value()
                                        ? std::to_string(parent_snapshot_id_.value())
                                        : "null";
  metadata_["format-version"] = "1";

  manifest_list_schema_ = kManifestListSchema;
  return ToArrowSchema(*manifest_list_schema_, &schema_);
}

Status ManifestFileAdapterV1::Append(const ManifestFile& file) {
  if (file.content != ManifestContent::kData) {
    return InvalidManifestList("Cannot store delete manifests in a v1 table");
  }
  return AppendInternal(file);
}

}  // namespace iceberg
