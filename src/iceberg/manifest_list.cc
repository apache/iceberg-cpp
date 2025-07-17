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

#include "iceberg/manifest_list.h"

#include "iceberg/schema.h"

namespace iceberg {

bool PartitionFieldSummary::operator==(
    const iceberg::PartitionFieldSummary& other) const {
  return contains_null == other.contains_null && contains_nan == other.contains_nan &&
         lower_bound == other.lower_bound && upper_bound == other.upper_bound;
}

bool ManifestFile::operator==(const iceberg::ManifestFile& other) const {
  return manifest_path == other.manifest_path &&
         manifest_length == other.manifest_length &&
         partition_spec_id == other.partition_spec_id && content == other.content &&
         sequence_number == other.sequence_number &&
         min_sequence_number == other.min_sequence_number &&
         added_snapshot_id == other.added_snapshot_id &&
         added_files_count == other.added_files_count &&
         existing_files_count == other.existing_files_count &&
         deleted_files_count == other.deleted_files_count &&
         added_rows_count == other.added_rows_count &&
         existing_rows_count == other.existing_rows_count &&
         deleted_rows_count == other.deleted_rows_count &&
         partitions == other.partitions && key_metadata == other.key_metadata &&
         first_row_id == other.first_row_id;
}

const StructType& PartitionFieldSummary::Type() {
  static const StructType kInstance{{
      PartitionFieldSummary::kContainsNull,
      PartitionFieldSummary::kContainsNaN,
      PartitionFieldSummary::kLowerBound,
      PartitionFieldSummary::kUpperBound,
  }};
  return kInstance;
}

const StructType& ManifestFile::Type() {
  static const StructType kInstance(
      {kManifestPath, kManifestLength, kPartitionSpecId, kContent, kSequenceNumber,
       kMinSequenceNumber, kAddedSnapshotId, kAddedFilesCount, kExistingFilesCount,
       kDeletedFilesCount, kAddedRowsCount, kExistingRowsCount, kDeletedRowsCount,
       kPartitions, kKeyMetadata, kFirstRowId});
  return kInstance;
}

}  // namespace iceberg
