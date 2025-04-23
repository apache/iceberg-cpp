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

#include "iceberg/statistics_file.h"

#include <format>

#include "iceberg/util/formatter_internal.h"

namespace iceberg {

std::string ToString(const BlobMetadata& blob_metadata) {
  std::string repr = "BlobMetadata[";
  std::format_to(std::back_inserter(repr),
                 "type='{}',sourceSnapshotId={},sourceSnapshotSequenceNumber={},",
                 blob_metadata.type, blob_metadata.source_snapshot_id,
                 blob_metadata.source_snapshot_sequence_number);
  std::format_to(std::back_inserter(repr), "fields={},", blob_metadata.fields);
  std::format_to(std::back_inserter(repr), "properties={}", blob_metadata.properties);
  std::format_to(std::back_inserter(repr), "]");
  return repr;
}

std::string ToString(const StatisticsFile& statistics_file) {
  std::string repr = "StatisticsFile[";
  std::format_to(std::back_inserter(repr),
                 "snapshotId={},path={},fileSizeInBytes={},fileFooterSizeInBytes={},",
                 statistics_file.snapshot_id, statistics_file.path,
                 statistics_file.file_size_in_bytes,
                 statistics_file.file_footer_size_in_bytes);
  std::format_to(std::back_inserter(repr), "blobMetadata={}",
                 statistics_file.blob_metadata);
  std::format_to(std::back_inserter(repr), "]");
  return repr;
}

std::string ToString(const PartitionStatisticsFile& partition_statistics_file) {
  std::string repr = "PartitionStatisticsFile[";
  std::format_to(std::back_inserter(repr), "snapshotId={},path={},fileSizeInBytes={},",
                 partition_statistics_file.snapshot_id, partition_statistics_file.path,
                 partition_statistics_file.file_size_in_bytes);
  return repr;
}

}  // namespace iceberg
