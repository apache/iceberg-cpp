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

namespace iceberg {

bool BlobMetadata::Equals(const BlobMetadata& other) const {
  return type == other.type && source_snapshot_id == other.source_snapshot_id &&
         source_snapshot_sequence_number == other.source_snapshot_sequence_number &&
         fields == other.fields && properties == other.properties;
}

std::string BlobMetadata::ToString() const {
  std::string repr = "BlobMetadata[";
  std::format_to(std::back_inserter(repr),
                 "type='{}',sourceSnapshotId={},sourceSnapshotSequenceNumber={},", type,
                 source_snapshot_id, source_snapshot_sequence_number);
  std::format_to(std::back_inserter(repr), "fields=[");
  for (auto iter = fields.cbegin(); iter != fields.cend(); ++iter) {
    if (iter != fields.cbegin()) {
      std::format_to(std::back_inserter(repr), ",{}", *iter);
    } else {
      std::format_to(std::back_inserter(repr), "{}", *iter);
    }
  }
  std::format_to(std::back_inserter(repr), "],properties=[");
  for (auto iter = properties.cbegin(); iter != properties.cend(); ++iter) {
    const auto& [key, value] = *iter;
    if (iter != properties.cbegin()) {
      std::format_to(std::back_inserter(repr), ",{}:{}", key, value);
    } else {
      std::format_to(std::back_inserter(repr), "{}:{}", key, value);
    }
  }
  repr += "]]";
  return repr;
}

bool StatisticsFile::Equals(const StatisticsFile& other) const {
  return snapshot_id == other.snapshot_id && path == other.path &&
         file_size_in_bytes == other.file_size_in_bytes &&
         file_footer_size_in_bytes == other.file_footer_size_in_bytes &&
         blob_metadata == other.blob_metadata;
}

std::string StatisticsFile::ToString() const {
  std::string repr = "StatisticsFile[";
  std::format_to(std::back_inserter(repr),
                 "snapshotId={},path={},fileSizeInBytes={},fileFooterSizeInBytes={},",
                 snapshot_id, path, file_size_in_bytes, file_footer_size_in_bytes);
  std::format_to(std::back_inserter(repr), "blobMetadata=[");
  for (auto iter = blob_metadata.cbegin(); iter != blob_metadata.cend(); ++iter) {
    if (iter != blob_metadata.cbegin()) {
      std::format_to(std::back_inserter(repr), ",{}", iter->ToString());
    } else {
      std::format_to(std::back_inserter(repr), "{}", iter->ToString());
    }
  }
  repr += "]]";
  return repr;
}

}  // namespace iceberg
