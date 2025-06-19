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

/// \file iceberg/statistics_file.h
/// Statistics file for Iceberg tables.

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "iceberg/iceberg_export.h"

namespace iceberg {

/// \brief A metadata about a statistics or indices blob
struct ICEBERG_EXPORT BlobMetadata {
  /// Type of the blob
  std::string type;
  /// ID of the Iceberg table's snapshot the blob was computed from
  int64_t source_snapshot_id;
  /// Sequence number of the Iceberg table's snapshot the blob was computed from
  int64_t source_snapshot_sequence_number;
  /// Ordered list of fields the blob was calculated from
  std::vector<int32_t> fields;
  /// Additional properties of the blob, specific to the blob type
  std::unordered_map<std::string, std::string> properties;

  /// \brief Compare two BlobMetadatas for equality.
  friend bool operator==(const BlobMetadata& lhs, const BlobMetadata& rhs) {
    return lhs.type == rhs.type && lhs.source_snapshot_id == rhs.source_snapshot_id &&
           lhs.source_snapshot_sequence_number == rhs.source_snapshot_sequence_number &&
           lhs.fields == rhs.fields && lhs.properties == rhs.properties;
  }
};

/// \brief Represents a statistics file in the Puffin format
struct ICEBERG_EXPORT StatisticsFile {
  /// ID of the Iceberg table's snapshot the statistics file is associated with
  int64_t snapshot_id;
  /// Fully qualified path to the file
  std::string path;
  /// The size of the file in bytes
  int64_t file_size_in_bytes;
  /// The size of the file footer in bytes
  int64_t file_footer_size_in_bytes;
  /// List of statistics contained in the file
  std::vector<BlobMetadata> blob_metadata;

  /// \brief Compare two StatisticsFiles for equality.
  friend bool operator==(const StatisticsFile& lhs, const StatisticsFile& rhs) {
    return lhs.snapshot_id == rhs.snapshot_id && lhs.path == rhs.path &&
           lhs.file_size_in_bytes == rhs.file_size_in_bytes &&
           lhs.file_footer_size_in_bytes == rhs.file_footer_size_in_bytes &&
           lhs.blob_metadata == rhs.blob_metadata;
  }
};

/// \brief Represents a partition statistics file
struct ICEBERG_EXPORT PartitionStatisticsFile {
  /// Snapshot ID of the Iceberg table's snapshot the partition statistics file is
  /// associated with
  int64_t snapshot_id;
  /// Fully qualified path to the file
  std::string path;
  /// The size of the partition statistics file in bytes
  int64_t file_size_in_bytes;

  /// \brief Compare two PartitionStatisticsFiles for equality.
  friend bool operator==(const PartitionStatisticsFile& lhs,
                         const PartitionStatisticsFile& rhs) {
    return lhs.snapshot_id == rhs.snapshot_id && lhs.path == rhs.path &&
           lhs.file_size_in_bytes == rhs.file_size_in_bytes;
  }
};

/// \brief Returns a string representation of a BlobMetadata
ICEBERG_EXPORT std::string ToString(const BlobMetadata& blob_metadata);

/// \brief Returns a string representation of a StatisticsFile
ICEBERG_EXPORT std::string ToString(const StatisticsFile& statistics_file);

/// \brief Returns a string representation of a PartitionStatisticsFile
ICEBERG_EXPORT std::string ToString(
    const PartitionStatisticsFile& partition_statistics_file);

}  // namespace iceberg
