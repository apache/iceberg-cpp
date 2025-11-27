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

/// \file iceberg/update_partition_statistics.h
/// API for updating partition statistics files in a table

#include <cstdint>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/pending_update.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief API for updating partition statistics files in a table
///
/// UpdatePartitionStatistics accumulates partition statistics file changes and
/// commits them to the table. This API allows setting and removing partition
/// statistics files for snapshots.
///
/// Partition statistics are informational and used to read table data more
/// efficiently. A reader can choose to ignore partition statistics information.
/// Partition statistics support is not required to read the table correctly.
///
/// When committing, changes are applied to the latest table metadata. Commit
/// conflicts are resolved by applying the changes to the new latest metadata
/// and reattempting the commit.
///
/// Apply() returns a list of the partition statistics files that will be affected.
///
/// Example usage:
/// \code
///   table.UpdatePartitionStatistics()
///       .SetPartitionStatistics(partition_stats_file)
///       .RemovePartitionStatistics(old_snapshot_id)
///       .Commit();
/// \endcode
class ICEBERG_EXPORT UpdatePartitionStatistics
    : public PendingUpdateTyped<std::vector<PartitionStatisticsFile>> {
 public:
  ~UpdatePartitionStatistics() override = default;

  /// \brief Set the table's partition statistics file for a snapshot
  ///
  /// Sets the partition statistics file for a snapshot, replacing the previous
  /// partition statistics file for the snapshot if any exists. The snapshot ID
  /// is taken from the PartitionStatisticsFile object itself via
  /// partition_statistics_file.snapshot_id.
  ///
  /// \param partition_statistics_file The partition statistics file to set
  /// \return Reference to this for method chaining
  virtual UpdatePartitionStatistics& SetPartitionStatistics(
      const PartitionStatisticsFile& partition_statistics_file) = 0;

  /// \brief Remove the table's partition statistics file for a snapshot
  ///
  /// Removes the partition statistics file associated with the specified snapshot.
  ///
  /// \param snapshot_id The ID of the snapshot whose partition statistics should be
  /// removed
  /// \return Reference to this for method chaining
  virtual UpdatePartitionStatistics& RemovePartitionStatistics(int64_t snapshot_id) = 0;

  // Non-copyable, movable (inherited from PendingUpdate)
  UpdatePartitionStatistics(const UpdatePartitionStatistics&) = delete;
  UpdatePartitionStatistics& operator=(const UpdatePartitionStatistics&) = delete;

 protected:
  UpdatePartitionStatistics() = default;
};

}  // namespace iceberg
