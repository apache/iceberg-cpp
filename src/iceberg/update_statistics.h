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

/// \file iceberg/update_statistics.h
/// API for updating statistics files in a table

#include <cstdint>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/pending_update.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief API for updating statistics files in a table
///
/// UpdateStatistics accumulates statistics file changes and commits them to
/// the table. This API allows setting and removing statistics files for
/// snapshots.
///
/// Statistics are informational and used to read table data more efficiently.
/// A reader can choose to ignore statistics information. Statistics support
/// is not required to read the table correctly.
///
/// When committing, changes are applied to the latest table metadata. Commit
/// conflicts are resolved by applying the changes to the new latest metadata
/// and reattempting the commit.
///
/// Apply() returns a list of the statistics files that will be affected.
///
/// Example usage:
/// \code
///   table.UpdateStatistics()
///       .SetStatistics(statistics_file)
///       .RemoveStatistics(old_snapshot_id)
///       .Commit();
/// \endcode
class ICEBERG_EXPORT UpdateStatistics
    : public PendingUpdateTyped<std::vector<StatisticsFile>> {
 public:
  ~UpdateStatistics() override = default;

  /// \brief Set the table's statistics file for a snapshot
  ///
  /// Sets the statistics file for a snapshot, replacing the previous statistics
  /// file for the snapshot if any exists. The snapshot ID is taken from the
  /// StatisticsFile object itself via statistics_file.snapshot_id.
  ///
  /// \param statistics_file The statistics file to set (contains snapshot ID)
  /// \return Reference to this for method chaining
  virtual UpdateStatistics& SetStatistics(const StatisticsFile& statistics_file) = 0;

  /// \brief Remove the table's statistics file for a snapshot
  ///
  /// Removes the statistics file associated with the specified snapshot.
  ///
  /// \param snapshot_id The ID of the snapshot whose statistics file should be removed
  /// \return Reference to this for method chaining
  virtual UpdateStatistics& RemoveStatistics(int64_t snapshot_id) = 0;

  // Non-copyable, movable (inherited from PendingUpdate)
  UpdateStatistics(const UpdateStatistics&) = delete;
  UpdateStatistics& operator=(const UpdateStatistics&) = delete;

 protected:
  UpdateStatistics() = default;
};

}  // namespace iceberg
