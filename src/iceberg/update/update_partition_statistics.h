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

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/update/pending_update.h"

/// \file iceberg/update/update_partition_statistics.h
/// \brief Updates table partition statistics.

namespace iceberg {

/// \brief Updates table partition statistics.
class ICEBERG_EXPORT UpdatePartitionStatistics : public PendingUpdate {
 public:
  static Result<std::shared_ptr<UpdatePartitionStatistics>> Make(
      std::shared_ptr<Transaction> transaction);

  ~UpdatePartitionStatistics() override;

  /// \brief Set partition statistics file for a snapshot.
  ///
  /// Associates a partition statistics file with a snapshot ID. If partition statistics
  /// already exist for this snapshot, they will be replaced.
  ///
  /// \param partition_statistics_file The partition statistics file to set
  /// \return Reference to this UpdatePartitionStatistics for chaining
  UpdatePartitionStatistics& SetPartitionStatistics(
      std::shared_ptr<PartitionStatisticsFile> partition_statistics_file);

  /// \brief Remove partition statistics for a snapshot.
  ///
  /// Marks the partition statistics for the given snapshot ID for removal.
  ///
  /// \param snapshot_id The snapshot ID whose partition statistics to remove
  /// \return Reference to this UpdatePartitionStatistics for chaining
  UpdatePartitionStatistics& RemovePartitionStatistics(int64_t snapshot_id);

  Kind kind() const final { return Kind::kUpdatePartitionStatistics; }

  struct ApplyResult {
    std::vector<std::pair<int64_t, std::shared_ptr<PartitionStatisticsFile>>> to_set;
    std::vector<int64_t> to_remove;
  };

  Result<ApplyResult> Apply();

 private:
  explicit UpdatePartitionStatistics(std::shared_ptr<Transaction> transaction);

  std::unordered_map<int64_t, std::shared_ptr<PartitionStatisticsFile>>
      partition_statistics_to_set_;
};

}  // namespace iceberg
