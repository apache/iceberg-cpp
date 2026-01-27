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

#include "iceberg/update/update_partition_statistics.h"

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "iceberg/result.h"
#include "iceberg/statistics_file.h"
#include "iceberg/transaction.h"
#include "iceberg/util/macros.h"

namespace iceberg {

Result<std::shared_ptr<UpdatePartitionStatistics>> UpdatePartitionStatistics::Make(
    std::shared_ptr<Transaction> transaction) {
  ICEBERG_PRECHECK(transaction != nullptr,
                   "Cannot create UpdatePartitionStatistics without a transaction");
  return std::shared_ptr<UpdatePartitionStatistics>(
      new UpdatePartitionStatistics(std::move(transaction)));
}

UpdatePartitionStatistics::UpdatePartitionStatistics(
    std::shared_ptr<Transaction> transaction)
    : PendingUpdate(std::move(transaction)) {}

UpdatePartitionStatistics::~UpdatePartitionStatistics() = default;

UpdatePartitionStatistics& UpdatePartitionStatistics::SetPartitionStatistics(
    std::shared_ptr<PartitionStatisticsFile> partition_statistics_file) {
  ICEBERG_BUILDER_CHECK(partition_statistics_file != nullptr,
                        "Statistics file cannot be null");

  partition_statistics_to_set_[partition_statistics_file->snapshot_id] =
      std::move(partition_statistics_file);
  return *this;
}

UpdatePartitionStatistics& UpdatePartitionStatistics::RemovePartitionStatistics(
    int64_t snapshot_id) {
  partition_statistics_to_set_[snapshot_id] = nullptr;
  return *this;
}

Result<UpdatePartitionStatistics::ApplyResult> UpdatePartitionStatistics::Apply() {
  ICEBERG_RETURN_UNEXPECTED(CheckErrors());

  ApplyResult result;
  for (const auto& [snapshot_id, partition_stats] : partition_statistics_to_set_) {
    if (partition_stats) {
      result.to_set.emplace_back(snapshot_id, partition_stats);
    } else {
      result.to_remove.push_back(snapshot_id);
    }
  }
  return result;
}

}  // namespace iceberg
