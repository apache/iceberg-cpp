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

#include "iceberg/update/set_snapshot.h"

#include <cstdint>
#include <memory>
#include <optional>

#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
#include "iceberg/transaction.h"
#include "iceberg/util/error_collector.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/snapshot_util_internal.h"
#include "iceberg/util/timepoint.h"

namespace iceberg {

Result<std::shared_ptr<SetSnapshot>> SetSnapshot::Make(
    std::shared_ptr<Transaction> transaction) {
  ICEBERG_PRECHECK(transaction != nullptr,
                   "Cannot create SetSnapshot without a transaction");
  return std::shared_ptr<SetSnapshot>(new SetSnapshot(std::move(transaction)));
}

SetSnapshot::SetSnapshot(std::shared_ptr<Transaction> transaction)
    : PendingUpdate(std::move(transaction)) {}

SetSnapshot::~SetSnapshot() = default;

SetSnapshot& SetSnapshot::SetCurrentSnapshot(int64_t snapshot_id) {
  // Validate that the snapshot exists
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto snapshot, base().SnapshotById(snapshot_id));
  ICEBERG_BUILDER_CHECK(snapshot != nullptr,
                        "Cannot roll back to unknown snapshot id: {}", snapshot_id);
  target_snapshot_id_ = snapshot_id;
  return *this;
}

SetSnapshot& SetSnapshot::RollbackToTime(int64_t timestamp_ms) {
  // Find the latest snapshot by timestamp older than timestamp_ms
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto snapshot_id_opt,
                                   FindLatestAncestorOlderThan(timestamp_ms));

  ICEBERG_BUILDER_CHECK(snapshot_id_opt.has_value(),
                        "Cannot roll back, no valid snapshot older than: {}",
                        timestamp_ms);

  target_snapshot_id_ = snapshot_id_opt.value();
  is_rollback_ = true;

  return *this;
}

SetSnapshot& SetSnapshot::RollbackTo(int64_t snapshot_id) {
  // Validate that the snapshot exists
  auto snapshot_result = base().SnapshotById(snapshot_id);
  ICEBERG_BUILDER_CHECK(snapshot_result.has_value(),
                        "Cannot roll back to unknown snapshot id: {}", snapshot_id);

  // Validate that the snapshot is an ancestor of the current state
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(bool is_ancestor,
                                   SnapshotUtil::IsAncestorOf(base(), snapshot_id));
  ICEBERG_BUILDER_CHECK(
      is_ancestor,
      "Cannot roll back to snapshot, not an ancestor of the current state: {}",
      snapshot_id);

  return SetCurrentSnapshot(snapshot_id);
}

Result<int64_t> SetSnapshot::Apply() {
  ICEBERG_RETURN_UNEXPECTED(CheckErrors());

  const TableMetadata& base_metadata = transaction_->current();

  // If no target snapshot was configured, return current state (NOOP)
  if (!target_snapshot_id_.has_value()) {
    ICEBERG_ASSIGN_OR_RAISE(auto current_snapshot, base_metadata.Snapshot());
    return current_snapshot->snapshot_id;
  }

  // Validate that the snapshot exists
  auto snapshot_result = base_metadata.SnapshotById(target_snapshot_id_.value());
  ICEBERG_CHECK(snapshot_result.has_value(),
                "Cannot roll back to unknown snapshot id: {}",
                target_snapshot_id_.value());

  // If this is a rollback, validate that the target is still an ancestor
  if (is_rollback_) {
    ICEBERG_ASSIGN_OR_RAISE(
        bool is_ancestor,
        SnapshotUtil::IsAncestorOf(base_metadata, target_snapshot_id_.value()));
    ICEBERG_CHECK(is_ancestor,
                  "Cannot roll back to {}: not an ancestor of the current table state",
                  target_snapshot_id_.value());
  }

  return target_snapshot_id_.value();
}

Result<std::optional<int64_t>> SetSnapshot::FindLatestAncestorOlderThan(
    int64_t timestamp_ms) const {
  ICEBERG_ASSIGN_OR_RAISE(auto ancestors, SnapshotUtil::CurrentAncestors(base()));

  TimePointMs target_timestamp = TimePointMsFromUnixMs(timestamp_ms);
  TimePointMs latest_timestamp = TimePointMsFromUnixMs(0);
  std::optional<int64_t> result = std::nullopt;

  for (const auto& snapshot : ancestors) {
    if (snapshot == nullptr) {
      continue;
    }
    auto current_timestamp = snapshot->timestamp_ms;
    if (current_timestamp < target_timestamp && current_timestamp > latest_timestamp) {
      latest_timestamp = current_timestamp;
      result = snapshot->snapshot_id;
    }
  }

  return result;
}

}  // namespace iceberg
