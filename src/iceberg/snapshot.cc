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

#include "iceberg/snapshot.h"

namespace iceberg {

bool SnapshotRef::Branch::Equals(const SnapshotRef::Branch& other) const {
  return min_snapshots_to_keep == other.min_snapshots_to_keep &&
         max_snapshot_age_ms == other.max_snapshot_age_ms &&
         max_ref_age_ms == other.max_ref_age_ms;
}

bool SnapshotRef::Tag::Equals(const SnapshotRef::Tag& other) const {
  return max_ref_age_ms == other.max_ref_age_ms;
}

SnapshotRefType SnapshotRef::type() const noexcept {
  return std::visit(
      [&](const auto& retention) -> SnapshotRefType {
        using T = std::decay_t<decltype(retention)>;
        if constexpr (std::is_same_v<T, Branch>) {
          return SnapshotRefType::kBranch;
        } else {
          return SnapshotRefType::kTag;
        }
      },
      retention);
}

bool SnapshotRef::Equals(const SnapshotRef& other) const {
  if (this == &other) {
    return true;
  }
  if (type() != other.type()) {
    return false;
  }

  if (type() == SnapshotRefType::kBranch) {
    return snapshot_id == other.snapshot_id &&
           std::get<Branch>(retention) == std::get<Branch>(other.retention);

  } else {
    return snapshot_id == other.snapshot_id &&
           std::get<Tag>(retention) == std::get<Tag>(other.retention);
  }
}

std::optional<std::string_view> Snapshot::operation() const {
  auto it = summary.find(SnapshotSummaryFields::kOperation);
  if (it != summary.end()) {
    return it->second;
  }
  return std::nullopt;
}

bool Snapshot::Equals(const Snapshot& other) const {
  if (this == &other) {
    return true;
  }
  return snapshot_id == other.snapshot_id &&
         parent_snapshot_id == other.parent_snapshot_id &&
         sequence_number == other.sequence_number && timestamp_ms == other.timestamp_ms &&
         schema_id == other.schema_id;
}

}  // namespace iceberg
