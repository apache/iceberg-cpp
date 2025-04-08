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
#include <optional>

#include "iceberg/iceberg_export.h"

namespace iceberg {

/// \brief The type of snapshot reference
enum class SnapshotRefType {
  /// Branches are mutable named references that can be updated by committing a new
  /// snapshot as the branch’s referenced snapshot using the Commit Conflict Resolution
  /// and Retry procedures.
  kBranch,
  /// Tags are labels for individual snapshots
  kTag,
};

/// \brief A reference to a snapshot, either a branch or a tag.
struct ICEBERG_EXPORT SnapshotRef {
  /// A reference's snapshot ID. The tagged snapshot or latest snapshot of a branch.
  int64_t snapshot_id;
  /// Type of the reference, tag or branch
  SnapshotRefType type;
  /// For branch type only, a positive number for the minimum number of snapshots to keep
  /// in a branch while expiring snapshots. Defaults to table property
  /// history.expire.min-snapshots-to-keep.
  std::optional<int32_t> min_snapshots_to_keep;
  /// For branch type only, a positive number for the max age of snapshots to keep when
  /// expiring, including the latest snapshot. Defaults to table property
  /// history.expire.max-snapshot-age-ms.
  std::optional<int64_t> max_snapshot_age_ms;
  /// For snapshot references except the main branch, a positive number for the max age of
  /// the snapshot reference to keep while expiring snapshots. Defaults to table property
  /// history.expire.max-ref-age-ms. The main branch never expires.
  std::optional<int64_t> max_ref_age_ms;
};

}  // namespace iceberg
