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

const std::string SnapshotSummaryFields::kOperation = "operation";
const std::string SnapshotSummaryFields::kAddedDataFiles = "added-data-files";
const std::string SnapshotSummaryFields::kDeletedDataFiles = "deleted-data-files";
const std::string SnapshotSummaryFields::kTotalDataFiles = "total-data-files";
const std::string SnapshotSummaryFields::kAddedDeleteFiles = "added-delete-files";
const std::string SnapshotSummaryFields::kAddedEqDeleteFiles =
    "added-equality-delete-files";
const std::string SnapshotSummaryFields::kRemovedEqDeleteFiles =
    "removed-equality-delete-files";
const std::string SnapshotSummaryFields::kAddedPosDeleteFiles =
    "added-position-delete-files";
const std::string SnapshotSummaryFields::kRemovedPosDeleteFiles =
    "removed-position-delete-files";
const std::string SnapshotSummaryFields::kAddedDVs = "added-dvs";
const std::string SnapshotSummaryFields::kRemovedDVs = "removed-dvs";
const std::string SnapshotSummaryFields::kRemovedDeleteFiles = "removed-delete-files";
const std::string SnapshotSummaryFields::kTotalDeleteFiles = "total-delete-files";
const std::string SnapshotSummaryFields::kAddedRecords = "added-records";
const std::string SnapshotSummaryFields::kDeletedRecords = "deleted-records";
const std::string SnapshotSummaryFields::kTotalRecords = "total-records";
const std::string SnapshotSummaryFields::kAddedFileSize = "added-files-size";
const std::string SnapshotSummaryFields::kRemovedFileSize = "removed-files-size";
const std::string SnapshotSummaryFields::kTotalFileSize = "total-files-size";
const std::string SnapshotSummaryFields::kAddedPosDeletes = "added-position-deletes";
const std::string SnapshotSummaryFields::kRemovedPosDeletes = "removed-position-deletes";
const std::string SnapshotSummaryFields::kTotalPosDeletes = "total-position-deletes";
const std::string SnapshotSummaryFields::kAddedEqDeletes = "added-equality-deletes";
const std::string SnapshotSummaryFields::kRemovedEqDeletes = "removed-equality-deletes";
const std::string SnapshotSummaryFields::kTotalEqDeletes = "total-equality-deletes";
const std::string SnapshotSummaryFields::kDeletedDuplicatedFiles =
    "deleted-duplicate-files";
const std::string SnapshotSummaryFields::kChangedPartitionCountProp =
    "changed-partition-count";

const std::string SnapshotSummaryFields::kWAPID = "wap.id";
const std::string SnapshotSummaryFields::kPublishedWAPID = "published-wap-id";
const std::string SnapshotSummaryFields::kSourceSnapshotID = "source-snapshot-id";
const std::string SnapshotSummaryFields::kEngineName = "engine-name";
const std::string SnapshotSummaryFields::kEngineVersion = "engine-version";

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
