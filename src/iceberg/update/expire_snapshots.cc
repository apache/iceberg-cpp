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

#include "iceberg/update/expire_snapshots.h"

#include "iceberg/result.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"

namespace iceberg {

ExpireSnapshots::ExpireSnapshots(Table* table) : table_(table) {}

ExpireSnapshots& ExpireSnapshots::ExpireSnapshotId(int64_t snapshot_id) {
  snapshot_ids_to_expire_.push_back(snapshot_id);
  return *this;
}

ExpireSnapshots& ExpireSnapshots::ExpireOlderThan(int64_t timestamp_millis) {
  expire_older_than_ms_ = timestamp_millis;
  return *this;
}

ExpireSnapshots& ExpireSnapshots::RetainLast(int num_snapshots) {
  retain_last_ = num_snapshots;
  return *this;
}

ExpireSnapshots& ExpireSnapshots::DeleteWith(
    std::function<void(std::string_view)> delete_func) {
  delete_func_ = std::move(delete_func);
  return *this;
}

ExpireSnapshots& ExpireSnapshots::SetCleanupLevel(CleanupLevel level) {
  cleanup_level_ = level;
  return *this;
}

Result<std::vector<std::shared_ptr<Snapshot>>> ExpireSnapshots::ApplyTyped() {
  // Placeholder implementation - full snapshot expiration logic to be implemented
  return NotImplemented("ExpireSnapshots::ApplyTyped() is not yet implemented");
}

Status ExpireSnapshots::Commit() {
  // Placeholder implementation - full commit logic to be implemented
  return NotImplemented("ExpireSnapshots::Commit() is not yet implemented");
}

}  // namespace iceberg
