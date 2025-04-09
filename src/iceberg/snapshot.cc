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

#include <format>

#include "iceberg/util/formatter.h"

namespace iceberg {

std::optional<std::string> Snapshot::operation() const {
  auto it = summary.find(std::string(SnapshotSummaryFields::kOperation));
  if (it != summary.end()) {
    return it->second;
  }
  return std::nullopt;
}

std::optional<std::reference_wrapper<const Snapshot::manifest_list_t>>
Snapshot::ManifestList() const {
  if (std::holds_alternative<manifest_list_t>(manifest_list)) {
    return std::cref(std::get<manifest_list_t>(manifest_list));
  }
  return std::nullopt;
}

std::optional<std::reference_wrapper<const Snapshot::manifests_t>> Snapshot::Manifests()
    const {
  if (std::holds_alternative<manifests_t>(manifest_list)) {
    return std::cref(std::get<manifests_t>(manifest_list));
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
