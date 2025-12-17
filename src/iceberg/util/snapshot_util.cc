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

#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/util/snapshot_util_internal.h"
#include "iceberg/util/timepoint.h"

namespace iceberg {

namespace {}  // namespace

Result<std::vector<std::shared_ptr<Snapshot>>> SnapshotUtil::AncestorsOf(
    const Table& table, int64_t snapshot_id) {
  ICEBERG_ASSIGN_OR_RAISE(auto start, table.SnapshotById(snapshot_id));
  if (!start) {
    return InvalidArgument("Cannot find snapshot: {}", snapshot_id);
  }
  return AncestorsOf(table, start);
}

Result<bool> SnapshotUtil::IsAncestorOf(const Table& table, int64_t snapshot_id,
                                        int64_t ancestor_snapshot_id) {
  ICEBERG_ASSIGN_OR_RAISE(auto ancestors, AncestorsOf(table, snapshot_id));
  for (const auto& snapshot : ancestors) {
    if (snapshot->snapshot_id == ancestor_snapshot_id) {
      return true;
    }
  }
  return false;
}

Result<bool> SnapshotUtil::IsAncestorOf(const Table& table,
                                        int64_t ancestor_snapshot_id) {
  ICEBERG_ASSIGN_OR_RAISE(auto current, table.current_snapshot());
  return IsAncestorOf(table, current->snapshot_id, ancestor_snapshot_id);
}

Result<bool> SnapshotUtil::IsParentAncestorOf(const Table& table, int64_t snapshot_id,
                                              int64_t ancestor_parent_snapshot_id) {
  ICEBERG_ASSIGN_OR_RAISE(auto ancestors, AncestorsOf(table, snapshot_id));
  for (const auto& snapshot : ancestors) {
    if (snapshot->parent_snapshot_id.has_value() &&
        snapshot->parent_snapshot_id.value() == ancestor_parent_snapshot_id) {
      return true;
    }
  }
  return false;
}

Result<std::vector<std::shared_ptr<Snapshot>>> SnapshotUtil::CurrentAncestors(
    const Table& table) {
  ICEBERG_ASSIGN_OR_RAISE(auto current, table.current_snapshot());
  return AncestorsOf(table, current);
}

Result<std::vector<int64_t>> SnapshotUtil::CurrentAncestorIds(const Table& table) {
  ICEBERG_ASSIGN_OR_RAISE(auto ancestors, CurrentAncestors(table));
  return ToIds(ancestors);
}

Result<std::optional<std::shared_ptr<Snapshot>>> SnapshotUtil::OldestAncestor(
    const Table& table) {
  ICEBERG_ASSIGN_OR_RAISE(auto ancestors, CurrentAncestors(table));
  if (ancestors.empty()) {
    return std::nullopt;
  }
  return ancestors.back();
}

Result<std::optional<std::shared_ptr<Snapshot>>> SnapshotUtil::OldestAncestorOf(
    const Table& table, int64_t snapshot_id) {
  ICEBERG_ASSIGN_OR_RAISE(auto ancestors, AncestorsOf(table, snapshot_id));
  if (ancestors.empty()) {
    return std::nullopt;
  }
  return ancestors.back();
}

Result<std::optional<std::shared_ptr<Snapshot>>> SnapshotUtil::OldestAncestorAfter(
    const Table& table, TimePointMs timestamp_ms) {
  ICEBERG_ASSIGN_OR_RAISE(auto current, table.current_snapshot());
  if (!current) {
    // there are no snapshots or ancestors
    return std::nullopt;
  }

  std::shared_ptr<Snapshot> last_snapshot = nullptr;
  auto ancestors = AncestorsOf(table, current);
  for (const auto& snapshot : ancestors) {
    auto snapshot_timestamp_ms = snapshot->timestamp_ms;
    if (snapshot_timestamp_ms < timestamp_ms) {
      return last_snapshot ? std::make_optional(last_snapshot) : std::nullopt;
    } else if (snapshot_timestamp_ms == timestamp_ms) {
      return snapshot;
    }
    last_snapshot = snapshot;
  }

  if (last_snapshot && !last_snapshot->parent_snapshot_id.has_value()) {
    // this is the first snapshot in the table, return it
    return last_snapshot;
  }

  return ValidationFailed("Cannot find snapshot older than {}",
                          FormatTimestamp(timestamp_ms));
}

Result<std::vector<int64_t>> SnapshotUtil::SnapshotIdsBetween(const Table& table,
                                                              int64_t from_snapshot_id,
                                                              int64_t to_snapshot_id) {
  ICEBERG_ASSIGN_OR_RAISE(auto to_snapshot, table.SnapshotById(to_snapshot_id));
  if (!to_snapshot) {
    return InvalidArgument("Cannot find snapshot: {}", to_snapshot_id);
  }

  // Create a lookup function that returns null when snapshot_id equals from_snapshot_id
  // This effectively stops traversal at from_snapshot_id (exclusive)
  auto lookup = [&table,
                 from_snapshot_id](int64_t id) -> Result<std::shared_ptr<Snapshot>> {
    if (id == from_snapshot_id) {
      return nullptr;
    }
    return table.SnapshotById(id);
  };

  auto ancestors = AncestorsOf(to_snapshot, lookup);
  return ToIds(ancestors);
}

Result<std::vector<int64_t>> SnapshotUtil::AncestorIdsBetween(
    const Table& table, int64_t latest_snapshot_id,
    const std::optional<int64_t>& oldest_snapshot_id) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto ancestors, AncestorsBetween(table, latest_snapshot_id, oldest_snapshot_id));
  return ToIds(ancestors);
}

Result<std::vector<std::shared_ptr<Snapshot>>> SnapshotUtil::AncestorsBetween(
    const Table& table, int64_t latest_snapshot_id,
    const std::optional<int64_t>& oldest_snapshot_id) {
  ICEBERG_ASSIGN_OR_RAISE(auto start, table.SnapshotById(latest_snapshot_id));
  if (!start) {
    return InvalidArgument("Cannot find snapshot: {}", latest_snapshot_id);
  }

  if (oldest_snapshot_id.has_value()) {
    if (latest_snapshot_id == oldest_snapshot_id.value()) {
      return std::vector<std::shared_ptr<Snapshot>>();
    }

    auto lookup = [&table, oldest_snapshot_id = oldest_snapshot_id.value()](
                      int64_t id) -> Result<std::shared_ptr<Snapshot>> {
      if (id == oldest_snapshot_id) {
        return nullptr;
      }
      return table.SnapshotById(id);
    };
    return AncestorsOf(start, lookup);
  } else {
    return AncestorsOf(table, start);
  }
}

std::vector<std::shared_ptr<Snapshot>> SnapshotUtil::AncestorsOf(
    const Table& table, const std::shared_ptr<Snapshot>& snapshot) {
  std::vector<std::shared_ptr<Snapshot>> result;
  if (!snapshot) {
    return result;
  }

  std::shared_ptr<Snapshot> current = snapshot;
  while (current) {
    result.push_back(current);
    if (!current->parent_snapshot_id.has_value()) {
      break;
    }
    auto parent_result = table.SnapshotById(current->parent_snapshot_id.value());
    if (!parent_result.has_value()) {
      // Parent snapshot not found (e.g., expired), stop traversal
      break;
    }
    current = parent_result.value();
  }

  return result;
}

std::vector<std::shared_ptr<Snapshot>> SnapshotUtil::AncestorsOf(
    const std::shared_ptr<Snapshot>& snapshot,
    const std::function<Result<std::shared_ptr<Snapshot>>(int64_t)>& lookup) {
  std::vector<std::shared_ptr<Snapshot>> result;
  if (!snapshot) {
    return result;
  }

  std::shared_ptr<Snapshot> current = snapshot;
  while (current) {
    result.push_back(current);
    if (!current->parent_snapshot_id.has_value()) {
      break;
    }
    auto parent_result = lookup(current->parent_snapshot_id.value());
    if (!parent_result.has_value()) {
      break;
    }
    auto parent = parent_result.value();
    if (!parent) {
      break;
    }
    current = parent;
  }

  return result;
}

std::vector<int64_t> SnapshotUtil::ToIds(
    const std::vector<std::shared_ptr<Snapshot>>& snapshots) {
  std::vector<int64_t> ids;
  ids.reserve(snapshots.size());
  for (const auto& snapshot : snapshots) {
    ids.push_back(snapshot->snapshot_id);
  }
  return ids;
}

Result<std::shared_ptr<Snapshot>> SnapshotUtil::SnapshotAfter(const Table& table,
                                                              int64_t snapshot_id) {
  ICEBERG_ASSIGN_OR_RAISE(auto parent, table.SnapshotById(snapshot_id));
  if (!parent) {
    return InvalidArgument("Cannot find parent snapshot: {}", snapshot_id);
  }

  ICEBERG_ASSIGN_OR_RAISE(auto ancestors, CurrentAncestors(table));
  for (const auto& current : ancestors) {
    if (current->parent_snapshot_id.has_value() &&
        current->parent_snapshot_id.value() == snapshot_id) {
      return current;
    }
  }

  return ValidationFailed(
      "Cannot find snapshot after {}: not an ancestor of table's current snapshot",
      snapshot_id);
}

Result<int64_t> SnapshotUtil::SnapshotIdAsOfTime(const Table& table,
                                                 TimePointMs timestamp_ms) {
  auto snapshot_id = NullableSnapshotIdAsOfTime(table, timestamp_ms);
  if (!snapshot_id) {
    return ValidationFailed("Cannot find a snapshot older than {}",
                            FormatTimestamp(timestamp_ms));
  }
  return *snapshot_id;
}

std::optional<int64_t> SnapshotUtil::NullableSnapshotIdAsOfTime(
    const Table& table, TimePointMs timestamp_ms) {
  std::optional<int64_t> snapshot_id = std::nullopt;
  const auto& history = table.history();
  for (const auto& log_entry : history) {
    if (log_entry.timestamp_ms <= timestamp_ms) {
      snapshot_id = log_entry.snapshot_id;
    }
  }
  return snapshot_id;
}

Result<std::shared_ptr<Schema>> SnapshotUtil::SchemaFor(const Table& table,
                                                        int64_t snapshot_id) {
  ICEBERG_ASSIGN_OR_RAISE(auto snapshot, table.SnapshotById(snapshot_id));

  if (snapshot->schema_id.has_value()) {
    ICEBERG_ASSIGN_OR_RAISE(auto schemas, table.schemas());
    auto it = schemas.get().find(snapshot->schema_id.value());
    if (it == schemas.get().end()) {
      return ValidationFailed("Cannot find schema with schema id {}",
                              snapshot->schema_id.value());
    }
    return it->second;
  }

  return table.schema();
}

Result<std::shared_ptr<Schema>> SnapshotUtil::SchemaFor(const Table& table,
                                                        TimePointMs timestamp_ms) {
  ICEBERG_ASSIGN_OR_RAISE(auto id, SnapshotIdAsOfTime(table, timestamp_ms));
  return SchemaFor(table, id);
}

Result<std::shared_ptr<Schema>> SnapshotUtil::SchemaFor(const Table& table,
                                                        const std::string& ref) {
  if (ref.empty() || ref == SnapshotRef::kMainBranch) {
    return table.schema();
  }

  const auto& metadata = table.metadata();
  auto it = metadata->refs.find(ref);
  if (it == metadata->refs.end() || it->second->type() == SnapshotRefType::kBranch) {
    return table.schema();
  }

  return SchemaFor(table, it->second->snapshot_id);
}

Result<std::shared_ptr<Schema>> SnapshotUtil::SchemaFor(const TableMetadata& metadata,
                                                        const std::string& ref) {
  if (ref.empty() || ref == SnapshotRef::kMainBranch) {
    return metadata.Schema();
  }

  auto it = metadata.refs.find(ref);
  if (it == metadata.refs.end() || it->second->type() == SnapshotRefType::kBranch) {
    return metadata.Schema();
  }

  ICEBERG_ASSIGN_OR_RAISE(auto snapshot, metadata.SnapshotById(it->second->snapshot_id));
  if (!snapshot->schema_id.has_value()) {
    return metadata.Schema();
  }

  return metadata.SchemaById(snapshot->schema_id);
}

Result<std::optional<std::shared_ptr<Snapshot>>> SnapshotUtil::LatestSnapshot(
    const Table& table, const std::string& branch) {
  if (branch.empty() || branch == SnapshotRef::kMainBranch) {
    return table.current_snapshot();
  }

  const auto& metadata = table.metadata();
  auto it = metadata->refs.find(branch);
  if (it == metadata->refs.end()) {
    return table.current_snapshot();
  }

  return table.SnapshotById(it->second->snapshot_id);
}

Result<std::optional<std::shared_ptr<Snapshot>>> SnapshotUtil::LatestSnapshot(
    const TableMetadata& metadata, const std::string& branch) {
  if (branch.empty() || branch == SnapshotRef::kMainBranch) {
    return metadata.Snapshot();
  }

  auto it = metadata.refs.find(branch);
  if (it == metadata.refs.end()) {
    return metadata.Snapshot();
  }

  return metadata.SnapshotById(it->second->snapshot_id);
}

}  // namespace iceberg
