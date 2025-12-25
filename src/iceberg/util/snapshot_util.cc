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

#include <ranges>

#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/snapshot_util_internal.h"
#include "iceberg/util/timepoint.h"

namespace iceberg {

// Shorthand to return for a NotFound error.
#define ICEBERG_ACTION_FOR_NOT_FOUND(result, action)   \
  if (!result.has_value()) [[unlikely]] {              \
    if (result.error().kind == ErrorKind::kNotFound) { \
      action;                                          \
    }                                                  \
    return std::unexpected<Error>(result.error());     \
  }

Result<std::vector<std::shared_ptr<Snapshot>>> SnapshotUtil::AncestorsOf(
    const Table& table, int64_t snapshot_id) {
  return table.SnapshotById(snapshot_id).and_then([&table](const auto& snapshot) {
    return AncestorsOf(table, snapshot);
  });
}

Result<bool> SnapshotUtil::IsAncestorOf(const Table& table, int64_t snapshot_id,
                                        int64_t ancestor_snapshot_id) {
  ICEBERG_ASSIGN_OR_RAISE(auto ancestors, AncestorsOf(table, snapshot_id));
  return std::ranges::any_of(ancestors, [ancestor_snapshot_id](const auto& snapshot) {
    return snapshot != nullptr && snapshot->snapshot_id == ancestor_snapshot_id;
  });
}

Result<bool> SnapshotUtil::IsAncestorOf(const Table& table,
                                        int64_t ancestor_snapshot_id) {
  ICEBERG_ASSIGN_OR_RAISE(auto current, table.current_snapshot());
  ICEBERG_CHECK(current != nullptr, "Current snapshot is null");
  return IsAncestorOf(table, current->snapshot_id, ancestor_snapshot_id);
}

Result<bool> SnapshotUtil::IsParentAncestorOf(const Table& table, int64_t snapshot_id,
                                              int64_t ancestor_parent_snapshot_id) {
  ICEBERG_ASSIGN_OR_RAISE(auto ancestors, AncestorsOf(table, snapshot_id));
  return std::ranges::any_of(
      ancestors, [ancestor_parent_snapshot_id](const auto& snapshot) {
        return snapshot != nullptr && snapshot->parent_snapshot_id.has_value() &&
               snapshot->parent_snapshot_id.value() == ancestor_parent_snapshot_id;
      });
}

Result<std::vector<std::shared_ptr<Snapshot>>> SnapshotUtil::CurrentAncestors(
    const Table& table) {
  auto current_result = table.current_snapshot();
  ICEBERG_ACTION_FOR_NOT_FOUND(current_result, return {});
  return AncestorsOf(table, current_result.value());
}

Result<std::vector<int64_t>> SnapshotUtil::CurrentAncestorIds(const Table& table) {
  return CurrentAncestors(table).and_then(ToIds);
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
  auto current_result = table.current_snapshot();
  ICEBERG_ACTION_FOR_NOT_FOUND(current_result, { return std::nullopt; });
  auto current = std::move(current_result.value());

  std::optional<std::shared_ptr<Snapshot>> last_snapshot = std::nullopt;
  ICEBERG_ASSIGN_OR_RAISE(auto ancestors, AncestorsOf(table, current));
  for (const auto& snapshot : ancestors) {
    auto snapshot_timestamp_ms = snapshot->timestamp_ms;
    if (snapshot_timestamp_ms < timestamp_ms) {
      return last_snapshot;
    } else if (snapshot_timestamp_ms == timestamp_ms) {
      return snapshot;
    }
    last_snapshot = std::move(snapshot);
  }

  if (last_snapshot.has_value() && last_snapshot.value() != nullptr &&
      !last_snapshot.value()->parent_snapshot_id.has_value()) {
    // this is the first snapshot in the table, return it
    return last_snapshot;
  }

  // the first ancestor after the given time can't be determined
  return NotFound("Cannot find snapshot older than {}", FormatTimePointMs(timestamp_ms));
}

Result<std::vector<int64_t>> SnapshotUtil::SnapshotIdsBetween(const Table& table,
                                                              int64_t from_snapshot_id,
                                                              int64_t to_snapshot_id) {
  // Create a lookup function that returns null when snapshot_id equals from_snapshot_id.
  // This effectively stops traversal at from_snapshot_id (exclusive)
  auto lookup = [&table,
                 from_snapshot_id](int64_t id) -> Result<std::shared_ptr<Snapshot>> {
    if (id == from_snapshot_id) {
      return nullptr;
    }
    return table.SnapshotById(id);
  };

  return table.SnapshotById(to_snapshot_id)
      .and_then(
          [&lookup](const auto& to_snapshot) { return AncestorsOf(to_snapshot, lookup); })
      .and_then(ToIds);
}

Result<std::vector<int64_t>> SnapshotUtil::AncestorIdsBetween(
    const Table& table, int64_t latest_snapshot_id,
    const std::optional<int64_t>& oldest_snapshot_id) {
  return AncestorsBetween(table, latest_snapshot_id, oldest_snapshot_id).and_then(ToIds);
}

Result<std::vector<std::shared_ptr<Snapshot>>> SnapshotUtil::AncestorsBetween(
    const Table& table, int64_t latest_snapshot_id,
    std::optional<int64_t> oldest_snapshot_id) {
  ICEBERG_ASSIGN_OR_RAISE(auto start, table.SnapshotById(latest_snapshot_id));

  if (oldest_snapshot_id.has_value()) {
    if (latest_snapshot_id == oldest_snapshot_id.value()) {
      return {};
    }

    return AncestorsOf(start,
                       [&table, oldest_snapshot_id = oldest_snapshot_id.value()](
                           int64_t id) -> Result<std::shared_ptr<Snapshot>> {
                         if (id == oldest_snapshot_id) {
                           return nullptr;
                         }
                         return table.SnapshotById(id);
                       });
  } else {
    return AncestorsOf(table, start);
  }
}

Result<std::vector<std::shared_ptr<Snapshot>>> SnapshotUtil::AncestorsOf(
    const Table& table, const std::shared_ptr<Snapshot>& snapshot) {
  return AncestorsOf(snapshot, [&table](int64_t id) { return table.SnapshotById(id); });
}

Result<std::vector<std::shared_ptr<Snapshot>>> SnapshotUtil::AncestorsOf(
    const std::shared_ptr<Snapshot>& snapshot,
    const std::function<Result<std::shared_ptr<Snapshot>>(int64_t)>& lookup) {
  ICEBERG_PRECHECK(snapshot != nullptr, "Snapshot is null");

  std::shared_ptr<Snapshot> current = snapshot;
  std::vector<std::shared_ptr<Snapshot>> result;

  while (current != nullptr) {
    result.push_back(current);
    if (!current->parent_snapshot_id.has_value()) {
      break;
    }
    auto parent_result = lookup(current->parent_snapshot_id.value());
    ICEBERG_ACTION_FOR_NOT_FOUND(parent_result, { break; });
    current = std::move(parent_result.value());
  }

  return result;
}

Result<std::vector<int64_t>> SnapshotUtil::ToIds(
    const std::vector<std::shared_ptr<Snapshot>>& snapshots) {
  return snapshots |
         std::views::filter([](const auto& snapshot) { return snapshot != nullptr; }) |
         std::views::transform(
             [](const auto& snapshot) { return snapshot->snapshot_id; }) |
         std::ranges::to<std::vector<int64_t>>();
}

Result<std::shared_ptr<Snapshot>> SnapshotUtil::SnapshotAfter(const Table& table,
                                                              int64_t snapshot_id) {
  ICEBERG_ASSIGN_OR_RAISE(auto parent, table.SnapshotById(snapshot_id));
  ICEBERG_CHECK(parent != nullptr, "Snapshot is null for id {}", snapshot_id);

  ICEBERG_ASSIGN_OR_RAISE(auto ancestors, CurrentAncestors(table));
  for (const auto& current : ancestors) {
    if (current != nullptr && current->parent_snapshot_id.has_value() &&
        current->parent_snapshot_id.value() == snapshot_id) {
      return current;
    }
  }

  return NotFound(
      "Cannot find snapshot after {}: not an ancestor of table's current snapshot",
      snapshot_id);
}

Result<int64_t> SnapshotUtil::SnapshotIdAsOfTime(const Table& table,
                                                 TimePointMs timestamp_ms) {
  auto snapshot_id = OptionalSnapshotIdAsOfTime(table, timestamp_ms);
  ICEBERG_CHECK(snapshot_id.has_value(), "Cannot find a snapshot older than {}",
                FormatTimePointMs(timestamp_ms));
  return snapshot_id.value();
}

std::optional<int64_t> SnapshotUtil::OptionalSnapshotIdAsOfTime(
    const Table& table, TimePointMs timestamp_ms) {
  std::optional<int64_t> snapshot_id = std::nullopt;
  for (const auto& log_entry : table.history()) {
    if (log_entry.timestamp_ms <= timestamp_ms) {
      snapshot_id = log_entry.snapshot_id;
    }
  }
  return snapshot_id;
}

Result<std::shared_ptr<Schema>> SnapshotUtil::SchemaFor(const Table& table,
                                                        int64_t snapshot_id) {
  ICEBERG_ASSIGN_OR_RAISE(auto snapshot, table.SnapshotById(snapshot_id));
  ICEBERG_CHECK(snapshot, "Snapshot is null for id {}", snapshot_id);

  if (snapshot->schema_id.has_value()) {
    return table.metadata()->SchemaById(snapshot->schema_id.value());
  }

  // TODO(any): recover the schema by reading previous metadata files
  return table.schema();
}

Result<std::shared_ptr<Schema>> SnapshotUtil::SchemaFor(const Table& table,
                                                        TimePointMs timestamp_ms) {
  return SnapshotIdAsOfTime(table, timestamp_ms).and_then([&table](int64_t id) {
    return SchemaFor(table, id);
  });
}

Result<std::shared_ptr<Schema>> SnapshotUtil::SchemaFor(const Table& table,
                                                        const std::string& ref) {
  if (ref.empty() || ref == SnapshotRef::kMainBranch) {
    return table.schema();
  }

  const auto& metadata = table.metadata();
  auto it = metadata->refs.find(ref);
  if (it == metadata->refs.cend() || it->second->type() == SnapshotRefType::kBranch) {
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

Result<std::shared_ptr<Snapshot>> SnapshotUtil::LatestSnapshot(
    const Table& table, const std::string& branch) {
  return LatestSnapshot(*table.metadata(), branch);
}

Result<std::shared_ptr<Snapshot>> SnapshotUtil::LatestSnapshot(
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
