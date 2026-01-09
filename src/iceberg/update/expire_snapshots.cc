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

#include <cstdint>
#include <iostream>
#include <memory>
#include <unordered_set>
#include <vector>

#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/transaction.h"
#include "iceberg/util/error_collector.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/snapshot_util_internal.h"

namespace iceberg {

Result<std::shared_ptr<ExpireSnapshots>> ExpireSnapshots::Make(
    std::shared_ptr<Transaction> transaction) {
  ICEBERG_PRECHECK(transaction != nullptr,
                   "Cannot create ExpireSnapshots without a transaction");
  return std::shared_ptr<ExpireSnapshots>(new ExpireSnapshots(std::move(transaction)));
}

ExpireSnapshots::ExpireSnapshots(
    [[maybe_unused]] std::shared_ptr<Transaction> transaction)
    : PendingUpdate(std::move(transaction)),
      default_min_num_snapshots_(
          transaction_->current().properties.Get(TableProperties::kMinSnapshotsToKeep)),
      default_max_ref_age_ms_(
          transaction_->current().properties.Get(TableProperties::kMaxRefAgeMs)),
      current_time_ms_(std::chrono::time_point_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now())) {
  auto max_snapshot_age_ms =
      transaction_->current().properties.Get(TableProperties::kMaxSnapshotAgeMs);
  default_expire_older_than_ =
      current_time_ms_.time_since_epoch().count() - max_snapshot_age_ms;
}

ExpireSnapshots::~ExpireSnapshots() = default;

ExpireSnapshots& ExpireSnapshots::ExpireSnapshotId(int64_t snapshot_id) {
  const auto& current_metadata = transaction_->current();
  auto iter = std::ranges::find_if(current_metadata.snapshots,
                                   [&](const std::shared_ptr<Snapshot>& snapshot) {
                                     return snapshot->snapshot_id == snapshot_id;
                                   });

  ICEBERG_BUILDER_CHECK(iter != current_metadata.snapshots.end(),
                        "Snapshot:{} not exist.", snapshot_id);
  snapshot_ids_to_expire_.push_back(snapshot_id);
  return *this;
}

ExpireSnapshots& ExpireSnapshots::ExpireOlderThan(int64_t timestamp_millis) {
  ICEBERG_BUILDER_CHECK(timestamp_millis > 0, "Timestamp must be positive: {}",
                        timestamp_millis);
  default_expire_older_than_ = timestamp_millis;
  return *this;
}

ExpireSnapshots& ExpireSnapshots::RetainLast(int num_snapshots) {
  ICEBERG_BUILDER_CHECK(num_snapshots > 0,
                        "Number of snapshots to retain must be positive: {}",
                        num_snapshots);
  default_min_num_snapshots_ = num_snapshots;
  return *this;
}

ExpireSnapshots& ExpireSnapshots::DeleteWith(
    std::function<void(const std::string&)> delete_func) {
  ICEBERG_BUILDER_CHECK(delete_func != nullptr, "Delete function cannot be null");
  delete_func_ = std::move(delete_func);
  return *this;
}

ExpireSnapshots& ExpireSnapshots::CleanupLevel(enum CleanupLevel level) {
  cleanup_level_ = level;
  return *this;
}

ExpireSnapshots& ExpireSnapshots::CleanExpiredMetadata(bool clean) {
  clean_expired_metadata_ = clean;
  return *this;
}

Result<std::vector<int64_t>> ExpireSnapshots::ComputeBranchSnapshotsToRetain(
    const Table& table, int64_t snapshot, int64_t expire_snapshot_older_than,
    int32_t min_snapshots_to_keep) {
  std::vector<int64_t> snapshot_ids_to_retain;
  ICEBERG_ASSIGN_OR_RAISE(auto snapshots, SnapshotUtil::AncestorsOf(table, snapshot));
  for (const auto& ancestor : snapshots) {
    if (snapshot_ids_to_retain.size() < min_snapshots_to_keep ||
        ancestor->timestamp_ms.time_since_epoch().count() > expire_snapshot_older_than) {
      snapshot_ids_to_retain.emplace_back(ancestor->snapshot_id);
    } else {
      break;
    }
  }
  return snapshot_ids_to_retain;
}

Result<std::vector<int64_t>> ExpireSnapshots::ComputeAllBranchSnapshotIds(
    const Table& table, const SnapshotToRef& retained_refs) {
  std::vector<int64_t> snapshot_ids_to_retain;
  for (const auto& [key, ref] : retained_refs) {
    if (ref->type() == SnapshotRefType::kBranch) {
      const auto& branch = std::get<SnapshotRef::Branch>(ref->retention);
      int64_t expire_snapshot_older_than =
          branch.max_snapshot_age_ms.has_value()
              ? current_time_ms_.time_since_epoch().count() -
                    branch.max_snapshot_age_ms.value()
              : default_expire_older_than_;

      int32_t min_snapshots_to_keep = branch.min_snapshots_to_keep.has_value()
                                          ? branch.min_snapshots_to_keep.value()
                                          : default_min_num_snapshots_;

      ICEBERG_ASSIGN_OR_RAISE(auto to_retain,
                              ComputeBranchSnapshotsToRetain(table, ref->snapshot_id,
                                                             expire_snapshot_older_than,
                                                             min_snapshots_to_keep));
      snapshot_ids_to_retain.insert(snapshot_ids_to_retain.end(), to_retain.begin(),
                                    to_retain.end());
    }
  }
  return snapshot_ids_to_retain;
}

Result<std::vector<int64_t>> ExpireSnapshots::UnreferencedSnapshotIds(
    const Table& table, const TableMetadata& current_metadata,
    const SnapshotToRef& retained_refs) {
  std::unordered_set<int64_t> referenced_snapshot_ids;
  std::vector<int64_t> snapshot_ids_to_retain;
  for (const auto& [key, ref] : retained_refs) {
    if (ref->type() == SnapshotRefType::kBranch) {
      ICEBERG_ASSIGN_OR_RAISE(auto snapshots,
                              SnapshotUtil::AncestorsOf(table, ref->snapshot_id));
      for (const auto& snapshot : snapshots) {
        referenced_snapshot_ids.insert(snapshot->snapshot_id);
      }
    } else {
      referenced_snapshot_ids.insert(ref->snapshot_id);
    }
  }

  for (const auto& snapshot : current_metadata.snapshots) {
    if (!referenced_snapshot_ids.contains(snapshot->snapshot_id) &&
        // unreferenced, not old enough to expire
        snapshot->timestamp_ms.time_since_epoch().count() > default_expire_older_than_) {
      snapshot_ids_to_retain.emplace_back(snapshot->snapshot_id);
    }
  }
  return snapshot_ids_to_retain;
}

Result<ExpireSnapshots::ExpireSnapshotsResult> ExpireSnapshots::Apply() {
  ICEBERG_RETURN_UNEXPECTED(CheckErrors());
  const TableMetadata& current_metadata = transaction_->current();
  // attempt to clean expired metadata even if there are no snapshots to expire
  // table metadata builder takes care of the case when this should actually be a no-op
  if (current_metadata.snapshots.empty() && !clean_expired_metadata_) {
    return {};
  }
  std::unordered_set<int64_t> retained_snapshot_ids;
  SnapshotToRef retained_refs;
  std::unordered_map<int64_t, std::vector<std::string>> retained_id_to_ref;

  for (const auto& [key, ref] : current_metadata.refs) {
    std::cout << "handle ref:" << key << " snapshot:" << ref->snapshot_id << std::endl;
    if (key == SnapshotRef::kMainBranch) {
      std::cout << "retain main ref:" << key << " snapshot:" << ref->snapshot_id
                << std::endl;
      retained_refs[key] = ref;
      continue;
    }
    ICEBERG_ASSIGN_OR_RAISE(auto snapshot,
                            current_metadata.SnapshotById(ref->snapshot_id));
    auto max_ref_ags = ref->type() == SnapshotRefType::kBranch
                           ? std::get<SnapshotRef::Branch>(ref->retention).max_ref_age_ms
                           : std::get<SnapshotRef::Tag>(ref->retention).max_ref_age_ms;
    auto max_ref_ags_ms =
        max_ref_ags.has_value() ? max_ref_ags.value() : default_max_ref_age_ms_;
    std::cout << "max_ref_ags_ms:" << max_ref_ags_ms << std::endl;
    if (snapshot != nullptr) {
      auto ref_age_ms = (current_time_ms_ - snapshot->timestamp_ms).count();
      std::cout << "ref_age_ms:" << max_ref_ags_ms << std::endl;
      if (ref_age_ms <= max_ref_ags_ms) {
        retained_refs[key] = ref;
      }
    } else {
      // Invalid reference, remove it
    }
  }
  for (const auto& [key, ref] : retained_refs) {
    retained_id_to_ref[ref->snapshot_id].emplace_back(key);
    retained_snapshot_ids.insert(ref->snapshot_id);
    std::cout << "retained_snapshot_ids:" << ref->snapshot_id << std::endl;
  }

  for (auto id : snapshot_ids_to_expire_) {
    if (retained_id_to_ref.contains(id)) {
      return Invalid("Cannot expire {}. Still referenced by refs.", id);
    }
  }
  ICEBERG_ASSIGN_OR_RAISE(
      auto all_branch_snapshot_ids,
      ComputeAllBranchSnapshotIds(*transaction_->table(), retained_refs));

  ICEBERG_ASSIGN_OR_RAISE(
      auto unreferenced_snapshot_ids,
      UnreferencedSnapshotIds(*transaction_->table(), current_metadata, retained_refs));

  retained_snapshot_ids.insert(all_branch_snapshot_ids.begin(),
                               all_branch_snapshot_ids.end());
  retained_snapshot_ids.insert(unreferenced_snapshot_ids.begin(),
                               unreferenced_snapshot_ids.end());

  std::vector<int32_t> specs_to_remove;
  std::unordered_set<int32_t> schemas_to_remove;
  if (clean_expired_metadata_) {
    // TODO(xiao.dong) parallel processing
    std::unordered_set<int32_t> reachable_specs;
    std::unordered_set<int32_t> reachable_schemas;
    reachable_specs.insert(current_metadata.default_spec_id);
    reachable_schemas.insert(current_metadata.current_schema_id);

    for (const auto& snapshot_id : retained_snapshot_ids) {
      ICEBERG_ASSIGN_OR_RAISE(auto snapshot, current_metadata.SnapshotById(snapshot_id));
      auto file_io = transaction_->table()->io();
      auto snapshot_cache = std::make_unique<SnapshotCache>(snapshot.get());
      ICEBERG_ASSIGN_OR_RAISE(auto manifest_list, snapshot_cache->Manifests(file_io));
      for (const auto& manifest : manifest_list) {
        reachable_specs.insert(manifest.partition_spec_id);
      }
      if (snapshot->schema_id.has_value()) {
        reachable_schemas.insert(snapshot->schema_id.value());
      }
    }

    for (const auto& spec : current_metadata.partition_specs) {
      if (!reachable_specs.contains(spec->spec_id())) {
        specs_to_remove.emplace_back(spec->spec_id());
      }
    }
    for (const auto& schema : current_metadata.schemas) {
      if (!reachable_schemas.contains(schema->schema_id())) {
        schemas_to_remove.insert(schema->schema_id());
      }
    }
  }
  SnapshotToRef refs_to_remove;
  for (const auto& [key, ref] : current_metadata.refs) {
    if (!retained_refs.contains(key)) {
      refs_to_remove[key] = ref;
    }
  }
  for (const auto& snapshot : current_metadata.snapshots) {
    if (!retained_snapshot_ids.contains(snapshot->snapshot_id)) {
      snapshot_ids_to_expire_.emplace_back(snapshot->snapshot_id);
    }
  }
  return ExpireSnapshotsResult{.ref_to_remove = std::move(refs_to_remove),
                               .snapshot_ids_to_remove = snapshot_ids_to_expire_,
                               .partition_spec_ids_to_remove = std::move(specs_to_remove),
                               .schema_ids_to_remove = std::move(schemas_to_remove)};
}

Status ExpireSnapshots::Commit() {
  ICEBERG_RETURN_UNEXPECTED(PendingUpdate::Commit());
  // TODO(xiao.dong) implements of FileCleanupStrategy
  return {};
}

}  // namespace iceberg
