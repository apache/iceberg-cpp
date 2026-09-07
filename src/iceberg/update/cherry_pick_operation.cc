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

#include "iceberg/update/cherry_pick_operation.h"

#include <string>
#include <vector>

#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/transaction.h"
#include "iceberg/util/error_collector.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/snapshot_util_internal.h"

namespace iceberg {

namespace {

Result<std::unique_ptr<ManifestReader>> MakeManifestReader(
    const ManifestFile& manifest, const std::shared_ptr<FileIO>& file_io,
    const TableMetadata& metadata) {
  ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());
  TableMetadataCache metadata_cache(&metadata);
  ICEBERG_ASSIGN_OR_RAISE(auto specs_by_id, metadata_cache.GetPartitionSpecsById());
  return ManifestReader::Make(manifest, file_io, std::move(schema), specs_by_id.get());
}

/// \brief Data files added and removed by a snapshot.
struct SnapshotChanges {
  std::vector<std::shared_ptr<DataFile>> added;
  std::vector<std::shared_ptr<DataFile>> removed;
};

/// \brief Read the data files that the given snapshot added and removed.
///
/// Only manifests written by the snapshot carry its own entries, so manifests
/// inherited from earlier snapshots are skipped.
Result<SnapshotChanges> ReadSnapshotChanges(const Snapshot& snapshot,
                                            const std::shared_ptr<FileIO>& file_io,
                                            const TableMetadata& metadata) {
  SnapshotChanges changes;
  SnapshotCache cache(&snapshot);
  ICEBERG_ASSIGN_OR_RAISE(auto manifests, cache.DataManifests(file_io));
  for (const auto& manifest : manifests) {
    if (manifest.added_snapshot_id != snapshot.snapshot_id) {
      continue;
    }
    ICEBERG_ASSIGN_OR_RAISE(auto reader, MakeManifestReader(manifest, file_io, metadata));
    ICEBERG_ASSIGN_OR_RAISE(auto entries, reader->Entries());
    for (const auto& entry : entries) {
      if (!entry.data_file) {
        continue;
      }
      if (entry.status == ManifestStatus::kAdded) {
        changes.added.push_back(entry.data_file);
      } else if (entry.status == ManifestStatus::kDeleted) {
        changes.removed.push_back(entry.data_file);
      }
    }
  }
  return changes;
}

std::string StagedWapId(const Snapshot& snapshot) {
  auto it = snapshot.summary.find(SnapshotSummaryFields::kWAPId);
  return it == snapshot.summary.end() ? std::string() : it->second;
}

std::string PublishedWapId(const Snapshot& snapshot) {
  auto it = snapshot.summary.find(SnapshotSummaryFields::kPublishedWAPId);
  return it == snapshot.summary.end() ? std::string() : it->second;
}

Result<std::vector<int64_t>> CurrentAncestorIds(const TableMetadata& metadata) {
  std::vector<int64_t> ids;
  if (metadata.current_snapshot_id == kInvalidSnapshotId) {
    return ids;
  }
  ICEBERG_ASSIGN_OR_RAISE(
      auto ancestors, SnapshotUtil::AncestorsOf(metadata, metadata.current_snapshot_id));
  ids.reserve(ancestors.size());
  for (const auto& ancestor : ancestors) {
    ids.push_back(ancestor->snapshot_id);
  }
  return ids;
}

/// \brief Fail if the WAP id staged on the picked snapshot was already
/// published, and return that id when the snapshot has one.
Result<std::string> ValidateWapPublish(const TableMetadata& metadata,
                                       int64_t wap_snapshot_id) {
  ICEBERG_ASSIGN_OR_RAISE(auto snapshot, metadata.SnapshotById(wap_snapshot_id));
  std::string wap_id = StagedWapId(*snapshot);
  if (wap_id.empty()) {
    return wap_id;
  }

  ICEBERG_ASSIGN_OR_RAISE(auto ancestor_ids, CurrentAncestorIds(metadata));
  for (int64_t ancestor_id : ancestor_ids) {
    ICEBERG_ASSIGN_OR_RAISE(auto ancestor, metadata.SnapshotById(ancestor_id));
    if (wap_id == StagedWapId(*ancestor) || wap_id == PublishedWapId(*ancestor)) {
      return CommitFailed(
          "Duplicate request to cherry pick wap id that was published already: {}",
          wap_id);
    }
  }
  return wap_id;
}

bool IsReplacePartitions(const Snapshot& snapshot) {
  auto it = snapshot.summary.find(SnapshotSummaryFields::kReplacePartitions);
  return it != snapshot.summary.end() && it->second == "true";
}

}  // namespace

Result<std::unique_ptr<CherryPickOperation>> CherryPickOperation::Make(
    std::string table_name, std::shared_ptr<TransactionContext> ctx) {
  ICEBERG_PRECHECK(!table_name.empty(), "Table name cannot be empty");
  ICEBERG_PRECHECK(ctx != nullptr, "Cannot create CherryPickOperation without a context");
  return std::unique_ptr<CherryPickOperation>(
      new CherryPickOperation(std::move(table_name), std::move(ctx)));
}

CherryPickOperation::CherryPickOperation(std::string table_name,
                                         std::shared_ptr<TransactionContext> ctx)
    : MergingSnapshotUpdate(std::move(table_name), std::move(ctx)) {}

std::string CherryPickOperation::operation() {
  if (cherrypick_snapshot_ == nullptr) {
    return DataOperation::kAppend;
  }
  auto op = cherrypick_snapshot_->Operation();
  return op.has_value() ? std::string(*op) : DataOperation::kAppend;
}

Status CherryPickOperation::ValidateFastForward(const TableMetadata& metadata,
                                                const Snapshot& snapshot) {
  // Java runs the WAP check only for the two pickable operations; any other
  // snapshot reaches a fast-forward without one.
  const auto operation = snapshot.Operation();
  const bool is_pickable =
      operation == DataOperation::kAppend ||
      (operation == DataOperation::kOverwrite && IsReplacePartitions(snapshot));
  if (!is_pickable) {
    return {};
  }

  ICEBERG_ASSIGN_OR_RAISE(std::ignore,
                          ValidateWapPublish(metadata, snapshot.snapshot_id));
  return {};
}

bool CherryPickOperation::IsFastForward() const {
  return cherrypick_snapshot_ != nullptr &&
         SnapshotUtil::CanFastForward(ctx_->current(), *cherrypick_snapshot_);
}

CherryPickOperation& CherryPickOperation::Cherrypick(int64_t snapshot_id) {
  const TableMetadata& metadata = ctx_->current();
  ICEBERG_BUILDER_ASSIGN_OR_RETURN_WITH_ERROR(
      cherrypick_snapshot_, metadata.SnapshotById(snapshot_id),
      "Cannot cherry-pick unknown snapshot ID: {}", snapshot_id);

  const auto picked_operation = cherrypick_snapshot_->Operation();
  const bool is_append = picked_operation == DataOperation::kAppend;
  const bool is_dynamic_overwrite = picked_operation == DataOperation::kOverwrite &&
                                    IsReplacePartitions(*cherrypick_snapshot_);

  if (!is_append && !is_dynamic_overwrite) {
    ICEBERG_BUILDER_CHECK(
        IsFastForward(),
        "Cannot cherry-pick snapshot {}: not append, dynamic overwrite, or fast-forward",
        snapshot_id);
    return *this;
  }

  if (is_dynamic_overwrite) {
    // The replaced partitions can only be checked against files added since the
    // picked snapshot's parent, so that parent must still be in the history.
    if (cherrypick_snapshot_->parent_snapshot_id.has_value()) {
      ICEBERG_BUILDER_ASSIGN_OR_RETURN(
          bool is_ancestor,
          SnapshotUtil::IsAncestorOf(metadata,
                                     cherrypick_snapshot_->parent_snapshot_id.value()));
      ICEBERG_BUILDER_CHECK(is_ancestor,
                            "Cannot cherry-pick overwrite not based on an ancestor of "
                            "the current state: {}",
                            snapshot_id);
    }
  }

  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto wap_id,
                                   ValidateWapPublish(metadata, snapshot_id));
  if (!wap_id.empty()) {
    Set(SnapshotSummaryFields::kPublishedWAPId, wap_id);
  }
  Set(SnapshotSummaryFields::kSourceSnapshotId, std::to_string(snapshot_id));

  auto io = ctx_->table->io();
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(
      auto changes, ReadSnapshotChanges(*cherrypick_snapshot_, io, metadata));

  if (is_dynamic_overwrite) {
    // A replace can only be re-applied if the files it removed are all present.
    FailMissingDeletePaths();
    replaced_partitions_.emplace();
  }

  for (const auto& added : changes.added) {
    ICEBERG_BUILDER_RETURN_IF_ERROR(AddDataFile(added));
    if (replaced_partitions_.has_value()) {
      ICEBERG_BUILDER_CHECK(added->partition_spec_id.has_value(),
                            "Data file must have partition spec ID");
      replaced_partitions_->add(added->partition_spec_id.value(), added->partition);
    }
  }

  if (is_dynamic_overwrite) {
    for (const auto& removed : changes.removed) {
      ICEBERG_BUILDER_RETURN_IF_ERROR(DeleteDataFile(removed));
    }
  }

  return *this;
}

Status CherryPickOperation::ValidateNonAncestor(const TableMetadata& metadata,
                                                int64_t snapshot_id) const {
  ICEBERG_ASSIGN_OR_RAISE(bool is_ancestor,
                          SnapshotUtil::IsAncestorOf(metadata, snapshot_id));
  if (is_ancestor) {
    return CommitFailed("Cannot cherrypick snapshot {}: already an ancestor",
                        snapshot_id);
  }

  const std::string snapshot_id_str = std::to_string(snapshot_id);
  ICEBERG_ASSIGN_OR_RAISE(auto ancestor_ids, CurrentAncestorIds(metadata));
  for (int64_t ancestor_id : ancestor_ids) {
    ICEBERG_ASSIGN_OR_RAISE(auto ancestor, metadata.SnapshotById(ancestor_id));
    auto it = ancestor->summary.find(SnapshotSummaryFields::kSourceSnapshotId);
    if (it != ancestor->summary.end() && it->second == snapshot_id_str) {
      return CommitFailed(
          "Cannot cherrypick snapshot {}: already picked to create ancestor {}",
          snapshot_id, ancestor_id);
    }
  }
  return {};
}

Status CherryPickOperation::ValidateReplacedPartitions(
    const TableMetadata& metadata) const {
  if (!replaced_partitions_.has_value() ||
      metadata.current_snapshot_id == kInvalidSnapshotId) {
    return {};
  }

  const auto parent_id = cherrypick_snapshot_->parent_snapshot_id;
  if (parent_id.has_value()) {
    ICEBERG_ASSIGN_OR_RAISE(bool is_ancestor,
                            SnapshotUtil::IsAncestorOf(metadata, parent_id.value()));
    if (!is_ancestor) {
      return ValidationFailed(
          "Cannot cherry-pick overwrite, based on non-ancestor of the current state: {}",
          parent_id.value());
    }
  }

  // Walk back from the current snapshot to the picked snapshot's parent and
  // reject any file added into a partition this pick replaces.
  auto io = ctx_->table->io();
  ICEBERG_ASSIGN_OR_RAISE(
      auto ancestors, SnapshotUtil::AncestorsOf(metadata, metadata.current_snapshot_id));
  for (const auto& ancestor : ancestors) {
    if (parent_id.has_value() && ancestor->snapshot_id == parent_id.value()) {
      break;
    }
    ICEBERG_ASSIGN_OR_RAISE(auto changes, ReadSnapshotChanges(*ancestor, io, metadata));
    for (const auto& added : changes.added) {
      if (!added->partition_spec_id.has_value() ||
          !replaced_partitions_->contains(added->partition_spec_id.value(),
                                          added->partition)) {
        continue;
      }
      ICEBERG_ASSIGN_OR_RAISE(auto spec,
                              metadata.PartitionSpecById(*added->partition_spec_id));
      ICEBERG_ASSIGN_OR_RAISE(auto partition_path, spec->PartitionPath(added->partition));
      return ValidationFailed(
          "Cannot cherry-pick replace partitions with changed partition: {}",
          partition_path);
    }
  }
  return {};
}

Status CherryPickOperation::Validate(const TableMetadata& current_metadata,
                                     const std::shared_ptr<Snapshot>& snapshot) {
  if (cherrypick_snapshot_ == nullptr || IsFastForward()) {
    return {};
  }

  ICEBERG_RETURN_UNEXPECTED(
      ValidateNonAncestor(current_metadata, cherrypick_snapshot_->snapshot_id));
  ICEBERG_RETURN_UNEXPECTED(ValidateReplacedPartitions(current_metadata));
  ICEBERG_ASSIGN_OR_RAISE(
      std::ignore,
      ValidateWapPublish(current_metadata, cherrypick_snapshot_->snapshot_id));
  return {};
}

}  // namespace iceberg
