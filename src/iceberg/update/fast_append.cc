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

#include "iceberg/update/fast_append.h"

#include <iterator>
#include <vector>

#include "iceberg/constants.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_util_internal.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"
#include "iceberg/transaction.h"
#include "iceberg/util/error_collector.h"
#include "iceberg/util/macros.h"

namespace iceberg {

Result<std::unique_ptr<FastAppend>> FastAppend::Make(
    std::string table_name, std::shared_ptr<Transaction> transaction) {
  ICEBERG_PRECHECK(!table_name.empty(), "Table name cannot be empty");
  ICEBERG_PRECHECK(transaction != nullptr,
                   "Cannot create FastAppend without a transaction");
  return std::unique_ptr<FastAppend>(
      new FastAppend(std::move(table_name), std::move(transaction)));
}

FastAppend::FastAppend(std::string table_name, std::shared_ptr<Transaction> transaction)
    : SnapshotUpdate(std::move(transaction)), table_name_(std::move(table_name)) {}

FastAppend& FastAppend::AppendFile(const std::shared_ptr<DataFile>& file) {
  ICEBERG_BUILDER_CHECK(file != nullptr, "Invalid data file: null");
  ICEBERG_BUILDER_CHECK(file->partition_spec_id.has_value(),
                        "Data file must have partition spec ID");

  int32_t spec_id = file->partition_spec_id.value();
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto spec, Spec(spec_id));

  auto& data_files = new_data_files_by_spec_[spec_id];
  auto [iter, inserted] = data_files.insert(file);
  if (inserted) {
    has_new_files_ = true;
    ICEBERG_BUILDER_RETURN_IF_ERROR(summary_.AddedFile(*spec, *file));
  }

  return *this;
}

FastAppend& FastAppend::AppendManifest(const ManifestFile& manifest) {
  ICEBERG_BUILDER_CHECK(!manifest.has_existing_files(),
                        "Cannot append manifest with existing files");
  ICEBERG_BUILDER_CHECK(!manifest.has_deleted_files(),
                        "Cannot append manifest with deleted files");
  ICEBERG_BUILDER_CHECK(manifest.added_snapshot_id == kInvalidSnapshotId,
                        "Snapshot id must be assigned during commit");
  ICEBERG_BUILDER_CHECK(manifest.sequence_number == kInvalidSequenceNumber,
                        "Sequence number must be assigned during commit");

  if (can_inherit_snapshot_id() && manifest.added_snapshot_id == kInvalidSnapshotId) {
    summary_.AddedManifest(manifest);
    append_manifests_.push_back(manifest);
  } else {
    // The manifest must be rewritten with this update's snapshot ID
    ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto copied_manifest, CopyManifest(manifest));
    rewritten_append_manifests_.push_back(std::move(copied_manifest));
  }

  return *this;
}

std::string FastAppend::operation() { return DataOperation::kAppend; }

Result<std::vector<ManifestFile>> FastAppend::Apply(
    const TableMetadata& metadata_to_update, const std::shared_ptr<Snapshot>& snapshot) {
  std::vector<ManifestFile> manifests;

  ICEBERG_ASSIGN_OR_RAISE(auto new_written_manifests, WriteNewManifests());
  manifests.reserve(new_written_manifests.size() + append_manifests_.size() +
                    rewritten_append_manifests_.size());
  if (!new_written_manifests.empty()) {
    manifests.insert(manifests.end(),
                     std::make_move_iterator(new_written_manifests.begin()),
                     std::make_move_iterator(new_written_manifests.end()));
  }

  // Transform append manifests and rewritten append manifests with snapshot ID
  int64_t snapshot_id = SnapshotId();
  for (auto& manifest : append_manifests_) {
    manifest.added_snapshot_id = snapshot_id;
  }
  for (auto& manifest : rewritten_append_manifests_) {
    manifest.added_snapshot_id = snapshot_id;
  }
  manifests.insert(manifests.end(), append_manifests_.begin(), append_manifests_.end());
  manifests.insert(manifests.end(), rewritten_append_manifests_.begin(),
                   rewritten_append_manifests_.end());

  // Add all manifests from the snapshot
  if (snapshot != nullptr) {
    auto cached_snapshot = SnapshotCache(snapshot.get());
    ICEBERG_ASSIGN_OR_RAISE(auto snapshot_manifests,
                            cached_snapshot.Manifests(transaction_->table()->io()));
    manifests.insert(manifests.end(), snapshot_manifests.begin(),
                     snapshot_manifests.end());
  }

  return manifests;
}

std::unordered_map<std::string, std::string> FastAppend::Summary() {
  summary_.SetPartitionSummaryLimit(
      base().properties.Get(TableProperties::kWritePartitionSummaryLimit));
  return summary_.Build();
}

void FastAppend::CleanUncommitted(const std::unordered_set<std::string>& committed) {
  // Clean up new manifests that were written but not committed
  if (!new_manifests_.empty()) {
    for (const auto& manifest : new_manifests_) {
      if (!committed.contains(manifest.manifest_path)) {
        std::ignore = DeleteFile(manifest.manifest_path);
      }
    }
    new_manifests_.clear();
  }

  // Clean up only rewritten append manifests as they are always owned by the table
  // Don't clean up append manifests as they are added to the manifest list and are
  // not compacted
  if (!rewritten_append_manifests_.empty()) {
    for (const auto& manifest : rewritten_append_manifests_) {
      if (!committed.contains(manifest.manifest_path)) {
        std::ignore = DeleteFile(manifest.manifest_path);
      }
    }
  }
}

bool FastAppend::CleanupAfterCommit() const {
  // Cleanup after committing is disabled for FastAppend unless there are
  // rewritten_append_manifests_ because:
  // 1.) Appended manifests are never rewritten
  // 2.) Manifests which are written out as part of AppendFile are already cleaned
  //     up between commit attempts in WriteNewManifests
  return !rewritten_append_manifests_.empty();
}

Result<std::shared_ptr<PartitionSpec>> FastAppend::Spec(int32_t spec_id) {
  return base().PartitionSpecById(spec_id);
}

Result<ManifestFile> FastAppend::CopyManifest(const ManifestFile& manifest) {
  const TableMetadata& current = base();
  ICEBERG_ASSIGN_OR_RAISE(auto schema, current.Schema());
  ICEBERG_ASSIGN_OR_RAISE(auto spec,
                          current.PartitionSpecById(manifest.partition_spec_id));

  // Generate a unique manifest path using the transaction's metadata location
  std::string new_manifest_path = ManifestPath();
  int64_t snapshot_id = SnapshotId();

  // Copy the manifest with the new snapshot ID.
  return CopyAppendManifest(manifest, transaction_->table()->io(), schema, spec,
                            snapshot_id, new_manifest_path, current.format_version,
                            &summary_);
}

Result<std::vector<ManifestFile>> FastAppend::WriteNewManifests() {
  // If there are new files and manifests were already written, clean them up
  if (has_new_files_ && !new_manifests_.empty()) {
    for (const auto& manifest : new_manifests_) {
      std::ignore = DeleteFile(manifest.manifest_path);
    }
    new_manifests_.clear();
  }

  // Write new manifests if there are new data files
  if (new_manifests_.empty() && !new_data_files_by_spec_.empty()) {
    for (const auto& [spec_id, data_files] : new_data_files_by_spec_) {
      ICEBERG_ASSIGN_OR_RAISE(auto spec, Spec(spec_id));
      ICEBERG_ASSIGN_OR_RAISE(auto written_manifests,
                              WriteDataManifests(data_files.as_span(), spec));
      new_manifests_.insert(new_manifests_.end(),
                            std::make_move_iterator(written_manifests.begin()),
                            std::make_move_iterator(written_manifests.end()));
    }
    has_new_files_ = false;
  }

  return new_manifests_;
}

}  // namespace iceberg
