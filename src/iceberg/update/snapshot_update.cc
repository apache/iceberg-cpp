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

#include "iceberg/update/snapshot_update.h"

#include <charconv>
#include <chrono>
#include <format>

#include "iceberg/manifest/manifest_writer.h"
#include "iceberg/table.h"
#include "iceberg/table_requirements.h"
#include "iceberg/table_update.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/snapshot_util_internal.h"

namespace iceberg {

template <typename T>
SnapshotUpdate<T>::SnapshotUpdate(std::shared_ptr<Transaction> transaction)
    : PendingUpdate(std::move(transaction)) {
  // Read target manifest size bytes
  target_manifest_size_bytes_ =
      transaction_->table()->properties().Get(TableProperties::kManifestTargetSizeBytes);

  // For format version 1, check if snapshot ID inheritance is enabled
  if (transaction_->table()->metadata()->format_version == 1) {
    can_inherit_snapshot_id_ = transaction_->table()->properties().Get(
        TableProperties::kSnapshotIdInheritanceEnabled);
  }
}

template <typename T>
std::vector<ManifestFile> SnapshotUpdate<T>::WriteDataManifests(
    const std::vector<DataFile>& data_files) {
  // TODO(any): Implement
  return {};
}

template <typename T>
std::vector<ManifestFile> SnapshotUpdate<T>::WriteDeleteManifests(
    const std::vector<DataFile>& delete_files) {
  // TODO(any): Implement
  return {};
}

template <typename T>
Result<PendingUpdate::ApplyResult> SnapshotUpdate<T>::Apply() {
  // Get the latest snapshot for the target branch
  std::shared_ptr<Snapshot> parent_snapshot;
  if (auto ref_it = transaction_->table()->metadata()->refs.find(target_branch_);
      ref_it != transaction_->table()->metadata()->refs.end()) {
    ICEBERG_ASSIGN_OR_RAISE(parent_snapshot, transaction_->table()->SnapshotById(
                                                 ref_it->second->snapshot_id));
  } else {
    auto current_snapshot_result = transaction_->table()->current_snapshot();

    ICEBERG_ASSIGN_OR_RAISE(parent_snapshot, transaction_->table()->current_snapshot());
  }

  // Generate snapshot ID
  int64_t new_snapshot_id = SnapshotUtil::GenerateSnapshotId(*transaction_->table());
  std::optional<int64_t> parent_snapshot_id =
      parent_snapshot ? std::make_optional(parent_snapshot->snapshot_id) : std::nullopt;

  // Get sequence number
  int64_t sequence_number = transaction_->table()->metadata()->last_sequence_number + 1;

  // Write manifest list
  std::string manifest_list_path = ManifestListPath();
  manifest_lists_.push_back(manifest_list_path);

  ICEBERG_ASSIGN_OR_RAISE(
      auto writer,
      ManifestListWriter::Make(transaction_->table()->metadata()->format_version,
                               new_snapshot_id, parent_snapshot_id, manifest_list_path,
                               transaction_->table()->io(), sequence_number,
                               transaction_->table()->metadata()->next_row_id));

  ICEBERG_RETURN_UNEXPECTED(
      writer->AddAll(Apply(transaction_->table()->metadata(), parent_snapshot)));
  ICEBERG_RETURN_UNEXPECTED(writer->Close());

  // Compute summary
  std::unordered_map<std::string, std::string> summary =
      ComputeSummary(transaction_->table()->metadata());

  // Get current time
  auto now = std::chrono::system_clock::now();
  auto duration_since_epoch = now.time_since_epoch();
  TimePointMs timestamp_ms = std::chrono::time_point_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::time_point(duration_since_epoch));

  // Get schema ID
  std::optional<int32_t> schema_id = transaction_->table()->metadata()->current_schema_id;

  // Create snapshot
  staged_snapshot_ =
      std::make_shared<Snapshot>(Snapshot{.snapshot_id = new_snapshot_id,
                                          .parent_snapshot_id = parent_snapshot_id,
                                          .sequence_number = sequence_number,
                                          .timestamp_ms = timestamp_ms,
                                          .manifest_list = manifest_list_path,
                                          .summary = std::move(summary),
                                          .schema_id = schema_id});

  // Build metadata update
  auto builder = TableMetadataBuilder::BuildFrom(transaction_->table()->metadata().get());

  // Check if this is a rollback (snapshot already exists)
  auto existing_snapshot_result =
      transaction_->table()->metadata()->SnapshotById(staged_snapshot_->snapshot_id);
  if (existing_snapshot_result.has_value()) {
    // Rollback operation
    builder->SetBranchSnapshot(staged_snapshot_->snapshot_id, target_branch_);
  } else if (stage_only_) {
    // Stage only - add snapshot but don't set as current
    builder->AddSnapshot(staged_snapshot_);
  } else {
    // Normal commit - set as branch snapshot
    builder->SetBranchSnapshot(staged_snapshot_->snapshot_id, target_branch_);
  }

  // Build updated metadata
  ICEBERG_ASSIGN_OR_RAISE(auto updated_metadata, builder->Build());

  // Check if metadata has changed
  if (*updated_metadata == *transaction_->table()->metadata()) {
    // No changes, commit successful
    return ApplyResult{};
  }

  // Ensure UUID is set
  if (updated_metadata->table_uuid.empty()) {
    builder->AssignUUID();
    ICEBERG_ASSIGN_OR_RAISE(updated_metadata, builder->Build());
  }

  // Create table updates
  std::vector<std::unique_ptr<TableUpdate>> updates;
  updates.push_back(std::make_unique<table::AddSnapshot>(staged_snapshot_));
  if (!stage_only_) {
    // Set branch snapshot using SetSnapshotRef
    updates.push_back(std::make_unique<table::SetSnapshotRef>(
        target_branch_, staged_snapshot_->snapshot_id, SnapshotRefType::kBranch));
  }

  // Create requirements
  auto requirements_result =
      TableRequirements::ForUpdateTable(*transaction_->table()->metadata(), updates);
  ICEBERG_ASSIGN_OR_RAISE(auto requirements, std::move(requirements_result));

  return ApplyResult{std::move(updates)};
}

template <typename T>
Status SnapshotUpdate<T>::Commit() {
  ICEBERG_ASSIGN_OR_RAISE(auto apply_result, Apply());

  auto status = transaction_->Apply(std::move(apply_result.updates));

  // Cleanup after successful commit
  if (status.has_value() && staged_snapshot_ && cleanup_after_commit()) {
    CleanUncommitted(committed_manifest_paths_);
    // Clean up unused manifest lists
    for (const auto& manifest_list : manifest_lists_) {
      if (manifest_list != staged_snapshot_->manifest_list) {
        DeleteFile(manifest_list);
      }
    }
  }

  return status;
}

template <typename T>
void SnapshotUpdate<T>::SetTargetBranch(const std::string& branch) {
  if (branch.empty()) {
    AddError(ErrorKind::kInvalidArgument, "Invalid branch name: empty");
    return;
  }

  auto ref_it = transaction_->table()->metadata()->refs.find(branch);
  if (ref_it != transaction_->table()->metadata()->refs.end()) {
    if (ref_it->second->type() != SnapshotRefType::kBranch) {
      AddError(
          ErrorKind::kInvalidArgument,
          "{} is a tag, not a branch. Tags cannot be targets for producing snapshots",
          branch);
      return;
    }
  }

  target_branch_ = branch;
}

template <typename T>
std::unordered_map<std::string, std::string> SnapshotUpdate<T>::ComputeSummary(
    const std::shared_ptr<TableMetadata>& previous) {
  std::unordered_map<std::string, std::string> summary = Summary();

  auto op = operation();
  if (!op.empty()) {
    summary[SnapshotSummaryFields::kOperation] = op;
  }

  // Get previous summary
  std::unordered_map<std::string, std::string> previous_summary;
  if (auto ref_it = previous->refs.find(target_branch_); ref_it != previous->refs.end()) {
    auto snapshot_result = previous->SnapshotById(ref_it->second->snapshot_id);
    if (snapshot_result.has_value() && (*snapshot_result)->summary.size() > 0) {
      previous_summary = (*snapshot_result)->summary;
    }
  }

  // If no previous summary, initialize with zeros
  if (previous_summary.empty()) {
    previous_summary[SnapshotSummaryFields::kTotalRecords] = "0";
    previous_summary[SnapshotSummaryFields::kTotalFileSize] = "0";
    previous_summary[SnapshotSummaryFields::kTotalDataFiles] = "0";
    previous_summary[SnapshotSummaryFields::kTotalDeleteFiles] = "0";
    previous_summary[SnapshotSummaryFields::kTotalPosDeletes] = "0";
    previous_summary[SnapshotSummaryFields::kTotalEqDeletes] = "0";
  }

  // Copy all summary properties from the implementation
  for (const auto& [key, value] : summary_properties_) {
    summary[key] = value;
  }

  // Update totals
  UpdateTotal(summary, previous_summary, SnapshotSummaryFields::kTotalRecords,
              SnapshotSummaryFields::kAddedRecords,
              SnapshotSummaryFields::kDeletedRecords);
  UpdateTotal(summary, previous_summary, SnapshotSummaryFields::kTotalFileSize,
              SnapshotSummaryFields::kAddedFileSize,
              SnapshotSummaryFields::kRemovedFileSize);
  UpdateTotal(summary, previous_summary, SnapshotSummaryFields::kTotalDataFiles,
              SnapshotSummaryFields::kAddedDataFiles,
              SnapshotSummaryFields::kDeletedDataFiles);
  UpdateTotal(summary, previous_summary, SnapshotSummaryFields::kTotalDeleteFiles,
              SnapshotSummaryFields::kAddedDeleteFiles,
              SnapshotSummaryFields::kRemovedDeleteFiles);
  UpdateTotal(summary, previous_summary, SnapshotSummaryFields::kTotalPosDeletes,
              SnapshotSummaryFields::kAddedPosDeletes,
              SnapshotSummaryFields::kRemovedPosDeletes);
  UpdateTotal(summary, previous_summary, SnapshotSummaryFields::kTotalEqDeletes,
              SnapshotSummaryFields::kAddedEqDeletes,
              SnapshotSummaryFields::kRemovedEqDeletes);

  return summary;
}

template <typename T>
void SnapshotUpdate<T>::CleanAll() {
  for (const auto& manifest_list : manifest_lists_) {
    DeleteFile(manifest_list);
  }
  manifest_lists_.clear();
  CleanUncommitted(committed_manifest_paths_);
  committed_manifest_paths_.clear();
}

template <typename T>
Status SnapshotUpdate<T>::DeleteFile(const std::string& path) {
  return delete_func_(path);
}

template <typename T>
std::string SnapshotUpdate<T>::ManifestListPath() {
  // Generate manifest list path
  // Format: {metadata_location}/snap-{snapshot_id}-{attempt}-{uuid}.avro
  std::string filename = std::format(
      "snap-{}-{}-{}.avro", SnapshotUtil::GenerateSnapshotId(*transaction_->table()),
      attempt_.fetch_add(1) + 1, commit_uuid_);
  return std::format("{}/metadata/{}", transaction_->table()->location(), filename);
}

template <typename T>
void SnapshotUpdate<T>::UpdateTotal(
    std::unordered_map<std::string, std::string>& summary,
    const std::unordered_map<std::string, std::string>& previous_summary,
    const std::string& total_property, const std::string& added_property,
    const std::string& deleted_property) {
  auto total_it = previous_summary.find(total_property);
  if (total_it != previous_summary.end()) {
    int64_t new_total;
    auto [_, ec] =
        std::from_chars(total_it->second.data(),
                        total_it->second.data() + total_it->second.size(), new_total);
    if (ec != std::errc()) [[unlikely]] {
      // Ignore and do not add total
      return;
    }

    auto added_it = summary.find(added_property);
    if (new_total >= 0 && added_it != summary.end()) {
      int64_t added_value;
      auto [_, ec] =
          std::from_chars(added_it->second.data(),
                          added_it->second.data() + added_it->second.size(), added_value);
      if (ec == std::errc()) [[unlikely]] {
        new_total += added_value;
      }
    }

    auto deleted_it = summary.find(deleted_property);
    if (new_total >= 0 && deleted_it != summary.end()) {
      int64_t deleted_value;
      auto [_, ec] = std::from_chars(
          deleted_it->second.data(),
          deleted_it->second.data() + deleted_it->second.size(), deleted_value);
      if (ec == std::errc()) [[unlikely]] {
        new_total -= deleted_value;
      }
    }

    if (new_total >= 0) {
      summary[total_property] = std::to_string(new_total);
    }
  }
}

}  // namespace iceberg
