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

#include <format>
#include <ranges>

#include "iceberg/constants.h"
#include "iceberg/file_io.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/manifest/manifest_writer.h"
#include "iceberg/manifest/rolling_manifest_writer.h"
#include "iceberg/partition_summary_internal.h"
#include "iceberg/table.h"
#include "iceberg/transaction.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/snapshot_util_internal.h"
#include "iceberg/util/string_util.h"
#include "iceberg/util/uuid.h"

namespace iceberg {

namespace {

// The Java impl skips updating total if parsing fails. Here we choose to be strict.
Status UpdateTotal(std::unordered_map<std::string, std::string>& summary,
                   const std::unordered_map<std::string, std::string>& previous_summary,
                   const std::string& total_property, const std::string& added_property,
                   const std::string& deleted_property) {
  auto total_it = previous_summary.find(total_property);
  if (total_it != previous_summary.end()) {
    ICEBERG_ASSIGN_OR_RAISE(auto new_total,
                            StringUtils::ParseNumber<int64_t>(total_it->second));

    auto added_it = summary.find(added_property);
    if (new_total >= 0 && added_it != summary.end()) {
      ICEBERG_ASSIGN_OR_RAISE(auto added_value,
                              StringUtils::ParseNumber<int64_t>(added_it->second));
      new_total += added_value;
    }

    auto deleted_it = summary.find(deleted_property);
    if (new_total >= 0 && deleted_it != summary.end()) {
      ICEBERG_ASSIGN_OR_RAISE(auto deleted_value,
                              StringUtils::ParseNumber<int64_t>(deleted_it->second));
      new_total -= deleted_value;
    }

    if (new_total >= 0) {
      summary[total_property] = std::to_string(new_total);
    }
  }
  return {};
}

// Add metadata to a manifest file by reading it and extracting statistics.
Result<ManifestFile> AddMetadata(const ManifestFile& manifest, std::shared_ptr<FileIO> io,
                                 const TableMetadata& metadata) {
  ICEBERG_PRECHECK(manifest.added_snapshot_id != kInvalidSnapshotId,
                   "Manifest {} already has assigned a snapshot id: {}",
                   manifest.manifest_path, manifest.added_snapshot_id);

  ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());
  ICEBERG_ASSIGN_OR_RAISE(auto spec,
                          metadata.PartitionSpecById(manifest.partition_spec_id));
  ICEBERG_ASSIGN_OR_RAISE(auto partition_type, spec->PartitionType(*schema));

  ICEBERG_ASSIGN_OR_RAISE(auto reader,
                          ManifestReader::Make(manifest, std::move(io), schema, spec));
  ICEBERG_ASSIGN_OR_RAISE(auto entries, reader->Entries());

  PartitionSummary stats(*partition_type);
  int32_t added_files = 0;
  int64_t added_rows = 0;
  int32_t existing_files = 0;
  int64_t existing_rows = 0;
  int32_t deleted_files = 0;
  int64_t deleted_rows = 0;

  std::optional<int64_t> snapshot_id;
  int64_t max_snapshot_id = std::numeric_limits<int64_t>::min();
  for (const auto& entry : entries) {
    ICEBERG_PRECHECK(entry.data_file != nullptr,
                     "Manifest entry in {} is missing data_file", manifest.manifest_path);

    if (entry.snapshot_id.has_value() && entry.snapshot_id.value() > max_snapshot_id) {
      max_snapshot_id = entry.snapshot_id.value();
    }

    switch (entry.status) {
      case ManifestStatus::kAdded: {
        added_files += 1;
        added_rows += entry.data_file->record_count;
        if (!snapshot_id.has_value() && entry.snapshot_id.has_value()) {
          snapshot_id = entry.snapshot_id;
        }
      } break;
      case ManifestStatus::kExisting: {
        existing_files += 1;
        existing_rows += entry.data_file->record_count;
      } break;
      case ManifestStatus::kDeleted: {
        deleted_files += 1;
        deleted_rows += entry.data_file->record_count;
        if (!snapshot_id.has_value() && entry.snapshot_id.has_value()) {
          snapshot_id = entry.snapshot_id;
        }
      } break;
    }

    ICEBERG_RETURN_UNEXPECTED(stats.Update(entry.data_file->partition));
  }

  if (!snapshot_id.has_value()) {
    // If no files were added or deleted, use the largest snapshot ID in the manifest
    snapshot_id = max_snapshot_id;
  }

  ICEBERG_ASSIGN_OR_RAISE(auto partition_summaries, stats.Summaries());

  ManifestFile enriched = manifest;
  enriched.added_snapshot_id = snapshot_id.value();
  enriched.added_files_count = added_files;
  enriched.existing_files_count = existing_files;
  enriched.deleted_files_count = deleted_files;
  enriched.added_rows_count = added_rows;
  enriched.existing_rows_count = existing_rows;
  enriched.deleted_rows_count = deleted_rows;
  enriched.partitions = std::move(partition_summaries);
  enriched.first_row_id = std::nullopt;
  return enriched;
}

}  // anonymous namespace

SnapshotUpdate::~SnapshotUpdate() = default;

SnapshotUpdate::SnapshotUpdate(std::shared_ptr<Transaction> transaction)
    : PendingUpdate(std::move(transaction)),
      can_inherit_snapshot_id_(
          base().format_version > 1 ||
          base().properties.Get(TableProperties::kSnapshotIdInheritanceEnabled)),
      commit_uuid_(Uuid::GenerateV7().ToString()),
      target_manifest_size_bytes_(
          base().properties.Get(TableProperties::kManifestTargetSizeBytes)) {}

// TODO(xxx): write manifests in parallel
Result<std::vector<ManifestFile>> SnapshotUpdate::WriteDataManifests(
    std::span<const std::shared_ptr<DataFile>> files,
    const std::shared_ptr<PartitionSpec>& spec,
    std::optional<int64_t> data_sequence_number) {
  if (files.empty()) {
    return std::vector<ManifestFile>{};
  }

  ICEBERG_ASSIGN_OR_RAISE(auto current_schema, base().Schema());
  RollingManifestWriter rolling_writer(
      [this, spec, schema = std::move(current_schema),
       snapshot_id = SnapshotId()]() -> Result<std::unique_ptr<ManifestWriter>> {
        return ManifestWriter::MakeWriter(base().format_version, snapshot_id,
                                          ManifestPath(), transaction_->table()->io(),
                                          std::move(spec), std::move(schema),
                                          ManifestContent::kData,
                                          /*first_row_id=*/base().next_row_id);
      },
      target_manifest_size_bytes_);

  for (const auto& file : files) {
    ICEBERG_RETURN_UNEXPECTED(rolling_writer.WriteAddedEntry(file, data_sequence_number));
  }
  ICEBERG_RETURN_UNEXPECTED(rolling_writer.Close());
  return rolling_writer.ToManifestFiles();
}

// TODO(xxx): write manifests in parallel
Result<std::vector<ManifestFile>> SnapshotUpdate::WriteDeleteManifests(
    std::span<const std::shared_ptr<DataFile>> files,
    const std::shared_ptr<PartitionSpec>& spec) {
  if (files.empty()) {
    return std::vector<ManifestFile>{};
  }

  ICEBERG_ASSIGN_OR_RAISE(auto current_schema, base().Schema());
  RollingManifestWriter rolling_writer(
      [this, spec, schema = std::move(current_schema),
       snapshot_id = SnapshotId()]() -> Result<std::unique_ptr<ManifestWriter>> {
        return ManifestWriter::MakeWriter(base().format_version, snapshot_id,
                                          ManifestPath(), transaction_->table()->io(),
                                          std::move(spec), std::move(schema),
                                          ManifestContent::kDeletes);
      },
      target_manifest_size_bytes_);

  for (const auto& file : files) {
    // FIXME: Java impl wrap it with `PendingDeleteFile` and deals with
    // file->data_sequence_number
    ICEBERG_RETURN_UNEXPECTED(rolling_writer.WriteAddedEntry(file));
  }
  ICEBERG_RETURN_UNEXPECTED(rolling_writer.Close());
  return rolling_writer.ToManifestFiles();
}

int64_t SnapshotUpdate::SnapshotId() {
  if (!snapshot_id_.has_value()) {
    snapshot_id_ = SnapshotUtil::GenerateSnapshotId(base());
  }
  return snapshot_id_.value();
}

Result<SnapshotUpdate::ApplyResult> SnapshotUpdate::Apply() {
  ICEBERG_RETURN_UNEXPECTED(CheckErrors());
  ICEBERG_ASSIGN_OR_RAISE(auto parent_snapshot,
                          SnapshotUtil::OptionalLatestSnapshot(base(), target_branch_));

  int64_t sequence_number = base().NextSequenceNumber();
  std::optional<int64_t> parent_snapshot_id =
      parent_snapshot ? std::make_optional(parent_snapshot->snapshot_id) : std::nullopt;

  if (parent_snapshot) {
    ICEBERG_RETURN_UNEXPECTED(Validate(base(), parent_snapshot));
  }

  ICEBERG_ASSIGN_OR_RAISE(auto manifests, Apply(base(), parent_snapshot));
  for (auto& manifest : manifests) {
    if (manifest.added_snapshot_id != kInvalidSnapshotId) {
      continue;
    }
    // TODO(xxx): read in parallel and cache enriched manifests for retries
    ICEBERG_ASSIGN_OR_RAISE(manifest,
                            AddMetadata(manifest, transaction_->table()->io(), base()));
  }

  std::string manifest_list_path = ManifestListPath();
  manifest_lists_.push_back(manifest_list_path);
  ICEBERG_ASSIGN_OR_RAISE(
      auto writer, ManifestListWriter::MakeWriter(base().format_version, SnapshotId(),
                                                  parent_snapshot_id, manifest_list_path,
                                                  transaction_->table()->io(),
                                                  sequence_number, base().next_row_id));
  ICEBERG_RETURN_UNEXPECTED(writer->AddAll(manifests));
  ICEBERG_RETURN_UNEXPECTED(writer->Close());

  std::optional<int64_t> next_row_id;
  std::optional<int64_t> assigned_rows;
  if (base().format_version >= 3) {
    ICEBERG_CHECK(writer->next_row_id().has_value(),
                  "row id is required by format version >= 3");
    next_row_id = base().next_row_id;
    assigned_rows = writer->next_row_id().value() - base().next_row_id;
  }

  std::string op = operation();
  ICEBERG_CHECK(!op.empty(), "Snapshot operation cannot be empty");

  if (op == DataOperation::kReplace) {
    const auto summary = Summary();
    auto added_records_it = summary.find(SnapshotSummaryFields::kAddedRecords);
    auto replaced_records_it = summary.find(SnapshotSummaryFields::kDeletedRecords);
    if (added_records_it != summary.cend() && replaced_records_it != summary.cend()) {
      ICEBERG_ASSIGN_OR_RAISE(auto added_records, StringUtils::ParseNumber<int64_t>(
                                                      added_records_it->second));
      ICEBERG_ASSIGN_OR_RAISE(auto replaced_records, StringUtils::ParseNumber<int64_t>(
                                                         replaced_records_it->second));
      ICEBERG_PRECHECK(
          added_records <= replaced_records,
          "Invalid REPLACE operation: {} added records > {} replaced records",
          added_records, replaced_records);
    }
  }

  ICEBERG_ASSIGN_OR_RAISE(auto summary, ComputeSummary(base()));
  ICEBERG_ASSIGN_OR_RAISE(
      staged_snapshot_,
      Snapshot::Make(sequence_number, SnapshotId(), parent_snapshot_id,
                     CurrentTimePointMs(), std::move(op), std::move(summary),
                     base().current_schema_id, std::move(manifest_list_path), next_row_id,
                     assigned_rows));

  return ApplyResult{.snapshot = staged_snapshot_,
                     .target_branch = target_branch_,
                     .stage_only = stage_only_};
}

Status SnapshotUpdate::Finalize(std::optional<Error> commit_error) {
  if (commit_error.has_value()) {
    if (commit_error->kind == ErrorKind::kCommitStateUnknown) {
      return {};
    }
    CleanAll();
    return {};
  }

  if (CleanupAfterCommit()) {
    ICEBERG_CHECK(staged_snapshot_ != nullptr,
                  "Staged snapshot is null during finalize after commit");
    auto cached_snapshot = SnapshotCache(staged_snapshot_.get());
    ICEBERG_ASSIGN_OR_RAISE(auto manifests,
                            cached_snapshot.Manifests(transaction_->table()->io()));
    CleanUncommitted(manifests | std::views::transform([](const auto& manifest) {
                       return manifest.manifest_path;
                     }) |
                     std::ranges::to<std::unordered_set<std::string>>());
  }

  // Also clean up unused manifest lists created by multiple attempts
  for (const auto& manifest_list : manifest_lists_) {
    if (manifest_list != staged_snapshot_->manifest_list) {
      std::ignore = DeleteFile(manifest_list);
    }
  }

  return {};
}

Status SnapshotUpdate::SetTargetBranch(const std::string& branch) {
  ICEBERG_PRECHECK(!branch.empty(), "Branch name cannot be empty");

  if (auto ref_it = base().refs.find(branch); ref_it != base().refs.end()) {
    ICEBERG_PRECHECK(
        ref_it->second->type() == SnapshotRefType::kBranch,
        "{} is a tag, not a branch. Tags cannot be targets for producing snapshots",
        branch);
  }

  target_branch_ = branch;
  return {};
}

Result<std::unordered_map<std::string, std::string>> SnapshotUpdate::ComputeSummary(
    const TableMetadata& previous) {
  std::unordered_map<std::string, std::string> summary = Summary();
  if (summary.empty()) {
    return summary;
  }

  // Get previous summary from the target branch
  std::unordered_map<std::string, std::string> previous_summary;
  if (auto ref_it = previous.refs.find(target_branch_); ref_it != previous.refs.end()) {
    if (auto snap_it = previous.SnapshotById(ref_it->second->snapshot_id);
        snap_it.has_value()) {
      previous_summary = snap_it.value()->summary;
    }
  } else {
    // if there was no previous snapshot, default the summary to start totals at 0
    previous_summary[SnapshotSummaryFields::kTotalRecords] = "0";
    previous_summary[SnapshotSummaryFields::kTotalFileSize] = "0";
    previous_summary[SnapshotSummaryFields::kTotalDataFiles] = "0";
    previous_summary[SnapshotSummaryFields::kTotalDeleteFiles] = "0";
    previous_summary[SnapshotSummaryFields::kTotalPosDeletes] = "0";
    previous_summary[SnapshotSummaryFields::kTotalEqDeletes] = "0";
  }

  // Update totals
  ICEBERG_RETURN_UNEXPECTED(UpdateTotal(
      summary, previous_summary, SnapshotSummaryFields::kTotalRecords,
      SnapshotSummaryFields::kAddedRecords, SnapshotSummaryFields::kDeletedRecords));
  ICEBERG_RETURN_UNEXPECTED(UpdateTotal(
      summary, previous_summary, SnapshotSummaryFields::kTotalFileSize,
      SnapshotSummaryFields::kAddedFileSize, SnapshotSummaryFields::kRemovedFileSize));
  ICEBERG_RETURN_UNEXPECTED(UpdateTotal(
      summary, previous_summary, SnapshotSummaryFields::kTotalDataFiles,
      SnapshotSummaryFields::kAddedDataFiles, SnapshotSummaryFields::kDeletedDataFiles));
  ICEBERG_RETURN_UNEXPECTED(UpdateTotal(summary, previous_summary,
                                        SnapshotSummaryFields::kTotalDeleteFiles,
                                        SnapshotSummaryFields::kAddedDeleteFiles,
                                        SnapshotSummaryFields::kRemovedDeleteFiles));
  ICEBERG_RETURN_UNEXPECTED(UpdateTotal(summary, previous_summary,
                                        SnapshotSummaryFields::kTotalPosDeletes,
                                        SnapshotSummaryFields::kAddedPosDeletes,
                                        SnapshotSummaryFields::kRemovedPosDeletes));
  ICEBERG_RETURN_UNEXPECTED(UpdateTotal(
      summary, previous_summary, SnapshotSummaryFields::kTotalEqDeletes,
      SnapshotSummaryFields::kAddedEqDeletes, SnapshotSummaryFields::kRemovedEqDeletes));

  // TODO(xxx): add custom summary fields like engine info
  return summary;
}

void SnapshotUpdate::CleanAll() {
  for (const auto& manifest_list : manifest_lists_) {
    std::ignore = DeleteFile(manifest_list);
  }
  manifest_lists_.clear();
  CleanUncommitted(std::unordered_set<std::string>{});
}

Status SnapshotUpdate::DeleteFile(const std::string& path) {
  static const auto kDefaultDeleteFunc = [this](const std::string& path) {
    return this->transaction_->table()->io()->DeleteFile(path);
  };
  if (delete_func_) {
    return delete_func_(path);
  } else {
    return kDefaultDeleteFunc(path);
  }
}

std::string SnapshotUpdate::ManifestListPath() {
  // Generate manifest list path
  // Format: {metadata_location}/snap-{snapshot_id}-{attempt}-{uuid}.avro
  int64_t snapshot_id = SnapshotId();
  std::string filename =
      std::format("snap-{}-{}-{}.avro", snapshot_id, ++attempt_, commit_uuid_);
  return transaction_->MetadataFileLocation(filename);
}

std::string SnapshotUpdate::ManifestPath() {
  // Generate manifest path
  // Format: {metadata_location}/{uuid}-m{manifest_count}.avro
  std::string filename = std::format("{}-m{}.avro", commit_uuid_, manifest_count_++);
  return transaction_->MetadataFileLocation(filename);
}

}  // namespace iceberg
