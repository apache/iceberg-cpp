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

#include "iceberg/constants.h"
#include "iceberg/file_io.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/manifest/manifest_writer.h"
#include "iceberg/manifest/rolling_manifest_writer.h"
#include "iceberg/partition_summary_internal.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/snapshot_util_internal.h"
#include "iceberg/util/string_util.h"
#include "iceberg/util/uuid.h"

namespace iceberg {

namespace {

Status UpdateTotal(std::unordered_map<std::string, std::string>& summary,
                   const std::unordered_map<std::string, std::string>& previous_summary,
                   const std::string& total_property, const std::string& added_property,
                   const std::string& deleted_property) {
  auto total_it = previous_summary.find(total_property);
  if (total_it != previous_summary.end()) {
    ICEBERG_ASSIGN_OR_RAISE(auto new_total,
                            StringUtils::ParseInt<int64_t>(total_it->second));

    auto added_it = summary.find(added_property);
    if (new_total >= 0 && added_it != summary.end()) {
      ICEBERG_ASSIGN_OR_RAISE(auto added_value,
                              StringUtils::ParseInt<int64_t>(added_it->second));
      new_total += added_value;
    }

    auto deleted_it = summary.find(deleted_property);
    if (new_total >= 0 && deleted_it != summary.end()) {
      ICEBERG_ASSIGN_OR_RAISE(auto deleted_value,
                              StringUtils::ParseInt<int64_t>(deleted_it->second));
      new_total -= deleted_value;
    }

    if (new_total >= 0) {
      summary[total_property] = std::to_string(new_total);
    }
  }
  return {};
}

/// \brief Add metadata to a manifest file by reading it and extracting statistics.
///
/// This function reads the manifest file and fills in missing fields like
/// added_snapshot_id, file counts, row counts, and partition summaries.
Result<ManifestFile> AddMetadata(const ManifestFile& manifest,
                                 std::shared_ptr<FileIO> file_io,
                                 const TableMetadata& metadata) {
  ICEBERG_PRECHECK(manifest.added_snapshot_id == kInvalidSnapshotId,
                   "Manifest already has a snapshot ID");

  // Get the partition spec for this manifest
  auto spec_iter =
      std::ranges::find_if(metadata.partition_specs, [&manifest](const auto& spec) {
        return spec != nullptr && spec->spec_id() == manifest.partition_spec_id;
      });
  if (spec_iter == metadata.partition_specs.end()) {
    return NotFound("Partition spec with ID {} is not found", manifest.partition_spec_id);
  }
  auto spec = *spec_iter;
  ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());

  // Create a manifest reader and read all entries
  ICEBERG_ASSIGN_OR_RAISE(auto reader,
                          ManifestReader::Make(manifest, file_io, schema, spec));
  ICEBERG_ASSIGN_OR_RAISE(auto entries, reader->Entries());

  // Statistics
  int32_t added_files = 0;
  int64_t added_rows = 0;
  int32_t existing_files = 0;
  int64_t existing_rows = 0;
  int32_t deleted_files = 0;
  int64_t deleted_rows = 0;

  std::optional<int64_t> snapshot_id;
  int64_t max_snapshot_id = std::numeric_limits<int64_t>::min();

  // Get partition type and create partition summary
  ICEBERG_ASSIGN_OR_RAISE(auto partition_type, spec->PartitionType(*schema));
  PartitionSummary partition_summary(*partition_type);

  // Process entries
  for (const auto& entry : entries) {
    // Track max snapshot ID
    if (entry.snapshot_id.has_value()) {
      if (entry.snapshot_id.value() > max_snapshot_id) {
        max_snapshot_id = entry.snapshot_id.value();
      }
    }

    // Update statistics based on entry status
    switch (entry.status) {
      case ManifestStatus::kAdded:
        added_files += 1;
        if (entry.data_file) {
          added_rows += entry.data_file->record_count;
        }
        if (!snapshot_id.has_value() && entry.snapshot_id.has_value()) {
          snapshot_id = entry.snapshot_id;
        }
        break;
      case ManifestStatus::kExisting:
        existing_files += 1;
        if (entry.data_file) {
          existing_rows += entry.data_file->record_count;
        }
        break;
      case ManifestStatus::kDeleted:
        deleted_files += 1;
        if (entry.data_file) {
          deleted_rows += entry.data_file->record_count;
        }
        if (!snapshot_id.has_value() && entry.snapshot_id.has_value()) {
          snapshot_id = entry.snapshot_id;
        }
        break;
    }

    // Update partition summary
    if (entry.data_file) {
      ICEBERG_RETURN_UNEXPECTED(partition_summary.Update(entry.data_file->partition));
    }
  }

  // If no snapshot ID was found from ADDED/DELETED entries, use the max snapshot ID
  if (!snapshot_id.has_value()) {
    if (max_snapshot_id != std::numeric_limits<int64_t>::min()) {
      snapshot_id = max_snapshot_id;
    } else {
      return InvalidManifest("Cannot determine snapshot ID for manifest: {}",
                             manifest.manifest_path);
    }
  }

  ICEBERG_ASSIGN_OR_RAISE(auto partition_summaries, partition_summary.Summaries());

  // Create enriched manifest file
  ManifestFile enriched = manifest;
  enriched.added_snapshot_id = snapshot_id.value();
  enriched.added_files_count =
      added_files > 0 ? std::make_optional(added_files) : std::nullopt;
  enriched.existing_files_count =
      existing_files > 0 ? std::make_optional(existing_files) : std::nullopt;
  enriched.deleted_files_count =
      deleted_files > 0 ? std::make_optional(deleted_files) : std::nullopt;
  enriched.added_rows_count =
      added_rows > 0 ? std::make_optional(added_rows) : std::nullopt;
  enriched.existing_rows_count =
      existing_rows > 0 ? std::make_optional(existing_rows) : std::nullopt;
  enriched.deleted_rows_count =
      deleted_rows > 0 ? std::make_optional(deleted_rows) : std::nullopt;
  enriched.partitions = std::move(partition_summaries);

  return enriched;
}

}  // anonymous namespace

SnapshotUpdate::SnapshotUpdate(std::shared_ptr<Transaction> transaction)
    : PendingUpdate(std::move(transaction)),
      can_inherit_snapshot_id_(
          base().format_version == 1
              ? base().properties.Get(TableProperties::kSnapshotIdInheritanceEnabled)
              : true),
      commit_uuid_(Uuid::GenerateV7().ToString()),
      target_manifest_size_bytes_(
          base().properties.Get(TableProperties::kManifestTargetSizeBytes)) {
  // Initialize delete function if not set
  if (!delete_func_) {
    delete_func_ = [this](const std::string& path) {
      return transaction_->table()->io()->DeleteFile(path);
    };
  }
}

Result<std::vector<ManifestFile>> SnapshotUpdate::WriteDataManifests(
    const std::vector<DataFile>& data_files, const std::shared_ptr<PartitionSpec>& spec) {
  if (data_files.empty()) {
    return std::vector<ManifestFile>{};
  }

  ICEBERG_ASSIGN_OR_RAISE(auto current_schema, base().Schema());

  std::optional<int64_t> snapshot_id =
      snapshot_id_ ? std::make_optional(*snapshot_id_) : std::nullopt;

  // Create factory function for rolling manifest writer
  RollingManifestWriter::ManifestWriterFactory factory =
      [this, spec, current_schema,
       snapshot_id]() -> Result<std::unique_ptr<ManifestWriter>> {
    std::string manifest_path = ManifestPath();
    return ManifestWriter::MakeWriter(base().format_version, snapshot_id, manifest_path,
                                      transaction_->table()->io(), spec, current_schema,
                                      ManifestContent::kData,
                                      /*first_row_id=*/base().next_row_id);
  };

  // Create rolling manifest writer
  RollingManifestWriter rolling_writer(factory, target_manifest_size_bytes_);

  // Write all files
  for (const auto& file : data_files) {
    ICEBERG_RETURN_UNEXPECTED(
        rolling_writer.WriteAddedEntry(std::make_shared<DataFile>(file)));
  }

  // Close the rolling writer
  ICEBERG_RETURN_UNEXPECTED(rolling_writer.Close());

  // Get all manifest files
  ICEBERG_ASSIGN_OR_RAISE(auto manifest_files, rolling_writer.ToManifestFiles());

  return manifest_files;
}

Result<std::vector<ManifestFile>> SnapshotUpdate::WriteDeleteManifests(
    const std::vector<DataFile>& delete_files,
    const std::shared_ptr<PartitionSpec>& spec) {
  if (delete_files.empty()) {
    return std::vector<ManifestFile>{};
  }

  int8_t format_version = base().format_version;
  if (base().format_version < 2) {
    // Delete manifests are only supported in format version 2+
    return std::vector<ManifestFile>{};
  }

  ICEBERG_ASSIGN_OR_RAISE(auto current_schema, base().Schema());

  std::optional<int64_t> snapshot_id =
      snapshot_id_ ? std::make_optional(*snapshot_id_) : std::nullopt;

  // Create factory function for rolling manifest writer
  RollingManifestWriter::ManifestWriterFactory factory =
      [this, spec, current_schema, format_version,
       snapshot_id]() -> Result<std::unique_ptr<ManifestWriter>> {
    std::string manifest_path = ManifestPath();
    return ManifestWriter::MakeWriter(format_version, snapshot_id, manifest_path,
                                      transaction_->table()->io(), spec, current_schema,
                                      ManifestContent::kDeletes,
                                      /*first_row_id=*/base().next_row_id);
  };

  // Create rolling manifest writer
  RollingManifestWriter rolling_writer(factory, target_manifest_size_bytes_);

  // Write all delete files
  for (const auto& file : delete_files) {
    ICEBERG_RETURN_UNEXPECTED(
        rolling_writer.WriteAddedEntry(std::make_shared<DataFile>(file)));
  }

  // Close the rolling writer
  ICEBERG_RETURN_UNEXPECTED(rolling_writer.Close());

  // Get all manifest files
  ICEBERG_ASSIGN_OR_RAISE(auto manifest_files, rolling_writer.ToManifestFiles());

  return manifest_files;
}

int64_t SnapshotUpdate::SnapshotId() {
  if (snapshot_id_.has_value()) {
    return *snapshot_id_;
  }
  snapshot_id_ = SnapshotUtil::GenerateSnapshotId(base());
  return *snapshot_id_;
}

Result<SnapshotUpdate::ApplyResult> SnapshotUpdate::Apply() {
  ICEBERG_RETURN_UNEXPECTED(CheckErrors());

  // Get the latest snapshot for the target branch
  std::optional<int64_t> parent_snapshot_id;
  ICEBERG_ASSIGN_OR_RAISE(auto parent_snapshot,
                          SnapshotUtil::OptionalLatestSnapshot(base(), target_branch_));
  parent_snapshot_id =
      parent_snapshot ? std::make_optional(parent_snapshot->snapshot_id) : std::nullopt;
  int64_t sequence_number = base().NextSequenceNumber();

  if (parent_snapshot) {
    ICEBERG_RETURN_UNEXPECTED(Validate(base(), parent_snapshot));
  }

  ICEBERG_ASSIGN_OR_RAISE(auto manifests, Apply(base(), parent_snapshot));

  std::string manifest_list_path = ManifestListPath();
  manifest_lists_.push_back(manifest_list_path);

  // Create manifest list writer based on format version
  int8_t format_version = base().format_version;
  int64_t snapshot_id = SnapshotId();
  ICEBERG_ASSIGN_OR_RAISE(
      auto writer, ManifestListWriter::MakeWriter(
                       format_version, snapshot_id, parent_snapshot_id,
                       manifest_list_path, transaction_->table()->io(), sequence_number,
                       /*first_row_id=*/base().next_row_id));

  // Enrich manifests that are missing metadata (added_snapshot_id == kInvalidSnapshotId)
  std::vector<ManifestFile> enriched_manifests;
  enriched_manifests.reserve(manifests.size());
  for (const auto& manifest : manifests) {
    if (manifest.added_snapshot_id == kInvalidSnapshotId) {
      // Check cache first to avoid regenerating enriched manifest
      auto cache_it = enriched_manifest_cache_.find(manifest.manifest_path);
      if (cache_it != enriched_manifest_cache_.end()) {
        enriched_manifests.push_back(cache_it->second);
      } else {
        ICEBERG_ASSIGN_OR_RAISE(
            auto enriched, AddMetadata(manifest, transaction_->table()->io(), base()));
        // Store in cache for future use
        enriched_manifest_cache_[manifest.manifest_path] = enriched;
        enriched_manifests.push_back(std::move(enriched));
      }
    } else {
      enriched_manifests.push_back(manifest);
    }
  }

  ICEBERG_RETURN_UNEXPECTED(writer->AddAll(enriched_manifests));
  ICEBERG_RETURN_UNEXPECTED(writer->Close());

  // Get nextRowId and assignedRows for format version 3
  std::optional<int64_t> next_row_id;
  std::optional<int64_t> assigned_rows;
  if (format_version >= 3) {
    next_row_id = base().next_row_id;
    ICEBERG_CHECK(writer->next_row_id().has_value(),
                  "Next row ID is not set in manifest writer");
    assigned_rows = *writer->next_row_id() - *next_row_id;
  }

  std::string op = operation();
  ICEBERG_CHECK(!op.empty(), "Operation is empty");

  if (op == DataOperation::kReplace) {
    const auto& summary = Summary();
    auto added_records_it = summary.find(SnapshotSummaryFields::kAddedRecords);
    auto replaced_records_it = summary.find(SnapshotSummaryFields::kDeletedRecords);
    if (added_records_it != summary.end() && replaced_records_it != summary.end()) {
      ICEBERG_ASSIGN_OR_RAISE(auto added_records,
                              StringUtils::ParseInt<int64_t>(added_records_it->second));
      ICEBERG_ASSIGN_OR_RAISE(auto replaced_records, StringUtils::ParseInt<int64_t>(
                                                         replaced_records_it->second));
      if (added_records > replaced_records) {
        return InvalidArgument(
            "Invalid REPLACE operation: {} added records > {} replaced records",
            added_records, replaced_records);
      }
    }
  }

  ICEBERG_ASSIGN_OR_RAISE(auto summary, ComputeSummary(base()));
  if (next_row_id.has_value()) {
    summary[SnapshotSummaryFields::kFirstRowId] = std::to_string(*next_row_id);
  }
  if (assigned_rows.has_value()) {
    summary[SnapshotSummaryFields::kAddedRows] = std::to_string(*assigned_rows);
  }

  // Create snapshot
  staged_snapshot_ =
      std::make_shared<Snapshot>(Snapshot{.snapshot_id = snapshot_id,
                                          .parent_snapshot_id = parent_snapshot_id,
                                          .sequence_number = sequence_number,
                                          .timestamp_ms = CurrentTimePointMs(),
                                          .manifest_list = manifest_list_path,
                                          .summary = std::move(summary),
                                          .schema_id = base().current_schema_id});

  // Return the new snapshot
  return ApplyResult{.snapshot = staged_snapshot_,
                     .target_branch = target_branch_,
                     .stage_only = stage_only_};
}

Status SnapshotUpdate::Finalize() {
  // Cleanup after successful commit
  if (CleanupAfterCommit() && staged_snapshot_ != nullptr) {
    auto cached_snapshot = SnapshotCache(staged_snapshot_.get());
    ICEBERG_ASSIGN_OR_RAISE(auto manifests,
                            cached_snapshot.Manifests(transaction_->table()->io()));
    std::unordered_set<std::string> manifest_paths;
    for (const auto& manifest : manifests) {
      manifest_paths.insert(manifest.manifest_path);
    }
    CleanUncommitted(manifest_paths);
    // Clean up unused manifest lists
    for (const auto& manifest_list : manifest_lists_) {
      if (manifest_list != staged_snapshot_->manifest_list) {
        std::ignore = DeleteFile(manifest_list);
      }
    }
  }

  return {};
}

Status SnapshotUpdate::SetTargetBranch(const std::string& branch) {
  ICEBERG_PRECHECK(!branch.empty(), "Invalid branch name: empty");

  auto ref_it = base().refs.find(branch);
  if (ref_it != base().refs.end()) {
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
    auto snapshot = previous.OptionalSnapshotById(ref_it->second->snapshot_id);
    if (snapshot != nullptr && snapshot->summary.size() > 0) {
      previous_summary = snapshot->summary;
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

  // TODO(anyone): we can add custom summary fields like engine info in the future
  return summary;
}

void SnapshotUpdate::CleanAll() {
  for (const auto& manifest_list : manifest_lists_) {
    std::ignore = DeleteFile(manifest_list);
  }
  manifest_lists_.clear();
  // Pass empty set - subclasses will implement CleanUncommitted
  CleanUncommitted(std::unordered_set<std::string>{});
}

Status SnapshotUpdate::DeleteFile(const std::string& path) { return delete_func_(path); }

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
