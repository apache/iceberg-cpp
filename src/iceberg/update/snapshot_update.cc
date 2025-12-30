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

#include "iceberg/file_io.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/manifest_writer.h"
#include "iceberg/manifest/rolling_manifest_writer.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/snapshot_util_internal.h"
#include "iceberg/util/string_util.h"
#include "iceberg/util/uuid.h"

namespace iceberg {

SnapshotUpdate::SnapshotUpdate(std::shared_ptr<Transaction> transaction)
    : PendingUpdate(std::move(transaction)) {
  target_manifest_size_bytes_ =
      transaction_->current().properties.Get(TableProperties::kManifestTargetSizeBytes);

  // For format version 1, check if snapshot ID inheritance is enabled
  if (transaction_->current().format_version == 1) {
    can_inherit_snapshot_id_ = transaction_->current().properties.Get(
        TableProperties::kSnapshotIdInheritanceEnabled);
  }

  // Generate commit UUID
  commit_uuid_ = Uuid::GenerateV7().ToString();

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

  ICEBERG_ASSIGN_OR_RAISE(auto current_schema, transaction_->current().Schema());

  int8_t format_version = transaction_->current().format_version;
  std::optional<int64_t> snapshot_id =
      snapshot_id_ ? std::make_optional(*snapshot_id_) : std::nullopt;

  // Create factory function for rolling manifest writer
  RollingManifestWriter::ManifestWriterFactory factory =
      [this, spec, current_schema, format_version,
       snapshot_id]() -> Result<std::unique_ptr<ManifestWriter>> {
    std::string manifest_path = ManifestPath();

    if (format_version == 1) {
      return ManifestWriter::MakeV1Writer(
          snapshot_id, manifest_path, transaction_->table()->io(), spec, current_schema);
    } else if (format_version == 2) {
      return ManifestWriter::MakeV2Writer(snapshot_id, manifest_path,
                                          transaction_->table()->io(), spec,
                                          current_schema, ManifestContent::kData);
    } else {  // format_version == 3
      std::optional<int64_t> first_row_id =
          transaction_->table()->metadata()->next_row_id;
      return ManifestWriter::MakeV3Writer(snapshot_id, first_row_id, manifest_path,
                                          transaction_->table()->io(), spec,
                                          current_schema, ManifestContent::kData);
    }
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

  int8_t format_version = transaction_->current().format_version;
  if (format_version < 2) {
    // Delete manifests are only supported in format version 2+
    return std::vector<ManifestFile>{};
  }

  ICEBERG_ASSIGN_OR_RAISE(auto current_schema, transaction_->current().Schema());

  std::optional<int64_t> snapshot_id =
      snapshot_id_ ? std::make_optional(*snapshot_id_) : std::nullopt;

  // Create factory function for rolling manifest writer
  RollingManifestWriter::ManifestWriterFactory factory =
      [this, spec, current_schema, format_version,
       snapshot_id]() -> Result<std::unique_ptr<ManifestWriter>> {
    std::string manifest_path = ManifestPath();

    if (format_version == 2) {
      return ManifestWriter::MakeV2Writer(snapshot_id, manifest_path,
                                          transaction_->table()->io(), spec,
                                          current_schema, ManifestContent::kDeletes);
    } else {  // format_version == 3
      std::optional<int64_t> first_row_id =
          transaction_->table()->metadata()->next_row_id;
      return ManifestWriter::MakeV3Writer(snapshot_id, first_row_id, manifest_path,
                                          transaction_->table()->io(), spec,
                                          current_schema, ManifestContent::kDeletes);
    }
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
  snapshot_id_ = SnapshotUtil::GenerateSnapshotId(transaction_->current());
  return *snapshot_id_;
}

Result<SnapshotUpdate::ApplyResult> SnapshotUpdate::Apply() {
  ICEBERG_RETURN_UNEXPECTED(CheckErrors());

  // Get the latest snapshot for the target branch
  std::shared_ptr<Snapshot> parent_snapshot;
  std::optional<int64_t> parent_snapshot_id;
  auto parent_snapshot_result =
      SnapshotUtil::LatestSnapshot(transaction_->current(), target_branch_);
  if (!parent_snapshot_result.has_value()) [[unlikely]] {
    if (parent_snapshot_result.error().kind == ErrorKind::kNotFound) {
      parent_snapshot_id = std::nullopt;
    }
    return std::unexpected<Error>(parent_snapshot_result.error());
  } else {
    parent_snapshot = *parent_snapshot_result;
    parent_snapshot_id = parent_snapshot->snapshot_id;
  }
  int64_t sequence_number = transaction_->current().NextSequenceNumber();

  ICEBERG_RETURN_UNEXPECTED(Validate(transaction_->current(), parent_snapshot));

  std::vector<ManifestFile> manifests = Apply(transaction_->current(), parent_snapshot);

  std::string manifest_list_path = ManifestListPath();
  manifest_lists_.push_back(manifest_list_path);

  // Create manifest list writer based on format version
  int8_t format_version = transaction_->current().format_version;
  int64_t snapshot_id = SnapshotId();
  std::unique_ptr<ManifestListWriter> writer;

  if (format_version == 1) {
    ICEBERG_ASSIGN_OR_RAISE(writer, ManifestListWriter::MakeV1Writer(
                                        snapshot_id, parent_snapshot_id,
                                        manifest_list_path, transaction_->table()->io()));
  } else if (format_version == 2) {
    ICEBERG_ASSIGN_OR_RAISE(writer, ManifestListWriter::MakeV2Writer(
                                        snapshot_id, parent_snapshot_id, sequence_number,
                                        manifest_list_path, transaction_->table()->io()));
  } else {  // format_version == 3
    int64_t first_row_id = transaction_->current().next_row_id;
    ICEBERG_ASSIGN_OR_RAISE(
        writer, ManifestListWriter::MakeV3Writer(
                    snapshot_id, parent_snapshot_id, sequence_number, first_row_id,
                    manifest_list_path, transaction_->table()->io()));
  }

  ICEBERG_RETURN_UNEXPECTED(writer->AddAll(manifests));
  ICEBERG_RETURN_UNEXPECTED(writer->Close());

  // Get nextRowId and assignedRows for format version 3
  std::optional<int64_t> next_row_id;
  std::optional<int64_t> assigned_rows;
  if (format_version >= 3) {
    next_row_id = transaction_->current().next_row_id;
    if (writer->next_row_id().has_value()) {
      assigned_rows = writer->next_row_id().value() - next_row_id.value();
    }
  }

  std::unordered_map<std::string, std::string> summary = Summary();
  std::string op = operation();

  if (!op.empty() && op == DataOperation::kReplace) {
    auto added_records_it = summary.find(SnapshotSummaryFields::kAddedRecords);
    auto replaced_records_it = summary.find(SnapshotSummaryFields::kDeletedRecords);
    if (added_records_it != summary.end() && replaced_records_it != summary.end()) {
      auto added_records = StringUtils::ParseInt<int64_t>(added_records_it->second);
      auto replaced_records = StringUtils::ParseInt<int64_t>(replaced_records_it->second);
      if (added_records.has_value() && replaced_records.has_value() &&
          added_records.value() > replaced_records.value()) {
        return InvalidArgument(
            "Invalid REPLACE operation: {} added records > {} replaced records",
            added_records.value(), replaced_records.value());
      }
    }
  }

  summary = ComputeSummary(transaction_->current());

  // Get current time
  auto now = std::chrono::system_clock::now();
  auto duration_since_epoch = now.time_since_epoch();
  TimePointMs timestamp_ms = std::chrono::time_point_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::time_point(duration_since_epoch));

  // Get schema ID
  std::optional<int32_t> schema_id = transaction_->current().current_schema_id;

  // Create snapshot
  staged_snapshot_ =
      std::make_shared<Snapshot>(Snapshot{.snapshot_id = snapshot_id,
                                          .parent_snapshot_id = parent_snapshot_id,
                                          .sequence_number = sequence_number,
                                          .timestamp_ms = timestamp_ms,
                                          .manifest_list = manifest_list_path,
                                          .summary = std::move(summary),
                                          .schema_id = schema_id});

  // Return the new snapshot
  return ApplyResult{.snapshot = staged_snapshot_,
                     .target_branch = target_branch_,
                     .stage_only = stage_only_};
}

Status SnapshotUpdate::Finalize() {
  // Cleanup after successful commit
  if (cleanup_after_commit()) {
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

void SnapshotUpdate::SetTargetBranch(const std::string& branch) {
  if (branch.empty()) {
    AddError(ErrorKind::kInvalidArgument, "Invalid branch name: empty");
    return;
  }

  auto ref_it = transaction_->current().refs.find(branch);
  if (ref_it != transaction_->current().refs.end()) {
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

std::unordered_map<std::string, std::string> SnapshotUpdate::ComputeSummary(
    const TableMetadata& previous) {
  std::unordered_map<std::string, std::string> summary = Summary();

  if (summary.empty()) {
    return std::unordered_map<std::string, std::string>{};
  }

  // Get previous summary from the target branch
  std::unordered_map<std::string, std::string> previous_summary;
  if (auto ref_it = previous.refs.find(target_branch_); ref_it != previous.refs.end()) {
    auto snapshot_result = previous.SnapshotById(ref_it->second->snapshot_id);
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
  std::string filename = std::format("snap-{}-{}-{}.avro", snapshot_id,
                                     attempt_.fetch_add(1) + 1, commit_uuid_);
  return std::format("{}/metadata/{}", transaction_->table()->location(), filename);
}

std::string SnapshotUpdate::ManifestPath() {
  // Generate manifest path
  // Format: {metadata_location}/{uuid}-m{manifest_count}.avro
  int32_t count = manifest_count_.fetch_add(1);
  std::string filename = std::format("{}-m{}.avro", commit_uuid_, count);
  return std::format("{}/metadata/{}", transaction_->table()->location(), filename);
}

void SnapshotUpdate::UpdateTotal(
    std::unordered_map<std::string, std::string>& summary,
    const std::unordered_map<std::string, std::string>& previous_summary,
    const std::string& total_property, const std::string& added_property,
    const std::string& deleted_property) {
  auto total_it = previous_summary.find(total_property);
  if (total_it != previous_summary.end()) {
    auto new_total_opt = StringUtils::ParseInt<int64_t>(total_it->second);
    if (!new_total_opt.has_value()) [[unlikely]] {
      // Ignore and do not add total
      return;
    }
    int64_t new_total = new_total_opt.value();

    auto added_it = summary.find(added_property);
    if (new_total >= 0 && added_it != summary.end()) {
      auto added_value_opt = StringUtils::ParseInt<int64_t>(added_it->second);
      if (added_value_opt.has_value()) {
        new_total += added_value_opt.value();
      }
    }

    auto deleted_it = summary.find(deleted_property);
    if (new_total >= 0 && deleted_it != summary.end()) {
      auto deleted_value_opt = StringUtils::ParseInt<int64_t>(deleted_it->second);
      if (deleted_value_opt.has_value()) {
        new_total -= deleted_value_opt.value();
      }
    }

    if (new_total >= 0) {
      summary[total_property] = std::to_string(new_total);
    }
  }
}

}  // namespace iceberg
