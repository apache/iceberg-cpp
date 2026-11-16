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

#include "iceberg/update/merging_snapshot_update.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <format>
#include <optional>
#include <span>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "iceberg/constants.h"
#include "iceberg/delete_file_index.h"
#include "iceberg/deletes/dv_util_internal.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/expression/manifest_evaluator.h"
#include "iceberg/expression/projections.h"
#include "iceberg/file_io.h"
#include "iceberg/location_provider.h"
#include "iceberg/logging/log_macros.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_group.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/manifest/manifest_util_internal.h"
#include "iceberg/manifest/manifest_writer.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"
#include "iceberg/transaction.h"
#include "iceberg/util/content_file_util.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/snapshot_util.h"
#include "iceberg/util/struct_like_set.h"
#include "iceberg/util/task_group.h"

namespace iceberg {

namespace {

const std::array<std::string_view, 2> kValidateAddedFilesOperations = {
    DataOperation::kAppend, DataOperation::kOverwrite};
const std::array<std::string_view, 3> kValidateDataFilesExistOperations = {
    DataOperation::kOverwrite, DataOperation::kReplace, DataOperation::kDelete};
const std::array<std::string_view, 2> kValidateDataFilesExistSkipDeleteOperations = {
    DataOperation::kOverwrite, DataOperation::kReplace};
const std::array<std::string_view, 2> kValidateAddedDeleteFilesOperations = {
    DataOperation::kOverwrite, DataOperation::kDelete};
const std::array<std::string_view, 3> kValidateAddedDVsOperations = {
    DataOperation::kOverwrite, DataOperation::kDelete, DataOperation::kReplace};

bool MatchesOperation(std::optional<std::string_view> operation,
                      std::span<const std::string_view> expected) {
  return operation.has_value() &&
         std::ranges::find(expected, operation.value()) != expected.end();
}

struct ValidationHistoryResult {
  std::vector<ManifestFile> manifests;
  std::unordered_set<int64_t> snapshot_ids;
};

Result<ValidationHistoryResult> ValidationHistory(
    const TableMetadata& metadata, int64_t latest_snapshot_id,
    std::optional<int64_t> starting_snapshot_id,
    std::span<const std::string_view> matching_operations, ManifestContent content,
    const std::shared_ptr<FileIO>& io) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto ancestors,
      SnapshotUtil::AncestorsBetween(metadata, latest_snapshot_id, starting_snapshot_id));

  ValidationHistoryResult result;
  for (const auto& snapshot : ancestors) {
    if (!MatchesOperation(snapshot->Operation(), matching_operations)) {
      continue;
    }

    result.snapshot_ids.insert(snapshot->snapshot_id);
    auto snapshot_reader = SnapshotReader(snapshot.get());
    ICEBERG_ASSIGN_OR_RAISE(auto manifests, content == ManifestContent::kData
                                                ? snapshot_reader.DataManifests(io)
                                                : snapshot_reader.DeleteManifests(io));
    for (const auto& manifest : manifests) {
      if (manifest.added_snapshot_id == snapshot->snapshot_id) {
        result.manifests.push_back(manifest);
      }
    }
  }

  // The validation window must reach the requested starting snapshot, or the
  // root when no starting snapshot was provided.
  if (!ancestors.empty() &&
      ancestors.back()->parent_snapshot_id != starting_snapshot_id) {
    return ValidationFailed(
        "Cannot determine history between starting snapshot {} and the last known "
        "ancestor {}",
        starting_snapshot_id.has_value() ? std::to_string(*starting_snapshot_id) : "null",
        ancestors.back()->snapshot_id);
  }
  return result;
}

Result<std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>> PartitionSpecsByIdMap(
    const TableMetadata& metadata) {
  TableMetadataCache metadata_cache(&metadata);
  ICEBERG_ASSIGN_OR_RAISE(auto specs_ref, metadata_cache.GetPartitionSpecsById());
  return std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>(
      specs_ref.get().begin(), specs_ref.get().end());
}

Result<std::unique_ptr<ManifestGroup>> MakeValidationManifestGroup(
    const TableMetadata& metadata, const std::shared_ptr<FileIO>& io,
    std::vector<ManifestFile> manifests) {
  ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());
  ICEBERG_ASSIGN_OR_RAISE(auto specs_by_id, PartitionSpecsByIdMap(metadata));
  return ManifestGroup::Make(io, std::move(schema), std::move(specs_by_id),
                             std::move(manifests));
}

Result<int64_t> StartingSequenceNumber(const TableMetadata& metadata,
                                       std::optional<int64_t> starting_snapshot_id) {
  if (starting_snapshot_id.has_value()) {
    auto snapshot = metadata.SnapshotById(starting_snapshot_id.value());
    if (snapshot.has_value()) {
      return snapshot.value()->sequence_number;
    }
  }
  return TableMetadata::kInitialSequenceNumber;
}

Result<std::unique_ptr<DeleteFileIndex>> BuildDeleteFileIndex(
    const TableMetadata& metadata, const std::shared_ptr<FileIO>& io,
    std::vector<ManifestFile> delete_manifests, int64_t starting_sequence_number,
    std::shared_ptr<Expression> data_filter, std::shared_ptr<PartitionSet> partition_set,
    bool case_sensitive) {
  ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());
  ICEBERG_ASSIGN_OR_RAISE(auto specs_by_id, PartitionSpecsByIdMap(metadata));
  ICEBERG_ASSIGN_OR_RAISE(auto builder, DeleteFileIndex::BuilderFor(
                                            io, std::move(schema), std::move(specs_by_id),
                                            std::move(delete_manifests)));
  builder.AfterSequenceNumber(starting_sequence_number);
  builder.CaseSensitive(case_sensitive);
  if (data_filter != nullptr) {
    builder.DataFilter(std::move(data_filter));
  }
  if (partition_set != nullptr) {
    builder.FilterPartitions(std::move(partition_set));
  }
  return builder.Build();
}

Result<std::vector<ManifestFile>> FilterManifestsByPartition(
    const TableMetadata& metadata, std::shared_ptr<Expression> conflict_detection_filter,
    const std::vector<ManifestFile>& manifests, bool case_sensitive) {
  if (conflict_detection_filter == nullptr ||
      conflict_detection_filter->op() == Expression::Operation::kTrue) {
    return manifests;
  }

  const int32_t default_spec_id = metadata.default_spec_id;
  if (std::ranges::any_of(manifests, [default_spec_id](const ManifestFile& manifest) {
        return manifest.partition_spec_id != default_spec_id;
      })) {
    return manifests;
  }

  ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());
  ICEBERG_ASSIGN_OR_RAISE(auto specs_by_id, PartitionSpecsByIdMap(metadata));
  std::unordered_map<int32_t, std::unique_ptr<ManifestEvaluator>> eval_cache;
  std::vector<ManifestFile> matching_manifests;
  for (const auto& manifest : manifests) {
    auto it = eval_cache.find(manifest.partition_spec_id);
    if (it == eval_cache.end()) {
      auto spec_it = specs_by_id.find(manifest.partition_spec_id);
      if (spec_it == specs_by_id.end()) {
        return InvalidArgument("Cannot find partition spec ID {}",
                               manifest.partition_spec_id);
      }

      auto projector = Projections::Inclusive(*spec_it->second, *schema, case_sensitive);
      ICEBERG_ASSIGN_OR_RAISE(auto partition_filter,
                              projector->Project(conflict_detection_filter));
      ICEBERG_ASSIGN_OR_RAISE(
          auto evaluator,
          ManifestEvaluator::MakePartitionFilter(
              std::move(partition_filter), spec_it->second, *schema, case_sensitive));
      it = eval_cache.emplace(manifest.partition_spec_id, std::move(evaluator)).first;
    }

    ICEBERG_ASSIGN_OR_RAISE(auto matches, it->second->Evaluate(manifest));
    if (matches) {
      matching_manifests.push_back(manifest);
    }
  }
  return matching_manifests;
}

void FilterManifestEntriesByPartitionSet(ManifestGroup& group,
                                         const PartitionSet* partition_set) {
  if (partition_set != nullptr) {
    auto partitions = std::make_shared<PartitionSet>(*partition_set);
    group.FilterManifestEntries([partitions](const ManifestEntry& entry) {
      return entry.data_file != nullptr &&
             entry.data_file->partition_spec_id.has_value() &&
             partitions->contains(entry.data_file->partition_spec_id.value(),
                                  entry.data_file->partition);
    });
  }
}

Result<std::vector<ManifestEntry>> MatchingAddedDataFiles(
    const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id,
    std::shared_ptr<Expression> data_filter, const PartitionSet* partition_set,
    const std::shared_ptr<Snapshot>& parent, const std::shared_ptr<FileIO>& io,
    bool case_sensitive) {
  if (parent == nullptr) {
    return std::vector<ManifestEntry>{};
  }

  ICEBERG_ASSIGN_OR_RAISE(
      auto history,
      ValidationHistory(metadata, parent->snapshot_id, starting_snapshot_id,
                        kValidateAddedFilesOperations, ManifestContent::kData, io));
  auto new_snapshots =
      std::make_shared<std::unordered_set<int64_t>>(std::move(history.snapshot_ids));
  ICEBERG_ASSIGN_OR_RAISE(auto group, MakeValidationManifestGroup(
                                          metadata, io, std::move(history.manifests)));
  group->CaseSensitive(case_sensitive)
      .FilterManifestEntries([new_snapshots](const ManifestEntry& entry) {
        return entry.snapshot_id.has_value() &&
               new_snapshots->contains(entry.snapshot_id.value()) &&
               entry.data_file != nullptr;
      })
      .IgnoreDeleted()
      .IgnoreExisting();
  if (data_filter != nullptr) {
    group->FilterData(std::move(data_filter));
  }
  FilterManifestEntriesByPartitionSet(*group, partition_set);
  return group->Entries();
}

Result<std::vector<ManifestEntry>> MatchingDeletedDataFiles(
    const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id,
    std::shared_ptr<Expression> data_filter, const PartitionSet* partition_set,
    const std::shared_ptr<Snapshot>& parent, const std::shared_ptr<FileIO>& io,
    bool case_sensitive) {
  if (parent == nullptr) {
    return std::vector<ManifestEntry>{};
  }

  ICEBERG_ASSIGN_OR_RAISE(
      auto history,
      ValidationHistory(metadata, parent->snapshot_id, starting_snapshot_id,
                        kValidateDataFilesExistOperations, ManifestContent::kData, io));
  auto new_snapshots =
      std::make_shared<std::unordered_set<int64_t>>(std::move(history.snapshot_ids));
  ICEBERG_ASSIGN_OR_RAISE(auto group, MakeValidationManifestGroup(
                                          metadata, io, std::move(history.manifests)));
  group->CaseSensitive(case_sensitive)
      .FilterManifestEntries([new_snapshots](const ManifestEntry& entry) {
        return entry.snapshot_id.has_value() &&
               new_snapshots->contains(entry.snapshot_id.value());
      })
      .FilterManifestEntries([](const ManifestEntry& entry) {
        return entry.status == ManifestStatus::kDeleted && entry.data_file != nullptr;
      })
      .IgnoreExisting();
  if (data_filter != nullptr) {
    group->FilterData(std::move(data_filter));
  }
  FilterManifestEntriesByPartitionSet(*group, partition_set);
  return group->Entries();
}

std::string FormatLocations(std::vector<std::string> locations) {
  std::string result = "[";
  for (size_t i = 0; i < locations.size(); ++i) {
    if (i > 0) {
      result += ", ";
    }
    result += locations[i];
  }
  result += "]";
  return result;
}

std::vector<std::string> DataFilePaths(const std::vector<ManifestEntry>& entries) {
  std::vector<std::string> paths;
  paths.reserve(entries.size());
  for (const auto& entry : entries) {
    if (entry.data_file != nullptr) {
      paths.push_back(entry.data_file->file_path);
    }
  }
  return paths;
}

std::optional<std::string> DataFileLocations(const std::vector<ManifestEntry>& entries) {
  auto paths = DataFilePaths(entries);
  if (paths.empty()) {
    return std::optional<std::string>{};
  }
  return FormatLocations(std::move(paths));
}

std::vector<std::string> ReferencedDeleteFileLocations(
    const std::vector<std::unique_ptr<DeleteFileIndex>>& delete_indexes) {
  std::vector<std::string> locations;
  std::unordered_set<std::string> seen;
  for (const auto& deletes : delete_indexes) {
    for (const auto& delete_file : deletes->ReferencedDeleteFiles()) {
      if (seen.insert(delete_file->file_path).second) {
        locations.push_back(delete_file->file_path);
      }
    }
  }
  return locations;
}

bool ContainsPositionDeletes(const std::vector<std::shared_ptr<DataFile>>& delete_files) {
  for (const auto& delete_file : delete_files) {
    if (delete_file->content == DataFile::Content::kPositionDeletes) {
      return true;
    }
  }
  return false;
}

Status ValidateAddedDVsInManifest(
    const TableMetadata& metadata, const ManifestFile& manifest,
    std::shared_ptr<Expression> conflict_detection_filter,
    const std::unordered_set<int64_t>& new_snapshot_ids,
    const std::unordered_set<std::string>& referenced_data_files,
    const std::shared_ptr<FileIO>& io, const std::shared_ptr<Schema>& schema,
    bool case_sensitive) {
  ICEBERG_ASSIGN_OR_RAISE(auto spec,
                          metadata.PartitionSpecById(manifest.partition_spec_id));
  ICEBERG_ASSIGN_OR_RAISE(auto reader, ManifestReader::Make(manifest, io, schema, spec));
  reader->CaseSensitive(case_sensitive);
  reader->FilterRows(std::move(conflict_detection_filter));
  ICEBERG_ASSIGN_OR_RAISE(auto entries, reader->LiveEntries());

  for (const auto& entry : entries) {
    if (!entry.snapshot_id.has_value() ||
        !new_snapshot_ids.contains(entry.snapshot_id.value())) {
      continue;
    }
    if (entry.data_file == nullptr || !ContentFileUtil::IsDV(*entry.data_file) ||
        !entry.data_file->referenced_data_file.has_value()) {
      continue;
    }
    if (referenced_data_files.contains(*entry.data_file->referenced_data_file)) {
      return ValidationFailed("Found concurrently added DV for {}: {}",
                              *entry.data_file->referenced_data_file,
                              ContentFileUtil::DVDesc(*entry.data_file));
    }
  }
  return {};
}

}  // namespace

MergingSnapshotUpdate::MergingSnapshotUpdate(std::string table_name,
                                             std::shared_ptr<TransactionContext> ctx)
    : SnapshotUpdate(std::move(ctx)),
      table_name_(std::move(table_name)),
      delete_expression_(Expressions::AlwaysFalse()) {
  auto file_io = ctx_->table->io();
  auto data_filter_manager = ManifestFilterManager::Make(
      ManifestContent::kData, file_io,
      [this](const std::string& location) { return DeleteFile(location); },
      [this] { return plan_executor(); });
  if (!data_filter_manager.has_value()) {
    AddError(data_filter_manager.error());
  } else {
    data_filter_manager_ = std::move(data_filter_manager.value());
  }

  auto delete_filter_manager = ManifestFilterManager::Make(
      ManifestContent::kDeletes, file_io,
      [this](const std::string& location) { return DeleteFile(location); },
      [this] { return plan_executor(); });
  if (!delete_filter_manager.has_value()) {
    AddError(delete_filter_manager.error());
  } else {
    delete_filter_manager_ = std::move(delete_filter_manager.value());
  }

  const int64_t target_size_bytes =
      base().properties.Get(TableProperties::kManifestTargetSizeBytes);
  const int32_t min_count_to_merge =
      base().properties.Get(TableProperties::kManifestMinMergeCount);
  const bool merge_enabled =
      base().properties.Get(TableProperties::kManifestMergeEnabled);
  auto data_merge_manager = ManifestMergeManager::Make(
      ManifestContent::kData, target_size_bytes, min_count_to_merge, merge_enabled,
      file_io, [this] { return SnapshotId(); },
      [this](const std::string& location) { return DeleteFile(location); },
      [this] { return plan_executor(); });
  if (!data_merge_manager.has_value()) {
    AddError(data_merge_manager.error());
  } else {
    data_merge_manager_ = std::move(data_merge_manager.value());
  }

  auto delete_merge_manager = ManifestMergeManager::Make(
      ManifestContent::kDeletes, target_size_bytes, min_count_to_merge, merge_enabled,
      file_io, [this] { return SnapshotId(); },
      [this](const std::string& location) { return DeleteFile(location); },
      [this] { return plan_executor(); });
  if (!delete_merge_manager.has_value()) {
    AddError(delete_merge_manager.error());
  } else {
    delete_merge_manager_ = std::move(delete_merge_manager.value());
  }
}

void MergingSnapshotUpdate::SetSummaryProperty(const std::string& property,
                                               const std::string& value) {
  SnapshotUpdate::SetSummaryProperty(property, value);
}

void MergingSnapshotUpdate::CaseSensitive(bool case_sensitive) {
  case_sensitive_ = case_sensitive;
  data_filter_manager_->CaseSensitive(case_sensitive);
  delete_filter_manager_->CaseSensitive(case_sensitive);
}

Result<std::shared_ptr<PartitionSpec>> MergingSnapshotUpdate::DataSpec() const {
  if (new_data_files_by_spec_.empty()) {
    return InvalidArgument(
        "Cannot determine partition specs: no data files have been added");
  }
  if (new_data_files_by_spec_.size() > 1) {
    return InvalidArgument(
        "Cannot return a single partition spec: data files with different partition "
        "specs have been added");
  }
  return base().PartitionSpecById(new_data_files_by_spec_.begin()->first);
}

std::vector<std::shared_ptr<DataFile>> MergingSnapshotUpdate::AddedDataFiles() const {
  std::vector<std::shared_ptr<DataFile>> result;
  for (const auto& [spec_id, files] : new_data_files_by_spec_) {
    for (const auto& file : files) {
      result.push_back(file);
    }
  }
  return result;
}

void MergingSnapshotUpdate::FailAnyDelete() {
  data_filter_manager_->FailAnyDelete();
  delete_filter_manager_->FailAnyDelete();
}

void MergingSnapshotUpdate::FailMissingDeletePaths() {
  data_filter_manager_->FailMissingDeletePaths();
  delete_filter_manager_->FailMissingDeletePaths();
}

Status MergingSnapshotUpdate::DeleteByRowFilter(std::shared_ptr<Expression> expr) {
  // If a delete file matches the row filter, it can also be removed because the rows
  // it references will also be deleted. Both filter managers receive the expression.
  delete_expression_ = expr;
  ICEBERG_RETURN_UNEXPECTED(data_filter_manager_->DeleteByRowFilter(expr));
  return delete_filter_manager_->DeleteByRowFilter(std::move(expr));
}

Status MergingSnapshotUpdate::DropPartition(int32_t spec_id, PartitionValues partition) {
  // Dropping data in a partition also drops all delete files in that partition.
  ICEBERG_RETURN_UNEXPECTED(data_filter_manager_->DropPartition(spec_id, partition));
  ICEBERG_RETURN_UNEXPECTED(
      delete_filter_manager_->DropPartition(spec_id, std::move(partition)));
  return {};
}

Status MergingSnapshotUpdate::DeleteDataFile(std::shared_ptr<DataFile> file) {
  if (!file) {
    return InvalidArgument("Cannot delete a null data file");
  }
  auto staged_file = std::make_shared<DataFile>(*file);
  return data_filter_manager_->DeleteFile(std::move(staged_file));
}

Status MergingSnapshotUpdate::DeleteDeleteFile(std::shared_ptr<DataFile> file) {
  if (!file) {
    return InvalidArgument("Cannot delete a null delete file");
  }
  auto staged_file = std::make_shared<DataFile>(*file);
  return delete_filter_manager_->DeleteFile(std::move(staged_file));
}

Status MergingSnapshotUpdate::DeleteByPath(std::string_view path) {
  return data_filter_manager_->DeleteFile(path);
}

bool MergingSnapshotUpdate::DeletesDataFiles() const {
  return data_filter_manager_->ContainsDeletes();
}

bool MergingSnapshotUpdate::DeletesDeleteFiles() const {
  return delete_filter_manager_->ContainsDeletes();
}

bool MergingSnapshotUpdate::AddsDataFiles() const {
  return !new_data_files_by_spec_.empty();
}

bool MergingSnapshotUpdate::AddsDeleteFiles() const {
  return !v2_deletes_.empty() || !dvs_by_referenced_file_.empty();
}

Status MergingSnapshotUpdate::AddDataFile(std::shared_ptr<DataFile> file) {
  if (!file) {
    return InvalidArgument("Cannot add a null data file");
  }
  if (file->content != DataFile::Content::kData) {
    return InvalidArgument("Invalid data file to add: {} has delete-file content",
                           file->file_path);
  }
  if (!file->partition_spec_id.has_value()) {
    return InvalidArgument("Data file must have a partition spec ID");
  }

  int32_t spec_id = file->partition_spec_id.value();
  ICEBERG_ASSIGN_OR_RAISE(auto spec, base().PartitionSpecById(spec_id));

  // Suppress first_row_id in the staged copy. The commit assigns row IDs for newly
  // added files and must not mutate the caller-owned file object.
  auto staged_file = std::make_shared<DataFile>(*file);
  staged_file->first_row_id = std::nullopt;

  auto& data_files = new_data_files_by_spec_[spec_id];
  auto [it, inserted] = data_files.insert(staged_file);
  if (inserted) {
    has_new_data_files_ = true;
    ICEBERG_RETURN_UNEXPECTED(added_data_files_summary_.AddedFile(*spec, *staged_file));
  }
  return {};
}

Status MergingSnapshotUpdate::AddDeleteFile(std::shared_ptr<DataFile> file) {
  return AddDeleteFile(std::move(file), std::nullopt);
}

Status MergingSnapshotUpdate::AddDeleteFile(std::shared_ptr<DataFile> file,
                                            int64_t data_sequence_number) {
  return AddDeleteFile(std::move(file), std::optional<int64_t>(data_sequence_number));
}

Status MergingSnapshotUpdate::AddDeleteFile(std::shared_ptr<DataFile> file,
                                            std::optional<int64_t> data_sequence_number) {
  if (!file) {
    return InvalidArgument("Cannot add a null delete file");
  }
  ICEBERG_RETURN_UNEXPECTED(ValidateNewDeleteFile(base(), *file));
  if (!file->partition_spec_id.has_value()) {
    return InvalidArgument("Delete file must have a partition spec ID");
  }
  ICEBERG_RETURN_UNEXPECTED(base().PartitionSpecById(file->partition_spec_id.value()));

  auto staged_file = std::make_shared<DataFile>(*file);
  has_new_delete_files_ = true;
  PendingDeleteFile pending_file{.file = std::move(staged_file),
                                 .data_sequence_number = std::move(data_sequence_number)};
  if (ContentFileUtil::IsDV(*pending_file.file)) {
    ICEBERG_PRECHECK(pending_file.file->referenced_data_file.has_value(),
                     "DV must have a referenced data file: {}",
                     pending_file.file->file_path);
    auto referenced_data_file = *pending_file.file->referenced_data_file;
    dvs_by_referenced_file_.Add(std::move(referenced_data_file), std::move(pending_file));
  } else {
    v2_deletes_.push_back(std::move(pending_file));
  }
  return {};
}

Status MergingSnapshotUpdate::ValidateNewDeleteFile(const TableMetadata& metadata,
                                                    const DataFile& file) {
  if (file.content == DataFile::Content::kData) {
    return InvalidArgument("Expected a delete file but got a data file: {}",
                           file.file_path);
  }
  const int8_t format_version = metadata.format_version;
  const bool is_dv = ContentFileUtil::IsDV(file);
  switch (format_version) {
    case 1:
      return InvalidArgument("Deletes are supported in V2 and above");
    case 2:
      // Position deletes must NOT be DVs in v2.
      if (file.content == DataFile::Content::kPositionDeletes && is_dv) {
        return InvalidArgument("Must not use DVs for position deletes in V2: {}",
                               file.file_path);
      }
      break;
    default:
      if (format_version >= 3 &&
          format_version <= TableMetadata::kSupportedTableFormatVersion) {
        // Position deletes MUST be DVs in v3+.
        if (file.content == DataFile::Content::kPositionDeletes && !is_dv) {
          return InvalidArgument("Must use DVs for position deletes in V{}: {}",
                                 format_version, file.file_path);
        }
      } else {
        return InvalidArgument("Unsupported format version: {}", format_version);
      }
      break;
  }
  return {};
}

Status MergingSnapshotUpdate::AddManifest(ManifestFile manifest) {
  if (manifest.content != ManifestContent::kData) {
    return InvalidArgument("Cannot append delete manifest: {}", manifest.manifest_path);
  }
  if (can_inherit_snapshot_id() && !manifest.added_snapshot_id.has_value()) {
    if (manifest.first_row_id.has_value()) {
      return InvalidArgument("Cannot append manifest with assigned first row ID: {}",
                             manifest.manifest_path);
    }
    appended_manifests_summary_.AddedManifest(manifest);
    append_manifests_.push_back(std::move(manifest));
  } else {
    ICEBERG_RETURN_UNEXPECTED(EnsureRewrittenAppendManifests());
    // The manifest must be rewritten with this update's snapshot ID and null
    // first_row_ids.
    ICEBERG_ASSIGN_OR_RAISE(auto copied_manifest, CopyManifest(manifest));
    rewritten_append_manifests_.push_back(std::move(copied_manifest));
    append_manifests_to_copy_.push_back(std::move(manifest));
  }
  return {};
}

Result<ManifestFile> MergingSnapshotUpdate::CopyManifest(const ManifestFile& manifest) {
  const TableMetadata& current = base();
  ICEBERG_ASSIGN_OR_RAISE(auto schema, SnapshotUtil::SchemaFor(current, target_branch()));
  ICEBERG_ASSIGN_OR_RAISE(auto spec,
                          current.PartitionSpecById(manifest.partition_spec_id));
  std::string path = ManifestPath();
  SnapshotSummaryBuilder summary;
  ICEBERG_ASSIGN_OR_RAISE(
      auto copied_manifest,
      CopyAppendManifest(manifest, ctx_->table->io(), schema, spec, SnapshotId(), path,
                         current.format_version, &summary));
  appended_manifests_summary_.Merge(summary);
  return copied_manifest;
}

Status MergingSnapshotUpdate::ValidateAddedDataFiles(
    const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id,
    std::shared_ptr<Expression> data_filter, const std::shared_ptr<Snapshot>& parent,
    std::shared_ptr<FileIO> io, bool case_sensitive) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto conflict_entries,
      MatchingAddedDataFiles(metadata, starting_snapshot_id, data_filter,
                             /*partition_set=*/nullptr, parent, io, case_sensitive));
  auto conflict_paths = DataFileLocations(conflict_entries);
  if (conflict_paths.has_value()) {
    return ValidationFailed(
        "Found conflicting files that can contain records matching {}: {}",
        data_filter != nullptr ? data_filter->ToString() : "any expression",
        conflict_paths.value());
  }
  return {};
}

Status MergingSnapshotUpdate::ValidateAddedDataFiles(
    const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id,
    const PartitionSet& partition_set, const std::shared_ptr<Snapshot>& parent,
    std::shared_ptr<FileIO> io) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto conflict_entries,
      MatchingAddedDataFiles(metadata, starting_snapshot_id,
                             /*data_filter=*/nullptr, &partition_set, parent, io,
                             /*case_sensitive=*/true));
  auto conflict_paths = DataFileLocations(conflict_entries);
  if (conflict_paths.has_value()) {
    return ValidationFailed(
        "Found conflicting files that can contain records matching validated "
        "partitions: {}",
        conflict_paths.value());
  }
  return {};
}

Status MergingSnapshotUpdate::ValidateNoNewDeletesForDataFiles(
    const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id,
    const DataFileSet& replaced_files, const std::shared_ptr<Snapshot>& parent,
    std::shared_ptr<FileIO> io, bool ignore_equality_deletes) {
  if (parent == nullptr || metadata.format_version < 2) {
    return {};
  }

  ICEBERG_ASSIGN_OR_RAISE(
      auto delete_indexes,
      AddedDeleteFilesIndexedPerSnapshot(metadata, starting_snapshot_id,
                                         /*data_filter=*/nullptr,
                                         /*partition_set=*/nullptr, parent, io));
  ICEBERG_ASSIGN_OR_RAISE(auto starting_sequence_number,
                          StartingSequenceNumber(metadata, starting_snapshot_id));

  for (const auto& data_file : replaced_files) {
    for (const auto& deletes : delete_indexes) {
      // If any delete applies to files written in or before the starting snapshot, fail.
      ICEBERG_ASSIGN_OR_RAISE(auto delete_files,
                              deletes->ForDataFile(starting_sequence_number, *data_file));
      if (ignore_equality_deletes) {
        if (ContainsPositionDeletes(delete_files)) {
          return ValidationFailed(
              "Cannot commit, found new position delete for replaced data file: {}",
              data_file->file_path);
        }
      } else if (!delete_files.empty()) {
        return ValidationFailed(
            "Cannot commit, found new delete for replaced data file: {}",
            data_file->file_path);
      }
    }
  }
  return {};
}

Status MergingSnapshotUpdate::ValidateNoNewDeletesForDataFiles(
    const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id,
    std::shared_ptr<Expression> data_filter, const DataFileSet& replaced_files,
    const std::shared_ptr<Snapshot>& parent, std::shared_ptr<FileIO> io,
    bool case_sensitive) {
  if (parent == nullptr || metadata.format_version < 2) {
    return {};
  }

  ICEBERG_ASSIGN_OR_RAISE(auto delete_indexes,
                          AddedDeleteFilesIndexedPerSnapshot(
                              metadata, starting_snapshot_id, std::move(data_filter),
                              /*partition_set=*/nullptr, parent, io, case_sensitive));

  ICEBERG_ASSIGN_OR_RAISE(auto starting_sequence_number,
                          StartingSequenceNumber(metadata, starting_snapshot_id));

  for (const auto& data_file : replaced_files) {
    for (const auto& deletes : delete_indexes) {
      ICEBERG_ASSIGN_OR_RAISE(auto delete_files,
                              deletes->ForDataFile(starting_sequence_number, *data_file));
      if (!delete_files.empty()) {
        return ValidationFailed(
            "Cannot commit, found new delete for replaced data file: {}",
            data_file->file_path);
      }
    }
  }
  return {};
}

Status MergingSnapshotUpdate::ValidateNoNewDeleteFiles(
    const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id,
    std::shared_ptr<Expression> data_filter, const std::shared_ptr<Snapshot>& parent,
    std::shared_ptr<FileIO> io, bool case_sensitive) {
  std::string data_filter_text =
      data_filter != nullptr ? data_filter->ToString() : "any expression";
  ICEBERG_ASSIGN_OR_RAISE(auto delete_indexes,
                          AddedDeleteFilesIndexedPerSnapshot(
                              metadata, starting_snapshot_id, std::move(data_filter),
                              /*partition_set=*/nullptr, parent, io, case_sensitive));
  auto locations = ReferencedDeleteFileLocations(delete_indexes);
  if (!locations.empty()) {
    return ValidationFailed(
        "Found new conflicting delete files that can apply to records matching {}: {}",
        data_filter_text, FormatLocations(std::move(locations)));
  }
  return {};
}

Status MergingSnapshotUpdate::ValidateNoNewDeleteFiles(
    const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id,
    const PartitionSet& partition_set, const std::shared_ptr<Snapshot>& parent,
    std::shared_ptr<FileIO> io) {
  ICEBERG_ASSIGN_OR_RAISE(auto delete_indexes,
                          AddedDeleteFilesIndexedPerSnapshot(
                              metadata, starting_snapshot_id,
                              /*data_filter=*/nullptr,
                              std::make_shared<PartitionSet>(partition_set), parent, io));
  auto locations = ReferencedDeleteFileLocations(delete_indexes);
  if (!locations.empty()) {
    return ValidationFailed(
        "Found new conflicting delete files that can apply to records matching "
        "validated partitions: {}",
        FormatLocations(std::move(locations)));
  }
  return {};
}

Result<std::vector<std::unique_ptr<DeleteFileIndex>>>
MergingSnapshotUpdate::AddedDeleteFilesIndexedPerSnapshot(
    const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id,
    std::shared_ptr<Expression> data_filter, std::shared_ptr<PartitionSet> partition_set,
    const std::shared_ptr<Snapshot>& parent, std::shared_ptr<FileIO> io,
    bool case_sensitive) {
  // If there is no current table state, no delete files have been added.
  if (parent == nullptr || metadata.format_version < 2) {
    return std::vector<std::unique_ptr<DeleteFileIndex>>{};
  }

  ICEBERG_ASSIGN_OR_RAISE(
      auto history, ValidationHistory(metadata, parent->snapshot_id, starting_snapshot_id,
                                      kValidateAddedDeleteFilesOperations,
                                      ManifestContent::kDeletes, io));

  // History collects a manifest only from the snapshot that added it. Grouping by
  // snapshot ID assigns each manifest to exactly one index and still reads it once.
  // Preserve history order to keep the failure message deterministic.
  std::vector<std::vector<ManifestFile>> delete_manifests_by_snapshot;
  std::unordered_map<int64_t, size_t> group_by_snapshot_id;
  for (auto& manifest : history.manifests) {
    auto [iter, inserted] = group_by_snapshot_id.try_emplace(
        manifest.added_snapshot_id.value(), delete_manifests_by_snapshot.size());
    if (inserted) {
      delete_manifests_by_snapshot.emplace_back();
    }
    delete_manifests_by_snapshot[iter->second].push_back(std::move(manifest));
  }

  ICEBERG_ASSIGN_OR_RAISE(auto starting_sequence_number,
                          StartingSequenceNumber(metadata, starting_snapshot_id));
  std::vector<std::unique_ptr<DeleteFileIndex>> delete_indexes;
  delete_indexes.reserve(delete_manifests_by_snapshot.size());
  for (auto& delete_manifests : delete_manifests_by_snapshot) {
    ICEBERG_ASSIGN_OR_RAISE(
        auto deletes, BuildDeleteFileIndex(metadata, io, std::move(delete_manifests),
                                           starting_sequence_number, data_filter,
                                           partition_set, case_sensitive));
    delete_indexes.push_back(std::move(deletes));
  }
  return delete_indexes;
}

Status MergingSnapshotUpdate::ValidateDeletedDataFiles(
    const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id,
    std::shared_ptr<Expression> data_filter, const std::shared_ptr<Snapshot>& parent,
    std::shared_ptr<FileIO> io, bool case_sensitive) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto conflict_entries,
      MatchingDeletedDataFiles(metadata, starting_snapshot_id, data_filter,
                               /*partition_set=*/nullptr, parent, io, case_sensitive));
  auto conflict_paths = DataFileLocations(conflict_entries);
  if (conflict_paths.has_value()) {
    return ValidationFailed(
        "Found conflicting deleted files that can contain records matching {}: {}",
        data_filter != nullptr ? data_filter->ToString() : "any expression",
        conflict_paths.value());
  }
  return {};
}

Status MergingSnapshotUpdate::ValidateDeletedDataFiles(
    const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id,
    const PartitionSet& partition_set, const std::shared_ptr<Snapshot>& parent,
    std::shared_ptr<FileIO> io) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto conflict_entries,
      MatchingDeletedDataFiles(metadata, starting_snapshot_id,
                               /*data_filter=*/nullptr, &partition_set, parent, io,
                               /*case_sensitive=*/true));
  auto conflict_paths = DataFileLocations(conflict_entries);
  if (conflict_paths.has_value()) {
    return ValidationFailed(
        "Found conflicting deleted files that can apply to records matching "
        "validated partitions: {}",
        conflict_paths.value());
  }
  return {};
}

void MergingSnapshotUpdate::SetNewDataFilesDataSequenceNumber(int64_t sequence_number) {
  new_data_files_data_seq_number_ = sequence_number;
}

Status MergingSnapshotUpdate::ValidateDataFilesExist(
    const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id,
    const std::unordered_set<std::string>& file_paths, bool skip_deletes,
    std::shared_ptr<Expression> conflict_detection_filter,
    const std::shared_ptr<Snapshot>& parent, std::shared_ptr<FileIO> io,
    bool /*case_sensitive*/) {
  if (parent == nullptr) {
    return {};
  }

  std::span<const std::string_view> matching_operations =
      skip_deletes
          ? std::span<const std::string_view>(kValidateDataFilesExistSkipDeleteOperations)
          : std::span<const std::string_view>(kValidateDataFilesExistOperations);
  ICEBERG_ASSIGN_OR_RAISE(
      auto history, ValidationHistory(metadata, parent->snapshot_id, starting_snapshot_id,
                                      matching_operations, ManifestContent::kData, io));

  ICEBERG_ASSIGN_OR_RAISE(auto group, MakeValidationManifestGroup(
                                          metadata, io, std::move(history.manifests)));
  group->IgnoreExisting();
  group->FilterManifestEntries([&history, &file_paths](const ManifestEntry& entry) {
    return entry.status != ManifestStatus::kAdded && entry.snapshot_id.has_value() &&
           history.snapshot_ids.contains(entry.snapshot_id.value()) &&
           entry.data_file != nullptr && file_paths.contains(entry.data_file->file_path);
  });
  if (conflict_detection_filter != nullptr) {
    group->FilterData(std::move(conflict_detection_filter));
  }

  ICEBERG_ASSIGN_OR_RAISE(auto entries, group->Entries());
  auto deleted_paths = DataFileLocations(entries);
  if (deleted_paths.has_value()) {
    return ValidationFailed("Cannot commit, missing data files: {}",
                            deleted_paths.value());
  }
  return {};
}

Status MergingSnapshotUpdate::ValidateAddedDVs(
    const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id,
    std::shared_ptr<Expression> conflict_detection_filter,
    const std::unordered_set<std::string>& referenced_data_files,
    const std::shared_ptr<Snapshot>& parent, std::shared_ptr<FileIO> io,
    bool case_sensitive, OptionalExecutor executor) {
  if (parent == nullptr || referenced_data_files.empty()) {
    return {};
  }

  ICEBERG_ASSIGN_OR_RAISE(
      auto history,
      ValidationHistory(metadata, parent->snapshot_id, starting_snapshot_id,
                        kValidateAddedDVsOperations, ManifestContent::kDeletes, io));
  ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());

  ICEBERG_ASSIGN_OR_RAISE(auto matching_manifests,
                          FilterManifestsByPartition(metadata, conflict_detection_filter,
                                                     history.manifests, case_sensitive));
  std::atomic<bool> failed{false};
  auto tasks = TaskGroup().SetExecutor(executor);
  for (const auto& manifest : matching_manifests) {
    if (!manifest.has_added_files()) {
      continue;
    }
    tasks.Submit([&, manifest]() -> Status {
      if (failed.load()) {
        return {};
      }
      auto status = ValidateAddedDVsInManifest(
          metadata, manifest, conflict_detection_filter, history.snapshot_ids,
          referenced_data_files, io, schema, case_sensitive);
      if (!status) {
        failed.store(true);
      }
      return status;
    });
  }
  return std::move(tasks).Run();
}

Status MergingSnapshotUpdate::ValidateAddedDVs(
    const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id,
    std::shared_ptr<Expression> conflict_filter, const std::shared_ptr<Snapshot>& parent,
    std::shared_ptr<FileIO> io) const {
  if (parent == nullptr) {
    return {};
  }

  std::unordered_set<std::string> referenced_data_files;
  for (const auto& entry : dvs_by_referenced_file_.entries()) {
    referenced_data_files.insert(entry.referenced_file);
  }
  if (referenced_data_files.empty()) {
    return {};
  }
  // DVs for data files added in this operation cannot conflict with concurrent commits.
  auto unmatched_referenced_data_files = referenced_data_files;
  for (const auto& [spec_id, data_files] : new_data_files_by_spec_) {
    for (const auto& data_file : data_files) {
      if (unmatched_referenced_data_files.erase(data_file->file_path) != 0 &&
          unmatched_referenced_data_files.empty()) {
        return {};
      }
    }
  }

  return ValidateAddedDVs(metadata, starting_snapshot_id, std::move(conflict_filter),
                          referenced_data_files, parent, std::move(io), case_sensitive_,
                          plan_executor());
}

std::unordered_map<std::string, std::string> MergingSnapshotUpdate::Summary() {
  summary_builder().SetPartitionSummaryLimit(
      base().properties.Get(TableProperties::kWritePartitionSummaryLimit));
  return summary_builder().Build();
}

Status MergingSnapshotUpdate::ValidateDeleteFilesForVersion(
    const TableMetadata& current) {
  // Guard buffered deletes against concurrent format upgrade.
  for (const auto& file : v2_deletes_) {
    ICEBERG_RETURN_UNEXPECTED(ValidateNewDeleteFile(current, *file.file));
  }
  return {};
}

Result<std::vector<ManifestFile>> MergingSnapshotUpdate::Apply(
    const TableMetadata& metadata_to_update, const std::shared_ptr<Snapshot>& snapshot) {
  ICEBERG_RETURN_UNEXPECTED(ManagersReady());
  ICEBERG_RETURN_UNEXPECTED(ValidateDeleteFilesForVersion(metadata_to_update));
  ICEBERG_ASSIGN_OR_RAISE(auto table_schema,
                          SnapshotUtil::SchemaFor(metadata_to_update, target_branch()));
  auto writer_factory = MakeWriterFactory(table_schema);
  TableMetadataCache metadata_cache(&metadata_to_update);
  ICEBERG_ASSIGN_OR_RAISE(auto specs_by_id, metadata_cache.GetPartitionSpecsById());
  SnapshotReader snapshot_reader(snapshot.get());
  std::vector<const ManifestFile*> data_manifests;
  std::vector<const ManifestFile*> delete_manifests;
  if (snapshot) {
    ICEBERG_ASSIGN_OR_RAISE(auto manifests, snapshot_reader.Manifests(ctx_->table->io()));
    for (const auto& manifest : manifests) {
      (manifest.content == ManifestContent::kData ? data_manifests : delete_manifests)
          .push_back(&manifest);
    }
  }

  // Filter any existing manifests.
  ICEBERG_ASSIGN_OR_RAISE(auto filtered, data_filter_manager_->FilterManifests(
                                             table_schema, specs_by_id.get(),
                                             data_manifests, writer_factory));
  int64_t min_data_sequence_number = metadata_to_update.last_sequence_number;
  for (const auto& manifest : filtered) {
    // Filter out unassigned sequence numbers in rewritten manifests.
    if (manifest.min_sequence_number != kUnassignedSequenceNumber) {
      min_data_sequence_number =
          std::min(min_data_sequence_number, manifest.min_sequence_number);
    }
  }
  ICEBERG_RETURN_UNEXPECTED(
      delete_filter_manager_->DropDeleteFilesOlderThan(min_data_sequence_number));

  const auto& files_to_be_deleted = data_filter_manager_->FilesToBeDeleted();
  delete_filter_manager_->RemoveDanglingDeletesFor(files_to_be_deleted);
  ICEBERG_ASSIGN_OR_RAISE(auto filtered_deletes, delete_filter_manager_->FilterManifests(
                                                     table_schema, specs_by_id.get(),
                                                     delete_manifests, writer_factory));

  // Only keep manifests that have live data files or that were written by this commit.
  auto should_keep = [snapshot_id = SnapshotId()](const ManifestFile& manifest) {
    return manifest.has_added_files() || manifest.has_existing_files() ||
           manifest.added_snapshot_id == snapshot_id;
  };
  ICEBERG_ASSIGN_OR_RAISE(auto unmerged_manifests, PrepareNewDataManifests());
  unmerged_manifests.insert(unmerged_manifests.end(), filtered.begin(), filtered.end());
  std::erase_if(unmerged_manifests,
                [&](const auto& manifest) { return !should_keep(manifest); });
  ICEBERG_ASSIGN_OR_RAISE(auto unmerged_delete_manifests, PrepareDeleteManifests());
  unmerged_delete_manifests.insert(unmerged_delete_manifests.end(),
                                   filtered_deletes.begin(), filtered_deletes.end());
  std::erase_if(unmerged_delete_manifests,
                [&](const auto& manifest) { return !should_keep(manifest); });

  // Update the snapshot summary.
  summary_builder().Clear();
  summary_builder().Merge(added_data_files_summary_);
  summary_builder().Merge(added_delete_files_summary_);
  summary_builder().Merge(appended_manifests_summary_);
  ICEBERG_ASSIGN_OR_RAISE(auto filter_summary, data_filter_manager_->BuildSummary(
                                                   filtered, specs_by_id.get()));
  summary_builder().Merge(filter_summary);
  ICEBERG_ASSIGN_OR_RAISE(
      auto delete_filter_summary,
      delete_filter_manager_->BuildSummary(filtered_deletes, specs_by_id.get()));
  summary_builder().Merge(delete_filter_summary);

  ICEBERG_ASSIGN_OR_RAISE(auto manifests,
                          data_merge_manager_->MergeManifests(
                              unmerged_manifests, metadata_to_update, writer_factory));
  ICEBERG_ASSIGN_OR_RAISE(
      auto merged_delete_manifests,
      delete_merge_manager_->MergeManifests(unmerged_delete_manifests, metadata_to_update,
                                            writer_factory));
  manifests.insert(manifests.end(),
                   std::make_move_iterator(merged_delete_manifests.begin()),
                   std::make_move_iterator(merged_delete_manifests.end()));

  // Replaced manifests come from the two filters and the two merge managers.
  // Rewritten append manifests are new manifests (copies), not replaced ones.
  int32_t replaced_manifests_count = data_filter_manager_->ReplacedManifestsCount() +
                                     delete_filter_manager_->ReplacedManifestsCount() +
                                     data_merge_manager_->ReplacedManifestsCount() +
                                     delete_merge_manager_->ReplacedManifestsCount();
  summary_builder().Merge(BuildManifestCountSummary(manifests, replaced_manifests_count));
  return manifests;
}

Status MergingSnapshotUpdate::CleanUncommitted(
    const std::unordered_set<std::string>& committed) {
  ICEBERG_RETURN_UNEXPECTED(ManagersReady());
  ICEBERG_RETURN_UNEXPECTED(data_merge_manager_->CleanUncommitted(committed));
  ICEBERG_RETURN_UNEXPECTED(data_filter_manager_->CleanUncommitted(committed));
  ICEBERG_RETURN_UNEXPECTED(delete_merge_manager_->CleanUncommitted(committed));
  ICEBERG_RETURN_UNEXPECTED(delete_filter_manager_->CleanUncommitted(committed));
  ICEBERG_RETURN_UNEXPECTED(CleanUncommittedAppends(committed));
  return {};
}

Status MergingSnapshotUpdate::CleanUncommittedAppends(
    const std::unordered_set<std::string>& committed) {
  ICEBERG_RETURN_UNEXPECTED(
      DeleteUncommitted(cached_new_data_manifests_, committed, /*clear=*/true));
  ICEBERG_RETURN_UNEXPECTED(
      DeleteUncommitted(cached_new_delete_manifests_, committed, /*clear=*/true));
  // rewritten_append_manifests_ are always owned by the table.
  ICEBERG_RETURN_UNEXPECTED(
      DeleteUncommitted(rewritten_append_manifests_, committed, /*clear=*/true));
  if (committed.empty()) {
    std::unordered_set<std::string> deleted_merged_dv_paths;
    for (const auto& dv : merged_dvs_) {
      if (deleted_merged_dv_paths.insert(dv.file->file_path).second) {
        std::ignore = DeleteFile(dv.file->file_path);
      }
    }
  }
  merged_dvs_.clear();

  // append_manifests_ are only owned by the table if the commit succeeded.
  if (!committed.empty()) {
    ICEBERG_RETURN_UNEXPECTED(
        DeleteUncommitted(append_manifests_, committed, /*clear=*/false));
  }

  has_new_data_files_ = false;
  has_new_delete_files_ = false;
  return {};
}

Result<std::vector<ManifestFile>> MergingSnapshotUpdate::PrepareNewDataManifests() {
  ICEBERG_RETURN_UNEXPECTED(EnsureRewrittenAppendManifests());
  std::vector<ManifestFile> new_manifests;
  if (!new_data_files_by_spec_.empty()) {
    ICEBERG_ASSIGN_OR_RAISE(new_manifests, NewDataFilesAsManifests());
  }
  new_manifests.insert(new_manifests.end(), append_manifests_.begin(),
                       append_manifests_.end());
  new_manifests.insert(new_manifests.end(), rewritten_append_manifests_.begin(),
                       rewritten_append_manifests_.end());
  for (auto& manifest : new_manifests) {
    manifest.added_snapshot_id = SnapshotId();
  }
  return new_manifests;
}

Result<std::vector<ManifestFile>> MergingSnapshotUpdate::NewDataFilesAsManifests() {
  // If new files were staged after the cache was populated (commit retry), invalidate.
  if (has_new_data_files_ && !cached_new_data_manifests_.empty()) {
    for (const auto& m : cached_new_data_manifests_) {
      std::ignore = DeleteFile(m.manifest_path);
    }
    cached_new_data_manifests_.clear();
  }

  if (!cached_new_data_manifests_.empty()) {
    return cached_new_data_manifests_;
  }

  for (const auto& [spec_id, data_files] : new_data_files_by_spec_) {
    ICEBERG_ASSIGN_OR_RAISE(auto spec, base().PartitionSpecById(spec_id));
    ICEBERG_ASSIGN_OR_RAISE(
        auto written,
        WriteDataManifests(data_files.as_span(), spec, new_data_files_data_seq_number_));
    cached_new_data_manifests_.insert(cached_new_data_manifests_.end(),
                                      std::make_move_iterator(written.begin()),
                                      std::make_move_iterator(written.end()));
  }

  has_new_data_files_ = false;
  return cached_new_data_manifests_;
}

Result<std::vector<ManifestFile>> MergingSnapshotUpdate::PrepareDeleteManifests() {
  if (!AddsDeleteFiles()) {
    return std::vector<ManifestFile>{};
  }
  return NewDeleteFilesAsManifests();
}

Result<std::vector<ManifestFile>> MergingSnapshotUpdate::NewDeleteFilesAsManifests() {
  // If new files were staged after the cache was populated (commit retry), invalidate.
  if (has_new_delete_files_ && !cached_new_delete_manifests_.empty()) {
    for (const auto& m : cached_new_delete_manifests_) {
      std::ignore = DeleteFile(m.manifest_path);
    }
    cached_new_delete_manifests_.clear();
    std::unordered_set<std::string> deleted_merged_dv_paths;
    for (const auto& dv : merged_dvs_) {
      if (deleted_merged_dv_paths.insert(dv.file->file_path).second) {
        std::ignore = DeleteFile(dv.file->file_path);
      }
    }
    merged_dvs_.clear();
    added_delete_files_summary_.Clear();
  }

  if (!cached_new_delete_manifests_.empty()) {
    return cached_new_delete_manifests_;
  }

  ICEBERG_ASSIGN_OR_RAISE(auto merged_dvs, MergeDVs());

  std::vector<PendingDeleteFile> new_delete_files;
  new_delete_files.reserve(merged_dvs.size() + v2_deletes_.size());
  new_delete_files.insert(new_delete_files.end(), merged_dvs.begin(), merged_dvs.end());

  DeleteFileSet v2_delete_set;
  for (const auto& pending_file : v2_deletes_) {
    if (v2_delete_set.insert(pending_file.file).second) {
      new_delete_files.push_back(pending_file);
    }
  }

  // Group delete files by partition spec ID, mirroring Java newDeleteFilesAsManifests().
  std::unordered_map<int32_t, std::vector<PendingDeleteFile>> delete_files_by_spec;
  for (const auto& pending_file : new_delete_files) {
    delete_files_by_spec[pending_file.file->partition_spec_id.value()].push_back(
        pending_file);
  }

  added_delete_files_summary_.Clear();
  for (auto& [spec_id, delete_files] : delete_files_by_spec) {
    ICEBERG_ASSIGN_OR_RAISE(auto spec, base().PartitionSpecById(spec_id));
    std::vector<ContentFileWithSequenceNumber> delete_entries;
    delete_entries.reserve(delete_files.size());
    for (const auto& pending_file : delete_files) {
      ICEBERG_RETURN_UNEXPECTED(
          added_delete_files_summary_.AddedFile(*spec, *pending_file.file));
      delete_entries.push_back(ContentFileWithSequenceNumber{
          .file = pending_file.file,
          .data_sequence_number = pending_file.data_sequence_number,
      });
    }
    ICEBERG_ASSIGN_OR_RAISE(auto written, WriteDeleteManifests(delete_entries, spec));
    cached_new_delete_manifests_.insert(cached_new_delete_manifests_.end(),
                                        std::make_move_iterator(written.begin()),
                                        std::make_move_iterator(written.end()));
  }

  has_new_delete_files_ = false;
  return cached_new_delete_manifests_;
}

Result<std::vector<MergingSnapshotUpdate::PendingDeleteFile>>
MergingSnapshotUpdate::MergeDVs() {
  for (const auto& entry : dvs_by_referenced_file_.entries()) {
    if (entry.files.size() > 1) {
      ICEBERG_LOG_WARN("Merging {} duplicate DVs for data file {} in table {}.",
                       entry.files.size(), entry.referenced_file, table_name_);
    }
  }
  // Java allocates an attempt name even when no duplicate DV requires a write.
  ICEBERG_ASSIGN_OR_RAISE(auto location_provider, ctx_->NewLocationProvider());
  auto output_path = location_provider->NewDataLocation(
      std::format("merged-dvs-{}-{}.puffin", SnapshotId(), ++dv_merge_attempt_));

  std::vector<PendingDeleteFile> result;
  result.reserve(dvs_by_referenced_file_.size());
  std::vector<DeletionVectorMergeGroup> groups;

  for (const auto& entry : dvs_by_referenced_file_.entries()) {
    const auto& referenced_file = entry.referenced_file;
    const auto& dvs = entry.files;
    if (dvs.empty()) {
      continue;
    }
    if (dvs.size() == 1) {
      result.push_back(dvs.front());
      continue;
    }

    const auto& first_file = dvs.front().file;
    const auto first_data_sequence_number = dvs.front().data_sequence_number;
    const auto first_spec_id = first_file->partition_spec_id;
    ICEBERG_PRECHECK(first_spec_id.has_value(), "DV must have a partition spec ID: {}",
                     first_file->file_path);

    for (const auto& dv : dvs) {
      ICEBERG_PRECHECK(dv.data_sequence_number == first_data_sequence_number,
                       "Cannot merge DVs, mismatched sequence numbers for {}",
                       referenced_file);
      ICEBERG_PRECHECK(dv.file->partition_spec_id == first_spec_id,
                       "Cannot merge DVs, mismatched partition specs for {}",
                       referenced_file);
      // StructLikeEqual uses Java scalar equality, including nulls, canonical NaNs,
      // and distinct signed zeros, matching partition comparator equality.
      ICEBERG_ASSIGN_OR_RAISE(auto same_partition,
                              StructLikeEqual(dv.file->partition, first_file->partition));
      ICEBERG_PRECHECK(same_partition,
                       "Cannot merge DVs, mismatched partition tuples for {}",
                       referenced_file);
    }

    ICEBERG_ASSIGN_OR_RAISE(auto spec, base().PartitionSpecById(*first_spec_id));
    DeletionVectorMergeGroup group{
        .referenced_data_file = referenced_file,
        .spec = std::move(spec),
        .partition = first_file->partition,
    };
    group.delete_files.reserve(dvs.size());
    for (const auto& dv : dvs) {
      group.delete_files.push_back(dv.file);
    }
    groups.push_back(std::move(group));
  }

  if (groups.empty()) {
    return result;
  }

  RegisterStagedFile(output_path);
  auto merged_files =
      DVUtil::MergeAndWriteDVs(groups, output_path, ctx_->table->io(), plan_executor());
  if (!merged_files) {
    std::ignore = DeleteFile(output_path);
    return ::iceberg::unexpected<Error>(std::move(merged_files.error()));
  }

  std::unordered_map<std::string, std::shared_ptr<DataFile>> merged_by_ref;
  for (auto& file : merged_files.value()) {
    ICEBERG_PRECHECK(file != nullptr, "Merged DV file must not be null");
    ICEBERG_PRECHECK(file->referenced_data_file.has_value(),
                     "Merged DV must have a referenced data file: {}", file->file_path);
    const auto referenced_data_file = *file->referenced_data_file;
    auto insert_result = merged_by_ref.emplace(referenced_data_file, std::move(file));
    ICEBERG_PRECHECK(insert_result.second, "Duplicate merged DV for {}",
                     referenced_data_file);
  }
  ICEBERG_PRECHECK(merged_by_ref.size() == groups.size(),
                   "Expected {} merged DVs but got {}", groups.size(),
                   merged_by_ref.size());

  for (size_t pos = 0; pos < groups.size(); ++pos) {
    auto iter = merged_by_ref.find(groups[pos].referenced_data_file);
    ICEBERG_CHECK(iter != merged_by_ref.end(), "Missing merged DV for {}",
                  groups[pos].referenced_data_file);
    // A newly written merged DV inherits this commit's sequence number, as in
    // DVUtil.writeDVs; unchanged single DVs retain their explicit sequence number.
    PendingDeleteFile merged{.file = std::move(iter->second),
                             .data_sequence_number = std::nullopt};
    merged_dvs_.push_back(merged);
    result.push_back(std::move(merged));
  }

  // The operation-specific merged DV cache now owns cleanup for this file.
  UnregisterStagedFile(output_path);

  return result;
}

Status MergingSnapshotUpdate::EnsureRewrittenAppendManifests() {
  // C++ transactions clean failed staging attempts before replay. Recreate copies
  // from their originals after that cleanup; Java retains them across commit retries.
  if (rewritten_append_manifests_.size() == append_manifests_to_copy_.size()) {
    return {};
  }
  if (rewritten_append_manifests_.empty()) {
    appended_manifests_summary_.Clear();
    for (const auto& manifest : append_manifests_) {
      appended_manifests_summary_.AddedManifest(manifest);
    }
  }
  while (rewritten_append_manifests_.size() < append_manifests_to_copy_.size()) {
    const auto& manifest = append_manifests_to_copy_[rewritten_append_manifests_.size()];
    ICEBERG_ASSIGN_OR_RAISE(auto copied_manifest, CopyManifest(manifest));
    rewritten_append_manifests_.push_back(std::move(copied_manifest));
  }
  return {};
}

Status MergingSnapshotUpdate::DeleteUncommitted(
    std::vector<ManifestFile>& manifests,
    const std::unordered_set<std::string>& committed, bool clear) {
  bool any_deleted = false;
  for (const auto& manifest : manifests) {
    if (!committed.contains(manifest.manifest_path)) {
      std::ignore = DeleteFile(manifest.manifest_path);
      any_deleted = true;
    }
  }
  if (clear && any_deleted) {
    manifests.clear();
  }
  return {};
}

Status MergingSnapshotUpdate::ManagersReady() const {
  ICEBERG_CHECK(data_filter_manager_ != nullptr,
                "Data filter manager is not initialized");
  ICEBERG_CHECK(delete_filter_manager_ != nullptr,
                "Delete filter manager is not initialized");
  ICEBERG_CHECK(data_merge_manager_ != nullptr, "Data merge manager is not initialized");
  ICEBERG_CHECK(delete_merge_manager_ != nullptr,
                "Delete merge manager is not initialized");
  return {};
}

ManifestWriterFactory MergingSnapshotUpdate::MakeWriterFactory(
    const std::shared_ptr<Schema>& schema) {
  // Resolve lazy snapshot state before the managers invoke writers in parallel.
  const int64_t snapshot_id = SnapshotId();
  return [this, schema, snapshot_id](
             int32_t spec_id,
             ManifestContent content) -> Result<std::unique_ptr<ManifestWriter>> {
    const TableMetadata& current = base();
    ICEBERG_ASSIGN_OR_RAISE(auto spec, current.PartitionSpecById(spec_id));
    return ManifestWriter::MakeWriter(current.format_version, snapshot_id, ManifestPath(),
                                      ctx_->table->io(), std::move(spec), schema,
                                      content);
  };
}

void MergingSnapshotUpdate::PendingDeleteFilesByReferencedFile::Add(
    std::string referenced_file, PendingDeleteFile file) {
  auto [iter, inserted] =
      index_by_referenced_file_.try_emplace(referenced_file, entries_.size());
  if (inserted) {
    entries_.push_back(Entry{.referenced_file = std::move(referenced_file), .files = {}});
  }
  entries_[iter->second].files.push_back(std::move(file));
}

}  // namespace iceberg
