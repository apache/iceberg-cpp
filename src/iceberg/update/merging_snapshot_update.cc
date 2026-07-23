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
#include "iceberg/expression/expressions.h"
#include "iceberg/expression/manifest_evaluator.h"
#include "iceberg/expression/projections.h"
#include "iceberg/file_io.h"
#include "iceberg/location_provider.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_group.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/manifest/manifest_util_internal.h"
#include "iceberg/manifest/manifest_writer.h"
#include "iceberg/partition_spec.h"
#include "iceberg/puffin_dv_io.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"
#include "iceberg/transaction.h"
#include "iceberg/util/content_file_util.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/snapshot_util_internal.h"
#include "iceberg/util/struct_like_set.h"

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

Result<std::vector<std::shared_ptr<Snapshot>>> ValidationAncestorsBetween(
    const TableMetadata& metadata, int64_t latest_snapshot_id,
    std::optional<int64_t> starting_snapshot_id) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto ancestors,
      SnapshotUtil::AncestorsBetween(metadata, latest_snapshot_id, starting_snapshot_id));
  if (!starting_snapshot_id.has_value()) {
    if (!ancestors.empty()) {
      const auto& oldest_checked = ancestors.back();
      if (oldest_checked == nullptr || oldest_checked->parent_snapshot_id.has_value()) {
        return ValidationFailed(
            "Cannot validate history: cannot determine complete history for snapshot {}",
            latest_snapshot_id);
      }
    }
    return ancestors;
  }

  if (latest_snapshot_id == starting_snapshot_id.value()) {
    return ancestors;
  }
  if (ancestors.empty()) {
    return ValidationFailed(
        "Cannot validate history: starting snapshot {} is not an ancestor "
        "of snapshot {}",
        starting_snapshot_id.value(), latest_snapshot_id);
  }

  const auto& oldest_checked = ancestors.back();
  if (oldest_checked == nullptr || !oldest_checked->parent_snapshot_id.has_value() ||
      oldest_checked->parent_snapshot_id.value() != starting_snapshot_id.value()) {
    return ValidationFailed(
        "Cannot validate history: starting snapshot {} is not an ancestor "
        "of snapshot {}",
        starting_snapshot_id.value(), latest_snapshot_id);
  }
  return ancestors;
}

Result<ValidationHistoryResult> ValidationHistory(
    const TableMetadata& metadata, int64_t latest_snapshot_id,
    std::optional<int64_t> starting_snapshot_id,
    std::span<const std::string_view> matching_operations, ManifestContent content,
    const std::shared_ptr<FileIO>& io) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto ancestors,
      ValidationAncestorsBetween(metadata, latest_snapshot_id, starting_snapshot_id));

  ValidationHistoryResult result;
  for (const auto& snapshot : ancestors) {
    if (!MatchesOperation(snapshot->Operation(), matching_operations)) {
      continue;
    }

    result.snapshot_ids.insert(snapshot->snapshot_id);
    auto cached = SnapshotCache(snapshot.get());
    ICEBERG_ASSIGN_OR_RAISE(auto manifests, content == ManifestContent::kData
                                                ? cached.DataManifests(io)
                                                : cached.DeleteManifests(io));
    for (const auto& manifest : manifests) {
      if (manifest.added_snapshot_id == snapshot->snapshot_id) {
        result.manifests.push_back(manifest);
      }
    }
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

std::optional<std::string> DeleteFileLocations(
    const std::vector<std::shared_ptr<DataFile>>& delete_files) {
  std::vector<std::string> paths;
  paths.reserve(delete_files.size());
  for (const auto& delete_file : delete_files) {
    if (delete_file != nullptr) {
      paths.push_back(delete_file->file_path);
    }
  }
  if (paths.empty()) {
    return std::optional<std::string>{};
  }
  return FormatLocations(std::move(paths));
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
      [this](const std::string& location) { return DeleteFile(location); });
  if (!data_filter_manager.has_value()) {
    AddError(data_filter_manager.error());
  } else {
    data_filter_manager_ = std::move(data_filter_manager.value());
  }

  auto delete_filter_manager = ManifestFilterManager::Make(
      ManifestContent::kDeletes, file_io,
      [this](const std::string& location) { return DeleteFile(location); });
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
      [this](const std::string& location) { return DeleteFile(location); });
  if (!data_merge_manager.has_value()) {
    AddError(data_merge_manager.error());
  } else {
    data_merge_manager_ = std::move(data_merge_manager.value());
  }

  auto delete_merge_manager = ManifestMergeManager::Make(
      ManifestContent::kDeletes, target_size_bytes, min_count_to_merge, merge_enabled,
      file_io, [this] { return SnapshotId(); },
      [this](const std::string& location) { return DeleteFile(location); });
  if (!delete_merge_manager.has_value()) {
    AddError(delete_merge_manager.error());
  } else {
    delete_merge_manager_ = std::move(delete_merge_manager.value());
  }
}

// -------------------------------------------------------------------------
// Primitive API
// -------------------------------------------------------------------------

Status MergingSnapshotUpdate::AddDataFile(std::shared_ptr<DataFile> file) {
  if (!file) {
    return InvalidArgument("Cannot add a null data file");
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

Status MergingSnapshotUpdate::AddDeleteFile(std::shared_ptr<DataFile> file) {
  return AddDeleteFile(std::move(file), std::nullopt);
}

Status MergingSnapshotUpdate::AddDeleteFile(std::shared_ptr<DataFile> file,
                                            int64_t data_sequence_number) {
  return AddDeleteFile(std::move(file), std::optional<int64_t>(data_sequence_number));
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

void MergingSnapshotUpdate::FailMissingDeletePaths() {
  data_filter_manager_->FailMissingDeletePaths();
  delete_filter_manager_->FailMissingDeletePaths();
}

void MergingSnapshotUpdate::FailAnyDelete() {
  data_filter_manager_->FailAnyDelete();
  delete_filter_manager_->FailAnyDelete();
}

void MergingSnapshotUpdate::SetNewDataFilesDataSequenceNumber(int64_t sequence_number) {
  new_data_files_data_seq_number_ = sequence_number;
}

void MergingSnapshotUpdate::CaseSensitive(bool case_sensitive) {
  case_sensitive_ = case_sensitive;
  data_filter_manager_->CaseSensitive(case_sensitive);
  delete_filter_manager_->CaseSensitive(case_sensitive);
}

void MergingSnapshotUpdate::SetSummaryProperty(const std::string& property,
                                               const std::string& value) {
  custom_summary_properties_[property] = value;
  SnapshotUpdate::SetSummaryProperty(property, value);
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

Status MergingSnapshotUpdate::AddManifest(ManifestFile manifest) {
  if (manifest.content != ManifestContent::kData) {
    return InvalidArgument("Cannot append delete manifest: {}", manifest.manifest_path);
  }
  if (can_inherit_snapshot_id() && manifest.added_snapshot_id == kInvalidSnapshotId) {
    if (manifest.first_row_id.has_value()) {
      return InvalidArgument("Cannot append manifest with assigned first row ID: {}",
                             manifest.manifest_path);
    }
    appended_manifests_summary_.AddedManifest(manifest);
    append_manifests_.push_back(std::move(manifest));
  } else {
    ICEBERG_ASSIGN_OR_RAISE(auto copied, CopyManifest(manifest, /*update_summary=*/true));
    append_manifests_to_copy_.push_back(std::move(manifest));
    rewritten_append_manifests_.push_back(std::move(copied));
  }
  return {};
}

Result<ManifestFile> MergingSnapshotUpdate::CopyManifest(const ManifestFile& manifest,
                                                         bool update_summary) {
  const TableMetadata& current = base();
  ICEBERG_ASSIGN_OR_RAISE(auto schema, SnapshotUtil::SchemaFor(current, target_branch()));
  ICEBERG_ASSIGN_OR_RAISE(auto spec,
                          current.PartitionSpecById(manifest.partition_spec_id));
  std::string path = ManifestPath();
  return CopyAppendManifest(manifest, ctx_->table->io(), schema, spec, SnapshotId(), path,
                            current.format_version,
                            update_summary ? &appended_manifests_summary_ : nullptr);
}

// -------------------------------------------------------------------------
// State queries
// -------------------------------------------------------------------------

bool MergingSnapshotUpdate::AddsDataFiles() const {
  return !new_data_files_by_spec_.empty();
}

bool MergingSnapshotUpdate::AddsDeleteFiles() const {
  return !v2_deletes_.empty() || !dvs_by_referenced_file_.empty();
}

bool MergingSnapshotUpdate::DeletesDataFiles() const {
  return data_filter_manager_->ContainsDeletes();
}

bool MergingSnapshotUpdate::DeletesDeleteFiles() const {
  return delete_filter_manager_->ContainsDeletes();
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

// -------------------------------------------------------------------------
// Apply pipeline
// -------------------------------------------------------------------------

ManifestWriterFactory MergingSnapshotUpdate::MakeWriterFactory(
    const std::shared_ptr<Schema>& schema) {
  return
      [this, schema](int32_t spec_id,
                     ManifestContent content) -> Result<std::unique_ptr<ManifestWriter>> {
        const TableMetadata& meta = base();
        ICEBERG_ASSIGN_OR_RAISE(auto spec, meta.PartitionSpecById(spec_id));
        std::string path = ManifestPath();
        return ManifestWriter::MakeWriter(meta.format_version, SnapshotId(),
                                          std::move(path), ctx_->table->io(),
                                          std::move(spec), schema, content);
      };
}

Result<std::vector<ManifestFile>> MergingSnapshotUpdate::WriteNewDataManifests() {
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

  std::vector<ManifestFile> result;
  for (const auto& [spec_id, data_files] : new_data_files_by_spec_) {
    ICEBERG_ASSIGN_OR_RAISE(auto spec, base().PartitionSpecById(spec_id));
    ICEBERG_ASSIGN_OR_RAISE(
        auto written,
        WriteDataManifests(data_files.as_span(), spec, new_data_files_data_seq_number_));
    result.insert(result.end(), std::make_move_iterator(written.begin()),
                  std::make_move_iterator(written.end()));
  }

  cached_new_data_manifests_ = result;
  has_new_data_files_ = false;
  return result;
}

Result<std::vector<MergingSnapshotUpdate::PendingDeleteFile>>
MergingSnapshotUpdate::MergeDVs() {
  std::vector<PendingDeleteFile> result;
  result.reserve(dvs_by_referenced_file_.size());
  std::vector<DeletionVectorMergeGroup> groups;
  std::vector<std::optional<int64_t>> data_sequence_numbers;

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
      // TODO(gangwu): Java DVUtil.validateCanMerge compares partitions with
      // Comparators.forType(spec.partitionType()). Use a typed C++ comparator if
      // StructLikeEqual's scalar equality is not enough for future partition types.
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
    data_sequence_numbers.push_back(first_data_sequence_number);
  }

  if (groups.empty()) {
    return result;
  }

  ICEBERG_ASSIGN_OR_RAISE(auto location_provider, ctx_->NewLocationProvider());
  auto output_path = location_provider->NewDataLocation(
      std::format("merged-dvs-{}-{}.puffin", SnapshotId(), ++dv_merge_attempt_));

  auto merged_files =
      PuffinDVIORegistry::MergeAndWriteDVs(groups, output_path, ctx_->table->io());
  if (!merged_files) {
    std::ignore = DeleteFile(output_path);
    return std::unexpected<Error>(std::move(merged_files.error()));
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
    PendingDeleteFile merged{.file = std::move(iter->second),
                             .data_sequence_number = data_sequence_numbers[pos]};
    merged_dvs_.push_back(merged);
    result.push_back(std::move(merged));
  }

  return result;
}

Result<std::vector<ManifestFile>> MergingSnapshotUpdate::WriteNewDeleteManifests() {
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

  std::vector<ManifestFile> result;
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
    result.insert(result.end(), std::make_move_iterator(written.begin()),
                  std::make_move_iterator(written.end()));
  }

  cached_new_delete_manifests_ = result;
  has_new_delete_files_ = false;
  return result;
}

Result<std::vector<ManifestFile>> MergingSnapshotUpdate::Apply(
    const TableMetadata& metadata_to_update, const std::shared_ptr<Snapshot>& snapshot) {
  ICEBERG_RETURN_UNEXPECTED(ManagersReady());

  // Re-validate buffered delete files against the current format version. A format
  // upgrade between staging and commit could make previously-valid files invalid.
  for (const auto& pending_file : v2_deletes_) {
    ICEBERG_RETURN_UNEXPECTED(
        ValidateNewDeleteFile(metadata_to_update, *pending_file.file));
  }
  for (const auto& entry : dvs_by_referenced_file_.entries()) {
    for (const auto& pending_file : entry.files) {
      ICEBERG_RETURN_UNEXPECTED(
          ValidateNewDeleteFile(metadata_to_update, *pending_file.file));
    }
  }

  ICEBERG_ASSIGN_OR_RAISE(auto target_schema,
                          SnapshotUtil::SchemaFor(metadata_to_update, target_branch()));
  auto writer_factory = MakeWriterFactory(target_schema);

  // Step 1: Filter data manifests.
  ICEBERG_ASSIGN_OR_RAISE(auto filtered_data, data_filter_manager_->FilterManifests(
                                                  target_schema, metadata_to_update,
                                                  snapshot, writer_factory));

  // Step 2: Compute min data sequence number; set up delete filter cleanup.
  // Skip unassigned manifests written in this Apply() call.
  int64_t min_data_seq = metadata_to_update.last_sequence_number;
  for (const auto& manifest : filtered_data) {
    if (manifest.min_sequence_number != kUnassignedSequenceNumber) {
      min_data_seq = std::min(min_data_seq, manifest.min_sequence_number);
    }
  }
  ICEBERG_RETURN_UNEXPECTED(
      delete_filter_manager_->DropDeleteFilesOlderThan(min_data_seq));
  delete_filter_manager_->RemoveDanglingDeletesFor(
      data_filter_manager_->FilesToBeDeleted());

  // Step 3: Filter delete manifests.
  ICEBERG_ASSIGN_OR_RAISE(auto filtered_deletes, delete_filter_manager_->FilterManifests(
                                                     target_schema, metadata_to_update,
                                                     snapshot, writer_factory));

  TableMetadataCache metadata_cache(&metadata_to_update);
  ICEBERG_ASSIGN_OR_RAISE(auto specs_by_id, metadata_cache.GetPartitionSpecsById());
  ICEBERG_ASSIGN_OR_RAISE(
      auto data_filter_summary,
      data_filter_manager_->BuildSummary(filtered_data, specs_by_id.get()));
  ICEBERG_ASSIGN_OR_RAISE(
      auto delete_filter_summary,
      delete_filter_manager_->BuildSummary(filtered_deletes, specs_by_id.get()));

  // Drop manifests with no live files - they carry no data and should not be merged
  // into the new snapshot. Manifests written by the current snapshot are always kept
  // regardless of live-file counts; the merge stage handles any that are empty.
  int64_t snapshot_id = SnapshotId();
  auto should_keep = [snapshot_id](const ManifestFile& m) {
    return m.has_added_files() || m.has_existing_files() ||
           m.added_snapshot_id == snapshot_id;
  };

  // Step 4: Write (or retrieve cached) new data manifests.
  ICEBERG_ASSIGN_OR_RAISE(auto written_data_manifests, WriteNewDataManifests());
  if (rewritten_append_manifests_.empty() && !append_manifests_to_copy_.empty()) {
    for (const auto& manifest : append_manifests_to_copy_) {
      ICEBERG_ASSIGN_OR_RAISE(auto copied, CopyManifest(manifest,
                                                        /*update_summary=*/false));
      rewritten_append_manifests_.push_back(std::move(copied));
    }
  }

  // Incorporate append manifests (from AddManifest), stamping each with the
  // current snapshot ID. append_manifests_ are used directly (inherit path);
  // rewritten_append_manifests_ were already copied with the snapshot ID.
  std::vector<ManifestFile> new_data_manifests = std::move(written_data_manifests);
  for (const auto& src : append_manifests_) {
    ManifestFile m = src;
    m.added_snapshot_id = snapshot_id;
    new_data_manifests.push_back(std::move(m));
  }
  for (const auto& src : rewritten_append_manifests_) {
    ManifestFile m = src;
    m.added_snapshot_id = snapshot_id;
    new_data_manifests.push_back(std::move(m));
  }

  // Step 5: Write (or retrieve cached) new delete manifests.
  ICEBERG_ASSIGN_OR_RAISE(auto new_delete_manifests, WriteNewDeleteManifests());

  std::erase_if(new_data_manifests,
                [&](const ManifestFile& m) { return !should_keep(m); });
  std::erase_if(filtered_data, [&](const ManifestFile& m) { return !should_keep(m); });
  std::erase_if(new_delete_manifests,
                [&](const ManifestFile& m) { return !should_keep(m); });
  std::erase_if(filtered_deletes, [&](const ManifestFile& m) { return !should_keep(m); });

  // Rebuild summary from stable sub-builders so that commit retries don't double-count.
  summary_builder().Clear();
  summary_builder().Merge(added_data_files_summary_);
  summary_builder().Merge(added_delete_files_summary_);
  summary_builder().Merge(appended_manifests_summary_);
  for (const auto& [property, value] : custom_summary_properties_) {
    summary_builder().Set(property, value);
  }
  summary_builder().Merge(data_filter_summary);
  summary_builder().Merge(delete_filter_summary);

  // Step 6: Merge data manifests.
  ICEBERG_ASSIGN_OR_RAISE(auto merged_data, data_merge_manager_->MergeManifests(
                                                filtered_data, new_data_manifests,
                                                metadata_to_update, writer_factory));

  // Step 7: Merge delete manifests.
  ICEBERG_ASSIGN_OR_RAISE(auto merged_deletes, delete_merge_manager_->MergeManifests(
                                                   filtered_deletes, new_delete_manifests,
                                                   metadata_to_update, writer_factory));

  std::vector<ManifestFile> result;
  result.reserve(merged_data.size() + merged_deletes.size());
  result.insert(result.end(), std::make_move_iterator(merged_data.begin()),
                std::make_move_iterator(merged_data.end()));
  result.insert(result.end(), std::make_move_iterator(merged_deletes.begin()),
                std::make_move_iterator(merged_deletes.end()));

  int32_t replaced_manifests_count = data_filter_manager_->ReplacedManifestsCount() +
                                     delete_filter_manager_->ReplacedManifestsCount() +
                                     data_merge_manager_->ReplacedManifestsCount() +
                                     delete_merge_manager_->ReplacedManifestsCount();
  summary_builder().Merge(BuildManifestCountSummary(result, replaced_manifests_count));

  return result;
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

Status MergingSnapshotUpdate::DeleteUncommitted(
    std::vector<ManifestFile>& manifests,
    const std::unordered_set<std::string>& committed, bool clear) {
  for (const auto& manifest : manifests) {
    if (!committed.contains(manifest.manifest_path)) {
      std::ignore = DeleteFile(manifest.manifest_path);
    }
  }
  if (clear) {
    manifests.clear();
  }
  return {};
}

std::unordered_map<std::string, std::string> MergingSnapshotUpdate::Summary() {
  summary_builder().SetPartitionSummaryLimit(
      base().properties.Get(TableProperties::kWritePartitionSummaryLimit));
  return summary_builder().Build();
}

// -------------------------------------------------------------------------
// Conflict-detection helpers
// -------------------------------------------------------------------------

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

Status MergingSnapshotUpdate::ValidateDataFilesExist(
    const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id,
    const std::unordered_set<std::string>& file_paths, bool skip_deletes,
    std::shared_ptr<Expression> conflict_detection_filter,
    const std::shared_ptr<Snapshot>& parent, std::shared_ptr<FileIO> io,
    bool /*case_sensitive*/) {
  if (parent == nullptr || file_paths.empty()) {
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

Status MergingSnapshotUpdate::ValidateNoNewDeletesForDataFiles(
    const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id,
    const DataFileSet& replaced_files, const std::shared_ptr<Snapshot>& parent,
    std::shared_ptr<FileIO> io, bool ignore_equality_deletes) {
  if (parent == nullptr || replaced_files.empty() || metadata.format_version < 2) {
    return {};
  }

  ICEBERG_ASSIGN_OR_RAISE(auto deletes,
                          AddedDeleteFiles(metadata, starting_snapshot_id,
                                           /*data_filter=*/nullptr,
                                           /*partition_set=*/nullptr, parent, io));

  ICEBERG_ASSIGN_OR_RAISE(auto starting_sequence_number,
                          StartingSequenceNumber(metadata, starting_snapshot_id));

  for (const auto& data_file : replaced_files) {
    ICEBERG_ASSIGN_OR_RAISE(auto delete_files,
                            deletes->ForDataFile(starting_sequence_number, *data_file));
    if (ignore_equality_deletes) {
      // Only fail on position deletes — equality deletes at higher sequence numbers
      // still apply to the rewritten files and are not a conflict.
      for (const auto& df : delete_files) {
        if (df->content == DataFile::Content::kPositionDeletes) {
          return ValidationFailed(
              "Cannot commit, found new position delete for replaced data file: {}",
              data_file->file_path);
        }
      }
    } else {
      if (!delete_files.empty()) {
        return ValidationFailed(
            "Cannot commit, found new delete for replaced data file: {}",
            data_file->file_path);
      }
    }
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
    std::shared_ptr<Expression> data_filter, const DataFileSet& replaced_files,
    const std::shared_ptr<Snapshot>& parent, std::shared_ptr<FileIO> io,
    bool case_sensitive) {
  if (parent == nullptr || replaced_files.empty() || metadata.format_version < 2) {
    return {};
  }

  ICEBERG_ASSIGN_OR_RAISE(
      auto deletes,
      AddedDeleteFiles(metadata, starting_snapshot_id, std::move(data_filter),
                       /*partition_set=*/nullptr, parent, io, case_sensitive));

  ICEBERG_ASSIGN_OR_RAISE(auto starting_sequence_number,
                          StartingSequenceNumber(metadata, starting_snapshot_id));

  for (const auto& data_file : replaced_files) {
    ICEBERG_ASSIGN_OR_RAISE(auto delete_files,
                            deletes->ForDataFile(starting_sequence_number, *data_file));
    if (!delete_files.empty()) {
      return ValidationFailed(
          "Cannot commit, found new delete for replaced data file: {}",
          data_file->file_path);
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
  ICEBERG_ASSIGN_OR_RAISE(
      auto deletes,
      AddedDeleteFiles(metadata, starting_snapshot_id, std::move(data_filter),
                       /*partition_set=*/nullptr, parent, io, case_sensitive));
  auto referenced_delete_files = deletes->ReferencedDeleteFiles();
  auto delete_paths = DeleteFileLocations(referenced_delete_files);
  if (delete_paths.has_value()) {
    return ValidationFailed(
        "Found new conflicting delete files that can apply to records matching {}: {}",
        data_filter_text, delete_paths.value());
  }
  return {};
}

Status MergingSnapshotUpdate::ValidateNoNewDeleteFiles(
    const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id,
    const PartitionSet& partition_set, const std::shared_ptr<Snapshot>& parent,
    std::shared_ptr<FileIO> io) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto deletes,
      AddedDeleteFiles(metadata, starting_snapshot_id,
                       /*data_filter=*/nullptr,
                       std::make_shared<PartitionSet>(partition_set), parent, io));
  auto referenced_delete_files = deletes->ReferencedDeleteFiles();
  auto delete_paths = DeleteFileLocations(referenced_delete_files);
  if (delete_paths.has_value()) {
    return ValidationFailed(
        "Found new conflicting delete files that can apply to records matching "
        "validated partitions: {}",
        delete_paths.value());
  }
  return {};
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

Result<std::unique_ptr<DeleteFileIndex>> MergingSnapshotUpdate::AddedDeleteFiles(
    const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id,
    std::shared_ptr<Expression> data_filter, std::shared_ptr<PartitionSet> partition_set,
    const std::shared_ptr<Snapshot>& parent, std::shared_ptr<FileIO> io,
    bool case_sensitive) {
  if (parent == nullptr || metadata.format_version < 2) {
    ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());
    ICEBERG_ASSIGN_OR_RAISE(auto specs_by_id, PartitionSpecsByIdMap(metadata));
    ICEBERG_ASSIGN_OR_RAISE(
        auto builder,
        DeleteFileIndex::BuilderFor(io, std::move(schema), std::move(specs_by_id),
                                    /*delete_manifests=*/{}));
    return builder.Build();
  }

  ICEBERG_ASSIGN_OR_RAISE(
      auto history, ValidationHistory(metadata, parent->snapshot_id, starting_snapshot_id,
                                      kValidateAddedDeleteFilesOperations,
                                      ManifestContent::kDeletes, io));

  ICEBERG_ASSIGN_OR_RAISE(auto starting_sequence_number,
                          StartingSequenceNumber(metadata, starting_snapshot_id));
  return BuildDeleteFileIndex(metadata, io, std::move(history.manifests),
                              starting_sequence_number, std::move(data_filter),
                              std::move(partition_set), case_sensitive);
}

Status MergingSnapshotUpdate::ValidateAddedDVs(
    const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id,
    std::shared_ptr<Expression> conflict_detection_filter,
    const std::unordered_set<std::string>& referenced_data_files,
    const std::shared_ptr<Snapshot>& parent, std::shared_ptr<FileIO> io,
    bool case_sensitive) {
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
  for (const auto& manifest : matching_manifests) {
    if (!manifest.has_added_files()) {
      continue;
    }
    ICEBERG_RETURN_UNEXPECTED(ValidateAddedDVsInManifest(
        metadata, manifest, conflict_detection_filter, history.snapshot_ids,
        referenced_data_files, io, schema, case_sensitive));
  }
  return {};
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
  return ValidateAddedDVs(metadata, starting_snapshot_id, std::move(conflict_filter),
                          referenced_data_files, parent, std::move(io), case_sensitive_);
}

}  // namespace iceberg
