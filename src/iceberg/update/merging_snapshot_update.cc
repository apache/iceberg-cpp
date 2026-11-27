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
#include <iterator>
#include <optional>
#include <ranges>
#include <span>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include "iceberg/constants.h"
#include "iceberg/delete_file_index.h"
#include "iceberg/deletes/dv_util_internal.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/expression/manifest_evaluator.h"
#include "iceberg/file_io.h"
#include "iceberg/location_provider.h"
#include "iceberg/logging/log_macros.h"
#include "iceberg/manifest/manifest_entry.h"
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
#include "iceberg/util/error_util_internal.h"
#include "iceberg/util/formatter_internal.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/snapshot_util.h"
#include "iceberg/util/task_group_internal.h"

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

Status ValidateDeleteFileForVersion(const DataFile& file, int8_t version) {
  if (file.content == DataFile::Content::kData) {
    return InvalidArgument("Expected a delete file but got a data file: {}",
                           file.file_path);
  }
  switch (version) {
    case 1:
      return InvalidArgument("Deletes are supported in V2 and above");
    case 2:
      ICEBERG_PRECHECK(file.content == DataFile::Content::kEqualityDeletes ||
                           !ContentFileUtil::IsDV(file),
                       "Must not use DVs for position deletes in V2: {}", file.file_path);
      break;
    case 3:
      ICEBERG_PRECHECK(file.content == DataFile::Content::kEqualityDeletes ||
                           ContentFileUtil::IsDV(file),
                       "Must use DVs for position deletes in V3: {}", file.file_path);
      break;
    default:
      return InvalidArgument("Unsupported format version: {}", version);
  }
  return {};
}

struct ValidationHistoryResult {
  // Manifests from one snapshot are consecutive, with newest snapshots first.
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
  ValidationHistoryResult history;
  for (const auto& snapshot : ancestors) {
    auto operation = snapshot->Operation();
    if (!operation || !std::ranges::contains(matching_operations, *operation)) {
      continue;
    }

    history.snapshot_ids.insert(snapshot->snapshot_id);
    SnapshotReader reader(snapshot.get());
    ICEBERG_ASSIGN_OR_RAISE(auto manifests, content == ManifestContent::kData
                                                ? reader.DataManifests(io)
                                                : reader.DeleteManifests(io));
    for (const auto& manifest : manifests) {
      if (manifest.added_snapshot_id == snapshot->snapshot_id) {
        history.manifests.push_back(manifest);
      }
    }
  }
  if (!ancestors.empty() &&
      ancestors.back()->parent_snapshot_id != starting_snapshot_id) {
    return ValidationFailed(
        "Cannot determine history between starting snapshot {} and the last known "
        "ancestor {}",
        starting_snapshot_id ? std::to_string(*starting_snapshot_id) : "null",
        ancestors.back()->snapshot_id);
  }
  return history;
}

Result<std::vector<ManifestFile>> FilterManifestsByPartition(
    const TableMetadata& metadata, const std::shared_ptr<Schema>& schema,
    const std::shared_ptr<Expression>& filter, std::vector<ManifestFile> manifests,
    bool case_sensitive) {
  if (!filter || filter->op() == Expression::Operation::kTrue) {
    return manifests;
  }

  std::unordered_map<int32_t, std::unique_ptr<ManifestEvaluator>> evaluators;
  std::vector<ManifestFile> matching;
  for (auto& manifest : manifests) {
    auto& evaluator = evaluators[manifest.partition_spec_id];
    if (!evaluator) {
      ICEBERG_ASSIGN_OR_RAISE(auto spec,
                              metadata.PartitionSpecById(manifest.partition_spec_id));
      ICEBERG_ASSIGN_OR_RAISE(evaluator, ManifestEvaluator::MakeRowFilter(
                                             filter, spec, *schema, case_sensitive));
    }

    ICEBERG_ASSIGN_OR_RAISE(bool matches, evaluator->Evaluate(manifest));
    if (matches) {
      matching.push_back(std::move(manifest));
    }
  }
  return matching;
}

std::string FormatValidationPartitions(const TableMetadata& metadata,
                                       const PartitionSet& partitions) {
  std::vector<std::string> tuples;
  for (const auto& [spec_id, partition] : partitions) {
    auto spec = metadata.PartitionSpecById(spec_id);
    if (!spec || (*spec)->fields().size() != partition.num_fields()) {
      tuples.push_back(std::format("{{spec_id={}, values={}}}", spec_id,
                                   FormatRange(partition.values(), ", ", "[", "]")));
      continue;
    }
    std::vector<std::string> fields;
    for (const auto& [field, value] :
         std::views::zip((*spec)->fields(), partition.values())) {
      const auto* text = std::get_if<std::string>(&value.value());
      fields.push_back(
          std::format("{}={}", field.name(), text ? *text : value.ToString()));
    }
    tuples.push_back(FormatRange(fields, ", ", "{", "}"));
  }
  return FormatRange(tuples, ", ", "[", "]");
}

Result<std::vector<std::string>> MatchingDataFileLocations(
    const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id,
    const std::shared_ptr<Snapshot>& parent, const std::shared_ptr<FileIO>& io,
    std::span<const std::string_view> operations, ManifestStatus status,
    const std::shared_ptr<Expression>& filter, bool case_sensitive,
    const PartitionSet* partitions = nullptr,
    const std::unordered_set<std::string>* required_paths = nullptr) {
  if (!parent) {
    return {};
  }
  ICEBERG_ASSIGN_OR_RAISE(
      auto history, ValidationHistory(metadata, parent->snapshot_id, starting_snapshot_id,
                                      operations, ManifestContent::kData, io));
  if (history.manifests.empty()) {
    return {};
  }
  ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());
  ICEBERG_ASSIGN_OR_RAISE(
      auto manifests,
      FilterManifestsByPartition(metadata, schema, filter, std::move(history.manifests),
                                 case_sensitive));

  std::vector<std::string> locations;
  for (const auto& manifest : manifests) {
    const bool has_changes = manifest.has_added_files() || manifest.has_deleted_files();
    const bool has_live_files =
        manifest.has_added_files() || manifest.has_existing_files();
    if (!has_changes || (status == ManifestStatus::kAdded && !has_live_files)) {
      continue;
    }
    ICEBERG_ASSIGN_OR_RAISE(auto spec,
                            metadata.PartitionSpecById(manifest.partition_spec_id));
    ICEBERG_ASSIGN_OR_RAISE(auto reader,
                            ManifestReader::Make(manifest, io, schema, spec));
    reader->CaseSensitive(case_sensitive)
        .FilterRows(filter)
        .Select({"file_path", DataFile::kPartitionField});
    ICEBERG_ASSIGN_OR_RAISE(auto entries, status == ManifestStatus::kAdded
                                              ? reader->LiveEntriesStream()
                                              : reader->EntriesStream());
    while (true) {
      ICEBERG_ASSIGN_OR_RAISE(auto entry, entries->Next());
      if (!entry) {
        break;
      }
      if (entry->status != status || !entry->snapshot_id ||
          !history.snapshot_ids.contains(*entry->snapshot_id)) {
        continue;
      }
      auto& file = *entry->data_file;
      if ((!partitions ||
           partitions->contains(manifest.partition_spec_id, file.partition)) &&
          (!required_paths || required_paths->contains(file.file_path))) {
        locations.push_back(std::move(file.file_path));
      }
    }
  }
  return locations;
}

int64_t StartingSequenceNumber(const TableMetadata& metadata,
                               std::optional<int64_t> starting_snapshot_id) {
  if (starting_snapshot_id) {
    if (auto snapshot = metadata.SnapshotById(*starting_snapshot_id)) {
      return (*snapshot)->sequence_number;
    }
  }
  return TableMetadata::kInitialSequenceNumber;
}

Result<std::unique_ptr<DeleteFileIndex>> BuildDeleteFileIndex(
    const std::shared_ptr<Schema>& schema,
    const TableMetadataCache::PartitionSpecsMap& specs_by_id,
    const std::shared_ptr<FileIO>& io, std::vector<ManifestFile> manifests,
    int64_t starting_sequence_number, const std::shared_ptr<Expression>& filter,
    const std::shared_ptr<PartitionSet>& partitions, bool case_sensitive) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto builder,
      DeleteFileIndex::BuilderFor(io, schema, specs_by_id, std::move(manifests)));
  builder.AfterSequenceNumber(starting_sequence_number).CaseSensitive(case_sensitive);
  if (filter) {
    builder.DataFilter(filter);
  }
  if (partitions) {
    builder.FilterPartitions(partitions);
  }
  return builder.Build();
}

Result<std::vector<std::unique_ptr<DeleteFileIndex>>> AddedDeleteFilesIndexedPerSnapshot(
    const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id,
    const std::shared_ptr<Expression>& filter,
    const std::shared_ptr<PartitionSet>& partitions,
    const std::shared_ptr<Snapshot>& parent, const std::shared_ptr<FileIO>& io,
    bool case_sensitive) {
  if (!parent || metadata.format_version < 2) {
    return {};
  }
  ICEBERG_ASSIGN_OR_RAISE(
      auto history, ValidationHistory(metadata, parent->snapshot_id, starting_snapshot_id,
                                      kValidateAddedDeleteFilesOperations,
                                      ManifestContent::kDeletes, io));
  if (history.manifests.empty()) {
    return {};
  }

  ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());
  TableMetadataCache cache(&metadata);
  ICEBERG_ASSIGN_OR_RAISE(auto specs_by_id, cache.GetPartitionSpecsById());
  const auto starting_sequence_number =
      StartingSequenceNumber(metadata, starting_snapshot_id);
  // A data file can have a replacement DV in each snapshot in the validation window.
  auto same_snapshot = [](const ManifestFile& left, const ManifestFile& right) {
    return left.added_snapshot_id == right.added_snapshot_id;
  };
  std::vector<std::unique_ptr<DeleteFileIndex>> indexes;
  for (auto group : history.manifests | std::views::chunk_by(same_snapshot)) {
    std::vector<ManifestFile> manifests(std::make_move_iterator(group.begin()),
                                        std::make_move_iterator(group.end()));
    ICEBERG_ASSIGN_OR_RAISE(
        auto index, BuildDeleteFileIndex(schema, specs_by_id.get(), io,
                                         std::move(manifests), starting_sequence_number,
                                         filter, partitions, case_sensitive));
    indexes.push_back(std::move(index));
  }
  return indexes;
}

Status CheckNewDeletesForDataFiles(const TableMetadata& metadata,
                                   std::optional<int64_t> starting_snapshot_id,
                                   const std::shared_ptr<Expression>& filter,
                                   const DataFileSet& data_files,
                                   const std::shared_ptr<Snapshot>& parent,
                                   const std::shared_ptr<FileIO>& io,
                                   bool ignore_equality_deletes, bool case_sensitive) {
  ICEBERG_ASSIGN_OR_RAISE(auto indexes, AddedDeleteFilesIndexedPerSnapshot(
                                            metadata, starting_snapshot_id, filter,
                                            nullptr, parent, io, case_sensitive));
  if (indexes.empty()) {
    return {};
  }
  const auto starting_sequence_number =
      StartingSequenceNumber(metadata, starting_snapshot_id);
  for (const auto& data_file : data_files) {
    for (const auto& index : indexes) {
      ICEBERG_ASSIGN_OR_RAISE(auto deletes,
                              index->ForDataFile(starting_sequence_number, *data_file));
      if (ignore_equality_deletes) {
        if (std::ranges::any_of(deletes, [](const auto& file) {
              return file->content == DataFile::Content::kPositionDeletes;
            })) {
          return ValidationFailed(
              "Cannot commit, found new position delete for replaced data file: {}",
              data_file->file_path);
        }
      } else if (!deletes.empty()) {
        return ValidationFailed(
            "Cannot commit, found new delete for replaced data file: {}",
            data_file->file_path);
      }
    }
  }
  return {};
}

Result<std::vector<std::string>> ReferencedDeleteFileLocations(
    const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id,
    const std::shared_ptr<Expression>& filter,
    const std::shared_ptr<PartitionSet>& partitions,
    const std::shared_ptr<Snapshot>& parent, const std::shared_ptr<FileIO>& io,
    bool case_sensitive) {
  ICEBERG_ASSIGN_OR_RAISE(auto indexes, AddedDeleteFilesIndexedPerSnapshot(
                                            metadata, starting_snapshot_id, filter,
                                            partitions, parent, io, case_sensitive));
  std::vector<std::string> locations;
  std::unordered_set<std::string_view> seen;
  for (const auto& index : indexes) {
    for (const auto& file : index->ReferencedDeleteFiles()) {
      if (seen.insert(file->file_path).second) {
        locations.push_back(file->file_path);
      }
    }
  }
  return locations;
}

Status CheckAddedDVs(const TableMetadata& metadata,
                     std::optional<int64_t> starting_snapshot_id,
                     const std::shared_ptr<Expression>& filter,
                     const std::unordered_set<std::string>& referenced_data_files,
                     const std::shared_ptr<Snapshot>& parent,
                     const std::shared_ptr<FileIO>& io, bool case_sensitive,
                     OptionalExecutor executor) {
  if (!parent || referenced_data_files.empty()) {
    return {};
  }
  ICEBERG_ASSIGN_OR_RAISE(
      auto history,
      ValidationHistory(metadata, parent->snapshot_id, starting_snapshot_id,
                        kValidateAddedDVsOperations, ManifestContent::kDeletes, io));
  if (history.manifests.empty()) {
    return {};
  }
  ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());
  // A concurrent spec change can invalidate manifest pruning by the conflict filter.
  if (std::ranges::all_of(history.manifests, [&](const ManifestFile& manifest) {
        return manifest.partition_spec_id == metadata.default_spec_id;
      })) {
    ICEBERG_ASSIGN_OR_RAISE(
        history.manifests,
        FilterManifestsByPartition(metadata, schema, filter, std::move(history.manifests),
                                   case_sensitive));
  }
  std::erase_if(history.manifests,
                [](const ManifestFile& manifest) { return !manifest.has_added_files(); });

  return internal::RunTasksWithStopOnFailure(
      history.manifests.size(), executor, [&](size_t i) -> Status {
        const auto& manifest = history.manifests[i];
        ICEBERG_ASSIGN_OR_RAISE(auto spec,
                                metadata.PartitionSpecById(manifest.partition_spec_id));
        ICEBERG_ASSIGN_OR_RAISE(auto reader,
                                ManifestReader::Make(manifest, io, schema, spec));
        reader->CaseSensitive(case_sensitive).FilterRows(filter);
        ICEBERG_ASSIGN_OR_RAISE(auto entries, reader->LiveEntriesStream());
        while (true) {
          ICEBERG_ASSIGN_OR_RAISE(auto entry, entries->Next());
          if (!entry) {
            return {};
          }
          const auto& file = *entry->data_file;
          if (entry->snapshot_id && history.snapshot_ids.contains(*entry->snapshot_id) &&
              ContentFileUtil::IsDV(file) && file.referenced_data_file &&
              referenced_data_files.contains(*file.referenced_data_file)) {
            return ValidationFailed("Found concurrently added DV for {}: {}",
                                    *file.referenced_data_file,
                                    ContentFileUtil::DVDesc(file));
          }
        }
      });
}

}  // namespace

MergingSnapshotUpdate::MergingSnapshotUpdate(std::string table_name,
                                             std::shared_ptr<TransactionContext> ctx)
    : SnapshotUpdate(std::move(ctx)),
      table_name_(std::move(table_name)),
      delete_expression_(Expressions::AlwaysFalse()) {
  const auto& properties = base().properties;
  const int64_t target_size = properties.Get(TableProperties::kManifestTargetSizeBytes);
  const int32_t min_count = properties.Get(TableProperties::kManifestMinMergeCount);
  const bool merge_enabled = properties.Get(TableProperties::kManifestMergeEnabled);
  auto io = ctx_->table->io();
  // A later update in the transaction may still refer to a superseded output.
  // The transaction decides which staged files are safe to delete after replay.
  auto discard = [this](const std::string& path) -> Status {
    RegisterStagedFile(path);
    return {};
  };
  auto make_merge_manager = [&](ManifestContent content) {
    return ManifestMergeManager::Make(
        content, target_size, min_count, merge_enabled, io,
        [this] { return SnapshotId(); }, discard);
  };
  auto init = [this](auto& manager, auto result) {
    if (result) {
      manager = std::move(*result);
    } else {
      AddError(std::move(result.error()));
    }
  };
  init(data_merge_manager_, make_merge_manager(ManifestContent::kData));
  init(data_filter_manager_,
       ManifestFilterManager::Make(ManifestContent::kData, io, discard));
  init(delete_merge_manager_, make_merge_manager(ManifestContent::kDeletes));
  init(delete_filter_manager_,
       ManifestFilterManager::Make(ManifestContent::kDeletes, io, discard));
}

MergingSnapshotUpdate::~MergingSnapshotUpdate() {
  // An unsubmitted standalone update can already own manifest copies. Once
  // CommitUpdate has started, only the transaction knows which paths are live.
  if (!ctx_->transaction && !rewritten_append_manifests_.empty()) {
    internal::LogAndIgnoreFailure("Abandoned manifest copies", [this]() -> Status {
      if (CheckCommitAllowed()) {
        for (const auto& copy : rewritten_append_manifests_) {
          std::ignore = DeleteStagedFile(copy.manifest_path);
        }
      }
      return {};
    });
  }
}

Status MergingSnapshotUpdate::Commit() {
  ICEBERG_RETURN_UNEXPECTED(CheckCommitAllowed());
  // Java refreshes before applying to the branch head. Otherwise a manifest
  // list expired since this update was created can disappear before the CAS.
  // Explicit transactions apply against their own staged metadata.
  if (!ctx_->transaction && ctx_->kind == TransactionKind::kUpdate && CheckErrors()) {
    ICEBERG_RETURN_UNEXPECTED(internal::CatchExceptions(
        "Refresh table for snapshot update", [this] { return ctx_->table->Refresh(); }));
    ICEBERG_RETURN_UNEXPECTED(RebaseIfTableChanged());
  }
  return SnapshotUpdate::Commit();
}

Status MergingSnapshotUpdate::RebaseIfTableChanged() {
  if (!ctx_->transaction && ctx_->kind == TransactionKind::kUpdate &&
      ctx_->base() != ctx_->table->metadata().get()) {
    ctx_->ResetMetadataBuilder(ctx_->table->metadata());
    return PrepareForReplay();
  }
  return {};
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
  return AddInternal(std::move(file), std::nullopt);
}

Status MergingSnapshotUpdate::AddDeleteFile(std::shared_ptr<DataFile> file,
                                            int64_t data_sequence_number) {
  return AddInternal(std::move(file), data_sequence_number);
}

Status MergingSnapshotUpdate::AddInternal(std::shared_ptr<DataFile> file,
                                          std::optional<int64_t> data_sequence_number) {
  if (!file) {
    return InvalidArgument("Cannot add a null delete file");
  }
  ICEBERG_RETURN_UNEXPECTED(ValidateNewDeleteFile(base(), *file));
  if (!file->partition_spec_id.has_value()) {
    return InvalidArgument("Delete file must have a partition spec ID");
  }
  ICEBERG_RETURN_UNEXPECTED(base().PartitionSpecById(file->partition_spec_id.value()));

  // Pending delete files expose the sequence requested by this operation. A
  // caller may be reusing a committed file that already carries an inherited one.
  auto staged_file = std::make_shared<DataFile>(*file);
  staged_file->data_sequence_number = data_sequence_number;
  if (ContentFileUtil::IsDV(*staged_file)) {
    ICEBERG_PRECHECK(staged_file->referenced_data_file.has_value(),
                     "DV must have a referenced data file: {}", staged_file->file_path);
    auto referenced = *staged_file->referenced_data_file;
    dvs_by_referenced_file_.Add(std::move(referenced), std::move(staged_file));
  } else {
    v2_deletes_.push_back(std::move(staged_file));
  }
  has_new_delete_files_ = true;
  return {};
}

Status MergingSnapshotUpdate::ValidateNewDeleteFile(const TableMetadata& metadata,
                                                    const DataFile& file) {
  return ValidateDeleteFileForVersion(file, metadata.format_version);
}

Status MergingSnapshotUpdate::AddManifest(ManifestFile manifest) {
  ICEBERG_RETURN_UNEXPECTED(CheckCommitAllowed());
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
    if (ctx_->transaction) {
      auto transaction = ctx_->transaction->lock();
      ICEBERG_CHECK(
          transaction && transaction->state() == TransactionState::kUpdatePending,
          "Cannot copy a manifest for an inactive transaction update");
    }
    ICEBERG_ASSIGN_OR_RAISE(auto copied, CopyManifest(manifest));
    rewritten_append_manifests_.push_back(std::move(copied));
  }
  return {};
}

Result<ManifestFile> MergingSnapshotUpdate::CopyManifest(const ManifestFile& manifest) {
  // A standalone writer may have refreshed its table after creating the update.
  ICEBERG_RETURN_UNEXPECTED(RebaseIfTableChanged());
  const TableMetadata& current = base();
  ICEBERG_ASSIGN_OR_RAISE(auto schema, SnapshotUtil::SchemaFor(current, target_branch()));
  ICEBERG_ASSIGN_OR_RAISE(auto spec,
                          current.PartitionSpecById(manifest.partition_spec_id));
  std::string path;
  auto new_output_path = [&] {
    path = ManifestPath();
    return path;
  };
  // A failed copy must not leave partial counts in the cached summary.
  SnapshotSummaryBuilder summary;
  auto copied = internal::CatchExceptions("Copy append manifest", [&] {
    return CopyAppendManifest(manifest, ctx_->table->io(), schema, spec, SnapshotId(),
                              new_output_path, current.format_version, &summary);
  });
  if (!copied) {
    if (!path.empty()) std::ignore = DeleteStagedFile(path);
    return ::iceberg::unexpected(std::move(copied.error()));
  }
  appended_manifests_summary_.Merge(summary);
  return copied;
}

Status MergingSnapshotUpdate::ValidateAddedDataFiles(
    const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id,
    std::shared_ptr<Expression> data_filter, const std::shared_ptr<Snapshot>& parent,
    std::shared_ptr<FileIO> io, bool case_sensitive) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto paths,
      MatchingDataFileLocations(metadata, starting_snapshot_id, parent, io,
                                kValidateAddedFilesOperations, ManifestStatus::kAdded,
                                data_filter, case_sensitive));
  if (!paths.empty()) {
    return ValidationFailed(
        "Found conflicting files that can contain records matching {}: {}",
        data_filter ? data_filter->ToString() : "any expression",
        FormatRange(paths, ", ", "[", "]"));
  }
  return {};
}

Status MergingSnapshotUpdate::ValidateAddedDataFiles(
    const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id,
    const PartitionSet& partition_set, const std::shared_ptr<Snapshot>& parent,
    std::shared_ptr<FileIO> io) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto paths,
      MatchingDataFileLocations(metadata, starting_snapshot_id, parent, io,
                                kValidateAddedFilesOperations, ManifestStatus::kAdded,
                                nullptr, /*case_sensitive=*/true, &partition_set));
  if (!paths.empty()) {
    return ValidationFailed(
        "Found conflicting files that can contain records matching partitions {}: {}",
        FormatValidationPartitions(metadata, partition_set),
        FormatRange(paths, ", ", "[", "]"));
  }
  return {};
}

Status MergingSnapshotUpdate::ValidateNoNewDeletesForDataFiles(
    const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id,
    const DataFileSet& replaced_files, const std::shared_ptr<Snapshot>& parent,
    std::shared_ptr<FileIO> io, bool ignore_equality_deletes) {
  return CheckNewDeletesForDataFiles(metadata, starting_snapshot_id, nullptr,
                                     replaced_files, parent, io, ignore_equality_deletes,
                                     /*case_sensitive=*/true);
}

Status MergingSnapshotUpdate::ValidateNoNewDeletesForDataFiles(
    const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id,
    std::shared_ptr<Expression> data_filter, const DataFileSet& replaced_files,
    const std::shared_ptr<Snapshot>& parent, std::shared_ptr<FileIO> io,
    bool case_sensitive) {
  return CheckNewDeletesForDataFiles(metadata, starting_snapshot_id, data_filter,
                                     replaced_files, parent, io,
                                     /*ignore_equality_deletes=*/false, case_sensitive);
}

Status MergingSnapshotUpdate::ValidateNoNewDeleteFiles(
    const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id,
    std::shared_ptr<Expression> data_filter, const std::shared_ptr<Snapshot>& parent,
    std::shared_ptr<FileIO> io, bool case_sensitive) {
  ICEBERG_ASSIGN_OR_RAISE(auto paths, ReferencedDeleteFileLocations(
                                          metadata, starting_snapshot_id, data_filter,
                                          nullptr, parent, io, case_sensitive));
  if (!paths.empty()) {
    return ValidationFailed(
        "Found new conflicting delete files that can apply to records matching {}: {}",
        data_filter ? data_filter->ToString() : "any expression",
        FormatRange(paths, ", ", "[", "]"));
  }
  return {};
}

Status MergingSnapshotUpdate::ValidateNoNewDeleteFiles(
    const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id,
    const PartitionSet& partition_set, const std::shared_ptr<Snapshot>& parent,
    std::shared_ptr<FileIO> io) {
  ICEBERG_ASSIGN_OR_RAISE(auto paths, ReferencedDeleteFileLocations(
                                          metadata, starting_snapshot_id, nullptr,
                                          std::make_shared<PartitionSet>(partition_set),
                                          parent, io, /*case_sensitive=*/true));
  if (!paths.empty()) {
    return ValidationFailed(
        "Found new conflicting delete files that can apply to records matching {}: {}",
        FormatValidationPartitions(metadata, partition_set),
        FormatRange(paths, ", ", "[", "]"));
  }
  return {};
}

Result<std::unique_ptr<DeleteFileIndex>> MergingSnapshotUpdate::AddedDeleteFiles(
    const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id,
    std::shared_ptr<Expression> data_filter, std::shared_ptr<PartitionSet> partition_set,
    const std::shared_ptr<Snapshot>& parent, std::shared_ptr<FileIO> io,
    bool case_sensitive) {
  std::vector<ManifestFile> manifests;
  if (parent && metadata.format_version >= 2) {
    ICEBERG_ASSIGN_OR_RAISE(
        auto history,
        ValidationHistory(metadata, parent->snapshot_id, starting_snapshot_id,
                          kValidateAddedDeleteFilesOperations, ManifestContent::kDeletes,
                          io));
    manifests = std::move(history.manifests);
  }
  ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());
  TableMetadataCache cache(&metadata);
  ICEBERG_ASSIGN_OR_RAISE(auto specs_by_id, cache.GetPartitionSpecsById());
  return BuildDeleteFileIndex(schema, specs_by_id.get(), io, std::move(manifests),
                              StartingSequenceNumber(metadata, starting_snapshot_id),
                              data_filter, partition_set, case_sensitive);
}

Status MergingSnapshotUpdate::ValidateDeletedDataFiles(
    const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id,
    std::shared_ptr<Expression> data_filter, const std::shared_ptr<Snapshot>& parent,
    std::shared_ptr<FileIO> io, bool case_sensitive) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto paths,
      MatchingDataFileLocations(metadata, starting_snapshot_id, parent, io,
                                kValidateDataFilesExistOperations,
                                ManifestStatus::kDeleted, data_filter, case_sensitive));
  if (!paths.empty()) {
    return ValidationFailed(
        "Found conflicting deleted files that can contain records matching {}: {}",
        data_filter ? data_filter->ToString() : "any expression",
        FormatRange(paths, ", ", "[", "]"));
  }
  return {};
}

Status MergingSnapshotUpdate::ValidateDeletedDataFiles(
    const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id,
    const PartitionSet& partition_set, const std::shared_ptr<Snapshot>& parent,
    std::shared_ptr<FileIO> io) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto paths, MatchingDataFileLocations(metadata, starting_snapshot_id, parent, io,
                                            kValidateDataFilesExistOperations,
                                            ManifestStatus::kDeleted, nullptr,
                                            /*case_sensitive=*/true, &partition_set));
  if (!paths.empty()) {
    return ValidationFailed(
        "Found conflicting deleted files that can apply to records matching {}: {}",
        FormatValidationPartitions(metadata, partition_set),
        FormatRange(paths, ", ", "[", "]"));
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
  const auto operations =
      skip_deletes
          ? std::span<const std::string_view>(kValidateDataFilesExistSkipDeleteOperations)
          : std::span<const std::string_view>(kValidateDataFilesExistOperations);
  ICEBERG_ASSIGN_OR_RAISE(
      auto paths,
      MatchingDataFileLocations(metadata, starting_snapshot_id, parent, io, operations,
                                ManifestStatus::kDeleted, conflict_detection_filter,
                                /*case_sensitive=*/true,
                                /*partitions=*/nullptr, &file_paths));
  if (!paths.empty()) {
    return ValidationFailed("Cannot commit, missing data files: {}",
                            FormatRange(paths, ", ", "[", "]"));
  }
  return {};
}

Status MergingSnapshotUpdate::ValidateAddedDVs(
    const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id,
    std::shared_ptr<Expression> conflict_filter,
    const std::unordered_set<std::string>& referenced_data_files,
    const std::shared_ptr<Snapshot>& parent, std::shared_ptr<FileIO> io,
    bool case_sensitive) {
  return CheckAddedDVs(metadata, starting_snapshot_id, conflict_filter,
                       referenced_data_files, parent, io, case_sensitive,
                       /*executor=*/std::nullopt);
}

Status MergingSnapshotUpdate::ValidateAddedDVs(
    const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id,
    std::shared_ptr<Expression> conflict_filter, const std::shared_ptr<Snapshot>& parent,
    std::shared_ptr<FileIO> io) const {
  if (!parent || dvs_by_referenced_file_.empty()) {
    return {};
  }

  std::unordered_set<std::string> referenced_data_files;
  for (const auto& entry : dvs_by_referenced_file_.entries()) {
    referenced_data_files.insert(entry.referenced_file);
  }
  if (!new_data_files_by_spec_.empty()) {
    // Only skip history when every staged DV targets a data file added by this update.
    auto unmatched = referenced_data_files;
    for (const auto& [_, files] : new_data_files_by_spec_) {
      for (const auto& file : files) {
        if (unmatched.erase(file->file_path) && unmatched.empty()) {
          return {};
        }
      }
    }
  }
  return CheckAddedDVs(metadata, starting_snapshot_id, conflict_filter,
                       referenced_data_files, parent, io, case_sensitive_,
                       plan_executor());
}

std::unordered_map<std::string, std::string> MergingSnapshotUpdate::Summary() {
  summary_builder().SetPartitionSummaryLimit(
      base().properties.Get(TableProperties::kWritePartitionSummaryLimit));
  return summary_builder().Build();
}

Status MergingSnapshotUpdate::ValidateDeleteFilesForVersion(
    const TableMetadata& current) const {
  // A concurrent v2 -> v3 upgrade can invalidate a staged position delete.
  for (const auto& pending : v2_deletes_) {
    ICEBERG_RETURN_UNEXPECTED(
        ValidateDeleteFileForVersion(*pending, current.format_version));
  }
  return {};
}

Result<std::vector<ManifestFile>> MergingSnapshotUpdate::Apply(
    const TableMetadata& metadata_to_update, const std::shared_ptr<Snapshot>& snapshot) {
  ICEBERG_RETURN_UNEXPECTED(ManagersReady());
  ICEBERG_RETURN_UNEXPECTED(ValidateDeleteFilesForVersion(metadata_to_update));

  ICEBERG_ASSIGN_OR_RAISE(auto schema,
                          SnapshotUtil::SchemaFor(metadata_to_update, target_branch()));
  TableMetadataCache metadata_cache(&metadata_to_update);
  ICEBERG_ASSIGN_OR_RAISE(auto specs, metadata_cache.GetPartitionSpecsById());
  auto writer_factory = MakeWriterFactory(schema);
  // Initialize the ID before dispatching writers to the scan executor.
  const int64_t snapshot_id = SnapshotId();

  SnapshotReader reader(snapshot.get());
  auto filter = [&](ManifestFilterManager& manager,
                    ManifestContent content) -> Result<std::vector<ManifestFile>> {
    std::vector<const ManifestFile*> input;
    if (snapshot) {
      ICEBERG_ASSIGN_OR_RAISE(auto manifests,
                              content == ManifestContent::kData
                                  ? reader.DataManifests(ctx_->table->io())
                                  : reader.DeleteManifests(ctx_->table->io()));
      input.reserve(manifests.size());
      for (const auto& manifest : manifests) input.push_back(&manifest);
    }
    return manager.FilterManifests(schema, specs.get(), input, writer_factory,
                                   plan_executor());
  };

  ICEBERG_ASSIGN_OR_RAISE(auto filtered,
                          filter(*data_filter_manager_, ManifestContent::kData));
  int64_t min_data_sequence_number = metadata_to_update.last_sequence_number;
  for (const auto& manifest : filtered) {
    if (manifest.min_sequence_number != kUnassignedSequenceNumber) {
      min_data_sequence_number =
          std::min(min_data_sequence_number, manifest.min_sequence_number);
    }
  }
  ICEBERG_RETURN_UNEXPECTED(
      delete_filter_manager_->DropDeleteFilesOlderThan(min_data_sequence_number));
  delete_filter_manager_->RemoveDanglingDeletesFor(
      data_filter_manager_->FilesToBeDeleted());
  ICEBERG_ASSIGN_OR_RAISE(auto filtered_deletes,
                          filter(*delete_filter_manager_, ManifestContent::kDeletes));

  auto should_keep = [snapshot_id](const ManifestFile& manifest) {
    return manifest.has_added_files() || manifest.has_existing_files() ||
           manifest.added_snapshot_id == snapshot_id;
  };
  ICEBERG_ASSIGN_OR_RAISE(auto unmerged, PrepareNewDataManifests());
  ICEBERG_ASSIGN_OR_RAISE(auto unmerged_deletes, PrepareDeleteManifests());
  auto& summary = summary_builder();
  summary.Clear();
  summary.Merge(added_data_files_summary_);
  summary.Merge(added_delete_files_summary_);
  summary.Merge(appended_manifests_summary_);
  ICEBERG_ASSIGN_OR_RAISE(auto data_summary,
                          data_filter_manager_->BuildSummary(filtered, specs.get()));
  ICEBERG_ASSIGN_OR_RAISE(auto delete_summary, delete_filter_manager_->BuildSummary(
                                                   filtered_deletes, specs.get()));
  summary.Merge(data_summary);
  summary.Merge(delete_summary);

  // Account for tombstones before moving just the live or current manifests into the
  // merge.
  unmerged.append_range(filtered | std::views::filter(should_keep) |
                        std::views::as_rvalue);
  unmerged_deletes.append_range(filtered_deletes | std::views::filter(should_keep) |
                                std::views::as_rvalue);

  ICEBERG_ASSIGN_OR_RAISE(auto manifests, data_merge_manager_->MergeManifests(
                                              std::move(unmerged), metadata_to_update,
                                              writer_factory, plan_executor()));
  ICEBERG_ASSIGN_OR_RAISE(auto delete_manifests,
                          delete_merge_manager_->MergeManifests(
                              std::move(unmerged_deletes), metadata_to_update,
                              writer_factory, plan_executor()));
  manifests.append_range(std::views::as_rvalue(delete_manifests));

  const int32_t replaced_manifests_count =
      data_filter_manager_->ReplacedManifestsCount() +
      delete_filter_manager_->ReplacedManifestsCount() +
      data_merge_manager_->ReplacedManifestsCount() +
      delete_merge_manager_->ReplacedManifestsCount();
  summary.Merge(BuildManifestCountSummary(manifests, replaced_manifests_count));
  return manifests;
}

Status MergingSnapshotUpdate::PrepareForReplay() {
  ICEBERG_RETURN_UNEXPECTED(SnapshotUpdate::PrepareForReplay());
  // The same path can inherit new sequence numbers and row IDs on replay.
  // Drop derived caches, but keep their files staged for terminal cleanup.
  ICEBERG_RETURN_UNEXPECTED(CleanUncommittedManagers({}));
  // A refreshed catalog table can also supply a new FileIO for the new head.
  auto io = ctx_->table->io();
  data_filter_manager_->file_io_ = io;
  delete_filter_manager_->file_io_ = io;
  data_merge_manager_->file_io_ = io;
  delete_merge_manager_->file_io_ = io;
  return {};
}

Status MergingSnapshotUpdate::CleanUncommitted(
    const std::unordered_set<std::string>& committed) {
  ICEBERG_RETURN_UNEXPECTED(CleanUncommittedManagers(committed));
  CleanUncommittedAppends(committed);
  return {};
}

Status MergingSnapshotUpdate::CleanUncommittedManagers(
    const std::unordered_set<std::string>& committed) {
  ICEBERG_RETURN_UNEXPECTED(ManagersReady());
  ICEBERG_RETURN_UNEXPECTED(data_merge_manager_->CleanUncommitted(committed));
  ICEBERG_RETURN_UNEXPECTED(data_filter_manager_->CleanUncommitted(committed));
  ICEBERG_RETURN_UNEXPECTED(delete_merge_manager_->CleanUncommitted(committed));
  return delete_filter_manager_->CleanUncommitted(committed);
}

void MergingSnapshotUpdate::CleanUncommittedAppends(
    const std::unordered_set<std::string>& committed) {
  // Generated files are already tracked by SnapshotUpdate.
  cached_new_data_manifests_.clear();
  cached_new_delete_manifests_.clear();
  rewritten_append_manifests_.clear();

  // External append manifests become table-owned only after a successful commit.
  if (!committed.empty()) {
    for (const auto& manifest : append_manifests_) {
      if (!committed.contains(manifest.manifest_path)) {
        RegisterStagedFile(manifest.manifest_path);
      }
    }
  }

  has_new_data_files_ = false;
  has_new_delete_files_ = false;
}

Result<std::vector<ManifestFile>> MergingSnapshotUpdate::PrepareNewDataManifests() {
  std::vector<ManifestFile> manifests;
  if (AddsDataFiles()) {
    ICEBERG_ASSIGN_OR_RAISE(manifests, NewDataFilesAsManifests());
  }

  manifests.append_range(append_manifests_);
  manifests.append_range(rewritten_append_manifests_);
  for (auto& manifest : manifests) {
    manifest.added_snapshot_id = SnapshotId();
  }
  return manifests;
}

Result<std::vector<ManifestFile>> MergingSnapshotUpdate::NewDataFilesAsManifests() {
  // New additions invalidate the cache. Old paths stay staged until cleanup.
  if (has_new_data_files_ && !cached_new_data_manifests_.empty()) {
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
    result.append_range(std::views::as_rvalue(written));
  }

  cached_new_data_manifests_ = result;
  has_new_data_files_ = false;
  return result;
}

Result<std::vector<ManifestFile>> MergingSnapshotUpdate::PrepareDeleteManifests() {
  if (!AddsDeleteFiles()) return std::vector<ManifestFile>{};
  return NewDeleteFilesAsManifests();
}

Result<std::vector<ManifestFile>> MergingSnapshotUpdate::NewDeleteFilesAsManifests() {
  if (has_new_delete_files_) {
    cached_new_delete_manifests_.clear();
  }
  if (!cached_new_delete_manifests_.empty()) return cached_new_delete_manifests_;

  ICEBERG_ASSIGN_OR_RAISE(auto deletes, MergeDVs());
  DeleteFileSet unique_v2_deletes;
  for (const auto& file : v2_deletes_) {
    if (unique_v2_deletes.insert(file).second) deletes.push_back(file);
  }

  std::unordered_map<int32_t, std::vector<ContentFileWithSequenceNumber>> deletes_by_spec;
  for (const auto& file : deletes) {
    deletes_by_spec[*file->partition_spec_id].push_back(
        {.file = file, .data_sequence_number = file->data_sequence_number});
  }

  SnapshotSummaryBuilder summary;
  std::vector<ManifestFile> manifests;
  for (const auto& [spec_id, files] : deletes_by_spec) {
    ICEBERG_ASSIGN_OR_RAISE(auto spec, base().PartitionSpecById(spec_id));
    for (const auto& entry : files) {
      ICEBERG_RETURN_UNEXPECTED(summary.AddedFile(*spec, *entry.file));
    }
    ICEBERG_ASSIGN_OR_RAISE(auto written, WriteDeleteManifests(files, spec));
    manifests.append_range(std::views::as_rvalue(written));
  }
  // Publish the summary together with the successful write, including on retry.
  added_delete_files_summary_ = std::move(summary);
  cached_new_delete_manifests_ = manifests;
  has_new_delete_files_ = false;
  return manifests;
}

Result<std::vector<std::shared_ptr<DataFile>>> MergingSnapshotUpdate::MergeDVs() {
  std::vector<std::shared_ptr<DataFile>> final_dvs;
  std::vector<DeletionVectorMergeGroup> duplicates;
  for (const auto& [referenced_file, dvs] : dvs_by_referenced_file_.entries()) {
    if (dvs.size() == 1) {
      final_dvs.push_back(dvs.front());
    } else {
      ICEBERG_LOG_WARN("Merging {} duplicate DVs for data file {} in table {}",
                       dvs.size(), referenced_file, table_name_);
      const auto& first = *dvs.front();
      ICEBERG_ASSIGN_OR_RAISE(auto spec,
                              base().PartitionSpecById(*first.partition_spec_id));
      duplicates.push_back({.referenced_data_file = referenced_file,
                            .delete_files = dvs,
                            .spec = std::move(spec),
                            .partition = first.partition});
    }
  }

  ICEBERG_ASSIGN_OR_RAISE(auto locations, ctx_->NewLocationProvider());
  auto path = locations->NewDataLocation(
      std::format("merged-dvs-{}-{}.puffin", SnapshotId(), ++dv_merge_attempt_));
  if (!duplicates.empty()) {
    RegisterStagedFile(path, /*is_content=*/true);
    ICEBERG_ASSIGN_OR_RAISE(
        auto merged,
        DVUtil::MergeAndWriteDVs(duplicates, path, ctx_->table->io(), plan_executor()));
    // Newly written DVs inherit this commit's sequence. Singletons retain their
    // explicit pending data sequence number.
    final_dvs.append_range(std::views::as_rvalue(merged));
  }
  return final_dvs;
}

void MergingSnapshotUpdate::PendingDeleteFilesByReferencedFile::Add(
    std::string referenced_file, std::shared_ptr<DataFile> file) {
  auto [iter, inserted] =
      index_by_referenced_file_.try_emplace(referenced_file, entries_.size());
  if (inserted) {
    entries_.push_back(Entry{.referenced_file = std::move(referenced_file), .files = {}});
  }
  entries_[iter->second].files.push_back(std::move(file));
}

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

}  // namespace iceberg
