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
#include <span>
#include <unordered_map>
#include <vector>

#include "iceberg/constants.h"
#include "iceberg/delete_file_index.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/expression/inclusive_metrics_evaluator.h"
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
#include "iceberg/util/macros.h"
#include "iceberg/util/snapshot_util_internal.h"

namespace iceberg {

namespace {

bool MatchesOperation(std::optional<std::string_view> operation,
                      std::initializer_list<std::string_view> expected) {
  return operation.has_value() &&
         std::find(expected.begin(), expected.end(), operation.value()) != expected.end();
}

struct ValidationHistoryResult {
  std::vector<ManifestFile> manifests;
  std::unordered_set<int64_t> snapshot_ids;
};

Result<std::vector<std::shared_ptr<Snapshot>>> ValidationAncestorsBetween(
    const TableMetadata& metadata, int64_t latest_snapshot_id,
    int64_t starting_snapshot_id) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto ancestors,
      SnapshotUtil::AncestorsBetween(metadata, latest_snapshot_id, starting_snapshot_id));
  if (latest_snapshot_id == starting_snapshot_id) {
    return ancestors;
  }
  if (ancestors.empty()) {
    return InvalidArgument(
        "Cannot validate history: starting snapshot {} is not an ancestor "
        "of snapshot {}",
        starting_snapshot_id, latest_snapshot_id);
  }

  const auto& oldest_checked = ancestors.back();
  if (oldest_checked == nullptr || !oldest_checked->parent_snapshot_id.has_value() ||
      oldest_checked->parent_snapshot_id.value() != starting_snapshot_id) {
    return InvalidArgument(
        "Cannot validate history: starting snapshot {} is not an ancestor "
        "of snapshot {}",
        starting_snapshot_id, latest_snapshot_id);
  }
  return ancestors;
}

Result<ValidationHistoryResult> ValidationHistory(
    const TableMetadata& metadata, int64_t latest_snapshot_id,
    int64_t starting_snapshot_id,
    std::initializer_list<std::string_view> matching_operations, ManifestContent content,
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

Result<std::optional<std::string>> FindMatchingDataFile(
    const TableMetadata& metadata, const std::vector<ManifestFile>& manifests,
    ManifestStatus status, std::shared_ptr<Expression> filter,
    const PartitionSet* partition_set, const std::shared_ptr<FileIO>& io,
    bool case_sensitive) {
  ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());
  auto partition_filter = partition_set != nullptr
                              ? std::make_shared<PartitionSet>(*partition_set)
                              : std::shared_ptr<PartitionSet>{};

  for (const auto& manifest : manifests) {
    ICEBERG_ASSIGN_OR_RAISE(auto spec,
                            metadata.PartitionSpecById(manifest.partition_spec_id));
    ICEBERG_ASSIGN_OR_RAISE(auto reader,
                            ManifestReader::Make(manifest, io, schema, spec));
    reader->CaseSensitive(case_sensitive);
    if (filter != nullptr) {
      reader->FilterRows(filter);
    }
    if (partition_filter != nullptr) {
      reader->FilterPartitions(partition_filter);
    }

    ICEBERG_ASSIGN_OR_RAISE(auto entries, reader->Entries());
    for (const auto& entry : entries) {
      if (entry.status == status && entry.data_file != nullptr) {
        return entry.data_file->file_path;
      }
    }
  }

  return std::optional<std::string>{};
}

}  // namespace

MergingSnapshotUpdate::MergingSnapshotUpdate(std::string table_name,
                                             std::shared_ptr<TransactionContext> ctx)
    : SnapshotUpdate(std::move(ctx)),
      table_name_(std::move(table_name)),
      delete_expression_(Expressions::AlwaysFalse()),
      data_filter_manager_(ManifestContent::kData, ctx_->table->io()),
      delete_filter_manager_(ManifestContent::kDeletes, ctx_->table->io()),
      data_merge_manager_(
          base().properties.Get(TableProperties::kManifestTargetSizeBytes),
          base().properties.Get(TableProperties::kManifestMinMergeCount),
          base().properties.Get(TableProperties::kManifestMergeEnabled)),
      delete_merge_manager_(
          base().properties.Get(TableProperties::kManifestTargetSizeBytes),
          base().properties.Get(TableProperties::kManifestMinMergeCount),
          base().properties.Get(TableProperties::kManifestMergeEnabled)) {}

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

  // Suppress first_row_id — it will be assigned by the commit, not inherited from the
  // source file.
  file->first_row_id = std::nullopt;

  auto& data_files = new_data_files_by_spec_[spec_id];
  auto [it, inserted] = data_files.insert(file);
  if (inserted) {
    has_new_data_files_ = true;
    ICEBERG_RETURN_UNEXPECTED(added_data_files_summary_.AddedFile(*spec, *file));
  }
  return {};
}

Status MergingSnapshotUpdate::ValidateNewDeleteFile(const DataFile& file) {
  if (file.content == DataFile::Content::kData) {
    return InvalidArgument("Expected a delete file but got a data file: {}",
                           file.file_path);
  }
  const int8_t format_version = base().format_version;
  const bool is_dv = file.referenced_data_file.has_value();
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
      if (format_version >= 3) {
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

Status MergingSnapshotUpdate::AddDeleteFile(std::shared_ptr<DataFile> file,
                                            std::optional<int64_t> data_sequence_number) {
  if (!file) {
    return InvalidArgument("Cannot add a null delete file");
  }
  ICEBERG_RETURN_UNEXPECTED(ValidateNewDeleteFile(*file));
  if (!file->partition_spec_id.has_value()) {
    return InvalidArgument("Delete file must have a partition spec ID");
  }
  ICEBERG_ASSIGN_OR_RAISE(auto spec,
                          base().PartitionSpecById(file->partition_spec_id.value()));
  ICEBERG_RETURN_UNEXPECTED(added_delete_files_summary_.AddedFile(*spec, *file));
  has_new_delete_files_ = true;
  new_delete_files_.push_back(PendingDeleteFile{
      .file = std::move(file), .data_sequence_number = std::move(data_sequence_number)});
  return {};
}

Status MergingSnapshotUpdate::DeleteDataFile(std::shared_ptr<DataFile> file) {
  if (!file) {
    return InvalidArgument("Cannot delete a null data file");
  }
  return data_filter_manager_.DeleteFile(std::move(file));
}

Status MergingSnapshotUpdate::DeleteDeleteFile(std::shared_ptr<DataFile> file) {
  if (!file) {
    return InvalidArgument("Cannot delete a null delete file");
  }
  return delete_filter_manager_.DeleteFile(std::move(file));
}

void MergingSnapshotUpdate::DeleteByPath(std::string_view path) {
  data_filter_manager_.DeleteFile(path);
}

Status MergingSnapshotUpdate::DeleteByRowFilter(std::shared_ptr<Expression> expr) {
  // If a delete file matches the row filter, it can also be removed because the rows
  // it references will also be deleted. Both filter managers receive the expression.
  delete_expression_ = expr;
  ICEBERG_RETURN_UNEXPECTED(data_filter_manager_.DeleteByRowFilter(expr));
  return delete_filter_manager_.DeleteByRowFilter(std::move(expr));
}

void MergingSnapshotUpdate::DropPartition(int32_t spec_id, PartitionValues partition) {
  // Dropping data in a partition also drops all delete files in that partition.
  data_filter_manager_.DropPartition(spec_id, partition);
  delete_filter_manager_.DropPartition(spec_id, std::move(partition));
}

void MergingSnapshotUpdate::FailMissingDeletePaths() {
  data_filter_manager_.FailMissingDeletePaths();
  delete_filter_manager_.FailMissingDeletePaths();
}

void MergingSnapshotUpdate::FailAnyDelete() {
  data_filter_manager_.FailAnyDelete();
  delete_filter_manager_.FailAnyDelete();
}

void MergingSnapshotUpdate::SetNewDataFilesDataSequenceNumber(int64_t sequence_number) {
  new_data_files_data_seq_number_ = sequence_number;
}

void MergingSnapshotUpdate::CaseSensitive(bool case_sensitive) {
  case_sensitive_ = case_sensitive;
  data_filter_manager_.CaseSensitive(case_sensitive);
  delete_filter_manager_.CaseSensitive(case_sensitive);
}

void MergingSnapshotUpdate::Set(const std::string& property, const std::string& value) {
  summary_builder().Set(property, value);
}

Result<std::shared_ptr<PartitionSpec>> MergingSnapshotUpdate::DataSpec() const {
  if (new_data_files_by_spec_.empty()) {
    return InvalidArgument("DataSpec() called before any data file was added");
  }
  if (new_data_files_by_spec_.size() > 1) {
    return InvalidArgument(
        "DataSpec() requires exactly one partition spec; got {} different specs",
        new_data_files_by_spec_.size());
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
  if (can_inherit_snapshot_id() && manifest.added_snapshot_id == kInvalidSnapshotId &&
      !manifest.first_row_id.has_value()) {
    appended_manifests_summary_.AddedManifest(manifest);
    append_manifests_.push_back(std::move(manifest));
  } else {
    ICEBERG_ASSIGN_OR_RAISE(auto copied, CopyManifest(manifest));
    rewritten_append_manifests_.push_back(std::move(copied));
  }
  return {};
}

Result<ManifestFile> MergingSnapshotUpdate::CopyManifest(const ManifestFile& manifest) {
  const TableMetadata& current = base();
  ICEBERG_ASSIGN_OR_RAISE(auto schema, SnapshotUtil::SchemaFor(current, target_branch()));
  ICEBERG_ASSIGN_OR_RAISE(auto spec,
                          current.PartitionSpecById(manifest.partition_spec_id));
  std::string path = ManifestPath();
  all_written_manifests_.insert(path);
  return CopyAppendManifest(manifest, ctx_->table->io(), schema, spec, SnapshotId(), path,
                            current.format_version, &appended_manifests_summary_);
}

// -------------------------------------------------------------------------
// State queries
// -------------------------------------------------------------------------

bool MergingSnapshotUpdate::AddsDataFiles() const {
  return !new_data_files_by_spec_.empty();
}

bool MergingSnapshotUpdate::AddsDeleteFiles() const { return !new_delete_files_.empty(); }

bool MergingSnapshotUpdate::DeletesDataFiles() const {
  return data_filter_manager_.ContainsDeletes();
}

bool MergingSnapshotUpdate::DeletesDeleteFiles() const {
  return delete_filter_manager_.ContainsDeletes();
}

// -------------------------------------------------------------------------
// Apply pipeline
// -------------------------------------------------------------------------

ManifestWriterFactory MergingSnapshotUpdate::MakeTrackedWriterFactory(
    const std::shared_ptr<Schema>& schema) {
  return
      [this, schema](int32_t spec_id,
                     ManifestContent content) -> Result<std::unique_ptr<ManifestWriter>> {
        const TableMetadata& meta = base();
        ICEBERG_ASSIGN_OR_RAISE(auto spec, meta.PartitionSpecById(spec_id));
        std::string path = ManifestPath();
        all_written_manifests_.insert(path);
        return ManifestWriter::MakeWriter(meta.format_version, SnapshotId(),
                                          std::move(path), ctx_->table->io(),
                                          std::move(spec), schema, content);
      };
}

Result<std::vector<ManifestFile>> MergingSnapshotUpdate::WriteNewDataManifests() {
  // If new files were staged after the cache was populated (commit retry), invalidate.
  if (has_new_data_files_ && cached_new_data_manifests_.has_value()) {
    for (const auto& m : *cached_new_data_manifests_) {
      std::ignore = DeleteFile(m.manifest_path);
    }
    cached_new_data_manifests_.reset();
  }

  if (cached_new_data_manifests_.has_value()) {
    return *cached_new_data_manifests_;
  }

  std::vector<ManifestFile> result;
  for (const auto& [spec_id, data_files] : new_data_files_by_spec_) {
    ICEBERG_ASSIGN_OR_RAISE(auto spec, base().PartitionSpecById(spec_id));
    ICEBERG_ASSIGN_OR_RAISE(
        auto written,
        WriteDataManifests(data_files.as_span(), spec, new_data_files_data_seq_number_));
    for (const auto& m : written) {
      all_written_manifests_.insert(m.manifest_path);
    }
    result.insert(result.end(), std::make_move_iterator(written.begin()),
                  std::make_move_iterator(written.end()));
  }

  cached_new_data_manifests_ = result;
  has_new_data_files_ = false;
  return result;
}

Result<std::vector<ManifestFile>> MergingSnapshotUpdate::WriteNewDeleteManifests() {
  // If new files were staged after the cache was populated (commit retry), invalidate.
  if (has_new_delete_files_ && cached_new_delete_manifests_.has_value()) {
    for (const auto& m : *cached_new_delete_manifests_) {
      std::ignore = DeleteFile(m.manifest_path);
    }
    cached_new_delete_manifests_.reset();
  }

  if (cached_new_delete_manifests_.has_value()) {
    return *cached_new_delete_manifests_;
  }

  // Group delete files by partition spec ID, mirroring WriteNewDataManifests().
  std::unordered_map<int32_t, std::vector<PendingDeleteFile>> delete_files_by_spec;
  for (const auto& pending_file : new_delete_files_) {
    delete_files_by_spec[pending_file.file->partition_spec_id.value()].push_back(
        pending_file);
  }

  std::vector<ManifestFile> result;
  for (auto& [spec_id, delete_files] : delete_files_by_spec) {
    ICEBERG_ASSIGN_OR_RAISE(auto spec, base().PartitionSpecById(spec_id));
    std::vector<DeleteManifestEntry> delete_entries;
    delete_entries.reserve(delete_files.size());
    for (const auto& pending_file : delete_files) {
      delete_entries.push_back(DeleteManifestEntry{
          .file = pending_file.file,
          .data_sequence_number = pending_file.data_sequence_number,
      });
    }
    ICEBERG_ASSIGN_OR_RAISE(auto written, WriteDeleteManifests(delete_entries, spec));
    for (const auto& m : written) {
      all_written_manifests_.insert(m.manifest_path);
    }
    result.insert(result.end(), std::make_move_iterator(written.begin()),
                  std::make_move_iterator(written.end()));
  }

  cached_new_delete_manifests_ = result;
  has_new_delete_files_ = false;
  return result;
}

Result<std::vector<ManifestFile>> MergingSnapshotUpdate::Apply(
    const TableMetadata& metadata_to_update, const std::shared_ptr<Snapshot>& snapshot) {
  // Re-validate buffered delete files against the current format version. A format
  // upgrade between staging and commit could make previously-valid files invalid.
  for (const auto& pending_file : new_delete_files_) {
    ICEBERG_RETURN_UNEXPECTED(ValidateNewDeleteFile(*pending_file.file));
  }

  // Rebuild summary from stable sub-builders so that commit retries don't double-count.
  summary_builder().Clear();
  summary_builder().Merge(added_data_files_summary_);
  summary_builder().Merge(added_delete_files_summary_);
  summary_builder().Merge(appended_manifests_summary_);

  ICEBERG_ASSIGN_OR_RAISE(auto target_schema,
                          SnapshotUtil::SchemaFor(metadata_to_update, target_branch()));
  auto tracked_factory = MakeTrackedWriterFactory(target_schema);

  // Step 1: Filter data manifests.
  ICEBERG_ASSIGN_OR_RAISE(auto filtered_data, data_filter_manager_.FilterManifests(
                                                  target_schema, metadata_to_update,
                                                  snapshot, tracked_factory));

  // Track deleted data files in the summary builder.
  for (const auto& file : data_filter_manager_.FilesToBeDeleted()) {
    if (!file->partition_spec_id.has_value()) {
      continue;
    }
    ICEBERG_ASSIGN_OR_RAISE(
        auto spec, metadata_to_update.PartitionSpecById(*file->partition_spec_id));
    ICEBERG_RETURN_UNEXPECTED(summary_builder().DeletedFile(*spec, *file));
  }

  // Step 2: Compute min data sequence number; set up delete filter cleanup.
  // Use last_sequence_number as the initial value so that an empty filtered list
  // produces a sensible minimum. Skip manifests with kUnassignedSequenceNumber —
  // those are manifests written in the current Apply() call whose sequence number
  // hasn't been assigned yet. If all filtered manifests are unassigned (e.g. the
  // table has no pre-existing data manifests), the fallback to last_sequence_number
  // is safe: any delete file with seq > 0 and seq <= last_sequence_number can no
  // longer match live data rows, so cleaning them up is correct.
  int64_t min_data_seq = metadata_to_update.last_sequence_number;
  for (const auto& manifest : filtered_data) {
    if (manifest.min_sequence_number != kUnassignedSequenceNumber) {
      min_data_seq = std::min(min_data_seq, manifest.min_sequence_number);
    }
  }
  delete_filter_manager_.DropDeleteFilesOlderThan(min_data_seq);
  delete_filter_manager_.RemoveDanglingDeletesFor(
      data_filter_manager_.FilesToBeDeleted());

  // Step 3: Filter delete manifests.
  ICEBERG_ASSIGN_OR_RAISE(auto filtered_deletes, delete_filter_manager_.FilterManifests(
                                                     target_schema, metadata_to_update,
                                                     snapshot, tracked_factory));

  // Track deleted delete files in the summary builder.
  for (const auto& file : delete_filter_manager_.FilesToBeDeleted()) {
    if (!file->partition_spec_id.has_value()) {
      continue;
    }
    ICEBERG_ASSIGN_OR_RAISE(
        auto spec, metadata_to_update.PartitionSpecById(*file->partition_spec_id));
    ICEBERG_RETURN_UNEXPECTED(summary_builder().DeletedFile(*spec, *file));
  }

  // Drop manifests with no live files — they carry no data and should not be merged
  // into the new snapshot. Manifests written by the current snapshot are always kept
  // regardless of live-file counts; the merge stage handles any that are empty.
  int64_t snapshot_id = SnapshotId();
  auto should_keep = [snapshot_id](const ManifestFile& m) {
    return m.has_added_files() || m.has_existing_files() ||
           m.added_snapshot_id == snapshot_id;
  };
  std::erase_if(filtered_data, [&](const ManifestFile& m) { return !should_keep(m); });
  std::erase_if(filtered_deletes, [&](const ManifestFile& m) { return !should_keep(m); });

  // Step 4: Write (or retrieve cached) new data manifests.
  ICEBERG_ASSIGN_OR_RAISE(auto written_data_manifests, WriteNewDataManifests());

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

  // Step 6: Merge data manifests.
  ICEBERG_ASSIGN_OR_RAISE(auto merged_data,
                          data_merge_manager_.MergeManifests(
                              filtered_data, new_data_manifests, SnapshotId(),
                              metadata_to_update, ctx_->table->io(), tracked_factory));

  // Step 7: Merge delete manifests.
  ICEBERG_ASSIGN_OR_RAISE(auto merged_deletes,
                          delete_merge_manager_.MergeManifests(
                              filtered_deletes, new_delete_manifests, SnapshotId(),
                              metadata_to_update, ctx_->table->io(), tracked_factory));

  std::vector<ManifestFile> result;
  result.reserve(merged_data.size() + merged_deletes.size());
  result.insert(result.end(), std::make_move_iterator(merged_data.begin()),
                std::make_move_iterator(merged_data.end()));
  result.insert(result.end(), std::make_move_iterator(merged_deletes.begin()),
                std::make_move_iterator(merged_deletes.end()));

  // Manifest count summary.
  int32_t manifests_created = 0;
  int32_t manifests_kept = 0;
  for (const auto& m : result) {
    if (m.added_snapshot_id == snapshot_id) {
      ++manifests_created;
    } else {
      ++manifests_kept;
    }
  }
  int32_t replaced_manifests_count = data_filter_manager_.ReplacedManifestsCount() +
                                     delete_filter_manager_.ReplacedManifestsCount() +
                                     data_merge_manager_.ReplacedManifestsCount() +
                                     delete_merge_manager_.ReplacedManifestsCount();
  summary_builder().SetManifestCounts(manifests_created, manifests_kept,
                                      replaced_manifests_count);

  return result;
}

void MergingSnapshotUpdate::CleanUncommitted(
    const std::unordered_set<std::string>& committed) {
  for (const auto& path : all_written_manifests_) {
    if (!committed.contains(path)) {
      std::ignore = DeleteFile(path);
    }
  }
  all_written_manifests_.clear();
  cached_new_data_manifests_.reset();
  cached_new_delete_manifests_.reset();
  has_new_data_files_ = false;
  has_new_delete_files_ = false;

  // rewritten_append_manifests_ are always owned by the table (copied by us),
  // so delete any that were not committed.
  for (const auto& m : rewritten_append_manifests_) {
    if (!committed.contains(m.manifest_path)) {
      std::ignore = DeleteFile(m.manifest_path);
    }
  }

  // append_manifests_ are only owned by the table if the commit succeeded
  // (i.e., at least one manifest was committed).
  if (!committed.empty()) {
    for (const auto& m : append_manifests_) {
      if (!committed.contains(m.manifest_path)) {
        std::ignore = DeleteFile(m.manifest_path);
      }
    }
  }
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
    const TableMetadata& metadata, int64_t starting_snapshot_id,
    std::shared_ptr<Expression> filter, const std::shared_ptr<Snapshot>& parent,
    std::shared_ptr<FileIO> io, bool case_sensitive) {
  if (parent == nullptr) {
    return {};
  }

  ICEBERG_ASSIGN_OR_RAISE(
      auto history, ValidationHistory(metadata, parent->snapshot_id, starting_snapshot_id,
                                      {DataOperation::kAppend, DataOperation::kOverwrite},
                                      ManifestContent::kData, io));
  ICEBERG_ASSIGN_OR_RAISE(
      auto conflict_path,
      FindMatchingDataFile(metadata, history.manifests, ManifestStatus::kAdded, filter,
                           nullptr, io, case_sensitive));
  if (conflict_path.has_value()) {
    return InvalidArgument(
        "Found conflicting files that can contain rows matching {}: {}",
        filter != nullptr ? filter->ToString() : "any expression", conflict_path.value());
  }
  return {};
}

Status MergingSnapshotUpdate::ValidateDataFilesExist(
    const TableMetadata& metadata, int64_t starting_snapshot_id,
    const std::unordered_set<std::string>& file_paths, bool allow_deletes,
    std::shared_ptr<Expression> filter, const std::shared_ptr<Snapshot>& parent,
    std::shared_ptr<FileIO> io, bool case_sensitive) {
  if (parent == nullptr || file_paths.empty()) {
    return {};
  }

  ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());
  ICEBERG_ASSIGN_OR_RAISE(
      auto ancestors,
      ValidationAncestorsBetween(metadata, parent->snapshot_id, starting_snapshot_id));

  // Build the full set of matching snapshot IDs first, then scan their manifests.
  // The full set must be known before filtering manifests, since a manifest may have
  // been written by a different snapshot in the ancestry range.
  // Included operations: OVERWRITE and REPLACE always; DELETE when allow_deletes is
  // false.
  std::unordered_set<int64_t> matching_snapshot_ids;
  for (const auto& snap : ancestors) {
    auto op = snap->Operation();
    if (op == DataOperation::kOverwrite || op == DataOperation::kReplace) {
      matching_snapshot_ids.insert(snap->snapshot_id);
    } else if (!allow_deletes && op == DataOperation::kDelete) {
      matching_snapshot_ids.insert(snap->snapshot_id);
    }
  }

  // Build a metrics evaluator for the conflict-detection filter, if provided.
  std::unique_ptr<InclusiveMetricsEvaluator> evaluator;
  if (filter != nullptr) {
    ICEBERG_ASSIGN_OR_RAISE(
        evaluator, InclusiveMetricsEvaluator::Make(filter, *schema, case_sensitive));
  }

  for (const auto& snapshot : ancestors) {
    if (!matching_snapshot_ids.contains(snapshot->snapshot_id)) {
      continue;
    }
    auto cached = SnapshotCache(snapshot.get());
    ICEBERG_ASSIGN_OR_RAISE(auto data_manifests, cached.DataManifests(io));

    for (const auto& manifest : data_manifests) {
      if (!matching_snapshot_ids.contains(manifest.added_snapshot_id)) {
        continue;
      }
      ICEBERG_ASSIGN_OR_RAISE(auto spec,
                              metadata.PartitionSpecById(manifest.partition_spec_id));
      ICEBERG_ASSIGN_OR_RAISE(auto reader,
                              ManifestReader::Make(manifest, io, schema, spec));
      ICEBERG_ASSIGN_OR_RAISE(auto entries, reader->Entries());

      for (const auto& entry : entries) {
        if (entry.status != ManifestStatus::kDeleted) {
          continue;
        }
        if (entry.data_file == nullptr) {
          continue;
        }
        if (!file_paths.contains(entry.data_file->file_path)) {
          continue;
        }
        if (evaluator != nullptr) {
          ICEBERG_ASSIGN_OR_RAISE(bool matches, evaluator->Evaluate(*entry.data_file));
          if (!matches) {
            continue;
          }
        }
        return InvalidArgument("Cannot commit, missing data files: {} in snapshot {}",
                               entry.data_file->file_path, snapshot->snapshot_id);
      }
    }
  }
  return {};
}

Status MergingSnapshotUpdate::ValidateNoNewDeletesForDataFiles(
    const TableMetadata& metadata, int64_t starting_snapshot_id,
    const DataFileSet& replaced_files, const std::shared_ptr<Snapshot>& parent,
    std::shared_ptr<FileIO> io, bool ignore_equality_deletes) {
  if (parent == nullptr || replaced_files.empty() || metadata.format_version < 2) {
    return {};
  }

  // Build an index of delete files added since starting_snapshot_id.
  // Covers both position and equality deletes; the caller controls whether
  // equality deletes are ignored.
  ICEBERG_ASSIGN_OR_RAISE(auto deletes, AddedDeleteFiles(metadata, starting_snapshot_id,
                                                         nullptr, nullptr, parent, io));

  if (deletes->empty()) {
    return {};
  }

  // Compute the starting sequence number for the data file check.
  int64_t starting_seq = TableMetadata::kInitialSequenceNumber;
  if (auto snap_result = metadata.SnapshotById(starting_snapshot_id);
      snap_result.has_value()) {
    starting_seq = snap_result.value()->sequence_number;
  }

  for (const auto& data_file : replaced_files) {
    ICEBERG_ASSIGN_OR_RAISE(auto delete_files,
                            deletes->ForDataFile(starting_seq, *data_file));
    if (ignore_equality_deletes) {
      // Only fail on position deletes — equality deletes at higher sequence numbers
      // still apply to the rewritten files and are not a conflict.
      for (const auto& df : delete_files) {
        if (df->content == DataFile::Content::kPositionDeletes) {
          return InvalidArgument(
              "Cannot commit, found new position delete for replaced data file: {}",
              data_file->file_path);
        }
      }
    } else {
      if (!delete_files.empty()) {
        return InvalidArgument(
            "Cannot commit, found new delete for replaced data file: {}",
            data_file->file_path);
      }
    }
  }
  return {};
}

Status MergingSnapshotUpdate::ValidateAddedDataFiles(
    const TableMetadata& metadata, int64_t starting_snapshot_id,
    const PartitionSet& partition_set, const std::shared_ptr<Snapshot>& parent,
    std::shared_ptr<FileIO> io) {
  if (parent == nullptr) {
    return {};
  }

  ICEBERG_ASSIGN_OR_RAISE(
      auto history, ValidationHistory(metadata, parent->snapshot_id, starting_snapshot_id,
                                      {DataOperation::kAppend, DataOperation::kOverwrite},
                                      ManifestContent::kData, io));
  ICEBERG_ASSIGN_OR_RAISE(
      auto conflict_path,
      FindMatchingDataFile(metadata, history.manifests, ManifestStatus::kAdded, nullptr,
                           &partition_set, io, /*case_sensitive=*/true));
  if (conflict_path.has_value()) {
    return InvalidArgument(
        "Found conflicting files that can contain rows in validated partitions: {}",
        conflict_path.value());
  }
  return {};
}

Status MergingSnapshotUpdate::ValidateNoNewDeletesForDataFiles(
    const TableMetadata& metadata, int64_t starting_snapshot_id,
    std::shared_ptr<Expression> data_filter, const DataFileSet& replaced_files,
    const std::shared_ptr<Snapshot>& parent, std::shared_ptr<FileIO> io) {
  if (parent == nullptr || replaced_files.empty() || metadata.format_version < 2) {
    return {};
  }

  ICEBERG_ASSIGN_OR_RAISE(auto deletes, AddedDeleteFiles(metadata, starting_snapshot_id,
                                                         nullptr, nullptr, parent, io));
  if (deletes->empty()) {
    return {};
  }

  int64_t starting_seq = TableMetadata::kInitialSequenceNumber;
  if (auto snap_result = metadata.SnapshotById(starting_snapshot_id);
      snap_result.has_value()) {
    starting_seq = snap_result.value()->sequence_number;
  }

  ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());
  std::unique_ptr<InclusiveMetricsEvaluator> evaluator;
  if (data_filter != nullptr) {
    ICEBERG_ASSIGN_OR_RAISE(evaluator,
                            InclusiveMetricsEvaluator::Make(data_filter, *schema,
                                                            /*case_sensitive=*/true));
  }

  for (const auto& data_file : replaced_files) {
    ICEBERG_ASSIGN_OR_RAISE(auto delete_files,
                            deletes->ForDataFile(starting_seq, *data_file));
    for (const auto& delete_file : delete_files) {
      if (evaluator != nullptr) {
        ICEBERG_ASSIGN_OR_RAISE(bool matches, evaluator->Evaluate(*delete_file));
        if (!matches) {
          continue;
        }
      }
      return InvalidArgument("Cannot commit, found new delete for replaced data file: {}",
                             data_file->file_path);
    }
  }
  return {};
}

Status MergingSnapshotUpdate::ValidateNoNewDeleteFiles(
    const TableMetadata& metadata, int64_t starting_snapshot_id,
    std::shared_ptr<Expression> data_filter, const std::shared_ptr<Snapshot>& parent,
    std::shared_ptr<FileIO> io) {
  ICEBERG_ASSIGN_OR_RAISE(auto deletes, AddedDeleteFiles(metadata, starting_snapshot_id,
                                                         nullptr, nullptr, parent, io));
  auto referenced_delete_files = deletes->ReferencedDeleteFiles();

  ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());
  std::unique_ptr<InclusiveMetricsEvaluator> evaluator;
  if (data_filter != nullptr) {
    ICEBERG_ASSIGN_OR_RAISE(evaluator,
                            InclusiveMetricsEvaluator::Make(data_filter, *schema,
                                                            /*case_sensitive=*/true));
  }

  for (const auto& delete_file : referenced_delete_files) {
    if (evaluator != nullptr) {
      ICEBERG_ASSIGN_OR_RAISE(bool matches, evaluator->Evaluate(*delete_file));
      if (!matches) {
        continue;
      }
    }
    return InvalidArgument("Found new conflicting delete files: {}",
                           delete_file->file_path);
  }
  return {};
}

Status MergingSnapshotUpdate::ValidateNoNewDeleteFiles(
    const TableMetadata& metadata, int64_t starting_snapshot_id,
    const PartitionSet& partition_set, const std::shared_ptr<Snapshot>& parent,
    std::shared_ptr<FileIO> io) {
  ICEBERG_ASSIGN_OR_RAISE(auto deletes, AddedDeleteFiles(metadata, starting_snapshot_id,
                                                         nullptr, nullptr, parent, io));
  auto referenced_delete_files = deletes->ReferencedDeleteFiles();
  for (const auto& delete_file : referenced_delete_files) {
    if (!delete_file->partition_spec_id.has_value() ||
        !partition_set.contains(delete_file->partition_spec_id.value(),
                                delete_file->partition)) {
      continue;
    }
    return InvalidArgument(
        "Found new conflicting delete files in validated partitions: {}",
        delete_file->file_path);
  }
  return {};
}

Status MergingSnapshotUpdate::ValidateDeletedDataFiles(
    const TableMetadata& metadata, int64_t starting_snapshot_id,
    std::shared_ptr<Expression> data_filter, const std::shared_ptr<Snapshot>& parent,
    std::shared_ptr<FileIO> io) {
  if (parent == nullptr) {
    return {};
  }

  ICEBERG_ASSIGN_OR_RAISE(
      auto history, ValidationHistory(metadata, parent->snapshot_id, starting_snapshot_id,
                                      {DataOperation::kOverwrite, DataOperation::kReplace,
                                       DataOperation::kDelete},
                                      ManifestContent::kData, io));
  ICEBERG_ASSIGN_OR_RAISE(
      auto conflict_path,
      FindMatchingDataFile(metadata, history.manifests, ManifestStatus::kDeleted,
                           data_filter, nullptr, io, /*case_sensitive=*/true));
  if (conflict_path.has_value()) {
    return InvalidArgument(
        "Found conflicting deleted files that can contain rows matching {}: {}",
        data_filter != nullptr ? data_filter->ToString() : "any expression",
        conflict_path.value());
  }
  return {};
}

Status MergingSnapshotUpdate::ValidateDeletedDataFiles(
    const TableMetadata& metadata, int64_t starting_snapshot_id,
    const PartitionSet& partition_set, const std::shared_ptr<Snapshot>& parent,
    std::shared_ptr<FileIO> io) {
  if (parent == nullptr) {
    return {};
  }

  ICEBERG_ASSIGN_OR_RAISE(
      auto history, ValidationHistory(metadata, parent->snapshot_id, starting_snapshot_id,
                                      {DataOperation::kOverwrite, DataOperation::kReplace,
                                       DataOperation::kDelete},
                                      ManifestContent::kData, io));
  ICEBERG_ASSIGN_OR_RAISE(
      auto conflict_path,
      FindMatchingDataFile(metadata, history.manifests, ManifestStatus::kDeleted, nullptr,
                           &partition_set, io, /*case_sensitive=*/true));
  if (conflict_path.has_value()) {
    return InvalidArgument("Found conflicting deleted files in validated partitions: {}",
                           conflict_path.value());
  }
  return {};
}

Result<std::unique_ptr<DeleteFileIndex>> MergingSnapshotUpdate::AddedDeleteFiles(
    const TableMetadata& metadata, int64_t starting_snapshot_id,
    std::shared_ptr<Expression> data_filter, std::shared_ptr<PartitionSet> partition_set,
    const std::shared_ptr<Snapshot>& parent, std::shared_ptr<FileIO> io,
    bool case_sensitive) {
  ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());

  if (parent == nullptr || metadata.format_version < 2) {
    ICEBERG_ASSIGN_OR_RAISE(auto specs_ref,
                            TableMetadataCache(&metadata).GetPartitionSpecsById());
    std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> specs_by_id(
        specs_ref.get().begin(), specs_ref.get().end());
    ICEBERG_ASSIGN_OR_RAISE(auto builder, DeleteFileIndex::BuilderFor(
                                              io, schema, std::move(specs_by_id), {}));
    return builder.Build();
  }

  ICEBERG_ASSIGN_OR_RAISE(
      auto history, ValidationHistory(metadata, parent->snapshot_id, starting_snapshot_id,
                                      {DataOperation::kOverwrite, DataOperation::kDelete},
                                      ManifestContent::kDeletes, io));

  // Compute the starting sequence number from the starting snapshot.
  int64_t starting_seq = TableMetadata::kInitialSequenceNumber;
  if (auto snap_result = metadata.SnapshotById(starting_snapshot_id);
      snap_result.has_value()) {
    starting_seq = snap_result.value()->sequence_number;
  }

  ICEBERG_ASSIGN_OR_RAISE(auto specs_ref,
                          TableMetadataCache(&metadata).GetPartitionSpecsById());
  std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> specs_by_id(
      specs_ref.get().begin(), specs_ref.get().end());

  ICEBERG_ASSIGN_OR_RAISE(auto builder,
                          DeleteFileIndex::BuilderFor(io, schema, std::move(specs_by_id),
                                                      std::move(history.manifests)));
  builder.AfterSequenceNumber(starting_seq);
  builder.CaseSensitive(case_sensitive);
  if (data_filter != nullptr) {
    builder.DataFilter(std::move(data_filter));
  }
  if (partition_set != nullptr) {
    builder.FilterPartitions(std::move(partition_set));
  }
  return builder.Build();
}

Status MergingSnapshotUpdate::ValidateAddedDVs(
    const TableMetadata& /*metadata*/, int64_t /*starting_snapshot_id*/,
    std::shared_ptr<Expression> /*conflict_filter*/,
    const std::shared_ptr<Snapshot>& /*parent*/, std::shared_ptr<FileIO> /*io*/) {
  return NotImplemented(
      "ValidateAddedDVs is not yet supported (deletion vectors require format v3)");
}

}  // namespace iceberg
