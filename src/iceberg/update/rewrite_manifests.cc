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

#include "iceberg/update/rewrite_manifests.h"

#include <algorithm>
#include <iterator>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "iceberg/constants.h"
#include "iceberg/inheritable_metadata.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/manifest/manifest_writer.h"
#include "iceberg/manifest/rolling_manifest_writer.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"  // IWYU pragma: keep
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"
#include "iceberg/transaction.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

void SetSnapshotId(ManifestFile& manifest, int64_t snapshot_id) {
  manifest.added_snapshot_id = snapshot_id;
}

}  // namespace

Result<std::unique_ptr<RewriteManifests>> RewriteManifests::Make(
    std::string table_name, std::shared_ptr<TransactionContext> ctx) {
  ICEBERG_PRECHECK(!table_name.empty(), "Table name cannot be empty");
  ICEBERG_PRECHECK(ctx != nullptr, "Cannot create RewriteManifests without a context");
  return std::unique_ptr<RewriteManifests>(
      new RewriteManifests(std::move(table_name), std::move(ctx)));
}

RewriteManifests::RewriteManifests(std::string table_name,
                                   std::shared_ptr<TransactionContext> ctx)
    : SnapshotUpdate(std::move(ctx)), table_name_(std::move(table_name)) {}

RewriteManifests& RewriteManifests::ClusterBy(ClusterByFunc func) {
  ICEBERG_BUILDER_CHECK(static_cast<bool>(func), "Cluster function cannot be null");
  cluster_by_func_ = std::move(func);
  return *this;
}

RewriteManifests& RewriteManifests::RewriteIf(RewritePredicate predicate) {
  ICEBERG_BUILDER_CHECK(static_cast<bool>(predicate), "Rewrite predicate cannot be null");
  predicate_ = std::move(predicate);
  return *this;
}

RewriteManifests& RewriteManifests::DeleteManifest(const ManifestFile& manifest) {
  auto [_, inserted] = deleted_manifest_paths_.insert(manifest.manifest_path);
  if (inserted) {
    deleted_manifests_.push_back(manifest);
  }
  return *this;
}

RewriteManifests& RewriteManifests::AddManifest(const ManifestFile& manifest) {
  if (manifest.added_files_count.has_value()) {
    ICEBERG_BUILDER_CHECK(!manifest.has_added_files(),
                          "Cannot add manifest with added files");
  }
  if (manifest.deleted_files_count.has_value()) {
    ICEBERG_BUILDER_CHECK(!manifest.has_deleted_files(),
                          "Cannot add manifest with deleted files");
  }
  ICEBERG_BUILDER_CHECK(manifest.added_snapshot_id == kInvalidSnapshotId,
                        "Snapshot id must be assigned during commit");
  ICEBERG_BUILDER_CHECK(manifest.sequence_number == kInvalidSequenceNumber,
                        "Sequence number must be assigned during commit");

  if (can_inherit_snapshot_id()) {
    added_manifests_.push_back(manifest);
  } else {
    // The manifest must be rewritten with this update's snapshot ID. CopyManifest
    // also validates that the manifest only contains existing entries.
    ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto copied_manifest, CopyManifest(manifest));
    rewritten_added_manifests_.push_back(std::move(copied_manifest));
  }
  return *this;
}

std::string RewriteManifests::operation() { return DataOperation::kReplace; }

Result<std::vector<ManifestFile>> RewriteManifests::Apply(
    const TableMetadata& metadata_to_update, const std::shared_ptr<Snapshot>& snapshot) {
  ICEBERG_PRECHECK(snapshot != nullptr,
                   "Cannot rewrite manifests without a current snapshot");

  SnapshotCache cached_snapshot(snapshot.get());
  ICEBERG_ASSIGN_OR_RAISE(auto current_manifests,
                          cached_snapshot.Manifests(ctx_->table->io()));

  std::unordered_set<std::string> current_manifest_paths;
  current_manifest_paths.reserve(current_manifests.size());
  for (const auto& manifest : current_manifests) {
    current_manifest_paths.insert(manifest.manifest_path);
  }

  ICEBERG_RETURN_UNEXPECTED(
      ValidateDeletedManifests(current_manifest_paths, snapshot->snapshot_id));

  if (RequiresRewrite(current_manifest_paths)) {
    ICEBERG_ASSIGN_OR_RAISE(auto rewritten,
                            Rewrite(metadata_to_update, current_manifests));
    new_manifests_ = std::move(rewritten);
  } else {
    // Keep any existing manifests as-is that were not processed. Previously
    // created manifests in new_manifests_ are reused across commit retries.
    kept_manifests_.clear();
    for (const auto& manifest : current_manifests) {
      if (!rewritten_manifest_paths_.contains(manifest.manifest_path) &&
          !deleted_manifest_paths_.contains(manifest.manifest_path)) {
        kept_manifests_.push_back(manifest);
      }
    }
  }

  ICEBERG_RETURN_UNEXPECTED(ValidateAddedManifests());
  ICEBERG_RETURN_UNEXPECTED(ValidateActiveFiles());

  std::vector<ManifestFile> manifests;
  manifests.reserve(new_manifests_.size() + added_manifests_.size() +
                    rewritten_added_manifests_.size() + kept_manifests_.size());

  const int64_t snapshot_id = SnapshotId();
  for (auto& manifest : new_manifests_) {
    SetSnapshotId(manifest, snapshot_id);
    ICEBERG_ASSIGN_OR_RAISE(auto manifest_with_counts, FillMissingCounts(manifest));
    manifests.push_back(std::move(manifest_with_counts));
  }
  for (auto& manifest : added_manifests_) {
    SetSnapshotId(manifest, snapshot_id);
    ICEBERG_ASSIGN_OR_RAISE(auto manifest_with_counts, FillMissingCounts(manifest));
    manifests.push_back(std::move(manifest_with_counts));
  }
  for (auto& manifest : rewritten_added_manifests_) {
    SetSnapshotId(manifest, snapshot_id);
    ICEBERG_ASSIGN_OR_RAISE(auto manifest_with_counts, FillMissingCounts(manifest));
    manifests.push_back(std::move(manifest_with_counts));
  }
  for (const auto& manifest : kept_manifests_) {
    ICEBERG_ASSIGN_OR_RAISE(auto manifest_with_counts, FillMissingCounts(manifest));
    manifests.push_back(std::move(manifest_with_counts));
  }

  manifest_count_summary_ = BuildManifestCountSummary(
      manifests,
      static_cast<int32_t>(rewritten_manifests_.size() + deleted_manifests_.size()));
  return manifests;
}

std::unordered_map<std::string, std::string> RewriteManifests::Summary() {
  summary_.Clear();
  summary_.SetPartitionSummaryLimit(0);
  for (const auto& [property, value] : custom_summary_properties_) {
    summary_.Set(property, value);
  }
  summary_.Merge(manifest_count_summary_);
  summary_.Set(SnapshotSummaryFields::kEntriesProcessed, std::to_string(entry_count_));
  return summary_.Build();
}

void RewriteManifests::SetSummaryProperty(const std::string& property,
                                          const std::string& value) {
  custom_summary_properties_[property] = value;
  SnapshotUpdate::SetSummaryProperty(property, value);
}

Status RewriteManifests::CleanUncommitted(
    const std::unordered_set<std::string>& committed) {
  if (committed.empty() && !cleanup_all_) {
    return {};
  }
  ICEBERG_RETURN_UNEXPECTED(DeleteUncommitted(new_manifests_, committed,
                                              /*clear=*/false));
  ICEBERG_RETURN_UNEXPECTED(DeleteUncommitted(rewritten_added_manifests_, committed,
                                              /*clear=*/false));
  return {};
}

Status RewriteManifests::Finalize(Result<const TableMetadata*> commit_result) {
  if (!commit_result.has_value() &&
      commit_result.error().kind != ErrorKind::kCommitStateUnknown) {
    cleanup_all_ = true;
  }
  auto status = SnapshotUpdate::Finalize(std::move(commit_result));
  cleanup_all_ = false;
  return status;
}

bool RewriteManifests::RequiresRewrite(
    const std::unordered_set<std::string>& current_manifest_paths) const {
  if (!cluster_by_func_) {
    // manifests are deleted and added directly so don't perform a rewrite
    return false;
  }
  if (rewritten_manifests_.empty()) {
    // nothing yet processed so perform a full rewrite
    return true;
  }

  // if any processed manifest is not in the current manifest list, perform a full rewrite
  return std::ranges::any_of(rewritten_manifests_, [&](const ManifestFile& manifest) {
    return !current_manifest_paths.contains(manifest.manifest_path);
  });
}

bool RewriteManifests::MatchesPredicate(const ManifestFile& manifest) const {
  return !predicate_ || predicate_(manifest);
}

Status RewriteManifests::ValidateDeletedManifests(
    const std::unordered_set<std::string>& current_manifest_paths,
    int64_t current_snapshot_id) const {
  for (const auto& manifest : deleted_manifests_) {
    if (!current_manifest_paths.contains(manifest.manifest_path)) {
      return ValidationFailed(
          "Deleted manifest {} could not be found in the latest snapshot {}",
          manifest.manifest_path, current_snapshot_id);
    }
  }
  return {};
}

Status RewriteManifests::ValidateAddedManifests() const {
  // Manifests added via the inherit path are kept as-is, so their entries must be
  // verified here because ManifestFile counts may be absent. Manifests on the copy
  // path are already validated while being copied in CopyManifest.
  for (const auto& manifest : added_manifests_) {
    ICEBERG_ASSIGN_OR_RAISE(auto entries, Entries(manifest));
    for (const auto& entry : entries) {
      if (entry.status == ManifestStatus::kAdded) {
        return ValidationFailed("Cannot add manifest with added files");
      }
      if (entry.status == ManifestStatus::kDeleted) {
        return ValidationFailed("Cannot add manifest with deleted files");
      }
    }
  }
  return {};
}

Status RewriteManifests::ValidateActiveFiles() const {
  std::vector<ManifestFile> created;
  created.reserve(new_manifests_.size() + added_manifests_.size() +
                  rewritten_added_manifests_.size());
  created.insert(created.end(), new_manifests_.begin(), new_manifests_.end());
  created.insert(created.end(), added_manifests_.begin(), added_manifests_.end());
  created.insert(created.end(), rewritten_added_manifests_.begin(),
                 rewritten_added_manifests_.end());

  std::vector<ManifestFile> replaced;
  replaced.reserve(rewritten_manifests_.size() + deleted_manifests_.size());
  replaced.insert(replaced.end(), rewritten_manifests_.begin(),
                  rewritten_manifests_.end());
  replaced.insert(replaced.end(), deleted_manifests_.begin(), deleted_manifests_.end());

  ICEBERG_ASSIGN_OR_RAISE(auto created_files, ActiveFiles(created));
  ICEBERG_ASSIGN_OR_RAISE(auto replaced_files, ActiveFiles(replaced));
  if (created_files.size() != replaced_files.size()) {
    return ValidationFailed(
        "Replaced and created manifests must have the same number of active files: {} "
        "(new), {} (old)",
        created_files.size(), replaced_files.size());
  }

  // Group created files by path so each replaced file can be matched in amortized
  // constant time instead of scanning the whole list.
  std::unordered_multimap<std::string, DataFile> created_by_path;
  created_by_path.reserve(created_files.size());
  for (auto& file : created_files) {
    created_by_path.emplace(file.file_path, std::move(file));
  }

  for (const auto& file : replaced_files) {
    auto [begin, end] = created_by_path.equal_range(file.file_path);
    auto match =
        std::find_if(begin, end, [&](const auto& entry) { return entry.second == file; });
    if (match == end) {
      return ValidationFailed(
          "Replaced and created manifests must have the same active files");
    }
    created_by_path.erase(match);
  }
  return {};
}

Result<std::vector<ManifestEntry>> RewriteManifests::Entries(
    const ManifestFile& manifest) const {
  ICEBERG_ASSIGN_OR_RAISE(auto schema, base().Schema());
  ICEBERG_ASSIGN_OR_RAISE(auto spec,
                          base().PartitionSpecById(manifest.partition_spec_id));

  if (manifest.added_snapshot_id != kInvalidSnapshotId) {
    ICEBERG_ASSIGN_OR_RAISE(
        auto reader, ManifestReader::Make(manifest, ctx_->table->io(), schema, spec));
    return reader->Entries();
  }

  ICEBERG_ASSIGN_OR_RAISE(auto inheritable_metadata,
                          InheritableMetadataFactory::ForCopy(/*snapshot_id=*/0));
  ICEBERG_ASSIGN_OR_RAISE(
      auto reader,
      ManifestReader::Make(manifest.manifest_path, manifest.manifest_length,
                           ctx_->table->io(), schema, spec,
                           std::move(inheritable_metadata), manifest.first_row_id,
                           /*is_committed=*/false));
  return reader->Entries();
}

Result<std::vector<DataFile>> RewriteManifests::ActiveFiles(
    const std::vector<ManifestFile>& manifests) const {
  std::vector<DataFile> active_files;
  for (const auto& manifest : manifests) {
    ICEBERG_ASSIGN_OR_RAISE(auto entries, Entries(manifest));
    active_files.reserve(active_files.size() + entries.size());
    for (const auto& entry : entries) {
      if (!entry.IsAlive()) {
        continue;
      }
      ICEBERG_PRECHECK(entry.data_file != nullptr,
                       "Manifest entry in {} is missing data_file",
                       manifest.manifest_path);
      auto file = *entry.data_file;
      file.partition_spec_id = manifest.partition_spec_id;
      active_files.push_back(std::move(file));
    }
  }
  return active_files;
}

Result<ManifestFile> RewriteManifests::FillMissingCounts(
    const ManifestFile& manifest) const {
  if (manifest.added_files_count.has_value() &&
      manifest.existing_files_count.has_value() &&
      manifest.deleted_files_count.has_value() && manifest.added_rows_count.has_value() &&
      manifest.existing_rows_count.has_value() &&
      manifest.deleted_rows_count.has_value()) {
    return manifest;
  }

  ICEBERG_ASSIGN_OR_RAISE(auto entries, Entries(manifest));
  ManifestFile manifest_with_counts = manifest;
  int32_t added_files = 0;
  int64_t added_rows = 0;
  int32_t existing_files = 0;
  int64_t existing_rows = 0;
  int32_t deleted_files = 0;
  int64_t deleted_rows = 0;

  for (const auto& entry : entries) {
    ICEBERG_PRECHECK(entry.data_file != nullptr,
                     "Manifest entry in {} is missing data_file", manifest.manifest_path);
    switch (entry.status) {
      case ManifestStatus::kAdded:
        ++added_files;
        added_rows += entry.data_file->record_count;
        break;
      case ManifestStatus::kExisting:
        ++existing_files;
        existing_rows += entry.data_file->record_count;
        break;
      case ManifestStatus::kDeleted:
        ++deleted_files;
        deleted_rows += entry.data_file->record_count;
        break;
    }
  }

  manifest_with_counts.added_files_count = added_files;
  manifest_with_counts.added_rows_count = added_rows;
  manifest_with_counts.existing_files_count = existing_files;
  manifest_with_counts.existing_rows_count = existing_rows;
  manifest_with_counts.deleted_files_count = deleted_files;
  manifest_with_counts.deleted_rows_count = deleted_rows;
  return manifest_with_counts;
}

Result<ManifestFile> RewriteManifests::CopyManifest(const ManifestFile& manifest) {
  ICEBERG_ASSIGN_OR_RAISE(auto schema, base().Schema());
  ICEBERG_ASSIGN_OR_RAISE(auto spec,
                          base().PartitionSpecById(manifest.partition_spec_id));
  ICEBERG_ASSIGN_OR_RAISE(auto inheritable_metadata, InheritableMetadataFactory::Empty());
  ICEBERG_ASSIGN_OR_RAISE(
      auto reader,
      ManifestReader::Make(manifest.manifest_path, manifest.manifest_length,
                           ctx_->table->io(), schema, spec,
                           std::move(inheritable_metadata), manifest.first_row_id,
                           /*is_committed=*/false));
  ICEBERG_ASSIGN_OR_RAISE(auto entries, reader->Entries());

  ICEBERG_ASSIGN_OR_RAISE(
      auto writer,
      ManifestWriter::MakeWriter(base().format_version, SnapshotId(), ManifestPath(),
                                 ctx_->table->io(), std::move(spec), std::move(schema),
                                 manifest.content, manifest.first_row_id));
  for (const auto& entry : entries) {
    // A rewritten added manifest may only contain existing entries.
    if (entry.status == ManifestStatus::kAdded) {
      return ValidationFailed("Cannot add manifest with added files");
    }
    if (entry.status == ManifestStatus::kDeleted) {
      return ValidationFailed("Cannot add manifest with deleted files");
    }
    ICEBERG_RETURN_UNEXPECTED(writer->WriteExistingEntry(entry));
  }
  ICEBERG_RETURN_UNEXPECTED(writer->Close());
  return writer->ToManifestFile();
}

Result<std::vector<ManifestFile>> RewriteManifests::Rewrite(
    const TableMetadata& metadata_to_update,
    std::span<const ManifestFile> current_manifests) {
  ResetRewriteState();

  using WriterKey = std::pair<std::string, int32_t>;
  struct WriterKeyHash {
    size_t operator()(const WriterKey& key) const {
      size_t seed = std::hash<std::string>{}(key.first);
      seed ^= std::hash<int32_t>{}(key.second) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
      return seed;
    }
  };

  ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata_to_update.Schema());
  std::unordered_map<WriterKey, std::unique_ptr<RollingManifestWriter>, WriterKeyHash>
      writers;

  for (const auto& manifest : current_manifests) {
    if (deleted_manifest_paths_.contains(manifest.manifest_path)) {
      continue;
    }
    if (manifest.content == ManifestContent::kDeletes || !MatchesPredicate(manifest)) {
      kept_manifests_.push_back(manifest);
      continue;
    }

    rewritten_manifests_.push_back(manifest);
    rewritten_manifest_paths_.insert(manifest.manifest_path);
    ICEBERG_ASSIGN_OR_RAISE(
        auto spec, metadata_to_update.PartitionSpecById(manifest.partition_spec_id));
    ICEBERG_ASSIGN_OR_RAISE(
        auto reader, ManifestReader::Make(manifest, ctx_->table->io(), schema, spec));
    ICEBERG_ASSIGN_OR_RAISE(auto entries, reader->LiveEntries());
    for (const auto& entry : entries) {
      ICEBERG_PRECHECK(entry.data_file != nullptr,
                       "Manifest entry in {} is missing data_file",
                       manifest.manifest_path);
      auto key =
          WriterKey{cluster_by_func_(*entry.data_file), manifest.partition_spec_id};

      auto writer_it = writers.find(key);
      if (writer_it == writers.end()) {
        auto writer_spec = spec;
        auto writer_schema = schema;
        auto [inserted_it, _] = writers.emplace(
            key,
            std::make_unique<RollingManifestWriter>(
                [this, writer_spec, writer_schema, content = manifest.content]()
                    -> Result<std::unique_ptr<ManifestWriter>> {
                  std::optional<int64_t> first_row_id = std::nullopt;
                  if (base().format_version >= 3 && content == ManifestContent::kData) {
                    // Rewritten manifests contain existing files only. Use a
                    // non-null manifest first_row_id so v3 manifest-list writing
                    // does not assign new row IDs for existing rows.
                    first_row_id = 0;
                  }
                  return ManifestWriter::MakeWriter(base().format_version, SnapshotId(),
                                                    ManifestPath(), ctx_->table->io(),
                                                    writer_spec, writer_schema, content,
                                                    first_row_id);
                },
                target_manifest_size_bytes()));
        writer_it = inserted_it;
      }

      ICEBERG_RETURN_UNEXPECTED(writer_it->second->WriteExistingEntry(entry));
      ++entry_count_;
    }
  }

  std::vector<ManifestFile> result;
  for (auto& [_, writer] : writers) {
    ICEBERG_RETURN_UNEXPECTED(writer->Close());
    ICEBERG_ASSIGN_OR_RAISE(auto manifests, writer->ToManifestFiles());
    result.insert(result.end(), std::make_move_iterator(manifests.begin()),
                  std::make_move_iterator(manifests.end()));
  }
  return result;
}

Status RewriteManifests::DeleteUncommitted(
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

void RewriteManifests::ResetRewriteState() {
  std::ignore = DeleteUncommitted(new_manifests_, {}, /*clear=*/true);
  entry_count_ = 0;
  kept_manifests_.clear();
  rewritten_manifests_.clear();
  rewritten_manifest_paths_.clear();
}

}  // namespace iceberg
