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
#include <memory>
#include <optional>
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
#include "iceberg/snapshot.h"
#include "iceberg/table.h"  // IWYU pragma: keep
#include "iceberg/table_metadata.h"
#include "iceberg/transaction.h"
#include "iceberg/util/executor_util_internal.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

void SetSnapshotId(ManifestFile& manifest, int64_t snapshot_id) {
  manifest.added_snapshot_id = snapshot_id;
}

struct RewriteCandidate {
  ManifestFile manifest;
  std::shared_ptr<PartitionSpec> spec;
};

struct ManifestEntries {
  ManifestFile manifest;
  std::shared_ptr<PartitionSpec> spec;
  std::vector<ManifestEntry> entries;
};

struct RewriteWriter {
  std::shared_ptr<PartitionSpec> spec;
  ManifestContent content;
  std::unique_ptr<ManifestWriter> writer;
};

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
  // Reject added/deleted files unconditionally, matching Java's checkArgument. A
  // missing count is treated as non-zero (has_*_files defaults to true), so the
  // error is reported at the AddManifest call site rather than deferred to Apply.
  ICEBERG_BUILDER_CHECK(!manifest.has_added_files(),
                        "Cannot add manifest with added files");
  ICEBERG_BUILDER_CHECK(!manifest.has_deleted_files(),
                        "Cannot add manifest with deleted files");
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

Status RewriteManifests::ValidateTargetBranch(const std::string& branch) const {
  return NotSupported(
      "Cannot commit to branch {}: RewriteManifests does not support branch commits",
      branch);
}

std::string RewriteManifests::operation() { return DataOperation::kReplace; }

Result<std::vector<ManifestFile>> RewriteManifests::Apply(
    const TableMetadata& /*metadata_to_update*/,
    const std::shared_ptr<Snapshot>& snapshot) {
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
    ICEBERG_ASSIGN_OR_RAISE(auto rewritten, Rewrite(current_manifests));
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

  ICEBERG_RETURN_UNEXPECTED(ValidateActiveFiles());

  std::vector<ManifestFile> manifests;
  manifests.reserve(new_manifests_.size() + added_manifests_.size() +
                    rewritten_added_manifests_.size() + kept_manifests_.size());

  const int64_t snapshot_id = SnapshotId();
  for (auto& manifest : new_manifests_) {
    SetSnapshotId(manifest, snapshot_id);
    manifests.push_back(manifest);
  }
  for (auto& manifest : added_manifests_) {
    SetSnapshotId(manifest, snapshot_id);
    manifests.push_back(manifest);
  }
  for (auto& manifest : rewritten_added_manifests_) {
    SetSnapshotId(manifest, snapshot_id);
    manifests.push_back(manifest);
  }
  // Kept manifests are carried over unchanged, matching Java which adds them
  // as-is without recomputing counts.
  for (const auto& manifest : kept_manifests_) {
    manifests.push_back(manifest);
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

Status RewriteManifests::ValidateActiveFiles() const {
  // Compare the number of active (added + existing) files between created and
  // replaced manifests using the persisted manifest counts, mirroring Java's
  // BaseRewriteManifests.validateFilesCounts. This avoids re-reading manifest
  // entries on every apply, including commit retries.
  auto accumulate_active_files = [](const std::vector<ManifestFile>& manifests,
                                    int64_t& active_files) -> Status {
    for (const auto& manifest : manifests) {
      if (!manifest.added_files_count.has_value() ||
          !manifest.existing_files_count.has_value()) {
        return ValidationFailed("Missing file counts in {}", manifest.manifest_path);
      }
      active_files += manifest.added_files_count.value();
      active_files += manifest.existing_files_count.value();
    }
    return {};
  };

  int64_t created_active_files = 0;
  ICEBERG_RETURN_UNEXPECTED(
      accumulate_active_files(new_manifests_, created_active_files));
  ICEBERG_RETURN_UNEXPECTED(
      accumulate_active_files(added_manifests_, created_active_files));
  ICEBERG_RETURN_UNEXPECTED(
      accumulate_active_files(rewritten_added_manifests_, created_active_files));

  int64_t replaced_active_files = 0;
  ICEBERG_RETURN_UNEXPECTED(
      accumulate_active_files(rewritten_manifests_, replaced_active_files));
  ICEBERG_RETURN_UNEXPECTED(
      accumulate_active_files(deleted_manifests_, replaced_active_files));

  if (created_active_files != replaced_active_files) {
    return ValidationFailed(
        "Replaced and created manifests must have the same number of active files: {} "
        "(new), {} (old)",
        created_active_files, replaced_active_files);
  }
  return {};
}

Result<ManifestFile> RewriteManifests::CopyManifest(const ManifestFile& manifest) {
  ICEBERG_ASSIGN_OR_RAISE(auto schema, base().Schema());
  ICEBERG_ASSIGN_OR_RAISE(auto spec,
                          base().PartitionSpecById(manifest.partition_spec_id));
  // For a rewritten manifest all entries must already carry explicit snapshot ids.
  // Use empty inheritable metadata so reading throws if any snapshot id is missing,
  // and existing snapshot ids are preserved (matching Java copyRewriteManifest).
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

  ICEBERG_ASSIGN_OR_RAISE(auto schema, base().Schema());
  std::vector<RewriteCandidate> rewrite_candidates;
  rewrite_candidates.reserve(current_manifests.size());

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
    ICEBERG_ASSIGN_OR_RAISE(auto spec,
                            base().PartitionSpecById(manifest.partition_spec_id));
    rewrite_candidates.push_back(
        RewriteCandidate{.manifest = manifest, .spec = std::move(spec)});
  }

  auto file_io = ctx_->table->io();
  ICEBERG_ASSIGN_OR_RAISE(
      auto manifest_entries,
      ParallelCollect(
          plan_executor(), rewrite_candidates,
          [&](const RewriteCandidate& candidate) -> Result<std::vector<ManifestEntries>> {
            ICEBERG_ASSIGN_OR_RAISE(
                auto reader, ManifestReader::Make(candidate.manifest, file_io, schema,
                                                  candidate.spec));
            ICEBERG_ASSIGN_OR_RAISE(auto entries, reader->LiveEntries());
            std::vector<ManifestEntries> result;
            result.push_back(ManifestEntries{.manifest = candidate.manifest,
                                             .spec = candidate.spec,
                                             .entries = std::move(entries)});
            return result;
          }));

  std::unordered_map<WriterKey, RewriteWriter, WriterKeyHash> writers;

  auto close_writer =
      [](RewriteWriter& rewrite_writer) -> Result<std::optional<ManifestFile>> {
    if (rewrite_writer.writer == nullptr) {
      return std::nullopt;
    }
    ICEBERG_RETURN_UNEXPECTED(rewrite_writer.writer->Close());
    ICEBERG_ASSIGN_OR_RAISE(auto manifest_file, rewrite_writer.writer->ToManifestFile());
    rewrite_writer.writer.reset();
    return manifest_file;
  };

  auto new_writer = [this, &schema](const RewriteWriter& rewrite_writer)
      -> Result<std::unique_ptr<ManifestWriter>> {
    return ManifestWriter::MakeWriter(base().format_version, SnapshotId(), ManifestPath(),
                                      ctx_->table->io(), rewrite_writer.spec, schema,
                                      rewrite_writer.content);
  };

  std::vector<ManifestFile> result;
  for (const auto& manifest_entry : manifest_entries) {
    for (const auto& entry : manifest_entry.entries) {
      ICEBERG_PRECHECK(entry.data_file != nullptr,
                       "Manifest entry in {} is missing data_file",
                       manifest_entry.manifest.manifest_path);
      auto key =
          WriterKey{cluster_by_func_ ? cluster_by_func_(*entry.data_file) : std::string{},
                    manifest_entry.manifest.partition_spec_id};

      auto writer_it = writers.find(key);
      if (writer_it == writers.end()) {
        auto [inserted_it, _] = writers.emplace(
            key, RewriteWriter{.spec = manifest_entry.spec,
                               .content = manifest_entry.manifest.content});
        writer_it = inserted_it;
      }

      auto& rewrite_writer = writer_it->second;
      if (rewrite_writer.writer == nullptr) {
        ICEBERG_ASSIGN_OR_RAISE(rewrite_writer.writer, new_writer(rewrite_writer));
      } else {
        ICEBERG_ASSIGN_OR_RAISE(auto length, rewrite_writer.writer->length());
        if (length >= target_manifest_size_bytes()) {
          ICEBERG_ASSIGN_OR_RAISE(auto manifest_file, close_writer(rewrite_writer));
          if (manifest_file.has_value()) {
            result.push_back(std::move(manifest_file).value());
          }
          ICEBERG_ASSIGN_OR_RAISE(rewrite_writer.writer, new_writer(rewrite_writer));
        }
      }

      ICEBERG_RETURN_UNEXPECTED(rewrite_writer.writer->WriteExistingEntry(entry));
      ++entry_count_;
    }
  }

  for (auto& [_, writer] : writers) {
    ICEBERG_ASSIGN_OR_RAISE(auto manifest_file, close_writer(writer));
    if (manifest_file.has_value()) {
      result.push_back(std::move(manifest_file).value());
    }
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
