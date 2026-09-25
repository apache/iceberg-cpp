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
#include <array>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "iceberg/constants.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/manifest/manifest_util_internal.h"
#include "iceberg/manifest/manifest_writer.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"  // IWYU pragma: keep
#include "iceberg/table_metadata.h"
#include "iceberg/transaction.h"
#include "iceberg/util/executor_util_internal.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

struct RewriteCandidate {
  ManifestFile manifest;
  std::shared_ptr<PartitionSpec> spec;
};

struct ManifestStream {
  ManifestFile manifest;
  std::shared_ptr<PartitionSpec> spec;
  ManifestEntryStreamPtr entries;
};

struct RewriteWriter {
  std::shared_ptr<PartitionSpec> spec;
  std::unique_ptr<ManifestWriter> writer;
};

}  // namespace

Result<std::shared_ptr<RewriteManifests>> RewriteManifests::Make(
    std::string table_name, std::shared_ptr<TransactionContext> ctx) {
  ICEBERG_PRECHECK(!table_name.empty(), "Table name cannot be empty");
  ICEBERG_PRECHECK(ctx != nullptr, "Cannot create RewriteManifests without a context");
  return std::shared_ptr<RewriteManifests>(
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
  deleted_manifests_.insert(manifest);
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
  ICEBERG_BUILDER_CHECK(!manifest.added_snapshot_id.has_value() ||
                            manifest.added_snapshot_id == kInvalidSnapshotId,
                        "Snapshot id must be assigned during commit");
  ICEBERG_BUILDER_CHECK(manifest.sequence_number == kInvalidSequenceNumber,
                        "Sequence number must be assigned during commit");

  if (can_inherit_snapshot_id()) {
    added_manifests_.push_back(manifest);
  } else {
    // Retain the input so Apply can recreate the copy after retry cleanup.
    added_manifests_to_copy_.push_back(manifest);
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

  SnapshotReader snapshot_reader(snapshot.get());
  ICEBERG_ASSIGN_OR_RAISE(auto current_manifests,
                          snapshot_reader.Manifests(ctx_->table->io()));

  auto current_manifest_set =
      current_manifests | std::ranges::to<std::unordered_set<ManifestFile>>();

  ICEBERG_RETURN_UNEXPECTED(
      ValidateDeletedManifests(current_manifest_set, snapshot->snapshot_id));

  if (rewritten_added_manifests_.empty()) {
    for (const auto& manifest : added_manifests_to_copy_) {
      ICEBERG_ASSIGN_OR_RAISE(auto copied_manifest, CopyManifest(manifest));
      rewritten_added_manifests_.push_back(std::move(copied_manifest));
    }
  }

  if (RequiresRewrite(current_manifest_set)) {
    ICEBERG_RETURN_UNEXPECTED(Rewrite(current_manifests));
  } else {
    kept_manifests_ = current_manifests |
                      std::views::filter([this](const auto& manifest) {
                        return !rewritten_manifests_.contains(manifest) &&
                               !deleted_manifests_.contains(manifest);
                      }) |
                      std::ranges::to<std::vector>();
  }

  ICEBERG_RETURN_UNEXPECTED(ValidateFilesCounts());

  const std::array created_manifests{std::span(new_manifests_),
                                     std::span(added_manifests_),
                                     std::span(rewritten_added_manifests_)};
  auto manifests =
      created_manifests | std::views::join |
      std::views::transform([snapshot_id = SnapshotId()](ManifestFile manifest) {
        manifest.added_snapshot_id = snapshot_id;
        return manifest;
      }) |
      std::ranges::to<std::vector>();
  manifests.insert(manifests.end(), kept_manifests_.begin(), kept_manifests_.end());

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
  DeleteUncommitted(new_manifests_, committed);
  DeleteUncommitted(rewritten_added_manifests_, committed);
  entry_count_ = 0;
  kept_manifests_.clear();
  rewritten_manifests_.clear();
  manifest_count_summary_.Clear();
  return {};
}

bool RewriteManifests::RequiresRewrite(
    const std::unordered_set<ManifestFile>& current_manifests) const {
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
    return !current_manifests.contains(manifest);
  });
}

bool RewriteManifests::MatchesPredicate(const ManifestFile& manifest) const {
  return !predicate_ || predicate_(manifest);
}

Status RewriteManifests::ValidateDeletedManifests(
    const std::unordered_set<ManifestFile>& current_manifests,
    int64_t current_snapshot_id) const {
  for (const auto& manifest : deleted_manifests_) {
    if (!current_manifests.contains(manifest)) {
      return ValidationFailed(
          "Deleted manifest {} could not be found in the latest snapshot {}",
          manifest.manifest_path, current_snapshot_id);
    }
  }
  return {};
}

Status RewriteManifests::ValidateFilesCounts() const {
  auto accumulate_active_files = [](const auto& manifests,
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
  return CopyRewriteManifest(manifest, ctx_->table->io(), schema, spec, SnapshotId(),
                             ManifestPath(), base().format_version);
}

Status RewriteManifests::Rewrite(std::span<const ManifestFile> current_manifests) {
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
    if (deleted_manifests_.contains(manifest)) {
      continue;
    }
    if (manifest.content == ManifestContent::kDeletes || !MatchesPredicate(manifest)) {
      kept_manifests_.push_back(manifest);
      continue;
    }

    rewritten_manifests_.insert(manifest);
    ICEBERG_ASSIGN_OR_RAISE(auto spec,
                            base().PartitionSpecById(manifest.partition_spec_id));
    rewrite_candidates.push_back(
        RewriteCandidate{.manifest = manifest, .spec = std::move(spec)});
  }

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

  auto new_writer = [this, &schema](const RewriteWriter& rewrite_writer) {
    return ManifestWriter::MakeWriter(base().format_version, SnapshotId(), ManifestPath(),
                                      ctx_->table->io(), rewrite_writer.spec, schema,
                                      ManifestContent::kData);
  };

  auto write_entry = [&](const ManifestEntry& entry,
                         const ManifestStream& manifest_stream) -> Status {
    ICEBERG_PRECHECK(entry.data_file != nullptr,
                     "Manifest entry in {} is missing data_file",
                     manifest_stream.manifest.manifest_path);
    auto key = WriterKey{cluster_by_func_(*entry.data_file),
                         manifest_stream.manifest.partition_spec_id};

    auto writer_it = writers.find(key);
    if (writer_it == writers.end()) {
      auto [inserted_it, _] =
          writers.emplace(key, RewriteWriter{.spec = manifest_stream.spec});
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
          new_manifests_.push_back(std::move(manifest_file).value());
        }
        ICEBERG_ASSIGN_OR_RAISE(rewrite_writer.writer, new_writer(rewrite_writer));
      }
    }

    ICEBERG_RETURN_UNEXPECTED(rewrite_writer.writer->WriteExistingEntry(entry));
    ++entry_count_;
    return {};
  };

  // Capture read and write failures so all open writers can still be closed below.
  Status write_status = [&]() -> Status {
    // Open a bounded batch of streams concurrently, then consume entries incrementally.
    // Without an executor, keep only one reader open at a time.
    constexpr size_t kManifestReadBatchSize = 32;
    const size_t batch_size = plan_executor().has_value() ? kManifestReadBatchSize : 1;
    auto file_io = ctx_->table->io();
    for (size_t offset = 0; offset < rewrite_candidates.size(); offset += batch_size) {
      auto candidates =
          std::span(rewrite_candidates)
              .subspan(offset, std::min(batch_size, rewrite_candidates.size() - offset));
      ICEBERG_ASSIGN_OR_RAISE(
          auto streams,
          ParallelCollect(
              plan_executor(), candidates,
              [&](const RewriteCandidate& candidate)
                  -> Result<std::vector<ManifestStream>> {
                ICEBERG_ASSIGN_OR_RAISE(
                    auto reader, ManifestReader::Make(candidate.manifest, file_io, schema,
                                                      candidate.spec));
                ICEBERG_ASSIGN_OR_RAISE(auto entries, reader->LiveEntriesStream());
                std::vector<ManifestStream> result;
                result.push_back(ManifestStream{.manifest = candidate.manifest,
                                                .spec = candidate.spec,
                                                .entries = std::move(entries)});
                return result;
              }));
      for (auto& stream : streams) {
        while (true) {
          ICEBERG_ASSIGN_OR_RAISE(auto entry, stream.entries->Next());
          if (!entry.has_value()) {
            break;
          }
          ICEBERG_RETURN_UNEXPECTED(write_entry(*entry, stream));
        }
        stream.entries.reset();
      }
    }
    return {};
  }();

  // Keep the first close error, but continue closing the remaining writers.
  std::optional<Error> first_close_error;
  for (auto& [_, writer] : writers) {
    auto manifest_file_result = close_writer(writer);
    if (!manifest_file_result.has_value()) {
      if (!first_close_error.has_value()) {
        first_close_error = manifest_file_result.error();
      }
      continue;
    }
    auto manifest_file = std::move(manifest_file_result).value();
    if (manifest_file.has_value()) {
      new_manifests_.push_back(std::move(manifest_file).value());
    }
  }

  if (!write_status.has_value()) {
    auto error = std::move(write_status).error();
    if (first_close_error.has_value()) {
      error.message += "; additionally failed to close manifest writer: ";
      error.message += first_close_error->message;
    }
    return std::unexpected<Error>(std::move(error));
  }
  if (first_close_error.has_value()) {
    return std::unexpected<Error>(std::move(first_close_error).value());
  }
  return {};
}

void RewriteManifests::DeleteUncommitted(
    std::vector<ManifestFile>& manifests,
    const std::unordered_set<std::string>& committed) {
  for (const auto& manifest : manifests) {
    if (!committed.contains(manifest.manifest_path)) {
      std::ignore = DeleteFile(manifest.manifest_path);
    }
  }
  manifests.clear();
}

void RewriteManifests::ResetRewriteState() {
  DeleteUncommitted(new_manifests_, {});
  entry_count_ = 0;
  kept_manifests_.clear();
  rewritten_manifests_.clear();
}

}  // namespace iceberg
