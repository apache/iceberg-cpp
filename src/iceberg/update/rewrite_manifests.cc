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
#include <exception>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <string_view>
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
  const ManifestFile* manifest;
  std::shared_ptr<PartitionSpec> spec;
};

struct ManifestStream {
  const RewriteCandidate* candidate;
  ManifestEntryStreamPtr entries;
};

template <std::ranges::input_range Range>
Result<int64_t> CountActiveFiles(Range&& manifests) {
  int64_t count = 0;
  for (const auto& manifest : manifests) {
    if (!manifest.added_files_count.has_value() ||
        !manifest.existing_files_count.has_value()) {
      return ValidationFailed("Missing file counts in {}", manifest.manifest_path);
    }
    count += manifest.added_files_count.value();
    count += manifest.existing_files_count.value();
  }
  return count;
}

template <typename Fn>
Status CatchStatus(std::string_view operation, Fn&& fn) {
  try {
    return std::forward<Fn>(fn)();
  } catch (const std::exception& e) {
    return ValidationFailed("{} threw: {}", operation, e.what());
  } catch (...) {
    return ValidationFailed("{} threw an unknown exception", operation);
  }
}

Status FinishWithCloseStatus(Status rewrite_status, Status close_status) {
  // Keep the rewrite error when both operations fail.
  if (!rewrite_status.has_value()) {
    auto error = std::move(rewrite_status).error();
    if (!close_status.has_value()) {
      error.message += "; additionally failed to close manifest writer: ";
      error.message += close_status.error().message;
    }
    return ::iceberg::unexpected<Error>(std::move(error));
  }
  return close_status;
}

}  // namespace

class RewriteManifests::WriterWrapper {
 public:
  WriterWrapper(RewriteManifests* owner, std::shared_ptr<PartitionSpec> spec)
      : owner_(owner), spec_(std::move(spec)) {}

  Status AddEntry(const ManifestEntry& entry) {
    if (writer_ == nullptr) {
      ICEBERG_ASSIGN_OR_RAISE(writer_, owner_->NewManifestWriter(spec_));
    } else {
      ICEBERG_ASSIGN_OR_RAISE(auto length, writer_->length());
      if (length >= owner_->target_manifest_size_bytes()) {
        ICEBERG_RETURN_UNEXPECTED(Close());
        ICEBERG_ASSIGN_OR_RAISE(writer_, owner_->NewManifestWriter(spec_));
      }
    }
    return writer_->WriteExistingEntry(entry);
  }

  Status Close() {
    // Release the output on every exit, including flush and close failures.
    auto writer = std::move(writer_);
    if (writer == nullptr) {
      return {};
    }
    ICEBERG_RETURN_UNEXPECTED(writer->Close());
    ICEBERG_ASSIGN_OR_RAISE(auto manifest, writer->ToManifestFile());
    owner_->new_manifests_.push_back(std::move(manifest));
    return {};
  }

 private:
  RewriteManifests* owner_;
  std::shared_ptr<PartitionSpec> spec_;
  std::unique_ptr<ManifestWriter> writer_;
};

Result<std::shared_ptr<RewriteManifests>> RewriteManifests::Make(
    std::string table_name, std::shared_ptr<TransactionContext> ctx) {
  ICEBERG_PRECHECK(!table_name.empty(), "Table name cannot be empty");
  ICEBERG_PRECHECK(ctx != nullptr, "Cannot create RewriteManifests without a context");
  return std::shared_ptr<RewriteManifests>(
      new RewriteManifests(std::move(table_name), std::move(ctx)));
}

RewriteManifests::RewriteManifests(std::string /*table_name*/,
                                   std::shared_ptr<TransactionContext> ctx)
    : SnapshotUpdate(std::move(ctx)) {}

RewriteManifests::~RewriteManifests() = default;

size_t RewriteManifests::WriterKeyHash::operator()(const WriterKey& key) const {
  size_t seed = std::hash<std::string>{}(key.cluster_key);
  seed ^= std::hash<int32_t>{}(key.partition_spec_id) + 0x9e3779b9 + (seed << 6) +
          (seed >> 2);
  return seed;
}

RewriteManifests& RewriteManifests::ClusterBy(ClusterByFunc func) {
  ICEBERG_BUILDER_CHECK(static_cast<bool>(func), "Cluster function cannot be null");
  cluster_by_func_ = std::move(func);
  rewrite_dirty_ = true;
  return *this;
}

RewriteManifests& RewriteManifests::RewriteIf(RewritePredicate predicate) {
  ICEBERG_BUILDER_CHECK(static_cast<bool>(predicate), "Rewrite predicate cannot be null");
  predicate_ = std::move(predicate);
  rewrite_dirty_ = true;
  return *this;
}

RewriteManifests& RewriteManifests::DeleteManifest(const ManifestFile& manifest) {
  deleted_manifests_.insert(manifest);
  return *this;
}

RewriteManifests& RewriteManifests::AddManifest(const ManifestFile& manifest) {
  // The added manifest cannot contain new or deleted files.
  ICEBERG_BUILDER_CHECK(!manifest.has_added_files(),
                        "Cannot add manifest with added files");
  ICEBERG_BUILDER_CHECK(!manifest.has_deleted_files(),
                        "Cannot add manifest with deleted files");
  ICEBERG_BUILDER_CHECK(!manifest.added_snapshot_id.has_value() ||
                            manifest.added_snapshot_id == kInvalidSnapshotId,
                        "Snapshot id must be assigned during commit");
  ICEBERG_BUILDER_CHECK(manifest.sequence_number == kInvalidSequenceNumber,
                        "Sequence number must be assigned during commit");

  if (can_inherit_snapshot_id() && !manifest.added_snapshot_id.has_value()) {
    added_manifests_.push_back(manifest);
  } else {
    // Retain the input so replay can rebuild the copy.
    added_manifests_to_copy_.push_back(manifest);
  }
  return *this;
}

std::string RewriteManifests::operation() { return DataOperation::kReplace; }

Status RewriteManifests::PrepareForReplay() {
  // Rebuild against the latest manifest list on every replay.
  return CleanUncommitted({});
}

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

  if (base().format_version > 1) {
    for (auto it = added_manifests_.begin(); it != added_manifests_.end();) {
      ICEBERG_ASSIGN_OR_RAISE(auto format_version,
                              ReadManifestFormatVersion(*it, ctx_->table->io()));
      if (format_version == 1) {
        // Reusing a v1 manifest with a new positive commit sequence would leave
        // its EXISTING entries without sequence numbers. Copy it on this apply,
        // including when the table was upgraded before transaction replay.
        added_manifests_to_copy_.push_back(*it);
        it = added_manifests_.erase(it);
      } else {
        ++it;
      }
    }
  }
  for (size_t i = rewritten_added_manifests_.size(); i < added_manifests_to_copy_.size();
       ++i) {
    ICEBERG_ASSIGN_OR_RAISE(auto copied_manifest,
                            CopyManifest(added_manifests_to_copy_[i]));
    rewritten_added_manifests_.push_back(std::move(copied_manifest));
  }

  if (RequiresRewrite(current_manifest_set)) {
    ICEBERG_RETURN_UNEXPECTED(PerformRewrite(current_manifests));
  } else {
    KeepActiveManifests(current_manifests);
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

  return manifests;
}

std::unordered_map<std::string, std::string> RewriteManifests::Summary() {
  summary_.Clear();
  summary_.Set(SnapshotSummaryFields::kManifestsCreated,
               std::to_string(new_manifests_.size() + added_manifests_.size() +
                              rewritten_added_manifests_.size()));
  summary_.Set(SnapshotSummaryFields::kManifestsKept,
               std::to_string(kept_manifests_.size()));
  summary_.Set(SnapshotSummaryFields::kManifestsReplaced,
               std::to_string(rewritten_manifests_.size() + deleted_manifests_.size()));
  summary_.Set(SnapshotSummaryFields::kEntriesProcessed, std::to_string(entry_count_));
  // do not include partition summaries because data did not change
  summary_.SetPartitionSummaryLimit(0);
  return summary_.Build();
}

Status RewriteManifests::CleanUncommitted(
    const std::unordered_set<std::string>& /*committed*/) {
  Reset();
  rewritten_added_manifests_.clear();
  return {};
}

bool RewriteManifests::RequiresRewrite(
    const std::unordered_set<ManifestFile>& current_manifests) const {
  if (!cluster_by_func_) {
    // manifests are deleted and added directly so don't perform a rewrite
    return false;
  }
  if (rewrite_dirty_ || rewritten_manifests_.empty()) {
    // Configuration changes or an empty cache require a full rewrite.
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

bool RewriteManifests::ContainsDeletes(const ManifestFile& manifest) const {
  return manifest.content == ManifestContent::kDeletes;
}

void RewriteManifests::KeepActiveManifests(
    std::span<const ManifestFile> current_manifests) {
  // keep any existing manifests as-is that were not processed
  kept_manifests_ = current_manifests | std::views::filter([this](const auto& manifest) {
                      return !rewritten_manifests_.contains(manifest) &&
                             !deleted_manifests_.contains(manifest);
                    }) |
                    std::ranges::to<std::vector>();
}

Status RewriteManifests::ValidateDeletedManifests(
    const std::unordered_set<ManifestFile>& current_manifests,
    int64_t current_snapshot_id) const {
  // Directly deleted manifests must still be present in the current snapshot.
  for (const auto& manifest : deleted_manifests_) {
    auto current = current_manifests.find(manifest);
    if (current == current_manifests.end()) {
      return ValidationFailed(
          "Deleted manifest {} could not be found in the latest snapshot {}",
          manifest.manifest_path, current_snapshot_id);
    }
    // A prior update may reuse this path with new inherited values on replay.
    // The caller's replacement still contains the original values.
    if (manifest.sequence_number != current->sequence_number ||
        manifest.min_sequence_number != current->min_sequence_number ||
        manifest.first_row_id != current->first_row_id) {
      return ValidationFailed("Deleted manifest metadata changed for {} in snapshot {}",
                              manifest.manifest_path, current_snapshot_id);
    }
  }
  return {};
}

Status RewriteManifests::ValidateFilesCounts() const {
  ICEBERG_ASSIGN_OR_RAISE(auto created_new, CountActiveFiles(new_manifests_));
  ICEBERG_ASSIGN_OR_RAISE(auto created_added, CountActiveFiles(added_manifests_));
  ICEBERG_ASSIGN_OR_RAISE(auto created_rewritten,
                          CountActiveFiles(rewritten_added_manifests_));
  const int64_t created_active_files = created_new + created_added + created_rewritten;

  ICEBERG_ASSIGN_OR_RAISE(auto replaced_rewritten,
                          CountActiveFiles(rewritten_manifests_));
  ICEBERG_ASSIGN_OR_RAISE(auto replaced_deleted, CountActiveFiles(deleted_manifests_));
  const int64_t replaced_active_files = replaced_rewritten + replaced_deleted;

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

Status RewriteManifests::AppendEntry(const ManifestEntry& entry,
                                     const ManifestFile& manifest,
                                     const std::shared_ptr<PartitionSpec>& spec) {
  ICEBERG_PRECHECK(entry.data_file != nullptr,
                   "Manifest entry in {} is missing data_file", manifest.manifest_path);
  auto key = WriterKey{.cluster_key = cluster_by_func_(*entry.data_file),
                       .partition_spec_id = manifest.partition_spec_id};
  ICEBERG_ASSIGN_OR_RAISE(auto writer, GetWriter(key, spec));
  ICEBERG_RETURN_UNEXPECTED(writer->AddEntry(entry));
  ++entry_count_;
  return {};
}

Result<RewriteManifests::WriterWrapper*> RewriteManifests::GetWriter(
    const WriterKey& key, const std::shared_ptr<PartitionSpec>& spec) {
  auto it = writers_.find(key);
  if (it == writers_.end()) {
    auto writer = std::make_unique<WriterWrapper>(this, spec);
    auto [inserted_it, _] = writers_.emplace(key, std::move(writer));
    it = inserted_it;
  }
  return it->second.get();
}

Result<std::unique_ptr<ManifestWriter>> RewriteManifests::NewManifestWriter(
    const std::shared_ptr<PartitionSpec>& spec) {
  ICEBERG_ASSIGN_OR_RAISE(auto schema, base().Schema());
  return ManifestWriter::MakeWriter(base().format_version, SnapshotId(), ManifestPath(),
                                    ctx_->table->io(), spec, schema,
                                    ManifestContent::kData);
}

Status RewriteManifests::CloseWriters() {
  std::optional<Error> first_close_error;
  for (auto& [_, writer] : writers_) {
    auto status = CatchStatus("Manifest writer close", [&] { return writer->Close(); });
    if (!status.has_value() && !first_close_error) {
      first_close_error = status.error();
    }
  }
  writers_.clear();
  if (first_close_error.has_value()) {
    return ::iceberg::unexpected<Error>(std::move(first_close_error).value());
  }
  return {};
}

Status RewriteManifests::PerformRewrite(std::span<const ManifestFile> current_manifests) {
  Reset();

  ICEBERG_ASSIGN_OR_RAISE(auto schema, base().Schema());
  auto file_io = ctx_->table->io();

  // Open a bounded batch of streams concurrently, then consume entries incrementally.
  // Without an executor, keep only one reader open at a time.
  constexpr size_t kManifestReadBatchSize = 32;
  const size_t batch_size = plan_executor().has_value() ? kManifestReadBatchSize : 1;
  std::vector<RewriteCandidate> candidates;
  candidates.reserve(batch_size);

  auto process_batch = [&]() -> Status {
    if (candidates.empty()) {
      return {};
    }

    ICEBERG_ASSIGN_OR_RAISE(
        auto streams,
        ParallelCollect(
            plan_executor(), candidates,
            [&](const RewriteCandidate& candidate)
                -> Result<std::vector<ManifestStream>> {
              ICEBERG_ASSIGN_OR_RAISE(
                  auto reader, ManifestReader::Make(*candidate.manifest, file_io, schema,
                                                    candidate.spec));
              ICEBERG_ASSIGN_OR_RAISE(auto entries, reader->LiveEntriesStream());
              std::vector<ManifestStream> result;
              result.emplace_back(&candidate, std::move(entries));
              return result;
            }));

    for (auto& stream : streams) {
      while (true) {
        ICEBERG_ASSIGN_OR_RAISE(auto entry, stream.entries->Next());
        if (!entry.has_value()) {
          break;
        }
        ICEBERG_RETURN_UNEXPECTED(
            AppendEntry(*entry, *stream.candidate->manifest, stream.candidate->spec));
      }
      stream.entries.reset();
    }

    candidates.clear();
    return {};
  };

  Status write_status = CatchStatus("Manifest rewrite", [&]() -> Status {
    for (const auto& manifest : current_manifests) {
      if (deleted_manifests_.contains(manifest)) {
        continue;
      }
      if (ContainsDeletes(manifest) || !MatchesPredicate(manifest)) {
        kept_manifests_.push_back(manifest);
        continue;
      }

      rewritten_manifests_.insert(manifest);
      ICEBERG_ASSIGN_OR_RAISE(auto spec,
                              base().PartitionSpecById(manifest.partition_spec_id));
      candidates.push_back(
          RewriteCandidate{.manifest = &manifest, .spec = std::move(spec)});
      if (candidates.size() == batch_size) {
        ICEBERG_RETURN_UNEXPECTED(process_batch());
      }
    }
    return process_batch();
  });

  // Close every writer before returning the first failure.
  auto status = FinishWithCloseStatus(std::move(write_status), CloseWriters());
  if (!status.has_value()) {
    return status;
  }
  rewrite_dirty_ = false;
  return {};
}

void RewriteManifests::Reset() {
  // Replay keeps staged outputs alive until the transaction reaches a terminal
  // state. Clear only the in-memory generation.
  new_manifests_.clear();
  entry_count_ = 0;
  kept_manifests_.clear();
  rewritten_manifests_.clear();
  writers_.clear();
  rewrite_dirty_ = true;
}

}  // namespace iceberg
