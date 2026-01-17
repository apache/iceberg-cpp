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

#include "iceberg/snapshot.h"

#include <memory>
#include <sstream>
#include <utility>

#include "iceberg/file_io.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/string_util.h"

namespace iceberg {

namespace {

/// \brief Helper function to conditionally add a property to the summary
template <typename T>
void SetIf(bool condition, std::unordered_map<std::string, std::string>& builder,
           const std::string& property, T value) {
  if (condition) {
    if constexpr (std::is_same_v<T, const char*> || std::is_same_v<T, std::string> ||
                  std::is_convertible_v<T, std::string_view>) {
      builder[property] = value;
    } else {
      builder[property] = std::to_string(value);
    }
  }
}

}  // namespace

bool SnapshotRef::Branch::Equals(const SnapshotRef::Branch& other) const {
  return min_snapshots_to_keep == other.min_snapshots_to_keep &&
         max_snapshot_age_ms == other.max_snapshot_age_ms &&
         max_ref_age_ms == other.max_ref_age_ms;
}

bool SnapshotRef::Tag::Equals(const SnapshotRef::Tag& other) const {
  return max_ref_age_ms == other.max_ref_age_ms;
}

SnapshotRefType SnapshotRef::type() const noexcept {
  return std::visit(
      [&](const auto& retention) -> SnapshotRefType {
        using T = std::remove_cvref_t<decltype(retention)>;
        if constexpr (std::is_same_v<T, Branch>) {
          return SnapshotRefType::kBranch;
        } else {
          return SnapshotRefType::kTag;
        }
      },
      retention);
}

std::optional<int64_t> SnapshotRef::max_ref_age_ms() const noexcept {
  return std::visit(
      [&](const auto& retention) -> std::optional<int64_t> {
        using T = std::remove_cvref_t<decltype(retention)>;
        if constexpr (std::is_same_v<T, Branch>) {
          return retention.max_ref_age_ms;
        } else {
          return retention.max_ref_age_ms;
        }
      },
      retention);
}

Status SnapshotRef::Validate() const {
  if (type() == SnapshotRefType::kBranch) {
    const auto& branch = std::get<Branch>(this->retention);
    ICEBERG_CHECK(!branch.min_snapshots_to_keep.has_value() ||
                      branch.min_snapshots_to_keep.value() > 0,
                  "Min snapshots to keep must be greater than 0");
    ICEBERG_CHECK(
        !branch.max_snapshot_age_ms.has_value() || branch.max_snapshot_age_ms.value() > 0,
        "Max snapshot age must be greater than 0 ms");
    ICEBERG_CHECK(!branch.max_ref_age_ms.has_value() || branch.max_ref_age_ms.value() > 0,
                  "Max reference age must be greater than 0");
  } else {
    const auto& tag = std::get<Tag>(this->retention);
    ICEBERG_CHECK(!tag.max_ref_age_ms.has_value() || tag.max_ref_age_ms.value() > 0,
                  "Max reference age must be greater than 0");
  }
  return {};
}

Result<std::unique_ptr<SnapshotRef>> SnapshotRef::MakeBranch(
    int64_t snapshot_id, std::optional<int32_t> min_snapshots_to_keep,
    std::optional<int64_t> max_snapshot_age_ms, std::optional<int64_t> max_ref_age_ms) {
  auto ref = std::make_unique<SnapshotRef>(
      SnapshotRef{.snapshot_id = snapshot_id,
                  .retention = Branch{
                      .min_snapshots_to_keep = min_snapshots_to_keep,
                      .max_snapshot_age_ms = max_snapshot_age_ms,
                      .max_ref_age_ms = max_ref_age_ms,
                  }});
  ICEBERG_RETURN_UNEXPECTED(ref->Validate());
  return ref;
}

Result<std::unique_ptr<SnapshotRef>> SnapshotRef::MakeTag(
    int64_t snapshot_id, std::optional<int64_t> max_ref_age_ms) {
  auto ref = std::make_unique<SnapshotRef>(SnapshotRef{
      .snapshot_id = snapshot_id, .retention = Tag{.max_ref_age_ms = max_ref_age_ms}});
  ICEBERG_RETURN_UNEXPECTED(ref->Validate());
  return ref;
}

std::unique_ptr<SnapshotRef> SnapshotRef::Clone(
    std::optional<int64_t> new_snapshot_id) const {
  auto ref = std::make_unique<SnapshotRef>();
  ref->snapshot_id = new_snapshot_id.value_or(snapshot_id);
  ref->retention = retention;
  return ref;
}

bool SnapshotRef::Equals(const SnapshotRef& other) const {
  if (this == &other) {
    return true;
  }
  if (type() != other.type()) {
    return false;
  }

  if (type() == SnapshotRefType::kBranch) {
    return snapshot_id == other.snapshot_id &&
           std::get<Branch>(retention) == std::get<Branch>(other.retention);

  } else {
    return snapshot_id == other.snapshot_id &&
           std::get<Tag>(retention) == std::get<Tag>(other.retention);
  }
}

std::optional<std::string_view> Snapshot::Operation() const {
  auto it = summary.find(SnapshotSummaryFields::kOperation);
  if (it != summary.end()) {
    return it->second;
  }
  return std::nullopt;
}

Result<std::optional<int64_t>> Snapshot::FirstRowId() const {
  auto it = summary.find(SnapshotSummaryFields::kFirstRowId);
  if (it == summary.end()) {
    return std::nullopt;
  }

  return StringUtils::ParseInt<int64_t>(it->second);
}

Result<std::optional<int64_t>> Snapshot::AddedRows() const {
  auto it = summary.find(SnapshotSummaryFields::kAddedRows);
  if (it == summary.end()) {
    return std::nullopt;
  }

  return StringUtils::ParseInt<int64_t>(it->second);
}

bool Snapshot::Equals(const Snapshot& other) const {
  if (this == &other) {
    return true;
  }
  return snapshot_id == other.snapshot_id &&
         parent_snapshot_id == other.parent_snapshot_id &&
         sequence_number == other.sequence_number && timestamp_ms == other.timestamp_ms &&
         schema_id == other.schema_id;
}

Result<std::unique_ptr<Snapshot>> Snapshot::Make(
    int64_t sequence_number, int64_t snapshot_id,
    std::optional<int64_t> parent_snapshot_id, TimePointMs timestamp_ms,
    std::string operation, std::unordered_map<std::string, std::string> summary,
    std::optional<int32_t> schema_id, std::string manifest_list,
    std::optional<int64_t> first_row_id, std::optional<int64_t> added_rows) {
  ICEBERG_PRECHECK(!operation.empty(), "Operation cannot be empty");
  ICEBERG_PRECHECK(!first_row_id.has_value() || first_row_id.value() >= 0,
                   "Invalid first-row-id (cannot be negative): {}", first_row_id.value());
  ICEBERG_PRECHECK(!added_rows.has_value() || added_rows.value() >= 0,
                   "Invalid added-rows (cannot be negative): {}", added_rows.value());
  ICEBERG_PRECHECK(!first_row_id.has_value() || added_rows.has_value(),
                   "Missing added-rows when first-row-id is set");
  summary[SnapshotSummaryFields::kOperation] = operation;
  if (first_row_id.has_value()) {
    summary[SnapshotSummaryFields::kFirstRowId] = std::to_string(first_row_id.value());
  }
  if (added_rows.has_value()) {
    summary[SnapshotSummaryFields::kAddedRows] = std::to_string(added_rows.value());
  }
  return std::make_unique<Snapshot>(Snapshot{
      .snapshot_id = snapshot_id,
      .parent_snapshot_id = parent_snapshot_id,
      .sequence_number = sequence_number,
      .timestamp_ms = timestamp_ms,
      .manifest_list = std::move(manifest_list),
      .summary = std::move(summary),
      .schema_id = schema_id,
  });
}

Result<SnapshotCache::ManifestsCache> SnapshotCache::InitManifestsCache(
    const Snapshot* snapshot, std::shared_ptr<FileIO> file_io) {
  if (file_io == nullptr) {
    return InvalidArgument("Cannot cache manifests: FileIO is null");
  }

  // Read manifest list
  ICEBERG_ASSIGN_OR_RAISE(auto reader,
                          ManifestListReader::Make(snapshot->manifest_list, file_io));
  ICEBERG_ASSIGN_OR_RAISE(auto manifest_files, reader->Files());

  std::vector<ManifestFile> manifests;
  manifests.reserve(manifest_files.size());

  // Partition manifests: data manifests first, then delete manifests
  // First pass: collect data manifests
  for (const auto& manifest_file : manifest_files) {
    if (manifest_file.content == ManifestContent::kData) {
      manifests.push_back(manifest_file);
    }
  }
  size_t data_manifests_count = manifests.size();

  // Second pass: append delete manifests
  for (const auto& manifest_file : manifest_files) {
    if (manifest_file.content == ManifestContent::kDeletes) {
      manifests.push_back(manifest_file);
    }
  }

  return std::make_pair(std::move(manifests), data_manifests_count);
}

Result<std::span<ManifestFile>> SnapshotCache::Manifests(
    std::shared_ptr<FileIO> file_io) const {
  ICEBERG_ASSIGN_OR_RAISE(auto cache_ref, manifests_cache_.Get(snapshot_, file_io));
  auto& cache = cache_ref.get();
  return std::span<ManifestFile>(cache.first.data(), cache.first.size());
}

Result<std::span<ManifestFile>> SnapshotCache::DataManifests(
    std::shared_ptr<FileIO> file_io) const {
  ICEBERG_ASSIGN_OR_RAISE(auto cache_ref, manifests_cache_.Get(snapshot_, file_io));
  auto& cache = cache_ref.get();
  return std::span<ManifestFile>(cache.first.data(), cache.second);
}

Result<std::span<ManifestFile>> SnapshotCache::DeleteManifests(
    std::shared_ptr<FileIO> file_io) const {
  ICEBERG_ASSIGN_OR_RAISE(auto cache_ref, manifests_cache_.Get(snapshot_, file_io));
  auto& cache = cache_ref.get();
  const size_t delete_start = cache.second;
  const size_t delete_count = cache.first.size() - delete_start;
  return std::span<ManifestFile>(cache.first.data() + delete_start, delete_count);
}

// SnapshotSummaryBuilder::UpdateMetrics implementation

void SnapshotSummaryBuilder::UpdateMetrics::Clear() {
  added_size_ = 0;
  removed_size_ = 0;
  added_files_ = 0;
  removed_files_ = 0;
  added_eq_delete_files_ = 0;
  removed_eq_delete_files_ = 0;
  added_pos_delete_files_ = 0;
  removed_pos_delete_files_ = 0;
  added_delete_files_ = 0;
  removed_delete_files_ = 0;
  added_dvs_ = 0;
  removed_dvs_ = 0;
  added_records_ = 0;
  deleted_records_ = 0;
  added_pos_deletes_ = 0;
  removed_pos_deletes_ = 0;
  added_eq_deletes_ = 0;
  removed_eq_deletes_ = 0;
  trust_size_and_delete_counts_ = true;
}

void SnapshotSummaryBuilder::UpdateMetrics::AddTo(
    std::unordered_map<std::string, std::string>& builder) const {
  SetIf(added_files_ > 0, builder, SnapshotSummaryFields::kAddedDataFiles, added_files_);
  SetIf(removed_files_ > 0, builder, SnapshotSummaryFields::kDeletedDataFiles,
        removed_files_);
  SetIf(added_eq_delete_files_ > 0, builder, SnapshotSummaryFields::kAddedEqDeleteFiles,
        added_eq_delete_files_);
  SetIf(removed_eq_delete_files_ > 0, builder,
        SnapshotSummaryFields::kRemovedEqDeleteFiles, removed_eq_delete_files_);
  SetIf(added_pos_delete_files_ > 0, builder, SnapshotSummaryFields::kAddedPosDeleteFiles,
        added_pos_delete_files_);
  SetIf(removed_pos_delete_files_ > 0, builder,
        SnapshotSummaryFields::kRemovedPosDeleteFiles, removed_pos_delete_files_);
  SetIf(added_delete_files_ > 0, builder, SnapshotSummaryFields::kAddedDeleteFiles,
        added_delete_files_);
  SetIf(removed_delete_files_ > 0, builder, SnapshotSummaryFields::kRemovedDeleteFiles,
        removed_delete_files_);
  SetIf(added_dvs_ > 0, builder, SnapshotSummaryFields::kAddedDVs, added_dvs_);
  SetIf(removed_dvs_ > 0, builder, SnapshotSummaryFields::kRemovedDVs, removed_dvs_);
  SetIf(added_records_ > 0, builder, SnapshotSummaryFields::kAddedRecords,
        added_records_);
  SetIf(deleted_records_ > 0, builder, SnapshotSummaryFields::kDeletedRecords,
        deleted_records_);

  if (trust_size_and_delete_counts_) {
    SetIf(added_size_ > 0, builder, SnapshotSummaryFields::kAddedFileSize, added_size_);
    SetIf(removed_size_ > 0, builder, SnapshotSummaryFields::kRemovedFileSize,
          removed_size_);
    SetIf(added_pos_deletes_ > 0, builder, SnapshotSummaryFields::kAddedPosDeletes,
          added_pos_deletes_);
    SetIf(removed_pos_deletes_ > 0, builder, SnapshotSummaryFields::kRemovedPosDeletes,
          removed_pos_deletes_);
    SetIf(added_eq_deletes_ > 0, builder, SnapshotSummaryFields::kAddedEqDeletes,
          added_eq_deletes_);
    SetIf(removed_eq_deletes_ > 0, builder, SnapshotSummaryFields::kRemovedEqDeletes,
          removed_eq_deletes_);
  }
}

void SnapshotSummaryBuilder::UpdateMetrics::AddedFile(const DataFile& file) {
  added_size_ += file.file_size_in_bytes;

  switch (file.content) {
    case DataFile::Content::kData:
      added_files_ += 1;
      added_records_ += file.record_count;
      break;
    case DataFile::Content::kPositionDeletes:
      if (file.IsDeletionVector()) {
        added_dvs_ += 1;
      } else {
        added_pos_delete_files_ += 1;
      }
      added_delete_files_ += 1;
      added_pos_deletes_ += file.record_count;
      break;
    case DataFile::Content::kEqualityDeletes:
      added_delete_files_ += 1;
      added_eq_delete_files_ += 1;
      added_eq_deletes_ += file.record_count;
      break;
    default:
      std::unreachable();
  }
}

void SnapshotSummaryBuilder::UpdateMetrics::RemovedFile(const DataFile& file) {
  removed_size_ += file.file_size_in_bytes;

  switch (file.content) {
    case DataFile::Content::kData:
      removed_files_ += 1;
      deleted_records_ += file.record_count;
      break;
    case DataFile::Content::kPositionDeletes:
      if (file.IsDeletionVector()) {
        removed_dvs_ += 1;
      } else {
        removed_pos_delete_files_ += 1;
      }
      removed_delete_files_ += 1;
      removed_pos_deletes_ += file.record_count;
      break;
    case DataFile::Content::kEqualityDeletes:
      removed_delete_files_ += 1;
      removed_eq_delete_files_ += 1;
      removed_eq_deletes_ += file.record_count;
      break;
    default:
      std::unreachable();
  }
}

void SnapshotSummaryBuilder::UpdateMetrics::AddedManifest(const ManifestFile& manifest) {
  switch (manifest.content) {
    case ManifestContent::kData:
      added_files_ += manifest.added_files_count.value_or(0);
      added_records_ += manifest.added_rows_count.value_or(0);
      removed_files_ += manifest.deleted_files_count.value_or(0);
      deleted_records_ += manifest.deleted_rows_count.value_or(0);
      break;
    case ManifestContent::kDeletes:
      added_delete_files_ += manifest.added_files_count.value_or(0);
      removed_delete_files_ += manifest.deleted_files_count.value_or(0);
      trust_size_and_delete_counts_ = false;
      break;
    default:
      std::unreachable();
  }
}

void SnapshotSummaryBuilder::UpdateMetrics::Merge(const UpdateMetrics& other) {
  added_files_ += other.added_files_;
  removed_files_ += other.removed_files_;
  added_eq_delete_files_ += other.added_eq_delete_files_;
  removed_eq_delete_files_ += other.removed_eq_delete_files_;
  added_pos_delete_files_ += other.added_pos_delete_files_;
  removed_pos_delete_files_ += other.removed_pos_delete_files_;
  added_dvs_ += other.added_dvs_;
  removed_dvs_ += other.removed_dvs_;
  added_delete_files_ += other.added_delete_files_;
  removed_delete_files_ += other.removed_delete_files_;
  added_size_ += other.added_size_;
  removed_size_ += other.removed_size_;
  added_records_ += other.added_records_;
  deleted_records_ += other.deleted_records_;
  added_pos_deletes_ += other.added_pos_deletes_;
  removed_pos_deletes_ += other.removed_pos_deletes_;
  added_eq_deletes_ += other.added_eq_deletes_;
  removed_eq_deletes_ += other.removed_eq_deletes_;
  trust_size_and_delete_counts_ =
      trust_size_and_delete_counts_ && other.trust_size_and_delete_counts_;
}

// SnapshotSummaryBuilder implementation

void SnapshotSummaryBuilder::Clear() {
  partition_metrics_.clear();
  metrics_.Clear();
  deleted_duplicate_files_ = 0;
  trust_partition_metrics_ = true;
}

void SnapshotSummaryBuilder::SetPartitionSummaryLimit(int32_t max) {
  max_changed_partitions_for_summaries_ = max;
}

void SnapshotSummaryBuilder::IncrementDuplicateDeletes(int32_t increment) {
  deleted_duplicate_files_ += increment;
}

Status SnapshotSummaryBuilder::AddedFile(const PartitionSpec& spec,
                                         const DataFile& file) {
  metrics_.AddedFile(file);
  ICEBERG_RETURN_UNEXPECTED(UpdatePartitions(spec, file, true));
  return {};
}

Status SnapshotSummaryBuilder::DeletedFile(const PartitionSpec& spec,
                                           const DataFile& file) {
  metrics_.RemovedFile(file);
  ICEBERG_RETURN_UNEXPECTED(UpdatePartitions(spec, file, false));
  return {};
}

void SnapshotSummaryBuilder::AddedManifest(const ManifestFile& manifest) {
  trust_partition_metrics_ = false;
  partition_metrics_.clear();
  metrics_.AddedManifest(manifest);
}

void SnapshotSummaryBuilder::Set(const std::string& property, const std::string& value) {
  properties_[property] = value;
}

void SnapshotSummaryBuilder::Merge(const SnapshotSummaryBuilder& other) {
  for (const auto& [key, value] : other.properties_) {
    properties_[key] = value;
  }
  metrics_.Merge(other.metrics_);

  trust_partition_metrics_ = trust_partition_metrics_ && other.trust_partition_metrics_;
  if (trust_partition_metrics_) {
    for (const auto& [key, value] : other.partition_metrics_) {
      partition_metrics_[key].Merge(value);
    }
  } else {
    partition_metrics_.clear();
  }

  deleted_duplicate_files_ += other.deleted_duplicate_files_;
}

std::unordered_map<std::string, std::string> SnapshotSummaryBuilder::Build() const {
  std::unordered_map<std::string, std::string> builder;

  // Copy custom summary properties
  builder.insert(properties_.begin(), properties_.end());

  metrics_.AddTo(builder);

  SetIf(deleted_duplicate_files_ > 0, builder,
        SnapshotSummaryFields::kDeletedDuplicatedFiles, deleted_duplicate_files_);

  SetIf(trust_partition_metrics_, builder,
        SnapshotSummaryFields::kChangedPartitionCountProp, partition_metrics_.size());

  // Add partition summaries if enabled
  if (trust_partition_metrics_ && max_changed_partitions_for_summaries_ >= 0 &&
      partition_metrics_.size() <=
          static_cast<size_t>(max_changed_partitions_for_summaries_)) {
    SetIf(!partition_metrics_.empty(), builder,
          SnapshotSummaryFields::kPartitionSummaryProp, "true");
    for (const auto& [key, metrics] : partition_metrics_) {
      if (!key.empty()) {
        builder[SnapshotSummaryFields::kChangedPartitionPrefix + key] =
            PartitionSummary(metrics);
      }
    }
  }

  return builder;
}

Status SnapshotSummaryBuilder::UpdatePartitions(const PartitionSpec& spec,
                                                const DataFile& file, bool is_addition) {
  if (trust_partition_metrics_) {
    ICEBERG_ASSIGN_OR_RAISE(std::string partition_path,
                            spec.PartitionPath(file.partition));
    auto& part_metrics = partition_metrics_[partition_path];
    if (is_addition) {
      part_metrics.AddedFile(file);
    } else {
      part_metrics.RemovedFile(file);
    }
  }
  return {};
}

std::string SnapshotSummaryBuilder::PartitionSummary(const UpdateMetrics& metrics) const {
  std::unordered_map<std::string, std::string> part_builder;
  metrics.AddTo(part_builder);

  // Format as comma-separated key=value pairs
  std::ostringstream oss;
  bool first = true;
  for (const auto& [key, value] : part_builder) {
    if (!first) {
      oss << ",";
    }
    oss << key << "=" << value;
    first = false;
  }
  return oss.str();
}

}  // namespace iceberg
