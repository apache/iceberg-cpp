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

#include "iceberg/delete_file_index.h"

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <ranges>
#include <vector>

#include "iceberg/expression/expression.h"
#include "iceberg/expression/manifest_evaluator.h"
#include "iceberg/expression/projections.h"
#include "iceberg/file_io.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/content_file_util.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace internal {

Status EqualityDeleteFile::ConvertBoundsIfNeeded() const {
  if (bounds_converted) {
    return {};
  }

  // Convert bounds for equality field IDs only
  for (int32_t field_id : wrapped.data_file->equality_ids) {
    ICEBERG_ASSIGN_OR_RAISE(auto field, schema->FindFieldById(field_id));
    if (!field.has_value()) {
      continue;
    }

    const auto& schema_field = field.value().get();
    if (schema_field.type()->is_nested()) {
      continue;
    }

    const auto primitive_type = checked_pointer_cast<PrimitiveType>(schema_field.type());

    // Convert lower bound
    if (auto it = wrapped.data_file->lower_bounds.find(field_id);
        it != wrapped.data_file->lower_bounds.cend() && !it->second.empty()) {
      ICEBERG_ASSIGN_OR_RAISE(auto lower,
                              Literal::Deserialize(it->second, primitive_type));
      lower_bounds.emplace(field_id, std::move(lower));
    }

    // Convert upper bound
    if (auto it = wrapped.data_file->upper_bounds.find(field_id);
        it != wrapped.data_file->upper_bounds.cend() && !it->second.empty()) {
      ICEBERG_ASSIGN_OR_RAISE(auto upper,
                              Literal::Deserialize(it->second, primitive_type));
      upper_bounds.emplace(field_id, std::move(upper));
    }
  }

  bounds_converted = true;
  return {};
}

// Check if an equality delete file can contain deletes for a data file.
Result<bool> CanContainEqDeletesForFile(const DataFile& data_file,
                                        const EqualityDeleteFile& delete_file) {
  // Whether to check data ranges or to assume that the ranges match.  If upper/lower
  // bounds are missing, null counts may still be used to determine delete files can be
  // skipped.
  bool check_ranges = !data_file.lower_bounds.empty() &&
                      !data_file.upper_bounds.empty() &&
                      delete_file.HasLowerAndUpperBounds();

  const auto* wrapped_delete_file = delete_file.wrapped.data_file.get();

  for (int32_t field_id : wrapped_delete_file->equality_ids) {
    ICEBERG_ASSIGN_OR_RAISE(auto found_field,
                            delete_file.schema->FindFieldById(field_id));
    if (!found_field.has_value()) {
      continue;
    }

    const auto& field = found_field.value().get();
    if (field.type()->is_nested()) {
      continue;
    }

    bool is_required = !field.optional();
    bool data_contains_null =
        ContainsNull(data_file.null_value_counts, field_id, is_required);
    bool delete_contains_null =
        ContainsNull(wrapped_delete_file->null_value_counts, field_id, is_required);

    if (data_contains_null && delete_contains_null) {
      // Both have nulls - delete may apply
      continue;
    }

    if (AllNull(data_file.null_value_counts, data_file.value_counts, field_id,
                is_required) &&
        AllNonNull(wrapped_delete_file->null_value_counts, field_id, is_required)) {
      return false;  // Data is all null, delete has no nulls - cannot match
    }

    if (AllNull(wrapped_delete_file->null_value_counts, wrapped_delete_file->value_counts,
                field_id, is_required) &&
        AllNonNull(data_file.null_value_counts, field_id, is_required)) {
      return false;  // Delete is all null, data has no nulls - cannot match
    }

    if (!check_ranges) {
      continue;
    }

    // Check range overlap
    auto data_lower_it = data_file.lower_bounds.find(field_id);
    auto data_upper_it = data_file.upper_bounds.find(field_id);
    if (data_lower_it == data_file.lower_bounds.cend() || data_lower_it->second.empty() ||
        data_upper_it == data_file.upper_bounds.cend() || data_upper_it->second.empty()) {
      continue;  // Missing bounds, assume may match
    }

    auto delete_lower = delete_file.LowerBound(field_id);
    auto delete_upper = delete_file.UpperBound(field_id);
    if (!delete_lower.has_value() || !delete_upper.has_value()) {
      continue;  // Missing bounds, assume may match
    }

    // Convert data bounds
    auto primitive_type = checked_pointer_cast<PrimitiveType>(field.type());
    ICEBERG_ASSIGN_OR_RAISE(auto data_lower,
                            Literal::Deserialize(data_lower_it->second, primitive_type));
    ICEBERG_ASSIGN_OR_RAISE(auto data_upper,
                            Literal::Deserialize(data_upper_it->second, primitive_type));

    if (!RangesOverlap(data_lower, data_upper, delete_lower->value().get(),
                       delete_upper->value().get())) {
      return false;  // Ranges don't overlap - cannot match
    }
  }

  return true;
}

// PositionDeletes implementation

Status PositionDeletes::Add(ManifestEntry&& entry) {
  ICEBERG_PRECHECK(entry.sequence_number.has_value(),
                   "Missing sequence number from position delete: {}",
                   entry.data_file->file_path);
  files_.emplace_back(std::move(entry));
  indexed_ = false;
  return {};
}

std::vector<std::shared_ptr<DataFile>> PositionDeletes::Filter(int64_t seq) {
  IndexIfNeeded();

  auto iter = std::ranges::lower_bound(seqs_, seq);
  if (iter == seqs_.end()) {
    return {};
  }
  return files_ | std::views::drop(iter - seqs_.begin()) |
         std::views::transform(&ManifestEntry::data_file) |
         std::ranges::to<std::vector<std::shared_ptr<DataFile>>>();
}

std::vector<std::shared_ptr<DataFile>> PositionDeletes::ReferencedDeleteFiles() {
  IndexIfNeeded();
  return files_ | std::views::transform(&ManifestEntry::data_file) |
         std::ranges::to<std::vector<std::shared_ptr<DataFile>>>();
}

void PositionDeletes::IndexIfNeeded() {
  if (indexed_) {
    return;
  }

  // Sort by data sequence number
  std::ranges::sort(files_, std::ranges::less{}, &ManifestEntry::sequence_number);

  // Build sequence number array for binary search
  seqs_ = files_ |
          std::views::transform([](const auto& e) { return e.sequence_number.value(); }) |
          std::ranges::to<std::vector<int64_t>>();

  indexed_ = true;
}

// EqualityDeletes implementation

Status EqualityDeletes::Add(ManifestEntry&& entry) {
  ICEBERG_PRECHECK(entry.sequence_number.has_value(),
                   "Missing sequence number from equality delete: {}",
                   entry.data_file->file_path);
  files_.emplace_back(&schema_, std::move(entry));
  indexed_ = false;
  return {};
}

Result<std::vector<std::shared_ptr<DataFile>>> EqualityDeletes::Filter(
    int64_t seq, const DataFile& data_file) {
  IndexIfNeeded();

  auto iter = std::ranges::lower_bound(seqs_, seq);
  if (iter == seqs_.end()) {
    return {};
  }
  std::vector<std::shared_ptr<DataFile>> result;
  result.reserve(seqs_.end() - iter);
  for (auto& delete_file : files_ | std::views::drop(iter - seqs_.begin())) {
    ICEBERG_ASSIGN_OR_RAISE(bool may_contain,
                            CanContainEqDeletesForFile(data_file, delete_file));
    if (may_contain) {
      result.push_back(delete_file.wrapped.data_file);
    }
  }

  return result;
}

std::vector<std::shared_ptr<DataFile>> EqualityDeletes::ReferencedDeleteFiles() {
  IndexIfNeeded();
  return files_ |
         std::views::transform([](const auto& f) { return f.wrapped.data_file; }) |
         std::ranges::to<std::vector<std::shared_ptr<DataFile>>>();
}

void EqualityDeletes::IndexIfNeeded() {
  if (indexed_) {
    return;
  }

  // Sort by apply sequence number
  std::ranges::sort(files_, std::ranges::less{},
                    &EqualityDeleteFile::apply_sequence_number);

  // Build sequence number array for binary search
  seqs_ = files_ | std::views::transform(&EqualityDeleteFile::apply_sequence_number) |
          std::ranges::to<std::vector<int64_t>>();

  indexed_ = true;
}

}  // namespace internal

// DeleteFileIndex implementation

DeleteFileIndex::DeleteFileIndex(
    std::unique_ptr<internal::EqualityDeletes> global_deletes,
    std::unique_ptr<PartitionMap<std::unique_ptr<internal::EqualityDeletes>>>
        eq_deletes_by_partition,
    std::unique_ptr<PartitionMap<std::unique_ptr<internal::PositionDeletes>>>
        pos_deletes_by_partition,
    std::unique_ptr<
        std::unordered_map<std::string, std::unique_ptr<internal::PositionDeletes>>>
        pos_deletes_by_path,
    std::unique_ptr<std::unordered_map<std::string, ManifestEntry>> dv_by_path)
    : global_deletes_(std::move(global_deletes)),
      eq_deletes_by_partition_(std::move(eq_deletes_by_partition)),
      pos_deletes_by_partition_(std::move(pos_deletes_by_partition)),
      pos_deletes_by_path_(std::move(pos_deletes_by_path)),
      dv_by_path_(std::move(dv_by_path)) {
  has_eq_deletes_ = (global_deletes_ && !global_deletes_->empty()) ||
                    (eq_deletes_by_partition_ && !eq_deletes_by_partition_->empty());
  has_pos_deletes_ = (pos_deletes_by_partition_ && !pos_deletes_by_partition_->empty()) ||
                     (pos_deletes_by_path_ && !pos_deletes_by_path_->empty()) ||
                     (dv_by_path_ && !dv_by_path_->empty());
  is_empty_ = !has_eq_deletes_ && !has_pos_deletes_;
}

DeleteFileIndex::~DeleteFileIndex() = default;
DeleteFileIndex::DeleteFileIndex(DeleteFileIndex&&) noexcept = default;
DeleteFileIndex& DeleteFileIndex::operator=(DeleteFileIndex&&) noexcept = default;

bool DeleteFileIndex::empty() const { return is_empty_; }

bool DeleteFileIndex::has_equality_deletes() const { return has_eq_deletes_; }

bool DeleteFileIndex::has_position_deletes() const { return has_pos_deletes_; }

std::vector<std::shared_ptr<DataFile>> DeleteFileIndex::ReferencedDeleteFiles() const {
  std::vector<std::shared_ptr<DataFile>> result;

  if (global_deletes_) {
    auto files = global_deletes_->ReferencedDeleteFiles();
    std::ranges::move(files, std::back_inserter(result));
  }

  if (eq_deletes_by_partition_) {
    for (const auto& [_, deletes] : *eq_deletes_by_partition_) {
      auto files = deletes->ReferencedDeleteFiles();
      std::ranges::move(files, std::back_inserter(result));
    }
  }

  if (pos_deletes_by_partition_) {
    for (const auto& [_, deletes] : *pos_deletes_by_partition_) {
      auto files = deletes->ReferencedDeleteFiles();
      std::ranges::move(files, std::back_inserter(result));
    }
  }

  if (pos_deletes_by_path_) {
    for (auto& [_, deletes] : *pos_deletes_by_path_) {
      auto files = deletes->ReferencedDeleteFiles();
      std::ranges::move(files, std::back_inserter(result));
    }
  }

  if (dv_by_path_) {
    for (const auto& [_, dv] : *dv_by_path_) {
      result.push_back(dv.data_file);
    }
  }

  return result;
}

Result<std::vector<std::shared_ptr<DataFile>>> DeleteFileIndex::ForEntry(
    const ManifestEntry& entry) const {
  ICEBERG_PRECHECK(entry.data_file != nullptr, "Manifest entry has null data file");
  ICEBERG_PRECHECK(entry.sequence_number.has_value(),
                   "Missing sequence number from data file: {}",
                   entry.data_file->file_path);
  return ForDataFile(entry.sequence_number.value(), *entry.data_file);
}

Result<std::vector<std::shared_ptr<DataFile>>> DeleteFileIndex::ForDataFile(
    int64_t sequence_number, const DataFile& file) const {
  if (is_empty_) {
    return {};
  }

  ICEBERG_ASSIGN_OR_RAISE(auto global, FindGlobalDeletes(sequence_number, file));
  ICEBERG_ASSIGN_OR_RAISE(auto eq_partition,
                          FindEqPartitionDeletes(sequence_number, file));
  ICEBERG_ASSIGN_OR_RAISE(auto dv, FindDV(sequence_number, file));

  if (dv && global.empty() && eq_partition.empty()) {
    return std::vector<std::shared_ptr<DataFile>>{std::move(dv)};
  }

  std::vector<std::shared_ptr<DataFile>> result;
  result.reserve(global.size() + eq_partition.size() + 1);

  std::ranges::move(global, std::back_inserter(result));
  std::ranges::move(eq_partition, std::back_inserter(result));

  if (dv) {
    result.push_back(dv);
  } else {
    ICEBERG_ASSIGN_OR_RAISE(auto pos_partition,
                            FindPosPartitionDeletes(sequence_number, file));
    ICEBERG_ASSIGN_OR_RAISE(auto pos_path, FindPathDeletes(sequence_number, file));
    std::ranges::move(pos_partition, std::back_inserter(result));
    std::ranges::move(pos_path, std::back_inserter(result));
  }

  return result;
}

Result<std::vector<std::shared_ptr<DataFile>>> DeleteFileIndex::FindGlobalDeletes(
    int64_t seq, const DataFile& data_file) const {
  if (!global_deletes_) {
    return {};
  }
  return global_deletes_->Filter(seq, data_file);
}

Result<std::vector<std::shared_ptr<DataFile>>> DeleteFileIndex::FindEqPartitionDeletes(
    int64_t seq, const DataFile& data_file) const {
  if (!eq_deletes_by_partition_) {
    return {};
  }

  ICEBERG_PRECHECK(data_file.partition_spec_id.has_value(),
                   "Missing partition spec id from data file {}", data_file.file_path);

  auto deletes = eq_deletes_by_partition_->get(data_file.partition_spec_id.value(),
                                               data_file.partition);
  if (!deletes.has_value()) {
    return {};
  }
  return deletes->get()->Filter(seq, data_file);
}

Result<std::vector<std::shared_ptr<DataFile>>> DeleteFileIndex::FindPosPartitionDeletes(
    int64_t seq, const DataFile& data_file) const {
  if (!pos_deletes_by_partition_) {
    return {};
  }

  ICEBERG_PRECHECK(data_file.partition_spec_id.has_value(),
                   "Missing partition spec id from data file {}", data_file.file_path);

  auto deletes = pos_deletes_by_partition_->get(data_file.partition_spec_id.value(),
                                                data_file.partition);
  if (!deletes.has_value()) {
    return {};
  }

  return deletes->get()->Filter(seq);
}

Result<std::vector<std::shared_ptr<DataFile>>> DeleteFileIndex::FindPathDeletes(
    int64_t seq, const DataFile& data_file) const {
  if (!pos_deletes_by_path_) {
    return {};
  }

  auto it = pos_deletes_by_path_->find(data_file.file_path);
  if (it == pos_deletes_by_path_->end()) {
    return {};
  }

  return it->second->Filter(seq);
}

Result<std::shared_ptr<DataFile>> DeleteFileIndex::FindDV(
    int64_t seq, const DataFile& data_file) const {
  if (!dv_by_path_) {
    return nullptr;
  }

  auto it = dv_by_path_->find(data_file.file_path);
  if (it == dv_by_path_->end()) {
    return nullptr;
  }

  ICEBERG_CHECK(it->second.sequence_number.value() >= seq,
                "DV data sequence number {} must be greater than or equal to data file "
                "sequence number {}",
                it->second.sequence_number.value(), seq);

  return it->second.data_file;
}

Result<DeleteFileIndex::Builder> DeleteFileIndex::BuilderFor(
    std::shared_ptr<FileIO> io, std::shared_ptr<Schema> schema,
    std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> specs_by_id,
    std::vector<ManifestFile> delete_manifests) {
  ICEBERG_PRECHECK(io != nullptr, "FileIO cannot be null");
  ICEBERG_PRECHECK(schema != nullptr, "Schema cannot be null");
  ICEBERG_PRECHECK(!specs_by_id.empty(), "Partition specs cannot be empty");
  return Builder(std::move(io), std::move(schema), std::move(specs_by_id),
                 std::move(delete_manifests));
}

// Builder implementation

DeleteFileIndex::Builder::Builder(
    std::shared_ptr<FileIO> io, std::shared_ptr<Schema> schema,
    std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> specs_by_id,
    std::vector<ManifestFile> delete_manifests)
    : io_(std::move(io)),
      schema_(std::move(schema)),
      specs_by_id_(std::move(specs_by_id)),
      delete_manifests_(std::move(delete_manifests)) {}

DeleteFileIndex::Builder::~Builder() = default;
DeleteFileIndex::Builder::Builder(Builder&&) noexcept = default;
DeleteFileIndex::Builder& DeleteFileIndex::Builder::operator=(Builder&&) noexcept =
    default;

DeleteFileIndex::Builder& DeleteFileIndex::Builder::AfterSequenceNumber(int64_t seq) {
  min_sequence_number_ = seq;
  return *this;
}

DeleteFileIndex::Builder& DeleteFileIndex::Builder::DataFilter(
    std::shared_ptr<Expression> filter) {
  if (data_filter_) {
    ICEBERG_BUILDER_ASSIGN_OR_RETURN(data_filter_,
                                     And::Make(data_filter_, std::move(filter)));
  } else {
    data_filter_ = std::move(filter);
  }
  return *this;
}

DeleteFileIndex::Builder& DeleteFileIndex::Builder::PartitionFilter(
    std::shared_ptr<Expression> filter) {
  if (partition_filter_) {
    ICEBERG_BUILDER_ASSIGN_OR_RETURN(partition_filter_,
                                     And::Make(partition_filter_, std::move(filter)));
  } else {
    partition_filter_ = std::move(filter);
  }
  return *this;
}

DeleteFileIndex::Builder& DeleteFileIndex::Builder::FilterPartitions(
    std::shared_ptr<PartitionSet> partition_set) {
  partition_set_ = std::move(partition_set);
  return *this;
}

DeleteFileIndex::Builder& DeleteFileIndex::Builder::CaseSensitive(bool case_sensitive) {
  case_sensitive_ = case_sensitive;
  return *this;
}

DeleteFileIndex::Builder& DeleteFileIndex::Builder::IgnoreResiduals() {
  ignore_residuals_ = true;
  return *this;
}

Result<std::vector<ManifestEntry>> DeleteFileIndex::Builder::LoadDeleteFiles() {
  // Build expression caches per spec ID
  std::unordered_map<int32_t, std::shared_ptr<Expression>> part_expr_cache;
  std::unordered_map<int32_t, std::unique_ptr<ManifestEvaluator>> eval_cache;

  auto data_filter = ignore_residuals_ ? True::Instance() : data_filter_;

  // Filter and read manifests into manifest entries
  std::vector<ManifestEntry> files;
  for (const auto& manifest : delete_manifests_) {
    if (manifest.content != ManifestContent::kDeletes) {
      continue;
    }
    if (!manifest.has_added_files() && !manifest.has_existing_files()) {
      continue;
    }

    const int32_t spec_id = manifest.partition_spec_id;
    auto spec_iter = specs_by_id_.find(spec_id);
    ICEBERG_CHECK(spec_iter != specs_by_id_.cend(),
                  "Partition spec ID {} not found when loading delete files", spec_id);

    const auto& spec = spec_iter->second;

    // Get or compute projected partition expression
    if (!part_expr_cache.contains(spec_id) && data_filter_) {
      auto projector = Projections::Inclusive(*spec, *schema_, case_sensitive_);
      ICEBERG_ASSIGN_OR_RAISE(auto projected, projector->Project(data_filter_));
      part_expr_cache[spec_id] = std::move(projected);
    }

    // Get or create manifest evaluator
    if (!eval_cache.contains(spec_id)) {
      auto filter = partition_filter_;
      if (auto it = part_expr_cache.find(spec_id); it != part_expr_cache.cend()) {
        if (filter) {
          ICEBERG_ASSIGN_OR_RAISE(filter, And::Make(filter, it->second));
        } else {
          filter = it->second;
        }
      }
      if (filter) {
        ICEBERG_ASSIGN_OR_RAISE(auto evaluator,
                                ManifestEvaluator::MakePartitionFilter(
                                    std::move(filter), spec, *schema_, case_sensitive_));
        eval_cache[spec_id] = std::move(evaluator);
      }
    }

    // Evaluate manifest against filter
    if (auto it = eval_cache.find(spec_id); it != eval_cache.end()) {
      ICEBERG_ASSIGN_OR_RAISE(auto should_match, it->second->Evaluate(manifest));
      if (!should_match) {
        continue;  // Manifest doesn't match filter
      }
    }

    // Read manifest entries
    ICEBERG_ASSIGN_OR_RAISE(auto reader,
                            ManifestReader::Make(manifest, io_, schema_, spec));

    auto partition_filter = partition_filter_;
    if (auto it = part_expr_cache.find(spec_id); it != part_expr_cache.cend()) {
      if (partition_filter) {
        ICEBERG_ASSIGN_OR_RAISE(partition_filter,
                                And::Make(partition_filter, it->second));
      } else {
        partition_filter = it->second;
      }
    }
    if (partition_filter) {
      reader->FilterPartitions(std::move(partition_filter));
    }
    if (partition_set_) {
      reader->FilterPartitions(partition_set_);
    }
    reader->FilterRows(data_filter).CaseSensitive(case_sensitive_).TryDropStats();

    ICEBERG_ASSIGN_OR_RAISE(auto entries, reader->LiveEntries());
    files.reserve(files.size() + entries.size());

    for (auto& entry : entries) {
      ICEBERG_CHECK(entry.data_file != nullptr, "ManifestEntry must have a data file");
      ICEBERG_CHECK(entry.sequence_number.has_value(),
                    "Missing sequence number from delete file: {}",
                    entry.data_file->file_path);
      if (entry.sequence_number.value() > min_sequence_number_) {
        auto& file = *entry.data_file;
        // keep minimum stats to avoid memory pressure
        std::unordered_set<int32_t> columns =
            file.content == DataFile::Content::kPositionDeletes
                ? std::unordered_set<int32_t>{MetadataColumns::kDeleteFilePathColumnId}
                : std::unordered_set<int32_t>(file.equality_ids.begin(),
                                              file.equality_ids.end());
        ContentFileUtil::DropUnselectedStats(*entry.data_file, columns);
        files.emplace_back(std::move(entry));
      }
    }
  }

  return files;
}

Status DeleteFileIndex::Builder::AddDV(
    std::unordered_map<std::string, ManifestEntry>& dv_by_path, ManifestEntry&& entry) {
  ICEBERG_PRECHECK(entry.data_file != nullptr, "ManifestEntry must have a data file");
  ICEBERG_PRECHECK(entry.sequence_number.has_value(),
                   "Missing sequence number from DV {}", entry.data_file->file_path);

  const auto& path = entry.data_file->referenced_data_file;
  ICEBERG_PRECHECK(path.has_value(), "DV must have a referenced data file");

  std::string referenced_path = path.value();
  auto [it, inserted] = dv_by_path.emplace(referenced_path, std::move(entry));
  if (!inserted) {
    return ValidationFailed("Can't index multiple DVs for {}", referenced_path);
  }
  return {};
}

Status DeleteFileIndex::Builder::AddPositionDelete(
    std::unordered_map<std::string, std::unique_ptr<internal::PositionDeletes>>&
        deletes_by_path,
    PartitionMap<std::unique_ptr<internal::PositionDeletes>>& deletes_by_partition,
    ManifestEntry&& entry) {
  ICEBERG_PRECHECK(entry.data_file != nullptr, "ManifestEntry must have a data file");
  ICEBERG_PRECHECK(entry.sequence_number.has_value(),
                   "Missing sequence number from position delete {}",
                   entry.data_file->file_path);

  ICEBERG_ASSIGN_OR_RAISE(auto referenced_path,
                          ContentFileUtil::ReferencedDataFile(*entry.data_file));

  if (referenced_path.has_value()) {
    // File-scoped position delete
    auto& deletes = deletes_by_path[referenced_path.value()];
    if (!deletes) {
      deletes = std::make_unique<internal::PositionDeletes>();
    }
    ICEBERG_RETURN_UNEXPECTED(deletes->Add(std::move(entry)));
  } else {
    // Partition-scoped position delete
    ICEBERG_PRECHECK(entry.data_file->partition_spec_id.has_value(),
                     "Missing partition spec id from position delete {}",
                     entry.data_file->file_path);
    int32_t spec_id = entry.data_file->partition_spec_id.value();
    const auto& partition = entry.data_file->partition;

    auto existing = deletes_by_partition.get(spec_id, partition);
    if (existing.has_value()) {
      ICEBERG_RETURN_UNEXPECTED(existing->get()->Add(std::move(entry)));
    } else {
      auto deletes = std::make_unique<internal::PositionDeletes>();
      ICEBERG_RETURN_UNEXPECTED(deletes->Add(std::move(entry)));
      deletes_by_partition.put(spec_id, partition, std::move(deletes));
    }
  }

  return {};
}

Status DeleteFileIndex::Builder::AddEqualityDelete(
    internal::EqualityDeletes& global_deletes,
    PartitionMap<std::unique_ptr<internal::EqualityDeletes>>& deletes_by_partition,
    ManifestEntry&& entry) {
  ICEBERG_PRECHECK(entry.data_file != nullptr, "ManifestEntry must have a data file");
  ICEBERG_PRECHECK(entry.sequence_number.has_value(),
                   "Missing sequence number from equality delete {}",
                   entry.data_file->file_path);
  ICEBERG_PRECHECK(entry.data_file->partition_spec_id.has_value(),
                   "Missing partition spec id from equality delete {}",
                   entry.data_file->file_path);

  int32_t spec_id = entry.data_file->partition_spec_id.value();

  auto spec_it = specs_by_id_.find(spec_id);
  if (spec_it == specs_by_id_.end()) {
    return InvalidArgument("Unknown partition spec ID: {}", spec_id);
  }
  const auto& spec = spec_it->second;

  if (spec->fields().empty()) {
    // Global equality delete for unpartitioned tables
    ICEBERG_RETURN_UNEXPECTED(global_deletes.Add(std::move(entry)));
  } else {
    // Partition-scoped equality delete
    const auto& partition = entry.data_file->partition;

    auto existing = deletes_by_partition.get(spec_id, partition);
    if (existing.has_value()) {
      ICEBERG_RETURN_UNEXPECTED(existing->get()->Add(std::move(entry)));
    } else {
      auto deletes = std::make_unique<internal::EqualityDeletes>(*schema_);
      ICEBERG_RETURN_UNEXPECTED(deletes->Add(std::move(entry)));
      deletes_by_partition.put(spec_id, partition, std::move(deletes));
    }
  }

  return {};
}

Result<std::unique_ptr<DeleteFileIndex>> DeleteFileIndex::Builder::Build() {
  ICEBERG_RETURN_UNEXPECTED(CheckErrors());

  std::vector<ManifestEntry> entries;
  if (!delete_manifests_.empty()) {
    ICEBERG_ASSIGN_OR_RAISE(entries, LoadDeleteFiles());
  }

  // Build index structures
  auto global_deletes = std::make_unique<internal::EqualityDeletes>(*schema_);
  auto eq_deletes_by_partition =
      std::make_unique<PartitionMap<std::unique_ptr<internal::EqualityDeletes>>>();
  auto pos_deletes_by_partition =
      std::make_unique<PartitionMap<std::unique_ptr<internal::PositionDeletes>>>();
  auto pos_deletes_by_path = std::make_unique<
      std::unordered_map<std::string, std::unique_ptr<internal::PositionDeletes>>>();
  auto dv_by_path = std::make_unique<std::unordered_map<std::string, ManifestEntry>>();

  for (auto& entry : entries) {
    ICEBERG_CHECK(entry.data_file != nullptr, "ManifestEntry must have a data file");

    switch (entry.data_file->content) {
      case DataFile::Content::kPositionDeletes:
        if (ContentFileUtil::IsDV(*entry.data_file)) {
          ICEBERG_RETURN_UNEXPECTED(AddDV(*dv_by_path, std::move(entry)));
        } else {
          ICEBERG_RETURN_UNEXPECTED(AddPositionDelete(
              *pos_deletes_by_path, *pos_deletes_by_partition, std::move(entry)));
        }
        break;

      case DataFile::Content::kEqualityDeletes:
        ICEBERG_RETURN_UNEXPECTED(AddEqualityDelete(
            *global_deletes, *eq_deletes_by_partition, std::move(entry)));
        break;

      default:
        return NotSupported("Unsupported content type: {}",
                            static_cast<int>(entry.data_file->content));
    }
  }

  return std::unique_ptr<DeleteFileIndex>(new DeleteFileIndex(
      global_deletes->empty() ? nullptr : std::move(global_deletes),
      eq_deletes_by_partition->empty() ? nullptr : std::move(eq_deletes_by_partition),
      pos_deletes_by_partition->empty() ? nullptr : std::move(pos_deletes_by_partition),
      pos_deletes_by_path->empty() ? nullptr : std::move(pos_deletes_by_path),
      dv_by_path->empty() ? nullptr : std::move(dv_by_path)));
}

}  // namespace iceberg
