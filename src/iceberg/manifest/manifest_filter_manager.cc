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

#include "iceberg/manifest/manifest_filter_manager.h"

#include <algorithm>
#include <string>
#include <tuple>
#include <unordered_set>
#include <vector>

#include "iceberg/expression/expression.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/expression/inclusive_metrics_evaluator.h"
#include "iceberg/expression/manifest_evaluator.h"
#include "iceberg/expression/residual_evaluator.h"
#include "iceberg/expression/strict_metrics_evaluator.h"
#include "iceberg/file_io.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/partition_spec.h"
#include "iceberg/result.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

using PartitionSpecsById = ManifestFilterManager::PartitionSpecsById;

bool HasRowFilterExpression(const std::shared_ptr<Expression>& expr) {
  return expr != nullptr && expr->op() != Expression::Operation::kFalse;
}

Result<std::shared_ptr<PartitionSpec>> PartitionSpecById(
    const PartitionSpecsById& specs_by_id, int32_t spec_id) {
  auto iter = specs_by_id.find(spec_id);
  if (iter == specs_by_id.end() || iter->second == nullptr) {
    return NotFound("Partition spec with ID {} is not found", spec_id);
  }
  return iter->second;
}

struct FieldSummary {
  std::shared_ptr<PrimitiveType> type;
  bool contains_null;
  std::optional<bool> contains_nan;
  std::optional<Literal> lower;
  std::optional<Literal> upper;

  bool CanContain(const Literal& value) const {
    if (value.IsNull()) return contains_null;
    if (value.IsNaN()) return contains_nan.value_or(true);
    if (type->type_id() == TypeId::kUnknown) return true;
    // An all-null or all-NaN field has no ordinary values to compare.
    if (!lower && !upper && (contains_null || contains_nan.value_or(false))) return false;
    auto in_bounds = [&](const Literal& candidate) {
      return (!lower || candidate >= *lower) && (!upper || candidate <= *upper);
    };
    // Some callers use the physical Int/Long/Binary literal for a logical field.
    if (value.type()->type_id() != type->type_id()) {
      auto logical = value.CastTo(type);
      return !logical || logical->type()->type_id() != type->type_id() ||
             in_bounds(*logical);
    }
    return in_bounds(value);
  }
};

Result<bool> CanContainAny(const ManifestFile& manifest, const PartitionSet& partitions,
                           const Schema& schema, const PartitionSpecsById& specs_by_id) {
  if (partitions.empty()) return false;
  if (manifest.partitions.empty()) return true;

  const auto same_spec = [&](const auto& key) {
    return key.first == manifest.partition_spec_id;
  };
  if (std::ranges::none_of(partitions, same_spec)) return false;

  ICEBERG_ASSIGN_OR_RAISE(auto spec,
                          PartitionSpecById(specs_by_id, manifest.partition_spec_id));
  ICEBERG_ASSIGN_OR_RAISE(auto type, spec->PartitionType(schema));
  const auto fields = type->fields();
  if (manifest.partitions.size() != fields.size()) return true;

  std::vector<FieldSummary> summaries;
  summaries.reserve(fields.size());
  for (size_t i = 0; i < fields.size(); ++i) {
    const auto& stats = manifest.partitions[i];
    auto& summary = summaries.emplace_back(FieldSummary{
        .type = internal::checked_pointer_cast<PrimitiveType>(fields[i].type()),
        .contains_null = stats.contains_null,
        .contains_nan = stats.contains_nan});
    if (summary.type->type_id() != TypeId::kUnknown) {
      if (stats.lower_bound) {
        ICEBERG_ASSIGN_OR_RAISE(summary.lower,
                                Literal::Deserialize(*stats.lower_bound, summary.type));
      }
      if (stats.upper_bound) {
        ICEBERG_ASSIGN_OR_RAISE(summary.upper,
                                Literal::Deserialize(*stats.upper_bound, summary.type));
      }
    }
  }

  return std::ranges::any_of(partitions, [&](const auto& key) {
    const auto values = key.second.values();
    if (!same_spec(key)) return false;
    // C++ file objects can have a spec ID but no partition tuple.
    if (values.size() != summaries.size()) return true;
    return std::ranges::equal(summaries, values, &FieldSummary::CanContain);
  });
}

Result<std::string> FormatPartitionPath(const PartitionSpecsById& specs_by_id,
                                        const DataFile& file, int32_t spec_id) {
  ICEBERG_ASSIGN_OR_RAISE(auto spec, PartitionSpecById(specs_by_id, spec_id));
  return spec->PartitionPath(file.partition);
}

void AddDeletedFileToManager(ManifestContent manifest_content, DataFileSet& data_files,
                             DeleteFileSet& delete_files,
                             std::vector<std::shared_ptr<DataFile>>& deleted_files,
                             DataFileSet& deleted_data_file_set,
                             DeleteFileSet& deleted_file_set,
                             const std::shared_ptr<DataFile>& file) {
  if (file == nullptr) {
    return;
  }

  bool inserted;
  if (manifest_content == ManifestContent::kData) {
    data_files.insert(file);
    inserted = deleted_data_file_set.insert(file).second;
  } else {
    delete_files.insert(file);
    inserted = deleted_file_set.insert(file).second;
  }
  if (inserted) {
    deleted_files.push_back(file);
  }
}

bool AddDeletedFileToManifest(ManifestContent manifest_content,
                              std::vector<std::shared_ptr<DataFile>>& deleted_files,
                              DataFileSet& deleted_data_file_set,
                              DeleteFileSet& deleted_file_set,
                              const std::shared_ptr<DataFile>& file) {
  if (file == nullptr) {
    return false;
  }
  bool inserted = manifest_content == ManifestContent::kData
                      ? deleted_data_file_set.insert(file).second
                      : deleted_file_set.insert(file).second;
  if (inserted) {
    deleted_files.push_back(file);
  }
  return inserted;
}

}  // namespace

Result<std::unique_ptr<ManifestFilterManager>> ManifestFilterManager::Make(
    ManifestContent content, std::shared_ptr<FileIO> file_io,
    std::function<Status(const std::string&)> delete_file) {
  ICEBERG_PRECHECK(file_io != nullptr, "FileIO cannot be null");
  return std::unique_ptr<ManifestFilterManager>(
      new ManifestFilterManager(content, std::move(file_io), std::move(delete_file)));
}

ManifestFilterManager::ManifestFilterManager(
    ManifestContent content, std::shared_ptr<FileIO> file_io,
    std::function<Status(const std::string&)> delete_file)
    : manifest_content_(content),
      file_io_(std::move(file_io)),
      delete_file_(std::move(delete_file)),
      delete_expr_(Expressions::AlwaysFalse()) {
  ICEBERG_DCHECK(file_io_, "FileIO cannot be null");
  if (delete_file_ == nullptr) {
    delete_file_ = [this](const std::string& location) {
      return file_io_->DeleteFile(location);
    };
  }
}

ManifestFilterManager::~ManifestFilterManager() = default;

Status ManifestFilterManager::DeleteByRowFilter(std::shared_ptr<Expression> expr) {
  ICEBERG_PRECHECK(expr != nullptr, "Cannot delete files using filter: null");
  ICEBERG_RETURN_UNEXPECTED(InvalidateFilteredCache());
  ICEBERG_ASSIGN_OR_RAISE(delete_expr_, Or::MakeFolded(delete_expr_, std::move(expr)));
  all_deletes_reference_manifests_ = false;
  manifest_evaluator_cache_.clear();
  residual_evaluator_cache_.clear();
  return {};
}

void ManifestFilterManager::CaseSensitive(bool case_sensitive) {
  if (case_sensitive_ == case_sensitive) return;
  case_sensitive_ = case_sensitive;
  if (HasRowFilterExpression(delete_expr_)) {
    std::ignore = InvalidateFilteredCache();
  }
  manifest_evaluator_cache_.clear();
  residual_evaluator_cache_.clear();
}

Status ManifestFilterManager::DeleteFile(std::string_view path) {
  ICEBERG_RETURN_UNEXPECTED(InvalidateFilteredCache());
  all_deletes_reference_manifests_ = false;
  delete_paths_.insert(std::string(path));
  return {};
}

Status ManifestFilterManager::DeleteFile(std::shared_ptr<DataFile> file) {
  ICEBERG_PRECHECK(file != nullptr, "Cannot delete file: null");
  ICEBERG_RETURN_UNEXPECTED(InvalidateFilteredCache());
  if (file->manifest_location) {
    manifests_with_deletes_.insert(*file->manifest_location);
  } else {
    all_deletes_reference_manifests_ = false;
  }
  if (!file->partition_spec_id) {
    delete_file_partitions_.reset();
  } else if (delete_file_partitions_) {
    delete_file_partitions_->add(*file->partition_spec_id, file->partition);
  }
  if (manifest_content_ == ManifestContent::kData) {
    data_files_.insert(file);
    data_files_to_delete_.insert(std::move(file));
  } else {
    delete_files_.insert(file);
    delete_files_to_delete_.insert(std::move(file));
  }
  return {};
}

const DataFileSet& ManifestFilterManager::FilesToBeDeleted() const { return data_files_; }

const std::vector<std::shared_ptr<DataFile>>& ManifestFilterManager::DeletedFiles()
    const {
  return deleted_files_;
}

Result<SnapshotSummaryBuilder> ManifestFilterManager::BuildSummary(
    const std::vector<ManifestFile>& manifests,
    const PartitionSpecsById& specs_by_id) const {
  SnapshotSummaryBuilder summary;
  for (const auto& manifest : manifests) {
    auto deleted_iter = filtered_manifest_to_deleted_files_.find(manifest);
    if (deleted_iter == filtered_manifest_to_deleted_files_.end()) {
      continue;
    }

    ICEBERG_ASSIGN_OR_RAISE(auto spec,
                            PartitionSpecById(specs_by_id, manifest.partition_spec_id));
    for (const auto& file : deleted_iter->second.files) {
      if (file != nullptr) {
        ICEBERG_RETURN_UNEXPECTED(summary.DeletedFile(*spec, *file));
      }
    }
  }
  summary.IncrementDuplicateDeletes(duplicate_deletes_count_);
  return summary;
}

Status ManifestFilterManager::DropPartition(int32_t spec_id, PartitionValues partition) {
  ICEBERG_RETURN_UNEXPECTED(InvalidateFilteredCache());
  all_deletes_reference_manifests_ = false;
  drop_partitions_.add(spec_id, std::move(partition));
  return {};
}

void ManifestFilterManager::FailMissingDeletePaths() {
  fail_missing_delete_paths_ = true;
}

void ManifestFilterManager::FailAnyDelete() {
  if (!fail_any_delete_) {
    fail_any_delete_ = true;
    std::ignore = InvalidateFilteredCache();
  }
}

bool ManifestFilterManager::ContainsDeletes() const {
  return HasRowFilterExpression(delete_expr_) || !delete_paths_.empty() ||
         !data_files_to_delete_.empty() || !delete_files_to_delete_.empty() ||
         !drop_partitions_.empty();
}

Status ManifestFilterManager::DropDeleteFilesOlderThan(int64_t sequence_number) {
  ICEBERG_PRECHECK(sequence_number >= 0, "Invalid minimum data sequence number: {}",
                   sequence_number);
  if (min_sequence_number_ != sequence_number &&
      manifest_content_ == ManifestContent::kDeletes) {
    ICEBERG_RETURN_UNEXPECTED(InvalidateFilteredCache());
  }
  min_sequence_number_ = sequence_number;
  return {};
}

void ManifestFilterManager::RemoveDanglingDeletesFor(const DataFileSet& deleted_files) {
  std::unordered_set<std::string> removed_data_file_paths;
  for (const auto& file : deleted_files) {
    if (file != nullptr) {
      removed_data_file_paths.insert(file->file_path);
    }
  }
  if (removed_data_file_paths_ != removed_data_file_paths) {
    removed_data_file_paths_ = std::move(removed_data_file_paths);
    std::ignore = InvalidateFilteredCache();
  }
}

Result<bool> ManifestFilterManager::CanContainDroppedFiles(
    const ManifestFile& manifest, const Schema& schema,
    const PartitionSpecsById& specs_by_id) const {
  if (!delete_paths_.empty() || !removed_data_file_paths_.empty()) return true;
  if (data_files_to_delete_.empty() && delete_files_to_delete_.empty()) return false;
  if (!delete_file_partitions_) return true;
  return CanContainAny(manifest, *delete_file_partitions_, schema, specs_by_id);
}

Result<bool> ManifestFilterManager::CanContainDroppedPartitions(
    const ManifestFile& manifest, const Schema& schema,
    const PartitionSpecsById& specs_by_id) const {
  return CanContainAny(manifest, drop_partitions_, schema, specs_by_id);
}

Result<bool> ManifestFilterManager::CanContainExpressionDeletes(
    const ManifestFile& manifest, const std::shared_ptr<Schema>& schema,
    const PartitionSpecsById& specs_by_id) {
  if (!HasRowFilterExpression(delete_expr_)) return false;
  int32_t spec_id = manifest.partition_spec_id;
  ICEBERG_ASSIGN_OR_RAISE(auto spec, PartitionSpecById(specs_by_id, spec_id));
  ICEBERG_ASSIGN_OR_RAISE(auto* evaluator,
                          GetManifestEvaluator(schema, specs_by_id, spec_id));
  if (manifest.partitions.size() != spec->fields().size()) return true;
  return evaluator->Evaluate(manifest);
}

Result<bool> ManifestFilterManager::CanContainDeletedFiles(
    const ManifestFile& manifest, const std::shared_ptr<Schema>& schema,
    const PartitionSpecsById& specs_by_id, bool trust_manifest_references) {
  if (!manifest.has_added_files() && !manifest.has_existing_files()) return false;
  if (trust_manifest_references) {
    return manifests_with_deletes_.contains(manifest.manifest_path);
  }

  ICEBERG_ASSIGN_OR_RAISE(auto can_contain_dropped_files,
                          CanContainDroppedFiles(manifest, *schema, specs_by_id));
  if (can_contain_dropped_files) return true;

  ICEBERG_ASSIGN_OR_RAISE(auto can_contain_expression_deletes,
                          CanContainExpressionDeletes(manifest, schema, specs_by_id));
  if (can_contain_expression_deletes) return true;

  return CanContainDroppedPartitions(manifest, *schema, specs_by_id);
}

Result<ManifestEvaluator*> ManifestFilterManager::GetManifestEvaluator(
    const std::shared_ptr<Schema>& schema, const PartitionSpecsById& specs_by_id,
    int32_t spec_id) {
  auto& evaluator = manifest_evaluator_cache_[spec_id];
  if (!evaluator) {
    ICEBERG_ASSIGN_OR_RAISE(auto spec, PartitionSpecById(specs_by_id, spec_id));
    ICEBERG_ASSIGN_OR_RAISE(evaluator, ManifestEvaluator::MakeRowFilter(
                                           delete_expr_, spec, *schema, case_sensitive_));
  }
  return evaluator.get();
}

Result<ResidualEvaluator*> ManifestFilterManager::GetResidualEvaluator(
    const std::shared_ptr<Schema>& schema, const PartitionSpecsById& specs_by_id,
    int32_t spec_id) {
  auto& evaluator = residual_evaluator_cache_[spec_id];
  if (!evaluator) {
    ICEBERG_ASSIGN_OR_RAISE(auto spec, PartitionSpecById(specs_by_id, spec_id));
    ICEBERG_ASSIGN_OR_RAISE(evaluator, ResidualEvaluator::Make(delete_expr_, *spec,
                                                               *schema, case_sensitive_));
  }
  return evaluator.get();
}

Result<bool> ManifestFilterManager::ShouldDelete(const ManifestEntry& entry,
                                                 const std::shared_ptr<Schema>& schema,
                                                 const PartitionSpecsById& specs_by_id,
                                                 int32_t manifest_spec_id) {
  if (!entry.data_file) return false;
  const DataFile& file = *entry.data_file;
  int32_t spec_id = file.partition_spec_id.value_or(manifest_spec_id);

  // All delete branches share fail-any-delete handling.
  auto marked_for_delete = [&]() -> Result<bool> {
    if (fail_any_delete_) {
      ICEBERG_ASSIGN_OR_RAISE(auto partition_path,
                              FormatPartitionPath(specs_by_id, file, spec_id));
      return ValidationFailed("Operation would delete existing data: {}", partition_path);
    }
    return true;
  };

  // Path/object-based and partition-drop checks.
  bool object_delete = manifest_content_ == ManifestContent::kData
                           ? data_files_to_delete_.contains(file)
                           : delete_files_to_delete_.contains(file);
  if (delete_paths_.count(file.file_path) || object_delete ||
      drop_partitions_.contains(spec_id, file.partition)) {
    return marked_for_delete();
  }

  // Delete-manifest-specific cleanup (only for ManifestContent::kDeletes).
  if (manifest_content_ == ManifestContent::kDeletes) {
    // Drop delete files whose data sequence number is older than the minimum
    // retained by the table (they can no longer match any live data rows).
    // seq == 0 (kInitialSequenceNumber / nullopt) is intentionally excluded:
    // those entries predate sequence number assignment and must not be pruned.
    int64_t seq = entry.sequence_number.value_or(0);
    if (min_sequence_number_ > 0 && seq > 0 && seq < min_sequence_number_) {
      return marked_for_delete();
    }

    // Drop DVs that reference a data file that has been removed (dangling DV).
    if (!removed_data_file_paths_.empty() && file.IsDeletionVector() &&
        file.referenced_data_file.has_value() &&
        removed_data_file_paths_.count(*file.referenced_data_file)) {
      return marked_for_delete();
    }
  }

  if (HasRowFilterExpression(delete_expr_)) {
    ICEBERG_ASSIGN_OR_RAISE(auto* residual_eval,
                            GetResidualEvaluator(schema, specs_by_id, spec_id));
    ICEBERG_ASSIGN_OR_RAISE(auto residual_expr,
                            residual_eval->ResidualFor(file.partition));
    // TODO(Guotao): Cache strict/inclusive metrics evaluators per partition residual.
    ICEBERG_ASSIGN_OR_RAISE(
        auto strict_eval,
        StrictMetricsEvaluator::Make(residual_expr, schema, case_sensitive_));
    ICEBERG_ASSIGN_OR_RAISE(auto strict_match, strict_eval->Evaluate(file));
    if (strict_match) {
      return marked_for_delete();
    }

    ICEBERG_ASSIGN_OR_RAISE(auto incl_eval, InclusiveMetricsEvaluator::Make(
                                                residual_expr, *schema, case_sensitive_));
    ICEBERG_ASSIGN_OR_RAISE(auto incl_match, incl_eval->Evaluate(file));
    if (incl_match) {
      if (manifest_content_ == ManifestContent::kDeletes) {
        return false;
      }
      return ValidationFailed(
          "Cannot delete file where some, but not all, rows match filter: {}",
          file.file_path);
    }
  }

  return false;
}

bool ManifestFilterManager::CanTrustManifestReferences(
    const std::vector<const ManifestFile*>& manifests) const {
  // A dangling DV may live outside all explicitly referenced delete manifests.
  if (!all_deletes_reference_manifests_ || manifests_with_deletes_.empty() ||
      !removed_data_file_paths_.empty())
    return false;

  std::unordered_set<std::string_view> locations;
  for (const auto* manifest : manifests) locations.insert(manifest->manifest_path);
  return std::ranges::all_of(manifests_with_deletes_,
                             [&](const auto& path) { return locations.contains(path); });
}

Result<ManifestFile> ManifestFilterManager::FilterManifest(
    const std::shared_ptr<Schema>& schema, const PartitionSpecsById& specs_by_id,
    const ManifestFile& manifest, bool trust_manifest_references,
    const ManifestWriterFactory& writer_factory) {
  auto cached = filtered_manifests_.find(manifest);
  if (cached != filtered_manifests_.end()) {
    auto deleted_iter = filtered_manifest_to_deleted_files_.find(cached->second);
    if (deleted_iter != filtered_manifest_to_deleted_files_.end()) {
      for (const auto& file : deleted_iter->second.files) {
        AddDeletedFileToManager(manifest_content_, data_files_, delete_files_,
                                deleted_files_, deleted_data_file_set_,
                                deleted_delete_file_set_, file);
      }
      duplicate_deletes_count_ += deleted_iter->second.duplicate_deletes_count;
    }
    return cached->second;
  }

  ICEBERG_ASSIGN_OR_RAISE(
      auto can_contain_deleted_files,
      CanContainDeletedFiles(manifest, schema, specs_by_id, trust_manifest_references));
  if (!can_contain_deleted_files) {
    filtered_manifests_.emplace(manifest, manifest);
    return manifest;
  }

  int32_t spec_id = manifest.partition_spec_id;
  ICEBERG_ASSIGN_OR_RAISE(auto spec, PartitionSpecById(specs_by_id, spec_id));
  ICEBERG_ASSIGN_OR_RAISE(auto reader,
                          ManifestReader::Make(manifest, file_io_, schema, spec));
  ICEBERG_ASSIGN_OR_RAISE(auto entries, reader->LiveEntries());

  ICEBERG_ASSIGN_OR_RAISE(auto has_deleted_files,
                          ManifestHasDeletedFiles(entries, schema, specs_by_id, spec_id));
  if (!has_deleted_files) {
    filtered_manifests_.emplace(manifest, manifest);
    return manifest;
  }

  ICEBERG_ASSIGN_OR_RAISE(auto filtered_manifest,
                          FilterManifestWithDeletedFiles(entries, spec_id, schema,
                                                         specs_by_id, writer_factory));
  filtered_manifests_.emplace(manifest, filtered_manifest);
  ++replaced_manifests_count_;
  return filtered_manifest;
}

Result<bool> ManifestFilterManager::ManifestHasDeletedFiles(
    const std::vector<ManifestEntry>& entries, const std::shared_ptr<Schema>& schema,
    const PartitionSpecsById& specs_by_id, int32_t manifest_spec_id) {
  for (const auto& entry : entries) {
    ICEBERG_ASSIGN_OR_RAISE(auto should_delete,
                            ShouldDelete(entry, schema, specs_by_id, manifest_spec_id));
    if (should_delete) {
      return true;
    }
  }
  return false;
}

Result<ManifestFile> ManifestFilterManager::FilterManifestWithDeletedFiles(
    const std::vector<ManifestEntry>& entries, int32_t manifest_spec_id,
    const std::shared_ptr<Schema>& schema, const PartitionSpecsById& specs_by_id,
    const ManifestWriterFactory& writer_factory) {
  ICEBERG_ASSIGN_OR_RAISE(auto writer,
                          writer_factory(manifest_spec_id, manifest_content_));
  std::vector<std::shared_ptr<DataFile>> deleted_files;
  DataFileSet deleted_data_file_set;
  DeleteFileSet deleted_file_set;
  int32_t duplicate_deletes_count = 0;
  for (const auto& entry : entries) {
    ICEBERG_ASSIGN_OR_RAISE(auto should_delete,
                            ShouldDelete(entry, schema, specs_by_id, manifest_spec_id));
    if (should_delete) {
      if (entry.data_file) {
        auto file = std::make_shared<DataFile>(*entry.data_file);
        AddDeletedFileToManager(manifest_content_, data_files_, delete_files_,
                                deleted_files_, deleted_data_file_set_,
                                deleted_delete_file_set_, file);
        if (!AddDeletedFileToManifest(manifest_content_, deleted_files,
                                      deleted_data_file_set, deleted_file_set, file)) {
          ++duplicate_deletes_count;
        }
      }
      ICEBERG_RETURN_UNEXPECTED(writer->WriteDeletedEntry(entry));
    } else {
      ICEBERG_RETURN_UNEXPECTED(writer->WriteExistingEntry(entry));
    }
  }

  ICEBERG_RETURN_UNEXPECTED(writer->Close());
  ICEBERG_ASSIGN_OR_RAISE(auto filtered_manifest, writer->ToManifestFile());
  duplicate_deletes_count_ += duplicate_deletes_count;
  filtered_manifest_to_deleted_files_[filtered_manifest] = FilteredManifestDeletes{
      .files = std::move(deleted_files),
      .duplicate_deletes_count = duplicate_deletes_count,
  };
  return filtered_manifest;
}

Status ManifestFilterManager::InvalidateFilteredCache() {
  ICEBERG_RETURN_UNEXPECTED(CleanUncommitted({}));
  replaced_manifests_count_ = 0;
  return {};
}

void ManifestFilterManager::ResetDeletedFiles() {
  data_files_.clear();
  for (const auto& file : data_files_to_delete_) {
    data_files_.insert(file);
  }
  delete_files_.clear();
  for (const auto& file : delete_files_to_delete_) {
    delete_files_.insert(file);
  }
}

Status ManifestFilterManager::ValidateRequiredDeletes() const {
  if (!fail_missing_delete_paths_) {
    return {};
  }

  std::string missing_files;
  const auto append_missing = [&missing_files](const std::string& path) {
    if (!missing_files.empty()) missing_files += ",";
    missing_files += path;
  };
  for (const auto& key : data_files_to_delete_) {
    if (!deleted_data_file_set_.contains(key)) {
      append_missing(key->file_path);
    }
  }
  for (const auto& key : delete_files_to_delete_) {
    if (!deleted_delete_file_set_.contains(key)) {
      append_missing(key->file_path);
    }
  }
  if (!missing_files.empty()) {
    return ValidationFailed("Missing required files to delete: {}", missing_files);
  }

  std::string missing_paths;
  for (const auto& path : delete_paths_) {
    bool found = false;
    for (const auto& deleted_file : deleted_files_) {
      if (deleted_file != nullptr && deleted_file->file_path == path) {
        found = true;
        break;
      }
    }
    if (!found) {
      if (!missing_paths.empty()) missing_paths += ",";
      missing_paths += path;
    }
  }
  if (!missing_paths.empty()) {
    return ValidationFailed("Missing required files to delete: {}", missing_paths);
  }
  return {};
}

Result<std::vector<ManifestFile>> ManifestFilterManager::FilterManifests(
    const TableMetadata& metadata, const std::shared_ptr<Snapshot>& base_snapshot,
    const ManifestWriterFactory& writer_factory) {
  ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());
  return FilterManifests(schema, metadata, base_snapshot, writer_factory);
}

Result<std::vector<ManifestFile>> ManifestFilterManager::FilterManifests(
    const std::shared_ptr<Schema>& schema, const TableMetadata& metadata,
    const std::shared_ptr<Snapshot>& base_snapshot,
    const ManifestWriterFactory& writer_factory) {
  ResetDeletedFiles();
  deleted_files_.clear();
  deleted_data_file_set_.clear();
  deleted_delete_file_set_.clear();
  duplicate_deletes_count_ = 0;
  if (!base_snapshot) {
    ICEBERG_RETURN_UNEXPECTED(ValidateRequiredDeletes());
    return std::vector<ManifestFile>{};
  }

  ICEBERG_PRECHECK(file_io_ != nullptr, "Cannot filter manifests: FileIO is null");

  ICEBERG_ASSIGN_OR_RAISE(
      auto list_reader, ManifestListReader::Make(base_snapshot->manifest_list, file_io_));
  ICEBERG_ASSIGN_OR_RAISE(auto all_manifests, list_reader->Files());

  std::vector<const ManifestFile*> manifests;
  manifests.reserve(all_manifests.size());
  for (auto& manifest : all_manifests) {
    manifests.push_back(&manifest);
  }

  TableMetadataCache metadata_cache(&metadata);
  ICEBERG_ASSIGN_OR_RAISE(auto specs_by_id, metadata_cache.GetPartitionSpecsById());

  return FilterManifests(schema, specs_by_id.get(), manifests, writer_factory);
}

Result<std::vector<ManifestFile>> ManifestFilterManager::FilterManifests(
    const std::shared_ptr<Schema>& schema, const PartitionSpecsById& specs_by_id,
    const std::vector<const ManifestFile*>& input_manifests,
    const ManifestWriterFactory& writer_factory) {
  ICEBERG_PRECHECK(schema != nullptr, "Cannot filter manifests: schema is null");
  ICEBERG_PRECHECK(file_io_ != nullptr, "Cannot filter manifests: FileIO is null");

  std::vector<const ManifestFile*> manifests;
  manifests.reserve(input_manifests.size());
  for (const auto* manifest : input_manifests) {
    ICEBERG_PRECHECK(manifest != nullptr, "Cannot filter manifests: manifest is null");
    if (manifest->content == manifest_content_) {
      manifests.push_back(manifest);
    }
  }

  ResetDeletedFiles();
  deleted_files_.clear();
  deleted_data_file_set_.clear();
  deleted_delete_file_set_.clear();
  duplicate_deletes_count_ = 0;
  if (manifests.empty()) {
    ICEBERG_RETURN_UNEXPECTED(ValidateRequiredDeletes());
    return std::vector<ManifestFile>{};
  }

  const bool trust_manifest_references = CanTrustManifestReferences(manifests);
  if (trust_manifest_references != trusted_manifest_references_) {
    // An unrelated manifest skipped by provenance may contain a duplicate of
    // the target file once its referenced manifest is no longer in the snapshot.
    std::unordered_set<std::string> still_valid;
    for (const auto& [input, output] : filtered_manifests_) {
      if (manifests_with_deletes_.contains(input.manifest_path))
        still_valid.insert(output.manifest_path);
    }
    ICEBERG_RETURN_UNEXPECTED(CleanUncommitted(still_valid));
    trusted_manifest_references_ = trust_manifest_references;
  }
  manifest_evaluator_cache_.clear();
  residual_evaluator_cache_.clear();

  // TODO(Guotao): Parallelize manifest filtering with per-manifest results, then
  // merge found paths and deleted files after the loop.
  std::vector<ManifestFile> filtered;
  filtered.reserve(manifests.size());
  for (const auto* manifest_ptr : manifests) {
    ICEBERG_ASSIGN_OR_RAISE(auto filtered_manifest,
                            FilterManifest(schema, specs_by_id, *manifest_ptr,
                                           trust_manifest_references, writer_factory));
    filtered.push_back(std::move(filtered_manifest));
  }

  ICEBERG_RETURN_UNEXPECTED(ValidateRequiredDeletes());
  return filtered;
}

Status ManifestFilterManager::CleanUncommitted(
    const std::unordered_set<std::string>& committed) {
  auto entries = std::vector<std::pair<ManifestFile, ManifestFile>>{
      filtered_manifests_.begin(), filtered_manifests_.end()};
  for (const auto& [manifest, filtered] : entries) {
    if (committed.contains(filtered.manifest_path)) {
      continue;
    }

    if (manifest != filtered) {
      std::ignore = delete_file_(filtered.manifest_path);
      if (replaced_manifests_count_ > 0) {
        --replaced_manifests_count_;
      }
    }
    filtered_manifests_.erase(manifest);
    filtered_manifest_to_deleted_files_.erase(filtered);
  }
  return {};
}

}  // namespace iceberg
