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
#include <optional>
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
#include "iceberg/logging/log_macros.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/partition_spec.h"
#include "iceberg/result.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/content_file_util.h"
#include "iceberg/util/error_util_internal.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/task_group_internal.h"

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
    return same_spec(key) && values.size() == summaries.size() &&
           std::ranges::equal(summaries, values, &FieldSummary::CanContain);
  });
}

}  // namespace

bool ManifestFilterManager::FileSet::Contains(ManifestContent content,
                                              const DataFile& file) const {
  return content == ManifestContent::kData ? data.contains(file) : deletes.contains(file);
}

void ManifestFilterManager::FileSet::Add(ManifestContent content,
                                         const std::shared_ptr<DataFile>& file) {
  if (content == ManifestContent::kData) {
    data.insert(file);
  } else {
    deletes.insert(file);
  }
}

std::span<const std::shared_ptr<DataFile>> ManifestFilterManager::FileSet::Files(
    ManifestContent content) const {
  return content == ManifestContent::kData ? data.as_span() : deletes.as_span();
}

class ManifestFilterManager::PartitionAndMetricsEvaluator {
 public:
  PartitionAndMetricsEvaluator(const std::shared_ptr<Schema>& schema,
                               std::unique_ptr<ResidualEvaluator> residual,
                               bool case_sensitive)
      : schema_(schema),
        residual_(std::move(residual)),
        case_sensitive_(case_sensitive) {}

  Result<bool> RowsMightMatch(const DataFile& file) {
    ICEBERG_ASSIGN_OR_RAISE(auto evaluators, ForPartition(file.partition));
    return evaluators->inclusive->Evaluate(file);
  }

  Result<bool> RowsMustMatch(const DataFile& file) {
    ICEBERG_ASSIGN_OR_RAISE(auto evaluators, ForPartition(file.partition));
    return evaluators->strict->Evaluate(file);
  }

 private:
  struct MetricsEvaluators {
    std::unique_ptr<InclusiveMetricsEvaluator> inclusive;
    std::unique_ptr<StrictMetricsEvaluator> strict;
  };

  Result<const MetricsEvaluators*> ForPartition(const PartitionValues& partition) {
    if (auto it = metrics_.find(partition); it != metrics_.end()) return &it->second;

    ICEBERG_ASSIGN_OR_RAISE(auto expr, residual_->ResidualFor(partition));
    ICEBERG_ASSIGN_OR_RAISE(
        auto inclusive, InclusiveMetricsEvaluator::Make(expr, *schema_, case_sensitive_));
    ICEBERG_ASSIGN_OR_RAISE(auto strict, StrictMetricsEvaluator::Make(
                                             std::move(expr), schema_, case_sensitive_));
    auto it = metrics_
                  .emplace(partition,
                           MetricsEvaluators{std::move(inclusive), std::move(strict)})
                  .first;
    return &it->second;
  }

  std::shared_ptr<Schema> schema_;
  std::unique_ptr<ResidualEvaluator> residual_;
  bool case_sensitive_;
  std::unordered_map<PartitionValues, MetricsEvaluators, PartitionValuesHash,
                     PartitionValuesEqual>
      metrics_;
};

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
  if (!delete_file_) {
    delete_file_ = [this](const std::string& location) {
      return file_io_->DeleteFile(location);
    };
  }
}

ManifestFilterManager::~ManifestFilterManager() = default;

Status ManifestFilterManager::DeleteByRowFilter(std::shared_ptr<Expression> expr) {
  ICEBERG_PRECHECK(expr != nullptr, "Cannot delete files using filter: null");
  ICEBERG_RETURN_UNEXPECTED(CleanUncommitted({}));
  ICEBERG_ASSIGN_OR_RAISE(delete_expr_, Or::MakeFolded(delete_expr_, std::move(expr)));
  all_deletes_reference_manifests_ = false;
  return {};
}

void ManifestFilterManager::CaseSensitive(bool case_sensitive) {
  if (case_sensitive_ == case_sensitive) return;
  case_sensitive_ = case_sensitive;
  if (HasRowFilterExpression(delete_expr_)) {
    files_to_delete_ = requested_files_;
    std::ignore = CleanUncommitted({});
  }
}

Status ManifestFilterManager::DeleteFile(std::string_view path) {
  ICEBERG_RETURN_UNEXPECTED(CleanUncommitted({}));
  all_deletes_reference_manifests_ = false;
  delete_paths_.emplace(path);
  return {};
}

Status ManifestFilterManager::DeleteFile(std::shared_ptr<DataFile> file) {
  ICEBERG_PRECHECK(file != nullptr, "Cannot delete file: null");
  ICEBERG_RETURN_UNEXPECTED(CleanUncommitted({}));
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
  requested_files_.Add(manifest_content_, file);
  files_to_delete_.Add(manifest_content_, file);
  return {};
}

const DataFileSet& ManifestFilterManager::FilesToBeDeleted() const {
  return files_to_delete_.data;
}

const std::vector<std::shared_ptr<DataFile>>& ManifestFilterManager::DeletedFiles()
    const {
  return deleted_files_;
}

Result<SnapshotSummaryBuilder> ManifestFilterManager::BuildSummary(
    const std::vector<ManifestFile>& manifests,
    const PartitionSpecsById& specs_by_id) const {
  SnapshotSummaryBuilder summary;
  for (const auto& manifest : manifests) {
    auto it = filtered_manifest_deletes_.find(manifest);
    if (it == filtered_manifest_deletes_.end()) continue;
    ICEBERG_ASSIGN_OR_RAISE(auto spec,
                            PartitionSpecById(specs_by_id, manifest.partition_spec_id));
    for (const auto& file : it->second.files.Files(manifest_content_)) {
      ICEBERG_RETURN_UNEXPECTED(summary.DeletedFile(*spec, *file));
    }
    summary.IncrementDuplicateDeletes(it->second.duplicates);
  }
  return summary;
}

Status ManifestFilterManager::DropPartition(int32_t spec_id, PartitionValues partition) {
  ICEBERG_RETURN_UNEXPECTED(CleanUncommitted({}));
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
    std::ignore = CleanUncommitted({});
  }
}

bool ManifestFilterManager::ContainsDeletes() const {
  return HasRowFilterExpression(delete_expr_) || !delete_paths_.empty() ||
         !files_to_delete_.Files(manifest_content_).empty() || !drop_partitions_.empty();
}

Status ManifestFilterManager::DropDeleteFilesOlderThan(int64_t sequence_number) {
  ICEBERG_PRECHECK(sequence_number >= 0, "Invalid minimum data sequence number: {}",
                   sequence_number);
  if (min_sequence_number_ != sequence_number &&
      manifest_content_ == ManifestContent::kDeletes) {
    if (sequence_number < min_sequence_number_) files_to_delete_ = requested_files_;
    ICEBERG_RETURN_UNEXPECTED(CleanUncommitted({}));
  }
  min_sequence_number_ = sequence_number;
  return {};
}

void ManifestFilterManager::RemoveDanglingDeletesFor(const DataFileSet& deleted_files) {
  std::unordered_set<std::string> paths;
  for (const auto& file : deleted_files) paths.insert(file->file_path);
  if (removed_data_file_paths_ != paths) {
    if (!std::ranges::all_of(removed_data_file_paths_,
                             [&](const auto& path) { return paths.contains(path); })) {
      files_to_delete_ = requested_files_;
    }
    removed_data_file_paths_ = std::move(paths);
    std::ignore = CleanUncommitted({});
  }
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

Result<bool> ManifestFilterManager::CanContainDeletedFiles(
    const ManifestFile& manifest, const Schema& schema,
    const PartitionSpecsById& specs_by_id, bool trust_manifest_references) const {
  if (!manifest.has_added_files() && !manifest.has_existing_files()) return false;
  if (trust_manifest_references)
    return manifests_with_deletes_.contains(manifest.manifest_path);

  ICEBERG_ASSIGN_OR_RAISE(bool dropped_files,
                          CanContainDroppedFiles(manifest, schema, specs_by_id));
  if (dropped_files) return true;
  ICEBERG_ASSIGN_OR_RAISE(bool expression_deletes,
                          CanContainExpressionDeletes(manifest, schema, specs_by_id));
  if (expression_deletes) return true;
  return CanContainDroppedPartitions(manifest, schema, specs_by_id);
}

Result<bool> ManifestFilterManager::CanContainDroppedFiles(
    const ManifestFile& manifest, const Schema& schema,
    const PartitionSpecsById& specs_by_id) const {
  if (!delete_paths_.empty() || !removed_data_file_paths_.empty()) return true;
  if (files_to_delete_.Files(manifest_content_).empty()) return false;
  if (!delete_file_partitions_) return true;
  return CanContainAny(manifest, *delete_file_partitions_, schema, specs_by_id);
}

Result<bool> ManifestFilterManager::CanContainExpressionDeletes(
    const ManifestFile& manifest, const Schema& schema,
    const PartitionSpecsById& specs_by_id) const {
  if (!HasRowFilterExpression(delete_expr_)) return false;
  ICEBERG_ASSIGN_OR_RAISE(auto spec,
                          PartitionSpecById(specs_by_id, manifest.partition_spec_id));
  ICEBERG_ASSIGN_OR_RAISE(
      auto evaluator,
      ManifestEvaluator::MakeRowFilter(delete_expr_, spec, schema, case_sensitive_));
  if (manifest.partitions.size() != spec->fields().size()) return true;
  return evaluator->Evaluate(manifest);
}

Result<bool> ManifestFilterManager::CanContainDroppedPartitions(
    const ManifestFile& manifest, const Schema& schema,
    const PartitionSpecsById& specs_by_id) const {
  return CanContainAny(manifest, drop_partitions_, schema, specs_by_id);
}

Result<bool> ManifestFilterManager::ShouldDelete(
    const ManifestEntry& entry, int32_t spec_id,
    PartitionAndMetricsEvaluator& evaluator) const {
  const auto& file = *entry.data_file;
  if (delete_paths_.contains(file.file_path) ||
      files_to_delete_.Contains(manifest_content_, file) ||
      drop_partitions_.contains(spec_id, file.partition))
    return true;

  if (manifest_content_ == ManifestContent::kDeletes) {
    const auto sequence_number = entry.sequence_number.value_or(0);
    if (sequence_number > 0 && sequence_number < min_sequence_number_) return true;
    if (IsDanglingDV(file)) return true;
  }

  if (!HasRowFilterExpression(delete_expr_)) return false;
  ICEBERG_ASSIGN_OR_RAISE(bool may_match, evaluator.RowsMightMatch(file));
  if (!may_match) return false;
  ICEBERG_ASSIGN_OR_RAISE(bool must_match, evaluator.RowsMustMatch(file));
  if (!must_match && manifest_content_ == ManifestContent::kData) {
    return ValidationFailed(
        "Cannot delete file where some, but not all, rows match filter {}: {}",
        delete_expr_->ToString(), file.file_path);
  }
  return must_match;
}

bool ManifestFilterManager::IsDanglingDV(const DataFile& file) const {
  return ContentFileUtil::IsDV(file) && file.referenced_data_file &&
         removed_data_file_paths_.contains(*file.referenced_data_file);
}

Result<bool> ManifestFilterManager::ManifestHasDeletedFiles(
    ManifestReader& reader, const PartitionSpec& spec,
    PartitionAndMetricsEvaluator& evaluator) const {
  ICEBERG_ASSIGN_OR_RAISE(auto entries, reader.LiveEntriesStream());
  while (true) {
    ICEBERG_ASSIGN_OR_RAISE(auto entry, entries->Next());
    if (!entry) return false;
    ICEBERG_ASSIGN_OR_RAISE(auto should_delete,
                            ShouldDelete(*entry, spec.spec_id(), evaluator));
    if (should_delete) {
      if (fail_any_delete_) {
        ICEBERG_ASSIGN_OR_RAISE(auto path,
                                spec.PartitionPath(entry->data_file->partition));
        return ValidationFailed("Operation would delete existing data: {}", path);
      }
      return true;
    }
  }
}

Result<ManifestFilterManager::FilteredManifest> ManifestFilterManager::FilterManifest(
    const std::shared_ptr<Schema>& schema, const PartitionSpecsById& specs_by_id,
    const ManifestFile& manifest, bool trust_manifest_references,
    const ManifestWriterFactory& writer_factory) {
  ICEBERG_ASSIGN_OR_RAISE(
      bool can_contain,
      CanContainDeletedFiles(manifest, *schema, specs_by_id, trust_manifest_references));
  if (!can_contain) return FilteredManifest{.manifest = manifest};

  ICEBERG_ASSIGN_OR_RAISE(auto spec,
                          PartitionSpecById(specs_by_id, manifest.partition_spec_id));
  ICEBERG_ASSIGN_OR_RAISE(auto reader,
                          ManifestReader::Make(manifest, file_io_, schema, spec));
  ICEBERG_ASSIGN_OR_RAISE(
      auto residual,
      ResidualEvaluator::Make(delete_expr_, *spec, *schema, case_sensitive_));
  PartitionAndMetricsEvaluator evaluator(schema, std::move(residual), case_sensitive_);

  if (fail_any_delete_ || !manifests_with_deletes_.contains(manifest.manifest_path)) {
    ICEBERG_ASSIGN_OR_RAISE(bool found,
                            ManifestHasDeletedFiles(*reader, *spec, evaluator));
    if (!found) return FilteredManifest{.manifest = manifest};
  }

  return FilterManifestWithDeletedFiles(*reader, manifest, evaluator, writer_factory);
}

Result<ManifestFilterManager::FilteredManifest>
ManifestFilterManager::FilterManifestWithDeletedFiles(
    ManifestReader& reader, const ManifestFile& manifest,
    PartitionAndMetricsEvaluator& evaluator,
    const ManifestWriterFactory& writer_factory) {
  const auto spec_id = manifest.partition_spec_id;
  ICEBERG_ASSIGN_OR_RAISE(auto writer, writer_factory(spec_id, manifest_content_));
  FilteredManifest result;
  auto write_entries = [&]() -> Status {
    ICEBERG_ASSIGN_OR_RAISE(auto entries, reader.LiveEntriesStream());
    while (true) {
      ICEBERG_ASSIGN_OR_RAISE(auto entry, entries->Next());
      if (!entry) return {};
      ICEBERG_ASSIGN_OR_RAISE(bool should_delete,
                              ShouldDelete(*entry, spec_id, evaluator));
      if (should_delete) {
        ICEBERG_RETURN_UNEXPECTED(writer->WriteDeletedEntry(*entry));
        if (result.deletes.files.Contains(manifest_content_, *entry->data_file)) {
          ICEBERG_LOG_WARN("Deleting a duplicate path from manifest {}: {}",
                           manifest.manifest_path, entry->data_file->file_path);
          ++result.deletes.duplicates;
        } else {
          result.deletes.files.Add(manifest_content_,
                                   entry->data_file->CopyWithoutStats());
        }
      } else {
        ICEBERG_RETURN_UNEXPECTED(writer->WriteExistingEntry(*entry));
      }
    }
  };
  ICEBERG_ASSIGN_OR_RAISE(result.manifest,
                          writer->WriteAndClose(write_entries, delete_file_));
  return result;
}

Status ManifestFilterManager::ValidateRequiredDeletes(const FileSet& deleted) const {
  if (!fail_missing_delete_paths_) return {};

  std::string missing;
  auto append = [&](const std::string& path) {
    if (!missing.empty()) missing += ",";
    missing += path;
  };
  for (const auto& file : files_to_delete_.Files(manifest_content_)) {
    if (!deleted.Contains(manifest_content_, *file)) append(file->file_path);
  }
  if (!missing.empty())
    return ValidationFailed("Missing required files to delete: {}", missing);

  std::unordered_set<std::string_view> deleted_paths;
  for (const auto& file : deleted.Files(manifest_content_))
    deleted_paths.insert(file->file_path);
  for (const auto& path : delete_paths_) {
    if (!deleted_paths.contains(path)) append(path);
  }
  if (!missing.empty())
    return ValidationFailed("Missing required files to delete: {}", missing);
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
  if (!base_snapshot) {
    deleted_files_.clear();
    duplicate_deletes_count_ = 0;
    ICEBERG_RETURN_UNEXPECTED(ValidateRequiredDeletes({}));
    return std::vector<ManifestFile>{};
  }

  ICEBERG_ASSIGN_OR_RAISE(
      auto reader, ManifestListReader::Make(base_snapshot->manifest_list, file_io_));
  ICEBERG_ASSIGN_OR_RAISE(auto all_manifests, reader->Files());
  std::vector<const ManifestFile*> manifests;
  manifests.reserve(all_manifests.size());
  for (const auto& manifest : all_manifests) manifests.push_back(&manifest);
  TableMetadataCache metadata_cache(&metadata);
  ICEBERG_ASSIGN_OR_RAISE(auto specs_by_id, metadata_cache.GetPartitionSpecsById());
  return FilterManifests(schema, specs_by_id.get(), manifests, writer_factory);
}

Result<std::vector<ManifestFile>> ManifestFilterManager::FilterManifests(
    const std::shared_ptr<Schema>& schema, const PartitionSpecsById& specs_by_id,
    const std::vector<const ManifestFile*>& manifests,
    const ManifestWriterFactory& writer_factory) {
  std::vector<const ManifestFile*> matching;
  matching.reserve(manifests.size());
  for (const auto* manifest : manifests) {
    ICEBERG_PRECHECK(manifest != nullptr, "Cannot filter manifests: manifest is null");
    if (manifest->content == manifest_content_) matching.push_back(manifest);
  }
  return FilterManifests(schema, specs_by_id, matching, writer_factory, std::nullopt);
}

Result<std::vector<ManifestFile>> ManifestFilterManager::FilterManifests(
    const std::shared_ptr<Schema>& schema, const PartitionSpecsById& specs_by_id,
    const std::vector<const ManifestFile*>& manifests,
    const ManifestWriterFactory& writer_factory, OptionalExecutor executor) {
  ICEBERG_PRECHECK(schema != nullptr, "Cannot filter manifests: schema is null");
  deleted_files_.clear();
  duplicate_deletes_count_ = 0;
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
  std::vector<const ManifestFile*> pending;
  std::unordered_set<std::string_view> scheduled;
  for (const auto* manifest : manifests) {
    if (!filtered_manifests_.contains(*manifest) &&
        scheduled.insert(manifest->manifest_path).second) {
      pending.push_back(manifest);
    }
  }

  std::vector<std::optional<FilteredManifest>> completed(pending.size());
  auto status = internal::RunTasksWithStopOnFailure(
      pending.size(), executor, [&](size_t index) -> Status {
        ICEBERG_ASSIGN_OR_RAISE(
            completed[index], FilterManifest(schema, specs_by_id, *pending[index],
                                             trust_manifest_references, writer_factory));
        return {};
      });

  // Keep every completed rewrite available for retry or cleanup, including when
  // another manifest failed. Only a complete pass contributes to required deletes.
  for (size_t index = 0; index < completed.size(); ++index) {
    if (!completed[index]) continue;
    auto& result = *completed[index];
    if (result.manifest != *pending[index]) {
      filtered_manifest_deletes_.emplace(result.manifest, std::move(result.deletes));
      ++replaced_manifests_count_;
    }
    filtered_manifests_.emplace(*pending[index], std::move(result.manifest));
  }
  ICEBERG_RETURN_UNEXPECTED(status);

  FileSet deleted;
  std::vector<ManifestFile> filtered;
  filtered.reserve(manifests.size());
  for (const auto* original : manifests) {
    const auto& output = filtered_manifests_.at(*original);
    filtered.push_back(output);
    auto it = filtered_manifest_deletes_.find(output);
    if (it != filtered_manifest_deletes_.end()) {
      for (const auto& file : it->second.files.Files(manifest_content_)) {
        deleted.Add(manifest_content_, file);
      }
      duplicate_deletes_count_ += it->second.duplicates;
    }
  }

  auto files = deleted.Files(manifest_content_);
  deleted_files_.assign(files.begin(), files.end());
  for (const auto& file : files) files_to_delete_.Add(manifest_content_, file);
  ICEBERG_RETURN_UNEXPECTED(ValidateRequiredDeletes(deleted));
  return filtered;
}

Status ManifestFilterManager::CleanUncommitted(
    const std::unordered_set<std::string>& committed) {
  for (auto it = filtered_manifests_.begin(); it != filtered_manifests_.end();) {
    const auto& [original, filtered] = *it;
    if (committed.contains(filtered.manifest_path)) {
      ++it;
      continue;
    }
    if (original != filtered) {
      internal::LogAndIgnoreFailure("Filtered manifest cleanup",
                                    [&] { return delete_file_(filtered.manifest_path); });
      filtered_manifest_deletes_.erase(filtered);
      --replaced_manifests_count_;
    }
    it = filtered_manifests_.erase(it);
  }
  return {};
}

}  // namespace iceberg
