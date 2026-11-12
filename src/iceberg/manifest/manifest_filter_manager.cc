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
#include <tuple>

#include "iceberg/expression/expression.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/expression/inclusive_metrics_evaluator.h"
#include "iceberg/expression/manifest_evaluator.h"
#include "iceberg/expression/residual_evaluator.h"
#include "iceberg/expression/strict_metrics_evaluator.h"
#include "iceberg/file_io.h"
#include "iceberg/logging/log_macros.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/partition_spec.h"
#include "iceberg/table_metadata.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/task_group.h"

namespace iceberg {
namespace {

using PartitionSpecsById = ManifestFilterManager::PartitionSpecsById;

Result<std::shared_ptr<PartitionSpec>> PartitionSpecById(
    const PartitionSpecsById& specs_by_id, int32_t spec_id) {
  auto iter = specs_by_id.find(spec_id);
  if (iter == specs_by_id.end() || iter->second == nullptr) {
    return NotFound("Partition spec with ID {} is not found", spec_id);
  }
  return iter->second;
}

// ManifestFileUtil.canContainAny: partition summaries are inclusive and spec-local.
Result<bool> CanContainAny(const ManifestFile& manifest, const PartitionSet& partitions,
                           const Schema& table_schema,
                           const PartitionSpecsById& specs_by_id) {
  // C++ represents both absent summaries and the unpartitioned empty tuple as {}.
  if (manifest.partitions.empty()) {
    return true;
  }
  ICEBERG_ASSIGN_OR_RAISE(auto spec,
                          PartitionSpecById(specs_by_id, manifest.partition_spec_id));
  ICEBERG_ASSIGN_OR_RAISE(auto partition_type, spec->PartitionType(table_schema));
  ICEBERG_PRECHECK(manifest.partitions.size() <= partition_type->fields().size(),
                   "Invalid partition summaries for manifest: {}",
                   manifest.manifest_path);
  for (const auto& [spec_id, partition] : partitions) {
    if (spec_id != manifest.partition_spec_id ||
        partition.num_fields() != manifest.partitions.size()) {
      continue;
    }
    bool can_contain = true;
    for (size_t pos = 0; pos < manifest.partitions.size(); ++pos) {
      const auto& value = partition.values()[pos];
      const auto& summary = manifest.partitions[pos];
      auto type =
          std::static_pointer_cast<PrimitiveType>(partition_type->fields()[pos].type());
      if (value.IsNull()) {
        can_contain = summary.contains_null;
      } else if (value.IsNaN()) {
        can_contain = summary.contains_nan.value_or(true);
      } else if (type->type_id() == TypeId::kUnknown) {
        can_contain = true;
      } else if (!summary.lower_bound.has_value()) {
        can_contain = false;
      } else {
        ICEBERG_ASSIGN_OR_RAISE(auto lower_bound,
                                Literal::Deserialize(*summary.lower_bound, type));
        if (value.value().index() != lower_bound.value().index()) {
          can_contain = false;
        } else {
          ICEBERG_PRECHECK(summary.upper_bound.has_value(),
                           "Missing upper partition bound: {}", manifest.manifest_path);
          ICEBERG_ASSIGN_OR_RAISE(auto upper_bound,
                                  Literal::Deserialize(*summary.upper_bound, type));
          ICEBERG_ASSIGN_OR_RAISE(auto typed_value, value.CastTo(type));
          can_contain = !(typed_value < lower_bound || typed_value > upper_bound);
        }
      }
      if (!can_contain) break;
    }
    if (can_contain) return true;
  }
  return false;
}

bool InsertFile(ManifestContent content, DataFileSet& data_files,
                DeleteFileSet& delete_files, const std::shared_ptr<DataFile>& file) {
  return content == ManifestContent::kData ? data_files.insert(file).second
                                           : delete_files.insert(file).second;
}

}  // namespace

// First partially evaluate the expression using the partition tuple, then check the
// remaining expression using metrics evaluators, cached for each partition.
class ManifestFilterManager::PartitionAndMetricsEvaluator {
 public:
  static Result<std::unique_ptr<PartitionAndMetricsEvaluator>> Make(
      std::shared_ptr<Schema> table_schema, const PartitionSpec& spec,
      std::shared_ptr<Expression> expr, bool case_sensitive) {
    ICEBERG_ASSIGN_OR_RAISE(
        auto residual_evaluator,
        ResidualEvaluator::Make(std::move(expr), spec, *table_schema, case_sensitive));
    return std::unique_ptr<PartitionAndMetricsEvaluator>(new PartitionAndMetricsEvaluator(
        std::move(table_schema), std::move(residual_evaluator), case_sensitive));
  }

  Result<bool> RowsMightMatch(const DataFile& file) {
    ICEBERG_ASSIGN_OR_RAISE(auto evaluators, MetricsEvaluators(file));
    return evaluators->first->Evaluate(file);
  }

  Result<bool> RowsMustMatch(const DataFile& file) {
    ICEBERG_ASSIGN_OR_RAISE(auto evaluators, MetricsEvaluators(file));
    return evaluators->second->Evaluate(file);
  }

 private:
  using Evaluators = std::pair<std::unique_ptr<InclusiveMetricsEvaluator>,
                               std::unique_ptr<StrictMetricsEvaluator>>;
  PartitionAndMetricsEvaluator(std::shared_ptr<Schema> table_schema,
                               std::unique_ptr<ResidualEvaluator> residual_evaluator,
                               bool case_sensitive)
      : table_schema_(std::move(table_schema)),
        residual_evaluator_(std::move(residual_evaluator)),
        case_sensitive_(case_sensitive) {}

  Result<Evaluators*> MetricsEvaluators(const DataFile& file) {
    auto iter = metrics_evaluators_.find(file.partition);
    if (iter == metrics_evaluators_.end()) {
      // ResidualEvaluator replaces predicates proved true/false by partition
      // projections. The remaining predicates need evaluation using file metrics.
      ICEBERG_ASSIGN_OR_RAISE(auto residual,
                              residual_evaluator_->ResidualFor(file.partition));
      ICEBERG_ASSIGN_OR_RAISE(
          auto inclusive,
          InclusiveMetricsEvaluator::Make(residual, *table_schema_, case_sensitive_));
      ICEBERG_ASSIGN_OR_RAISE(auto strict, StrictMetricsEvaluator::Make(
                                               residual, table_schema_, case_sensitive_));
      // Copy the partition: readers may reuse their partition container.
      iter = metrics_evaluators_
                 .emplace(file.partition,
                          Evaluators{std::move(inclusive), std::move(strict)})
                 .first;
    }
    return &iter->second;
  }

  std::shared_ptr<Schema> table_schema_;
  std::unique_ptr<ResidualEvaluator> residual_evaluator_;
  bool case_sensitive_;
  std::unordered_map<PartitionValues, Evaluators, PartitionValuesHash,
                     PartitionValuesEqual>
      metrics_evaluators_;
};

Result<std::unique_ptr<ManifestFilterManager>> ManifestFilterManager::Make(
    ManifestContent content, std::shared_ptr<FileIO> file_io,
    std::function<Status(const std::string&)> delete_file,
    WorkerPoolSupplier worker_pool_supplier) {
  ICEBERG_PRECHECK(file_io != nullptr, "FileIO cannot be null");
  return std::unique_ptr<ManifestFilterManager>(
      new ManifestFilterManager(content, std::move(file_io), std::move(delete_file),
                                std::move(worker_pool_supplier)));
}

ManifestFilterManager::ManifestFilterManager(
    ManifestContent content, std::shared_ptr<FileIO> file_io,
    std::function<Status(const std::string&)> delete_file,
    WorkerPoolSupplier worker_pool_supplier)
    : manifest_content_(content),
      file_io_(std::move(file_io)),
      delete_file_(std::move(delete_file)),
      delete_expression_(Expressions::AlwaysFalse()),
      worker_pool_supplier_(std::move(worker_pool_supplier)) {
  if (!delete_file_) {
    delete_file_ = [this](const std::string& location) {
      return file_io_->DeleteFile(location);
    };
  }
}

ManifestFilterManager::~ManifestFilterManager() = default;

void ManifestFilterManager::FailAnyDelete() { fail_any_delete_ = true; }

void ManifestFilterManager::FailMissingDeletePaths() {
  fail_missing_delete_paths_ = true;
}

const DataFileSet& ManifestFilterManager::FilesToBeDeleted() const { return data_files_; }

const DeleteFileSet& ManifestFilterManager::DeleteFilesToBeDeleted() const {
  return delete_files_;
}

Status ManifestFilterManager::DeleteByRowFilter(std::shared_ptr<Expression> expr) {
  ICEBERG_PRECHECK(expr != nullptr, "Cannot delete files using filter: null");
  ICEBERG_RETURN_UNEXPECTED(InvalidateFilteredCache());
  ICEBERG_ASSIGN_OR_RAISE(delete_expression_,
                          Or::MakeFolded(delete_expression_, std::move(expr)));
  all_deletes_reference_manifests_ = false;
  return {};
}

Status ManifestFilterManager::DropPartition(int32_t spec_id, PartitionValues partition) {
  ICEBERG_RETURN_UNEXPECTED(InvalidateFilteredCache());
  drop_partitions_.add(spec_id, std::move(partition));
  all_deletes_reference_manifests_ = false;
  return {};
}

Status ManifestFilterManager::DropDeleteFilesOlderThan(int64_t sequence_number) {
  ICEBERG_PRECHECK(sequence_number >= 0, "Invalid minimum data sequence number: {}",
                   sequence_number);
  min_sequence_number_ = sequence_number;
  return {};
}

void ManifestFilterManager::CaseSensitive(bool new_case_sensitive) {
  case_sensitive_ = new_case_sensitive;
}

void ManifestFilterManager::RemoveDanglingDeletesFor(const DataFileSet& data_files) {
  removed_data_file_paths_.clear();
  for (const auto& file : data_files) {
    removed_data_file_paths_.insert(file->file_path);
  }
}

Status ManifestFilterManager::DeleteFile(std::shared_ptr<DataFile> file) {
  ICEBERG_PRECHECK(file != nullptr, "Cannot delete file: null");
  ICEBERG_RETURN_UNEXPECTED(InvalidateFilteredCache());
  if (!file->manifest_location.has_value()) {
    all_deletes_reference_manifests_ = false;
  } else {
    manifests_with_deletes_.insert(*file->manifest_location);
  }
  InsertFile(manifest_content_, data_files_, delete_files_, file);
  // Files built without inherited metadata use the initial spec, as Java builders do.
  delete_file_partitions_.add(file->partition_spec_id.value_or(0), file->partition);
  return {};
}

Status ManifestFilterManager::DeleteFile(std::string_view path) {
  ICEBERG_RETURN_UNEXPECTED(InvalidateFilteredCache());
  all_deletes_reference_manifests_ = false;
  delete_paths_.insert(std::string(path));
  return {};
}

bool ManifestFilterManager::ContainsDeletes() const {
  return !delete_paths_.empty() || !data_files_.empty() || !delete_files_.empty() ||
         delete_expression_->op() != Expression::Operation::kFalse ||
         !drop_partitions_.empty();
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
    CollectDeletedFiles({});
    ICEBERG_RETURN_UNEXPECTED(ValidateRequiredDeletes());
    return std::vector<ManifestFile>{};
  }
  ICEBERG_ASSIGN_OR_RAISE(
      auto reader, ManifestListReader::Make(base_snapshot->manifest_list, file_io_));
  ICEBERG_ASSIGN_OR_RAISE(auto all_manifests, reader->Files());
  std::vector<const ManifestFile*> manifests;
  for (const auto& manifest : all_manifests) {
    if (manifest.content == manifest_content_) manifests.push_back(&manifest);
  }
  TableMetadataCache metadata_cache(&metadata);
  ICEBERG_ASSIGN_OR_RAISE(auto specs_by_id, metadata_cache.GetPartitionSpecsById());
  return FilterManifests(schema, specs_by_id.get(), manifests, writer_factory);
}

Result<std::vector<ManifestFile>> ManifestFilterManager::FilterManifests(
    const std::shared_ptr<Schema>& table_schema, const PartitionSpecsById& specs_by_id,
    const std::vector<const ManifestFile*>& input_manifests,
    const ManifestWriterFactory& writer_factory) {
  std::vector<const ManifestFile*> manifests;
  for (const auto* manifest : input_manifests) {
    ICEBERG_PRECHECK(manifest != nullptr, "Cannot filter manifests: manifest is null");
    if (manifest->content == manifest_content_) manifests.push_back(manifest);
  }
  if (manifests.empty()) {
    CollectDeletedFiles({});
    ICEBERG_RETURN_UNEXPECTED(ValidateRequiredDeletes());
    return std::vector<ManifestFile>{};
  }
  ICEBERG_PRECHECK(table_schema != nullptr, "Cannot filter manifests: schema is null");
  bool trust_manifest_references = CanTrustManifestReferences(manifests);
  std::vector<ManifestFile> filtered(manifests.size());
  // Open all manifest files in parallel, using the index to avoid reordering.
  TaskGroup tasks;
  std::atomic<bool> failed{false};
  if (worker_pool_supplier_) tasks.SetExecutor(worker_pool_supplier_());
  for (size_t index = 0; index < manifests.size(); ++index) {
    tasks.Submit([&, index]() -> Status {
      if (failed.load()) return {};
      auto manifest = FilterManifest(table_schema, specs_by_id, *manifests[index],
                                     trust_manifest_references, writer_factory);
      if (!manifest) {
        failed.store(true);
        return ::iceberg::unexpected(std::move(manifest.error()));
      }
      filtered[index] = std::move(manifest.value());
      return {};
    });
  }
  ICEBERG_RETURN_UNEXPECTED(std::move(tasks).Run());
  CollectDeletedFiles(filtered);
  for (const auto& file : deleted_files_) {
    InsertFile(manifest_content_, data_files_, delete_files_, file);
  }
  ICEBERG_RETURN_UNEXPECTED(ValidateRequiredDeletes());
  return filtered;
}

bool ManifestFilterManager::CanTrustManifestReferences(
    const std::vector<const ManifestFile*>& manifests) const {
  std::unordered_set<std::string> manifest_locations;
  for (const auto* manifest : manifests)
    manifest_locations.insert(manifest->manifest_path);
  return all_deletes_reference_manifests_ && !manifests_with_deletes_.empty() &&
         std::ranges::all_of(manifests_with_deletes_, [&](const auto& location) {
           return manifest_locations.contains(location);
         });
}

Result<SnapshotSummaryBuilder> ManifestFilterManager::BuildSummary(
    const std::vector<ManifestFile>& manifests,
    const PartitionSpecsById& specs_by_id) const {
  SnapshotSummaryBuilder summary_builder;
  for (const auto& manifest : manifests) {
    auto result = filtered_manifest_results_.find(manifest);
    if (result != filtered_manifest_results_.end()) {
      ICEBERG_ASSIGN_OR_RAISE(auto manifest_spec,
                              PartitionSpecById(specs_by_id, manifest.partition_spec_id));
      for (const auto& file : result->second.files) {
        ICEBERG_RETURN_UNEXPECTED(summary_builder.DeletedFile(*manifest_spec, *file));
      }
      summary_builder.IncrementDuplicateDeletes(result->second.duplicate_delete_count);
    }
  }
  return summary_builder;
}

Status ManifestFilterManager::ValidateRequiredDeletes() const {
  if (fail_missing_delete_paths_) {
    std::string missing_files;
    auto append_missing = [&missing_files](const std::string& path) {
      if (!missing_files.empty()) missing_files += ",";
      missing_files += path;
    };
    for (const auto& file : data_files_) {
      if (!deleted_data_file_set_.contains(file)) append_missing(file->file_path);
    }
    for (const auto& file : delete_files_) {
      if (!deleted_delete_file_set_.contains(file)) append_missing(file->file_path);
    }
    if (!missing_files.empty()) {
      return ValidationFailed("Missing required files to delete: {}", missing_files);
    }
    std::unordered_set<std::string> deleted_file_paths;
    for (const auto& file : deleted_files_) deleted_file_paths.insert(file->file_path);
    for (const auto& path : delete_paths_) {
      if (!deleted_file_paths.contains(path)) append_missing(path);
    }
    if (!missing_files.empty()) {
      return ValidationFailed("Missing required files to delete: {}", missing_files);
    }
  }
  return {};
}

void ManifestFilterManager::CollectDeletedFiles(
    const std::vector<ManifestFile>& manifests) {
  deleted_files_.clear();
  deleted_data_file_set_.clear();
  deleted_delete_file_set_.clear();
  duplicate_deletes_count_ = 0;
  for (const auto& manifest : manifests) {
    auto result = filtered_manifest_results_.find(manifest);
    if (result != filtered_manifest_results_.end()) {
      for (const auto& file : result->second.files) {
        if (InsertFile(manifest_content_, deleted_data_file_set_,
                       deleted_delete_file_set_, file)) {
          deleted_files_.push_back(file);
        }
      }
      duplicate_deletes_count_ += result->second.duplicate_delete_count;
    }
  }
}

const std::vector<std::shared_ptr<DataFile>>& ManifestFilterManager::DeletedFiles()
    const {
  return deleted_files_;
}

Status ManifestFilterManager::CleanUncommitted(
    const std::unordered_set<std::string>& committed) {
  // Iterate over a copy of entries to avoid concurrent modification.
  auto filter_entries = std::vector<std::pair<ManifestFile, ManifestFile>>{
      filtered_manifests_.begin(), filtered_manifests_.end()};
  for (const auto& [manifest, filtered] : filter_entries) {
    if (!committed.contains(filtered.manifest_path)) {
      if (manifest != filtered) {
        std::ignore = delete_file_(filtered.manifest_path);
        --replaced_manifests_count_;
      }
      filtered_manifests_.erase(manifest);
      filtered_manifest_results_.erase(filtered);
    }
  }
  return {};
}

Status ManifestFilterManager::InvalidateFilteredCache() {
  ICEBERG_RETURN_UNEXPECTED(CleanUncommitted({}));
  replaced_manifests_count_ = 0;
  return {};
}

Result<ManifestFile> ManifestFilterManager::FilterManifest(
    const std::shared_ptr<Schema>& table_schema, const PartitionSpecsById& specs_by_id,
    const ManifestFile& manifest, bool trust_manifest_references,
    const ManifestWriterFactory& writer_factory) {
  {
    std::lock_guard lock(cache_mutex_);
    auto cached = filtered_manifests_.find(manifest);
    if (cached != filtered_manifests_.end()) return cached->second;
  }
  ICEBERG_ASSIGN_OR_RAISE(auto can_contain_deleted_files,
                          CanContainDeletedFiles(manifest, table_schema, specs_by_id,
                                                 trust_manifest_references));
  if (!can_contain_deleted_files) {
    std::lock_guard lock(cache_mutex_);
    filtered_manifests_[manifest] = manifest;
    return manifest;
  }
  ICEBERG_ASSIGN_OR_RAISE(auto spec,
                          PartitionSpecById(specs_by_id, manifest.partition_spec_id));
  ICEBERG_ASSIGN_OR_RAISE(auto reader,
                          ManifestReader::Make(manifest, file_io_, table_schema, spec));
  ICEBERG_ASSIGN_OR_RAISE(auto evaluator,
                          PartitionAndMetricsEvaluator::Make(
                              table_schema, *spec, delete_expression_, case_sensitive_));
  // The C++ reader materializes live entries; both passes reuse that vector.
  ICEBERG_ASSIGN_OR_RAISE(auto entries, reader->LiveEntries());
  ICEBERG_ASSIGN_OR_RAISE(auto has_deleted_files,
                          ManifestHasDeletedFiles(*evaluator, manifest, *spec, entries));
  if (has_deleted_files) {
    ICEBERG_ASSIGN_OR_RAISE(
        auto filtered,
        FilterManifestWithDeletedFiles(*evaluator, manifest, entries, writer_factory));
    ++replaced_manifests_count_;
    return filtered;
  }
  std::lock_guard lock(cache_mutex_);
  filtered_manifests_[manifest] = manifest;
  return manifest;
}

Result<bool> ManifestFilterManager::CanContainDeletedFiles(
    const ManifestFile& manifest, const std::shared_ptr<Schema>& table_schema,
    const PartitionSpecsById& specs_by_id, bool trust_manifest_references) const {
  if (HasNoLiveFiles(manifest)) return false;
  if (trust_manifest_references)
    return manifests_with_deletes_.contains(manifest.manifest_path);
  ICEBERG_ASSIGN_OR_RAISE(auto dropped_files,
                          CanContainDroppedFiles(manifest, *table_schema, specs_by_id));
  if (dropped_files) return true;
  ICEBERG_ASSIGN_OR_RAISE(
      auto expression_deletes,
      CanContainExpressionDeletes(manifest, table_schema, specs_by_id));
  if (expression_deletes) return true;
  return CanContainDroppedPartitions(manifest, *table_schema, specs_by_id);
}

bool ManifestFilterManager::HasNoLiveFiles(const ManifestFile& manifest) {
  return !manifest.has_added_files() && !manifest.has_existing_files();
}

Result<bool> ManifestFilterManager::CanContainExpressionDeletes(
    const ManifestFile& manifest, const std::shared_ptr<Schema>& table_schema,
    const PartitionSpecsById& specs_by_id) const {
  if (delete_expression_ && delete_expression_->op() != Expression::Operation::kFalse) {
    ICEBERG_ASSIGN_OR_RAISE(auto spec,
                            PartitionSpecById(specs_by_id, manifest.partition_spec_id));
    ICEBERG_ASSIGN_OR_RAISE(auto manifest_evaluator, ManifestEvaluator::MakeRowFilter(
                                                         delete_expression_, spec,
                                                         *table_schema, case_sensitive_));
    return manifest_evaluator->Evaluate(manifest);
  }
  return false;
}

Result<bool> ManifestFilterManager::CanContainDroppedPartitions(
    const ManifestFile& manifest, const Schema& table_schema,
    const PartitionSpecsById& specs_by_id) const {
  if (!drop_partitions_.empty())
    return CanContainAny(manifest, drop_partitions_, table_schema, specs_by_id);
  return false;
}

Result<bool> ManifestFilterManager::CanContainDroppedFiles(
    const ManifestFile& manifest, const Schema& table_schema,
    const PartitionSpecsById& specs_by_id) const {
  if (!delete_paths_.empty()) {
    return true;
  } else if (!data_files_.empty() || !delete_files_.empty()) {
    return CanContainAny(manifest, delete_file_partitions_, table_schema, specs_by_id);
  } else if (!removed_data_file_paths_.empty()) {
    return true;
  }
  return false;
}

Result<bool> ManifestFilterManager::ManifestHasDeletedFiles(
    PartitionAndMetricsEvaluator& evaluator, const ManifestFile& manifest,
    const PartitionSpec& spec, const std::vector<ManifestEntry>& entries) const {
  if (manifests_with_deletes_.contains(manifest.manifest_path)) return true;
  bool is_delete = manifest_content_ == ManifestContent::kDeletes;
  for (const auto& entry : entries) {
    const auto& file = *entry.data_file;
    bool marked_for_delete =
        delete_paths_.contains(file.file_path) ||
        (is_delete ? delete_files_.contains(file) : data_files_.contains(file)) ||
        drop_partitions_.contains(
            file.partition_spec_id.value_or(manifest.partition_spec_id),
            file.partition) ||
        (is_delete && entry.status != ManifestStatus::kDeleted &&
         entry.sequence_number.value_or(0) > 0 &&
         entry.sequence_number.value_or(0) < min_sequence_number_) ||
        (is_delete && IsDanglingDV(file));
    bool rows_might_match = marked_for_delete;
    if (!marked_for_delete) {
      ICEBERG_ASSIGN_OR_RAISE(rows_might_match, evaluator.RowsMightMatch(file));
    }
    if (rows_might_match) {
      bool all_rows_match = marked_for_delete;
      if (!marked_for_delete) {
        ICEBERG_ASSIGN_OR_RAISE(all_rows_match, evaluator.RowsMustMatch(file));
      }
      if (!all_rows_match && !is_delete) {
        return ValidationFailed(
            "Cannot delete file where some, but not all, rows match filter {}: {}",
            delete_expression_->ToString(), file.file_path);
      }
      if (all_rows_match) {
        if (fail_any_delete_) {
          ICEBERG_ASSIGN_OR_RAISE(auto partition_path,
                                  spec.PartitionPath(file.partition));
          return ValidationFailed("Operation would delete existing data: {}",
                                  partition_path);
        }
        return true;
      }
    }
  }
  return false;
}

bool ManifestFilterManager::IsDanglingDV(const DataFile& file) const {
  return file.IsDeletionVector() && file.referenced_data_file.has_value() &&
         removed_data_file_paths_.contains(*file.referenced_data_file);
}

Result<ManifestFile> ManifestFilterManager::FilterManifestWithDeletedFiles(
    PartitionAndMetricsEvaluator& evaluator, const ManifestFile& manifest,
    const std::vector<ManifestEntry>& entries,
    const ManifestWriterFactory& writer_factory) {
  bool is_delete = manifest_content_ == ManifestContent::kDeletes;
  DataFileSet deleted_data_files;
  DeleteFileSet deleted_delete_files;
  FilteredManifestResult result;
  ICEBERG_ASSIGN_OR_RAISE(auto writer,
                          writer_factory(manifest.partition_spec_id, manifest_content_));
  auto write_entries = [&]() -> Status {
    for (const auto& entry : entries) {
      const auto& file = *entry.data_file;
      bool is_dangling_dv = is_delete && IsDanglingDV(file);
      bool marked_for_delete =
          is_dangling_dv || delete_paths_.contains(file.file_path) ||
          (is_delete ? delete_files_.contains(file) : data_files_.contains(file)) ||
          drop_partitions_.contains(
              file.partition_spec_id.value_or(manifest.partition_spec_id),
              file.partition) ||
          (is_delete && entry.status != ManifestStatus::kDeleted &&
           entry.sequence_number.value_or(0) > 0 &&
           entry.sequence_number.value_or(0) < min_sequence_number_);
      bool rows_might_match = marked_for_delete;
      if (!marked_for_delete) {
        ICEBERG_ASSIGN_OR_RAISE(rows_might_match, evaluator.RowsMightMatch(file));
      }
      if (rows_might_match) {
        bool all_rows_match = marked_for_delete;
        if (!marked_for_delete) {
          ICEBERG_ASSIGN_OR_RAISE(all_rows_match, evaluator.RowsMustMatch(file));
        }
        if (!all_rows_match && !is_delete) {
          return ValidationFailed(
              "Cannot delete file where some, but not all, rows match filter {}: {}",
              delete_expression_->ToString(), file.file_path);
        }
        if (all_rows_match) {
          ICEBERG_RETURN_UNEXPECTED(writer->WriteDeletedEntry(entry));
          auto file_copy = file.CopyWithoutStats();
          if (!InsertFile(manifest_content_, deleted_data_files, deleted_delete_files,
                          file_copy)) {
            ICEBERG_LOG_WARN("Deleting a duplicate path from manifest {}: {}",
                             manifest.manifest_path, file.file_path);
            ++result.duplicate_delete_count;
          } else {
            // Only add a new delete to keep the snapshot summary accurate for duplicates.
            result.files.push_back(std::move(file_copy));
          }
        } else {
          ICEBERG_RETURN_UNEXPECTED(writer->WriteExistingEntry(entry));
        }
      } else {
        ICEBERG_RETURN_UNEXPECTED(writer->WriteExistingEntry(entry));
      }
    }
    return {};
  };
  auto write_status = write_entries();
  // Java closes in finally: a close failure takes precedence over a write failure.
  ICEBERG_RETURN_UNEXPECTED(writer->Close());
  ICEBERG_RETURN_UNEXPECTED(write_status);
  ICEBERG_ASSIGN_OR_RAISE(auto filtered, writer->ToManifestFile());
  std::lock_guard lock(cache_mutex_);
  filtered_manifests_[manifest] = filtered;
  filtered_manifest_results_[filtered] = std::move(result);
  return filtered;
}

}  // namespace iceberg
