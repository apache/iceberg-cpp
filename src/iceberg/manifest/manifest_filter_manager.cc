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

#include <string>
#include <unordered_set>
#include <vector>

#include "iceberg/expression/expression.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/expression/inclusive_metrics_evaluator.h"
#include "iceberg/expression/manifest_evaluator.h"
#include "iceberg/expression/residual_evaluator.h"
#include "iceberg/expression/strict_metrics_evaluator.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/partition_spec.h"
#include "iceberg/result.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
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

Result<std::string> FormatPartitionPath(const PartitionSpecsById& specs_by_id,
                                        const DataFile& file, int32_t spec_id) {
  ICEBERG_ASSIGN_OR_RAISE(auto spec, PartitionSpecById(specs_by_id, spec_id));
  return spec->PartitionPath(file.partition);
}

}  // namespace

ManifestFilterManager::ManifestFilterManager(ManifestContent content,
                                             std::shared_ptr<FileIO> file_io)
    : manifest_content_(content),
      file_io_(std::move(file_io)),
      delete_expr_(Expressions::AlwaysFalse()) {}

ManifestFilterManager::~ManifestFilterManager() = default;

Status ManifestFilterManager::DeleteByRowFilter(std::shared_ptr<Expression> expr) {
  ICEBERG_PRECHECK(expr != nullptr, "Cannot delete files using filter: null");
  ICEBERG_ASSIGN_OR_RAISE(delete_expr_, Or::MakeFolded(delete_expr_, std::move(expr)));
  manifest_evaluator_cache_.clear();
  residual_evaluator_cache_.clear();
  return {};
}

void ManifestFilterManager::CaseSensitive(bool case_sensitive) {
  case_sensitive_ = case_sensitive;
  manifest_evaluator_cache_.clear();
  residual_evaluator_cache_.clear();
}

void ManifestFilterManager::DeleteFile(std::string_view path) {
  delete_paths_.insert(std::string(path));
}

Status ManifestFilterManager::DeleteFile(std::shared_ptr<DataFile> file) {
  ICEBERG_PRECHECK(file != nullptr, "Cannot delete file: null");
  delete_paths_.insert(file->file_path);
  delete_files_.insert(std::move(file));
  return {};
}

const DataFileSet& ManifestFilterManager::FilesToBeDeleted() const {
  return delete_files_;
}

void ManifestFilterManager::DropPartition(int32_t spec_id, PartitionValues partition) {
  drop_partitions_.add(spec_id, std::move(partition));
}

void ManifestFilterManager::FailMissingDeletePaths() {
  fail_missing_delete_paths_ = true;
}

void ManifestFilterManager::FailAnyDelete() { fail_any_delete_ = true; }

bool ManifestFilterManager::ContainsDeletes() const {
  return HasRowFilterExpression(delete_expr_) || !delete_paths_.empty() ||
         !drop_partitions_.empty();
}

void ManifestFilterManager::DropDeleteFilesOlderThan(int64_t sequence_number) {
  min_sequence_number_ = sequence_number;
}

void ManifestFilterManager::RemoveDanglingDeletesFor(const DataFileSet& deleted_files) {
  for (const auto& file : deleted_files) {
    removed_data_file_paths_.insert(file->file_path);
  }
}

Result<bool> ManifestFilterManager::CanContainDroppedFiles(const ManifestFile&) const {
  // TODO(Guotao): Use the manifest descriptor to skip unrelated object-delete
  // manifests once object-delete partitions are tracked separately.
  // Currently, DeleteFile(std::shared_ptr<DataFile>) degrades to a path-based delete,
  // which forces scanning all manifests.
  // Also open delete manifests when a minimum sequence number is set for cleanup.
  return !delete_paths_.empty() || !removed_data_file_paths_.empty() ||
         (manifest_content_ == ManifestContent::kDeletes && min_sequence_number_ > 0);
}

Result<bool> ManifestFilterManager::CanContainDroppedPartitions(
    const ManifestFile& manifest) const {
  if (drop_partitions_.empty()) return false;
  // TODO(Guotao): Use partition_summaries bounds to skip manifests that cannot
  // contain any dropped partition, instead of only matching partition spec IDs.
  // Only manifests whose partition spec matches a registered drop can contain
  // entries for that partition.  PartitionKey is pair<spec_id, values>.
  int32_t spec_id = manifest.partition_spec_id;
  for (const auto& key : drop_partitions_) {
    if (key.first == spec_id) return true;
  }
  return false;
}

Result<bool> ManifestFilterManager::CanContainExpressionDeletes(
    const ManifestFile& manifest, const std::shared_ptr<Schema>& schema,
    const PartitionSpecsById& specs_by_id) {
  if (!HasRowFilterExpression(delete_expr_)) return false;
  int32_t spec_id = manifest.partition_spec_id;
  ICEBERG_ASSIGN_OR_RAISE(auto* evaluator,
                          GetManifestEvaluator(schema, specs_by_id, spec_id));
  return evaluator->Evaluate(manifest);
}

Result<bool> ManifestFilterManager::CanContainDeletedFiles(
    const ManifestFile& manifest, const std::shared_ptr<Schema>& schema,
    const PartitionSpecsById& specs_by_id, bool trust_manifest_references) {
  // A manifest with no live files cannot contain files to delete.
  // Missing counts mean the count is unknown; treat it as possibly non-zero.
  bool has_live = !manifest.added_files_count.has_value() ||
                  manifest.added_files_count.value() > 0 ||
                  !manifest.existing_files_count.has_value() ||
                  manifest.existing_files_count.value() > 0;
  if (!has_live) return false;

  if (trust_manifest_references) {
    // TODO(Guotao): Return whether this manifest is in the referenced manifest set.
    return true;
  }

  ICEBERG_ASSIGN_OR_RAISE(auto can_contain_dropped_files,
                          CanContainDroppedFiles(manifest));
  if (can_contain_dropped_files) return true;

  ICEBERG_ASSIGN_OR_RAISE(auto can_contain_expression_deletes,
                          CanContainExpressionDeletes(manifest, schema, specs_by_id));
  if (can_contain_expression_deletes) return true;

  return CanContainDroppedPartitions(manifest);
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

  // Path-based and partition-drop checks
  if (delete_paths_.count(file.file_path) ||
      drop_partitions_.contains(spec_id, file.partition)) {
    if (fail_any_delete_) {
      ICEBERG_ASSIGN_OR_RAISE(auto partition_path,
                              FormatPartitionPath(specs_by_id, file, spec_id));
      return InvalidArgument("Operation would delete existing data: {}", partition_path);
    }
    return true;
  }

  // Delete-manifest-specific cleanup (only for ManifestContent::kDeletes).
  if (manifest_content_ == ManifestContent::kDeletes) {
    // Drop delete files whose data sequence number is older than the minimum
    // retained by the table (they can no longer match any live data rows).
    // seq == 0 (kInitialSequenceNumber / nullopt) is intentionally excluded:
    // those entries predate sequence number assignment and must not be pruned.
    int64_t seq = entry.sequence_number.value_or(0);
    if (min_sequence_number_ > 0 && seq > 0 && seq < min_sequence_number_) {
      return true;
    }

    // Drop DVs that reference a data file that has been removed (dangling DV).
    if (!removed_data_file_paths_.empty() && file.IsDeletionVector() &&
        file.referenced_data_file.has_value() &&
        removed_data_file_paths_.count(*file.referenced_data_file)) {
      return true;
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
      if (fail_any_delete_) {
        ICEBERG_ASSIGN_OR_RAISE(auto partition_path,
                                FormatPartitionPath(specs_by_id, file, spec_id));
        return InvalidArgument("Operation would delete existing data: {}",
                               partition_path);
      }
      return true;
    }

    ICEBERG_ASSIGN_OR_RAISE(auto incl_eval, InclusiveMetricsEvaluator::Make(
                                                residual_expr, *schema, case_sensitive_));
    ICEBERG_ASSIGN_OR_RAISE(auto incl_match, incl_eval->Evaluate(file));
    if (incl_match) {
      if (manifest_content_ == ManifestContent::kDeletes) {
        return false;
      }
      return InvalidArgument(
          "Cannot delete file where some, but not all, rows match filter: {}",
          file.file_path);
    }
  }

  return false;
}

bool ManifestFilterManager::CanTrustManifestReferences(
    const std::vector<const ManifestFile*>&) const {
  // TODO(Guotao): Track source manifest locations for object deletes so manifests
  // outside the referenced set can be skipped before any other delete checks.
  return false;
}

Result<ManifestFile> ManifestFilterManager::FilterManifest(
    const std::shared_ptr<Schema>& schema, const PartitionSpecsById& specs_by_id,
    const ManifestFile& manifest, bool trust_manifest_references,
    const ManifestWriterFactory& writer_factory,
    std::unordered_set<std::string>& found_paths) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto can_contain_deleted_files,
      CanContainDeletedFiles(manifest, schema, specs_by_id, trust_manifest_references));
  if (!can_contain_deleted_files) {
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
    return manifest;
  }

  return FilterManifestWithDeletedFiles(entries, spec_id, schema, specs_by_id,
                                        writer_factory, found_paths);
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
    const ManifestWriterFactory& writer_factory,
    std::unordered_set<std::string>& found_paths) {
  ICEBERG_ASSIGN_OR_RAISE(auto writer,
                          writer_factory(manifest_spec_id, manifest_content_));
  for (const auto& entry : entries) {
    ICEBERG_ASSIGN_OR_RAISE(auto should_delete,
                            ShouldDelete(entry, schema, specs_by_id, manifest_spec_id));
    if (should_delete) {
      if (entry.data_file && delete_paths_.count(entry.data_file->file_path)) {
        found_paths.insert(entry.data_file->file_path);
      }
      if (entry.data_file) {
        // TODO(Guotao): Track duplicate deletes and avoid full DataFile copies when
        // summary generation can use lighter records.
        delete_files_.insert(std::make_shared<DataFile>(*entry.data_file));
      }
      ICEBERG_RETURN_UNEXPECTED(writer->WriteDeletedEntry(entry));
    } else {
      ICEBERG_RETURN_UNEXPECTED(writer->WriteExistingEntry(entry));
    }
  }

  ICEBERG_RETURN_UNEXPECTED(writer->Close());
  return writer->ToManifestFile();
}

Status ManifestFilterManager::ValidateRequiredDeletes(
    const std::unordered_set<std::string>& found_paths) const {
  if (!fail_missing_delete_paths_) {
    return {};
  }

  std::string missing;
  for (const auto& path : delete_paths_) {
    if (!found_paths.count(path)) {
      if (!missing.empty()) missing += ", ";
      missing += path;
    }
  }
  if (!missing.empty()) {
    return InvalidArgument("Missing delete paths: {}", missing);
  }
  return {};
}

Result<std::vector<ManifestFile>> ManifestFilterManager::FilterManifests(
    const TableMetadata& metadata, const std::shared_ptr<Snapshot>& base_snapshot,
    const ManifestWriterFactory& writer_factory) {
  if (!base_snapshot) {
    ICEBERG_RETURN_UNEXPECTED(ValidateRequiredDeletes({}));
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

  ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());
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

  std::unordered_set<std::string> found_paths;
  if (manifests.empty()) {
    ICEBERG_RETURN_UNEXPECTED(ValidateRequiredDeletes(found_paths));
    return std::vector<ManifestFile>{};
  }

  bool trust_manifest_references = CanTrustManifestReferences(manifests);
  manifest_evaluator_cache_.clear();
  residual_evaluator_cache_.clear();
  replaced_manifests_count_ = 0;

  // TODO(Guotao): Parallelize manifest filtering with per-manifest results, then
  // merge found paths and deleted files after the loop.
  std::vector<ManifestFile> filtered;
  filtered.reserve(manifests.size());
  for (const auto* manifest_ptr : manifests) {
    ICEBERG_ASSIGN_OR_RAISE(
        auto filtered_manifest,
        FilterManifest(schema, specs_by_id, *manifest_ptr, trust_manifest_references,
                       writer_factory, found_paths));
    if (filtered_manifest.manifest_path != manifest_ptr->manifest_path) {
      ++replaced_manifests_count_;
    }
    filtered.push_back(std::move(filtered_manifest));
  }

  ICEBERG_RETURN_UNEXPECTED(ValidateRequiredDeletes(found_paths));
  return filtered;
}

}  // namespace iceberg
