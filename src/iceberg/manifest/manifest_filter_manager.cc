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

#include "iceberg/expression/inclusive_metrics_evaluator.h"
#include "iceberg/expression/manifest_evaluator.h"
#include "iceberg/expression/residual_evaluator.h"
#include "iceberg/expression/strict_metrics_evaluator.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/result.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
#include "iceberg/util/macros.h"

namespace iceberg {

ManifestFilterManager::ManifestFilterManager(ManifestContent content,
                                             std::shared_ptr<FileIO> file_io)
    : manifest_content_(content), file_io_(std::move(file_io)) {}

void ManifestFilterManager::DeleteByRowFilter(std::shared_ptr<Expression> expr) {
  ICEBERG_CHECK_OR_DIE(expr != nullptr, "Cannot delete files using filter: null");
  delete_exprs_.push_back({.expr = std::move(expr)});
}

void ManifestFilterManager::CaseSensitive(bool case_sensitive) {
  case_sensitive_ = case_sensitive;
  manifest_evaluator_cache_.clear();
  residual_evaluator_cache_.clear();
}

void ManifestFilterManager::DeleteFile(std::string_view path) {
  delete_paths_.insert(std::string(path));
}

void ManifestFilterManager::DeleteFile(std::shared_ptr<DataFile> file) {
  ICEBERG_CHECK_OR_DIE(file != nullptr, "Cannot delete file: null");
  delete_paths_.insert(file->file_path);
  delete_files_.insert(std::move(file));
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
  return !delete_exprs_.empty() || !delete_paths_.empty() || !drop_partitions_.empty();
}

bool ManifestFilterManager::CanContainDroppedFiles() const {
  return !delete_paths_.empty();
}

bool ManifestFilterManager::CanContainDroppedPartitions(const ManifestFile& manifest) {
  if (drop_partitions_.empty()) return false;
  // Only manifests whose partition spec matches a registered drop can contain
  // entries for that partition.  PartitionKey is pair<spec_id, values>.
  int32_t spec_id = manifest.partition_spec_id;
  for (const auto& key : drop_partitions_) {
    if (key.first == spec_id) return true;
  }
  return false;
}

bool ManifestFilterManager::CanContainExpressionDeletes(const ManifestFile& manifest,
                                                        const TableMetadata& metadata) {
  if (delete_exprs_.empty()) return false;
  int32_t spec_id = manifest.partition_spec_id;
  for (const auto& delete_expr : delete_exprs_) {
    auto* evaluator_ptr =
        GetManifestEvaluator(metadata, spec_id, delete_expr).value_or(nullptr);
    if (evaluator_ptr == nullptr) return true;  // conservative on error
    auto result = evaluator_ptr->Evaluate(manifest);
    if (!result.has_value() || result.value()) return true;
  }
  return false;
}

bool ManifestFilterManager::CanContainDeletedFiles(const ManifestFile& manifest,
                                                   const TableMetadata& metadata) {
  // A manifest with no live files cannot contain files to delete.
  // Null counts mean the count is unknown — treat conservatively as possibly non-zero
  // (matches Java's ManifestFile.hasAddedFiles / hasExistingFiles behaviour).
  bool has_live = !manifest.added_files_count.has_value() ||
                  manifest.added_files_count.value() > 0 ||
                  !manifest.existing_files_count.has_value() ||
                  manifest.existing_files_count.value() > 0;
  if (!has_live) return false;

  return CanContainDroppedFiles() || CanContainExpressionDeletes(manifest, metadata) ||
         CanContainDroppedPartitions(manifest);
}

Result<ManifestEvaluator*> ManifestFilterManager::GetManifestEvaluator(
    const TableMetadata& metadata, int32_t spec_id, const DeleteExpr& de) {
  auto& vec = manifest_evaluator_cache_[spec_id];
  size_t idx = &de - delete_exprs_.data();
  if (idx >= vec.size()) {
    vec.resize(delete_exprs_.size());
  }
  if (!vec[idx]) {
    ICEBERG_ASSIGN_OR_RAISE(auto spec, metadata.PartitionSpecById(spec_id));
    ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());
    ICEBERG_ASSIGN_OR_RAISE(vec[idx], ManifestEvaluator::MakeRowFilter(
                                          de.expr, spec, *schema, case_sensitive_));
  }
  return vec[idx].get();
}

Result<ResidualEvaluator*> ManifestFilterManager::GetResidualEvaluator(
    const TableMetadata& metadata, int32_t spec_id, const DeleteExpr& de) {
  auto& vec = residual_evaluator_cache_[spec_id];
  size_t idx = &de - delete_exprs_.data();
  if (idx >= vec.size()) {
    vec.resize(delete_exprs_.size());
  }
  if (!vec[idx]) {
    ICEBERG_ASSIGN_OR_RAISE(auto spec, metadata.PartitionSpecById(spec_id));
    ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());
    ICEBERG_ASSIGN_OR_RAISE(
        vec[idx], ResidualEvaluator::Make(de.expr, *spec, *schema, case_sensitive_));
  }
  return vec[idx].get();
}

Result<bool> ManifestFilterManager::ShouldDelete(const ManifestEntry& entry,
                                                 const TableMetadata& metadata,
                                                 int32_t manifest_spec_id) {
  if (!entry.data_file) return false;
  const DataFile& file = *entry.data_file;
  int32_t spec_id = file.partition_spec_id.value_or(manifest_spec_id);
  std::shared_ptr<Schema> schema;
  if (!delete_exprs_.empty()) {
    ICEBERG_ASSIGN_OR_RAISE(schema, metadata.Schema());
  }

  // Path-based and partition-drop checks
  if (delete_paths_.count(file.file_path) ||
      drop_partitions_.contains(spec_id, file.partition)) {
    if (fail_any_delete_) {
      return InvalidArgument("Operation would delete existing data: {}", file.file_path);
    }
    return true;
  }

  // Expression-based check.
  // Java semantics: compute a partition residual first, then use strict/inclusive
  // metrics on that residual. Data manifests reject partial matches; delete manifests
  // tolerate them because only data-file deletes require all-rows-match validation.
  for (const auto& de : delete_exprs_) {
    ICEBERG_ASSIGN_OR_RAISE(auto* residual_eval,
                            GetResidualEvaluator(metadata, spec_id, de));
    ICEBERG_ASSIGN_OR_RAISE(auto residual_expr,
                            residual_eval->ResidualFor(file.partition));
    ICEBERG_ASSIGN_OR_RAISE(
        auto strict_eval,
        StrictMetricsEvaluator::Make(residual_expr, schema, case_sensitive_));
    ICEBERG_ASSIGN_OR_RAISE(auto strict_match, strict_eval->Evaluate(file));
    if (strict_match) {
      if (fail_any_delete_) {
        return InvalidArgument("Operation would delete existing data: {}",
                               file.file_path);
      }
      return true;
    }

    ICEBERG_ASSIGN_OR_RAISE(auto incl_eval, InclusiveMetricsEvaluator::Make(
                                                residual_expr, *schema, case_sensitive_));
    ICEBERG_ASSIGN_OR_RAISE(auto incl_match, incl_eval->Evaluate(file));
    if (incl_match) {
      if (manifest_content_ == ManifestContent::kDeletes) {
        continue;
      }
      return InvalidArgument(
          "Cannot delete file where some, but not all, rows match filter: {}",
          file.file_path);
    }
  }

  return false;
}

Result<std::vector<ManifestFile>> ManifestFilterManager::FilterManifests(
    const TableMetadata& metadata, const std::shared_ptr<Snapshot>& base_snapshot,
    const ManifestWriterFactory& writer_factory) {
  // Validate required delete paths before any early return — even an empty base
  // snapshot must report missing required paths (matches Java's validateRequiredDeletes).
  if (fail_missing_delete_paths_ && !delete_paths_.empty() && !base_snapshot) {
    return InvalidArgument("Missing delete paths: {}", [&] {
      std::string s;
      for (const auto& p : delete_paths_) {
        if (!s.empty()) s += ", ";
        s += p;
      }
      return s;
    }());
  }

  // No base snapshot → nothing to filter
  if (!base_snapshot) return std::vector<ManifestFile>{};

  // Load the relevant manifests from the manifest list
  ICEBERG_ASSIGN_OR_RAISE(
      auto list_reader, ManifestListReader::Make(base_snapshot->manifest_list, file_io_));
  ICEBERG_ASSIGN_OR_RAISE(auto all_manifests, list_reader->Files());

  // Keep only manifests for this manager's content type
  std::vector<ManifestFile> manifests;
  manifests.reserve(all_manifests.size());
  for (const auto& m : all_manifests) {
    if (m.content == manifest_content_) manifests.push_back(m);
  }

  // No conditions registered → return unchanged
  if (!ContainsDeletes()) return manifests;

  ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());

  std::vector<ManifestFile> result;
  result.reserve(manifests.size());
  // Track which registered delete paths were actually found across all manifests.
  // Using delete_paths_ as the immutable source of truth makes FilterManifests
  // idempotent across commit retries (matches Java's validateRequiredDeletes design).
  std::unordered_set<std::string> found_paths;

  for (const auto& manifest : manifests) {
    // Fast path: metadata skip
    if (!CanContainDeletedFiles(manifest, metadata)) {
      result.push_back(manifest);
      continue;
    }

    int32_t spec_id = manifest.partition_spec_id;
    ICEBERG_ASSIGN_OR_RAISE(auto spec, metadata.PartitionSpecById(spec_id));

    // Read all live entries from the manifest
    ICEBERG_ASSIGN_OR_RAISE(auto reader,
                            ManifestReader::Make(manifest, file_io_, schema, spec));
    ICEBERG_ASSIGN_OR_RAISE(auto entries, reader->LiveEntries());

    // Check whether any entry should be deleted
    bool has_deletes = false;
    for (const auto& entry : entries) {
      ICEBERG_ASSIGN_OR_RAISE(auto should_delete, ShouldDelete(entry, metadata, spec_id));
      if (should_delete) {
        has_deletes = true;
        break;
      }
    }

    if (!has_deletes) {
      result.push_back(manifest);
      continue;
    }

    // Rewrite the manifest with deleted entries marked; record found paths.
    ICEBERG_ASSIGN_OR_RAISE(auto writer, writer_factory(spec_id, manifest_content_));
    for (const auto& entry : entries) {
      ICEBERG_ASSIGN_OR_RAISE(auto should_delete, ShouldDelete(entry, metadata, spec_id));
      if (should_delete) {
        if (entry.data_file && delete_paths_.count(entry.data_file->file_path)) {
          found_paths.insert(entry.data_file->file_path);
        }
        if (entry.data_file) {
          delete_files_.insert(std::make_shared<DataFile>(*entry.data_file));
        }
        ICEBERG_RETURN_UNEXPECTED(writer->WriteDeletedEntry(entry));
      } else {
        ICEBERG_RETURN_UNEXPECTED(writer->WriteExistingEntry(entry));
      }
    }
    ICEBERG_RETURN_UNEXPECTED(writer->Close());
    ICEBERG_ASSIGN_OR_RAISE(auto filtered_manifest, writer->ToManifestFile());
    result.push_back(std::move(filtered_manifest));
  }

  // Validate that all registered delete paths were found.  Uses delete_paths_ (not a
  // consumed set) so repeated calls on the same manager produce the same result.
  if (fail_missing_delete_paths_) {
    std::string missing;
    for (const auto& p : delete_paths_) {
      if (!found_paths.count(p)) {
        if (!missing.empty()) missing += ", ";
        missing += p;
      }
    }
    if (!missing.empty()) {
      return InvalidArgument("Missing delete paths: {}", missing);
    }
  }

  return result;
}

}  // namespace iceberg
