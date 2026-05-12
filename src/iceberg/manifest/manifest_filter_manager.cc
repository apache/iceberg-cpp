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

void ManifestFilterManager::DeleteByRowFilter(std::shared_ptr<Expression> expr,
                                              bool case_sensitive) {
  delete_exprs_.push_back({std::move(expr), case_sensitive});
}

void ManifestFilterManager::DeleteFile(std::string_view path) {
  std::string p(path);
  delete_paths_.insert(p);
  pending_paths_.insert(std::move(p));
}

void ManifestFilterManager::DropPartition(int32_t spec_id, PartitionValues partition) {
  drop_partitions_.add(spec_id, std::move(partition));
}

void ManifestFilterManager::FailMissingDeletePaths() {
  fail_missing_delete_paths_ = true;
}

bool ManifestFilterManager::DeletesFiles() const {
  return !delete_exprs_.empty() || !delete_paths_.empty() || !drop_partitions_.empty();
}

bool ManifestFilterManager::CanContainDroppedFiles() const {
  return !delete_paths_.empty();
}

bool ManifestFilterManager::CanContainDroppedPartitions(const ManifestFile& manifest) {
  if (drop_partitions_.empty()) return false;
  // Without a partition filter helper, we conservatively say yes.
  // A full implementation would use ManifestFileUtil::canContainAny; for now
  // we open the manifest and let per-entry checks decide.
  (void)manifest;
  return true;
}

bool ManifestFilterManager::CanContainExpressionDeletes(const ManifestFile& manifest,
                                                        const TableMetadata& metadata) {
  if (delete_exprs_.empty()) return false;
  int32_t spec_id = manifest.partition_spec_id;
  for (size_t i = 0; i < delete_exprs_.size(); ++i) {
    auto* evaluator_ptr = GetManifestEvaluator(metadata, spec_id, delete_exprs_[i])
                              .value_or(nullptr);
    if (evaluator_ptr == nullptr) return true;  // conservative on error
    auto result = evaluator_ptr->Evaluate(manifest);
    if (!result.has_value() || result.value()) return true;
  }
  return false;
}

bool ManifestFilterManager::CanContainDeletedFiles(const ManifestFile& manifest,
                                                   const TableMetadata& metadata) {
  // A manifest with no live files cannot contain files to delete.
  bool has_live = (manifest.added_files_count.value_or(0) > 0) ||
                  (manifest.existing_files_count.value_or(0) > 0);
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
                                          de.expr, spec, *schema, de.case_sensitive));
  }
  return vec[idx].get();
}

Result<InclusiveMetricsEvaluator*> ManifestFilterManager::GetMetricsEvaluator(
    const TableMetadata& metadata, int32_t spec_id, const DeleteExpr& de) {
  auto& vec = metrics_evaluator_cache_[spec_id];
  size_t idx = &de - delete_exprs_.data();
  if (idx >= vec.size()) {
    vec.resize(delete_exprs_.size());
  }
  if (!vec[idx]) {
    ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());
    ICEBERG_ASSIGN_OR_RAISE(vec[idx],
                            InclusiveMetricsEvaluator::Make(de.expr, *schema,
                                                            de.case_sensitive));
  }
  return vec[idx].get();
}

bool ManifestFilterManager::ShouldDelete(const ManifestEntry& entry,
                                         const TableMetadata& metadata,
                                         int32_t manifest_spec_id) {
  if (!entry.data_file) return false;
  const DataFile& file = *entry.data_file;

  // Path-based check
  if (delete_paths_.count(file.file_path)) {
    pending_paths_.erase(file.file_path);
    return true;
  }

  // Partition-drop check
  int32_t spec_id = file.partition_spec_id.value_or(manifest_spec_id);
  if (drop_partitions_.contains(spec_id, file.partition)) {
    return true;
  }

  // Expression-based check (inclusive metrics)
  for (const auto& de : delete_exprs_) {
    auto* eval = GetMetricsEvaluator(metadata, spec_id, de).value_or(nullptr);
    if (eval == nullptr) return true;  // conservative on error
    auto result = eval->Evaluate(file);
    if (!result.has_value() || result.value()) return true;
  }

  return false;
}

Result<std::vector<ManifestFile>> ManifestFilterManager::FilterManifests(
    const TableMetadata& metadata, const std::shared_ptr<Snapshot>& base_snapshot,
    const ManifestWriterFactory& writer_factory) {
  // No base snapshot → nothing to filter
  if (!base_snapshot) return std::vector<ManifestFile>{};

  // Load the relevant manifests from the manifest list
  ICEBERG_ASSIGN_OR_RAISE(auto list_reader,
                           ManifestListReader::Make(base_snapshot->manifest_list, file_io_));
  ICEBERG_ASSIGN_OR_RAISE(auto all_manifests, list_reader->Files());

  // Keep only manifests for this manager's content type
  std::vector<ManifestFile> manifests;
  manifests.reserve(all_manifests.size());
  for (const auto& m : all_manifests) {
    if (m.content == manifest_content_) manifests.push_back(m);
  }

  // No conditions registered → return unchanged
  if (!DeletesFiles()) return manifests;

  ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());

  std::vector<ManifestFile> result;
  result.reserve(manifests.size());

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
      if (ShouldDelete(entry, metadata, spec_id)) {
        has_deletes = true;
        break;
      }
    }

    if (!has_deletes) {
      result.push_back(manifest);
      continue;
    }

    // Rewrite the manifest with deleted entries marked
    ICEBERG_ASSIGN_OR_RAISE(auto writer, writer_factory(spec_id, manifest_content_));
    for (const auto& entry : entries) {
      if (ShouldDelete(entry, metadata, spec_id)) {
        ICEBERG_RETURN_UNEXPECTED(writer->WriteDeletedEntry(entry));
      } else {
        ICEBERG_RETURN_UNEXPECTED(writer->WriteExistingEntry(entry));
      }
    }
    ICEBERG_RETURN_UNEXPECTED(writer->Close());
    ICEBERG_ASSIGN_OR_RAISE(auto filtered_manifest, writer->ToManifestFile());
    result.push_back(std::move(filtered_manifest));
  }

  // Validate that all registered delete paths were found
  if (fail_missing_delete_paths_ && !pending_paths_.empty()) {
    std::string missing;
    for (const auto& p : pending_paths_) {
      if (!missing.empty()) missing += ", ";
      missing += p;
    }
    return InvalidArgument("Missing delete paths: {}", missing);
  }

  return result;
}

}  // namespace iceberg
