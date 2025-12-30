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

#include "iceberg/manifest/manifest_group.h"

#include <utility>

#include "iceberg/expression/evaluator.h"
#include "iceberg/expression/expression.h"
#include "iceberg/expression/manifest_evaluator.h"
#include "iceberg/expression/projections.h"
#include "iceberg/expression/residual_evaluator.h"
#include "iceberg/file_io.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/table_scan.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/content_file_util.h"
#include "iceberg/util/macros.h"

namespace iceberg {

Result<std::unique_ptr<ManifestGroup>> ManifestGroup::Make(
    std::shared_ptr<FileIO> io, std::shared_ptr<Schema> schema,
    std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> specs_by_id,
    std::vector<ManifestFile> manifests) {
  std::vector<ManifestFile> data_manifests;
  std::vector<ManifestFile> delete_manifests;
  for (auto& manifest : manifests) {
    if (manifest.content == ManifestContent::kData) {
      data_manifests.push_back(std::move(manifest));
    } else if (manifest.content == ManifestContent::kDeletes) {
      delete_manifests.push_back(std::move(manifest));
    }
  }

  return ManifestGroup::Make(std::move(io), std::move(schema), std::move(specs_by_id),
                             std::move(data_manifests), std::move(delete_manifests));
}

Result<std::unique_ptr<ManifestGroup>> ManifestGroup::Make(
    std::shared_ptr<FileIO> io, std::shared_ptr<Schema> schema,
    std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> specs_by_id,
    std::vector<ManifestFile> data_manifests,
    std::vector<ManifestFile> delete_manifests) {
  // DeleteFileIndex::Builder validates all input parameters so we skip validation here
  ICEBERG_ASSIGN_OR_RAISE(
      auto delete_index_builder,
      DeleteFileIndex::BuilderFor(io, schema, specs_by_id, std::move(delete_manifests)));
  return std::unique_ptr<ManifestGroup>(
      new ManifestGroup(std::move(io), std::move(schema), std::move(specs_by_id),
                        std::move(data_manifests), std::move(delete_index_builder)));
}

ManifestGroup::ManifestGroup(
    std::shared_ptr<FileIO> io, std::shared_ptr<Schema> schema,
    std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> specs_by_id,
    std::vector<ManifestFile> data_manifests,
    DeleteFileIndex::Builder&& delete_index_builder)
    : io_(std::move(io)),
      schema_(std::move(schema)),
      specs_by_id_(std::move(specs_by_id)),
      data_manifests_(std::move(data_manifests)),
      delete_index_builder_(std::move(delete_index_builder)),
      data_filter_(True::Instance()),
      file_filter_(True::Instance()),
      partition_filter_(True::Instance()),
      manifest_entry_predicate_([](const ManifestEntry&) { return true; }) {}

ManifestGroup::~ManifestGroup() = default;

ManifestGroup::ManifestGroup(ManifestGroup&&) noexcept = default;
ManifestGroup& ManifestGroup::operator=(ManifestGroup&&) noexcept = default;

ManifestGroup& ManifestGroup::FilterData(std::shared_ptr<Expression> filter) {
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(data_filter_, And::Make(data_filter_, filter));
  delete_index_builder_.DataFilter(std::move(filter));
  return *this;
}

ManifestGroup& ManifestGroup::FilterFiles(std::shared_ptr<Expression> filter) {
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(file_filter_,
                                   And::Make(file_filter_, std::move(filter)));
  return *this;
}

ManifestGroup& ManifestGroup::FilterPartitions(std::shared_ptr<Expression> filter) {
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(partition_filter_,
                                   And::Make(partition_filter_, filter));
  delete_index_builder_.PartitionFilter(std::move(filter));
  return *this;
}

ManifestGroup& ManifestGroup::FilterManifestEntries(
    std::function<bool(const ManifestEntry&)> predicate) {
  manifest_entry_predicate_ = [old_predicate = std::move(manifest_entry_predicate_),
                               predicate =
                                   std::move(predicate)](const ManifestEntry& entry) {
    return old_predicate(entry) && predicate(entry);
  };
  return *this;
}

ManifestGroup& ManifestGroup::IgnoreDeleted() {
  ignore_deleted_ = true;
  return *this;
}

ManifestGroup& ManifestGroup::IgnoreExisting() {
  ignore_existing_ = true;
  return *this;
}

ManifestGroup& ManifestGroup::IgnoreResiduals() {
  ignore_residuals_ = true;
  delete_index_builder_.IgnoreResiduals();
  return *this;
}

ManifestGroup& ManifestGroup::Select(std::vector<std::string> columns) {
  columns_ = std::move(columns);
  return *this;
}

ManifestGroup& ManifestGroup::CaseSensitive(bool case_sensitive) {
  case_sensitive_ = case_sensitive;
  delete_index_builder_.CaseSensitive(case_sensitive);
  return *this;
}

ManifestGroup& ManifestGroup::ColumnsToKeepStats(std::unordered_set<int32_t> column_ids) {
  columns_to_keep_stats_ = std::move(column_ids);
  return *this;
}

Result<std::vector<std::shared_ptr<FileScanTask>>> ManifestGroup::PlanFiles() {
  auto create_file_scan_tasks =
      [this](std::vector<ManifestEntry>&& entries,
             const TaskContext& ctx) -> Result<std::vector<std::shared_ptr<ScanTask>>> {
    std::vector<std::shared_ptr<ScanTask>> tasks;
    tasks.reserve(entries.size());

    for (auto& entry : entries) {
      if (ctx.drop_stats) {
        ContentFileUtil::DropAllStats(*entry.data_file);
      } else if (!ctx.columns_to_keep_stats.empty()) {
        ContentFileUtil::DropUnselectedStats(*entry.data_file, ctx.columns_to_keep_stats);
      }
      ICEBERG_ASSIGN_OR_RAISE(auto delete_files, ctx.deletes->ForEntry(entry));
      ICEBERG_ASSIGN_OR_RAISE(auto residual,
                              ctx.residuals->ResidualFor(entry.data_file->partition));
      tasks.push_back(std::make_shared<FileScanTask>(
          std::move(entry.data_file), std::move(delete_files), std::move(residual)));
    }

    return tasks;
  };

  ICEBERG_ASSIGN_OR_RAISE(auto tasks, Plan(create_file_scan_tasks));

  // Convert ScanTask to FileScanTask
  std::vector<std::shared_ptr<FileScanTask>> file_tasks;
  file_tasks.reserve(tasks.size());
  for (auto& task : tasks) {
    file_tasks.push_back(internal::checked_pointer_cast<FileScanTask>(task));
  }
  return file_tasks;
}

Result<std::vector<std::shared_ptr<ScanTask>>> ManifestGroup::Plan(
    const CreateTasksFunction& create_tasks) {
  std::unordered_map<int32_t, std::shared_ptr<ResidualEvaluator>> residual_cache;
  auto get_residual_evaluator = [&](int32_t spec_id) -> Result<ResidualEvaluator*> {
    if (residual_cache.contains(spec_id)) {
      return residual_cache[spec_id].get();
    }

    auto spec_iter = specs_by_id_.find(spec_id);
    ICEBERG_CHECK(spec_iter != specs_by_id_.cend(),
                  "Cannot find partition spec for ID {}", spec_id);

    const auto& spec = spec_iter->second;
    ICEBERG_ASSIGN_OR_RAISE(
        auto residual_evaluator,
        ResidualEvaluator::Make((ignore_residuals_ ? True::Instance() : data_filter_),
                                *spec, *schema_, case_sensitive_));
    residual_cache[spec_id] = std::move(residual_evaluator);

    return residual_cache[spec_id].get();
  };

  ICEBERG_ASSIGN_OR_RAISE(auto delete_index, delete_index_builder_.Build());

  bool drop_stats = ManifestReader::ShouldDropStats(columns_);
  if (delete_index->has_equality_deletes()) {
    columns_ = ManifestReader::WithStatsColumns(columns_);
  }

  std::unordered_map<int32_t, std::unique_ptr<TaskContext>> task_context_cache;
  auto get_task_context = [&](int32_t spec_id) -> Result<TaskContext*> {
    if (task_context_cache.contains(spec_id)) {
      return task_context_cache[spec_id].get();
    }

    auto spec_iter = specs_by_id_.find(spec_id);
    ICEBERG_CHECK(spec_iter != specs_by_id_.cend(),
                  "Cannot find partition spec for ID {}", spec_id);

    const auto& spec = spec_iter->second;
    ICEBERG_ASSIGN_OR_RAISE(auto residuals, get_residual_evaluator(spec_id));
    task_context_cache[spec_id] = std::make_unique<TaskContext>(
        TaskContext{.spec = spec,
                    .deletes = delete_index.get(),
                    .residuals = residuals,
                    .drop_stats = drop_stats,
                    .columns_to_keep_stats = columns_to_keep_stats_});

    return task_context_cache[spec_id].get();
  };

  ICEBERG_ASSIGN_OR_RAISE(auto entry_groups, ReadEntries());

  std::vector<std::shared_ptr<ScanTask>> all_tasks;
  for (auto& [spec_id, entries] : entry_groups) {
    ICEBERG_ASSIGN_OR_RAISE(auto ctx, get_task_context(spec_id));
    ICEBERG_ASSIGN_OR_RAISE(auto tasks, create_tasks(std::move(entries), *ctx));
    all_tasks.insert(all_tasks.end(), std::make_move_iterator(tasks.begin()),
                     std::make_move_iterator(tasks.end()));
  }

  return all_tasks;
}

Result<std::vector<ManifestEntry>> ManifestGroup::Entries() {
  ICEBERG_ASSIGN_OR_RAISE(auto entry_groups, ReadEntries());

  std::vector<ManifestEntry> all_entries;
  for (auto& [_, entries] : entry_groups) {
    all_entries.insert(all_entries.end(), std::make_move_iterator(entries.begin()),
                       std::make_move_iterator(entries.end()));
  }

  return all_entries;
}

Result<std::unique_ptr<ManifestReader>> ManifestGroup::MakeReader(
    const ManifestFile& manifest) {
  auto spec_it = specs_by_id_.find(manifest.partition_spec_id);
  if (spec_it == specs_by_id_.end()) {
    return InvalidArgument("Partition spec {} not found for manifest {}",
                           manifest.partition_spec_id, manifest.manifest_path);
  }

  ICEBERG_ASSIGN_OR_RAISE(auto reader,
                          ManifestReader::Make(manifest, io_, schema_, spec_it->second));

  reader->FilterRows(data_filter_)
      .FilterPartitions(partition_filter_)
      .CaseSensitive(case_sensitive_)
      .Select(columns_);

  return reader;
}

Result<std::unordered_map<int32_t, std::vector<ManifestEntry>>>
ManifestGroup::ReadEntries() {
  std::unordered_map<int32_t, std::unique_ptr<ManifestEvaluator>> eval_cache;
  auto get_manifest_evaluator = [&](int32_t spec_id) -> Result<ManifestEvaluator*> {
    if (eval_cache.contains(spec_id)) {
      return eval_cache[spec_id].get();
    }

    auto spec_iter = specs_by_id_.find(spec_id);
    ICEBERG_CHECK(spec_iter != specs_by_id_.cend(),
                  "Cannot find partition spec for ID {}", spec_id);

    const auto& spec = spec_iter->second;
    auto projector = Projections::Inclusive(*spec, *schema_, case_sensitive_);
    ICEBERG_ASSIGN_OR_RAISE(auto partition_filter, projector->Project(data_filter_));
    ICEBERG_ASSIGN_OR_RAISE(partition_filter,
                            And::Make(partition_filter, partition_filter_));
    ICEBERG_ASSIGN_OR_RAISE(
        auto manifest_evaluator,
        ManifestEvaluator::MakePartitionFilter(std::move(partition_filter), spec,
                                               *schema_, case_sensitive_));
    eval_cache[spec_id] = std::move(manifest_evaluator);

    return eval_cache[spec_id].get();
  };

  std::unique_ptr<Evaluator> data_file_evaluator;
  if (file_filter_ && file_filter_->op() != Expression::Operation::kTrue) {
    // TODO(gangwu): create an Evaluator on the DataFile schema with empty
    // partition type
  }

  std::unordered_map<int32_t, std::vector<ManifestEntry>> result;

  // TODO(gangwu): Parallelize reading manifests
  for (const auto& manifest : data_manifests_) {
    const int32_t spec_id = manifest.partition_spec_id;

    ICEBERG_ASSIGN_OR_RAISE(auto manifest_evaluator, get_manifest_evaluator(spec_id));
    ICEBERG_ASSIGN_OR_RAISE(bool should_match, manifest_evaluator->Evaluate(manifest));
    if (!should_match) {
      // Skip this manifest because it doesn't match partition filter
      continue;
    }

    if (ignore_deleted_) {
      // only scan manifests that have entries other than deletes
      if (!manifest.has_added_files() && !manifest.has_existing_files()) {
        continue;
      }
    }

    if (ignore_existing_) {
      // only scan manifests that have entries other than existing
      if (!manifest.has_added_files() && !manifest.has_deleted_files()) {
        continue;
      }
    }

    // Read manifest entries
    ICEBERG_ASSIGN_OR_RAISE(auto reader, MakeReader(manifest));
    ICEBERG_ASSIGN_OR_RAISE(auto entries,
                            ignore_deleted_ ? reader->LiveEntries() : reader->Entries());

    for (auto& entry : entries) {
      if (ignore_existing_ && entry.status == ManifestStatus::kExisting) {
        continue;
      }

      if (data_file_evaluator != nullptr) {
        // TODO(gangwu): implement data_file_evaluator to evaluate StructLike on
        // top of entry.data_file
      }

      if (!manifest_entry_predicate_(entry)) {
        continue;
      }

      result[spec_id].push_back(std::move(entry));
    }
  }

  return result;
}

}  // namespace iceberg
