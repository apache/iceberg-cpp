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

#include <algorithm>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "iceberg/expression/binder.h"
#include "iceberg/expression/evaluator.h"
#include "iceberg/expression/expression.h"
#include "iceberg/expression/manifest_evaluator.h"
#include "iceberg/expression/projections.h"
#include "iceberg/expression/residual_evaluator.h"
#include "iceberg/file_io.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/metrics/scan_report.h"
#include "iceberg/partition_spec.h"
#include "iceberg/row/manifest_wrapper.h"
#include "iceberg/schema.h"
#include "iceberg/table_scan.h"
#include "iceberg/type.h"
#include "iceberg/util/cache_internal.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/content_file_util.h"
#include "iceberg/util/executor_util_internal.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

std::shared_ptr<Schema> DataFileFilterSchema() {
  auto empty_partition_type = std::make_shared<StructType>(std::vector<SchemaField>{});
  return std::make_shared<Schema>(std::vector<SchemaField>{
      DataFile::kContent,
      DataFile::kFilePath,
      DataFile::kFileFormat,
      DataFile::kSpecId,
      SchemaField::MakeRequired(DataFile::kPartitionFieldId, DataFile::kPartitionField,
                                std::move(empty_partition_type), DataFile::kPartitionDoc),
      DataFile::kRecordCount,
      DataFile::kFileSize,
      DataFile::kColumnSizes,
      DataFile::kValueCounts,
      DataFile::kNullValueCounts,
      DataFile::kNanValueCounts,
      DataFile::kLowerBounds,
      DataFile::kUpperBounds,
      DataFile::kKeyMetadata,
      DataFile::kSplitOffsets,
      DataFile::kEqualityIds,
      DataFile::kSortOrderId,
      DataFile::kFirstRowId,
      DataFile::kReferencedDataFile,
      DataFile::kContentOffset,
      DataFile::kContentSize});
}

}  // namespace

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

class ManifestGroup::FilePlanningIterator final
    : public Iterator<std::shared_ptr<FileScanTask>> {
 public:
  static Result<FileScanTaskIterator> Make(
      std::unique_ptr<ManifestGroup> group) {
    ICEBERG_RETURN_UNEXPECTED(group->CheckErrors());

    group->delete_index_builder_.WithScanMetrics(group->scan_metrics_);
    ICEBERG_ASSIGN_OR_RAISE(auto delete_index, group->delete_index_builder_.Build());

    const bool drop_stats =
        group->PrepareStatsProjection(delete_index->has_equality_deletes());

    std::unique_ptr<Evaluator> data_file_evaluator;
    if (group->file_filter_ &&
        group->file_filter_->op() != Expression::Operation::kTrue) {
      ICEBERG_ASSIGN_OR_RAISE(
          data_file_evaluator,
          Evaluator::Make(*DataFileFilterSchema(), group->file_filter_,
                          group->case_sensitive_));
    }

    return FileScanTaskIterator(
        new FilePlanningIterator(std::move(group), std::move(delete_index),
                                 std::move(data_file_evaluator), drop_stats));
  }

  Result<std::optional<std::shared_ptr<FileScanTask>>> NextImpl() override {
    while (true) {
      ICEBERG_ASSIGN_OR_RAISE(auto entry, NextEntry());
      if (!entry.has_value()) {
        return std::nullopt;
      }

      auto [spec_id, value] = std::move(entry).value();
      if (group_->ignore_existing_ && value.status == ManifestStatus::kExisting) {
        IncrementSkippedDataFiles();
        continue;
      }

      ICEBERG_DCHECK(value.data_file != nullptr, "Data file cannot be null");
      if (data_file_evaluator_) {
        DataFileStructLike data_file(*value.data_file);
        ICEBERG_ASSIGN_OR_RAISE(bool should_match,
                                data_file_evaluator_->Evaluate(data_file));
        if (!should_match) {
          IncrementSkippedDataFiles();
          continue;
        }
      }

      if (!group_->manifest_entry_predicate_(value)) {
        IncrementSkippedDataFiles();
        continue;
      }

      ICEBERG_ASSIGN_OR_RAISE(auto delete_files, delete_index_->ForEntry(value));

      // Equality-delete matching uses data-file statistics. Drop unrequested stats only
      // after the delete index has finished matching this entry.
      if (drop_stats_) {
        ContentFileUtil::DropAllStats(*value.data_file);
      } else if (!group_->columns_to_keep_stats_.empty()) {
        ContentFileUtil::DropUnselectedStats(*value.data_file,
                                             group_->columns_to_keep_stats_);
      }

      UpdateResultMetrics(*value.data_file, delete_files);

      ICEBERG_ASSIGN_OR_RAISE(auto residuals, GetResidualEvaluator(spec_id));
      ICEBERG_ASSIGN_OR_RAISE(auto residual,
                              residuals->ResidualFor(value.data_file->partition));

      return std::optional<std::shared_ptr<FileScanTask>>{std::make_shared<FileScanTask>(
          std::move(value.data_file), std::move(delete_files), std::move(residual))};
    }
  }

 private:
  FilePlanningIterator(std::unique_ptr<ManifestGroup> group,
                       std::unique_ptr<DeleteFileIndex> delete_index,
                       std::unique_ptr<Evaluator> data_file_evaluator, bool drop_stats)
      : group_(std::move(group)),
        delete_index_(std::move(delete_index)),
        data_file_evaluator_(std::move(data_file_evaluator)),
        drop_stats_(drop_stats) {}

  using TaggedEntry = std::pair<int32_t, ManifestEntry>;
  using TaggedIterator = std::pair<int32_t, std::unique_ptr<Iterator<ManifestEntry>>>;

  Result<std::optional<TaggedEntry>> NextEntry() {
    if (!group_->executor_.has_value()) {
      while (true) {
        if (!entry_iterator_) {
          ICEBERG_ASSIGN_OR_RAISE(bool opened, OpenNextManifest());
          if (!opened) {
            return std::nullopt;
          }
        }

        ICEBERG_ASSIGN_OR_RAISE(auto entry, entry_iterator_->Next());
        if (!entry.has_value()) {
          entry_iterator_.reset();
          continue;
        }
        return std::optional<TaggedEntry>{std::in_place, current_spec_id_,
                                          std::move(entry).value()};
      }
    }

    while (true) {
      if (next_batch_iterator_ == batch_iterators_.size()) {
        ICEBERG_ASSIGN_OR_RAISE(bool loaded, LoadNextManifestBatch());
        if (!loaded) {
          return std::nullopt;
        }
      }

      auto& [spec_id, iterator] = batch_iterators_[next_batch_iterator_];
      ICEBERG_ASSIGN_OR_RAISE(auto entry, iterator->Next());
      if (!entry.has_value()) {
        iterator.reset();
        ++next_batch_iterator_;
        continue;
      }
      return std::optional<TaggedEntry>{std::in_place, spec_id, std::move(entry).value()};
    }
  }

  Result<ManifestEvaluator*> GetManifestEvaluator(int32_t spec_id) {
    auto cached = manifest_evaluators_.find(spec_id);
    if (cached != manifest_evaluators_.end()) {
      return cached->second.get();
    }

    auto spec_iter = group_->specs_by_id_.find(spec_id);
    ICEBERG_CHECK(spec_iter != group_->specs_by_id_.cend(),
                  "Cannot find partition spec for ID {}", spec_id);

    const auto& spec = spec_iter->second;
    auto projector =
        Projections::Inclusive(*spec, *group_->schema_, group_->case_sensitive_);
    ICEBERG_ASSIGN_OR_RAISE(auto partition_filter,
                            projector->Project(group_->data_filter_));
    ICEBERG_ASSIGN_OR_RAISE(partition_filter, And::Make(std::move(partition_filter),
                                                        group_->partition_filter_));
    ICEBERG_ASSIGN_OR_RAISE(auto evaluator,
                            ManifestEvaluator::MakePartitionFilter(
                                std::move(partition_filter), spec, *group_->schema_,
                                group_->case_sensitive_));
    auto* result = evaluator.get();
    manifest_evaluators_.emplace(spec_id, std::move(evaluator));
    return result;
  }

  Result<ResidualEvaluator*> GetResidualEvaluator(int32_t spec_id) {
    auto cached = residual_evaluators_.find(spec_id);
    if (cached != residual_evaluators_.end()) {
      return cached->second.get();
    }

    auto spec_iter = group_->specs_by_id_.find(spec_id);
    ICEBERG_CHECK(spec_iter != group_->specs_by_id_.cend(),
                  "Cannot find partition spec for ID {}", spec_id);

    ICEBERG_ASSIGN_OR_RAISE(
        auto evaluator,
        ResidualEvaluator::Make(
            (group_->ignore_residuals_ ? True::Instance() : group_->data_filter_),
            *spec_iter->second, *group_->schema_, group_->case_sensitive_));
    auto* result = evaluator.get();
    residual_evaluators_.emplace(spec_id, std::move(evaluator));
    return result;
  }

  Result<bool> ShouldReadManifest(const ManifestFile& manifest) {
    ICEBERG_ASSIGN_OR_RAISE(auto evaluator,
                            GetManifestEvaluator(manifest.partition_spec_id));
    ICEBERG_ASSIGN_OR_RAISE(bool should_match, evaluator->Evaluate(manifest));
    const bool has_non_deleted_files =
        manifest.has_added_files() || manifest.has_existing_files();
    const bool has_non_existing_files =
        manifest.has_added_files() || manifest.has_deleted_files();
    const bool has_only_ignored_files =
        (group_->ignore_deleted_ && !has_non_deleted_files) ||
        (group_->ignore_existing_ && !has_non_existing_files);
    if (!should_match || has_only_ignored_files) {
      IncrementSkippedDataManifests();
      return false;
    }

    if (group_->scan_metrics_) {
      group_->scan_metrics_->scanned_data_manifests->Increment(1);
    }
    return true;
  }

  Result<bool> OpenNextManifest() {
    while (next_manifest_ < group_->data_manifests_.size()) {
      const auto& manifest = group_->data_manifests_[next_manifest_++];
      ICEBERG_ASSIGN_OR_RAISE(bool should_read, ShouldReadManifest(manifest));
      if (!should_read) {
        continue;
      }

      ICEBERG_ASSIGN_OR_RAISE(auto reader, group_->MakeReader(manifest));
      ICEBERG_ASSIGN_OR_RAISE(entry_iterator_, group_->ignore_deleted_
                                                   ? reader->LiveEntriesIterator()
                                                   : reader->EntriesIterator());
      current_spec_id_ = manifest.partition_spec_id;
      return true;
    }
    return false;
  }

  Result<bool> LoadNextManifestBatch() {
    std::vector<const ManifestFile*> manifests;
    manifests.reserve(kManifestReadBatchSize);
    while (next_manifest_ < group_->data_manifests_.size() &&
           manifests.size() < kManifestReadBatchSize) {
      const auto& manifest = group_->data_manifests_[next_manifest_++];
      ICEBERG_ASSIGN_OR_RAISE(bool should_read, ShouldReadManifest(manifest));
      if (should_read) {
        manifests.push_back(&manifest);
      }
    }

    if (manifests.empty()) {
      return false;
    }

    // Open the readers concurrently, but keep their iterators instead of collecting
    // entries here. This preserves bounded memory for large manifests while retaining
    // parallel manifest initialization when an executor is configured.
    ICEBERG_ASSIGN_OR_RAISE(
        batch_iterators_,
        ParallelCollect(
            group_->executor_, manifests,
            [this](const ManifestFile* manifest) -> Result<std::vector<TaggedIterator>> {
              ICEBERG_ASSIGN_OR_RAISE(auto reader, group_->MakeReader(*manifest));
              ICEBERG_ASSIGN_OR_RAISE(auto iterator, group_->ignore_deleted_
                                                         ? reader->LiveEntriesIterator()
                                                         : reader->EntriesIterator());

              std::vector<TaggedIterator> tagged_iterators;
              tagged_iterators.emplace_back(manifest->partition_spec_id,
                                            std::move(iterator));
              return tagged_iterators;
            }));
    next_batch_iterator_ = 0;
    return true;
  }

  void IncrementSkippedDataManifests() {
    if (group_->scan_metrics_) {
      group_->scan_metrics_->skipped_data_manifests->Increment(1);
    }
  }

  void IncrementSkippedDataFiles() {
    if (group_->scan_metrics_) {
      group_->scan_metrics_->skipped_data_files->Increment(1);
    }
  }

  void UpdateResultMetrics(const DataFile& data_file,
                           const std::vector<std::shared_ptr<DataFile>>& delete_files) {
    if (!group_->scan_metrics_) {
      return;
    }

    group_->scan_metrics_->total_file_size_in_bytes->Increment(
        ContentFileUtil::ContentSizeInBytes(data_file));
    group_->scan_metrics_->result_data_files->Increment(1);
    group_->scan_metrics_->result_delete_files->Increment(
        static_cast<int64_t>(delete_files.size()));
    int64_t deletes_size = 0;
    for (const auto& delete_file : delete_files) {
      deletes_size += ContentFileUtil::ContentSizeInBytes(*delete_file);
    }
    group_->scan_metrics_->total_delete_file_size_in_bytes->Increment(deletes_size);
  }

  std::unique_ptr<ManifestGroup> group_;
  std::unique_ptr<DeleteFileIndex> delete_index_;
  std::unique_ptr<Evaluator> data_file_evaluator_;
  std::unordered_map<int32_t, std::unique_ptr<ManifestEvaluator>> manifest_evaluators_;
  std::unordered_map<int32_t, std::shared_ptr<ResidualEvaluator>> residual_evaluators_;
  std::unique_ptr<Iterator<ManifestEntry>> entry_iterator_;
  std::vector<TaggedIterator> batch_iterators_;
  size_t next_manifest_ = 0;
  size_t next_batch_iterator_ = 0;
  int32_t current_spec_id_ = 0;
  bool drop_stats_;

  // Limit the number of manifest readers and iterators retained by executor-backed
  // planning. The executor still controls actual task concurrency, while this fixed
  // cap prevents resource use from scaling with the total manifest count. Entries
  // within each manifest remain streamed, so this does not cap manifest size.
  static constexpr size_t kManifestReadBatchSize = 32;
};

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

ManifestGroup& ManifestGroup::PlanWith(OptionalExecutor executor) {
  executor_ = executor;
  delete_index_builder_.PlanWith(executor);
  return *this;
}

ManifestGroup& ManifestGroup::WithScanMetrics(std::shared_ptr<ScanMetrics> scan_metrics) {
  scan_metrics_ = std::move(scan_metrics);
  return *this;
}

Result<std::vector<std::shared_ptr<FileScanTask>>> ManifestGroup::PlanFiles() {
  auto create_file_scan_tasks =
      [this](std::vector<ManifestEntry>&& entries,
             const TaskContext& ctx) -> Result<std::vector<std::shared_ptr<ScanTask>>> {
    std::vector<std::shared_ptr<ScanTask>> tasks;
    tasks.reserve(entries.size());

    for (auto& entry : entries) {
      ICEBERG_ASSIGN_OR_RAISE(auto delete_files, ctx.deletes->ForEntry(entry));

      // Equality-delete matching uses data-file statistics. Drop unrequested stats only
      // after the delete index has finished matching this entry.
      if (ctx.drop_stats) {
        ContentFileUtil::DropAllStats(*entry.data_file);
      } else if (!ctx.columns_to_keep_stats.empty()) {
        ContentFileUtil::DropUnselectedStats(*entry.data_file, ctx.columns_to_keep_stats);
      }
      // Count result metrics once per data file task. A delete file shared by
      // multiple data files contributes once to each task, unlike indexed delete files.
      if (scan_metrics_) {
        scan_metrics_->total_file_size_in_bytes->Increment(
            ContentFileUtil::ContentSizeInBytes(*entry.data_file));
        scan_metrics_->result_data_files->Increment(1);
        scan_metrics_->result_delete_files->Increment(
            static_cast<int64_t>(delete_files.size()));
        int64_t deletes_size = 0;
        for (const auto& delete_file : delete_files) {
          deletes_size += ContentFileUtil::ContentSizeInBytes(*delete_file);
        }
        scan_metrics_->total_delete_file_size_in_bytes->Increment(deletes_size);
      }
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

Result<FileScanTaskIterator> ManifestGroup::PlanFilesIterator() && {
  auto group = std::make_unique<ManifestGroup>(std::move(*this));
  return FilePlanningIterator::Make(std::move(group));
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

  delete_index_builder_.WithScanMetrics(scan_metrics_);
  ICEBERG_ASSIGN_OR_RAISE(auto delete_index, delete_index_builder_.Build());

  const bool drop_stats = PrepareStatsProjection(delete_index->has_equality_deletes());

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
  ICEBERG_ASSIGN_OR_RAISE(auto reader,
                          ManifestReader::Make(manifest, io_, schema_, specs_by_id_));

  auto columns = columns_;
  if (file_filter_ && file_filter_->op() != Expression::Operation::kTrue &&
      !columns.empty() && !std::ranges::contains(columns, Schema::kAllColumns)) {
    auto data_file_schema = DataFileFilterSchema();
    ICEBERG_ASSIGN_OR_RAISE(
        auto bound_file_filter,
        Binder::Bind(*data_file_schema, file_filter_, case_sensitive_));
    ICEBERG_ASSIGN_OR_RAISE(auto referenced_field_ids,
                            ReferenceVisitor::GetReferencedFieldIds(bound_file_filter));

    std::unordered_set<std::string> selected_columns(columns.cbegin(), columns.cend());
    for (const auto field_id : referenced_field_ids) {
      if (field_id == DataFile::kSpecIdFieldId) {
        continue;
      }
      ICEBERG_ASSIGN_OR_RAISE(auto column_name,
                              data_file_schema->FindColumnNameById(field_id));
      if (column_name.has_value()) {
        std::string column_name_str(column_name.value());
        if (selected_columns.contains(column_name_str)) {
          continue;
        }
        columns.push_back(std::move(column_name_str));
        selected_columns.insert(columns.back());
      }
    }
  }

  reader->FilterRows(data_filter_)
      .FilterPartitions(partition_filter_)
      .CaseSensitive(case_sensitive_)
      .Select(std::move(columns));

  if (scan_metrics_) {
    reader->SkipCounter(scan_metrics_->skipped_data_files);
  }

  return reader;
}

bool ManifestGroup::PrepareStatsProjection(bool has_equality_deletes) {
  // The caller's projection records whether stats were requested. Equality-delete
  // matching may add stats temporarily, but they should still be dropped from the
  // result when the original projection did not request them. Keeping this decision
  // here ensures eager and iterator planning use identical semantics.
  const bool drop_stats = ManifestReader::ShouldDropStats(columns_);
  if (has_equality_deletes) {
    columns_ = ManifestReader::WithStatsColumns(columns_);
  }
  return drop_stats;
}

Result<std::unordered_map<int32_t, std::vector<ManifestEntry>>>
ManifestGroup::ReadEntries() {
  const auto cache_capacity = static_cast<int32_t>(specs_by_id_.size());
  auto get_manifest_evaluator = internal::MemoizeLru(
      [this](int32_t spec_id) -> Result<std::shared_ptr<ManifestEvaluator>> {
        auto spec_iter = specs_by_id_.find(spec_id);
        ICEBERG_CHECK(spec_iter != specs_by_id_.cend(),
                      "Cannot find partition spec for ID {}", spec_id);

        auto projector =
            Projections::Inclusive(*spec_iter->second, *schema_, case_sensitive_);
        ICEBERG_ASSIGN_OR_RAISE(auto partition_filter, projector->Project(data_filter_));
        ICEBERG_ASSIGN_OR_RAISE(partition_filter,
                                And::Make(partition_filter, partition_filter_));
        ICEBERG_ASSIGN_OR_RAISE(
            auto evaluator, ManifestEvaluator::MakePartitionFilter(
                                std::move(partition_filter), spec_iter->second, *schema_,
                                case_sensitive_));
        return std::shared_ptr<ManifestEvaluator>(std::move(evaluator));
      },
      cache_capacity);

  const bool has_file_filter =
      file_filter_ && file_filter_->op() != Expression::Operation::kTrue;
  std::unique_ptr<Evaluator> data_file_evaluator;
  if (has_file_filter) {
    ICEBERG_ASSIGN_OR_RAISE(
        data_file_evaluator,
        Evaluator::Make(*DataFileFilterSchema(), file_filter_, case_sensitive_));
  }

  return ParallelCollect(
      executor_, data_manifests_,
      [&](const ManifestFile& manifest)
          -> Result<std::unordered_map<int32_t, std::vector<ManifestEntry>>> {
        const int32_t spec_id = manifest.partition_spec_id;

        ICEBERG_ASSIGN_OR_RAISE(auto manifest_evaluator, get_manifest_evaluator(spec_id));
        ICEBERG_ASSIGN_OR_RAISE(bool should_match,
                                manifest_evaluator->Evaluate(manifest));
        if (!should_match) {
          // Skip this manifest because it doesn't match partition filter
          if (scan_metrics_) {
            scan_metrics_->skipped_data_manifests->Increment(1);
          }
          return {};
        }
        if (ignore_deleted_) {
          // only scan manifests that have entries other than deletes
          if (!manifest.has_added_files() && !manifest.has_existing_files()) {
            if (scan_metrics_) scan_metrics_->skipped_data_manifests->Increment(1);
            return {};
          }
        }

        if (ignore_existing_) {
          // only scan manifests that have entries other than existing
          if (!manifest.has_added_files() && !manifest.has_deleted_files()) {
            if (scan_metrics_) scan_metrics_->skipped_data_manifests->Increment(1);
            return {};
          }
        }

        if (scan_metrics_) {
          scan_metrics_->scanned_data_manifests->Increment(1);
        }

        // Read manifest entries
        ICEBERG_ASSIGN_OR_RAISE(auto reader, MakeReader(manifest));
        ICEBERG_ASSIGN_OR_RAISE(
            auto entries, ignore_deleted_ ? reader->LiveEntries() : reader->Entries());

        std::unordered_map<int32_t, std::vector<ManifestEntry>> manifest_result;

        for (auto& entry : entries) {
          if (ignore_existing_ && entry.status == ManifestStatus::kExisting) {
            if (scan_metrics_) scan_metrics_->skipped_data_files->Increment(1);
            continue;
          }

          if (data_file_evaluator != nullptr) {
            DataFileStructLike data_file(*entry.data_file);
            ICEBERG_ASSIGN_OR_RAISE(bool should_match,
                                    data_file_evaluator->Evaluate(data_file));
            if (!should_match) {
              if (scan_metrics_) scan_metrics_->skipped_data_files->Increment(1);
              continue;
            }
          }

          if (!manifest_entry_predicate_(entry)) {
            if (scan_metrics_) scan_metrics_->skipped_data_files->Increment(1);
            continue;
          }

          manifest_result[spec_id].push_back(std::move(entry));
        }
        return manifest_result;
      });
}

}  // namespace iceberg
