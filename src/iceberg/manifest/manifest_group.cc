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
#include "iceberg/util/content_file_util.h"
#include "iceberg/util/executor_util_internal.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/stream.h"

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

// The same manifest may be listed more than once (e.g. overlapping snapshots),
// and it should only be scanned once.
std::vector<ManifestFile> DeduplicateManifests(std::vector<ManifestFile> manifests) {
  std::unordered_set<ManifestFile> seen;
  seen.reserve(manifests.size());
  std::vector<ManifestFile> deduped;
  deduped.reserve(manifests.size());
  for (auto& manifest : manifests) {
    if (seen.insert(manifest).second) {
      deduped.push_back(std::move(manifest));
    }
  }
  return deduped;
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
  return std::unique_ptr<ManifestGroup>(new ManifestGroup(
      std::move(io), std::move(schema), std::move(specs_by_id),
      DeduplicateManifests(std::move(data_manifests)), std::move(delete_index_builder)));
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
      manifest_entry_predicate_([](const ManifestEntry&) { return true; }),
      columns_{std::string(Schema::kAllColumns)} {}

ManifestGroup::~ManifestGroup() = default;

ManifestGroup::ManifestGroup(ManifestGroup&&) noexcept = default;
ManifestGroup& ManifestGroup::operator=(ManifestGroup&&) noexcept = default;

class ManifestGroup::FilePlanningStream final : public FileScanTaskStream {
 public:
  // Manifest/residual evaluator caches and entry-filtering rules shared by EntryStream
  // and by ManifestGroup::ReadEntries().
  class Evaluators {
   public:
    static Result<Evaluators> Make(ManifestGroup& group) {
      std::unique_ptr<Evaluator> data_file_evaluator;
      if (group.file_filter_ &&
          group.file_filter_->op() != Expression::Operation::kTrue) {
        ICEBERG_ASSIGN_OR_RAISE(
            data_file_evaluator,
            Evaluator::Make(*DataFileFilterSchema(), group.file_filter_,
                            group.case_sensitive_));
      }
      return Evaluators(group, std::move(data_file_evaluator));
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
        if (group_->scan_metrics_) {
          group_->scan_metrics_->skipped_data_manifests->Increment(1);
        }
        return false;
      }

      if (group_->scan_metrics_) {
        group_->scan_metrics_->scanned_data_manifests->Increment(1);
      }
      return true;
    }

    Result<bool> ShouldKeepEntry(const ManifestEntry& entry) {
      if (group_->ignore_existing_ && entry.status == ManifestStatus::kExisting) {
        IncrementSkippedDataFiles();
        return false;
      }

      if (data_file_evaluator_) {
        DataFileStructLike data_file(*entry.data_file);
        ICEBERG_ASSIGN_OR_RAISE(bool should_match,
                                data_file_evaluator_->Evaluate(data_file));
        if (!should_match) {
          IncrementSkippedDataFiles();
          return false;
        }
      }

      if (!group_->manifest_entry_predicate_(entry)) {
        IncrementSkippedDataFiles();
        return false;
      }

      return true;
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

   private:
    Evaluators(ManifestGroup& group, std::unique_ptr<Evaluator> data_file_evaluator)
        : group_(&group), data_file_evaluator_(std::move(data_file_evaluator)) {}

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

    void IncrementSkippedDataFiles() {
      if (group_->scan_metrics_) {
        group_->scan_metrics_->skipped_data_files->Increment(1);
      }
    }

    ManifestGroup* group_;
    std::unique_ptr<Evaluator> data_file_evaluator_;
    std::unordered_map<int32_t, std::unique_ptr<ManifestEvaluator>> manifest_evaluators_;
    std::unordered_map<int32_t, std::shared_ptr<ResidualEvaluator>> residual_evaluators_;
  };

  // Filtered (spec_id, ManifestEntry) pairs across all data manifests.
  class EntryStream final : public Stream<std::pair<int32_t, ManifestEntry>> {
   public:
    using TaggedEntry = std::pair<int32_t, ManifestEntry>;

    EntryStream(ManifestGroup& group, Evaluators& evaluators,
                std::vector<std::string> columns)
        : group_(&group),
          evaluators_(&evaluators),
          columns_(std::move(columns)),
          batch_size_(group_->executor_.has_value() ? kManifestReadBatchSize : 1) {}

    Result<std::optional<TaggedEntry>> NextImpl() override {
      while (true) {
        if (next_batch_stream_ == batch_streams_.size()) {
          ICEBERG_ASSIGN_OR_RAISE(bool loaded, LoadNextBatch());
          if (!loaded) {
            return std::nullopt;
          }
        }

        auto& [spec_id, stream] = batch_streams_[next_batch_stream_];
        ICEBERG_ASSIGN_OR_RAISE(auto entry, stream->Next());
        if (!entry.has_value()) {
          stream.reset();
          ++next_batch_stream_;
          continue;
        }

        ICEBERG_ASSIGN_OR_RAISE(bool keep, evaluators_->ShouldKeepEntry(*entry));
        if (!keep) {
          continue;
        }
        return std::optional<TaggedEntry>{std::in_place, spec_id,
                                          std::move(entry).value()};
      }
    }

   private:
    using TaggedStream = std::pair<int32_t, ManifestEntryStreamPtr>;

    Result<bool> LoadNextBatch() {
      std::vector<const ManifestFile*> manifests;
      manifests.reserve(batch_size_);
      while (next_manifest_ < group_->data_manifests_.size() &&
             manifests.size() < batch_size_) {
        const auto& manifest = group_->data_manifests_[next_manifest_++];
        ICEBERG_ASSIGN_OR_RAISE(bool should_read,
                                evaluators_->ShouldReadManifest(manifest));
        if (should_read) {
          manifests.push_back(&manifest);
        }
      }

      if (manifests.empty()) {
        return false;
      }

      // Opens readers concurrently when batch_size_ > 1, but keeps their streams
      // instead of collecting entries here, bounding memory for large manifests.
      ICEBERG_ASSIGN_OR_RAISE(
          batch_streams_,
          ParallelCollect(
              group_->executor_, manifests,
              [this](const ManifestFile* manifest) -> Result<std::vector<TaggedStream>> {
                ICEBERG_ASSIGN_OR_RAISE(auto reader,
                                        group_->MakeReader(*manifest, columns_));
                ICEBERG_ASSIGN_OR_RAISE(auto stream, group_->ignore_deleted_
                                                         ? reader->LiveEntriesStream()
                                                         : reader->EntriesStream());

                std::vector<TaggedStream> tagged_streams;
                tagged_streams.emplace_back(manifest->partition_spec_id,
                                            std::move(stream));
                return tagged_streams;
              }));
      next_batch_stream_ = 0;
      return true;
    }

    ManifestGroup* group_;
    Evaluators* evaluators_;
    std::vector<std::string> columns_;
    size_t batch_size_;
    std::vector<TaggedStream> batch_streams_;
    size_t next_manifest_ = 0;
    size_t next_batch_stream_ = 0;

    // Caps concurrent manifest readers/streams so resource use doesn't scale with the
    // total manifest count; entries within a manifest are still streamed one at a time.
    static constexpr size_t kManifestReadBatchSize = 32;
  };

  static Result<FileScanTaskStreamPtr> Make(std::unique_ptr<ManifestGroup> group) {
    ICEBERG_RETURN_UNEXPECTED(group->CheckErrors());

    group->delete_index_builder_.WithScanMetrics(group->scan_metrics_);
    ICEBERG_ASSIGN_OR_RAISE(auto delete_index, group->delete_index_builder_.Build());

    auto stats_projection =
        group->PrepareStatsProjection(delete_index->has_equality_deletes());
    const bool drop_stats = stats_projection.drop_stats;

    ICEBERG_ASSIGN_OR_RAISE(auto evaluators, Evaluators::Make(*group));

    return FileScanTaskStreamPtr(new FilePlanningStream(
        std::move(group), std::move(delete_index), std::move(evaluators),
        std::move(stats_projection.columns), drop_stats));
  }

  Result<std::optional<std::shared_ptr<FileScanTask>>> NextImpl() override {
    ICEBERG_ASSIGN_OR_RAISE(auto entry, entries_->Next());
    if (!entry.has_value()) {
      return std::nullopt;
    }

    auto [spec_id, value] = std::move(entry).value();
    ICEBERG_DCHECK(value.data_file != nullptr, "Data file cannot be null");

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

    ICEBERG_ASSIGN_OR_RAISE(auto residuals, evaluators_.GetResidualEvaluator(spec_id));
    ICEBERG_ASSIGN_OR_RAISE(auto residual,
                            residuals->ResidualFor(value.data_file->partition));

    return std::make_shared<FileScanTask>(std::move(value.data_file),
                                          std::move(delete_files), std::move(residual));
  }

 private:
  FilePlanningStream(std::unique_ptr<ManifestGroup> group,
                     std::unique_ptr<DeleteFileIndex> delete_index, Evaluators evaluators,
                     std::vector<std::string> columns, bool drop_stats)
      : group_(std::move(group)),
        delete_index_(std::move(delete_index)),
        evaluators_(std::move(evaluators)),
        drop_stats_(drop_stats),
        entries_(
            std::make_unique<EntryStream>(*group_, evaluators_, std::move(columns))) {}

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
  Evaluators evaluators_;
  bool drop_stats_;
  std::unique_ptr<EntryStream> entries_;
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

Result<std::vector<std::shared_ptr<FileScanTask>>> ManifestGroup::PlanFiles() && {
  ICEBERG_ASSIGN_OR_RAISE(auto stream, std::move(*this).PlanFilesStream());
  return stream->ToVector();
}

Result<FileScanTaskStreamPtr> ManifestGroup::PlanFilesStream() && {
  auto group = std::make_unique<ManifestGroup>(std::move(*this));
  return FilePlanningStream::Make(std::move(group));
}

Result<std::vector<std::shared_ptr<ScanTask>>> ManifestGroup::Plan(
    const CreateTasksFunction& create_tasks) {
  ICEBERG_ASSIGN_OR_RAISE(auto evaluators, FilePlanningStream::Evaluators::Make(*this));

  delete_index_builder_.WithScanMetrics(scan_metrics_);
  ICEBERG_ASSIGN_OR_RAISE(auto delete_index, delete_index_builder_.Build());

  auto stats_projection = PrepareStatsProjection(delete_index->has_equality_deletes());

  std::unordered_map<int32_t, std::unique_ptr<TaskContext>> task_context_cache;
  auto get_task_context = [&](int32_t spec_id) -> Result<TaskContext*> {
    if (task_context_cache.contains(spec_id)) {
      return task_context_cache[spec_id].get();
    }

    auto spec_iter = specs_by_id_.find(spec_id);
    ICEBERG_CHECK(spec_iter != specs_by_id_.cend(),
                  "Cannot find partition spec for ID {}", spec_id);

    const auto& spec = spec_iter->second;
    ICEBERG_ASSIGN_OR_RAISE(auto residuals, evaluators.GetResidualEvaluator(spec_id));
    task_context_cache[spec_id] = std::make_unique<TaskContext>(
        TaskContext{.spec = spec,
                    .deletes = delete_index.get(),
                    .residuals = residuals,
                    .drop_stats = stats_projection.drop_stats,
                    .columns_to_keep_stats = columns_to_keep_stats_});

    return task_context_cache[spec_id].get();
  };

  ICEBERG_ASSIGN_OR_RAISE(auto entry_groups, ReadEntries(stats_projection.columns));

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
  ICEBERG_ASSIGN_OR_RAISE(auto entry_groups, ReadEntries(columns_));

  std::vector<ManifestEntry> all_entries;
  for (auto& [_, entries] : entry_groups) {
    all_entries.insert(all_entries.end(), std::make_move_iterator(entries.begin()),
                       std::make_move_iterator(entries.end()));
  }

  return all_entries;
}

Result<std::unique_ptr<ManifestReader>> ManifestGroup::MakeReader(
    const ManifestFile& manifest, std::vector<std::string> columns) {
  ICEBERG_ASSIGN_OR_RAISE(auto reader,
                          ManifestReader::Make(manifest, io_, schema_, specs_by_id_));

  if (file_filter_ && file_filter_->op() != Expression::Operation::kTrue &&
      !std::ranges::contains(columns, Schema::kAllColumns)) {
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
      .Select(columns);

  if (scan_metrics_) {
    reader->SkipCounter(scan_metrics_->skipped_data_files);
  }

  return reader;
}

ManifestGroup::StatsProjection ManifestGroup::PrepareStatsProjection(
    bool has_equality_deletes) const {
  // The caller's projection records whether stats were requested. Equality-delete
  // matching may add stats temporarily, but they should still be dropped from the
  // result when the original projection did not request them. Keeping this decision
  // here ensures eager and stream planning use identical semantics.
  StatsProjection result{.columns = columns_,
                         .drop_stats = ManifestReader::ShouldDropStats(columns_)};
  // Delete matching and residual evaluation require partition values even when the
  // caller selects no columns. A select-all projection already includes them.
  if (!std::ranges::contains(result.columns, Schema::kAllColumns) &&
      !std::ranges::contains(result.columns, DataFile::kPartitionField)) {
    result.columns.emplace_back(DataFile::kPartitionField);
  }
  if (has_equality_deletes) {
    result.columns = ManifestReader::WithStatsColumns(result.columns);
  }
  return result;
}

Result<std::unordered_map<int32_t, std::vector<ManifestEntry>>>
ManifestGroup::ReadEntries(const std::vector<std::string>& columns) {
  ICEBERG_ASSIGN_OR_RAISE(auto evaluators, FilePlanningStream::Evaluators::Make(*this));
  FilePlanningStream::EntryStream entries(*this, evaluators, columns);
  ICEBERG_ASSIGN_OR_RAISE(auto tagged_entries, entries.ToVector());

  std::unordered_map<int32_t, std::vector<ManifestEntry>> result;
  for (auto& [spec_id, entry] : tagged_entries) {
    result[spec_id].push_back(std::move(entry));
  }
  return result;
}

}  // namespace iceberg
