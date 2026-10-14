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

#include "iceberg/data/position_delete_update.h"

#include <algorithm>
#include <format>
#include <iterator>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "iceberg/data/delete_loader.h"
#include "iceberg/data/output_file_cleanup_internal.h"
#include "iceberg/data/position_delete_writer.h"
#include "iceberg/deletes/dv_writer.h"
#include "iceberg/file_format.h"
#include "iceberg/file_io.h"
#include "iceberg/location_provider.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"
#include "iceberg/table_scan.h"
#include "iceberg/update/row_delta.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/string_util.h"
#include "iceberg/util/uuid.h"

namespace iceberg {

namespace {

struct TargetFile {
  std::shared_ptr<DataFile> data_file;
  std::shared_ptr<PartitionSpec> spec;
  std::vector<std::shared_ptr<DataFile>> position_delete_files;
};

}  // namespace

class PositionDeleteUpdate::Impl {
 public:
  explicit Impl(std::shared_ptr<Table> table) : table_(std::move(table)) {}

  Status Delete(std::string_view data_file_path, int64_t pos) {
    ICEBERG_PRECHECK(!terminal_, "Position delete update is no longer usable");
    ICEBERG_PRECHECK(!data_file_path.empty(), "Data file path cannot be empty");
    ICEBERG_PRECHECK(pos >= 0, "Position delete must be non-negative: {}", pos);
    deletes_[std::string(data_file_path)].push_back(pos);
    return {};
  }

  Status Commit() {
    ICEBERG_PRECHECK(!terminal_, "Position delete update is no longer usable");
    ICEBERG_PRECHECK(!deletes_.empty(), "Position delete update is empty");
    ICEBERG_PRECHECK(table_->metadata()->format_version >= 2,
                     "Position deletes require table format version 2 or later");
    ICEBERG_RETURN_UNEXPECTED(internal::CleanupOutputFiles(*table_->io(), output_paths_));
    auto status = CommitDeletes();
    if (!status.has_value() && status.error().kind != ErrorKind::kCommitStateUnknown) {
      return internal::FailWithOutputCleanup(std::move(status.error()), *table_->io(),
                                             output_paths_);
    }
    terminal_ = true;
    output_paths_.clear();
    return status;
  }

 private:
  Status CommitDeletes() {
    ICEBERG_ASSIGN_OR_RAISE(auto snapshot, table_->current_snapshot());
    ICEBERG_ASSIGN_OR_RAISE(auto targets, ResolveTargets());

    ICEBERG_ASSIGN_OR_RAISE(auto written, table_->metadata()->format_version >= 3
                                              ? WriteDeletionVectors(targets)
                                              : WriteParquetDeletes(targets));
    ICEBERG_ASSIGN_OR_RAISE(auto row_delta, table_->NewRowDelta());
    row_delta->ValidateFromSnapshot(snapshot->snapshot_id)
        .ValidateDataFilesExist(written.referenced_data_files)
        .ValidateDeletedFiles();
    for (const auto& file : written.data_files) {
      row_delta->AddDeletes(file);
    }
    for (const auto& file : written.rewritten_delete_files) {
      row_delta->RemoveDeletes(file);
    }

    return row_delta->Commit();
  }

  Result<std::unordered_map<std::string, TargetFile>> ResolveTargets() const {
    ICEBERG_ASSIGN_OR_RAISE(auto scan_builder, table_->NewScan());
    ICEBERG_ASSIGN_OR_RAISE(auto scan, scan_builder->Build());
    ICEBERG_ASSIGN_OR_RAISE(auto tasks, scan->PlanFilesStream());

    std::unordered_map<std::string, TargetFile> targets;
    while (targets.size() < deletes_.size()) {
      ICEBERG_ASSIGN_OR_RAISE(auto task, tasks->Next());
      if (!task.has_value()) break;
      const auto& data_file = (*task)->data_file();
      if (!deletes_.contains(data_file->file_path)) {
        continue;
      }

      ICEBERG_PRECHECK(data_file->partition_spec_id.has_value(),
                       "Data file is missing partition spec ID: {}",
                       data_file->file_path);
      ICEBERG_ASSIGN_OR_RAISE(auto spec, table_->metadata()->PartitionSpecById(
                                             *data_file->partition_spec_id));

      TargetFile target{.data_file = data_file, .spec = std::move(spec)};
      for (const auto& delete_file : (*task)->delete_files()) {
        if (delete_file->content == DataFile::Content::kPositionDeletes) {
          target.position_delete_files.push_back(delete_file);
        }
      }

      targets.emplace(data_file->file_path, std::move(target));
    }

    for (const auto& [path, _] : deletes_) {
      ICEBERG_PRECHECK(targets.contains(path), "Cannot find live data file: {}", path);
    }
    return targets;
  }

  Result<DeleteWriteResult> WriteDeletionVectors(
      const std::unordered_map<std::string, TargetFile>& targets) {
    ICEBERG_ASSIGN_OR_RAISE(auto location_provider, table_->location_provider());
    auto output_path = location_provider->NewDataLocation(
        std::format("position-deletes-{}.puffin", Uuid::GenerateV7().ToString()));
    output_paths_.insert(output_path);

    DeleteLoader loader(table_->io());
    ICEBERG_ASSIGN_OR_RAISE(
        auto writer,
        DVWriter::Make(DVWriterOptions{
            .path = output_path,
            .io = table_->io(),
            .load_previous_deletes = [&targets, &loader](std::string_view path)
                -> Result<std::optional<PositionDeleteIndex>> {
              const auto& previous = targets.at(std::string(path)).position_delete_files;
              if (previous.empty()) return std::nullopt;
              ICEBERG_ASSIGN_OR_RAISE(auto index,
                                      loader.LoadPositionDeletes(previous, path));
              return std::optional<PositionDeleteIndex>(std::move(index));
            },
        }));

    for (const auto& [path, positions] : deletes_) {
      const auto& target = targets.at(path);
      for (int64_t pos : positions) {
        ICEBERG_RETURN_UNEXPECTED(
            writer->Delete(path, pos, target.spec, target.data_file->partition));
      }
    }
    ICEBERG_RETURN_UNEXPECTED(writer->Close());
    return writer->Metadata();
  }

  Result<DeleteWriteResult> WriteParquetDeletes(
      const std::unordered_map<std::string, TargetFile>& targets) {
    ICEBERG_ASSIGN_OR_RAISE(auto location_provider, table_->location_provider());
    ICEBERG_ASSIGN_OR_RAISE(auto schema, table_->schema());

    DeleteWriteResult result;
    result.data_files.reserve(deletes_.size());
    result.referenced_data_files.reserve(deletes_.size());
    auto properties = table_->properties().configs();
    properties[TableProperties::kParquetCompression.key()] =
        table_->properties().Get(TableProperties::kDeleteParquetCompression);
    properties[TableProperties::kParquetCompressionLevel.key()] =
        table_->properties().Get(TableProperties::kDeleteParquetCompressionLevel);
    const auto write_uuid = Uuid::GenerateV7().ToString();
    size_t file_number = 0;

    for (const auto& [path, positions] : deletes_) {
      auto sorted_positions = positions;
      std::ranges::sort(sorted_positions);

      const auto& target = targets.at(path);
      const auto filename =
          std::format("position-deletes-{}-{}.parquet", write_uuid, file_number++);
      auto output_path = location_provider->NewDataLocation(filename);
      output_paths_.insert(output_path);

      ICEBERG_ASSIGN_OR_RAISE(auto writer,
                              PositionDeleteWriter::Make(PositionDeleteWriterOptions{
                                  .path = output_path,
                                  .schema = schema,
                                  .spec = target.spec,
                                  .partition = target.data_file->partition,
                                  .format = FileFormatType::kParquet,
                                  .io = table_->io(),
                                  .properties = properties,
                              }));
      for (int64_t pos : sorted_positions) {
        ICEBERG_RETURN_UNEXPECTED(writer->WriteDelete(path, pos));
      }
      ICEBERG_RETURN_UNEXPECTED(writer->Close());
      ICEBERG_ASSIGN_OR_RAISE(auto metadata, writer->Metadata());
      result.data_files.insert(result.data_files.end(),
                               std::make_move_iterator(metadata.data_files.begin()),
                               std::make_move_iterator(metadata.data_files.end()));
      result.referenced_data_files.push_back(path);
    }
    return result;
  }

  std::shared_ptr<Table> table_;
  std::map<std::string, std::vector<int64_t>, StringLess> deletes_;
  std::set<std::string> output_paths_;
  bool terminal_ = false;
};

PositionDeleteUpdate::PositionDeleteUpdate(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

PositionDeleteUpdate::~PositionDeleteUpdate() = default;

Result<std::unique_ptr<PositionDeleteUpdate>> PositionDeleteUpdate::Make(
    std::shared_ptr<Table> table) {
  ICEBERG_PRECHECK(table != nullptr,
                   "Cannot create position delete update without table");
  return std::unique_ptr<PositionDeleteUpdate>(
      new PositionDeleteUpdate(std::make_unique<Impl>(std::move(table))));
}

PositionDeleteUpdate& PositionDeleteUpdate::Delete(std::string_view data_file_path,
                                                   int64_t pos) {
  ICEBERG_BUILDER_RETURN_IF_ERROR(impl_->Delete(data_file_path, pos));
  return *this;
}

Status PositionDeleteUpdate::Commit() {
  ICEBERG_RETURN_UNEXPECTED(CheckErrors());
  return impl_->Commit();
}

}  // namespace iceberg
