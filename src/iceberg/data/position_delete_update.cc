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
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "iceberg/data/delete_loader.h"
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

struct PreparedDeletes {
  std::vector<std::shared_ptr<DataFile>> added_files;
  std::vector<std::shared_ptr<DataFile>> rewritten_files;
  std::vector<std::string> referenced_data_files;
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
    ICEBERG_RETURN_UNEXPECTED(CleanupOutput());

    ICEBERG_ASSIGN_OR_RAISE(auto snapshot, table_->current_snapshot());
    ICEBERG_ASSIGN_OR_RAISE(auto targets, ResolveTargets());

    auto prepared = table_->metadata()->format_version >= 3
                        ? WriteDeletionVectors(targets)
                        : WriteParquetDeletes(targets);
    if (!prepared.has_value()) {
      return FailAfterCleanup(std::move(prepared.error()));
    }

    auto row_delta_result = table_->NewRowDelta();
    if (!row_delta_result.has_value()) {
      return FailAfterCleanup(std::move(row_delta_result.error()));
    }
    auto row_delta = std::move(row_delta_result.value());
    row_delta->ValidateFromSnapshot(snapshot->snapshot_id)
        .ValidateDataFilesExist(prepared->referenced_data_files)
        .ValidateDeletedFiles();
    for (const auto& file : prepared->added_files) {
      row_delta->AddDeletes(file);
    }
    for (const auto& file : prepared->rewritten_files) {
      row_delta->RemoveDeletes(file);
    }

    auto status = row_delta->Commit();
    if (!status.has_value()) {
      if (status.error().kind == ErrorKind::kCommitStateUnknown) {
        terminal_ = true;
        output_paths_.clear();
      } else {
        return FailAfterCleanup(std::move(status.error()));
      }
      return status;
    }

    terminal_ = true;
    output_paths_.clear();
    return {};
  }

 private:
  Result<std::unordered_map<std::string, TargetFile>> ResolveTargets() const {
    ICEBERG_ASSIGN_OR_RAISE(auto scan_builder, table_->NewScan());
    ICEBERG_ASSIGN_OR_RAISE(auto scan, scan_builder->Build());
    ICEBERG_ASSIGN_OR_RAISE(auto tasks, scan->PlanFiles());

    std::unordered_map<std::string, TargetFile> targets;
    for (const auto& task : tasks) {
      const auto& data_file = task->data_file();
      if (!deletes_.contains(data_file->file_path)) {
        continue;
      }

      ICEBERG_PRECHECK(data_file->partition_spec_id.has_value(),
                       "Data file is missing partition spec ID: {}",
                       data_file->file_path);
      ICEBERG_ASSIGN_OR_RAISE(auto spec, table_->metadata()->PartitionSpecById(
                                             *data_file->partition_spec_id));

      TargetFile target{.data_file = data_file, .spec = std::move(spec)};
      for (const auto& delete_file : task->delete_files()) {
        if (delete_file->content == DataFile::Content::kPositionDeletes) {
          target.position_delete_files.push_back(delete_file);
        }
      }

      auto [_, inserted] = targets.emplace(data_file->file_path, std::move(target));
      ICEBERG_PRECHECK(inserted, "Duplicate live data file path: {}",
                       data_file->file_path);
    }

    for (const auto& [path, _] : deletes_) {
      ICEBERG_PRECHECK(targets.contains(path), "Cannot find live data file: {}", path);
    }
    return targets;
  }

  Result<PreparedDeletes> WriteDeletionVectors(
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
              const auto target = targets.find(std::string(path));
              ICEBERG_CHECK(target != targets.end(),
                            "Missing target data file while loading deletes: {}", path);
              if (target->second.position_delete_files.empty()) {
                return std::nullopt;
              }
              ICEBERG_ASSIGN_OR_RAISE(
                  auto index,
                  loader.LoadPositionDeletes(target->second.position_delete_files, path));
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
    ICEBERG_ASSIGN_OR_RAISE(auto result, writer->Metadata());
    return PreparedDeletes{
        .added_files = std::move(result.data_files),
        .rewritten_files = std::move(result.rewritten_delete_files),
        .referenced_data_files = std::move(result.referenced_data_files),
    };
  }

  Result<PreparedDeletes> WriteParquetDeletes(
      const std::unordered_map<std::string, TargetFile>& targets) {
    ICEBERG_ASSIGN_OR_RAISE(auto location_provider, table_->location_provider());
    ICEBERG_ASSIGN_OR_RAISE(auto schema, table_->schema());

    PreparedDeletes result;
    result.added_files.reserve(deletes_.size());
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
      result.added_files.insert(result.added_files.end(),
                                std::make_move_iterator(metadata.data_files.begin()),
                                std::make_move_iterator(metadata.data_files.end()));
      result.referenced_data_files.push_back(path);
    }
    return result;
  }

  Status CleanupOutput() {
    std::optional<Error> first_error;
    for (auto it = output_paths_.begin(); it != output_paths_.end();) {
      auto status = table_->io()->DeleteFile(*it);
      if (status.has_value()) {
        it = output_paths_.erase(it);
        continue;
      }

      if (!first_error.has_value()) {
        first_error = std::move(status.error());
      } else {
        first_error->message += "; additionally failed to delete output file: ";
        first_error->message += status.error().message;
      }
      ++it;
    }
    if (first_error.has_value()) {
      return std::unexpected(std::move(*first_error));
    }
    return {};
  }

  Status FailAfterCleanup(Error error) {
    auto cleanup_status = CleanupOutput();
    if (!cleanup_status.has_value()) {
      error.message += "; additionally failed to clean output files: ";
      error.message += cleanup_status.error().message;
    }
    return std::unexpected(std::move(error));
  }

  std::shared_ptr<Table> table_;
  std::map<std::string, std::vector<int64_t>, StringLess> deletes_;
  std::set<std::string, StringLess> output_paths_;
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
