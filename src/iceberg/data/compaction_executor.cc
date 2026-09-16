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

#include "iceberg/data/compaction_executor.h"

#include <format>
#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "iceberg/arrow_c_data_guard_internal.h"
#include "iceberg/compaction_planner.h"
#include "iceberg/data/data_writer.h"
#include "iceberg/data/file_scan_task_reader.h"
#include "iceberg/file_format.h"
#include "iceberg/file_io.h"
#include "iceberg/location_provider.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"
#include "iceberg/table_scan.h"
#include "iceberg/update/rewrite_files.h"
#include "iceberg/util/content_file_util.h"
#include "iceberg/util/data_file_set.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/uuid.h"

namespace iceberg {
namespace {

class ArrowArrayStreamGuard {
 public:
  explicit ArrowArrayStreamGuard(ArrowArrayStream* stream) : stream_(stream) {}
  ~ArrowArrayStreamGuard() {
    if (stream_ != nullptr && stream_->release != nullptr) {
      stream_->release(stream_);
    }
  }

 private:
  ArrowArrayStream* stream_;
};

Result<std::optional<ArrowArray>> Next(ArrowArrayStream& stream) {
  ArrowArray batch{};
  if (stream.get_next(&stream, &batch) != 0) {
    const char* detail =
        stream.get_last_error == nullptr ? nullptr : stream.get_last_error(&stream);
    return IOError("Failed to read compaction input: {}",
                   detail == nullptr ? "unknown stream error" : detail);
  }
  if (batch.release == nullptr) {
    return std::nullopt;
  }
  return batch;
}

Result<std::shared_ptr<Schema>> RewriteSchema(const Table& table) {
  ICEBERG_ASSIGN_OR_RAISE(auto table_schema, table.schema());
  if (table.metadata()->format_version < 3) {
    return table_schema;
  }

  std::vector<SchemaField> fields(table_schema->fields().begin(),
                                  table_schema->fields().end());
  fields.push_back(MetadataColumns::kRowId);
  fields.push_back(MetadataColumns::kLastUpdatedSequenceNumber);
  ICEBERG_ASSIGN_OR_RAISE(auto schema,
                          Schema::Make(std::move(fields), table_schema->schema_id(),
                                       table_schema->IdentifierFieldIds()));
  return std::shared_ptr<Schema>(std::move(schema));
}

}  // namespace

class CompactionExecutor::Impl {
 public:
  explicit Impl(std::shared_ptr<Table> table) : table_(std::move(table)) {}

  Status Execute(const CompactionPlan& plan) {
    ICEBERG_PRECHECK(!terminal_, "Compaction executor is no longer usable");
    ICEBERG_RETURN_UNEXPECTED(CleanupOutput());
    ICEBERG_ASSIGN_OR_RAISE(auto inputs, ValidatePlan(plan));

    ICEBERG_RETURN_UNEXPECTED(table_->Refresh());
    ICEBERG_ASSIGN_OR_RAISE(auto snapshot, table_->current_snapshot());
    if (snapshot->snapshot_id != plan.source_snapshot_id) {
      return ValidationFailed(
          "Compaction plan snapshot {} is stale; current snapshot is {}",
          plan.source_snapshot_id, snapshot->snapshot_id);
    }
    ICEBERG_ASSIGN_OR_RAISE(auto table_schema, table_->schema());
    ICEBERG_ASSIGN_OR_RAISE(auto rewrite_schema, RewriteSchema(*table_));
    ICEBERG_ASSIGN_OR_RAISE(
        auto format, FileFormatTypeFromString(
                         table_->properties().Get(TableProperties::kDefaultFileFormat)));
    ICEBERG_PRECHECK(format != FileFormatType::kPuffin,
                     "Puffin is not a data file format");
    ICEBERG_ASSIGN_OR_RAISE(auto location_provider, table_->location_provider());

    std::vector<std::shared_ptr<Schema>> schemas(table_->metadata()->schemas.begin(),
                                                 table_->metadata()->schemas.end());
    ICEBERG_ASSIGN_OR_RAISE(auto reader, FileScanTaskReader::Make({
                                             .io = table_->io(),
                                             .table_schema = std::move(table_schema),
                                             .schemas = std::move(schemas),
                                             .projected_schema = rewrite_schema,
                                             .properties = table_->properties().configs(),
                                         }));

    std::vector<std::shared_ptr<DataFile>> added_files;
    for (size_t group_index = 0; group_index < plan.groups.size(); ++group_index) {
      auto result = RewriteGroup(plan.groups[group_index], group_index, format,
                                 rewrite_schema, *reader, *location_provider);
      if (!result.has_value()) {
        return ReturnAfterCleanup(std::move(result.error()));
      }
      if (result.value() != nullptr) {
        added_files.push_back(std::move(result.value()));
      }
    }

    auto rewrite_result = table_->NewRewriteFiles();
    if (!rewrite_result.has_value()) {
      return ReturnAfterCleanup(std::move(rewrite_result.error()));
    }
    auto rewrite = std::move(rewrite_result.value());
    rewrite->ValidateFromSnapshot(plan.source_snapshot_id)
        .SetDataSequenceNumber(snapshot->sequence_number)
        .Rewrite(inputs.data_files, inputs.delete_files, added_files, {});

    auto status = rewrite->Commit();
    if (!status.has_value()) {
      if (status.error().kind == ErrorKind::kCommitStateUnknown) {
        terminal_ = true;
        output_paths_.clear();
      } else {
        return ReturnAfterCleanup(std::move(status.error()));
      }
      return status;
    }

    output_paths_.clear();
    return {};
  }

  Status Cleanup() { return CleanupOutput(); }

 private:
  struct PlanInputs {
    std::vector<std::shared_ptr<DataFile>> data_files;
    std::vector<std::shared_ptr<DataFile>> delete_files;
  };

  Result<PlanInputs> ValidatePlan(const CompactionPlan& plan) {
    ICEBERG_PRECHECK(plan.source_snapshot_id >= 0,
                     "Compaction plan snapshot ID must be non-negative");
    ICEBERG_PRECHECK(!plan.groups.empty(), "Compaction plan must contain a group");

    DataFileSet data_files;
    DeleteFileSet delete_files;
    for (const auto& group : plan.groups) {
      ICEBERG_PRECHECK(!group.files.empty(), "Compaction group must contain a file");
      for (const auto& compaction_file : group.files) {
        ICEBERG_PRECHECK(compaction_file.scan_task != nullptr,
                         "Compaction file is missing its scan task");
        const auto& data_file = compaction_file.scan_task->data_file();
        ICEBERG_PRECHECK(data_file != nullptr,
                         "Compaction scan task is missing data file");
        ICEBERG_PRECHECK(data_file->content == DataFile::Content::kData,
                         "Compaction input is not a data file: {}", data_file->file_path);
        ICEBERG_PRECHECK(data_file->partition_spec_id == group.partition_spec_id &&
                             data_file->partition == group.partition,
                         "Compaction input does not match its group: {}",
                         data_file->file_path);
        ICEBERG_PRECHECK(data_files.insert(data_file).second,
                         "Duplicate compaction input data file: {}",
                         data_file->file_path);

        for (const auto& delete_file : compaction_file.scan_task->delete_files()) {
          ICEBERG_PRECHECK(delete_file != nullptr,
                           "Compaction scan task contains a null delete file");
          if (delete_file->content != DataFile::Content::kPositionDeletes) {
            continue;
          }
          ICEBERG_ASSIGN_OR_RAISE(auto referenced_file,
                                  ContentFileUtil::ReferencedDataFile(*delete_file));
          if (referenced_file == data_file->file_path) {
            delete_files.insert(delete_file);
          }
        }
      }
    }

    return PlanInputs{
        .data_files =
            std::vector<std::shared_ptr<DataFile>>(data_files.begin(), data_files.end()),
        .delete_files = std::vector<std::shared_ptr<DataFile>>(delete_files.begin(),
                                                               delete_files.end()),
    };
  }

  Result<std::shared_ptr<DataFile>> RewriteGroup(
      const CompactionGroup& group, size_t group_index, FileFormatType format,
      const std::shared_ptr<Schema>& rewrite_schema, FileScanTaskReader& reader,
      LocationProvider& location_provider) {
    ICEBERG_PRECHECK(!group.files.empty(), "Compaction group must contain a file");
    ICEBERG_ASSIGN_OR_RAISE(
        auto spec, table_->metadata()->PartitionSpecById(group.partition_spec_id));

    std::unique_ptr<DataWriter> writer;
    for (const auto& compaction_file : group.files) {
      const auto& data_file = compaction_file.scan_task->data_file();
      FileScanTask task(data_file, compaction_file.scan_task->delete_files());
      ICEBERG_ASSIGN_OR_RAISE(auto stream, reader.Open(task));
      ArrowArrayStreamGuard stream_guard(&stream);
      while (true) {
        ICEBERG_ASSIGN_OR_RAISE(auto batch, Next(stream));
        if (!batch.has_value()) {
          break;
        }

        internal::ArrowArrayGuard batch_guard(&batch.value());
        if (batch->length == 0) {
          continue;
        }
        if (writer == nullptr) {
          const auto filename =
              std::format("compacted-{}-{}.{}", Uuid::GenerateV7().ToString(),
                          group_index, ToString(format));
          ICEBERG_ASSIGN_OR_RAISE(
              auto output_path,
              location_provider.NewDataLocation(*spec, group.partition, filename));
          output_paths_.insert(output_path);
          ICEBERG_ASSIGN_OR_RAISE(writer,
                                  DataWriter::Make({
                                      .path = output_path,
                                      .schema = rewrite_schema,
                                      .spec = spec,
                                      .partition = group.partition,
                                      .format = format,
                                      .io = table_->io(),
                                      .properties = table_->properties().configs(),
                                  }));
        }
        batch_guard.Release();
        ICEBERG_RETURN_UNEXPECTED(writer->Write(&batch.value()));
      }
    }

    if (writer != nullptr) {
      ICEBERG_RETURN_UNEXPECTED(writer->Close());
      ICEBERG_ASSIGN_OR_RAISE(auto metadata, writer->Metadata());
      ICEBERG_PRECHECK(metadata.data_files.size() == 1,
                       "Compaction writer produced {} data files",
                       metadata.data_files.size());
      return std::move(metadata.data_files.front());
    }
    return nullptr;
  }

  Status ReturnAfterCleanup(Error error) {
    auto cleanup_status = CleanupOutput();
    if (!cleanup_status.has_value()) {
      error.message += "; additionally failed to clean output files: ";
      error.message += cleanup_status.error().message;
    }
    return std::unexpected<Error>(std::move(error));
  }

  Status CleanupOutput() {
    std::optional<Error> first_error;
    for (auto iter = output_paths_.begin(); iter != output_paths_.end();) {
      auto status = table_->io()->DeleteFile(*iter);
      if (status.has_value()) {
        iter = output_paths_.erase(iter);
        continue;
      }
      if (!first_error.has_value()) {
        first_error = std::move(status.error());
      } else {
        first_error->message += "; additionally failed to delete output file: ";
        first_error->message += status.error().message;
      }
      ++iter;
    }
    if (first_error.has_value()) {
      return std::unexpected<Error>(std::move(first_error.value()));
    }
    return {};
  }

  std::shared_ptr<Table> table_;
  std::unordered_set<std::string> output_paths_;
  bool terminal_ = false;
};

CompactionExecutor::CompactionExecutor(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

CompactionExecutor::~CompactionExecutor() = default;

Result<std::unique_ptr<CompactionExecutor>> CompactionExecutor::Make(
    std::shared_ptr<Table> table) {
  ICEBERG_PRECHECK(table != nullptr, "Cannot create compaction executor without table");
  return std::unique_ptr<CompactionExecutor>(
      new CompactionExecutor(std::make_unique<Impl>(std::move(table))));
}

Status CompactionExecutor::Execute(const CompactionPlan& plan) {
  return impl_->Execute(plan);
}

Status CompactionExecutor::Cleanup() { return impl_->Cleanup(); }

}  // namespace iceberg
