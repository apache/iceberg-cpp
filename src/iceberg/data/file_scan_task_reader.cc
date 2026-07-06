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

#include "iceberg/data/file_scan_task_reader.h"

#include <algorithm>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "iceberg/arrow_c_data_guard_internal.h"
#include "iceberg/arrow_c_data_util_internal.h"
#include "iceberg/data/delete_filter.h"
#include "iceberg/file_reader.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
#include "iceberg/table_scan.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

ReaderOptions MakeReaderOptions(const DataFile& data_file, std::shared_ptr<FileIO> io,
                                std::shared_ptr<Schema> projection,
                                std::shared_ptr<Expression> filter,
                                std::shared_ptr<NameMapping> name_mapping,
                                ReaderProperties properties,
                                std::optional<int64_t> first_row_id,
                                std::optional<int64_t> data_sequence_number) {
  return ReaderOptions{
      .path = data_file.file_path,
      .length = static_cast<size_t>(data_file.file_size_in_bytes),
      .io = std::move(io),
      .projection = std::move(projection),
      .filter = std::move(filter),
      .name_mapping = std::move(name_mapping),
      .first_row_id = first_row_id,
      .data_sequence_number = data_sequence_number,
      .properties = std::move(properties),
  };
}

class MergeOnReadStreamSource {
 public:
  MergeOnReadStreamSource(std::unique_ptr<Reader> reader,
                          std::unique_ptr<DeleteFilter> delete_filter,
                          std::shared_ptr<::iceberg::Schema> required_schema,
                          std::shared_ptr<::iceberg::Schema> projected_schema,
                          ProjectionContext projection_context)
      : reader_(std::move(reader)),
        delete_filter_(std::move(delete_filter)),
        required_schema_(std::move(required_schema)),
        projected_schema_(std::move(projected_schema)),
        project_all_rows_(required_schema_->SameSchema(*projected_schema_)),
        projection_context_(std::move(projection_context)) {}

  ~MergeOnReadStreamSource() {
    if (cached_schema_.has_value() && cached_schema_->release != nullptr) {
      cached_schema_->release(&cached_schema_.value());
    }
  }

  Status Close() {
    if (reader_ == nullptr) {
      return {};
    }
    return reader_->Close();
  }

  Result<std::optional<ArrowArray>> Next() {
    if (!cached_schema_.has_value()) {
      // File readers expose one stable Arrow schema for every batch in the stream.
      ICEBERG_ASSIGN_OR_RAISE(cached_schema_, reader_->Schema());
    }
    ArrowSchema& input_arrow_schema = cached_schema_.value();

    while (true) {
      ICEBERG_ASSIGN_OR_RAISE(auto next_batch, reader_->Next());
      if (!next_batch.has_value()) {
        return std::nullopt;
      }

      ArrowArray input_batch = std::move(next_batch.value());
      internal::ArrowArrayGuard input_batch_guard(&input_batch);

      ICEBERG_ASSIGN_OR_RAISE(
          auto alive, delete_filter_->ComputeAliveRows(input_arrow_schema, input_batch));
      if (alive.empty()) {
        continue;
      }

      if (alive.alive_count() == input_batch.length && project_all_rows_) {
        // Transfer ownership to the stream result; the local guard must not release it.
        ArrowArray output_batch = input_batch;
        input_batch.release = nullptr;
        return output_batch;
      }

      return ProjectBatch(&input_batch, alive.indices, projection_context_);
    }
  }

  Result<ArrowSchema> Schema() {
    ArrowSchema schema;
    ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(*projected_schema_, &schema));
    return schema;
  }

 private:
  std::unique_ptr<Reader> reader_;
  std::unique_ptr<DeleteFilter> delete_filter_;
  std::shared_ptr<::iceberg::Schema> required_schema_;
  std::shared_ptr<::iceberg::Schema> projected_schema_;
  bool project_all_rows_ = false;
  ProjectionContext projection_context_;
  std::optional<ArrowSchema> cached_schema_;
};

}  // namespace

class FileScanTaskReader::Impl {
 public:
  static Result<std::unique_ptr<Impl>> Make(Options options) {
    ICEBERG_PRECHECK(options.io != nullptr, "FileIO must not be null");
    ICEBERG_PRECHECK(options.table_schema != nullptr, "Table schema must not be null");
    ICEBERG_PRECHECK(options.projected_schema != nullptr,
                     "Projected schema must not be null");
    for (const auto& schema : options.schemas) {
      ICEBERG_PRECHECK(schema != nullptr, "Schema list must not contain null schemas");
    }

    ICEBERG_ASSIGN_OR_RAISE(
        auto field_lookup,
        DeleteFilter::MakeFieldLookup(options.table_schema, options.schemas));
    auto delete_counter = std::make_shared<DeleteCounter>();

    return std::unique_ptr<Impl>(
        new Impl(std::move(options), std::move(field_lookup), std::move(delete_counter)));
  }

  Result<ArrowArrayStream> Open(const FileScanTask& task) {
    const auto& data_file = task.data_file();
    ICEBERG_PRECHECK(data_file != nullptr, "Data file must not be null");
    ICEBERG_PRECHECK(data_file->file_size_in_bytes >= 0,
                     "Data file size must not be negative: {}",
                     data_file->file_size_in_bytes);

    if (task.delete_files().empty()) {
      auto options = MakeReaderOptions(
          *data_file, io_, projected_schema_, task.residual_filter(), name_mapping_,
          properties_, data_file->first_row_id, data_file->data_sequence_number);
      ICEBERG_ASSIGN_OR_RAISE(
          auto reader, ReaderFactoryRegistry::Open(data_file->file_format, options));
      return MakeArrowArrayStream(std::move(reader));
    }

    const bool has_position_deletes =
        std::any_of(task.delete_files().begin(), task.delete_files().end(),
                    [](const std::shared_ptr<DataFile>& f) {
                      return f->content == DataFile::Content::kPositionDeletes;
                    });

    ICEBERG_ASSIGN_OR_RAISE(
        auto delete_filter,
        DeleteFilter::Make(data_file->file_path, task.delete_files(), projected_schema_,
                           io_, field_lookup_, has_position_deletes, delete_counter_));

    auto required_schema = delete_filter->RequiredSchema();
    auto project_batch_function = ProjectionContext::ResolveProjectBatchFunction();
    // ProjectionContext borrows schemas that are kept in MergeOnReadStreamSource.
    ICEBERG_ASSIGN_OR_RAISE(auto projection_context,
                            ProjectionContext::Make(*required_schema, *projected_schema_,
                                                    project_batch_function));

    auto options = MakeReaderOptions(
        *data_file, io_, required_schema, task.residual_filter(), name_mapping_,
        properties_, data_file->first_row_id, data_file->data_sequence_number);
    ICEBERG_ASSIGN_OR_RAISE(auto reader,
                            ReaderFactoryRegistry::Open(data_file->file_format, options));

    auto mor_reader = std::make_unique<MergeOnReadStreamSource>(
        std::move(reader), std::move(delete_filter), std::move(required_schema),
        projected_schema_, std::move(projection_context));
    return MakeArrowArrayStream(std::move(mor_reader));
  }

 private:
  Impl(Options options, DeleteFilter::FieldLookup field_lookup,
       std::shared_ptr<DeleteCounter> delete_counter)
      : io_(std::move(options.io)),
        schemas_(std::move(options.schemas)),
        projected_schema_(std::move(options.projected_schema)),
        name_mapping_(std::move(options.name_mapping)),
        properties_(ReaderProperties::FromMap(options.properties)),
        field_lookup_(std::move(field_lookup)),
        delete_counter_(std::move(delete_counter)) {}

  std::shared_ptr<FileIO> io_;
  std::vector<std::shared_ptr<Schema>> schemas_;
  std::shared_ptr<Schema> projected_schema_;
  std::shared_ptr<NameMapping> name_mapping_;
  ReaderProperties properties_;
  DeleteFilter::FieldLookup field_lookup_;
  std::shared_ptr<DeleteCounter> delete_counter_;
};

Result<std::unique_ptr<FileScanTaskReader>> FileScanTaskReader::Make(Options options) {
  ICEBERG_ASSIGN_OR_RAISE(auto impl, Impl::Make(std::move(options)));
  return std::unique_ptr<FileScanTaskReader>(new FileScanTaskReader(std::move(impl)));
}

FileScanTaskReader::FileScanTaskReader(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

FileScanTaskReader::~FileScanTaskReader() = default;

Result<ArrowArrayStream> FileScanTaskReader::Open(const FileScanTask& task) {
  return impl_->Open(task);
}

}  // namespace iceberg
