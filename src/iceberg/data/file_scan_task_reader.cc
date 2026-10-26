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
#include <numeric>
#include <optional>
#include <utility>
#include <vector>

#include "iceberg/arrow_c_data_guard_internal.h"
#include "iceberg/arrow_c_data_util_internal.h"
#include "iceberg/data/delete_filter.h"
#include "iceberg/expression/binder.h"
#include "iceberg/expression/evaluator.h"
#include "iceberg/file_reader.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/row/arrow_array_wrapper.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
#include "iceberg/table_scan.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

ReaderOptions MakeReaderOptions(const FileScanTask& task, std::shared_ptr<FileIO> io,
                                std::shared_ptr<Schema> projection,
                                std::shared_ptr<Expression> filter,
                                std::shared_ptr<NameMapping> name_mapping,
                                ReaderProperties properties,
                                std::optional<int64_t> first_row_id,
                                std::optional<int64_t> data_sequence_number) {
  const auto& data_file = *task.data_file();
  return ReaderOptions{
      .path = data_file.file_path,
      .length = static_cast<size_t>(data_file.file_size_in_bytes),
      .split = task.is_split() ? std::make_optional(
                                     Split{.offset = static_cast<size_t>(task.start()),
                                           .length = static_cast<size_t>(task.length())})
                               : std::nullopt,
      .io = std::move(io),
      .projection = std::move(projection),
      .filter = std::move(filter),
      .name_mapping = std::move(name_mapping),
      .first_row_id = first_row_id,
      .data_sequence_number = data_sequence_number,
      .properties = std::move(properties),
  };
}

// Returns the top-level field with the given id, or nullptr when the id belongs
// to a nested field or is absent.
const SchemaField* FindTopLevelField(const Schema& schema, int32_t field_id) {
  for (const auto& field : schema.fields()) {
    if (field.field_id() == field_id) {
      return &field;
    }
  }
  return nullptr;
}

// Streams one task's rows through optional delete filtering, optional residual
// evaluation, and projection to the caller's schema. Tasks with deletes and
// tasks whose residual filter is enforced both read a possibly wider schema
// (delete keys, row positions, residual columns) and project it back.
class TaskStreamSource {
 public:
  // `delete_filter` is null for tasks without delete files; `residual_evaluator`
  // is null when residual enforcement is disabled or not needed for the task.
  TaskStreamSource(std::unique_ptr<Reader> reader,
                   std::unique_ptr<DeleteFilter> delete_filter,
                   std::unique_ptr<Evaluator> residual_evaluator,
                   std::shared_ptr<::iceberg::Schema> required_schema,
                   std::shared_ptr<::iceberg::Schema> projected_schema,
                   ProjectionContext projection_context)
      : reader_(std::move(reader)),
        delete_filter_(std::move(delete_filter)),
        residual_evaluator_(std::move(residual_evaluator)),
        projected_schema_(std::move(projected_schema)),
        project_all_rows_(required_schema->SameSchema(*projected_schema_)),
        projection_context_(std::move(projection_context)) {}

  ~TaskStreamSource() {
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

      // Rows still selected. While `filtered` is false, every row of the batch
      // is still a candidate and `selection` has not been materialized.
      std::vector<int32_t> selection;
      bool filtered = false;

      if (delete_filter_ != nullptr) {
        ICEBERG_ASSIGN_OR_RAISE(auto alive, delete_filter_->ComputeAliveRows(
                                                input_arrow_schema, input_batch));
        if (alive.empty()) {
          continue;
        }
        filtered = alive.alive_count() != input_batch.length;
        if (filtered) {
          selection = std::move(alive.indices);
        }
      }

      if (residual_evaluator_ != nullptr) {
        const int64_t candidate_count =
            filtered ? static_cast<int64_t>(selection.size()) : input_batch.length;
        ICEBERG_ASSIGN_OR_RAISE(
            auto kept, KeepResidualMatches(input_arrow_schema, input_batch, selection,
                                           /*all_rows_selected=*/!filtered));
        filtered = filtered || kept.size() != candidate_count;
        if (filtered) {
          selection = std::move(kept);
          if (selection.empty()) {
            continue;
          }
        }
      }

      if (!filtered && project_all_rows_) {
        // Transfer ownership to the stream result; the local guard must not release it.
        ArrowArray output_batch = input_batch;
        input_batch.release = nullptr;
        return output_batch;
      }

      if (!filtered) {
        // Project a fully selected batch, e.g. because columns were pruned or
        // reordered for evaluation.
        selection.resize(static_cast<size_t>(input_batch.length));
        std::iota(selection.begin(), selection.end(), int32_t{0});
      }
      return ProjectBatch(&input_batch, selection, projection_context_);
    }
  }

  Result<ArrowSchema> Schema() {
    ArrowSchema schema;
    ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(*projected_schema_, &schema));
    return schema;
  }

 private:
  // Returns the candidate rows that match the residual filter: either the
  // previously selected rows, or every row when no filter has removed a row yet.
  Result<std::vector<int32_t>> KeepResidualMatches(const ArrowSchema& input_arrow_schema,
                                                   const ArrowArray& input_batch,
                                                   const std::vector<int32_t>& selection,
                                                   bool all_rows_selected) {
    ICEBERG_PRECHECK(std::in_range<int32_t>(input_batch.length),
                     "Batch length {} exceeds int32_t row index capacity",
                     input_batch.length);
    ICEBERG_ASSIGN_OR_RAISE(auto row,
                            ArrowArrayStructLike::Make(input_arrow_schema, input_batch));
    std::vector<int32_t> kept;
    auto keep = [&](int64_t index) -> Status {
      ICEBERG_RETURN_UNEXPECTED(row->Reset(index));
      ICEBERG_ASSIGN_OR_RAISE(bool matches, residual_evaluator_->Evaluate(*row));
      if (matches) {
        kept.push_back(static_cast<int32_t>(index));
      }
      return {};
    };
    if (all_rows_selected) {
      kept.reserve(static_cast<size_t>(input_batch.length));
      for (int64_t index = 0; index < input_batch.length; ++index) {
        ICEBERG_RETURN_UNEXPECTED(keep(index));
      }
    } else {
      kept.reserve(selection.size());
      for (int32_t index : selection) {
        ICEBERG_RETURN_UNEXPECTED(keep(index));
      }
    }
    return kept;
  }

  std::unique_ptr<Reader> reader_;
  std::unique_ptr<DeleteFilter> delete_filter_;
  std::unique_ptr<Evaluator> residual_evaluator_;
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

  // Columns that must be added to the read schema so the task's residual filter
  // can be evaluated, or nullopt when the residual must not be enforced. An
  // empty vector means every referenced column is already projected.
  Result<std::optional<std::vector<SchemaField>>> ResidualReadColumns(
      const FileScanTask& task) const {
    if (!apply_residuals_) {
      return std::nullopt;
    }
    const auto& residual = task.residual_filter();
    if (residual == nullptr || residual->op() == Expression::Operation::kTrue) {
      return std::nullopt;
    }

    // Residuals are unbound and reference the planning scan's schema: bind them
    // against the table schema with the scan's case sensitivity.
    ICEBERG_ASSIGN_OR_RAISE(auto bound,
                            Binder::Bind(*table_schema_, residual, case_sensitive_));
    ICEBERG_ASSIGN_OR_RAISE(auto field_id_set,
                            ReferenceVisitor::GetReferencedFieldIds(bound));
    std::vector<int32_t> field_ids(field_id_set.begin(), field_id_set.end());
    std::sort(field_ids.begin(), field_ids.end());

    std::vector<SchemaField> read_columns;
    for (int32_t field_id : field_ids) {
      if (FindTopLevelField(*projected_schema_, field_id) != nullptr) {
        continue;
      }
      const SchemaField* field = FindTopLevelField(*table_schema_, field_id);
      if (field == nullptr) {
        return InvalidArgument(
            "Residual filter references field id {}, which is not a top-level "
            "column; nested residual references are not supported",
            field_id);
      }
      read_columns.push_back(*field);
    }
    return read_columns;
  }

  Result<ArrowArrayStream> Open(const FileScanTask& task) {
    const auto& data_file = task.data_file();
    ICEBERG_PRECHECK(data_file != nullptr, "Data file must not be null");
    ICEBERG_PRECHECK(data_file->file_size_in_bytes >= 0,
                     "Data file size must not be negative: {}",
                     data_file->file_size_in_bytes);
    ICEBERG_PRECHECK(std::in_range<size_t>(data_file->file_size_in_bytes),
                     "Data file size cannot be represented as size_t: {}",
                     data_file->file_size_in_bytes);
    if (task.is_split()) {
      ICEBERG_PRECHECK(task.start() >= 0 &&
                           task.start() <= data_file->file_size_in_bytes &&
                           task.length() > 0 &&
                           task.length() <= data_file->file_size_in_bytes - task.start(),
                       "Task range is outside file {} of size {} (start={}, length={})",
                       data_file->file_path, data_file->file_size_in_bytes, task.start(),
                       task.length());
      // The validated endpoints are bounded by the size_t-representable file size.
      if (data_file->file_format != FileFormatType::kParquet) {
        return NotSupported(
            "Reading a partial FileScanTask requires Parquet: {} "
            "(start={}, length={})",
            data_file->file_path, task.start(), task.length());
      }
    }

    ICEBERG_ASSIGN_OR_RAISE(auto residual_columns, ResidualReadColumns(task));

    if (task.delete_files().empty() && !residual_columns.has_value()) {
      auto options = MakeReaderOptions(
          task, io_, projected_schema_, task.residual_filter(), name_mapping_,
          properties_, data_file->first_row_id, data_file->data_sequence_number);
      ICEBERG_ASSIGN_OR_RAISE(
          auto reader, ReaderFactoryRegistry::Open(data_file->file_format, options));
      return MakeArrowArrayStream(std::move(reader));
    }

    // The read schema covers the projection plus any columns needed to enforce
    // the residual filter; the stream projects back to the caller's schema.
    std::shared_ptr<Schema> requested_schema = projected_schema_;
    if (residual_columns.has_value() && !residual_columns->empty()) {
      std::vector<SchemaField> fields(projected_schema_->fields().begin(),
                                      projected_schema_->fields().end());
      fields.insert(fields.end(), residual_columns->begin(), residual_columns->end());
      requested_schema = std::make_shared<Schema>(std::move(fields));
    }

    std::unique_ptr<DeleteFilter> delete_filter;
    std::shared_ptr<Schema> required_schema = requested_schema;
    if (!task.delete_files().empty()) {
      const bool has_position_deletes =
          std::any_of(task.delete_files().begin(), task.delete_files().end(),
                      [](const std::shared_ptr<DataFile>& f) {
                        return f->content == DataFile::Content::kPositionDeletes;
                      });

      ICEBERG_ASSIGN_OR_RAISE(
          delete_filter,
          DeleteFilter::Make(data_file->file_path, task.delete_files(), requested_schema,
                             io_, field_lookup_, has_position_deletes, delete_counter_));
      required_schema = delete_filter->RequiredSchema();
    }

    auto options = MakeReaderOptions(task, io_, required_schema, task.residual_filter(),
                                     name_mapping_, properties_, data_file->first_row_id,
                                     data_file->data_sequence_number);
    ICEBERG_ASSIGN_OR_RAISE(auto reader,
                            ReaderFactoryRegistry::Open(data_file->file_format, options));

    auto project_batch_function = ProjectionContext::ResolveProjectBatchFunction();
    // ProjectionContext borrows schemas that are kept in TaskStreamSource.
    ICEBERG_ASSIGN_OR_RAISE(auto projection_context,
                            ProjectionContext::Make(*required_schema, *projected_schema_,
                                                    project_batch_function));

    std::unique_ptr<Evaluator> residual_evaluator;
    if (residual_columns.has_value()) {
      // Bind to the read schema: its field positions describe every batch.
      ICEBERG_ASSIGN_OR_RAISE(
          residual_evaluator,
          Evaluator::Make(*required_schema, task.residual_filter(), case_sensitive_));
    }

    auto task_source = std::make_unique<TaskStreamSource>(
        std::move(reader), std::move(delete_filter), std::move(residual_evaluator),
        std::move(required_schema), projected_schema_, std::move(projection_context));
    return MakeArrowArrayStream(std::move(task_source));
  }

 private:
  Impl(Options options, DeleteFilter::FieldLookup field_lookup,
       std::shared_ptr<DeleteCounter> delete_counter)
      : io_(std::move(options.io)),
        table_schema_(std::move(options.table_schema)),
        schemas_(std::move(options.schemas)),
        projected_schema_(std::move(options.projected_schema)),
        name_mapping_(std::move(options.name_mapping)),
        properties_(ReaderProperties::FromMap(options.properties)),
        case_sensitive_(options.case_sensitive),
        apply_residuals_(options.apply_residuals),
        field_lookup_(std::move(field_lookup)),
        delete_counter_(std::move(delete_counter)) {}

  std::shared_ptr<FileIO> io_;
  std::shared_ptr<Schema> table_schema_;
  std::vector<std::shared_ptr<Schema>> schemas_;
  std::shared_ptr<Schema> projected_schema_;
  std::shared_ptr<NameMapping> name_mapping_;
  ReaderProperties properties_;
  bool case_sensitive_ = true;
  bool apply_residuals_ = false;
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
