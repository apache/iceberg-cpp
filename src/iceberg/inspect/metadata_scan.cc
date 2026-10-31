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

#include "iceberg/inspect/metadata_scan.h"

#include <optional>
#include <utility>

#include "iceberg/arrow_c_data_util_internal.h"
#include "iceberg/inspect/metadata_scan_task_reader.h"
#include "iceberg/inspect/metadata_table.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
#include "iceberg/table.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/stream.h"

namespace iceberg {
namespace {

class TaskArrowStreamSource {
 public:
  TaskArrowStreamSource(std::vector<std::shared_ptr<MetadataScanTask>> tasks,
                        std::unique_ptr<MetadataScanTaskReader> reader,
                        std::shared_ptr<Schema> schema)
      : tasks_(std::move(tasks)),
        reader_(std::move(reader)),
        schema_(std::move(schema)) {}

  ~TaskArrowStreamSource() { std::ignore = Close(); }

  Status Close() {
    if (current_.release != nullptr) {
      current_.release(&current_);
    }
    return {};
  }

  Result<ArrowSchema> Schema() {
    ArrowSchema schema{};
    ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(*schema_, &schema));
    return schema;
  }

  Result<std::optional<ArrowArray>> Next() {
    if (error_) {
      return iceberg::unexpected(*error_);
    }
    auto result = NextBatch();
    if (!result) {
      error_ = result.error();
    }
    return result;
  }

  Result<std::optional<ArrowArray>> NextBatch() {
    while (true) {
      if (current_.release == nullptr) {
        if (next_task_ == tasks_.size()) {
          return std::nullopt;
        }
        ICEBERG_ASSIGN_OR_RAISE(current_, reader_->Open(*tasks_[next_task_++]));
      }

      ArrowArray array{};
      const int error = current_.get_next(&current_, &array);
      if (error != 0) {
        std::string message = current_.get_last_error != nullptr &&
                                      current_.get_last_error(&current_) != nullptr
                                  ? current_.get_last_error(&current_)
                                  : "Metadata task stream failed";
        current_.release(&current_);
        return IOError("{}", message);
      }
      if (array.release != nullptr) {
        return array;
      }
      current_.release(&current_);
    }
  }

  const std::vector<std::shared_ptr<MetadataScanTask>> tasks_;
  const std::unique_ptr<MetadataScanTaskReader> reader_;
  const std::shared_ptr<iceberg::Schema> schema_;
  size_t next_task_ = 0;
  ArrowArrayStream current_{};
  std::optional<Error> error_;
};

}  // namespace

MetadataScanBuilder::MetadataScanBuilder(const MetadataTable& table)
    : table_(&table),
      context_{.kind = table.kind(),
               .table_metadata = table.source_table()->metadata(),
               .schema = table.schema(),
               .projected_schema = table.schema()} {}

MetadataScanBuilder::~MetadataScanBuilder() = default;

Result<std::unique_ptr<MetadataScanBuilder>> MetadataScanBuilder::Make(
    const MetadataTable& table) {
  ICEBERG_PRECHECK(table.source_table() != nullptr, "Source table cannot be null");
  ICEBERG_PRECHECK(table.schema() != nullptr, "Metadata table schema cannot be null");
  return std::unique_ptr<MetadataScanBuilder>(new MetadataScanBuilder(table));
}

MetadataScanBuilder& MetadataScanBuilder::Select(
    const std::vector<std::string>& columns) {
  auto projected = context_.schema->Select(columns, context_.case_sensitive);
  if (projected) {
    context_.projected_schema = std::move(projected).value();
  } else {
    AddError(projected.error());
  }
  return *this;
}

MetadataScanBuilder& MetadataScanBuilder::Project(std::shared_ptr<Schema> schema) {
  context_.projected_schema = std::move(schema);
  return *this;
}

MetadataScanBuilder& MetadataScanBuilder::CaseSensitive(bool value) {
  context_.case_sensitive = value;
  return *this;
}

MetadataScanBuilder& MetadataScanBuilder::UseSnapshot(int64_t snapshot_id) {
  ICEBERG_BUILDER_CHECK(
      !context_.snapshot_id.has_value() && !context_.as_of_time_in_millis.has_value(),
      "Cannot override snapshot, a snapshot or timestamp is already set");
  context_.snapshot_id = snapshot_id;
  return *this;
}

MetadataScanBuilder& MetadataScanBuilder::UseRef(const std::string& ref) {
  ICEBERG_BUILDER_CHECK(context_.ref_name.empty(),
                        "Cannot override ref, already set ref={}", context_.ref_name);
  ICEBERG_BUILDER_CHECK(!ref.empty(), "Ref name cannot be empty");
  context_.ref_name = ref;
  return *this;
}

MetadataScanBuilder& MetadataScanBuilder::AsOfTime(int64_t as_of_time_in_millis) {
  ICEBERG_BUILDER_CHECK(
      !context_.snapshot_id.has_value() && !context_.as_of_time_in_millis.has_value(),
      "Cannot override snapshot, a snapshot or timestamp is already set");
  context_.as_of_time_in_millis = as_of_time_in_millis;
  return *this;
}

Result<std::unique_ptr<MetadataScan>> MetadataScanBuilder::Build() {
  ICEBERG_RETURN_UNEXPECTED(CheckErrors());
  ICEBERG_PRECHECK(context_.projected_schema != nullptr,
                   "Projected schema cannot be null");
  return table_->MakeScan(context_);
}

MetadataScan::MetadataScan(internal::MetadataScanContext context)
    : context_(std::move(context)) {}

MetadataScan::~MetadataScan() = default;

Result<std::vector<std::shared_ptr<MetadataScanTask>>> MetadataScan::PlanTasks() const {
  ICEBERG_ASSIGN_OR_RAISE(auto stream, PlanTasksStream());
  return stream->ToVector();
}

Result<ArrowArrayStream> MetadataScan::ToArrowArrayStream() const {
  ICEBERG_ASSIGN_OR_RAISE(auto tasks, PlanTasks());
  ICEBERG_ASSIGN_OR_RAISE(auto reader, MetadataScanTaskReader::Make({}));
  return MakeArrowArrayStream(std::make_unique<TaskArrowStreamSource>(
      std::move(tasks), std::move(reader), context_.projected_schema));
}

}  // namespace iceberg
