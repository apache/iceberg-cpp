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

#include "iceberg/inspect/snapshots_table.h"

#include <chrono>
#include <cstddef>
#include <memory>
#include <optional>
#include <tuple>
#include <utility>
#include <vector>

#include <nanoarrow/nanoarrow.h>

#include "iceberg/arrow/nanoarrow_status_internal.h"
#include "iceberg/arrow_c_data_util_internal.h"
#include "iceberg/arrow_row_builder_internal.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/schema_internal.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"

namespace iceberg {
namespace {

Status AppendSnapshot(ArrowRowBuilder& builder, const Snapshot& snapshot) {
  ICEBERG_RETURN_UNEXPECTED(
      AppendInt(builder.column(0), std::chrono::duration_cast<std::chrono::microseconds>(
                                       snapshot.timestamp_ms.time_since_epoch())
                                       .count()));
  ICEBERG_RETURN_UNEXPECTED(AppendInt(builder.column(1), snapshot.snapshot_id));

  if (snapshot.parent_snapshot_id.has_value()) {
    ICEBERG_RETURN_UNEXPECTED(AppendInt(builder.column(2), *snapshot.parent_snapshot_id));
  } else {
    ICEBERG_RETURN_UNEXPECTED(AppendNull(builder.column(2)));
  }

  auto operation = snapshot.Operation();
  if (operation.has_value()) {
    ICEBERG_RETURN_UNEXPECTED(AppendString(builder.column(3), *operation));
  } else {
    ICEBERG_RETURN_UNEXPECTED(AppendNull(builder.column(3)));
  }

  ICEBERG_RETURN_UNEXPECTED(AppendString(builder.column(4), snapshot.manifest_list));

  auto summary = snapshot.summary;
  summary.erase(SnapshotSummaryFields::kOperation);
  if (summary.empty()) {
    ICEBERG_RETURN_UNEXPECTED(AppendNull(builder.column(5)));
  } else {
    ICEBERG_RETURN_UNEXPECTED(AppendStringMap(builder.column(5), summary));
  }

  return builder.FinishRow();
}

class SnapshotsTableStream {
 public:
  static Result<std::unique_ptr<SnapshotsTableStream>> Make(
      std::shared_ptr<const TableMetadata> metadata, const iceberg::Schema& schema) {
    ArrowSchema arrow_schema{};
    ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(schema, &arrow_schema));
    return std::unique_ptr<SnapshotsTableStream>(
        new SnapshotsTableStream(std::move(metadata), std::move(arrow_schema)));
  }

  ~SnapshotsTableStream() { std::ignore = Close(); }

  Status Close() {
    metadata_.reset();
    if (arrow_schema_.release != nullptr) {
      ArrowSchemaRelease(&arrow_schema_);
    }
    return {};
  }

  Result<std::optional<ArrowArray>> Next() {
    if (metadata_ == nullptr) [[unlikely]] {
      return InvalidArgument("Cannot read from a closed snapshots table stream");
    }
    const auto& snapshots = metadata_->snapshots;
    if (next_snapshot_ == snapshots.size()) {
      return std::nullopt;
    }

    ICEBERG_ASSIGN_OR_RAISE(auto builder, ArrowRowBuilder::Make(&arrow_schema_));
    while (next_snapshot_ < snapshots.size() &&
           builder.num_rows() < MetadataTable::kBatchSize) {
      const auto& snapshot = snapshots[next_snapshot_++];
      if (snapshot == nullptr) [[unlikely]] {
        continue;
      }
      ICEBERG_RETURN_UNEXPECTED(AppendSnapshot(builder, *snapshot));
    }
    if (builder.num_rows() == 0) {
      return std::nullopt;
    }

    ICEBERG_ASSIGN_OR_RAISE(auto array, std::move(builder).Finish());
    return array;
  }

  Result<ArrowSchema> Schema() {
    if (arrow_schema_.release == nullptr) [[unlikely]] {
      return InvalidArgument("Cannot read schema from a closed snapshots table stream");
    }
    ArrowSchema schema_copy{};
    ICEBERG_NANOARROW_RETURN_UNEXPECTED(
        ArrowSchemaDeepCopy(&arrow_schema_, &schema_copy));
    return schema_copy;
  }

 private:
  SnapshotsTableStream(std::shared_ptr<const TableMetadata> metadata,
                       ArrowSchema arrow_schema)
      : metadata_(std::move(metadata)), arrow_schema_(std::move(arrow_schema)) {}

  std::shared_ptr<const TableMetadata> metadata_;
  ArrowSchema arrow_schema_{};
  size_t next_snapshot_ = 0;
};

}  // namespace

SnapshotsTable::SnapshotsTable(std::shared_ptr<Table> table)
    : MetadataTable(std::move(table)) {}

SnapshotsTable::~SnapshotsTable() = default;

const std::shared_ptr<Schema>& SnapshotsTable::schema() const {
  static const auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "committed_at", timestamp_tz()),
      SchemaField::MakeRequired(2, "snapshot_id", int64()),
      SchemaField::MakeOptional(3, "parent_id", int64()),
      SchemaField::MakeOptional(4, "operation", string()),
      SchemaField::MakeOptional(5, "manifest_list", string()),
      SchemaField::MakeOptional(6, "summary",
                                std::make_shared<iceberg::MapType>(
                                    SchemaField::MakeRequired(7, "key", string()),
                                    SchemaField::MakeRequired(8, "value", string())))});
  return schema;
}

Result<std::unique_ptr<SnapshotsTable>> SnapshotsTable::Make(
    std::shared_ptr<Table> table) {
  ICEBERG_PRECHECK(table != nullptr, "Table cannot be null");
  return std::unique_ptr<SnapshotsTable>(new SnapshotsTable(std::move(table)));
}

Result<ArrowArrayStream> SnapshotsTable::Scan() {
  ICEBERG_ASSIGN_OR_RAISE(
      auto stream, SnapshotsTableStream::Make(source_table()->metadata(), *schema()));
  return MakeArrowArrayStream(std::move(stream));
}

}  // namespace iceberg
