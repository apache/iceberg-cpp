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

#include <array>
#include <chrono>
#include <cstddef>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include <nanoarrow/nanoarrow.h>

#include "iceberg/arrow_c_data_util_internal.h"
#include "iceberg/arrow_row_builder_internal.h"
#include "iceberg/inspect/metadata_table_columns_internal.h"
#include "iceberg/nanoarrow_status_internal.h"
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

Status AppendCommittedAt(ArrowArray* column, const Snapshot& snapshot) {
  return AppendInt(column, std::chrono::duration_cast<std::chrono::microseconds>(
                               snapshot.timestamp_ms.time_since_epoch())
                               .count());
}

Status AppendSnapshotId(ArrowArray* column, const Snapshot& snapshot) {
  return AppendInt(column, snapshot.snapshot_id);
}

Status AppendParentId(ArrowArray* column, const Snapshot& snapshot) {
  if (snapshot.parent_snapshot_id.has_value()) {
    return AppendInt(column, *snapshot.parent_snapshot_id);
  }
  return AppendNull(column);
}

Status AppendOperation(ArrowArray* column, const Snapshot& snapshot) {
  auto operation = snapshot.Operation();
  if (operation.has_value()) {
    return AppendString(column, *operation);
  }
  return AppendNull(column);
}

Status AppendManifestList(ArrowArray* column, const Snapshot& snapshot) {
  return AppendString(column, snapshot.manifest_list);
}

Status AppendSummary(ArrowArray* column, const Snapshot& snapshot) {
  if (snapshot.summary.empty()) {
    return AppendNull(column);
  }
  if (snapshot.summary.contains(SnapshotSummaryFields::kOperation)) {
    auto summary = snapshot.summary;
    summary.erase(SnapshotSummaryFields::kOperation);
    return AppendStringMap(column, summary);
  }
  return AppendStringMap(column, snapshot.summary);
}

constexpr std::array<internal::MetadataTableColumn<Snapshot>, 6> kSnapshotColumns{{
    {.field_id = 1, .append = AppendCommittedAt},
    {.field_id = 2, .append = AppendSnapshotId},
    {.field_id = 3, .append = AppendParentId},
    {.field_id = 4, .append = AppendOperation},
    {.field_id = 5, .append = AppendManifestList},
    {.field_id = 6, .append = AppendSummary},
}};

class SnapshotsTableStream {
 public:
  static Result<std::unique_ptr<SnapshotsTableStream>> Make(
      std::shared_ptr<const TableMetadata> metadata,
      const iceberg::Schema& projected_schema,
      std::vector<internal::AppendColumnFn<Snapshot>> append_fns) {
    ArrowSchema arrow_schema{};
    ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(projected_schema, &arrow_schema));
    return std::unique_ptr<SnapshotsTableStream>(new SnapshotsTableStream(
        std::move(metadata), std::move(append_fns), std::move(arrow_schema)));
  }

  ~SnapshotsTableStream() = default;

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
      ICEBERG_RETURN_UNEXPECTED(
          internal::AppendRow<Snapshot>(builder, append_fns_, *snapshot));
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
                       std::vector<internal::AppendColumnFn<Snapshot>> append_fns,
                       ArrowSchema arrow_schema)
      : metadata_(std::move(metadata)),
        append_fns_(std::move(append_fns)),
        arrow_schema_(std::move(arrow_schema)) {}

  std::shared_ptr<const TableMetadata> metadata_;
  std::vector<internal::AppendColumnFn<Snapshot>> append_fns_;
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

Result<ArrowArrayStream> SnapshotsTable::ScanProjection(
    const Schema& projected_schema) const {
  ICEBERG_ASSIGN_OR_RAISE(
      auto append_fns,
      internal::SelectAppendFns<Snapshot>(*schema(), projected_schema, kSnapshotColumns));
  ICEBERG_ASSIGN_OR_RAISE(
      auto stream, SnapshotsTableStream::Make(source_table()->metadata(),
                                              projected_schema, std::move(append_fns)));
  return MakeArrowArrayStream(std::move(stream));
}

}  // namespace iceberg
