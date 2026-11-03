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
#include <memory>
#include <utility>
#include <vector>

#include "iceberg/arrow_row_builder_internal.h"
#include "iceberg/inspect/metadata_scan_task.h"
#include "iceberg/inspect/metadata_scan_util_internal.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"

namespace iceberg {
namespace {

constexpr std::array<internal::MetadataTableColumn<Snapshot>, 6> kSnapshotColumns = {
    {{.field_id = 1,
      .append =
          [](ArrowArray* column, const Snapshot& row) {
            return AppendInt(column, UnixMsFromTimePointMs(row.timestamp_ms) * 1000);
          }},
     {.field_id = 2,
      .append = [](ArrowArray* column,
                   const Snapshot& row) { return AppendInt(column, row.snapshot_id); }},
     {.field_id = 3,
      .append =
          [](ArrowArray* column, const Snapshot& row) {
            return row.parent_snapshot_id ? AppendInt(column, *row.parent_snapshot_id)
                                          : AppendNull(column);
          }},
     {.field_id = 4,
      .append =
          [](ArrowArray* column, const Snapshot& row) {
            auto operation = row.Operation();
            return operation ? AppendString(column, *operation) : AppendNull(column);
          }},
     {.field_id = 5,
      .append =
          [](ArrowArray* column, const Snapshot& row) {
            return row.manifest_list.empty() ? AppendNull(column)
                                             : AppendString(column, row.manifest_list);
          }},
     {.field_id = 6, .append = [](ArrowArray* column, const Snapshot& row) {
        if (row.summary.empty()) {
          return AppendNull(column);
        }
        auto summary = row.summary;
        summary.erase(SnapshotSummaryFields::kOperation);
        return AppendStringMap(column, summary);
      }}}};

}  // namespace

// SnapshotsTable implementation
SnapshotsTable::SnapshotsTable(std::shared_ptr<const Table> table)
    : MetadataTable(std::move(table)) {}

SnapshotsTable::~SnapshotsTable() = default;

const std::shared_ptr<Schema>& SnapshotsTable::schema() const {
  return CanonicalSchema();
}

const std::shared_ptr<Schema>& SnapshotsTable::CanonicalSchema() {
  static const auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "committed_at", timestamp_tz()),
      SchemaField::MakeRequired(2, "snapshot_id", int64()),
      SchemaField::MakeOptional(3, "parent_id", int64()),
      SchemaField::MakeOptional(4, "operation", string()),
      SchemaField::MakeOptional(5, "manifest_list", string()),
      SchemaField::MakeOptional(
          6, "summary",
          std::make_shared<MapType>(SchemaField::MakeRequired(7, "key", string()),
                                    SchemaField::MakeRequired(8, "value", string())))});
  return schema;
}

Result<std::unique_ptr<SnapshotsTable>> SnapshotsTable::Make(
    std::shared_ptr<const Table> table) {
  ICEBERG_PRECHECK(table != nullptr, "Table cannot be null");
  return std::unique_ptr<SnapshotsTable>(new SnapshotsTable(std::move(table)));
}

Result<std::unique_ptr<MetadataScan>> SnapshotsTable::MakeScan(
    internal::MetadataScanContext context) const {
  return SnapshotsScan::Make(std::move(context));
}

Result<ArrowArrayStream> SnapshotsTable::ReadRows(
    std::shared_ptr<const TableMetadata> metadata,
    const std::shared_ptr<Schema>& projected_schema, int64_t batch_size) {
  std::vector<size_t> snapshot_indices;
  for (size_t i = 0; i < metadata->snapshots.size(); ++i) {
    if (metadata->snapshots[i]) {
      snapshot_indices.push_back(i);
    }
  }
  const size_t num_rows = snapshot_indices.size();
  return internal::MakeMetadataBatchStream<Snapshot>(
      CanonicalSchema(), projected_schema, kSnapshotColumns, num_rows, batch_size,
      [metadata = std::move(metadata),
       snapshot_indices = std::move(snapshot_indices)](size_t index) -> const Snapshot& {
        return *metadata->snapshots[snapshot_indices[index]];
      });
}

// SnapshotsScan implementation
SnapshotsScan::SnapshotsScan(internal::MetadataScanContext context,
                             std::shared_ptr<MetadataScanTask> task)
    : MetadataScan(std::move(context)), task_(std::move(task)) {}

SnapshotsScan::~SnapshotsScan() = default;

Result<std::unique_ptr<SnapshotsScan>> SnapshotsScan::Make(
    internal::MetadataScanContext context) {
  ICEBERG_PRECHECK(context.kind == MetadataTableKind::kSnapshots,
                   "Unexpected metadata table kind for snapshots scan");
  if (context.snapshot_id.has_value() || !context.ref_name.empty() ||
      context.as_of_time_in_millis.has_value()) {
    return NotSupported("Metadata table does not support time travel");
  }
  ICEBERG_PRECHECK(context.table_metadata != nullptr, "Table metadata cannot be null");
  ICEBERG_PRECHECK(context.schema != nullptr, "Metadata table schema cannot be null");
  ICEBERG_PRECHECK(context.projected_schema != nullptr,
                   "Projected schema cannot be null");
  ICEBERG_RETURN_UNEXPECTED(internal::SelectAppendFns<Snapshot>(
      *SnapshotsTable::CanonicalSchema(), *context.projected_schema, kSnapshotColumns));
  ICEBERG_ASSIGN_OR_RAISE(auto task,
                          StaticMetadataTask::Make(context.kind, context.table_metadata,
                                                   context.projected_schema));
  return std::unique_ptr<SnapshotsScan>(
      new SnapshotsScan(std::move(context), std::move(task)));
}

Result<MetadataScanTaskStreamPtr> SnapshotsScan::PlanTasksStream() const {
  return MetadataScanTaskStream::FromVector({task_});
}

}  // namespace iceberg
