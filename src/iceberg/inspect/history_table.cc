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

#include "iceberg/inspect/history_table.h"

#include <array>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "iceberg/arrow_row_builder_internal.h"
#include "iceberg/constants.h"
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

struct HistoryRow {
  const SnapshotLogEntry& entry;
  const Snapshot* snapshot;
  bool is_current_ancestor;
};

constexpr std::array<internal::MetadataTableColumn<HistoryRow>, 4> kHistoryColumns = {
    {{.field_id = 1,
      .append =
          [](ArrowArray* column, const HistoryRow& row) {
            return AppendInt(column,
                             UnixMsFromTimePointMs(row.entry.timestamp_ms) * 1000);
          }},
     {.field_id = 2,
      .append =
          [](ArrowArray* column, const HistoryRow& row) {
            return AppendInt(column, row.entry.snapshot_id);
          }},
     {.field_id = 3,
      .append =
          [](ArrowArray* column, const HistoryRow& row) {
            return row.snapshot != nullptr && row.snapshot->parent_snapshot_id
                       ? AppendInt(column, *row.snapshot->parent_snapshot_id)
                       : AppendNull(column);
          }},
     {.field_id = 4, .append = [](ArrowArray* column, const HistoryRow& row) {
        return AppendBoolean(column, row.is_current_ancestor);
      }}}};

}  // namespace

// HistoryTable implementation
HistoryTable::HistoryTable(std::shared_ptr<const Table> table)
    : MetadataTable(std::move(table)) {}

HistoryTable::~HistoryTable() = default;

const std::shared_ptr<Schema>& HistoryTable::schema() const { return CanonicalSchema(); }

const std::shared_ptr<Schema>& HistoryTable::CanonicalSchema() {
  static const auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "made_current_at", timestamp_tz()),
      SchemaField::MakeRequired(2, "snapshot_id", int64()),
      SchemaField::MakeOptional(3, "parent_id", int64()),
      SchemaField::MakeRequired(4, "is_current_ancestor", boolean())});
  return schema;
}

Result<std::unique_ptr<HistoryTable>> HistoryTable::Make(
    std::shared_ptr<const Table> table) {
  ICEBERG_PRECHECK(table != nullptr, "Table cannot be null");
  return std::unique_ptr<HistoryTable>(new HistoryTable(std::move(table)));
}

Result<std::unique_ptr<MetadataScan>> HistoryTable::MakeScan(
    internal::MetadataScanContext context) const {
  return HistoryScan::Make(std::move(context));
}

Result<ArrowArrayStream> HistoryTable::ReadRows(
    std::shared_ptr<const TableMetadata> metadata,
    const std::shared_ptr<Schema>& projected_schema, int64_t batch_size) {
  std::unordered_map<int64_t, const Snapshot*> snapshots_by_id;
  for (const auto& snapshot : metadata->snapshots) {
    if (snapshot) {
      snapshots_by_id.emplace(snapshot->snapshot_id, snapshot.get());
    }
  }

  // Walk the current snapshot's ancestry; the insert check stops on cycles.
  std::unordered_set<int64_t> ancestors;
  for (auto current = metadata->current_snapshot_id;
       current != kInvalidSnapshotId && ancestors.insert(current).second;) {
    auto it = snapshots_by_id.find(current);
    if (it == snapshots_by_id.end() || !it->second->parent_snapshot_id) {
      break;
    }
    current = *it->second->parent_snapshot_id;
  }

  const size_t num_rows = metadata->snapshot_log.size();
  return internal::MakeMetadataBatchStream<HistoryRow>(
      CanonicalSchema(), projected_schema, kHistoryColumns, num_rows, batch_size,
      [metadata = std::move(metadata), snapshots_by_id = std::move(snapshots_by_id),
       ancestors = std::move(ancestors)](size_t index) -> HistoryRow {
        const auto& entry = metadata->snapshot_log[index];
        auto it = snapshots_by_id.find(entry.snapshot_id);
        return {.entry = entry,
                .snapshot = it != snapshots_by_id.end() ? it->second : nullptr,
                .is_current_ancestor = ancestors.contains(entry.snapshot_id)};
      });
}

// HistoryScan implementation
HistoryScan::HistoryScan(internal::MetadataScanContext context,
                         std::shared_ptr<MetadataScanTask> task)
    : MetadataScan(std::move(context)), task_(std::move(task)) {}

HistoryScan::~HistoryScan() = default;

Result<std::unique_ptr<HistoryScan>> HistoryScan::Make(
    internal::MetadataScanContext context) {
  ICEBERG_PRECHECK(context.kind == MetadataTableKind::kHistory,
                   "Unexpected metadata table kind for history scan");
  if (context.snapshot_id.has_value() || !context.ref_name.empty() ||
      context.as_of_time_in_millis.has_value()) {
    return NotSupported("Metadata table does not support time travel");
  }
  ICEBERG_PRECHECK(context.table_metadata != nullptr, "Table metadata cannot be null");
  ICEBERG_PRECHECK(context.schema != nullptr, "Metadata table schema cannot be null");
  ICEBERG_PRECHECK(context.projected_schema != nullptr,
                   "Projected schema cannot be null");
  ICEBERG_RETURN_UNEXPECTED(internal::SelectAppendFns<HistoryRow>(
      *HistoryTable::CanonicalSchema(), *context.projected_schema, kHistoryColumns));
  ICEBERG_ASSIGN_OR_RAISE(auto task,
                          StaticMetadataTask::Make(context.kind, context.table_metadata,
                                                   context.projected_schema));
  return std::unique_ptr<HistoryScan>(
      new HistoryScan(std::move(context), std::move(task)));
}

Result<MetadataScanTaskStreamPtr> HistoryScan::PlanTasksStream() const {
  return MetadataScanTaskStream::FromVector({task_});
}

}  // namespace iceberg
