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

#include "iceberg/inspect/refs_table.h"

#include <algorithm>
#include <array>
#include <optional>
#include <string>
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

struct RefRow {
  std::string name;
  std::string type;
  int64_t snapshot_id;
  std::optional<int64_t> max_ref_age_ms;
  std::optional<int32_t> min_snapshots_to_keep;
  std::optional<int64_t> max_snapshot_age_ms;
};

Status AppendOptional(ArrowArray* array, const auto& value) {
  if (!value.has_value()) {
    return AppendNull(array);
  }
  return AppendInt(array, static_cast<int64_t>(*value));
}

constexpr std::array<internal::MetadataTableColumn<RefRow>, 6> kRefColumns = {
    {{.field_id = 1,
      .append = [](ArrowArray* column, const RefRow& row) { return AppendString(column, row.name); }},
     {.field_id = 2,
      .append = [](ArrowArray* column, const RefRow& row) { return AppendString(column, row.type); }},
     {.field_id = 3,
      .append = [](ArrowArray* column, const RefRow& row) { return AppendInt(column, row.snapshot_id); }},
     {.field_id = 4,
      .append = [](ArrowArray* column, const RefRow& row) { return AppendOptional(column, row.max_ref_age_ms); }},
     {.field_id = 5,
      .append = [](ArrowArray* column, const RefRow& row) { return AppendOptional(column, row.min_snapshots_to_keep); }},
     {.field_id = 6,
      .append = [](ArrowArray* column, const RefRow& row) { return AppendOptional(column, row.max_snapshot_age_ms); }}}};

}  // namespace

// RefsTable implementation
RefsTable::RefsTable(std::shared_ptr<const Table> table)
    : MetadataTable(std::move(table)) {}

RefsTable::~RefsTable() = default;

const std::shared_ptr<Schema>& RefsTable::schema() const {
  return CanonicalSchema();
}

const std::shared_ptr<Schema>& RefsTable::CanonicalSchema() {
  static const auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "name", string()),
      SchemaField::MakeRequired(2, "type", string()),
      SchemaField::MakeRequired(3, "snapshot_id", int64()),
      SchemaField::MakeOptional(4, "max_reference_age_in_ms", int64()),
      SchemaField::MakeOptional(5, "min_snapshots_to_keep", int32()),
      SchemaField::MakeOptional(6, "max_snapshot_age_in_ms", int64()),
  });
  return schema;
}

Result<std::unique_ptr<RefsTable>> RefsTable::Make(
    std::shared_ptr<const Table> table) {
  ICEBERG_PRECHECK(table != nullptr, "Table cannot be null");
  return std::unique_ptr<RefsTable>(new RefsTable(std::move(table)));
}

Result<std::unique_ptr<MetadataScan>> RefsTable::MakeScan(
    internal::MetadataScanContext context) const {
  return RefsScan::Make(std::move(context));
}

Result<ArrowArrayStream> RefsTable::ReadRows(
    std::shared_ptr<const TableMetadata> metadata,
    const std::shared_ptr<Schema>& projected_schema, int64_t batch_size) {
  std::vector<RefRow> rows;
  for (const auto& [name, ref] : metadata->refs) {
    ICEBERG_PRECHECK(ref != nullptr, "Snapshot reference '{}' is null", name);
    RefRow row{.name = name,
               .type = ref->type() == SnapshotRefType::kBranch ? "BRANCH" : "TAG",
               .snapshot_id = ref->snapshot_id,
               .max_ref_age_ms = ref->max_ref_age_ms()};
    if (const auto* branch = std::get_if<SnapshotRef::Branch>(&ref->retention)) {
      row.min_snapshots_to_keep = branch->min_snapshots_to_keep;
      row.max_snapshot_age_ms = branch->max_snapshot_age_ms;
    }
    rows.push_back(std::move(row));
  }
  std::ranges::sort(rows, {}, &RefRow::name);
  const size_t num_rows = rows.size();
  return internal::MakeMetadataBatchStream<RefRow>(
      CanonicalSchema(), projected_schema, kRefColumns, num_rows, batch_size,
      [rows = std::move(rows)](size_t index) -> const RefRow& { return rows[index]; });
}

// RefsScan implementation
RefsScan::RefsScan(internal::MetadataScanContext context,
                             std::shared_ptr<MetadataScanTask> task)
    : MetadataScan(std::move(context)), task_(std::move(task)) {}

RefsScan::~RefsScan() = default;

Result<std::unique_ptr<RefsScan>> RefsScan::Make(
    internal::MetadataScanContext context) {
  ICEBERG_PRECHECK(context.kind == MetadataTableKind::kRefs,
                   "Unexpected metadata table kind for refs scan");
  if (context.snapshot_id.has_value() || !context.ref_name.empty() ||
      context.as_of_time_in_millis.has_value()) {
    return NotSupported("Metadata table does not support time travel");
  }
  ICEBERG_PRECHECK(context.table_metadata != nullptr, "Table metadata cannot be null");
  ICEBERG_PRECHECK(context.schema != nullptr, "Metadata table schema cannot be null");
  ICEBERG_PRECHECK(context.projected_schema != nullptr,
                   "Projected schema cannot be null");
  ICEBERG_RETURN_UNEXPECTED(internal::SelectAppendFns<RefRow>(
      *RefsTable::CanonicalSchema(), *context.projected_schema, kRefColumns));
  ICEBERG_ASSIGN_OR_RAISE(auto task,
                          StaticMetadataTask::Make(context.kind, context.table_metadata,
                                                   context.projected_schema));
  return std::unique_ptr<RefsScan>(
      new RefsScan(std::move(context), std::move(task)));
}

Result<MetadataScanTaskStreamPtr> RefsScan::PlanTasksStream() const {
  return MetadataScanTaskStream::FromVector({task_});
}

}  // namespace iceberg
