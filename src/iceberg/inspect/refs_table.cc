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
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "iceberg/arrow_row_builder_internal.h"
#include "iceberg/inspect/metadata_table_stream_internal.h"
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

Status AppendRef(ArrowRowBuilder& builder, const RefRow& ref) {
  ICEBERG_RETURN_UNEXPECTED(AppendString(builder.column(0), ref.name));
  ICEBERG_RETURN_UNEXPECTED(AppendString(builder.column(1), ref.type));
  ICEBERG_RETURN_UNEXPECTED(AppendInt(builder.column(2), ref.snapshot_id));
  ICEBERG_RETURN_UNEXPECTED(AppendOptional(builder.column(3), ref.max_ref_age_ms));
  ICEBERG_RETURN_UNEXPECTED(AppendOptional(builder.column(4), ref.min_snapshots_to_keep));
  ICEBERG_RETURN_UNEXPECTED(AppendOptional(builder.column(5), ref.max_snapshot_age_ms));
  return builder.FinishRow();
}

}  // namespace

RefsTable::RefsTable(std::shared_ptr<Table> table) : MetadataTable(std::move(table)) {}

RefsTable::~RefsTable() = default;

const std::shared_ptr<Schema>& RefsTable::schema() const {
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

Result<std::unique_ptr<RefsTable>> RefsTable::Make(std::shared_ptr<Table> table) {
  ICEBERG_PRECHECK(table != nullptr, "Table cannot be null");
  return std::unique_ptr<RefsTable>(new RefsTable(std::move(table)));
}

Result<ArrowArrayStream> RefsTable::Scan() {
  std::vector<RefRow> rows;
  for (const auto& [name, ref] : source_table()->metadata()->refs) {
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
  return internal::MakeMetadataTableStream(*schema(), std::move(rows), AppendRef);
}

}  // namespace iceberg
