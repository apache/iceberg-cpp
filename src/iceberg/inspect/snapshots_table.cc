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
#include <memory>
#include <utility>
#include <vector>

#include "iceberg/arrow_row_builder_internal.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/table.h"
#include "iceberg/table_identifier.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"

namespace iceberg {
namespace {

std::shared_ptr<Schema> MakeSnapshotsTableSchema() {
  return std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "committed_at", timestamp_tz()),
      SchemaField::MakeRequired(2, "snapshot_id", int64()),
      SchemaField::MakeOptional(3, "parent_id", int64()),
      SchemaField::MakeOptional(4, "operation", string()),
      SchemaField::MakeOptional(5, "manifest_list", string()),
      SchemaField::MakeOptional(6, "summary",
                                std::make_shared<iceberg::MapType>(
                                    SchemaField::MakeRequired(7, "key", string()),
                                    SchemaField::MakeRequired(8, "value", string())))});
}

TableIdentifier MakeSnapshotsTableName(const TableIdentifier& source_name) {
  return TableIdentifier{.ns = source_name.ns, .name = source_name.name + ".snapshots"};
}

}  // namespace

SnapshotsTable::SnapshotsTable(std::shared_ptr<Table> table)
    : MetadataTable(table, MakeSnapshotsTableName(table->name()),
                    MakeSnapshotsTableSchema()) {}

SnapshotsTable::~SnapshotsTable() = default;

Result<std::unique_ptr<SnapshotsTable>> SnapshotsTable::Make(
    std::shared_ptr<Table> table) {
  if (table == nullptr) [[unlikely]] {
    return InvalidArgument("Table cannot be null");
  }
  return std::unique_ptr<SnapshotsTable>(new SnapshotsTable(std::move(table)));
}

Result<ArrowArray> SnapshotsTable::Scan(
    std::optional<SnapshotSelection> /*snapshot_selection*/) {
  ICEBERG_ASSIGN_OR_RAISE(auto builder, ArrowRowBuilder::Make(*schema()));

  for (const auto& snapshot : source_table()->snapshots()) {
    // column 0: committed_at (timestamptz -> int64 micros)
    ICEBERG_RETURN_UNEXPECTED(AppendInt(
        builder.column(0), std::chrono::duration_cast<std::chrono::microseconds>(
                               snapshot->timestamp_ms.time_since_epoch())
                               .count()));

    // column 1: snapshot_id (long)
    ICEBERG_RETURN_UNEXPECTED(AppendInt(builder.column(1), snapshot->snapshot_id));

    // column 2: parent_id (long, optional)
    if (snapshot->parent_snapshot_id.has_value()) {
      ICEBERG_RETURN_UNEXPECTED(
          AppendInt(builder.column(2), *snapshot->parent_snapshot_id));
    } else {
      ICEBERG_RETURN_UNEXPECTED(AppendNull(builder.column(2)));
    }

    // column 3: operation (string, optional)
    auto op = snapshot->Operation();
    if (op.has_value()) {
      ICEBERG_RETURN_UNEXPECTED(AppendString(builder.column(3), *op));
    } else {
      ICEBERG_RETURN_UNEXPECTED(AppendNull(builder.column(3)));
    }

    // column 4: manifest_list (string, optional)
    ICEBERG_RETURN_UNEXPECTED(AppendString(builder.column(4), snapshot->manifest_list));

    // column 5: summary (map<string,string>)
    ICEBERG_RETURN_UNEXPECTED(AppendStringMap(builder.column(5), snapshot->summary));

    ICEBERG_RETURN_UNEXPECTED(builder.FinishRow());
  }

  return std::move(builder).Finish();
}

}  // namespace iceberg
