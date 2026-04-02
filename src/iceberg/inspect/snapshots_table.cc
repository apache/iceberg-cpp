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

#include <memory>
#include <utility>

#include "iceberg/inspect/metadata_table.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/table_identifier.h"
#include "iceberg/type.h"

namespace iceberg {

SnapshotsTable::SnapshotsTable(std::shared_ptr<Table> table)
    : BaseMetadataTable(table, CreateName(table->name()), CreateSchema()) {}

SnapshotsTable::~SnapshotsTable() = default;

std::shared_ptr<Schema> SnapshotsTable::CreateSchema() {
  return std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "committed_at", int64()),
                               SchemaField::MakeOptional(2, "snapshot_id", int64()),
                               SchemaField::MakeRequired(3, "parent_id", int64()),
                               SchemaField::MakeRequired(4, "manifest_list", string()),
                               SchemaField::MakeRequired(
                                   5, "summary",
                                   std::make_shared<iceberg::MapType>(
                                       SchemaField::MakeRequired(6, "key", string()),
                                       SchemaField::MakeRequired(7, "value", string())))},
      1);
}

TableIdentifier SnapshotsTable::CreateName(const TableIdentifier& source_name) {
  return TableIdentifier{source_name.ns, source_name.name + ".snapshots"};
}

Result<std::shared_ptr<SnapshotsTable>> SnapshotsTable::Make(
    std::shared_ptr<Table> table) {
  return std::shared_ptr<SnapshotsTable>(new SnapshotsTable(table));
}

}  // namespace iceberg
