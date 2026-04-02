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

#include <memory>
#include <utility>

#include "iceberg/inspect/metadata_table.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/table_identifier.h"
#include "iceberg/type.h"

namespace iceberg {

HistoryTable::HistoryTable(std::shared_ptr<Table> table)
    : BaseMetadataTable(table, CreateName(table->name()), CreateSchema()) {}

HistoryTable::~HistoryTable() = default;

std::shared_ptr<Schema> HistoryTable::CreateSchema() {
  return std::make_shared<Schema>(
      std::vector<SchemaField>{
          SchemaField::MakeRequired(1, "made_current_at", int64()),
          SchemaField::MakeRequired(2, "snapshot_id", int64()),
          SchemaField::MakeOptional(3, "parent_id", int64()),
          SchemaField::MakeRequired(4, "is_current_ancestor", boolean())},
      1);
}

TableIdentifier HistoryTable::CreateName(const TableIdentifier& source_name) {
  return TableIdentifier{source_name.ns, source_name.name + ".history"};
}

Result<std::shared_ptr<HistoryTable>> HistoryTable::Make(std::shared_ptr<Table> table) {
  return std::shared_ptr<HistoryTable>(new HistoryTable(table));
}

}  // namespace iceberg
