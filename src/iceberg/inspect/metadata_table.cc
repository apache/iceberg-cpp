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

#include "iceberg/inspect/metadata_table.h"

#include <memory>
#include <utility>

#include "iceberg/inspect/history_table.h"
#include "iceberg/inspect/snapshots_table.h"

namespace iceberg {

MetadataTable::MetadataTable(std::shared_ptr<Table> source_table,
                             TableIdentifier identifier, std::shared_ptr<Schema> schema)
    : identifier_(std::move(identifier)),
      schema_(std::move(schema)),
      source_table_(std::move(source_table)) {}

MetadataTable::~MetadataTable() = default;

bool MetadataTable::supports_time_travel() const noexcept { return false; }

Result<ArrowArray> MetadataTable::Scan(
    const std::optional<SnapshotSelection>& /*snapshot_selection*/) {
  return NotSupported("Scan is not supported for this metadata table type");
}

Result<std::unique_ptr<MetadataTable>> MetadataTable::Make(std::shared_ptr<Table> table,
                                                           Kind kind) {
  if (table == nullptr) [[unlikely]] {
    return InvalidArgument("Table cannot be null");
  }

  switch (kind) {
    case Kind::kSnapshots:
      return SnapshotsTable::Make(table);
    case Kind::kHistory:
      return HistoryTable::Make(table);
  }

  return NotSupported("Unsupported metadata table type");
}

}  // namespace iceberg
