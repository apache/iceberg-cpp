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
#include "iceberg/inspect/metadata_scan.h"
#include "iceberg/inspect/snapshots_table.h"

namespace iceberg {

MetadataTable::MetadataTable(std::shared_ptr<const Table> source_table)
    : source_table_(std::move(source_table)) {}

MetadataTable::~MetadataTable() = default;

Result<std::unique_ptr<MetadataTable>> MetadataTable::Make(
    std::shared_ptr<const Table> table, MetadataTableKind kind) {
  switch (kind) {
    case MetadataTableKind::kSnapshots:
      return SnapshotsTable::Make(std::move(table));
    case MetadataTableKind::kHistory:
      return HistoryTable::Make(std::move(table));
    default:
      break;
  }
  return NotImplemented("Metadata table kind is not implemented");
}

const std::shared_ptr<const Table>& MetadataTable::source_table() const {
  return source_table_;
}

Result<std::unique_ptr<MetadataScanBuilder>> MetadataTable::NewScan() const {
  return MetadataScanBuilder::Make(*this);
}

}  // namespace iceberg
