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
#include <string>
#include <utility>

#include "iceberg/file_io.h"
#include "iceberg/inspect/history_table.h"
#include "iceberg/inspect/snapshots_table.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_scan.h"
#include "iceberg/type.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/uuid.h"

namespace iceberg {

MetadataTable::MetadataTable(std::shared_ptr<Table> source_table,
                             TableIdentifier identifier)
    : StaticTable(identifier, source_table->metadata(),
                  std::string(source_table->metadata_file_location()), source_table->io(),
                  source_table->catalog()),
      source_table_(std::move(source_table)) {
  uuid_ = Uuid::GenerateV4().ToString();
}

MetadataTable::~MetadataTable() = default;

Status MetadataTable::Refresh() { return source_table_->Refresh(); }

Result<std::unique_ptr<TableScanBuilder>> MetadataTable::NewScan() const {
  return NotSupported("TODO: Scanning metadata tables is not yet supported");
};

Result<std::unique_ptr<SnapshotsTable>> MetadataTableFactory::GetSnapshotsTable(
    std::shared_ptr<Table> table) {
  return SnapshotsTable::Make(table);
}

Result<std::unique_ptr<HistoryTable>> MetadataTableFactory::GetHistoryTable(
    std::shared_ptr<Table> table) {
  return HistoryTable::Make(table);
}

}  // namespace iceberg
