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
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/sort_order.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"
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
  auto schema = GetSchema();
  if (!schema) {
    schema = std::make_shared<Schema>(std::vector<SchemaField>{}, 1);
  }

  auto builder =
      TableMetadataBuilder::BuildFromEmpty(TableMetadata::kDefaultTableFormatVersion);
  auto result = builder->AssignUUID(Uuid::GenerateV4().ToString())
                    .SetLocation(std::string(source_table_->location()))
                    .SetCurrentSchema(schema, schema->schema_id())
                    .SetDefaultSortOrder(SortOrder::Unsorted())
                    .SetDefaultPartitionSpec(PartitionSpec::Unpartitioned())
                    .SetProperties({})
                    .Build();

  if (!result.has_value()) {
    // If metadata building fails, keep the original metadata from source_table
    return;
  }

  std::shared_ptr<TableMetadata> built_metadata = std::move(result.value());

  metadata_ = built_metadata;
  metadata_cache_ = std::make_unique<TableMetadataCache>(metadata_.get());
}

MetadataTable::~MetadataTable() = default;

Status MetadataTable::Refresh() { return source_table_->Refresh(); }

Result<std::unique_ptr<DataTableScanBuilder>> MetadataTable::NewScan() const {
  return NotSupported("TODO: Scanning metadata tables is not yet supported");
};

Result<std::unique_ptr<MetadataTable>> MetadataTableFactory::CreateMetadataTable(
    std::shared_ptr<Table> table, MetadataTableType type) {
  switch (type) {
    case MetadataTableType::kSnapshots:
      return SnapshotsTable::Make(table);
    case MetadataTableType::kHistory:
      return HistoryTable::Make(table);
  }

  return Invalid("Unsupported metadata table type");
}

}  // namespace iceberg
