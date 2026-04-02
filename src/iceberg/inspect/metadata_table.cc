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
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_scan.h"
#include "iceberg/type.h"
#include "iceberg/util/uuid.h"

namespace iceberg {

BaseMetadataTable::BaseMetadataTable(std::shared_ptr<Table> source_table,
                                     TableIdentifier identifier,
                                     std::shared_ptr<Schema> schema)
    : Table(identifier, source_table->metadata(),
            std::string(source_table->metadata_file_location()), source_table->io(),
            source_table->catalog()),
      source_table_(std::move(source_table)),
      schema_(schema) {
  uuid_ = Uuid::GenerateV4().ToString();
  schemas_[schema->schema_id()] = schema;
}

BaseMetadataTable::~BaseMetadataTable() = default;

Status BaseMetadataTable::Refresh() {
  return NotSupported("Cannot refresh a metadata table");
}

Result<std::unique_ptr<TableScanBuilder>> BaseMetadataTable::NewScan() const {
  return NotSupported("TODO: Scanning metadata tables is not yet supported");
};

Result<std::shared_ptr<Transaction>> BaseMetadataTable::NewTransaction() {
  return NotSupported("Cannot create a transaction for a metadata table");
}

Result<std::shared_ptr<UpdateProperties>> BaseMetadataTable::NewUpdateProperties() {
  return NotSupported("Cannot create an update properties for a metadata table");
}

Result<std::shared_ptr<UpdateSchema>> BaseMetadataTable::NewUpdateSchema() {
  return NotSupported("Cannot create an update schema for a metadata table");
}

Result<std::shared_ptr<UpdateLocation>> BaseMetadataTable::NewUpdateLocation() {
  return NotSupported("Cannot create an update location for a metadata table");
}

Result<std::shared_ptr<UpdatePartitionSpec>> BaseMetadataTable::NewUpdatePartitionSpec() {
  return NotSupported("Cannot create an update partition spec for a metadata table");
}

Result<std::shared_ptr<UpdateSortOrder>> BaseMetadataTable::NewUpdateSortOrder() {
  return NotSupported("Cannot create an update sort order for a metadata table");
}

Result<std::shared_ptr<ExpireSnapshots>> BaseMetadataTable::NewExpireSnapshots() {
  return NotSupported("Cannot create an expire snapshots for a metadata table");
}

}  // namespace iceberg
