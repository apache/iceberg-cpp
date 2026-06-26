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

#include "iceberg/catalog/rest/rest_table.h"

#include <memory>
#include <utility>

#include "iceberg/catalog/rest/rest_table_scan.h"
#include "iceberg/result.h"
#include "iceberg/table_metadata.h"
#include "iceberg/util/macros.h"

namespace iceberg::rest {

RestTable::RestTable(TableIdentifier identifier, std::shared_ptr<TableMetadata> metadata,
                     std::string metadata_location, std::shared_ptr<FileIO> io,
                     std::shared_ptr<Catalog> catalog, RestScanContext rest_context)
    : Table(std::move(identifier), std::move(metadata), std::move(metadata_location),
            std::move(io), std::move(catalog)),
      rest_context_(std::move(rest_context)) {}

RestTable::~RestTable() = default;

Result<std::shared_ptr<RestTable>> RestTable::Make(TableIdentifier identifier,
                                                   std::shared_ptr<TableMetadata> metadata,
                                                   std::string metadata_location,
                                                   std::shared_ptr<FileIO> io,
                                                   std::shared_ptr<Catalog> catalog,
                                                   RestScanContext rest_context) {
  if (metadata == nullptr) {
    return InvalidArgument("Metadata cannot be null");
  }
  return std::shared_ptr<RestTable>(
      new RestTable(std::move(identifier), std::move(metadata),
                    std::move(metadata_location), std::move(io), std::move(catalog),
                    std::move(rest_context)));
}

Result<std::unique_ptr<DataTableScanBuilder>> RestTable::NewScan() const {
  return std::make_unique<RestTableScanBuilder>(metadata_, io_, rest_context_);
}

}  // namespace iceberg::rest
