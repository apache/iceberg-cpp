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

#pragma once

#include <memory>
#include <string>

#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/catalog/rest/rest_table_scan.h"
#include "iceberg/result.h"
#include "iceberg/table.h"
#include "iceberg/type_fwd.h"

/// \file iceberg/catalog/rest/rest_table.h
/// A Table subclass that uses server-side distributed scan planning via the REST catalog.

namespace iceberg::rest {

/// \brief A Table whose NewScan() returns a RestTableScanBuilder, delegating
/// PlanFiles() to the REST catalog server's scan planning endpoints.
class ICEBERG_REST_EXPORT RestTable final : public Table {
 public:
  static Result<std::shared_ptr<RestTable>> Make(TableIdentifier identifier,
                                                 std::shared_ptr<TableMetadata> metadata,
                                                 std::string metadata_location,
                                                 std::shared_ptr<FileIO> io,
                                                 std::shared_ptr<Catalog> catalog,
                                                 RestScanContext rest_context);

  ~RestTable() override;

  /// \brief Returns a RestTableScanBuilder that will delegate PlanFiles() to the
  /// REST catalog server.
  Result<std::unique_ptr<DataTableScanBuilder>> NewScan() const override;

 private:
  RestTable(TableIdentifier identifier, std::shared_ptr<TableMetadata> metadata,
            std::string metadata_location, std::shared_ptr<FileIO> io,
            std::shared_ptr<Catalog> catalog, RestScanContext rest_context);

  RestScanContext rest_context_;
};

}  // namespace iceberg::rest
