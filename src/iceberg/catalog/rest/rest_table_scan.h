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
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "iceberg/catalog/rest/endpoint.h"
#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/metrics/metrics_reporter.h"
#include "iceberg/result.h"
#include "iceberg/storage_credential.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_scan.h"
#include "iceberg/type_fwd.h"

/// \file iceberg/catalog/rest/rest_table_scan.h
/// REST-specific table scans that delegate scan planning to the REST catalog server.

namespace iceberg::rest {

class HttpClient;
class ResourcePaths;

namespace auth {
class AuthSession;
}  // namespace auth

/// \brief HTTP context shared between RestTable and REST scan classes.
struct ICEBERG_REST_EXPORT RestScanContext {
  std::shared_ptr<HttpClient> client;
  std::shared_ptr<ResourcePaths> paths;
  std::shared_ptr<auth::AuthSession> session;
  std::unordered_set<Endpoint> supported_endpoints;
  TableIdentifier identifier;
  /// Catalog-level config, used with table_config to build a scan-scoped FileIO
  /// when the server vends storage credentials in a planning response.
  std::unordered_map<std::string, std::string> catalog_config;
  /// Table-level config merged with catalog_config for scan-scoped FileIO creation.
  std::unordered_map<std::string, std::string> table_config;
};

/// \brief A DataTableScan that delegates PlanFilesStream() to the REST catalog server
/// via the scan planning endpoints (planTableScan / fetchPlanningResult /
/// cancelPlanning / fetchScanTasks).
class ICEBERG_REST_EXPORT RestTableScan : public DataTableScan {
 public:
  ~RestTableScan() override = default;

  static Result<std::unique_ptr<DataTableScan>> Make(
      std::shared_ptr<TableMetadata> metadata, std::shared_ptr<Schema> schema,
      std::shared_ptr<FileIO> io, internal::TableScanContext context,
      RestScanContext rest_context);

  /// \brief Plans files lazily via the REST scan planning endpoints.
  Result<FileScanTaskStreamPtr> PlanFilesStream() const override;

  /// \brief Returns the effective FileIO for reading scan results.
  ///
  /// If the server vended storage credentials during planning, returns a FileIO
  /// initialised with those credentials; otherwise returns the table's FileIO.
  const std::shared_ptr<FileIO>& io() const override;

 private:
  RestTableScan(std::shared_ptr<TableMetadata> metadata, std::shared_ptr<Schema> schema,
                std::shared_ptr<FileIO> io, internal::TableScanContext context,
                RestScanContext rest_context);

  RestScanContext rest_context_;
  /// Shared slot so credentials vended by any lazy FetchScanTasks response are
  /// visible through io() even after the stream has been consumed.
  mutable std::shared_ptr<std::shared_ptr<FileIO>> scan_io_slot_;
};

/// \brief Builder that produces a RestTableScan with the REST HTTP context injected.
class ICEBERG_REST_EXPORT RestTableScanBuilder : public DataTableScanBuilder {
 public:
  RestTableScanBuilder(std::shared_ptr<TableMetadata> metadata,
                       std::shared_ptr<FileIO> io, std::string table_name,
                       std::shared_ptr<MetricsReporter> metrics_reporter,
                       RestScanContext rest_context);

  /// \brief Resolves schema/context via parent logic then creates a RestTableScan.
  Result<std::unique_ptr<DataTableScan>> Build() override;

 private:
  RestScanContext rest_context_;
};

/// \brief An IncrementalAppendScan that delegates PlanFiles() to the REST catalog server.
///
/// IncrementalChangelogScan is not delegated because the REST planTableScan response
/// only carries FileScanTask objects; reconstructing ChangelogScanTask entries
/// (AddedRowsScanTask vs DeletedDataFileScanTask) requires per-snapshot operation
/// metadata that the server does not return. Changelog scans always plan locally.
class ICEBERG_REST_EXPORT RestIncrementalAppendScan : public IncrementalAppendScan {
 public:
  ~RestIncrementalAppendScan() override = default;

  static Result<std::unique_ptr<IncrementalAppendScan>> Make(
      std::shared_ptr<TableMetadata> metadata, std::shared_ptr<Schema> schema,
      std::shared_ptr<FileIO> io, internal::TableScanContext context,
      RestScanContext rest_context);

  /// \brief Plans files via the REST scan planning endpoints.
  Result<std::vector<std::shared_ptr<FileScanTask>>> PlanFiles() const override;

 private:
  RestIncrementalAppendScan(std::shared_ptr<TableMetadata> metadata,
                            std::shared_ptr<Schema> schema, std::shared_ptr<FileIO> io,
                            internal::TableScanContext context,
                            RestScanContext rest_context);

  RestScanContext rest_context_;
  mutable std::shared_ptr<FileIO> scan_io_;
};

/// \brief Builder that produces a RestIncrementalAppendScan.
class ICEBERG_REST_EXPORT RestIncrementalAppendScanBuilder
    : public IncrementalAppendScanBuilder {
 public:
  RestIncrementalAppendScanBuilder(std::shared_ptr<TableMetadata> metadata,
                                   std::shared_ptr<FileIO> io, std::string table_name,
                                   std::shared_ptr<MetricsReporter> metrics_reporter,
                                   RestScanContext rest_context);

  Result<std::unique_ptr<IncrementalAppendScan>> Build() override;

 private:
  RestScanContext rest_context_;
};

}  // namespace iceberg::rest
