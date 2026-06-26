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
#include "iceberg/result.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_scan.h"
#include "iceberg/type_fwd.h"

/// \file iceberg/catalog/rest/rest_table_scan.h
/// REST-specific table scan that delegates scan planning to the REST catalog server.

namespace iceberg::rest {

class HttpClient;
class ResourcePaths;

namespace auth {
class AuthSession;
}  // namespace auth

/// \brief HTTP context shared between RestTable and RestTableScan.
struct ICEBERG_REST_EXPORT RestScanContext {
  std::shared_ptr<HttpClient> client;
  std::shared_ptr<ResourcePaths> paths;
  std::shared_ptr<auth::AuthSession> session;
  std::unordered_set<Endpoint> supported_endpoints;
  TableIdentifier identifier;
};

/// \brief A DataTableScan that delegates PlanFiles() to the REST catalog server
/// via the scan planning endpoints (planTableScan / fetchPlanningResult /
/// cancelPlanning / fetchScanTasks).
class ICEBERG_REST_EXPORT RestTableScan : public DataTableScan {
 public:
  ~RestTableScan() override = default;

  static Result<std::unique_ptr<DataTableScan>> Make(
      std::shared_ptr<TableMetadata> metadata, std::shared_ptr<Schema> schema,
      std::shared_ptr<FileIO> io, internal::TableScanContext context,
      RestScanContext rest_context);

  /// \brief Plans files via the REST scan planning endpoints.
  Result<std::vector<std::shared_ptr<FileScanTask>>> PlanFiles() const override;

 private:
  RestTableScan(std::shared_ptr<TableMetadata> metadata, std::shared_ptr<Schema> schema,
                std::shared_ptr<FileIO> io, internal::TableScanContext context,
                RestScanContext rest_context);

  /// POST /plan → handle COMPLETED / SUBMITTED / FAILED / CANCELLED.
  Result<std::vector<std::shared_ptr<FileScanTask>>> PlanTableScan(
      std::string& plan_id,
      const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>& specs) const;

  /// GET /plan/{plan_id} with exponential backoff until COMPLETED.
  Result<std::vector<std::shared_ptr<FileScanTask>>> FetchPlanningResult(
      const std::string& plan_id,
      const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>& specs) const;

  /// POST /tasks/{plan_task_id} → fetch FileScanTasks for one opaque plan task token.
  Result<std::vector<std::shared_ptr<FileScanTask>>> FetchScanTasks(
      const std::string& plan_task,
      const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>& specs) const;

  /// Flatten plan_tasks (opaque tokens) + file_scan_tasks into a single list.
  Result<std::vector<std::shared_ptr<FileScanTask>>> ResolveScanTasks(
      const std::optional<std::vector<std::string>>& plan_tasks,
      const std::optional<std::vector<std::shared_ptr<FileScanTask>>>& file_scan_tasks,
      const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>& specs) const;

  /// DELETE /plan/{plan_id}; best-effort, errors are silently ignored.
  void CancelPlanning(const std::string& plan_id) const;

  RestScanContext rest_context_;
};

/// \brief Builder that produces a RestTableScan with the REST HTTP context injected.
class ICEBERG_REST_EXPORT RestTableScanBuilder : public DataTableScanBuilder {
 public:
  RestTableScanBuilder(std::shared_ptr<TableMetadata> metadata, std::shared_ptr<FileIO> io,
                       RestScanContext rest_context);

  /// \brief Resolves schema/context via parent logic then creates a RestTableScan.
  Result<std::unique_ptr<DataTableScan>> Build() override;

 private:
  RestScanContext rest_context_;
};

}  // namespace iceberg::rest
