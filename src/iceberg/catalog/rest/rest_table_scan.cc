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

#include "iceberg/catalog/rest/rest_table_scan.h"

#include <chrono>
#include <thread>

#include <nlohmann/json.hpp>

#include "iceberg/catalog/rest/endpoint.h"
#include "iceberg/catalog/rest/error_handlers.h"
#include "iceberg/catalog/rest/http_client.h"
#include "iceberg/catalog/rest/json_serde_internal.h"
#include "iceberg/catalog/rest/resource_paths.h"
#include "iceberg/catalog/rest/types.h"
#include "iceberg/json_serde_internal.h"
#include "iceberg/partition_spec.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/table_metadata.h"
#include "iceberg/util/macros.h"

namespace iceberg::rest {

namespace {

constexpr int64_t kMinSleepMs = 1'000;
constexpr int64_t kMaxSleepMs = 60'000;
constexpr int kMaxRetries = 10;
constexpr int64_t kMaxWaitTimeMs = 5 * 60 * 1'000;

#define ICEBERG_ENDPOINT_CHECK(endpoints, endpoint)                           \
  do {                                                                        \
    if (!endpoints.contains(endpoint)) {                                      \
      return NotSupported("Not supported endpoint: {}", endpoint.ToString()); \
    }                                                                         \
  } while (0)

}  // namespace

// RestTableScan

RestTableScan::RestTableScan(std::shared_ptr<TableMetadata> metadata,
                             std::shared_ptr<Schema> schema, std::shared_ptr<FileIO> io,
                             internal::TableScanContext context,
                             RestScanContext rest_context)
    : DataTableScan(std::move(metadata), std::move(schema), std::move(io),
                    std::move(context)),
      rest_context_(std::move(rest_context)) {}

Result<std::unique_ptr<DataTableScan>> RestTableScan::Make(
    std::shared_ptr<TableMetadata> metadata, std::shared_ptr<Schema> schema,
    std::shared_ptr<FileIO> io, internal::TableScanContext context,
    RestScanContext rest_context) {
  ICEBERG_PRECHECK(metadata != nullptr, "Table metadata cannot be null");
  ICEBERG_PRECHECK(schema != nullptr, "Schema cannot be null");
  ICEBERG_PRECHECK(io != nullptr, "FileIO cannot be null");
  return std::unique_ptr<DataTableScan>(
      new RestTableScan(std::move(metadata), std::move(schema), std::move(io),
                        std::move(context), std::move(rest_context)));
}

Result<std::vector<std::shared_ptr<FileScanTask>>> RestTableScan::PlanFiles() const {
  TableMetadataCache metadata_cache(metadata_.get());
  ICEBERG_ASSIGN_OR_RAISE(auto specs_by_id, metadata_cache.GetPartitionSpecsById());

  std::string plan_id;
  return PlanTableScan(plan_id, specs_by_id);
}

Result<std::vector<std::shared_ptr<FileScanTask>>> RestTableScan::PlanTableScan(
    std::string& plan_id,
    const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>& specs) const {
  ICEBERG_ENDPOINT_CHECK(rest_context_.supported_endpoints, Endpoint::PlanTableScan());

  // Build request from scan context
  PlanTableScanRequest request;
  request.select = context_.selected_columns;
  request.filter = context_.filter;
  request.case_sensitive = context_.case_sensitive;
  request.min_rows_requested = context_.min_rows_requested;

  if (context_.from_snapshot_id.has_value() && context_.to_snapshot_id.has_value()) {
    request.start_snapshot_id = context_.from_snapshot_id;
    request.end_snapshot_id = context_.to_snapshot_id;
  } else if (context_.snapshot_id.has_value()) {
    request.snapshot_id = context_.snapshot_id;
  }

  if (!context_.columns_to_keep_stats.empty()) {
    for (int32_t field_id : context_.columns_to_keep_stats) {
      ICEBERG_ASSIGN_OR_RAISE(auto name, schema_->FindColumnNameById(field_id));
      if (name.has_value()) {
        request.stats_fields.emplace_back(*name);
      }
    }
  }

  ICEBERG_ASSIGN_OR_RAISE(auto path,
                          rest_context_.paths->Plan(rest_context_.identifier));
  ICEBERG_ASSIGN_OR_RAISE(auto request_json, ToJson(request));
  ICEBERG_ASSIGN_OR_RAISE(auto json_request, ToJsonString(request_json));
  ICEBERG_ASSIGN_OR_RAISE(
      const auto response,
      rest_context_.client->Post(path, json_request, /*headers=*/{},
                                 *PlanErrorHandler::Instance(), *rest_context_.session));
  ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.body()));
  ICEBERG_ASSIGN_OR_RAISE(auto result,
                          PlanTableScanResponseFromJson(json, specs, *schema_));
  ICEBERG_RETURN_UNEXPECTED(result.Validate());

  plan_id = result.plan_id;

  switch (result.plan_status) {
    case PlanStatus::kCompleted:
      return ResolveScanTasks(result.plan_tasks, result.file_scan_tasks, specs);
    case PlanStatus::kSubmitted:
      return FetchPlanningResult(plan_id, specs);
    case PlanStatus::kFailed:
      return IOError("Scan planning failed: {}",
                      result.error ? result.error->message : "unknown error");
    case PlanStatus::kCancelled:
      return IOError("Scan planning was cancelled for plan_id={}", plan_id);
  }
  return IOError("Unexpected plan status");
}

Result<std::vector<std::shared_ptr<FileScanTask>>> RestTableScan::FetchPlanningResult(
    const std::string& plan_id,
    const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>& specs) const {
  ICEBERG_ENDPOINT_CHECK(rest_context_.supported_endpoints,
                         Endpoint::FetchPlanningResult());

  ICEBERG_ASSIGN_OR_RAISE(auto path,
                          rest_context_.paths->Plan(rest_context_.identifier, plan_id));

  auto delay_ms = kMinSleepMs;
  auto start = std::chrono::steady_clock::now();

  for (int retry = 0; retry <= kMaxRetries; ++retry) {
    ICEBERG_ASSIGN_OR_RAISE(
        const auto response,
        rest_context_.client->Get(path, /*params=*/{}, /*headers=*/{},
                                  *PlanErrorHandler::Instance(), *rest_context_.session));
    ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.body()));
    ICEBERG_ASSIGN_OR_RAISE(auto result,
                            FetchPlanningResultResponseFromJson(json, specs, *schema_));
    ICEBERG_RETURN_UNEXPECTED(result.Validate());

    switch (result.plan_status) {
      case PlanStatus::kCompleted:
        return ResolveScanTasks(result.plan_tasks, result.file_scan_tasks, specs);
      case PlanStatus::kSubmitted: {
        auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - start)
                              .count();
        if (elapsed_ms >= kMaxWaitTimeMs) {
          CancelPlanning(plan_id);
          return IOError(
              "Scan planning timed out after {}ms waiting for plan_id={}", elapsed_ms,
              plan_id);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
        delay_ms = std::min(delay_ms * 2, kMaxSleepMs);
        continue;
      }
      case PlanStatus::kFailed:
        CancelPlanning(plan_id);
        return IOError("Scan planning failed: {}",
                        result.error ? result.error->message : "unknown error");
      case PlanStatus::kCancelled:
        return IOError("Scan planning was cancelled for plan_id={}", plan_id);
    }
  }

  CancelPlanning(plan_id);
  return IOError("Scan planning exceeded max retries ({}) for plan_id={}",
                          kMaxRetries, plan_id);
}

Result<std::vector<std::shared_ptr<FileScanTask>>> RestTableScan::FetchScanTasks(
    const std::string& plan_task,
    const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>& specs) const {
  ICEBERG_ENDPOINT_CHECK(rest_context_.supported_endpoints, Endpoint::FetchScanTasks());

  ICEBERG_ASSIGN_OR_RAISE(auto path,
                          rest_context_.paths->FetchScanTasks(rest_context_.identifier));
  FetchScanTasksRequest request{.planTask = plan_task};
  ICEBERG_ASSIGN_OR_RAISE(auto json_request, ToJsonString(ToJson(request)));
  ICEBERG_ASSIGN_OR_RAISE(
      const auto response,
      rest_context_.client->Post(path, json_request, /*headers=*/{},
                                 *PlanTaskErrorHandler::Instance(),
                                 *rest_context_.session));
  ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.body()));
  ICEBERG_ASSIGN_OR_RAISE(auto result,
                          FetchScanTasksResponseFromJson(json, specs, *schema_));
  ICEBERG_RETURN_UNEXPECTED(result.Validate());

  return ResolveScanTasks(result.plan_tasks, result.file_scan_tasks, specs);
}

Result<std::vector<std::shared_ptr<FileScanTask>>> RestTableScan::ResolveScanTasks(
    const std::optional<std::vector<std::string>>& plan_tasks,
    const std::optional<std::vector<std::shared_ptr<FileScanTask>>>& file_scan_tasks,
    const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>& specs) const {
  std::vector<std::shared_ptr<FileScanTask>> result;

  if (file_scan_tasks.has_value()) {
    result.insert(result.end(), file_scan_tasks->begin(), file_scan_tasks->end());
  }

  if (plan_tasks.has_value()) {
    for (const auto& plan_task : *plan_tasks) {
      ICEBERG_ASSIGN_OR_RAISE(auto tasks, FetchScanTasks(plan_task, specs));
      result.insert(result.end(), tasks.begin(), tasks.end());
    }
  }

  return result;
}

void RestTableScan::CancelPlanning(const std::string& plan_id) const {
  if (plan_id.empty()) return;
  if (!rest_context_.supported_endpoints.contains(Endpoint::CancelPlanning())) return;

  auto path = rest_context_.paths->Plan(rest_context_.identifier, plan_id);
  if (!path.has_value()) return;

  // Best-effort: ignore errors.
  std::ignore = rest_context_.client->Delete(*path, /*params=*/{}, /*headers=*/{},
                                             *PlanErrorHandler::Instance(),
                                             *rest_context_.session);
}

// RestTableScanBuilder

RestTableScanBuilder::RestTableScanBuilder(std::shared_ptr<TableMetadata> metadata,
                                           std::shared_ptr<FileIO> io,
                                           RestScanContext rest_context)
    : DataTableScanBuilder(std::move(metadata), std::move(io)),
      rest_context_(std::move(rest_context)) {}

Result<std::unique_ptr<DataTableScan>> RestTableScanBuilder::Build() {
  ICEBERG_RETURN_UNEXPECTED(CheckErrors());
  ICEBERG_RETURN_UNEXPECTED(context_.Validate());
  ICEBERG_ASSIGN_OR_RAISE(auto schema, ResolveSnapshotSchema());
  return RestTableScan::Make(metadata_, schema.get(), io_, std::move(context_),
                             rest_context_);
}

}  // namespace iceberg::rest
