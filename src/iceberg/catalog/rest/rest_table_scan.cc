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

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <thread>

#include <nlohmann/json.hpp>

#include "iceberg/catalog/rest/catalog_properties.h"
#include "iceberg/catalog/rest/endpoint.h"
#include "iceberg/catalog/rest/error_handlers.h"
#include "iceberg/catalog/rest/http_client.h"
#include "iceberg/catalog/rest/json_serde_internal.h"
#include "iceberg/catalog/rest/resource_paths.h"
#include "iceberg/catalog/rest/rest_file_io.h"
#include "iceberg/catalog/rest/types.h"
#include "iceberg/json_serde_internal.h"
#include "iceberg/partition_spec.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/table_metadata.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/type_util.h"

namespace iceberg::rest {

namespace {

constexpr int64_t kMinSleepMs = 1'000;
constexpr int64_t kMaxSleepMs = 60'000;

struct PollConfig {
  int64_t max_wait_ms;
  int32_t max_retries;
};

Result<PollConfig> ResolvePollConfig(const RestScanContext& ctx) {
  auto properties = RestCatalogProperties::FromMap(ctx.catalog_config);
  ICEBERG_ASSIGN_OR_RAISE(auto max_wait_ms, properties.ScanPlanningPollTimeoutMs());
  ICEBERG_ASSIGN_OR_RAISE(auto max_retries, properties.ScanPlanningPollNumRetries());
  return PollConfig{.max_wait_ms = max_wait_ms, .max_retries = max_retries};
}

using SpecsById = std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>;

#define ICEBERG_ENDPOINT_CHECK(endpoints, endpoint)                           \
  do {                                                                        \
    if (!endpoints.contains(endpoint)) {                                      \
      return NotSupported("Not supported endpoint: {}", endpoint.ToString()); \
    }                                                                         \
  } while (0)

// ---------------------------------------------------------------------------
// Shared HTTP scan planning helpers used by all REST scan implementations.
// ---------------------------------------------------------------------------

// Column names of the projected schema, sent as the request's select.
Result<std::vector<std::string>> ProjectedColumnNames(const Schema& projected) {
  ICEBERG_ASSIGN_OR_RAISE(auto id_set, GetProjectedIdsVisitor::GetProjectedIds(
                                           projected, /*include_struct_ids=*/true));
  std::vector<int32_t> field_ids(id_set.begin(), id_set.end());
  std::ranges::sort(field_ids);

  std::vector<std::string> names;
  names.reserve(field_ids.size());
  for (int32_t field_id : field_ids) {
    ICEBERG_ASSIGN_OR_RAISE(auto name, projected.FindColumnNameById(field_id));
    ICEBERG_CHECK(name.has_value(), "Projected field {} not found in schema", field_id);
    names.emplace_back(*name);
  }
  return names;
}

Status ApplyStorageCredentials(const RestScanContext& ctx, const std::string& plan_id,
                               const std::vector<StorageCredential>& credentials,
                               std::shared_ptr<FileIO>& scan_io) {
  if (credentials.empty()) return {};
  std::shared_ptr<StorageCredentialProvider> provider;
  if (ctx.credential_provider_factory) {
    provider = ctx.credential_provider_factory(plan_id);
  }
  ICEBERG_ASSIGN_OR_RAISE(auto io, MakeTableFileIO(ctx.catalog_config, ctx.table_config,
                                                   credentials, std::move(provider)));
  scan_io = std::move(io);
  return {};
}

void CancelPlanning(const RestScanContext& ctx, const std::string& plan_id) {
  if (plan_id.empty()) return;
  if (!ctx.supported_endpoints.contains(Endpoint::CancelPlanning())) return;

  auto path = ctx.paths->Plan(ctx.identifier, plan_id);
  if (!path.has_value()) return;

  std::ignore = ctx.client->Delete(*path, /*params=*/{}, /*headers=*/{},
                                   *PlanErrorHandler::Instance(), *ctx.session);
}

Result<std::vector<std::shared_ptr<FileScanTask>>> FetchScanTasks(
    const RestScanContext& ctx, const Schema& schema, const std::string& plan_task,
    const SpecsById& specs);

Result<std::vector<std::shared_ptr<FileScanTask>>> ResolveScanTasks(
    const RestScanContext& ctx, const Schema& schema,
    const std::optional<std::vector<std::string>>& plan_tasks,
    const std::optional<std::vector<std::shared_ptr<FileScanTask>>>& file_scan_tasks,
    const SpecsById& specs) {
  std::vector<std::shared_ptr<FileScanTask>> result;

  if (file_scan_tasks.has_value()) {
    result.insert(result.end(), file_scan_tasks->begin(), file_scan_tasks->end());
  }

  if (plan_tasks.has_value()) {
    for (const auto& token : *plan_tasks) {
      ICEBERG_ASSIGN_OR_RAISE(auto tasks, FetchScanTasks(ctx, schema, token, specs));
      result.insert(result.end(), tasks.begin(), tasks.end());
    }
  }

  return result;
}

// Fetches one plan task without resolving any nested plan tasks it returns.
Result<FetchScanTasksResponse> FetchScanTasksOnce(const RestScanContext& ctx,
                                                  const Schema& schema,
                                                  const std::string& plan_task,
                                                  const SpecsById& specs) {
  ICEBERG_ENDPOINT_CHECK(ctx.supported_endpoints, Endpoint::FetchScanTasks());

  ICEBERG_ASSIGN_OR_RAISE(auto path, ctx.paths->FetchScanTasks(ctx.identifier));
  FetchScanTasksRequest request{.planTask = plan_task};
  ICEBERG_ASSIGN_OR_RAISE(auto json_request, ToJsonString(ToJson(request)));
  ICEBERG_ASSIGN_OR_RAISE(
      const auto response,
      ctx.client->Post(path, json_request, /*headers=*/{},
                       *PlanTaskErrorHandler::Instance(), *ctx.session));
  ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.body()));
  ICEBERG_ASSIGN_OR_RAISE(auto result,
                          FetchScanTasksResponseFromJson(json, specs, schema));
  ICEBERG_RETURN_UNEXPECTED(result.Validate());
  return result;
}

Result<std::vector<std::shared_ptr<FileScanTask>>> FetchScanTasks(
    const RestScanContext& ctx, const Schema& schema, const std::string& plan_task,
    const SpecsById& specs) {
  ICEBERG_ASSIGN_OR_RAISE(auto result, FetchScanTasksOnce(ctx, schema, plan_task, specs));
  return ResolveScanTasks(ctx, schema, result.plan_tasks, result.file_scan_tasks, specs);
}

// Polls until the plan completes and returns the completed response. Plan tasks
// are left unresolved.
Result<FetchPlanningResultResponse> FetchPlanningResult(
    const RestScanContext& ctx, const Schema& schema, const std::string& plan_id,
    const SpecsById& specs, const PollConfig& poll_config,
    std::shared_ptr<FileIO>& scan_io) {
  ICEBERG_ENDPOINT_CHECK(ctx.supported_endpoints, Endpoint::FetchPlanningResult());

  ICEBERG_ASSIGN_OR_RAISE(auto path, ctx.paths->Plan(ctx.identifier, plan_id));

  auto delay_ms = kMinSleepMs;
  auto start = std::chrono::steady_clock::now();

  for (int32_t retry = 0; retry <= poll_config.max_retries; ++retry) {
    auto response_or = ctx.client->Get(path, /*params=*/{}, /*headers=*/{},
                                       *PlanErrorHandler::Instance(), *ctx.session);
    if (!response_or) {
      CancelPlanning(ctx, plan_id);
      return ::iceberg::unexpected<Error>(response_or.error());
    }

    auto json_or = FromJsonString(response_or->body());
    if (!json_or) {
      CancelPlanning(ctx, plan_id);
      return ::iceberg::unexpected<Error>(json_or.error());
    }

    auto result_or = FetchPlanningResultResponseFromJson(*json_or, specs, schema);
    if (!result_or) {
      CancelPlanning(ctx, plan_id);
      return ::iceberg::unexpected<Error>(result_or.error());
    }

    if (auto s = result_or->Validate(); !s) {
      CancelPlanning(ctx, plan_id);
      return ::iceberg::unexpected<Error>(s.error());
    }

    auto& result = *result_or;

    switch (result.plan_status) {
      case PlanStatus::kCompleted: {
        if (auto s = ApplyStorageCredentials(ctx, plan_id, result.storage_credentials,
                                             scan_io);
            !s) {
          CancelPlanning(ctx, plan_id);
          return ::iceberg::unexpected<Error>(s.error());
        }
        return std::move(result);
      }
      case PlanStatus::kSubmitted: {
        auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - start)
                              .count();
        if (elapsed_ms >= poll_config.max_wait_ms) {
          CancelPlanning(ctx, plan_id);
          return IOError("Scan planning timed out after {}ms waiting for plan_id={}",
                         elapsed_ms, plan_id);
        }
        if (retry == poll_config.max_retries) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
        delay_ms = std::min(delay_ms * 2, kMaxSleepMs);
        continue;
      }
      case PlanStatus::kFailed:
        CancelPlanning(ctx, plan_id);
        return IOError("Scan planning failed: {}",
                       result.error ? result.error->message : "unknown error");
      case PlanStatus::kCancelled:
        return IOError("Scan planning was cancelled for plan_id={}", plan_id);
    }
  }

  CancelPlanning(ctx, plan_id);
  return IOError("Scan planning exceeded max retries ({}) for plan_id={}",
                 poll_config.max_retries, plan_id);
}

// TODO(sandeepg): Fetch plan tasks concurrently when an executor is configured via
// PlanWith(), as Java's ScanTaskIterable does.
/// A lazy stream that drives FetchScanTasks calls on demand.
///
/// The eager POST /plan (or poll until COMPLETED) is done before this stream is
/// constructed. The stream then yields the directly-returned file_scan_tasks first,
/// then fetches each plan_task token lazily one at a time as Next() is called,
/// queuing any nested plan tasks returned by the server.
class RestFileScanTaskStream final : public FileScanTaskStream {
 public:
  RestFileScanTaskStream(RestScanContext ctx, std::shared_ptr<Schema> schema,
                         std::string plan_id,
                         std::vector<std::shared_ptr<FileScanTask>> initial_tasks,
                         std::vector<std::string> plan_task_tokens, SpecsById specs)
      : ctx_(std::move(ctx)),
        schema_(std::move(schema)),
        plan_id_(std::move(plan_id)),
        buffer_(std::move(initial_tasks)),
        plan_task_tokens_(std::move(plan_task_tokens)),
        specs_(std::move(specs)) {}

  ~RestFileScanTaskStream() override {
    if (!consumed_) CancelPlanning(ctx_, plan_id_);
  }

 protected:
  Result<std::optional<std::shared_ptr<FileScanTask>>> NextImpl() override {
    while (true) {
      if (buffer_pos_ < buffer_.size()) {
        return buffer_[buffer_pos_++];
      }
      if (token_pos_ >= plan_task_tokens_.size()) {
        consumed_ = true;
        return std::nullopt;
      }
      const std::string token = plan_task_tokens_[token_pos_++];
      ICEBERG_ASSIGN_OR_RAISE(auto result,
                              FetchScanTasksOnce(ctx_, *schema_, token, specs_));
      if (result.plan_tasks.has_value()) {
        plan_task_tokens_.insert(plan_task_tokens_.end(), result.plan_tasks->begin(),
                                 result.plan_tasks->end());
      }
      buffer_ = std::move(result.file_scan_tasks)
                    .value_or(std::vector<std::shared_ptr<FileScanTask>>{});
      buffer_pos_ = 0;
    }
  }

 private:
  RestScanContext ctx_;
  std::shared_ptr<Schema> schema_;
  std::string plan_id_;
  std::vector<std::shared_ptr<FileScanTask>> buffer_;
  size_t buffer_pos_ = 0;
  std::vector<std::string> plan_task_tokens_;
  size_t token_pos_ = 0;
  SpecsById specs_;
  bool consumed_ = false;
};

/// POST /plan (polling if SUBMITTED), apply credentials, return a lazy stream.
Result<FileScanTaskStreamPtr> ExecuteScanPlanStream(const RestScanContext& ctx,
                                                    const Schema& schema,
                                                    std::shared_ptr<Schema> schema_ptr,
                                                    PlanTableScanRequest request,
                                                    const SpecsById& specs,
                                                    std::shared_ptr<FileIO>& scan_io) {
  ICEBERG_ENDPOINT_CHECK(ctx.supported_endpoints, Endpoint::PlanTableScan());
  ICEBERG_ASSIGN_OR_RAISE(auto poll_config, ResolvePollConfig(ctx));

  ICEBERG_ASSIGN_OR_RAISE(auto path, ctx.paths->Plan(ctx.identifier));
  ICEBERG_ASSIGN_OR_RAISE(auto request_json, ToJson(request));
  ICEBERG_ASSIGN_OR_RAISE(auto json_request, ToJsonString(request_json));
  ICEBERG_ASSIGN_OR_RAISE(const auto response,
                          ctx.client->Post(path, json_request, /*headers=*/{},
                                           *PlanErrorHandler::Instance(), *ctx.session));
  ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.body()));
  // The parser validates the response, so read the plan id first to cancel on failure.
  std::string raw_plan_id;
  if (auto it = json.find("plan-id"); it != json.end() && it->is_string()) {
    raw_plan_id = it->get<std::string>();
  }
  auto result_or = PlanTableScanResponseFromJson(json, specs, schema);
  if (!result_or) {
    CancelPlanning(ctx, raw_plan_id);
    return ::iceberg::unexpected<Error>(result_or.error());
  }
  auto& result = *result_or;

  const std::string plan_id = result.plan_id;

  if (result.plan_status == PlanStatus::kSubmitted) {
    ICEBERG_ASSIGN_OR_RAISE(
        auto completed,
        FetchPlanningResult(ctx, schema, plan_id, specs, poll_config, scan_io));
    return std::make_unique<RestFileScanTaskStream>(
        ctx, std::move(schema_ptr), plan_id,
        std::move(completed.file_scan_tasks)
            .value_or(std::vector<std::shared_ptr<FileScanTask>>{}),
        std::move(completed.plan_tasks).value_or(std::vector<std::string>{}), specs);
  }

  if (result.plan_status == PlanStatus::kFailed) {
    CancelPlanning(ctx, plan_id);
    return IOError("Scan planning failed: {}",
                   result.error ? result.error->message : "unknown error");
  }
  if (result.plan_status == PlanStatus::kCancelled) {
    return IOError("Scan planning was cancelled for plan_id={}", plan_id);
  }

  // kCompleted: apply credentials from the initial response, then build the lazy stream.
  if (auto s = ApplyStorageCredentials(ctx, plan_id, result.storage_credentials, scan_io);
      !s) {
    CancelPlanning(ctx, plan_id);
    return ::iceberg::unexpected<Error>(s.error());
  }

  std::vector<std::shared_ptr<FileScanTask>> initial_tasks;
  if (result.file_scan_tasks.has_value()) {
    initial_tasks = std::move(*result.file_scan_tasks);
  }
  std::vector<std::string> plan_task_tokens;
  if (result.plan_tasks.has_value()) {
    plan_task_tokens = std::move(*result.plan_tasks);
  }

  return std::make_unique<RestFileScanTaskStream>(ctx, std::move(schema_ptr), plan_id,
                                                  std::move(initial_tasks),
                                                  std::move(plan_task_tokens), specs);
}

/// Eager batch planning: used by RestIncrementalAppendScan which has no stream path.
Result<std::vector<std::shared_ptr<FileScanTask>>> ExecuteScanPlan(
    const RestScanContext& ctx, const Schema& schema, PlanTableScanRequest request,
    const SpecsById& specs, std::shared_ptr<FileIO>& scan_io) {
  ICEBERG_ENDPOINT_CHECK(ctx.supported_endpoints, Endpoint::PlanTableScan());
  ICEBERG_ASSIGN_OR_RAISE(auto poll_config, ResolvePollConfig(ctx));

  ICEBERG_ASSIGN_OR_RAISE(auto path, ctx.paths->Plan(ctx.identifier));
  ICEBERG_ASSIGN_OR_RAISE(auto request_json, ToJson(request));
  ICEBERG_ASSIGN_OR_RAISE(auto json_request, ToJsonString(request_json));
  ICEBERG_ASSIGN_OR_RAISE(const auto response,
                          ctx.client->Post(path, json_request, /*headers=*/{},
                                           *PlanErrorHandler::Instance(), *ctx.session));
  ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.body()));
  // The parser validates the response, so read the plan id first to cancel on failure.
  std::string raw_plan_id;
  if (auto it = json.find("plan-id"); it != json.end() && it->is_string()) {
    raw_plan_id = it->get<std::string>();
  }
  auto result_or = PlanTableScanResponseFromJson(json, specs, schema);
  if (!result_or) {
    CancelPlanning(ctx, raw_plan_id);
    return ::iceberg::unexpected<Error>(result_or.error());
  }
  auto& result = *result_or;

  const std::string plan_id = result.plan_id;

  switch (result.plan_status) {
    case PlanStatus::kCompleted: {
      if (auto s =
              ApplyStorageCredentials(ctx, plan_id, result.storage_credentials, scan_io);
          !s) {
        CancelPlanning(ctx, plan_id);
        return ::iceberg::unexpected<Error>(s.error());
      }
      auto tasks =
          ResolveScanTasks(ctx, schema, result.plan_tasks, result.file_scan_tasks, specs);
      if (!tasks) CancelPlanning(ctx, plan_id);
      return tasks;
    }
    case PlanStatus::kSubmitted: {
      ICEBERG_ASSIGN_OR_RAISE(
          auto completed,
          FetchPlanningResult(ctx, schema, plan_id, specs, poll_config, scan_io));
      auto tasks = ResolveScanTasks(ctx, schema, completed.plan_tasks,
                                    completed.file_scan_tasks, specs);
      if (!tasks) CancelPlanning(ctx, plan_id);
      return tasks;
    }
    case PlanStatus::kFailed:
      CancelPlanning(ctx, plan_id);
      return IOError("Scan planning failed: {}",
                     result.error ? result.error->message : "unknown error");
    case PlanStatus::kCancelled:
      return IOError("Scan planning was cancelled for plan_id={}", plan_id);
  }
  return IOError("Unexpected plan status");
}

}  // namespace

// ---------------------------------------------------------------------------
// RestTableScan
// ---------------------------------------------------------------------------

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

Result<FileScanTaskStreamPtr> RestTableScan::PlanFilesStream() const {
  scan_io_ = nullptr;  // reset so stale credentials from a prior plan are not reused
  TableMetadataCache metadata_cache(metadata_.get());
  ICEBERG_ASSIGN_OR_RAISE(auto specs, metadata_cache.GetPartitionSpecsById());

  PlanTableScanRequest request;
  ICEBERG_ASSIGN_OR_RAISE(auto projected_schema, schema());
  ICEBERG_ASSIGN_OR_RAISE(request.select, ProjectedColumnNames(*projected_schema));
  request.filter = context_.filter;
  request.case_sensitive = context_.case_sensitive;
  request.min_rows_requested = context_.min_rows_requested;

  if (context_.from_snapshot_id.has_value() && context_.to_snapshot_id.has_value()) {
    request.start_snapshot_id = context_.from_snapshot_id;
    request.end_snapshot_id = context_.to_snapshot_id;
    request.use_snapshot_schema = true;
  } else if (context_.snapshot_id.has_value()) {
    request.snapshot_id = context_.snapshot_id;
    request.use_snapshot_schema = context_.use_snapshot_schema;
  }

  if (!context_.columns_to_keep_stats.empty()) {
    for (int32_t field_id : context_.columns_to_keep_stats) {
      ICEBERG_ASSIGN_OR_RAISE(auto name, schema_->FindColumnNameById(field_id));
      if (name.has_value()) {
        request.stats_fields.emplace_back(*name);
      }
    }
  }

  return ExecuteScanPlanStream(rest_context_, *schema_, schema_, std::move(request),
                               specs, scan_io_);
}

const std::shared_ptr<FileIO>& RestTableScan::io() const {
  return scan_io_ ? scan_io_ : io_;
}

// ---------------------------------------------------------------------------
// RestTableScanBuilder
// ---------------------------------------------------------------------------

RestTableScanBuilder::RestTableScanBuilder(
    std::shared_ptr<TableMetadata> metadata, std::shared_ptr<FileIO> io,
    std::string table_name, std::shared_ptr<MetricsReporter> metrics_reporter,
    RestScanContext rest_context)
    : DataTableScanBuilder(std::move(metadata), std::move(io), std::move(table_name),
                           std::move(metrics_reporter)),
      rest_context_(std::move(rest_context)) {}

Result<std::unique_ptr<DataTableScan>> RestTableScanBuilder::Build() {
  ICEBERG_RETURN_UNEXPECTED(CheckErrors());
  ICEBERG_RETURN_UNEXPECTED(ResolveColumnStatsSelection());
  ICEBERG_RETURN_UNEXPECTED(context_.Validate());
  ICEBERG_ASSIGN_OR_RAISE(auto schema, ResolveSnapshotSchema());
  return RestTableScan::Make(metadata_, schema.get(), io_, std::move(context_),
                             rest_context_);
}

// ---------------------------------------------------------------------------
// RestIncrementalAppendScan
// ---------------------------------------------------------------------------

RestIncrementalAppendScan::RestIncrementalAppendScan(
    std::shared_ptr<TableMetadata> metadata, std::shared_ptr<Schema> schema,
    std::shared_ptr<FileIO> io, internal::TableScanContext context,
    RestScanContext rest_context)
    : IncrementalAppendScan(std::move(metadata), std::move(schema), std::move(io),
                            std::move(context)),
      rest_context_(std::move(rest_context)) {}

Result<std::unique_ptr<IncrementalAppendScan>> RestIncrementalAppendScan::Make(
    std::shared_ptr<TableMetadata> metadata, std::shared_ptr<Schema> schema,
    std::shared_ptr<FileIO> io, internal::TableScanContext context,
    RestScanContext rest_context) {
  ICEBERG_PRECHECK(metadata != nullptr, "Table metadata cannot be null");
  ICEBERG_PRECHECK(schema != nullptr, "Schema cannot be null");
  ICEBERG_PRECHECK(io != nullptr, "FileIO cannot be null");
  return std::unique_ptr<IncrementalAppendScan>(
      new RestIncrementalAppendScan(std::move(metadata), std::move(schema), std::move(io),
                                    std::move(context), std::move(rest_context)));
}

Result<std::vector<std::shared_ptr<FileScanTask>>> RestIncrementalAppendScan::PlanFiles(
    std::optional<int64_t> from_snapshot_id_exclusive,
    int64_t to_snapshot_id_inclusive) const {
  // The REST API requires start-snapshot-id for incremental scans.
  if (!from_snapshot_id_exclusive.has_value()) {
    return NotSupported("REST incremental append scan requires a start snapshot");
  }

  scan_io_ = nullptr;  // reset so stale credentials from a prior plan are not reused
  TableMetadataCache metadata_cache(metadata_.get());
  ICEBERG_ASSIGN_OR_RAISE(auto specs, metadata_cache.GetPartitionSpecsById());

  PlanTableScanRequest request;
  ICEBERG_ASSIGN_OR_RAISE(auto projected_schema, schema());
  ICEBERG_ASSIGN_OR_RAISE(request.select, ProjectedColumnNames(*projected_schema));
  request.filter = context_.filter;
  request.case_sensitive = context_.case_sensitive;
  request.min_rows_requested = context_.min_rows_requested;
  request.start_snapshot_id = from_snapshot_id_exclusive;
  request.end_snapshot_id = to_snapshot_id_inclusive;
  request.use_snapshot_schema = true;

  if (!context_.columns_to_keep_stats.empty()) {
    for (int32_t field_id : context_.columns_to_keep_stats) {
      ICEBERG_ASSIGN_OR_RAISE(auto name, schema_->FindColumnNameById(field_id));
      if (name.has_value()) {
        request.stats_fields.emplace_back(*name);
      }
    }
  }

  return ExecuteScanPlan(rest_context_, *schema_, std::move(request), specs, scan_io_);
}

const std::shared_ptr<FileIO>& RestIncrementalAppendScan::io() const {
  return scan_io_ ? scan_io_ : io_;
}

// ---------------------------------------------------------------------------
// RestIncrementalAppendScanBuilder
// ---------------------------------------------------------------------------

RestIncrementalAppendScanBuilder::RestIncrementalAppendScanBuilder(
    std::shared_ptr<TableMetadata> metadata, std::shared_ptr<FileIO> io,
    std::string table_name, std::shared_ptr<MetricsReporter> metrics_reporter,
    RestScanContext rest_context)
    : IncrementalAppendScanBuilder(std::move(metadata), std::move(io),
                                   std::move(table_name), std::move(metrics_reporter)),
      rest_context_(std::move(rest_context)) {}

Result<std::unique_ptr<IncrementalAppendScan>> RestIncrementalAppendScanBuilder::Build() {
  ICEBERG_RETURN_UNEXPECTED(CheckErrors());
  ICEBERG_RETURN_UNEXPECTED(ResolveColumnStatsSelection());
  ICEBERG_RETURN_UNEXPECTED(context_.Validate());
  ICEBERG_ASSIGN_OR_RAISE(auto schema, ResolveSnapshotSchema());
  return RestIncrementalAppendScan::Make(metadata_, schema.get(), io_,
                                         std::move(context_), rest_context_);
}

}  // namespace iceberg::rest
