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
#include <deque>
#include <iterator>
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
#include "iceberg/expression/expression.h"
#include "iceberg/json_serde_internal.h"
#include "iceberg/logging/log_macros.h"
#include "iceberg/partition_spec.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/table_metadata.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/string_util.h"
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
  const auto& timeout = RestCatalogProperties::kScanPlanningPollTimeoutMs;
  const auto& retries = RestCatalogProperties::kScanPlanningPollNumRetries;
  PollConfig config{
      .max_wait_ms = timeout.value(),
      .max_retries = retries.value(),
  };

  if (auto it = ctx.catalog_config.find(timeout.key()); it != ctx.catalog_config.end()) {
    ICEBERG_ASSIGN_OR_RAISE(config.max_wait_ms,
                            StringUtils::ParseNumber<int64_t>(it->second));
  }
  ICEBERG_PRECHECK(config.max_wait_ms > 0, "Invalid value for {}: {} (must be positive)",
                   timeout.key(), config.max_wait_ms);

  if (auto it = ctx.catalog_config.find(retries.key()); it != ctx.catalog_config.end()) {
    ICEBERG_ASSIGN_OR_RAISE(config.max_retries,
                            StringUtils::ParseNumber<int32_t>(it->second));
  }
  ICEBERG_PRECHECK(config.max_retries >= 0,
                   "Invalid value for {}: {} (must be non-negative)", retries.key(),
                   config.max_retries);

  return config;
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

// Column names in field ID order.
// FIXME: Sending both "s" and "s.x" asks the server for all of "s".
// Preserve partial struct projections; Java currently has the same issue.
Result<std::vector<std::string>> ProjectedColumnNames(const Schema& projected,
                                                      bool include_struct_ids = true) {
  ICEBERG_ASSIGN_OR_RAISE(auto id_set, GetProjectedIdsVisitor::GetProjectedIds(
                                           projected, include_struct_ids));
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

Result<std::shared_ptr<FileIO>> MakeScanFileIO(
    const RestScanContext& ctx, const std::string& plan_id,
    const std::vector<StorageCredential>& credentials) {
  if (credentials.empty()) return std::shared_ptr<FileIO>{};
  std::shared_ptr<StorageCredentialProvider> provider;
  if (ctx.credential_provider_factory) {
    ICEBERG_ASSIGN_OR_RAISE(provider, ctx.credential_provider_factory(plan_id));
  }
  ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<FileIO> io,
                          MakeTableFileIO(ctx.catalog_config, ctx.table_config,
                                          credentials, std::move(provider)));
  return io;
}

Status CancelPlanning(const RestScanContext& ctx, const std::string& plan_id) {
  if (plan_id.empty()) return {};
  if (!ctx.supported_endpoints.contains(Endpoint::CancelPlanning())) return {};

  ICEBERG_ASSIGN_OR_RAISE(auto path, ctx.paths->Plan(ctx.identifier, plan_id));
  ICEBERG_RETURN_UNEXPECTED(ctx.client->Delete(
      path, /*params=*/{}, /*headers=*/{}, *PlanErrorHandler::Instance(), *ctx.session));
  return {};
}

// Cancels the plan on scope exit unless released.
class PlanCancellationGuard {
 public:
  PlanCancellationGuard(const RestScanContext& ctx, const std::string& plan_id)
      : ctx_(ctx), plan_id_(plan_id) {}

  PlanCancellationGuard(const PlanCancellationGuard&) = delete;
  PlanCancellationGuard& operator=(const PlanCancellationGuard&) = delete;

  ~PlanCancellationGuard() {
    if (!active_) return;
    // Cleanup is best-effort; a missing plan has already been released.
    if (auto status = CancelPlanning(ctx_, plan_id_);
        !status && status.error().kind != ErrorKind::kNoSuchPlanId) {
      ICEBERG_LOG_WARN("Failed to cancel scan plan {}: {}", plan_id_,
                       status.error().message);
    }
  }

  void Release() { active_ = false; }

 private:
  const RestScanContext& ctx_;
  const std::string& plan_id_;
  bool active_ = true;
};

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

// Polls until the plan completes and returns the completed response. Plan tasks
// are left unresolved.
Result<FetchPlanningResultResponse> FetchPlanningResult(
    const RestScanContext& ctx, const Schema& schema, const std::string& plan_id,
    const SpecsById& specs, const PollConfig& poll_config,
    std::shared_ptr<FileIO>& scan_io, PlanCancellationGuard& cleanup) {
  ICEBERG_ENDPOINT_CHECK(ctx.supported_endpoints, Endpoint::FetchPlanningResult());
  ICEBERG_ASSIGN_OR_RAISE(auto path, ctx.paths->Plan(ctx.identifier, plan_id));

  auto delay_ms = kMinSleepMs;
  auto start = std::chrono::steady_clock::now();

  for (int32_t retry = 0; retry <= poll_config.max_retries; ++retry) {
    ICEBERG_ASSIGN_OR_RAISE(auto response,
                            ctx.client->Get(path, /*params=*/{}, /*headers=*/{},
                                            *PlanErrorHandler::Instance(), *ctx.session));
    ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.body()));
    ICEBERG_ASSIGN_OR_RAISE(auto result,
                            FetchPlanningResultResponseFromJson(json, specs, schema));
    ICEBERG_RETURN_UNEXPECTED(result.Validate());

    switch (result.plan_status) {
      case PlanStatus::kCompleted: {
        ICEBERG_ASSIGN_OR_RAISE(scan_io,
                                MakeScanFileIO(ctx, plan_id, result.storage_credentials));
        return result;
      }
      case PlanStatus::kSubmitted: {
        auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - start)
                              .count();
        if (elapsed_ms >= poll_config.max_wait_ms) {
          return IOError("Scan planning timed out after {}ms waiting for plan_id={}",
                         elapsed_ms, plan_id);
        }
        if (retry == poll_config.max_retries) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
        delay_ms = std::min(delay_ms * 2, kMaxSleepMs);
        continue;
      }
      case PlanStatus::kFailed:
        return IOError("Scan planning failed: {}",
                       result.error ? result.error->message : "unknown error");
      case PlanStatus::kCancelled:
        cleanup.Release();
        return IOError("Scan planning was cancelled for plan_id={}", plan_id);
    }
  }

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
  RestFileScanTaskStream(std::shared_ptr<const RestScanContext> ctx,
                         std::shared_ptr<Schema> schema, std::string plan_id,
                         std::vector<std::shared_ptr<FileScanTask>> initial_tasks,
                         std::vector<std::string> plan_task_tokens, SpecsById specs,
                         std::shared_ptr<Expression> filter, bool ignore_residuals)
      : ctx_(std::move(ctx)),
        schema_(std::move(schema)),
        plan_id_(std::move(plan_id)),
        buffer_(std::move(initial_tasks)),
        plan_task_tokens_(std::make_move_iterator(plan_task_tokens.begin()),
                          std::make_move_iterator(plan_task_tokens.end())),
        specs_(std::move(specs)),
        filter_(filter ? std::move(filter) : True::Instance()),
        ignore_residuals_(ignore_residuals),
        cleanup_(*ctx_, plan_id_) {}

  ~RestFileScanTaskStream() override = default;

 protected:
  Result<std::optional<std::shared_ptr<FileScanTask>>> NextImpl() override {
    while (true) {
      if (buffer_pos_ < buffer_.size()) {
        auto task = std::move(buffer_[buffer_pos_++]);
        if (ignore_residuals_ || !task->residual_filter()) {
          // The server may leave row filtering to the client.
          return std::make_shared<FileScanTask>(
              task->data_file(), task->delete_files(),
              ignore_residuals_ ? True::Instance() : filter_);
        }
        return task;
      }
      if (plan_task_tokens_.empty()) {
        cleanup_.Release();
        return std::nullopt;
      }
      const std::string token = std::move(plan_task_tokens_.front());
      plan_task_tokens_.pop_front();
      ICEBERG_ASSIGN_OR_RAISE(auto result,
                              FetchScanTasksOnce(*ctx_, *schema_, token, specs_));
      if (result.plan_tasks.has_value()) {
        plan_task_tokens_.insert(plan_task_tokens_.end(),
                                 std::make_move_iterator(result.plan_tasks->begin()),
                                 std::make_move_iterator(result.plan_tasks->end()));
      }
      buffer_ = std::move(result.file_scan_tasks)
                    .value_or(std::vector<std::shared_ptr<FileScanTask>>{});
      buffer_pos_ = 0;
    }
  }

 private:
  std::shared_ptr<const RestScanContext> ctx_;
  std::shared_ptr<Schema> schema_;
  std::string plan_id_;
  std::vector<std::shared_ptr<FileScanTask>> buffer_;
  size_t buffer_pos_ = 0;
  std::deque<std::string> plan_task_tokens_;
  SpecsById specs_;
  std::shared_ptr<Expression> filter_;
  bool ignore_residuals_;
  PlanCancellationGuard cleanup_;
};

/// POST /plan (polling if SUBMITTED), apply credentials, return a lazy stream.
Result<FileScanTaskStreamPtr> ExecuteScanPlanStream(
    const std::shared_ptr<const RestScanContext>& rest_context, const Schema& schema,
    std::shared_ptr<Schema> schema_ptr, PlanTableScanRequest request,
    const SpecsById& specs, std::shared_ptr<FileIO>& scan_io, bool ignore_residuals) {
  const auto& ctx = *rest_context;
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
  PlanCancellationGuard cleanup(ctx, raw_plan_id);
  ICEBERG_ASSIGN_OR_RAISE(auto result,
                          PlanTableScanResponseFromJson(json, specs, schema));

  const std::string plan_id = result.plan_id;

  if (result.plan_status == PlanStatus::kSubmitted) {
    ICEBERG_ASSIGN_OR_RAISE(
        auto completed,
        FetchPlanningResult(ctx, schema, plan_id, specs, poll_config, scan_io, cleanup));
    FileScanTaskStreamPtr stream = std::make_unique<RestFileScanTaskStream>(
        rest_context, std::move(schema_ptr), plan_id,
        std::move(completed.file_scan_tasks)
            .value_or(std::vector<std::shared_ptr<FileScanTask>>{}),
        std::move(completed.plan_tasks).value_or(std::vector<std::string>{}), specs,
        std::move(request.filter), ignore_residuals);
    cleanup.Release();
    return stream;
  }

  if (result.plan_status == PlanStatus::kFailed) {
    return IOError("Scan planning failed: {}",
                   result.error ? result.error->message : "unknown error");
  }
  if (result.plan_status == PlanStatus::kCancelled) {
    cleanup.Release();
    return IOError("Scan planning was cancelled for plan_id={}", plan_id);
  }

  ICEBERG_CHECK(result.plan_status == PlanStatus::kCompleted,
                "Unexpected scan planning status: {}", ToString(result.plan_status));
  ICEBERG_ASSIGN_OR_RAISE(scan_io,
                          MakeScanFileIO(ctx, plan_id, result.storage_credentials));

  std::vector<std::shared_ptr<FileScanTask>> initial_tasks;
  if (result.file_scan_tasks.has_value()) {
    initial_tasks = std::move(*result.file_scan_tasks);
  }
  std::vector<std::string> plan_task_tokens;
  if (result.plan_tasks.has_value()) {
    plan_task_tokens = std::move(*result.plan_tasks);
  }

  FileScanTaskStreamPtr stream = std::make_unique<RestFileScanTaskStream>(
      rest_context, std::move(schema_ptr), plan_id, std::move(initial_tasks),
      std::move(plan_task_tokens), specs, std::move(request.filter), ignore_residuals);
  cleanup.Release();
  return stream;
}

}  // namespace

// ---------------------------------------------------------------------------
// RestTableScan
// ---------------------------------------------------------------------------

RestTableScan::RestTableScan(std::shared_ptr<TableMetadata> metadata,
                             std::shared_ptr<Schema> schema, std::shared_ptr<FileIO> io,
                             internal::TableScanContext context,
                             std::shared_ptr<const RestScanContext> rest_context)
    : DataTableScan(std::move(metadata), std::move(schema), std::move(io),
                    std::move(context)),
      rest_context_(std::move(rest_context)) {}

Result<std::unique_ptr<DataTableScan>> RestTableScan::Make(
    std::shared_ptr<TableMetadata> metadata, std::shared_ptr<Schema> schema,
    std::shared_ptr<FileIO> io, internal::TableScanContext context,
    std::shared_ptr<const RestScanContext> rest_context) {
  ICEBERG_PRECHECK(metadata != nullptr, "Table metadata cannot be null");
  ICEBERG_PRECHECK(schema != nullptr, "Schema cannot be null");
  ICEBERG_PRECHECK(io != nullptr, "FileIO cannot be null");
  ICEBERG_PRECHECK(rest_context != nullptr, "REST scan context cannot be null");
  ICEBERG_CHECK(
      !context.from_snapshot_id.has_value() && !context.to_snapshot_id.has_value(),
      "Use RestIncrementalAppendScan for incremental scans");
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

  if (context_.snapshot_id.has_value()) {
    request.snapshot_id = context_.snapshot_id;
    request.use_snapshot_schema = context_.use_snapshot_schema;
  }

  if (context_.return_column_stats && context_.columns_to_keep_stats.empty()) {
    ICEBERG_ASSIGN_OR_RAISE(request.stats_fields,
                            ProjectedColumnNames(*schema_, /*include_struct_ids=*/false));
  } else {
    for (int32_t field_id : context_.columns_to_keep_stats) {
      ICEBERG_ASSIGN_OR_RAISE(auto name, schema_->FindColumnNameById(field_id));
      if (name.has_value()) {
        request.stats_fields.emplace_back(*name);
      }
    }
  }

  return ExecuteScanPlanStream(rest_context_, *schema_, schema_, std::move(request),
                               specs, scan_io_, context_.ignore_residuals);
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
    std::shared_ptr<const RestScanContext> rest_context)
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
    std::shared_ptr<const RestScanContext> rest_context)
    : IncrementalAppendScan(std::move(metadata), std::move(schema), std::move(io),
                            std::move(context)),
      rest_context_(std::move(rest_context)) {}

Result<std::unique_ptr<IncrementalAppendScan>> RestIncrementalAppendScan::Make(
    std::shared_ptr<TableMetadata> metadata, std::shared_ptr<Schema> schema,
    std::shared_ptr<FileIO> io, internal::TableScanContext context,
    std::shared_ptr<const RestScanContext> rest_context) {
  ICEBERG_PRECHECK(metadata != nullptr, "Table metadata cannot be null");
  ICEBERG_PRECHECK(schema != nullptr, "Schema cannot be null");
  ICEBERG_PRECHECK(io != nullptr, "FileIO cannot be null");
  ICEBERG_PRECHECK(rest_context != nullptr, "REST scan context cannot be null");
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
  // Match Java's RESTTableScan: schema selection for incremental ranges is
  // unspecified in the REST spec.
  request.use_snapshot_schema = true;

  if (context_.return_column_stats && context_.columns_to_keep_stats.empty()) {
    ICEBERG_ASSIGN_OR_RAISE(request.stats_fields,
                            ProjectedColumnNames(*schema_, /*include_struct_ids=*/false));
  } else {
    for (int32_t field_id : context_.columns_to_keep_stats) {
      ICEBERG_ASSIGN_OR_RAISE(auto name, schema_->FindColumnNameById(field_id));
      if (name.has_value()) {
        request.stats_fields.emplace_back(*name);
      }
    }
  }

  ICEBERG_ASSIGN_OR_RAISE(
      auto stream,
      ExecuteScanPlanStream(rest_context_, *schema_, schema_, std::move(request), specs,
                            scan_io_, context_.ignore_residuals));
  return stream->ToVector();
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
    std::shared_ptr<const RestScanContext> rest_context)
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
