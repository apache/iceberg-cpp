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
#include "iceberg/catalog/rest/rest_file_io.h"
#include "iceberg/catalog/rest/types.h"
#include "iceberg/json_serde_internal.h"
#include "iceberg/partition_spec.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
#include "iceberg/util/macros.h"

namespace iceberg::rest {

namespace {

constexpr int64_t kMinSleepMs = 1'000;
constexpr int64_t kMaxSleepMs = 60'000;
constexpr int kMaxRetries = 10;
constexpr int64_t kMaxWaitTimeMs = 5 * 60 * 1'000;

using SpecsById = std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>;
// A shared slot that lets the scan and the stream share a single FileIO reference.
// Credentials vended by any lazy FetchScanTasks response are written into *slot,
// making them visible to RestTableScan::io() even after the stream is consumed.
using ScanIoSlot = std::shared_ptr<std::shared_ptr<FileIO>>;

#define ICEBERG_ENDPOINT_CHECK(endpoints, endpoint)                           \
  do {                                                                        \
    if (!endpoints.contains(endpoint)) {                                      \
      return NotSupported("Not supported endpoint: {}", endpoint.ToString()); \
    }                                                                         \
  } while (0)

// ---------------------------------------------------------------------------
// Shared HTTP scan planning helpers used by all REST scan implementations.
// ---------------------------------------------------------------------------

Status ApplyStorageCredentials(const RestScanContext& ctx,
                               const std::vector<StorageCredential>& credentials,
                               std::shared_ptr<FileIO>& scan_io) {
  if (credentials.empty()) return {};
  ICEBERG_ASSIGN_OR_RAISE(
      auto io, MakeTableFileIO(ctx.catalog_config, ctx.table_config, credentials));
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
    const SpecsById& specs, std::shared_ptr<FileIO>& scan_io);

Result<std::vector<std::shared_ptr<FileScanTask>>> ResolveScanTasks(
    const RestScanContext& ctx, const Schema& schema,
    const std::optional<std::vector<std::string>>& plan_tasks,
    const std::optional<std::vector<std::shared_ptr<FileScanTask>>>& file_scan_tasks,
    const SpecsById& specs, std::shared_ptr<FileIO>& scan_io) {
  std::vector<std::shared_ptr<FileScanTask>> result;

  if (file_scan_tasks.has_value()) {
    result.insert(result.end(), file_scan_tasks->begin(), file_scan_tasks->end());
  }

  if (plan_tasks.has_value()) {
    for (const auto& token : *plan_tasks) {
      ICEBERG_ASSIGN_OR_RAISE(auto tasks, FetchScanTasks(ctx, schema, token, specs, scan_io));
      result.insert(result.end(), tasks.begin(), tasks.end());
    }
  }

  return result;
}

Result<std::vector<std::shared_ptr<FileScanTask>>> FetchScanTasks(
    const RestScanContext& ctx, const Schema& schema, const std::string& plan_task,
    const SpecsById& specs, std::shared_ptr<FileIO>& scan_io) {
  ICEBERG_ENDPOINT_CHECK(ctx.supported_endpoints, Endpoint::FetchScanTasks());

  ICEBERG_ASSIGN_OR_RAISE(auto path, ctx.paths->FetchScanTasks(ctx.identifier));
  FetchScanTasksRequest request{.planTask = plan_task};
  ICEBERG_ASSIGN_OR_RAISE(auto json_request, ToJsonString(ToJson(request)));
  ICEBERG_ASSIGN_OR_RAISE(
      const auto response,
      ctx.client->Post(path, json_request, /*headers=*/{},
                       *PlanTaskErrorHandler::Instance(), *ctx.session));
  ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.body()));
  ICEBERG_ASSIGN_OR_RAISE(auto result, FetchScanTasksResponseFromJson(json, specs, schema));
  ICEBERG_RETURN_UNEXPECTED(result.Validate());
  ICEBERG_RETURN_UNEXPECTED(ApplyStorageCredentials(ctx, result.storage_credentials, scan_io));

  return ResolveScanTasks(ctx, schema, result.plan_tasks, result.file_scan_tasks, specs,
                          scan_io);
}

Result<std::vector<std::shared_ptr<FileScanTask>>> FetchPlanningResult(
    const RestScanContext& ctx, const Schema& schema, const std::string& plan_id,
    const SpecsById& specs, std::shared_ptr<FileIO>& scan_io) {
  ICEBERG_ENDPOINT_CHECK(ctx.supported_endpoints, Endpoint::FetchPlanningResult());

  ICEBERG_ASSIGN_OR_RAISE(auto path, ctx.paths->Plan(ctx.identifier, plan_id));

  auto delay_ms = kMinSleepMs;
  auto start = std::chrono::steady_clock::now();

  for (int retry = 0; retry <= kMaxRetries; ++retry) {
    ICEBERG_ASSIGN_OR_RAISE(
        const auto response,
        ctx.client->Get(path, /*params=*/{}, /*headers=*/{},
                        *PlanErrorHandler::Instance(), *ctx.session));
    ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.body()));
    ICEBERG_ASSIGN_OR_RAISE(auto result,
                            FetchPlanningResultResponseFromJson(json, specs, schema));
    ICEBERG_RETURN_UNEXPECTED(result.Validate());

    switch (result.plan_status) {
      case PlanStatus::kCompleted: {
        ICEBERG_RETURN_UNEXPECTED(
            ApplyStorageCredentials(ctx, result.storage_credentials, scan_io));
        auto tasks =
            ResolveScanTasks(ctx, schema, result.plan_tasks, result.file_scan_tasks, specs,
                             scan_io);
        if (!tasks.has_value()) CancelPlanning(ctx, plan_id);
        return tasks;
      }
      case PlanStatus::kSubmitted: {
        auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - start)
                              .count();
        if (elapsed_ms >= kMaxWaitTimeMs) {
          CancelPlanning(ctx, plan_id);
          return IOError("Scan planning timed out after {}ms waiting for plan_id={}",
                         elapsed_ms, plan_id);
        }
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
  return IOError("Scan planning exceeded max retries ({}) for plan_id={}", kMaxRetries,
                 plan_id);
}

/// A lazy stream that drives FetchScanTasks calls on demand.
///
/// The eager POST /plan (or poll until COMPLETED) is done before this stream is
/// constructed. The stream then yields the directly-returned file_scan_tasks first,
/// then fetches each plan_task token lazily one at a time as Next() is called.
///
/// scan_io_slot is shared with the owning RestTableScan so that credentials vended
/// by any FetchScanTasks response are visible through RestTableScan::io() even after
/// the stream has been consumed.
class RestFileScanTaskStream final : public FileScanTaskStream {
 public:
  RestFileScanTaskStream(RestScanContext ctx, std::shared_ptr<Schema> schema,
                         std::string plan_id,
                         std::vector<std::shared_ptr<FileScanTask>> initial_tasks,
                         std::vector<std::string> plan_task_tokens, SpecsById specs,
                         ScanIoSlot scan_io_slot)
      : ctx_(std::move(ctx)),
        schema_(std::move(schema)),
        plan_id_(std::move(plan_id)),
        buffer_(std::move(initial_tasks)),
        plan_task_tokens_(std::move(plan_task_tokens)),
        specs_(std::move(specs)),
        scan_io_slot_(std::move(scan_io_slot)) {}

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
      const auto& token = plan_task_tokens_[token_pos_++];
      ICEBERG_ASSIGN_OR_RAISE(
          buffer_, FetchScanTasks(ctx_, *schema_, token, specs_, *scan_io_slot_));
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
  ScanIoSlot scan_io_slot_;
  bool consumed_ = false;
};

/// POST /plan (polling if SUBMITTED), apply credentials, return a lazy stream.
///
/// scan_io_slot is shared with the caller (RestTableScan) so that credentials
/// vended by FetchScanTasks responses update RestTableScan::io() in place.
Result<FileScanTaskStreamPtr> ExecuteScanPlanStream(const RestScanContext& ctx,
                                                    const Schema& schema,
                                                    std::shared_ptr<Schema> schema_ptr,
                                                    PlanTableScanRequest request,
                                                    const SpecsById& specs,
                                                    ScanIoSlot scan_io_slot) {
  ICEBERG_ENDPOINT_CHECK(ctx.supported_endpoints, Endpoint::PlanTableScan());

  ICEBERG_ASSIGN_OR_RAISE(auto path, ctx.paths->Plan(ctx.identifier));
  ICEBERG_ASSIGN_OR_RAISE(auto request_json, ToJson(request));
  ICEBERG_ASSIGN_OR_RAISE(auto json_request, ToJsonString(request_json));
  ICEBERG_ASSIGN_OR_RAISE(
      const auto response,
      ctx.client->Post(path, json_request, /*headers=*/{}, *PlanErrorHandler::Instance(),
                       *ctx.session));
  ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.body()));
  ICEBERG_ASSIGN_OR_RAISE(auto result, PlanTableScanResponseFromJson(json, specs, schema));
  ICEBERG_RETURN_UNEXPECTED(result.Validate());

  const std::string plan_id = result.plan_id;

  if (result.plan_status == PlanStatus::kSubmitted) {
    // Poll until COMPLETED, eagerly collecting all tasks into the buffer.
    std::string mutable_plan_id = plan_id;
    ICEBERG_ASSIGN_OR_RAISE(
        auto tasks, FetchPlanningResult(ctx, schema, mutable_plan_id, specs, *scan_io_slot));
    return std::make_unique<RestFileScanTaskStream>(ctx, schema_ptr, mutable_plan_id,
                                                   std::move(tasks), {}, specs, scan_io_slot);
  }

  if (result.plan_status == PlanStatus::kFailed) {
    return IOError("Scan planning failed: {}",
                   result.error ? result.error->message : "unknown error");
  }
  if (result.plan_status == PlanStatus::kCancelled) {
    return IOError("Scan planning was cancelled for plan_id={}", plan_id);
  }

  // kCompleted: apply credentials from the initial response, then build the lazy stream.
  ICEBERG_RETURN_UNEXPECTED(ApplyStorageCredentials(ctx, result.storage_credentials, *scan_io_slot));

  std::vector<std::shared_ptr<FileScanTask>> initial_tasks;
  if (result.file_scan_tasks.has_value()) {
    initial_tasks = std::move(*result.file_scan_tasks);
  }
  std::vector<std::string> plan_task_tokens;
  if (result.plan_tasks.has_value()) {
    plan_task_tokens = std::move(*result.plan_tasks);
  }

  return std::make_unique<RestFileScanTaskStream>(
      ctx, std::move(schema_ptr), plan_id, std::move(initial_tasks),
      std::move(plan_task_tokens), specs, std::move(scan_io_slot));
}

/// Eager batch planning: used by RestIncrementalAppendScan which has no stream path.
Result<std::vector<std::shared_ptr<FileScanTask>>> ExecuteScanPlan(
    const RestScanContext& ctx, const Schema& schema, PlanTableScanRequest request,
    const SpecsById& specs, std::shared_ptr<FileIO>& scan_io) {
  ICEBERG_ENDPOINT_CHECK(ctx.supported_endpoints, Endpoint::PlanTableScan());

  ICEBERG_ASSIGN_OR_RAISE(auto path, ctx.paths->Plan(ctx.identifier));
  ICEBERG_ASSIGN_OR_RAISE(auto request_json, ToJson(request));
  ICEBERG_ASSIGN_OR_RAISE(auto json_request, ToJsonString(request_json));
  ICEBERG_ASSIGN_OR_RAISE(
      const auto response,
      ctx.client->Post(path, json_request, /*headers=*/{}, *PlanErrorHandler::Instance(),
                       *ctx.session));
  ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(response.body()));
  ICEBERG_ASSIGN_OR_RAISE(auto result, PlanTableScanResponseFromJson(json, specs, schema));
  ICEBERG_RETURN_UNEXPECTED(result.Validate());

  const std::string plan_id = result.plan_id;

  switch (result.plan_status) {
    case PlanStatus::kCompleted: {
      ICEBERG_RETURN_UNEXPECTED(
          ApplyStorageCredentials(ctx, result.storage_credentials, scan_io));
      auto tasks =
          ResolveScanTasks(ctx, schema, result.plan_tasks, result.file_scan_tasks, specs,
                           scan_io);
      if (!tasks.has_value()) CancelPlanning(ctx, plan_id);
      return tasks;
    }
    case PlanStatus::kSubmitted:
      return FetchPlanningResult(ctx, schema, plan_id, specs, scan_io);
    case PlanStatus::kFailed:
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
      rest_context_(std::move(rest_context)),
      scan_io_slot_(std::make_shared<std::shared_ptr<FileIO>>()) {}

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
  TableMetadataCache metadata_cache(metadata_.get());
  ICEBERG_ASSIGN_OR_RAISE(auto specs, metadata_cache.GetPartitionSpecsById());

  PlanTableScanRequest request;
  request.select = context_.selected_columns.value_or(std::vector<std::string>{});
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

  return ExecuteScanPlanStream(rest_context_, *schema_, schema_, std::move(request), specs,
                               scan_io_slot_);
}

const std::shared_ptr<FileIO>& RestTableScan::io() const {
  return *scan_io_slot_ ? *scan_io_slot_ : io_;
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
  return std::unique_ptr<IncrementalAppendScan>(new RestIncrementalAppendScan(
      std::move(metadata), std::move(schema), std::move(io), std::move(context),
      std::move(rest_context)));
}

Result<std::vector<std::shared_ptr<FileScanTask>>> RestIncrementalAppendScan::PlanFiles()
    const {
  TableMetadataCache metadata_cache(metadata_.get());
  ICEBERG_ASSIGN_OR_RAISE(auto specs, metadata_cache.GetPartitionSpecsById());

  PlanTableScanRequest request;
  request.select = context_.selected_columns.value_or(std::vector<std::string>{});
  request.filter = context_.filter;
  request.case_sensitive = context_.case_sensitive;
  request.min_rows_requested = context_.min_rows_requested;

  // Resolve end snapshot: use to_snapshot_id if set, else current table snapshot.
  if (context_.to_snapshot_id.has_value()) {
    request.end_snapshot_id = context_.to_snapshot_id;
  } else {
    ICEBERG_ASSIGN_OR_RAISE(auto snapshot, metadata_->Snapshot());
    if (!snapshot) return {};
    request.end_snapshot_id = snapshot->snapshot_id;
  }

  // Resolve start snapshot (exclusive): respect from_snapshot_id_inclusive.
  if (context_.from_snapshot_id.has_value()) {
    if (context_.from_snapshot_id_inclusive) {
      ICEBERG_ASSIGN_OR_RAISE(auto from_snap,
                              metadata_->SnapshotById(*context_.from_snapshot_id));
      request.start_snapshot_id = from_snap->parent_snapshot_id;
    } else {
      request.start_snapshot_id = context_.from_snapshot_id;
    }
  }

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
  ICEBERG_RETURN_UNEXPECTED(context_.Validate());
  ICEBERG_ASSIGN_OR_RAISE(auto schema, ResolveSnapshotSchema());
  return RestIncrementalAppendScan::Make(metadata_, schema.get(), io_, std::move(context_),
                                         rest_context_);
}

}  // namespace iceberg::rest
