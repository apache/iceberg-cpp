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

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/catalog/rest/auth/auth_session.h"
#include "iceberg/catalog/rest/endpoint.h"
#include "iceberg/catalog/rest/error_handlers.h"
#include "iceberg/catalog/rest/http_client.h"
#include "iceberg/catalog/rest/resource_paths.h"
#include "iceberg/catalog/rest/rest_table.h"
#include "iceberg/file_io.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_scan.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"

namespace iceberg::rest {

using ::testing::_;
using ::testing::Return;

// --------------------------------------------------------------------------
// Mock HTTP client that overrides the virtual methods of HttpClient.
// The base class constructor creates a cpr::ConnectionPool, which is a
// lightweight allocation (no network connections are opened at construction).
// --------------------------------------------------------------------------
class MockHttpClient : public HttpClient {
 public:
  MockHttpClient() : HttpClient({}) {}

  MOCK_METHOD(Result<HttpResponse>, Get,
              (const std::string& path,
               (const std::unordered_map<std::string, std::string>&) params,
               (const std::unordered_map<std::string, std::string>&) headers,
               const ErrorHandler& error_handler, auth::AuthSession& session),
              (override));

  MOCK_METHOD(Result<HttpResponse>, Post,
              (const std::string& path, const std::string& body,
               (const std::unordered_map<std::string, std::string>&) headers,
               const ErrorHandler& error_handler, auth::AuthSession& session),
              (override));

  MOCK_METHOD(Result<HttpResponse>, Delete,
              (const std::string& path,
               (const std::unordered_map<std::string, std::string>&) params,
               (const std::unordered_map<std::string, std::string>&) headers,
               const ErrorHandler& error_handler, auth::AuthSession& session),
              (override));
};

// --------------------------------------------------------------------------
// Minimal FileIO stub (no real I/O needed for server-side scan planning tests)
// --------------------------------------------------------------------------
class NoOpFileIO : public FileIO {
 public:
  Result<std::string> ReadFile(const std::string&,
                               std::optional<size_t>) override {
    return IOError("NoOpFileIO");
  }
  Status WriteFile(const std::string&, std::string_view) override { return {}; }
  Status DeleteFile(const std::string&) override { return {}; }
};

// --------------------------------------------------------------------------
// Test fixture shared by RestTableScan tests.
// --------------------------------------------------------------------------
class RestTableScanTest : public ::testing::Test {
 protected:
  void SetUp() override {
    schema_ = std::make_shared<Schema>(std::vector<SchemaField>{
        SchemaField::MakeRequired(1, "id", int32()),
        SchemaField::MakeRequired(2, "data", string())});

    auto spec = PartitionSpec::Unpartitioned();

    constexpr int64_t kSnapshotId = 1000L;
    auto snapshot = std::make_shared<Snapshot>(
        Snapshot{.snapshot_id = kSnapshotId,
                 .sequence_number = 1L,
                 .timestamp_ms = TimePointMsFromUnixMs(1609459200000L),
                 .manifest_list = "/tmp/manifest-list.avro",
                 .schema_id = schema_->schema_id()});

    metadata_ = std::make_shared<TableMetadata>(TableMetadata{
        .format_version = 2,
        .table_uuid = "test-uuid",
        .location = "/tmp/table",
        .last_sequence_number = 1L,
        .last_updated_ms = TimePointMsFromUnixMs(1609459200000L),
        .last_column_id = 2,
        .schemas = {schema_},
        .current_schema_id = schema_->schema_id(),
        .partition_specs = {spec},
        .default_spec_id = spec->spec_id(),
        .last_partition_id = 999,
        .current_snapshot_id = kSnapshotId,
        .snapshots = {snapshot},
        .refs = {{"main",
                  std::make_shared<SnapshotRef>(SnapshotRef{
                      .snapshot_id = kSnapshotId,
                      .retention = SnapshotRef::Branch{}})}}});

    file_io_ = std::make_shared<NoOpFileIO>();

    mock_client_ = std::make_shared<MockHttpClient>();

    ICEBERG_UNWRAP_OR_FAIL(paths_, ResourcePaths::Make(
                                       "http://test-server", /*prefix=*/"",
                                       /*namespace_separator=*/"%1F"));

    session_ = auth::AuthSession::MakeDefault(/*headers=*/{});

    identifier_ = TableIdentifier{Namespace{{"default"}}, "my_table"};

    all_plan_endpoints_ = {Endpoint::PlanTableScan(), Endpoint::FetchPlanningResult(),
                           Endpoint::CancelPlanning(), Endpoint::FetchScanTasks()};
  }

  // Pass std::nullopt to get the full set of plan endpoints (default).
  // Pass an explicit set (including empty) to use exactly that set.
  RestScanContext MakeContext(
      std::optional<std::unordered_set<Endpoint>> endpoints = std::nullopt) {
    auto effective =
        endpoints.has_value() ? std::move(*endpoints) : all_plan_endpoints_;
    return RestScanContext{
        .client = mock_client_,
        .paths = paths_,
        .session = session_,
        .supported_endpoints = std::move(effective),
        .identifier = identifier_,
    };
  }

  Result<std::unique_ptr<DataTableScan>> MakeScan(RestScanContext ctx) {
    return RestTableScan::Make(metadata_, schema_, file_io_,
                               internal::TableScanContext{}, std::move(ctx));
  }

  std::shared_ptr<Schema> schema_;
  std::shared_ptr<TableMetadata> metadata_;
  std::shared_ptr<FileIO> file_io_;
  std::shared_ptr<MockHttpClient> mock_client_;
  std::shared_ptr<ResourcePaths> paths_;
  std::shared_ptr<auth::AuthSession> session_;
  TableIdentifier identifier_;
  std::unordered_set<Endpoint> all_plan_endpoints_;
};

// --------------------------------------------------------------------------
// PlanFiles: server returns COMPLETED immediately, no file scan tasks.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, PlanFilesCompleted) {
  constexpr std::string_view kResponseBody = R"({"status":"completed"})";
  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kResponseBody))));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(MakeContext()));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());
}

// --------------------------------------------------------------------------
// PlanFiles: server returns COMPLETED with a non-empty plan-id (still valid).
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, PlanFilesCompletedWithPlanId) {
  constexpr std::string_view kResponseBody =
      R"({"status":"completed","plan-id":"plan-abc"})";
  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kResponseBody))));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(MakeContext()));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());
}

// --------------------------------------------------------------------------
// PlanFiles: server returns SUBMITTED → poll returns COMPLETED.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, PlanFilesSubmittedThenCompleted) {
  constexpr std::string_view kSubmittedBody =
      R"({"status":"submitted","plan-id":"plan-poll-1"})";
  constexpr std::string_view kCompletedBody = R"({"status":"completed"})";

  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kSubmittedBody))));
  EXPECT_CALL(*mock_client_, Get(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kCompletedBody))));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(MakeContext()));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());
}

// --------------------------------------------------------------------------
// PlanFiles: server returns FAILED → scan returns IOError.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, PlanFilesFailed) {
  constexpr std::string_view kFailedBody =
      R"({"status":"failed","error":{"message":"server error","type":"ServerError","code":500}})";
  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kFailedBody))));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(MakeContext()));
  auto result = scan->PlanFiles();
  EXPECT_THAT(result, IsError(ErrorKind::kIOError));
}

// --------------------------------------------------------------------------
// PlanFiles: PlanTableScan endpoint missing → NotSupported error.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, PlanFilesEndpointNotSupported) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto scan, MakeScan(MakeContext(std::unordered_set<Endpoint>{})));
  auto result = scan->PlanFiles();
  EXPECT_THAT(result, IsError(ErrorKind::kNotSupported));
}

// --------------------------------------------------------------------------
// PlanFiles with plan-tasks: server returns COMPLETED with opaque task token,
// then FetchScanTasks is called and returns no file scan tasks.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, PlanFilesWithPlanTasks) {
  constexpr std::string_view kPlanResponse =
      R"({"status":"completed","plan-id":"plan-1","plan-tasks":["tok-1"]})";
  // FetchScanTasksResponse requires at least one of plan-tasks or file-scan-tasks present.
  constexpr std::string_view kTasksResponse = R"({"file-scan-tasks":[]})";

  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kPlanResponse))))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kTasksResponse))));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(MakeContext()));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());
}

// --------------------------------------------------------------------------
// Cancel is called when FetchScanTasks fails after a COMPLETED response that
// included plan-tasks. This mirrors the Java cancelPlan-on-close behavior.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, CancelCalledWhenFetchScanTasksFails) {
  constexpr std::string_view kPlanResponse =
      R"({"status":"completed","plan-id":"plan-cancel-1","plan-tasks":["tok-a"]})";

  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kPlanResponse))))
      .WillOnce(Return(IOError("FetchScanTasks failed")));
  EXPECT_CALL(*mock_client_, Delete(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, "{}")));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(MakeContext()));
  auto result = scan->PlanFiles();
  EXPECT_THAT(result, IsError(ErrorKind::kIOError));
}

// --------------------------------------------------------------------------
// Cancel is a no-op when plan_id is empty (server returned no plan-id).
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, CancelIsNoOpWithEmptyPlanId) {
  constexpr std::string_view kPlanResponse =
      R"({"status":"completed","plan-tasks":["tok-b"]})";

  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kPlanResponse))))
      .WillOnce(Return(IOError("FetchScanTasks failed")));
  EXPECT_CALL(*mock_client_, Delete(_, _, _, _, _)).Times(0);

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(MakeContext()));
  auto result = scan->PlanFiles();
  EXPECT_THAT(result, IsError(ErrorKind::kIOError));
}

// --------------------------------------------------------------------------
// Cancel is a no-op when CancelPlanning endpoint is not advertised.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, CancelIsNoOpWhenEndpointNotAdvertised) {
  constexpr std::string_view kPlanResponse =
      R"({"status":"completed","plan-id":"plan-2","plan-tasks":["tok-c"]})";

  std::unordered_set<Endpoint> endpoints_without_cancel = {
      Endpoint::PlanTableScan(), Endpoint::FetchPlanningResult(),
      Endpoint::FetchScanTasks()};

  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kPlanResponse))))
      .WillOnce(Return(IOError("FetchScanTasks failed")));
  EXPECT_CALL(*mock_client_, Delete(_, _, _, _, _)).Times(0);

  ICEBERG_UNWRAP_OR_FAIL(auto scan,
                         MakeScan(MakeContext(endpoints_without_cancel)));
  auto result = scan->PlanFiles();
  EXPECT_THAT(result, IsError(ErrorKind::kIOError));
}

// --------------------------------------------------------------------------
// FetchPlanningResult: FetchPlanningResult endpoint missing → NotSupported.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, FetchPlanningResultEndpointNotSupported) {
  constexpr std::string_view kSubmittedBody =
      R"({"status":"submitted","plan-id":"plan-3"})";

  std::unordered_set<Endpoint> endpoints_without_fetch = {
      Endpoint::PlanTableScan(), Endpoint::CancelPlanning(),
      Endpoint::FetchScanTasks()};

  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kSubmittedBody))));

  ICEBERG_UNWRAP_OR_FAIL(auto scan,
                         MakeScan(MakeContext(endpoints_without_fetch)));
  auto result = scan->PlanFiles();
  EXPECT_THAT(result, IsError(ErrorKind::kNotSupported));
}

// --------------------------------------------------------------------------
// FetchScanTasks: endpoint missing → NotSupported.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, FetchScanTasksEndpointNotSupported) {
  constexpr std::string_view kPlanResponse =
      R"({"status":"completed","plan-id":"plan-4","plan-tasks":["tok-d"]})";

  std::unordered_set<Endpoint> endpoints_without_tasks = {
      Endpoint::PlanTableScan(), Endpoint::FetchPlanningResult(),
      Endpoint::CancelPlanning()};

  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kPlanResponse))));
  EXPECT_CALL(*mock_client_, Delete(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, "{}")));

  ICEBERG_UNWRAP_OR_FAIL(auto scan,
                         MakeScan(MakeContext(endpoints_without_tasks)));
  auto result = scan->PlanFiles();
  EXPECT_THAT(result, IsError(ErrorKind::kNotSupported));
}

// --------------------------------------------------------------------------
// use_snapshot_schema: UseSnapshot() sets it to true in the builder context.
// RestTableScanBuilder propagates context from DataTableScanBuilder.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, UseSnapshotPropagatesUseSnapshotSchemaInContext) {
  constexpr int64_t kSnapshotId = 1000L;
  RestTableScanBuilder builder(metadata_, file_io_, MakeContext(std::nullopt));
  builder.UseSnapshot(kSnapshotId);
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder.Build());
  EXPECT_TRUE(scan->context().use_snapshot_schema);
}

// --------------------------------------------------------------------------
// use_snapshot_schema: default scan does not set use_snapshot_schema.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, DefaultScanDoesNotSetUseSnapshotSchema) {
  RestTableScanBuilder builder(metadata_, file_io_, MakeContext(std::nullopt));
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder.Build());
  EXPECT_FALSE(scan->context().use_snapshot_schema);
}

// --------------------------------------------------------------------------
// RestTable::NewScan returns a RestTableScanBuilder (not a plain builder).
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, RestTableNewScanReturnsRestTableScanBuilder) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto table,
      RestTable::Make(identifier_, metadata_, "/tmp/metadata.json", file_io_,
                      /*catalog=*/nullptr, MakeContext(std::nullopt)));
  ICEBERG_UNWRAP_OR_FAIL(auto builder, table->NewScan());
  auto* typed = dynamic_cast<RestTableScanBuilder*>(builder.get());
  EXPECT_NE(typed, nullptr);
}

}  // namespace iceberg::rest
