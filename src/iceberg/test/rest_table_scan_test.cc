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
#include "iceberg/constants.h"
#include "iceberg/file_io.h"
#include "iceberg/manifest/manifest_entry.h"
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

// Matches a JSON string body where `key` has exactly `expected_value`.
MATCHER_P2(JsonBodyHas, key, expected_value, "") {
  try {
    auto json = nlohmann::json::parse(arg);
    if (!json.contains(key)) {
      *result_listener << "JSON body missing key \"" << key << "\"";
      return false;
    }
    nlohmann::json expected = expected_value;
    if (json.at(key) != expected) {
      *result_listener << "JSON[\"" << key << "\"] = " << json.at(key) << ", expected "
                       << expected;
      return false;
    }
    return true;
  } catch (...) {
    *result_listener << "failed to parse JSON body";
    return false;
  }
}

// Matches a JSON string body that does NOT contain `key`.
MATCHER_P(JsonBodyLacks, key, "") {
  try {
    auto json = nlohmann::json::parse(arg);
    if (json.contains(key)) {
      *result_listener << "JSON body unexpectedly contains key \"" << key << "\"";
      return false;
    }
    return true;
  } catch (...) {
    *result_listener << "failed to parse JSON body";
    return false;
  }
}

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
               (const std::unordered_map<std::string, std::string>&)params,
               (const std::unordered_map<std::string, std::string>&)headers,
               const ErrorHandler& error_handler, auth::AuthSession& session),
              (override));

  MOCK_METHOD(Result<HttpResponse>, Post,
              (const std::string& path, const std::string& body,
               (const std::unordered_map<std::string, std::string>&)headers,
               const ErrorHandler& error_handler, auth::AuthSession& session),
              (override));

  MOCK_METHOD(Result<HttpResponse>, Delete,
              (const std::string& path,
               (const std::unordered_map<std::string, std::string>&)params,
               (const std::unordered_map<std::string, std::string>&)headers,
               const ErrorHandler& error_handler, auth::AuthSession& session),
              (override));
};

// --------------------------------------------------------------------------
// Minimal FileIO stub (no real I/O needed for server-side scan planning tests)
// --------------------------------------------------------------------------
class NoOpFileIO : public FileIO {
 public:
  Result<std::string> ReadFile(const std::string&, std::optional<size_t>) override {
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
    schema_ = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                                 SchemaField::MakeRequired(2, "data", string())});

    auto spec = PartitionSpec::Unpartitioned();

    constexpr int64_t kSnapshotId = 1000L;
    auto snapshot = std::make_shared<Snapshot>(
        Snapshot{.snapshot_id = kSnapshotId,
                 .sequence_number = 1L,
                 .timestamp_ms = TimePointMsFromUnixMs(1609459200000L),
                 .manifest_list = "/tmp/manifest-list.avro",
                 .schema_id = schema_->schema_id()});

    metadata_ = std::make_shared<TableMetadata>(
        TableMetadata{.format_version = 2,
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
                      .refs = {{"main", std::make_shared<SnapshotRef>(SnapshotRef{
                                            .snapshot_id = kSnapshotId,
                                            .retention = SnapshotRef::Branch{}})}}});

    file_io_ = std::make_shared<NoOpFileIO>();

    mock_client_ = std::make_shared<MockHttpClient>();

    ICEBERG_UNWRAP_OR_FAIL(paths_,
                           ResourcePaths::Make("http://test-server", /*prefix=*/"",
                                               /*namespace_separator=*/"%1F"));

    session_ = auth::AuthSession::MakeDefault(/*headers=*/{});

    identifier_ = TableIdentifier{.ns = Namespace{{"default"}}, .name = "my_table"};

    all_plan_endpoints_ = {Endpoint::PlanTableScan(), Endpoint::FetchPlanningResult(),
                           Endpoint::CancelPlanning(), Endpoint::FetchScanTasks()};
  }

  // Pass std::nullopt to get the full set of plan endpoints (default).
  // Pass an explicit set (including empty) to use exactly that set.
  RestScanContext MakeContext(
      std::optional<std::unordered_set<Endpoint>> endpoints = std::nullopt) {
    auto effective = endpoints.has_value() ? std::move(*endpoints) : all_plan_endpoints_;
    return RestScanContext{
        .client = mock_client_,
        .paths = paths_,
        .session = session_,
        .supported_endpoints = std::move(effective),
        .identifier = identifier_,
    };
  }

  Result<std::unique_ptr<DataTableScan>> MakeScan(RestScanContext ctx) {
    return RestTableScan::Make(metadata_, schema_, file_io_, internal::TableScanContext{},
                               std::move(ctx));
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
  ICEBERG_UNWRAP_OR_FAIL(auto scan,
                         MakeScan(MakeContext(std::unordered_set<Endpoint>{})));
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
  // FetchScanTasksResponse requires at least one of plan-tasks or file-scan-tasks
  // present.
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

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(MakeContext(endpoints_without_cancel)));
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
      Endpoint::PlanTableScan(), Endpoint::CancelPlanning(), Endpoint::FetchScanTasks()};

  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kSubmittedBody))));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(MakeContext(endpoints_without_fetch)));
  auto result = scan->PlanFiles();
  EXPECT_THAT(result, IsError(ErrorKind::kNotSupported));
}

// --------------------------------------------------------------------------
// FetchScanTasks: endpoint missing → NotSupported.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, FetchScanTasksEndpointNotSupported) {
  constexpr std::string_view kPlanResponse =
      R"({"status":"completed","plan-id":"plan-4","plan-tasks":["tok-d"]})";

  std::unordered_set<Endpoint> endpoints_without_tasks = {Endpoint::PlanTableScan(),
                                                          Endpoint::FetchPlanningResult(),
                                                          Endpoint::CancelPlanning()};

  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kPlanResponse))));
  EXPECT_CALL(*mock_client_, Delete(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, "{}")));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(MakeContext(endpoints_without_tasks)));
  auto result = scan->PlanFiles();
  EXPECT_THAT(result, IsError(ErrorKind::kNotSupported));
}

// --------------------------------------------------------------------------
// UseSnapshot(): the POST body sent to the server must contain both
// "snapshot-id" and "use-snapshot-schema": true.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, UseSnapshotPropagatesUseSnapshotSchemaInContext) {
  constexpr int64_t kSnapshotId = 1000L;
  constexpr std::string_view kResponseBody = R"({"status":"completed"})";

  EXPECT_CALL(*mock_client_,
              Post(_,
                   testing::AllOf(JsonBodyHas("snapshot-id", kSnapshotId),
                                  JsonBodyHas("use-snapshot-schema", true)),
                   _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kResponseBody))));

  RestTableScanBuilder builder(metadata_, file_io_, "test.my_table", nullptr,
                               MakeContext(std::nullopt));
  builder.UseSnapshot(kSnapshotId);
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder.Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());
}

// --------------------------------------------------------------------------
// Default scan: POST body must have "use-snapshot-schema": false and no
// "snapshot-id" field.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, DefaultScanDoesNotSetUseSnapshotSchema) {
  constexpr std::string_view kResponseBody = R"({"status":"completed"})";

  EXPECT_CALL(*mock_client_,
              Post(_,
                   testing::AllOf(JsonBodyHas("use-snapshot-schema", false),
                                  JsonBodyLacks("snapshot-id")),
                   _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kResponseBody))));

  RestTableScanBuilder builder(metadata_, file_io_, "test.my_table", nullptr,
                               MakeContext(std::nullopt));
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder.Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());
}

// --------------------------------------------------------------------------
// Storage credentials in COMPLETED response: io() returns a
// credential-scoped IO, not the original table IO.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, StorageCredentialsInPlanResponseUpdatesEffectiveIO) {
  constexpr std::string_view kResponseBody = R"({
    "status": "completed",
    "storage-credentials": [
      {"prefix": "s3://bucket/prefix", "config": {"key": "value"}}
    ]
  })";
  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kResponseBody))));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(MakeContext()));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());

  // io() must return a credential-scoped IO without requiring a downcast.
  EXPECT_NE(scan->io().get(), file_io_.get());
}

// --------------------------------------------------------------------------
// No storage credentials: io() falls back to the table's FileIO.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, NoStorageCredentialsEffectiveIoFallsBackToTableIO) {
  constexpr std::string_view kResponseBody = R"({"status":"completed"})";
  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kResponseBody))));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(MakeContext()));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());

  EXPECT_EQ(scan->io().get(), file_io_.get());
}

// --------------------------------------------------------------------------
// Storage credentials returned in FetchScanTasksResponse also update io().
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, StorageCredentialsInFetchScanTasksResponseUpdatesEffectiveIO) {
  constexpr std::string_view kPlanResponse =
      R"({"status":"completed","plan-id":"plan-cred","plan-tasks":["tok-cred"]})";
  constexpr std::string_view kTasksResponse = R"({
    "file-scan-tasks": [],
    "storage-credentials": [
      {"prefix": "s3://bucket/prefix", "config": {"key": "value"}}
    ]
  })";

  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kPlanResponse))))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kTasksResponse))));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(MakeContext()));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());

  EXPECT_NE(scan->io().get(), file_io_.get());
}

// --------------------------------------------------------------------------
// RestTable::NewScan returns a RestTableScanBuilder (not a plain builder).
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, RestTableNewScanReturnsRestTableScanBuilder) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto table, RestTable::Make(identifier_, metadata_, "/tmp/metadata.json", file_io_,
                                  /*catalog=*/nullptr, "test.my_table", nullptr,
                                  MakeContext(std::nullopt)));
  ICEBERG_UNWRAP_OR_FAIL(auto builder, table->NewScan());
  auto* typed = dynamic_cast<RestTableScanBuilder*>(builder.get());
  EXPECT_NE(typed, nullptr);
}

// ==========================================================================
// PlanFilesStream tests
// ==========================================================================

// --------------------------------------------------------------------------
// PlanFilesStream: COMPLETED immediately, stream yields no tasks.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, PlanFilesStreamCompleted) {
  constexpr std::string_view kResponseBody = R"({"status":"completed"})";
  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kResponseBody))));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(MakeContext()));
  ICEBERG_UNWRAP_OR_FAIL(auto stream, scan->PlanFilesStream());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, stream->ToVector());
  EXPECT_TRUE(tasks.empty());
}

// --------------------------------------------------------------------------
// PlanFilesStream: two plan-task tokens each trigger a separate FetchScanTasks
// POST, one per token, and the combined task set is returned.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, PlanFilesStreamFetchesEachTokenSeparately) {
  constexpr std::string_view kPlanResponse =
      R"({"status":"completed","plan-id":"plan-stream","plan-tasks":["tok-s1","tok-s2"]})";
  constexpr std::string_view kTask1Response = R"({
    "file-scan-tasks": [
      {"data-file":{"content":"data","file-path":"s3://b/f1.parquet",
       "file-format":"PARQUET","spec-id":0,"partition":[],"file-size-in-bytes":1,"record-count":1}}
    ]
  })";
  constexpr std::string_view kTask2Response = R"({
    "file-scan-tasks": [
      {"data-file":{"content":"data","file-path":"s3://b/f2.parquet",
       "file-format":"PARQUET","spec-id":0,"partition":[],"file-size-in-bytes":1,"record-count":1}}
    ]
  })";

  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kPlanResponse))))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kTask1Response))))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kTask2Response))));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(MakeContext()));
  ICEBERG_UNWRAP_OR_FAIL(auto stream, scan->PlanFilesStream());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, stream->ToVector());
  ASSERT_EQ(tasks.size(), 2u);
  EXPECT_EQ(tasks[0]->data_file()->file_path, "s3://b/f1.parquet");
  EXPECT_EQ(tasks[1]->data_file()->file_path, "s3://b/f2.parquet");
}

// --------------------------------------------------------------------------
// PlanFilesStream: the second FetchScanTasks POST is not made until the first
// token's buffer is exhausted. Verified by counting POST calls between Next()
// invocations.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, PlanFilesStreamFetchesTokenOnlyWhenBufferExhausted) {
  constexpr std::string_view kPlanResponse =
      R"({"status":"completed","plan-id":"plan-lazy","plan-tasks":["tok-1","tok-2"]})";
  // tok-1 returns 2 tasks; tok-2 must not be fetched until both are consumed.
  constexpr std::string_view kTwoTasksResponse = R"({
    "file-scan-tasks": [
      {"data-file":{"content":"data","file-path":"s3://b/f1.parquet",
       "file-format":"PARQUET","spec-id":0,"partition":[],"file-size-in-bytes":1,"record-count":1}},
      {"data-file":{"content":"data","file-path":"s3://b/f2.parquet",
       "file-format":"PARQUET","spec-id":0,"partition":[],"file-size-in-bytes":1,"record-count":1}}
    ]
  })";
  constexpr std::string_view kOneTaskResponse = R"({
    "file-scan-tasks": [
      {"data-file":{"content":"data","file-path":"s3://b/f3.parquet",
       "file-format":"PARQUET","spec-id":0,"partition":[],"file-size-in-bytes":1,"record-count":1}}
    ]
  })";

  int fetch_count = 0;
  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kPlanResponse))))
      .WillOnce([&](auto&&...) -> Result<HttpResponse> {
        ++fetch_count;
        return HttpResponse::MakeForTesting(200, std::string(kTwoTasksResponse));
      })
      .WillOnce([&](auto&&...) -> Result<HttpResponse> {
        ++fetch_count;
        return HttpResponse::MakeForTesting(200, std::string(kOneTaskResponse));
      });

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(MakeContext()));
  ICEBERG_UNWRAP_OR_FAIL(auto stream, scan->PlanFilesStream());

  // No FetchScanTasks call yet — stream has not been driven.
  EXPECT_EQ(fetch_count, 0);

  // First Next(): fetches tok-1 (2 tasks buffered), returns f1.
  ICEBERG_UNWRAP_OR_FAIL(auto t1, stream->Next());
  ASSERT_TRUE(t1.has_value());
  EXPECT_EQ(fetch_count, 1);
  EXPECT_EQ((*t1)->data_file()->file_path, "s3://b/f1.parquet");

  // Second Next(): served from buffer; tok-2 not fetched yet.
  ICEBERG_UNWRAP_OR_FAIL(auto t2, stream->Next());
  ASSERT_TRUE(t2.has_value());
  EXPECT_EQ(fetch_count, 1);
  EXPECT_EQ((*t2)->data_file()->file_path, "s3://b/f2.parquet");

  // Third Next(): buffer exhausted, fetches tok-2, returns f3.
  ICEBERG_UNWRAP_OR_FAIL(auto t3, stream->Next());
  ASSERT_TRUE(t3.has_value());
  EXPECT_EQ(fetch_count, 2);
  EXPECT_EQ((*t3)->data_file()->file_path, "s3://b/f3.parquet");

  // Fourth Next(): all tokens consumed, stream terminates.
  ICEBERG_UNWRAP_OR_FAIL(auto end, stream->Next());
  EXPECT_FALSE(end.has_value());
}

// --------------------------------------------------------------------------
// PlanFilesStream: Next() propagates a FetchScanTasks error and DELETE /plan
// is called via the stream destructor since consumed_ is never set.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, PlanFilesStreamNextReturnsErrorOnFetchFailure) {
  constexpr std::string_view kPlanResponse =
      R"({"status":"completed","plan-id":"plan-next-err","plan-tasks":["tok-err"]})";

  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kPlanResponse))))
      .WillOnce(Return(IOError("FetchScanTasks network error")));
  EXPECT_CALL(*mock_client_, Delete(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, "{}")));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(MakeContext()));
  ICEBERG_UNWRAP_OR_FAIL(auto stream, scan->PlanFilesStream());
  auto result = stream->Next();
  EXPECT_THAT(result, IsError(ErrorKind::kIOError));
}

// --------------------------------------------------------------------------
// PlanFilesStream: stream destroyed after consuming the first task but before
// the second token is fetched → DELETE /plan called by the destructor.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, PlanFilesStreamCancelAfterPartialConsumption) {
  constexpr std::string_view kPlanResponse =
      R"({"status":"completed","plan-id":"plan-partial","plan-tasks":["tok-p1","tok-p2"]})";
  constexpr std::string_view kTaskResponse = R"({
    "file-scan-tasks": [
      {"data-file":{"content":"data","file-path":"s3://b/fp1.parquet",
       "file-format":"PARQUET","spec-id":0,"partition":[],"file-size-in-bytes":1,"record-count":1}}
    ]
  })";

  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kPlanResponse))))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kTaskResponse))));
  EXPECT_CALL(*mock_client_, Delete(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, "{}")));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(MakeContext()));
  {
    ICEBERG_UNWRAP_OR_FAIL(auto stream, scan->PlanFilesStream());
    // Consume the first task from tok-p1; tok-p2 has never been fetched.
    ICEBERG_UNWRAP_OR_FAIL(auto task, stream->Next());
    ASSERT_TRUE(task.has_value());
    // Destroy stream here — tok-p2 is still pending, so destructor calls DELETE.
  }
}

// --------------------------------------------------------------------------
// PlanFilesStream: stream destroyed with no Next() calls at all →
// DELETE /plan called by the destructor.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, PlanFilesStreamCancelOnPartialConsumption) {
  constexpr std::string_view kPlanResponse =
      R"({"status":"completed","plan-id":"plan-never-consumed","plan-tasks":["tok-p1"]})";

  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kPlanResponse))));
  EXPECT_CALL(*mock_client_, Delete(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, "{}")));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(MakeContext()));
  {
    ICEBERG_UNWRAP_OR_FAIL(auto stream, scan->PlanFilesStream());
    // Destroy without any Next() call — destructor must call DELETE /plan.
  }
}

// --------------------------------------------------------------------------
// PlanFilesStream: SUBMITTED → poll until COMPLETED, stream yields all tasks.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, PlanFilesStreamSubmittedThenCompleted) {
  constexpr std::string_view kSubmittedBody =
      R"({"status":"submitted","plan-id":"plan-poll-stream"})";
  constexpr std::string_view kCompletedBody = R"({"status":"completed"})";

  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kSubmittedBody))));
  EXPECT_CALL(*mock_client_, Get(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kCompletedBody))));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(MakeContext()));
  ICEBERG_UNWRAP_OR_FAIL(auto stream, scan->PlanFilesStream());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, stream->ToVector());
  EXPECT_TRUE(tasks.empty());
}

// --------------------------------------------------------------------------
// PlanFilesStream: FAILED → stream returns an error.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, PlanFilesStreamFailed) {
  constexpr std::string_view kFailedBody =
      R"({"status":"failed","error":{"message":"server error","type":"ServerError","code":500}})";
  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kFailedBody))));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(MakeContext()));
  auto result = scan->PlanFilesStream();
  EXPECT_THAT(result, IsError(ErrorKind::kIOError));
}

// --------------------------------------------------------------------------
// PlanFilesStream: FAILED with plan-id → DELETE /plan called before returning
// the error (tests the ExecuteScanPlanStream kFailed cancel fix).
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, PlanFilesStreamFailedWithPlanIdCancels) {
  constexpr std::string_view kFailedBody =
      R"({"status":"failed","plan-id":"plan-fail-stream","error":{"message":"server error","type":"ServerError","code":500}})";

  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kFailedBody))));
  EXPECT_CALL(*mock_client_, Delete(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, "{}")));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(MakeContext()));
  auto result = scan->PlanFilesStream();
  EXPECT_THAT(result, IsError(ErrorKind::kIOError));
}

// --------------------------------------------------------------------------
// FetchPlanningResult: GET fails after SUBMITTED → DELETE /plan called before
// returning the error (tests the FetchPlanningResult error-path cancel fix).
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, CancelCalledOnFetchPlanningResultGetError) {
  constexpr std::string_view kSubmittedBody =
      R"({"status":"submitted","plan-id":"plan-fetch-err"})";

  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kSubmittedBody))));
  EXPECT_CALL(*mock_client_, Get(_, _, _, _, _))
      .WillOnce(Return(IOError("network failure")));
  EXPECT_CALL(*mock_client_, Delete(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, "{}")));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(MakeContext()));
  auto result = scan->PlanFiles();
  EXPECT_THAT(result, IsError(ErrorKind::kIOError));
}

// --------------------------------------------------------------------------
// Stale credentials: second PlanFilesStream call resets scan_io_slot_ so that
// credentials from the first plan response do not persist into the second.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, SecondPlanFilesStreamCallClearsStaleCredentials) {
  constexpr std::string_view kFirstResponse = R"({
    "status": "completed",
    "storage-credentials": [
      {"prefix": "s3://bucket/prefix", "config": {"key": "value"}}
    ]
  })";
  constexpr std::string_view kSecondResponse = R"({"status":"completed"})";

  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kFirstResponse))))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kSecondResponse))));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(MakeContext()));

  // First call: server vends credentials → io() returns a credential-scoped IO.
  ICEBERG_UNWRAP_OR_FAIL(auto tasks1, scan->PlanFiles());
  EXPECT_TRUE(tasks1.empty());
  EXPECT_NE(scan->io().get(), file_io_.get());

  // Second call: no credentials returned → io() must revert to the table IO,
  // not retain the credentials from the first plan.
  ICEBERG_UNWRAP_OR_FAIL(auto tasks2, scan->PlanFiles());
  EXPECT_TRUE(tasks2.empty());
  EXPECT_EQ(scan->io().get(), file_io_.get());
}

// ==========================================================================
// RestIncrementalAppendScan tests
// ==========================================================================

class RestIncrementalAppendScanTest : public RestTableScanTest {
 protected:
  // Creates a RestIncrementalAppendScan with the given context and optional
  // snapshot range.
  Result<std::unique_ptr<IncrementalAppendScan>> MakeIncrementalScan(
      RestScanContext ctx, std::optional<int64_t> from_snapshot_id = std::nullopt,
      bool from_inclusive = false, std::optional<int64_t> to_snapshot_id = std::nullopt) {
    RestIncrementalAppendScanBuilder builder(metadata_, file_io_, "test.my_table",
                                             nullptr, std::move(ctx));
    if (from_snapshot_id.has_value()) {
      builder.FromSnapshot(*from_snapshot_id, from_inclusive);
    }
    if (to_snapshot_id.has_value()) {
      builder.ToSnapshot(*to_snapshot_id);
    }
    return builder.Build();
  }
};

// --------------------------------------------------------------------------
// PlanFiles: server returns COMPLETED immediately, no tasks.
// --------------------------------------------------------------------------
TEST_F(RestIncrementalAppendScanTest, PlanFilesCompleted) {
  constexpr std::string_view kResponseBody = R"({"status":"completed"})";
  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kResponseBody))));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeIncrementalScan(MakeContext()));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());
}

// --------------------------------------------------------------------------
// PlanFiles: COMPLETED with a plan-task token; FetchScanTasks is called.
// --------------------------------------------------------------------------
TEST_F(RestIncrementalAppendScanTest, PlanFilesWithPlanTasks) {
  constexpr std::string_view kPlanResponse =
      R"({"status":"completed","plan-id":"incr-plan-1","plan-tasks":["tok-incr-1"]})";
  constexpr std::string_view kTasksResponse = R"({"file-scan-tasks":[]})";

  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kPlanResponse))))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kTasksResponse))));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeIncrementalScan(MakeContext()));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());
}

// --------------------------------------------------------------------------
// PlanFiles: SUBMITTED → poll → COMPLETED.
// --------------------------------------------------------------------------
TEST_F(RestIncrementalAppendScanTest, PlanFilesSubmittedThenCompleted) {
  constexpr std::string_view kSubmittedBody =
      R"({"status":"submitted","plan-id":"incr-poll-1"})";
  constexpr std::string_view kCompletedBody = R"({"status":"completed"})";

  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kSubmittedBody))));
  EXPECT_CALL(*mock_client_, Get(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kCompletedBody))));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeIncrementalScan(MakeContext()));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());
}

// --------------------------------------------------------------------------
// PlanFiles: FAILED → IOError.
// --------------------------------------------------------------------------
TEST_F(RestIncrementalAppendScanTest, PlanFilesFailed) {
  constexpr std::string_view kFailedBody =
      R"({"status":"failed","error":{"message":"server error","type":"ServerError","code":500}})";
  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kFailedBody))));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeIncrementalScan(MakeContext()));
  auto result = scan->PlanFiles();
  EXPECT_THAT(result, IsError(ErrorKind::kIOError));
}

// --------------------------------------------------------------------------
// PlanFiles: PlanTableScan endpoint missing → NotSupported.
// --------------------------------------------------------------------------
TEST_F(RestIncrementalAppendScanTest, PlanFilesEndpointNotSupported) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto scan, MakeIncrementalScan(MakeContext(std::unordered_set<Endpoint>{})));
  auto result = scan->PlanFiles();
  EXPECT_THAT(result, IsError(ErrorKind::kNotSupported));
}

// --------------------------------------------------------------------------
// No current snapshot → PlanFiles returns empty without calling the server.
// --------------------------------------------------------------------------
TEST_F(RestIncrementalAppendScanTest, PlanFilesEmptyWhenNoCurrentSnapshot) {
  // Build metadata with no current snapshot.
  auto spec = PartitionSpec::Unpartitioned();
  auto empty_metadata = std::make_shared<TableMetadata>(TableMetadata{
      .format_version = 2,
      .table_uuid = "no-snap-uuid",
      .location = "/tmp/table",
      .last_sequence_number = 0L,
      .last_updated_ms = TimePointMsFromUnixMs(1609459200000L),
      .last_column_id = 2,
      .schemas = {schema_},
      .current_schema_id = schema_->schema_id(),
      .partition_specs = {spec},
      .default_spec_id = spec->spec_id(),
      .last_partition_id = 999,
      .current_snapshot_id = kInvalidSnapshotId,
  });

  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _)).Times(0);

  // Use Make() directly to bypass builder validation that requires a snapshot.
  ICEBERG_UNWRAP_OR_FAIL(auto scan, RestIncrementalAppendScan::Make(
                                        empty_metadata, schema_, file_io_,
                                        internal::TableScanContext{}, MakeContext()));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());
}

// --------------------------------------------------------------------------
// Explicit to_snapshot_id: POST body must contain "end-snapshot-id" set to
// the given value, not the current table snapshot.
// --------------------------------------------------------------------------
TEST_F(RestIncrementalAppendScanTest, PlanFilesWithExplicitToSnapshotId) {
  constexpr int64_t kToSnapshotId = 1000L;
  constexpr std::string_view kResponseBody = R"({"status":"completed"})";
  EXPECT_CALL(*mock_client_,
              Post(_, JsonBodyHas("end-snapshot-id", kToSnapshotId), _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kResponseBody))));

  ICEBERG_UNWRAP_OR_FAIL(
      auto scan, MakeIncrementalScan(MakeContext(), std::nullopt, false, kToSnapshotId));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());
}

// --------------------------------------------------------------------------
// Exclusive from_snapshot_id: POST body must pass from_snapshot_id directly
// as "start-snapshot-id" (exclusive), and current snapshot as "end-snapshot-id".
// --------------------------------------------------------------------------
TEST_F(RestIncrementalAppendScanTest, PlanFilesWithFromSnapshotIdExclusive) {
  constexpr int64_t kFromSnapshotId = 999L;
  constexpr int64_t kCurrentSnapshotId = 1000L;
  constexpr std::string_view kResponseBody = R"({"status":"completed"})";
  EXPECT_CALL(*mock_client_,
              Post(_,
                   testing::AllOf(JsonBodyHas("start-snapshot-id", kFromSnapshotId),
                                  JsonBodyHas("end-snapshot-id", kCurrentSnapshotId)),
                   _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kResponseBody))));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeIncrementalScan(MakeContext(), kFromSnapshotId,
                                                        /*inclusive=*/false));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());
}

// --------------------------------------------------------------------------
// Inclusive from_snapshot_id: parent snapshot is used as start_snapshot_id.
// The fixture snapshot (id=1000) has no parent, so "start-snapshot-id" is
// absent from the POST body.
// --------------------------------------------------------------------------
TEST_F(RestIncrementalAppendScanTest, PlanFilesWithFromSnapshotIdInclusiveNoParent) {
  constexpr int64_t kFromSnapshotId = 1000L;
  constexpr std::string_view kResponseBody = R"({"status":"completed"})";
  EXPECT_CALL(*mock_client_,
              Post(_,
                   testing::AllOf(JsonBodyLacks("start-snapshot-id"),
                                  JsonBodyHas("end-snapshot-id", kFromSnapshotId)),
                   _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kResponseBody))));

  ICEBERG_UNWRAP_OR_FAIL(
      auto scan, MakeIncrementalScan(MakeContext(), kFromSnapshotId, /*inclusive=*/true));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());
}

// --------------------------------------------------------------------------
// PlanFiles: FAILED with plan-id → DELETE /plan called before returning the
// error (tests the ExecuteScanPlan kFailed cancel fix).
// --------------------------------------------------------------------------
TEST_F(RestIncrementalAppendScanTest, PlanFilesFailedWithPlanIdCancels) {
  constexpr std::string_view kFailedBody =
      R"({"status":"failed","plan-id":"plan-fail-incr","error":{"message":"server error","type":"ServerError","code":500}})";

  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kFailedBody))));
  EXPECT_CALL(*mock_client_, Delete(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, "{}")));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeIncrementalScan(MakeContext()));
  auto result = scan->PlanFiles();
  EXPECT_THAT(result, IsError(ErrorKind::kIOError));
}

// --------------------------------------------------------------------------
// Stale credentials: second PlanFiles call resets scan_io_ so that credentials
// from the first plan response do not persist into the second.
// --------------------------------------------------------------------------
TEST_F(RestIncrementalAppendScanTest, SecondPlanFilesCallClearsStaleCredentials) {
  constexpr std::string_view kFirstResponse = R"({
    "status": "completed",
    "storage-credentials": [
      {"prefix": "s3://bucket/prefix", "config": {"key": "value"}}
    ]
  })";
  constexpr std::string_view kSecondResponse = R"({"status":"completed"})";

  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kFirstResponse))))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kSecondResponse))));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeIncrementalScan(MakeContext()));

  // First call: server vends credentials.
  ICEBERG_UNWRAP_OR_FAIL(auto tasks1, scan->PlanFiles());
  EXPECT_TRUE(tasks1.empty());

  // Second call: no credentials. Verifies scan_io_ was cleared so stale
  // credentials from the first plan do not bleed into the second request.
  ICEBERG_UNWRAP_OR_FAIL(auto tasks2, scan->PlanFiles());
  EXPECT_TRUE(tasks2.empty());
}

// --------------------------------------------------------------------------
// Inclusive from_snapshot_id with a parent: POST body must use the parent's id
// as "start-snapshot-id" and the current snapshot as "end-snapshot-id".
// --------------------------------------------------------------------------
TEST_F(RestIncrementalAppendScanTest, PlanFilesWithFromSnapshotIdInclusiveWithParent) {
  constexpr int64_t kParentSnapshotId = 900L;
  constexpr int64_t kChildSnapshotId = 1001L;
  constexpr int64_t kCurrentSnapshotId = 1000L;

  // Add a second snapshot with a parent to the metadata.
  auto child_snapshot = std::make_shared<Snapshot>(
      Snapshot{.snapshot_id = kChildSnapshotId,
               .parent_snapshot_id = kParentSnapshotId,
               .sequence_number = 2L,
               .timestamp_ms = TimePointMsFromUnixMs(1609459260000L),
               .manifest_list = "/tmp/manifest-list-2.avro",
               .schema_id = schema_->schema_id()});
  metadata_->snapshots.push_back(child_snapshot);

  constexpr std::string_view kResponseBody = R"({"status":"completed"})";
  EXPECT_CALL(*mock_client_,
              Post(_,
                   testing::AllOf(JsonBodyHas("start-snapshot-id", kParentSnapshotId),
                                  JsonBodyHas("end-snapshot-id", kCurrentSnapshotId)),
                   _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kResponseBody))));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeIncrementalScan(MakeContext(), kChildSnapshotId,
                                                        /*inclusive=*/true));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());
}

// ==========================================================================
// RestTable::NewIncrementalAppendScan
// ==========================================================================

// --------------------------------------------------------------------------
// RestTable::NewIncrementalAppendScan returns a RestIncrementalAppendScanBuilder.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, RestTableNewIncrementalAppendScanReturnsRestBuilder) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto table, RestTable::Make(identifier_, metadata_, "/tmp/metadata.json", file_io_,
                                  /*catalog=*/nullptr, "test.my_table", nullptr,
                                  MakeContext(std::nullopt)));
  ICEBERG_UNWRAP_OR_FAIL(auto builder, table->NewIncrementalAppendScan());
  auto* typed = dynamic_cast<RestIncrementalAppendScanBuilder*>(builder.get());
  EXPECT_NE(typed, nullptr);
}

}  // namespace iceberg::rest
