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

#include <array>
#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/catalog/rest/auth/auth_session.h"
#include "iceberg/catalog/rest/endpoint.h"
#include "iceberg/catalog/rest/error_handlers.h"
#include "iceberg/catalog/rest/http_client.h"
#include "iceberg/catalog/rest/resource_paths.h"
#include "iceberg/catalog/rest/rest_table.h"
#include "iceberg/catalog/rest/rest_table_scan.h"
#include "iceberg/constants.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/expression/json_serde_internal.h"
#include "iceberg/expression/literal.h"
#include "iceberg/file_io.h"
#include "iceberg/file_io_registry.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/storage_credential.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_scan.h"
#include "iceberg/test/executor.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"

namespace iceberg::rest {

namespace {

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

namespace {

// Records the credentials and provider installed on the scan FileIO.
std::vector<StorageCredential> installed_credentials;
std::shared_ptr<StorageCredentialProvider> installed_provider;

class CredentialedFileIO : public NoOpFileIO, public SupportsStorageCredentials {
 public:
  Status InitializeStorageCredentials(
      const std::vector<StorageCredential>& credentials,
      std::shared_ptr<StorageCredentialProvider> provider) override {
    installed_credentials = credentials;
    installed_provider = std::move(provider);
    return {};
  }

  Status SetStorageCredentials(
      const std::vector<StorageCredential>& credentials) override {
    installed_credentials = credentials;
    return {};
  }

  std::vector<StorageCredential> credentials() const override {
    return installed_credentials;
  }

  SupportsStorageCredentials* AsSupportsStorageCredentials() override { return this; }
};

constexpr std::string_view kCredentialedFileIO = "rest-table-scan-test-credentialed";

nlohmann::json FileTaskJson(const std::string& path) {
  return {{"data-file",
           {{"content", "data"},
            {"file-path", path},
            {"file-format", "PARQUET"},
            {"spec-id", 0},
            {"partition", nlohmann::json::array()},
            {"file-size-in-bytes", 100},
            {"record-count", 10}}}};
}

}  // namespace

// --------------------------------------------------------------------------
// Test fixture shared by RestTableScan tests.
// --------------------------------------------------------------------------
class RestTableScanTest : public ::testing::Test {
 protected:
  void TearDown() override { installed_provider.reset(); }

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

  static void RegisterCredentialedFileIO() {
    FileIORegistry::Register(
        std::string(kCredentialedFileIO),
        {.create = [](const std::unordered_map<std::string, std::string>&)
             -> Result<std::unique_ptr<FileIO>> {
          return std::make_unique<CredentialedFileIO>();
        }});
    installed_credentials.clear();
    installed_provider = nullptr;
  }

  Result<std::unique_ptr<DataTableScan>> MakeScan(RestScanContext ctx) {
    return RestTableScan::Make(metadata_, schema_, file_io_, internal::TableScanContext{},
                               std::make_shared<const RestScanContext>(std::move(ctx)));
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
// Cancel is a no-op when plan_id is empty (server returned no plan-id).
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, CancelIsNoOpWithEmptyPlanId) {
  constexpr std::string_view kPlanResponse =
      R"({"status":"completed","plan-tasks":["tok-b"]})";

  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse(200, std::string(kPlanResponse))))
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
      .WillOnce(Return(HttpResponse(200, std::string(kPlanResponse))))
      .WillOnce(Return(IOError("FetchScanTasks failed")));
  EXPECT_CALL(*mock_client_, Delete(_, _, _, _, _)).Times(0);

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(MakeContext(endpoints_without_cancel)));
  auto result = scan->PlanFiles();
  EXPECT_THAT(result, IsError(ErrorKind::kIOError));
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
      .WillOnce(Return(HttpResponse(200, std::string(kResponseBody))));

  RestTableScanBuilder builder(
      metadata_, file_io_, "test.my_table", nullptr,
      std::make_shared<const RestScanContext>(MakeContext(std::nullopt)));
  builder.UseSnapshot(kSnapshotId);
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder.Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());
}

// --------------------------------------------------------------------------
// Select("*") sends every column, not a literal "*".
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, SelectAllColumnsSendsAllColumns) {
  constexpr std::string_view kResponseBody = R"({"status":"completed"})";
  EXPECT_CALL(
      *mock_client_,
      Post(_, JsonBodyHas("select", std::vector<std::string>{"id", "data"}), _, _, _))
      .WillOnce(Return(HttpResponse(200, std::string(kResponseBody))));

  RestTableScanBuilder builder(metadata_, file_io_, "test.my_table", nullptr,
                               std::make_shared<const RestScanContext>(MakeContext()));
  builder.Select({std::string(Schema::kAllColumns)});
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder.Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());
}

// --------------------------------------------------------------------------
// UseRef(): branches use the table schema, tags use the snapshot schema.
// --------------------------------------------------------------------------
class RestTableScanRefSchemaTest : public RestTableScanTest {
 protected:
  static constexpr int64_t kEvolvedSnapshotId = 2000L;

  void SetUp() override {
    RestTableScanTest::SetUp();
    evolved_schema_ = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(10, "id", int32()),
                                 SchemaField::MakeRequired(11, "data", string())},
        /*schema_id=*/1);
    metadata_->schemas.push_back(evolved_schema_);
    metadata_->last_column_id = 11;
    metadata_->snapshots.push_back(std::make_shared<Snapshot>(
        Snapshot{.snapshot_id = kEvolvedSnapshotId,
                 .parent_snapshot_id = 1000L,
                 .sequence_number = 2L,
                 .timestamp_ms = TimePointMsFromUnixMs(1609459201000L),
                 .manifest_list = "/tmp/manifest-list-2.avro",
                 .schema_id = evolved_schema_->schema_id()}));
    metadata_->refs["evolved-branch"] = std::make_shared<SnapshotRef>(SnapshotRef{
        .snapshot_id = kEvolvedSnapshotId, .retention = SnapshotRef::Branch{}});
    metadata_->refs["evolved-tag"] = std::make_shared<SnapshotRef>(
        SnapshotRef{.snapshot_id = kEvolvedSnapshotId, .retention = SnapshotRef::Tag{}});
  }

  std::shared_ptr<Schema> evolved_schema_;
};

TEST_F(RestTableScanRefSchemaTest, UseRefBranchUsesTableSchema) {
  constexpr std::string_view kResponseBody = R"({"status":"completed"})";
  EXPECT_CALL(*mock_client_,
              Post(_,
                   testing::AllOf(JsonBodyHas("snapshot-id", kEvolvedSnapshotId),
                                  JsonBodyHas("use-snapshot-schema", false)),
                   _, _, _))
      .WillOnce(Return(HttpResponse(200, std::string(kResponseBody))));

  RestTableScanBuilder builder(metadata_, file_io_, "test.my_table", nullptr,
                               std::make_shared<const RestScanContext>(MakeContext()));
  builder.UseRef("evolved-branch");
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder.Build());
  ICEBERG_UNWRAP_OR_FAIL(auto scan_schema, scan->schema());
  EXPECT_EQ(scan_schema->schema_id(), schema_->schema_id());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());
}

TEST_F(RestTableScanRefSchemaTest, UseRefTagUsesSnapshotSchema) {
  constexpr std::string_view kResponseBody = R"({"status":"completed"})";
  EXPECT_CALL(*mock_client_,
              Post(_,
                   testing::AllOf(JsonBodyHas("snapshot-id", kEvolvedSnapshotId),
                                  JsonBodyHas("use-snapshot-schema", true)),
                   _, _, _))
      .WillOnce(Return(HttpResponse(200, std::string(kResponseBody))));

  RestTableScanBuilder builder(metadata_, file_io_, "test.my_table", nullptr,
                               std::make_shared<const RestScanContext>(MakeContext()));
  builder.UseRef("evolved-tag");
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder.Build());
  ICEBERG_UNWRAP_OR_FAIL(auto scan_schema, scan->schema());
  EXPECT_EQ(scan_schema->schema_id(), evolved_schema_->schema_id());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());
}

// --------------------------------------------------------------------------
// Storage credentials in a FetchScanTasks response are ignored.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, StorageCredentialsInFetchScanTasksResponseAreIgnored) {
  constexpr std::string_view kPlanResponse =
      R"({"status":"completed","plan-id":"plan-cred","plan-tasks":["tok-cred"]})";
  constexpr std::string_view kTasksResponse = R"({
    "file-scan-tasks": [],
    "storage-credentials": [
      {"prefix": "s3://bucket/prefix", "config": {"key": "value"}}
    ]
  })";

  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse(200, std::string(kPlanResponse))))
      .WillOnce(Return(HttpResponse(200, std::string(kTasksResponse))));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(MakeContext()));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());

  EXPECT_EQ(scan->io().get(), file_io_.get());
}

// --------------------------------------------------------------------------
// PlanFilesStream: Next() propagates a FetchScanTasks error and DELETE /plan
// is called via the stream destructor since consumed_ is never set.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, PlanFilesStreamNextReturnsErrorOnFetchFailure) {
  constexpr std::string_view kPlanResponse =
      R"({"status":"completed","plan-id":"plan-next-err","plan-tasks":["tok-err"]})";

  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse(200, std::string(kPlanResponse))))
      .WillOnce(Return(IOError("FetchScanTasks network error")));
  EXPECT_CALL(*mock_client_, Delete(_, _, _, _, _))
      .WillOnce(Return(HttpResponse(200, "{}")));

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
      .WillOnce(Return(HttpResponse(200, std::string(kPlanResponse))))
      .WillOnce(Return(HttpResponse(200, std::string(kTaskResponse))));
  EXPECT_CALL(*mock_client_, Delete(_, _, _, _, _))
      .WillOnce(Return(HttpResponse(200, "{}")));

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
// PlanFilesStream: nested plan tasks returned by FetchScanTasks are fetched
// only when the stream reaches them.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, PlanFilesStreamFetchesNestedPlanTasksLazily) {
  constexpr std::string_view kPlanResponse =
      R"({"status":"completed","plan-id":"plan-nested","plan-tasks":["tok-1"]})";
  constexpr std::string_view kTask1Response = R"({
    "plan-tasks": ["tok-nested"],
    "file-scan-tasks": [
      {"data-file":{"content":"data","file-path":"s3://b/f1.parquet",
       "file-format":"PARQUET","spec-id":0,"partition":[],"file-size-in-bytes":1,"record-count":1}}
    ]
  })";
  constexpr std::string_view kNestedResponse = R"({
    "file-scan-tasks": [
      {"data-file":{"content":"data","file-path":"s3://b/f2.parquet",
       "file-format":"PARQUET","spec-id":0,"partition":[],"file-size-in-bytes":1,"record-count":1}}
    ]
  })";

  int fetch_count = 0;
  EXPECT_CALL(*mock_client_, Post(::testing::EndsWith("/plan"), _, _, _, _))
      .WillOnce(Return(HttpResponse(200, std::string(kPlanResponse))));
  EXPECT_CALL(*mock_client_, Post(::testing::EndsWith("/tasks"),
                                  JsonBodyHas("plan-task", "tok-1"), _, _, _))
      .WillOnce([&](auto&&...) -> Result<HttpResponse> {
        ++fetch_count;
        return HttpResponse(200, std::string(kTask1Response));
      });
  EXPECT_CALL(*mock_client_, Post(::testing::EndsWith("/tasks"),
                                  JsonBodyHas("plan-task", "tok-nested"), _, _, _))
      .WillOnce([&](auto&&...) -> Result<HttpResponse> {
        ++fetch_count;
        return HttpResponse(200, std::string(kNestedResponse));
      });

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(MakeContext()));
  ICEBERG_UNWRAP_OR_FAIL(auto stream, scan->PlanFilesStream());

  ICEBERG_UNWRAP_OR_FAIL(auto t1, stream->Next());
  ASSERT_TRUE(t1.has_value());
  EXPECT_EQ(fetch_count, 1);
  EXPECT_EQ((*t1)->data_file()->file_path, "s3://b/f1.parquet");

  ICEBERG_UNWRAP_OR_FAIL(auto t2, stream->Next());
  ASSERT_TRUE(t2.has_value());
  EXPECT_EQ(fetch_count, 2);
  EXPECT_EQ((*t2)->data_file()->file_path, "s3://b/f2.parquet");

  ICEBERG_UNWRAP_OR_FAIL(auto end, stream->Next());
  EXPECT_FALSE(end.has_value());
}

// --------------------------------------------------------------------------
// PlanWith(): plan tasks are fetched serially and the executor is not used.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, PlanFilesStreamIgnoresPlanExecutor) {
  constexpr std::string_view kPlanResponse =
      R"({"status":"completed","plan-id":"plan-exec","plan-tasks":["tok-1","tok-2"]})";
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
      .WillOnce(Return(HttpResponse(200, std::string(kPlanResponse))))
      .WillOnce(Return(HttpResponse(200, std::string(kTask1Response))))
      .WillOnce(Return(HttpResponse(200, std::string(kTask2Response))));

  test::ThreadExecutor executor;
  RestTableScanBuilder builder(metadata_, file_io_, "test.my_table", nullptr,
                               std::make_shared<const RestScanContext>(MakeContext()));
  builder.PlanWith(executor);
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder.Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());

  ASSERT_EQ(tasks.size(), 2);
  EXPECT_EQ(tasks[0]->data_file()->file_path, "s3://b/f1.parquet");
  EXPECT_EQ(tasks[1]->data_file()->file_path, "s3://b/f2.parquet");
  EXPECT_EQ(executor.submit_count(), 0);
}

// --------------------------------------------------------------------------
// poll-num-retries=0: a single GET for a SUBMITTED plan, then cancel.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, PlanFilesStreamPollNumRetriesZero) {
  constexpr std::string_view kSubmittedBody =
      R"({"status":"submitted","plan-id":"plan-no-retry"})";
  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse(200, std::string(kSubmittedBody))));
  EXPECT_CALL(*mock_client_, Get(_, _, _, _, _))
      .WillOnce(Return(HttpResponse(200, std::string(kSubmittedBody))));
  EXPECT_CALL(*mock_client_, Delete(_, _, _, _, _))
      .WillOnce(Return(HttpResponse(200, "{}")));

  auto ctx = MakeContext();
  ctx.catalog_config = {{"rest-scan-planning.poll-num-retries", "0"}};
  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(std::move(ctx)));
  EXPECT_THAT(scan->PlanFilesStream(),
              ::testing::AllOf(IsError(ErrorKind::kIOError),
                               HasErrorMessage("exceeded max retries (0)")));
}

// --------------------------------------------------------------------------
// Without a provider factory, vended credentials are installed as static.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, StorageCredentialsWithoutFactoryInstallNoProvider) {
  constexpr std::string_view kPlanResponse = R"({
    "status": "completed",
    "plan-id": "plan-static",
    "storage-credentials": [{"prefix": "s3://bucket", "config": {"k": "v"}}]
  })";
  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse(200, std::string(kPlanResponse))));
  RegisterCredentialedFileIO();

  auto ctx = MakeContext();
  ctx.catalog_config = {{"io-impl", std::string(kCredentialedFileIO)}};
  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(std::move(ctx)));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());

  EXPECT_EQ(installed_provider, nullptr);
  EXPECT_EQ(installed_credentials.size(), 1);
}

// --------------------------------------------------------------------------
// FetchPlanningResult: GET fails after SUBMITTED → DELETE /plan called before
// returning the error (tests the FetchPlanningResult error-path cancel fix).
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, CancelCalledOnFetchPlanningResultGetError) {
  constexpr std::string_view kSubmittedBody =
      R"({"status":"submitted","plan-id":"plan-fetch-err"})";

  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse(200, std::string(kSubmittedBody))));
  EXPECT_CALL(*mock_client_, Get(_, _, _, _, _))
      .WillOnce(Return(IOError("network failure")));
  EXPECT_CALL(*mock_client_, Delete(_, _, _, _, _))
      .WillOnce(Return(IOError("cancel failure")));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(MakeContext()));
  auto result = scan->PlanFiles();
  EXPECT_THAT(result, ::testing::AllOf(IsError(ErrorKind::kIOError),
                                       HasErrorMessage("network failure")));
}

// ==========================================================================
// RestIncrementalAppendScan tests
// ==========================================================================
// Lineage: 100 <- 200 <- 300 (main), and 200 <- 400 (branch "audit").
class RestIncrementalAppendScanTest : public RestTableScanTest {
 protected:
  static constexpr int64_t kRootId = 100L;
  static constexpr int64_t kMiddleId = 200L;
  static constexpr int64_t kMainHeadId = 300L;
  static constexpr int64_t kBranchHeadId = 400L;
  static constexpr std::string_view kBranch = "audit";

  void SetUp() override {
    RestTableScanTest::SetUp();

    auto make_snapshot = [&](int64_t id, std::optional<int64_t> parent_id,
                             int64_t sequence_number) {
      return std::make_shared<Snapshot>(
          Snapshot{.snapshot_id = id,
                   .parent_snapshot_id = parent_id,
                   .sequence_number = sequence_number,
                   .timestamp_ms = TimePointMsFromUnixMs(1609459200000L + id),
                   .manifest_list = "/tmp/manifest-list.avro",
                   .schema_id = schema_->schema_id()});
    };
    auto make_branch = [](int64_t id) {
      return std::make_shared<SnapshotRef>(
          SnapshotRef{.snapshot_id = id, .retention = SnapshotRef::Branch{}});
    };

    metadata_->snapshots = {make_snapshot(kRootId, std::nullopt, 1L),
                            make_snapshot(kMiddleId, kRootId, 2L),
                            make_snapshot(kMainHeadId, kMiddleId, 3L),
                            make_snapshot(kBranchHeadId, kMiddleId, 4L)};
    metadata_->last_sequence_number = 4L;
    metadata_->current_snapshot_id = kMainHeadId;
    metadata_->refs = {{"main", make_branch(kMainHeadId)},
                       {std::string(kBranch), make_branch(kBranchHeadId)}};
  }

  RestIncrementalAppendScanBuilder MakeBuilder(RestScanContext ctx) {
    return {metadata_, file_io_, "test.my_table", nullptr,
            std::make_shared<const RestScanContext>(std::move(ctx))};
  }

  Result<std::unique_ptr<IncrementalAppendScan>> MakeIncrementalScan(
      RestScanContext ctx, std::optional<int64_t> from_snapshot_id = kRootId,
      bool from_inclusive = false, std::optional<int64_t> to_snapshot_id = std::nullopt) {
    auto builder = MakeBuilder(std::move(ctx));
    if (from_snapshot_id.has_value()) {
      builder.FromSnapshot(*from_snapshot_id, from_inclusive);
    }
    if (to_snapshot_id.has_value()) {
      builder.ToSnapshot(*to_snapshot_id);
    }
    return builder.Build();
  }

  template <typename Configure>
  Result<std::vector<std::shared_ptr<FileScanTask>>> PlanScan(bool incremental,
                                                              RestScanContext ctx,
                                                              Configure configure) {
    auto plan = [&](auto& builder) -> Result<std::vector<std::shared_ptr<FileScanTask>>> {
      configure(builder);
      ICEBERG_ASSIGN_OR_RAISE(auto scan, builder.Build());
      return scan->PlanFiles();
    };
    if (incremental) {
      auto builder = MakeBuilder(std::move(ctx));
      builder.FromSnapshot(kRootId);
      return plan(builder);
    }
    RestTableScanBuilder builder(metadata_, file_io_, "test.my_table", nullptr,
                                 std::make_shared<const RestScanContext>(std::move(ctx)));
    return plan(builder);
  }
};

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

  RestIncrementalAppendScanBuilder builder(
      empty_metadata, file_io_, "test.my_table", nullptr,
      std::make_shared<const RestScanContext>(MakeContext()));
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder.Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());
}

// --------------------------------------------------------------------------
// POST body carries the resolved snapshot range.
// --------------------------------------------------------------------------
TEST_F(RestIncrementalAppendScanTest, PlanFilesSendsResolvedRange) {
  constexpr std::string_view kResponseBody = R"({"status":"completed"})";
  EXPECT_CALL(*mock_client_,
              Post(_,
                   testing::AllOf(JsonBodyHas("start-snapshot-id", kRootId),
                                  JsonBodyHas("end-snapshot-id", kMainHeadId),
                                  JsonBodyHas("use-snapshot-schema", true)),
                   _, _, _))
      .WillOnce(Return(HttpResponse(200, std::string(kResponseBody))));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeIncrementalScan(MakeContext()));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());
}

// --------------------------------------------------------------------------
// Explicit to_snapshot_id is sent as end-snapshot-id.
// --------------------------------------------------------------------------
TEST_F(RestIncrementalAppendScanTest, PlanFilesWithExplicitToSnapshotId) {
  constexpr std::string_view kResponseBody = R"({"status":"completed"})";
  EXPECT_CALL(*mock_client_,
              Post(_,
                   testing::AllOf(JsonBodyHas("start-snapshot-id", kRootId),
                                  JsonBodyHas("end-snapshot-id", kMiddleId)),
                   _, _, _))
      .WillOnce(Return(HttpResponse(200, std::string(kResponseBody))));

  ICEBERG_UNWRAP_OR_FAIL(auto scan,
                         MakeIncrementalScan(MakeContext(), kRootId,
                                             /*from_inclusive=*/false, kMiddleId));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());
}

// --------------------------------------------------------------------------
// Exclusive from_snapshot_id is sent as start-snapshot-id.
// --------------------------------------------------------------------------
TEST_F(RestIncrementalAppendScanTest, PlanFilesWithFromSnapshotIdExclusive) {
  constexpr std::string_view kResponseBody = R"({"status":"completed"})";
  EXPECT_CALL(*mock_client_,
              Post(_,
                   testing::AllOf(JsonBodyHas("start-snapshot-id", kMiddleId),
                                  JsonBodyHas("end-snapshot-id", kMainHeadId)),
                   _, _, _))
      .WillOnce(Return(HttpResponse(200, std::string(kResponseBody))));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeIncrementalScan(MakeContext(), kMiddleId,
                                                        /*from_inclusive=*/false));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());
}

// --------------------------------------------------------------------------
// Inclusive from_snapshot_id sends its parent as start-snapshot-id.
// --------------------------------------------------------------------------
TEST_F(RestIncrementalAppendScanTest, PlanFilesWithFromSnapshotIdInclusiveWithParent) {
  constexpr std::string_view kResponseBody = R"({"status":"completed"})";
  EXPECT_CALL(*mock_client_,
              Post(_,
                   testing::AllOf(JsonBodyHas("start-snapshot-id", kRootId),
                                  JsonBodyHas("end-snapshot-id", kMainHeadId)),
                   _, _, _))
      .WillOnce(Return(HttpResponse(200, std::string(kResponseBody))));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeIncrementalScan(MakeContext(), kMiddleId,
                                                        /*from_inclusive=*/true));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());
}

// --------------------------------------------------------------------------
// Inclusive from_snapshot_id on the root snapshot → NotSupported.
// --------------------------------------------------------------------------
TEST_F(RestIncrementalAppendScanTest, PlanFilesWithFromSnapshotIdInclusiveNoParent) {
  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _)).Times(0);

  ICEBERG_UNWRAP_OR_FAIL(
      auto scan, MakeIncrementalScan(MakeContext(), kRootId, /*from_inclusive=*/true));
  auto result = scan->PlanFiles();
  EXPECT_THAT(result, IsError(ErrorKind::kNotSupported));
}

// --------------------------------------------------------------------------
// No from_snapshot_id → NotSupported.
// --------------------------------------------------------------------------
TEST_F(RestIncrementalAppendScanTest, PlanFilesWithoutFromSnapshotIdNotSupported) {
  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _)).Times(0);

  ICEBERG_UNWRAP_OR_FAIL(
      auto scan, MakeIncrementalScan(MakeContext(), /*from_snapshot_id=*/std::nullopt));
  auto result = scan->PlanFiles();
  EXPECT_THAT(result, IsError(ErrorKind::kNotSupported));
}

// --------------------------------------------------------------------------
// UseBranch() without to_snapshot_id sends the branch head as end-snapshot-id.
// --------------------------------------------------------------------------
TEST_F(RestIncrementalAppendScanTest, UseBranchWithoutToSnapshotUsesBranchHead) {
  constexpr std::string_view kResponseBody = R"({"status":"completed"})";
  EXPECT_CALL(*mock_client_,
              Post(_,
                   testing::AllOf(JsonBodyHas("start-snapshot-id", kRootId),
                                  JsonBodyHas("end-snapshot-id", kBranchHeadId)),
                   _, _, _))
      .WillOnce(Return(HttpResponse(200, std::string(kResponseBody))));

  auto builder = MakeBuilder(MakeContext());
  builder.FromSnapshot(kRootId).UseBranch(std::string(kBranch));
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder.Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());
}

// --------------------------------------------------------------------------
// UseBranch() with to_snapshot_id on the branch.
// --------------------------------------------------------------------------
TEST_F(RestIncrementalAppendScanTest, UseBranchWithToSnapshotOnBranch) {
  constexpr std::string_view kResponseBody = R"({"status":"completed"})";
  EXPECT_CALL(*mock_client_,
              Post(_,
                   testing::AllOf(JsonBodyHas("start-snapshot-id", kRootId),
                                  JsonBodyHas("end-snapshot-id", kMiddleId)),
                   _, _, _))
      .WillOnce(Return(HttpResponse(200, std::string(kResponseBody))));

  auto builder = MakeBuilder(MakeContext());
  builder.FromSnapshot(kRootId).ToSnapshot(kMiddleId).UseBranch(std::string(kBranch));
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder.Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());
}

// --------------------------------------------------------------------------
// UseBranch() with to_snapshot_id not on the branch → ValidationFailed.
// --------------------------------------------------------------------------
TEST_F(RestIncrementalAppendScanTest, UseBranchRejectsToSnapshotNotOnBranch) {
  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _)).Times(0);

  auto builder = MakeBuilder(MakeContext());
  builder.FromSnapshot(kRootId).ToSnapshot(kMainHeadId).UseBranch(std::string(kBranch));
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder.Build());
  auto result = scan->PlanFiles();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("not a valid snapshot on the current branch"));
}

// --------------------------------------------------------------------------
// from_snapshot_id not an ancestor of the end snapshot → ValidationFailed.
// --------------------------------------------------------------------------
TEST_F(RestIncrementalAppendScanTest, RejectsFromSnapshotNotAncestorOfEnd) {
  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _)).Times(0);

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeIncrementalScan(MakeContext(), kBranchHeadId,
                                                        /*from_inclusive=*/false));
  auto result = scan->PlanFiles();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
}

// Exercise residual handling for both scan types and each response path.
class RestScanResidualTest
    : public RestIncrementalAppendScanTest,
      public ::testing::WithParamInterface<std::tuple<bool, bool, bool, bool, bool>> {};

TEST_P(RestScanResidualTest, PreservesSplitRangesFilteringAndDeleteFiles) {
  const auto [incremental, submitted, fetch_token, server_residual, ignore_residuals] =
      GetParam();
  auto filter = Expressions::GreaterThan("id", Literal::Int(10));
  auto residual = Expressions::GreaterThan("id", Literal::Int(20));
  ICEBERG_UNWRAP_OR_FAIL(auto filter_json, iceberg::ToJson(*filter));
  ICEBERG_UNWRAP_OR_FAIL(auto residual_json, iceberg::ToJson(*residual));

  auto task = FileTaskJson("s3://bucket/data.parquet");
  task["start"] = 25;
  task["length"] = 50;
  task["delete-file-references"] = {0};
  if (server_residual) task["residual-filter"] = residual_json;
  auto delete_file = FileTaskJson("s3://bucket/delete.parquet")["data-file"];
  delete_file["content"] = "position-deletes";
  nlohmann::json tasks_response = {
      {"file-scan-tasks", nlohmann::json::array({task})},
      {"delete-files", nlohmann::json::array({delete_file})}};
  auto completed =
      fetch_token ? nlohmann::json{{"plan-tasks", {"residual-token"}}} : tasks_response;
  completed["status"] = "completed";
  completed["plan-id"] = "residual-plan";
  auto initial =
      submitted ? nlohmann::json{{"status", "submitted"}, {"plan-id", "residual-plan"}}
                : completed;
  EXPECT_CALL(*mock_client_,
              Post(::testing::EndsWith("/plan"),
                   ::testing::AllOf(
                       JsonBodyHas("filter", filter_json),
                       JsonBodyHas("select", std::vector<std::string>{"id", "data"})),
                   _, _, _))
      .WillOnce(Return(HttpResponse(200, initial.dump())));
  if (submitted) {
    completed.erase("plan-id");
    EXPECT_CALL(*mock_client_,
                Get(::testing::EndsWith("/plan/residual-plan"), _, _, _, _))
        .WillOnce(Return(HttpResponse(200, completed.dump())));
  }
  if (fetch_token) {
    EXPECT_CALL(*mock_client_, Post(::testing::EndsWith("/tasks"),
                                    JsonBodyHas("plan-task", "residual-token"), _, _, _))
        .WillOnce(Return(HttpResponse(200, tasks_response.dump())));
  }
  EXPECT_CALL(*mock_client_, Delete(_, _, _, _, _)).Times(0);

  ICEBERG_UNWRAP_OR_FAIL(auto tasks,
                         PlanScan(incremental, MakeContext(), [&](auto& builder) {
                           builder.Select({"data"}).Filter(filter);
                           if (ignore_residuals) builder.IgnoreResiduals();
                         }));
  ASSERT_EQ(tasks.size(), 1);
  EXPECT_EQ(tasks[0]->data_file()->file_path, "s3://bucket/data.parquet");
  EXPECT_TRUE(tasks[0]->is_split());
  EXPECT_EQ(tasks[0]->start(), 25);
  EXPECT_EQ(tasks[0]->length(), 50);
  ASSERT_EQ(tasks[0]->delete_files().size(), 1);
  EXPECT_EQ(tasks[0]->delete_files()[0]->file_path, "s3://bucket/delete.parquet");
  ASSERT_NE(tasks[0]->residual_filter(), nullptr);
  if (ignore_residuals) {
    EXPECT_EQ(tasks[0]->residual_filter()->op(), Expression::Operation::kTrue);
  } else {
    ICEBERG_UNWRAP_OR_FAIL(auto actual, iceberg::ToJson(*tasks[0]->residual_filter()));
    EXPECT_EQ(actual, server_residual ? residual_json : filter_json);
  }
}

INSTANTIATE_TEST_SUITE_P(ScanModes, RestScanResidualTest,
                         ::testing::Combine(::testing::Bool(), ::testing::Bool(),
                                            ::testing::Bool(), ::testing::Bool(),
                                            ::testing::Bool()));

class RestScanRegressionTest : public RestIncrementalAppendScanTest,
                               public ::testing::WithParamInterface<bool> {};

TEST_P(RestScanRegressionTest, NoFilterProducesTrueResidual) {
  nlohmann::json response = {
      {"status", "completed"},
      {"file-scan-tasks",
       nlohmann::json::array({FileTaskJson("s3://bucket/data.parquet")})}};
  EXPECT_CALL(
      *mock_client_,
      Post(::testing::EndsWith("/plan"),
           ::testing::AllOf(JsonBodyHas("select", std::vector<std::string>{"id", "data"}),
                            JsonBodyHas("use-snapshot-schema", GetParam()),
                            JsonBodyLacks("snapshot-id")),
           _, _, _))
      .WillOnce(Return(HttpResponse(200, response.dump())));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, PlanScan(GetParam(), MakeContext(), [](auto&) {}));
  ASSERT_EQ(tasks.size(), 1);
  ASSERT_NE(tasks[0]->residual_filter(), nullptr);
  EXPECT_EQ(tasks[0]->residual_filter()->op(), Expression::Operation::kTrue);
}

TEST_P(RestScanRegressionTest, AllColumnStatsIncludeUnprojectedColumns) {
  EXPECT_CALL(
      *mock_client_,
      Post(::testing::EndsWith("/plan"),
           ::testing::AllOf(
               JsonBodyHas("select", std::vector<std::string>{"data"}),
               JsonBodyHas("stats-fields", std::vector<std::string>{"id", "data"})),
           _, _, _))
      .WillOnce(Return(HttpResponse(200, R"({"status":"completed"})")));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks,
                         PlanScan(GetParam(), MakeContext(), [](auto& builder) {
                           builder.Select({"data"}).IncludeColumnStats();
                         }));
  EXPECT_TRUE(tasks.empty());
}

TEST_P(RestScanRegressionTest, PollingSleepRespectsTimeout) {
  constexpr std::string_view kSubmitted =
      R"({"status":"submitted","plan-id":"timeout-plan"})";
  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse(200, std::string(kSubmitted))));
  int poll_count = 0;
  EXPECT_CALL(*mock_client_, Get(_, _, _, _, _))
      .WillRepeatedly([&](auto&&...) -> Result<HttpResponse> {
        ++poll_count;
        return HttpResponse(
            200, poll_count == 1 ? std::string(kSubmitted) : R"({"status":"completed"})");
      });
  EXPECT_CALL(*mock_client_, Delete(_, _, _, _, _))
      .WillOnce(Return(HttpResponse(200, "{}")));

  auto ctx = MakeContext();
  ctx.catalog_config = {{"rest-scan-planning.poll-timeout-ms", "10"}};
  auto start = std::chrono::steady_clock::now();
  EXPECT_THAT(
      PlanScan(GetParam(), std::move(ctx), [](auto&) {}),
      ::testing::AllOf(IsError(ErrorKind::kIOError), HasErrorMessage("timed out")));
  auto elapsed = std::chrono::steady_clock::now() - start;
  EXPECT_EQ(poll_count, 1);
  EXPECT_GE(elapsed, std::chrono::milliseconds(10));
  EXPECT_LT(elapsed, std::chrono::milliseconds(500));
}

TEST_P(RestScanRegressionTest, PollingCompletionAfterTimeoutIsPreserved) {
  nlohmann::json completed = {
      {"status", "completed"},
      {"file-scan-tasks",
       nlohmann::json::array({FileTaskJson("s3://bucket/completed.parquet")})}};
  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(
          Return(HttpResponse(200, R"({"status":"submitted","plan-id":"slow-plan"})")));
  EXPECT_CALL(*mock_client_, Get(_, _, _, _, _))
      .WillOnce([&](auto&&...) -> Result<HttpResponse> {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        return HttpResponse(200, completed.dump());
      });
  EXPECT_CALL(*mock_client_, Delete(_, _, _, _, _)).Times(0);

  auto ctx = MakeContext();
  ctx.catalog_config = {{"rest-scan-planning.poll-timeout-ms", "10"}};
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, PlanScan(GetParam(), std::move(ctx), [](auto&) {}));
  ASSERT_EQ(tasks.size(), 1);
  EXPECT_EQ(tasks[0]->data_file()->file_path, "s3://bucket/completed.parquet");
}

TEST_P(RestScanRegressionTest, PollingFailureAfterTimeoutPreservesServerError) {
  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(
          Return(HttpResponse(200, R"({"status":"submitted","plan-id":"slow-plan"})")));
  EXPECT_CALL(*mock_client_, Get(_, _, _, _, _))
      .WillOnce([](auto&&...) -> Result<HttpResponse> {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        return HttpResponse(
            200,
            R"({"status":"failed","error":{"message":"server error","type":"ServerError","code":500}})");
      });
  EXPECT_CALL(*mock_client_, Delete(_, _, _, _, _))
      .WillOnce(Return(HttpResponse(200, "{}")));

  auto ctx = MakeContext();
  ctx.catalog_config = {{"rest-scan-planning.poll-timeout-ms", "10"}};
  EXPECT_THAT(
      PlanScan(GetParam(), std::move(ctx), [](auto&) {}),
      ::testing::AllOf(IsError(ErrorKind::kIOError), HasErrorMessage("server error")));
}

TEST_P(RestScanRegressionTest, PollingCancellationAfterTimeoutDoesNotCancelAgain) {
  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(
          Return(HttpResponse(200, R"({"status":"submitted","plan-id":"slow-plan"})")));
  EXPECT_CALL(*mock_client_, Get(_, _, _, _, _))
      .WillOnce([](auto&&...) -> Result<HttpResponse> {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        return HttpResponse(200, R"({"status":"cancelled"})");
      });
  EXPECT_CALL(*mock_client_, Delete(_, _, _, _, _)).Times(0);

  auto ctx = MakeContext();
  ctx.catalog_config = {{"rest-scan-planning.poll-timeout-ms", "10"}};
  EXPECT_THAT(
      PlanScan(GetParam(), std::move(ctx), [](auto&) {}),
      ::testing::AllOf(IsError(ErrorKind::kIOError), HasErrorMessage("was cancelled")));
}

TEST_P(RestScanRegressionTest, PollingSubmittedAfterTimeoutReturnsTimeout) {
  constexpr std::string_view kSubmitted =
      R"({"status":"submitted","plan-id":"slow-plan"})";
  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse(200, std::string(kSubmitted))));
  EXPECT_CALL(*mock_client_, Get(_, _, _, _, _))
      .WillOnce([&](auto&&...) -> Result<HttpResponse> {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        return HttpResponse(200, std::string(kSubmitted));
      });
  EXPECT_CALL(*mock_client_, Delete(_, _, _, _, _))
      .WillOnce(Return(HttpResponse(200, "{}")));

  auto ctx = MakeContext();
  ctx.catalog_config = {{"rest-scan-planning.poll-timeout-ms", "10"},
                        {"rest-scan-planning.poll-num-retries", "0"}};
  EXPECT_THAT(
      PlanScan(GetParam(), std::move(ctx), [](auto&) {}),
      ::testing::AllOf(IsError(ErrorKind::kIOError), HasErrorMessage("timed out")));
}

TEST_P(RestScanRegressionTest, InvalidPollingConfigFailsBeforePlanning) {
  struct PropertyCase {
    std::string_view key;
    std::string_view value;
    std::string_view error;
  };
  const std::array<PropertyCase, 7> cases{{{.key = "rest-scan-planning.poll-timeout-ms",
                                            .value = "0",
                                            .error = "must be positive"},
                                           {.key = "rest-scan-planning.poll-timeout-ms",
                                            .value = "-1",
                                            .error = "must be positive"},
                                           {.key = "rest-scan-planning.poll-timeout-ms",
                                            .value = "abc",
                                            .error = "invalid argument"},
                                           {.key = "rest-scan-planning.poll-timeout-ms",
                                            .value = "9223372036854775808",
                                            .error = "out of range"},
                                           {.key = "rest-scan-planning.poll-num-retries",
                                            .value = "-1",
                                            .error = "must be non-negative"},
                                           {.key = "rest-scan-planning.poll-num-retries",
                                            .value = "abc",
                                            .error = "invalid argument"},
                                           {.key = "rest-scan-planning.poll-num-retries",
                                            .value = "2147483648",
                                            .error = "out of range"}}};
  for (const auto& [key, value, error] : cases) {
    SCOPED_TRACE(std::string(key) + "=" + std::string(value));
    auto ctx = MakeContext();
    ctx.catalog_config = {{std::string(key), std::string(value)}};
    EXPECT_CALL(*mock_client_, Post(_, _, _, _, _)).Times(0);
    EXPECT_THAT(
        PlanScan(GetParam(), std::move(ctx), [](auto&) {}),
        ::testing::AllOf(IsError(ErrorKind::kInvalidArgument), HasErrorMessage(error)));
    ASSERT_TRUE(::testing::Mock::VerifyAndClearExpectations(mock_client_.get()));
  }
}

TEST_P(RestScanRegressionTest, MissingPollingEndpointCancelsSubmittedPlan) {
  auto ctx = MakeContext();
  ctx.supported_endpoints.erase(Endpoint::FetchPlanningResult());
  EXPECT_CALL(*mock_client_, Post(::testing::EndsWith("/plan"), _, _, _, _))
      .WillOnce(Return(
          HttpResponse(200, R"({"status":"submitted","plan-id":"missing-poll"})")));
  EXPECT_CALL(*mock_client_, Get(_, _, _, _, _)).Times(0);
  EXPECT_CALL(*mock_client_,
              Delete(::testing::EndsWith("/plan/missing-poll"), _, _, _, _))
      .WillOnce(Return(HttpResponse(204, "")));
  EXPECT_THAT(PlanScan(GetParam(), std::move(ctx), [](auto&) {}),
              IsError(ErrorKind::kNotSupported));
}

TEST_P(RestScanRegressionTest, LongContinuationChain) {
  constexpr int kPages = 2048;
  EXPECT_CALL(*mock_client_, Post(::testing::EndsWith("/plan"), _, _, _, _))
      .WillOnce(Return(HttpResponse(
          200, R"({"status":"completed","plan-id":"pages","plan-tasks":["0"]})")));
  int next_page = 0;
  EXPECT_CALL(*mock_client_, Post(::testing::EndsWith("/tasks"), _, _, _, _))
      .Times(kPages)
      .WillRepeatedly([&](const std::string&, const std::string& body,
                          auto&&...) -> Result<HttpResponse> {
        EXPECT_EQ(nlohmann::json::parse(body).at("plan-task"), std::to_string(next_page));
        auto task = FileTaskJson("s3://bucket/" + std::to_string(next_page) + ".parquet");
        nlohmann::json response = {
            {"file-scan-tasks", nlohmann::json::array({std::move(task)})}};
        if (++next_page < kPages) {
          response["plan-tasks"] = {std::to_string(next_page)};
        }
        return HttpResponse(200, response.dump());
      });
  EXPECT_CALL(*mock_client_, Delete(_, _, _, _, _)).Times(0);

  ICEBERG_UNWRAP_OR_FAIL(
      auto table, RestTable::Make(identifier_, metadata_, "/tmp/metadata.json", file_io_,
                                  nullptr, "test.my_table", nullptr, MakeContext()));
  auto plan = [&]() -> Result<std::vector<std::shared_ptr<FileScanTask>>> {
    if (GetParam()) {
      ICEBERG_ASSIGN_OR_RAISE(auto builder, table->NewIncrementalAppendScan());
      builder->FromSnapshot(kRootId);
      ICEBERG_ASSIGN_OR_RAISE(auto scan, builder->Build());
      return scan->PlanFiles();
    }
    ICEBERG_ASSIGN_OR_RAISE(auto builder, table->NewScan());
    ICEBERG_ASSIGN_OR_RAISE(auto scan, builder->Build());
    ICEBERG_ASSIGN_OR_RAISE(auto stream, scan->PlanFilesStream());
    scan.reset();
    table.reset();
    return stream->ToVector();
  };
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, plan());
  ASSERT_EQ(tasks.size(), kPages);
  for (int i = 0; i < kPages; ++i) {
    EXPECT_EQ(tasks[i]->data_file()->file_path,
              "s3://bucket/" + std::to_string(i) + ".parquet");
  }
}

TEST_P(RestScanRegressionTest, TaskFetchFailureCancelsPlan) {
  EXPECT_CALL(*mock_client_, Post(::testing::EndsWith("/plan"), _, _, _, _))
      .WillOnce(Return(HttpResponse(
          200, R"({"status":"completed","plan-id":"failed-page","plan-tasks":["0"]})")));
  EXPECT_CALL(*mock_client_,
              Post(::testing::EndsWith("/tasks"), JsonBodyHas("plan-task", "0"), _, _, _))
      .WillOnce(Return(IOError("task fetch failed")));
  EXPECT_CALL(*mock_client_, Delete(::testing::EndsWith("/plan/failed-page"), _, _, _, _))
      .WillOnce(Return(HttpResponse(204, "")));
  EXPECT_THAT(PlanScan(GetParam(), MakeContext(), [](auto&) {}),
              IsError(ErrorKind::kIOError));
}

class RestScanStreamTest : public RestTableScanTest,
                           public ::testing::WithParamInterface<bool> {
 protected:
  void ExpectPlan(nlohmann::json completed) {
    completed["status"] = "completed";
    completed["plan-id"] = "stream";
    auto initial = GetParam()
                       ? nlohmann::json{{"status", "submitted"}, {"plan-id", "stream"}}
                       : completed;
    EXPECT_CALL(*mock_client_, Post(::testing::EndsWith("/plan"), _, _, _, _))
        .WillOnce(Return(HttpResponse(200, initial.dump())));
    if (GetParam()) {
      completed.erase("plan-id");
      EXPECT_CALL(*mock_client_, Get(::testing::EndsWith("/plan/stream"), _, _, _, _))
          .WillOnce(Return(HttpResponse(200, completed.dump())));
    } else {
      EXPECT_CALL(*mock_client_, Get(_, _, _, _, _)).Times(0);
    }
  }
};

TEST_P(RestScanStreamTest, FetchesOnlyWhenBufferIsExhausted) {
  ExpectPlan(
      {{"file-scan-tasks", nlohmann::json::array({FileTaskJson("s3://b/0.parquet")})},
       {"plan-tasks", {"first", "second"}}});
  int fetch_count = 0;
  EXPECT_CALL(*mock_client_, Post(::testing::EndsWith("/tasks"),
                                  JsonBodyHas("plan-task", "first"), _, _, _))
      .WillOnce([&](auto&&...) -> Result<HttpResponse> {
        EXPECT_EQ(fetch_count++, 0);
        return HttpResponse(200, R"({"plan-tasks":["third"]})");
      });
  EXPECT_CALL(*mock_client_, Post(::testing::EndsWith("/tasks"),
                                  JsonBodyHas("plan-task", "second"), _, _, _))
      .WillOnce([&](auto&&...) -> Result<HttpResponse> {
        EXPECT_EQ(fetch_count++, 1);
        nlohmann::json response = {
            {"file-scan-tasks",
             nlohmann::json::array(
                 {FileTaskJson("s3://b/1.parquet"), FileTaskJson("s3://b/2.parquet")})}};
        return HttpResponse(200, response.dump());
      });
  EXPECT_CALL(*mock_client_, Post(::testing::EndsWith("/tasks"),
                                  JsonBodyHas("plan-task", "third"), _, _, _))
      .WillOnce([&](auto&&...) -> Result<HttpResponse> {
        EXPECT_EQ(fetch_count++, 2);
        nlohmann::json response = {
            {"file-scan-tasks",
             nlohmann::json::array({FileTaskJson("s3://b/3.parquet")})}};
        return HttpResponse(200, response.dump());
      });
  EXPECT_CALL(*mock_client_, Delete(_, _, _, _, _)).Times(0);
  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(MakeContext()));
  ICEBERG_UNWRAP_OR_FAIL(auto stream, scan->PlanFilesStream());
  EXPECT_EQ(fetch_count, 0);
  for (int i = 0; i <= 3; ++i) {
    ICEBERG_UNWRAP_OR_FAIL(auto task, stream->Next());
    ASSERT_TRUE(task.has_value());
    EXPECT_EQ((*task)->data_file()->file_path,
              "s3://b/" + std::to_string(i) + ".parquet");
    EXPECT_EQ(fetch_count, i == 0 ? 0 : (i <= 2 ? 2 : 3));
  }
  ICEBERG_UNWRAP_OR_FAIL(auto end, stream->Next());
  EXPECT_FALSE(end.has_value());
}

TEST_P(RestScanStreamTest, UnconsumedPlanIsCancelled) {
  ExpectPlan({{"plan-tasks", {"pending"}}});
  EXPECT_CALL(*mock_client_, Post(::testing::EndsWith("/tasks"), _, _, _, _)).Times(0);
  EXPECT_CALL(*mock_client_, Delete(::testing::EndsWith("/plan/stream"), _, _, _, _))
      .WillOnce(Return(HttpResponse(204, "")));
  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(MakeContext()));
  ICEBERG_UNWRAP_OR_FAIL(auto stream, scan->PlanFilesStream());
  stream.reset();
}

INSTANTIATE_TEST_SUITE_P(PlanningResponses, RestScanStreamTest, ::testing::Bool());

TEST_P(RestScanRegressionTest, EmptyCompletedPlan) {
  for (bool submitted : {false, true}) {
    SCOPED_TRACE(submitted);
    std::string initial = submitted ? R"({"status":"submitted","plan-id":"empty"})"
                                    : R"({"status":"completed"})";
    EXPECT_CALL(*mock_client_, Post(::testing::EndsWith("/plan"), _, _, _, _))
        .WillOnce(Return(HttpResponse(200, initial)));
    if (submitted) {
      EXPECT_CALL(*mock_client_, Get(::testing::EndsWith("/plan/empty"), _, _, _, _))
          .WillOnce(Return(HttpResponse(200, R"({"status":"completed"})")));
    } else {
      EXPECT_CALL(*mock_client_, Get(_, _, _, _, _)).Times(0);
    }
    EXPECT_CALL(*mock_client_, Delete(_, _, _, _, _)).Times(0);
    ICEBERG_UNWRAP_OR_FAIL(auto tasks, PlanScan(GetParam(), MakeContext(), [](auto&) {}));
    EXPECT_TRUE(tasks.empty());
    ASSERT_TRUE(::testing::Mock::VerifyAndClearExpectations(mock_client_.get()));
  }
}

TEST_P(RestScanRegressionTest, FailedPlanReturnsError) {
  EXPECT_CALL(*mock_client_, Post(::testing::EndsWith("/plan"), _, _, _, _))
      .WillOnce(Return(HttpResponse(
          200,
          R"({"status":"failed","error":{"message":"server error","type":"ServerError","code":500}})")));
  EXPECT_CALL(*mock_client_, Get(_, _, _, _, _)).Times(0);
  EXPECT_CALL(*mock_client_, Delete(_, _, _, _, _)).Times(0);
  EXPECT_THAT(
      PlanScan(GetParam(), MakeContext(), [](auto&) {}),
      ::testing::AllOf(IsError(ErrorKind::kIOError), HasErrorMessage("server error")));
}

TEST_P(RestScanRegressionTest, MissingPlanningEndpointDoesNotSendRequest) {
  auto ctx = MakeContext();
  ctx.supported_endpoints.erase(Endpoint::PlanTableScan());
  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _)).Times(0);
  EXPECT_THAT(PlanScan(GetParam(), std::move(ctx), [](auto&) {}),
              IsError(ErrorKind::kNotSupported));
}

TEST_P(RestScanRegressionTest, MissingTaskEndpointCancelsPlan) {
  auto ctx = MakeContext();
  ctx.supported_endpoints.erase(Endpoint::FetchScanTasks());
  EXPECT_CALL(*mock_client_, Post(::testing::EndsWith("/plan"), _, _, _, _))
      .WillOnce(Return(HttpResponse(
          200,
          R"({"status":"completed","plan-id":"no-tasks","plan-tasks":["pending"]})")));
  EXPECT_CALL(*mock_client_, Delete(::testing::EndsWith("/plan/no-tasks"), _, _, _, _))
      .WillOnce(Return(HttpResponse(204, "")));
  EXPECT_THAT(PlanScan(GetParam(), std::move(ctx), [](auto&) {}),
              IsError(ErrorKind::kNotSupported));
}

TEST_P(RestScanRegressionTest, InvalidPlanResponseCancelsKnownId) {
  struct Case {
    std::string body;
    ErrorKind error;
    bool cancel;
  };
  const std::array<Case, 3> cases{
      {{.body = R"({"status":"submitted","plan-id":"invalid","plan-tasks":["token"]})",
        .error = ErrorKind::kValidationFailed,
        .cancel = true},
       {.body =
            R"({"status":"failed","plan-id":"invalid","error":{"message":"error","type":"ServerError","code":500}})",
        .error = ErrorKind::kValidationFailed,
        .cancel = true},
       {.body = R"({"status":"failed"})",
        .error = ErrorKind::kJsonParseError,
        .cancel = false}}};
  for (const auto& test_case : cases) {
    SCOPED_TRACE(test_case.body);
    EXPECT_CALL(*mock_client_, Post(::testing::EndsWith("/plan"), _, _, _, _))
        .WillOnce(Return(HttpResponse(200, test_case.body)));
    EXPECT_CALL(*mock_client_, Get(_, _, _, _, _)).Times(0);
    if (test_case.cancel) {
      EXPECT_CALL(*mock_client_, Delete(::testing::EndsWith("/plan/invalid"), _, _, _, _))
          .WillOnce(Return(HttpResponse(204, "")));
    } else {
      EXPECT_CALL(*mock_client_, Delete(_, _, _, _, _)).Times(0);
    }
    EXPECT_THAT(PlanScan(GetParam(), MakeContext(), [](auto&) {}),
                IsError(test_case.error));
    ASSERT_TRUE(::testing::Mock::VerifyAndClearExpectations(mock_client_.get()));
  }
}

TEST_P(RestScanRegressionTest, RequestedColumnStatsAreSent) {
  EXPECT_CALL(*mock_client_,
              Post(::testing::EndsWith("/plan"),
                   JsonBodyHas("stats-fields", std::vector<std::string>{"id"}), _, _, _))
      .WillOnce(Return(HttpResponse(200, R"({"status":"completed"})")));
  ICEBERG_UNWRAP_OR_FAIL(
      auto tasks, PlanScan(GetParam(), MakeContext(),
                           [](auto& builder) { builder.IncludeColumnStats({"id"}); }));
  EXPECT_TRUE(tasks.empty());
}

TEST_P(RestScanRegressionTest, UnknownStatsColumnFailsBeforePlanning) {
  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _)).Times(0);
  EXPECT_THAT(PlanScan(GetParam(), MakeContext(),
                       [](auto& builder) { builder.IncludeColumnStats({"missing"}); }),
              ::testing::AllOf(IsError(ErrorKind::kValidationFailed),
                               HasErrorMessage("Cannot find stats column: missing")));
}

TEST_P(RestScanRegressionTest, ProjectUsesTheProjectedSchema) {
  EXPECT_CALL(*mock_client_,
              Post(::testing::EndsWith("/plan"),
                   JsonBodyHas("select", std::vector<std::string>{"id"}), _, _, _))
      .WillOnce(Return(HttpResponse(200, R"({"status":"completed"})")));
  ICEBERG_UNWRAP_OR_FAIL(
      auto tasks, PlanScan(GetParam(), MakeContext(), [](auto& builder) {
        builder.Project(std::make_shared<Schema>(
            std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32())}));
      }));
  EXPECT_TRUE(tasks.empty());
}

TEST_P(RestScanRegressionTest, SelectResolvesCaseInsensitiveNames) {
  EXPECT_CALL(*mock_client_,
              Post(::testing::EndsWith("/plan"),
                   JsonBodyHas("select", std::vector<std::string>{"id"}), _, _, _))
      .WillOnce(Return(HttpResponse(200, R"({"status":"completed"})")));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks,
                         PlanScan(GetParam(), MakeContext(), [](auto& builder) {
                           builder.CaseSensitive(false).Select({"ID"});
                         }));
  EXPECT_TRUE(tasks.empty());
}

TEST_P(RestScanRegressionTest, ReplanningDropsOldCredentials) {
  EXPECT_CALL(*mock_client_, Post(::testing::EndsWith("/plan"), _, _, _, _))
      .WillOnce(Return(HttpResponse(
          200,
          R"({"status":"completed","storage-credentials":[{"prefix":"s3://bucket","config":{"key":"value"}}]})")))
      .WillOnce(Return(HttpResponse(200, R"({"status":"completed"})")));
  auto exercise = [&](auto& builder) {
    ICEBERG_UNWRAP_OR_FAIL(auto scan, builder.Build());
    EXPECT_EQ(scan->io(), file_io_);
    ICEBERG_UNWRAP_OR_FAIL(auto first, scan->PlanFiles());
    auto* credentialed = scan->io()->AsSupportsStorageCredentials();
    ASSERT_NE(credentialed, nullptr);
    ASSERT_EQ(credentialed->credentials().size(), 1);
    EXPECT_EQ(credentialed->credentials()[0].prefix, "s3://bucket");
    ICEBERG_UNWRAP_OR_FAIL(auto second, scan->PlanFiles());
    EXPECT_EQ(scan->io(), file_io_);
  };
  if (GetParam()) {
    auto builder = MakeBuilder(MakeContext());
    builder.FromSnapshot(kRootId);
    exercise(builder);
  } else {
    RestTableScanBuilder builder(metadata_, file_io_, "test.my_table", nullptr,
                                 std::make_shared<const RestScanContext>(MakeContext()));
    exercise(builder);
  }
}

INSTANTIATE_TEST_SUITE_P(ScanModes, RestScanRegressionTest, ::testing::Bool());

}  // namespace

}  // namespace iceberg::rest
