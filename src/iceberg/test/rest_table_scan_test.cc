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
#include "iceberg/expression/expressions.h"
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

class StaticCredentialProvider : public StorageCredentialProvider {
 public:
  Result<std::vector<StorageCredential>> Load() override { return {}; }
};

}  // namespace

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
// IncludeColumnStats() columns are sent as stats-fields.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, IncludeColumnStatsSendsStatsFields) {
  constexpr std::string_view kResponseBody = R"({"status":"completed"})";
  EXPECT_CALL(
      *mock_client_,
      Post(_, JsonBodyHas("stats-fields", std::vector<std::string>{"id"}), _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kResponseBody))));

  RestTableScanBuilder builder(metadata_, file_io_, "test.my_table", nullptr,
                               MakeContext());
  builder.IncludeColumnStats({"id"});
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder.Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());
}

// --------------------------------------------------------------------------
// IncludeColumnStats() with an unknown column → ValidationFailed.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, IncludeColumnStatsRejectsMissingColumn) {
  RestTableScanBuilder builder(metadata_, file_io_, "test.my_table", nullptr,
                               MakeContext());
  builder.IncludeColumnStats({"missing"});
  EXPECT_THAT(builder.Build(),
              ::testing::AllOf(IsError(ErrorKind::kValidationFailed),
                               HasErrorMessage("Cannot find stats column: missing")));
}

// --------------------------------------------------------------------------
// Project() columns are sent as select.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, ProjectSendsProjectedColumnsAsSelect) {
  constexpr std::string_view kResponseBody = R"({"status":"completed"})";
  EXPECT_CALL(*mock_client_,
              Post(_, JsonBodyHas("select", std::vector<std::string>{"id"}), _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kResponseBody))));

  RestTableScanBuilder builder(metadata_, file_io_, "test.my_table", nullptr,
                               MakeContext());
  builder.Project(std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32())}));
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder.Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());
}

// --------------------------------------------------------------------------
// Select() sends the selected columns plus filter columns.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, SelectIncludesFilterColumns) {
  constexpr std::string_view kResponseBody = R"({"status":"completed"})";
  EXPECT_CALL(
      *mock_client_,
      Post(_, JsonBodyHas("select", std::vector<std::string>{"id", "data"}), _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kResponseBody))));

  RestTableScanBuilder builder(metadata_, file_io_, "test.my_table", nullptr,
                               MakeContext());
  builder.Select({"data"}).Filter(Expressions::Equal("id", Literal::Int(1)));
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder.Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());
}

// --------------------------------------------------------------------------
// Case-insensitive Select() sends the schema's column names.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, SelectResolvesCaseInsensitiveNames) {
  constexpr std::string_view kResponseBody = R"({"status":"completed"})";
  EXPECT_CALL(*mock_client_,
              Post(_, JsonBodyHas("select", std::vector<std::string>{"data"}), _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kResponseBody))));

  RestTableScanBuilder builder(metadata_, file_io_, "test.my_table", nullptr,
                               MakeContext());
  builder.CaseSensitive(false).Select({"DATA"});
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
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kResponseBody))));

  RestTableScanBuilder builder(metadata_, file_io_, "test.my_table", nullptr,
                               MakeContext());
  builder.Select({std::string(Schema::kAllColumns)});
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder.Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());
}

// --------------------------------------------------------------------------
// No projection sends every column.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, NoProjectionSendsAllColumns) {
  constexpr std::string_view kResponseBody = R"({"status":"completed"})";
  EXPECT_CALL(
      *mock_client_,
      Post(_, JsonBodyHas("select", std::vector<std::string>{"id", "data"}), _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kResponseBody))));

  RestTableScanBuilder builder(metadata_, file_io_, "test.my_table", nullptr,
                               MakeContext());
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
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kResponseBody))));

  RestTableScanBuilder builder(metadata_, file_io_, "test.my_table", nullptr,
                               MakeContext());
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
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kResponseBody))));

  RestTableScanBuilder builder(metadata_, file_io_, "test.my_table", nullptr,
                               MakeContext());
  builder.UseRef("evolved-tag");
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder.Build());
  ICEBERG_UNWRAP_OR_FAIL(auto scan_schema, scan->schema());
  EXPECT_EQ(scan_schema->schema_id(), evolved_schema_->schema_id());
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
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kPlanResponse))))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kTasksResponse))));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(MakeContext()));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());

  EXPECT_EQ(scan->io().get(), file_io_.get());
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
TEST_F(RestTableScanTest, PlanFilesStreamCancelOnNoConsumption) {
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
// PlanFilesStream: SUBMITTED → COMPLETED with plan tasks; tokens are fetched
// lazily as the stream is consumed.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, PlanFilesStreamSubmittedFetchesTokensLazily) {
  constexpr std::string_view kSubmittedBody =
      R"({"status":"submitted","plan-id":"plan-poll-lazy"})";
  constexpr std::string_view kCompletedBody =
      R"({"status":"completed","plan-tasks":["tok-1","tok-2"]})";
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

  int fetch_count = 0;
  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kSubmittedBody))))
      .WillOnce([&](auto&&...) -> Result<HttpResponse> {
        ++fetch_count;
        return HttpResponse::MakeForTesting(200, std::string(kTask1Response));
      })
      .WillOnce([&](auto&&...) -> Result<HttpResponse> {
        ++fetch_count;
        return HttpResponse::MakeForTesting(200, std::string(kTask2Response));
      });
  EXPECT_CALL(*mock_client_, Get(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kCompletedBody))));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(MakeContext()));
  ICEBERG_UNWRAP_OR_FAIL(auto stream, scan->PlanFilesStream());
  EXPECT_EQ(fetch_count, 0);

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
// PlanFilesStream: SUBMITTED → COMPLETED, stream destroyed with no Next() calls
// → DELETE /plan called by the destructor.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, PlanFilesStreamSubmittedCancelOnNoConsumption) {
  constexpr std::string_view kSubmittedBody =
      R"({"status":"submitted","plan-id":"plan-poll-partial"})";
  constexpr std::string_view kCompletedBody =
      R"({"status":"completed","plan-tasks":["tok-1"]})";

  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kSubmittedBody))));
  EXPECT_CALL(*mock_client_, Get(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kCompletedBody))));
  EXPECT_CALL(*mock_client_, Delete(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, "{}")));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(MakeContext()));
  {
    ICEBERG_UNWRAP_OR_FAIL(auto stream, scan->PlanFilesStream());
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
  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kPlanResponse))))
      .WillOnce([&](auto&&...) -> Result<HttpResponse> {
        ++fetch_count;
        return HttpResponse::MakeForTesting(200, std::string(kTask1Response));
      })
      .WillOnce([&](auto&&...) -> Result<HttpResponse> {
        ++fetch_count;
        return HttpResponse::MakeForTesting(200, std::string(kNestedResponse));
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
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kPlanResponse))))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kTask1Response))))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kTask2Response))));

  test::ThreadExecutor executor;
  RestTableScanBuilder builder(metadata_, file_io_, "test.my_table", nullptr,
                               MakeContext());
  builder.PlanWith(executor);
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder.Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());

  ASSERT_EQ(tasks.size(), 2);
  EXPECT_EQ(tasks[0]->data_file()->file_path, "s3://b/f1.parquet");
  EXPECT_EQ(tasks[1]->data_file()->file_path, "s3://b/f2.parquet");
  EXPECT_EQ(executor.submit_count(), 0);
}

// --------------------------------------------------------------------------
// PlanFilesStream: invalid plan response with a plan-id → DELETE /plan called.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, PlanFilesStreamInvalidResponseWithPlanIdCancels) {
  constexpr std::string_view kInvalidBody =
      R"({"status":"submitted","plan-id":"plan-invalid","plan-tasks":["tok-1"]})";
  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kInvalidBody))));
  EXPECT_CALL(*mock_client_, Delete(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, "{}")));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(MakeContext()));
  EXPECT_THAT(scan->PlanFilesStream(), IsError(ErrorKind::kValidationFailed));
}

// --------------------------------------------------------------------------
// PlanFilesStream: invalid plan response without a plan-id → no DELETE.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, PlanFilesStreamInvalidResponseWithoutPlanIdDoesNotCancel) {
  constexpr std::string_view kInvalidBody = R"({"status":"failed"})";
  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kInvalidBody))));
  EXPECT_CALL(*mock_client_, Delete(_, _, _, _, _)).Times(0);

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(MakeContext()));
  EXPECT_THAT(scan->PlanFilesStream(), IsError(ErrorKind::kJsonParseError));
}

// --------------------------------------------------------------------------
// poll-num-retries=0: a single GET for a SUBMITTED plan, then cancel.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, PlanFilesStreamPollNumRetriesZero) {
  constexpr std::string_view kSubmittedBody =
      R"({"status":"submitted","plan-id":"plan-no-retry"})";
  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kSubmittedBody))));
  EXPECT_CALL(*mock_client_, Get(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kSubmittedBody))));
  EXPECT_CALL(*mock_client_, Delete(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, "{}")));

  auto ctx = MakeContext();
  ctx.catalog_config = {{"rest-scan-planning.poll-num-retries", "0"}};
  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(std::move(ctx)));
  EXPECT_THAT(scan->PlanFilesStream(),
              ::testing::AllOf(IsError(ErrorKind::kIOError),
                               HasErrorMessage("exceeded max retries (0)")));
}

// --------------------------------------------------------------------------
// Invalid poll-timeout-ms fails before the plan is submitted.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, PlanFilesStreamInvalidPollTimeoutFailsBeforePlanning) {
  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _)).Times(0);

  auto ctx = MakeContext();
  ctx.catalog_config = {{"rest-scan-planning.poll-timeout-ms", "0"}};
  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(std::move(ctx)));
  EXPECT_THAT(scan->PlanFilesStream(), IsError(ErrorKind::kInvalidArgument));
}

// --------------------------------------------------------------------------
// Vended credentials in the plan response install a provider scoped to the plan.
// --------------------------------------------------------------------------
TEST_F(RestTableScanTest, StorageCredentialsInstallPlanScopedProvider) {
  constexpr std::string_view kPlanResponse = R"({
    "status": "completed",
    "plan-id": "plan-cred",
    "storage-credentials": [{"prefix": "s3://bucket", "config": {"k": "v"}}]
  })";
  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kPlanResponse))));
  RegisterCredentialedFileIO();

  std::vector<std::string> requested_plan_ids;
  auto provider = std::make_shared<StaticCredentialProvider>();
  auto ctx = MakeContext();
  ctx.catalog_config = {{"io-impl", std::string(kCredentialedFileIO)}};
  ctx.credential_provider_factory = [&](const std::string& plan_id) {
    requested_plan_ids.push_back(plan_id);
    return provider;
  };
  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(std::move(ctx)));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());

  EXPECT_THAT(requested_plan_ids, ::testing::ElementsAre("plan-cred"));
  EXPECT_EQ(installed_provider, provider);
  ASSERT_EQ(installed_credentials.size(), 1);
  EXPECT_EQ(installed_credentials[0].prefix, "s3://bucket");
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
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kPlanResponse))));
  RegisterCredentialedFileIO();

  auto ctx = MakeContext();
  ctx.catalog_config = {{"io-impl", std::string(kCredentialedFileIO)}};
  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeScan(std::move(ctx)));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());

  EXPECT_EQ(installed_provider, nullptr);
  EXPECT_EQ(installed_credentials.size(), 1);
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
// PlanFilesStream: FAILED with plan-id is an invalid response → DELETE /plan
// called before returning the error.
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
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
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
// Stale credentials: second PlanFilesStream call resets the scan FileIO so that
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
    return {metadata_, file_io_, "test.my_table", nullptr, std::move(ctx)};
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
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kResponseBody))));

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
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kResponseBody))));

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
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kResponseBody))));

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
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kResponseBody))));

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
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kResponseBody))));

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
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kResponseBody))));

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

// --------------------------------------------------------------------------
// IncludeColumnStats() columns are sent as stats-fields.
// --------------------------------------------------------------------------
TEST_F(RestIncrementalAppendScanTest, IncludeColumnStatsSendsStatsFields) {
  constexpr std::string_view kResponseBody = R"({"status":"completed"})";
  EXPECT_CALL(
      *mock_client_,
      Post(_, JsonBodyHas("stats-fields", std::vector<std::string>{"id"}), _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kResponseBody))));

  auto builder = MakeBuilder(MakeContext());
  builder.FromSnapshot(kRootId).IncludeColumnStats({"id"});
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder.Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());
}

// --------------------------------------------------------------------------
// IncludeColumnStats() with an unknown column → ValidationFailed.
// --------------------------------------------------------------------------
TEST_F(RestIncrementalAppendScanTest, IncludeColumnStatsRejectsMissingColumn) {
  auto builder = MakeBuilder(MakeContext());
  builder.FromSnapshot(kRootId).IncludeColumnStats({"missing"});
  EXPECT_THAT(builder.Build(),
              ::testing::AllOf(IsError(ErrorKind::kValidationFailed),
                               HasErrorMessage("Cannot find stats column: missing")));
}

// --------------------------------------------------------------------------
// Project() columns are sent as select.
// --------------------------------------------------------------------------
TEST_F(RestIncrementalAppendScanTest, ProjectSendsProjectedColumnsAsSelect) {
  constexpr std::string_view kResponseBody = R"({"status":"completed"})";
  EXPECT_CALL(*mock_client_,
              Post(_, JsonBodyHas("select", std::vector<std::string>{"id"}), _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kResponseBody))));

  auto builder = MakeBuilder(MakeContext());
  builder.FromSnapshot(kRootId).Project(std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32())}));
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder.Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());
}

// --------------------------------------------------------------------------
// PlanFiles: FAILED with plan-id is an invalid response → DELETE /plan called
// before returning the error.
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
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
}

// --------------------------------------------------------------------------
// PlanFiles: invalid plan response with a plan-id → DELETE /plan called.
// --------------------------------------------------------------------------
TEST_F(RestIncrementalAppendScanTest, PlanFilesInvalidResponseWithPlanIdCancels) {
  constexpr std::string_view kInvalidBody =
      R"({"status":"submitted","plan-id":"plan-invalid-incr","plan-tasks":["tok-1"]})";
  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kInvalidBody))));
  EXPECT_CALL(*mock_client_, Delete(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, "{}")));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeIncrementalScan(MakeContext()));
  EXPECT_THAT(scan->PlanFiles(), IsError(ErrorKind::kValidationFailed));
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
  EXPECT_NE(scan->io().get(), file_io_.get());

  // Second call: no credentials. Verifies scan_io_ was cleared so stale
  // credentials from the first plan do not bleed into the second request.
  ICEBERG_UNWRAP_OR_FAIL(auto tasks2, scan->PlanFiles());
  EXPECT_TRUE(tasks2.empty());
  EXPECT_EQ(scan->io().get(), file_io_.get());
}

// --------------------------------------------------------------------------
// Storage credentials in COMPLETED response: io() returns a credential-scoped IO.
// --------------------------------------------------------------------------
TEST_F(RestIncrementalAppendScanTest,
       StorageCredentialsInPlanResponseUpdatesEffectiveIO) {
  constexpr std::string_view kResponseBody = R"({
    "status": "completed",
    "storage-credentials": [
      {"prefix": "s3://bucket/prefix", "config": {"key": "value"}}
    ]
  })";
  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kResponseBody))));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeIncrementalScan(MakeContext()));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());
  EXPECT_NE(scan->io().get(), file_io_.get());
}

// --------------------------------------------------------------------------
// No storage credentials: io() falls back to the table's FileIO.
// --------------------------------------------------------------------------
TEST_F(RestIncrementalAppendScanTest, NoStorageCredentialsEffectiveIoFallsBackToTableIO) {
  constexpr std::string_view kResponseBody = R"({"status":"completed"})";
  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kResponseBody))));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeIncrementalScan(MakeContext()));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());
  EXPECT_EQ(scan->io().get(), file_io_.get());
}

// --------------------------------------------------------------------------
// Storage credentials in a FetchScanTasks response are ignored.
// --------------------------------------------------------------------------
TEST_F(RestIncrementalAppendScanTest,
       StorageCredentialsInFetchScanTasksResponseAreIgnored) {
  constexpr std::string_view kPlanResponse =
      R"({"status":"completed","plan-id":"incr-plan-1","plan-tasks":["tok-incr-1"]})";
  constexpr std::string_view kTasksResponse = R"({
    "file-scan-tasks": [],
    "storage-credentials": [
      {"prefix": "s3://bucket/prefix", "config": {"key": "value"}}
    ]
  })";
  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kPlanResponse))))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, std::string(kTasksResponse))));

  ICEBERG_UNWRAP_OR_FAIL(auto scan, MakeIncrementalScan(MakeContext()));
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());
  EXPECT_EQ(scan->io().get(), file_io_.get());
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
