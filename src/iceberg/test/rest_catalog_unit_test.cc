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

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/catalog.h"
#include "iceberg/catalog/rest/auth/auth_properties.h"
#include "iceberg/catalog/rest/auth/auth_session.h"
#include "iceberg/catalog/rest/catalog_properties.h"
#include "iceberg/catalog/rest/endpoint.h"
#include "iceberg/catalog/rest/error_handlers.h"
#include "iceberg/catalog/rest/http_client.h"
#include "iceberg/catalog/rest/json_serde_internal.h"
#include "iceberg/catalog/rest/rest_catalog.h"
#include "iceberg/catalog/rest/rest_table.h"
#include "iceberg/catalog/rest/rest_table_scan.h"
#include "iceberg/catalog/rest/types.h"
#include "iceberg/file_io.h"
#include "iceberg/file_io_registry.h"
#include "iceberg/labels.h"
#include "iceberg/schema.h"
#include "iceberg/storage_credential.h"
#include "iceberg/table.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_requirement.h"
#include "iceberg/table_update.h"
#include "iceberg/test/executor.h"
#include "iceberg/test/matchers.h"

namespace iceberg::rest {

namespace {

using ::testing::_;
using ::testing::Return;

// --------------------------------------------------------------------------
// Mock HTTP client (same pattern as rest_table_scan_test.cc)
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

  MOCK_METHOD(Result<HttpResponse>, PostForm,
              (const std::string& path,
               (const std::unordered_map<std::string, std::string>&)form_data,
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
// Minimal FileIO stub
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

std::shared_ptr<StorageCredentialProvider> installed_provider;

class CredentialedFileIO : public NoOpFileIO, public SupportsStorageCredentials {
 public:
  Status InitializeStorageCredentials(
      const std::vector<StorageCredential>& credentials,
      std::shared_ptr<StorageCredentialProvider> provider) override {
    credentials_ = credentials;
    installed_provider = std::move(provider);
    return {};
  }

  Status SetStorageCredentials(
      const std::vector<StorageCredential>& credentials) override {
    credentials_ = credentials;
    return {};
  }

  std::vector<StorageCredential> credentials() const override { return credentials_; }

  SupportsStorageCredentials* AsSupportsStorageCredentials() override { return this; }

 private:
  std::vector<StorageCredential> credentials_;
};

constexpr std::string_view kCredentialedFileIO = "rest-catalog-unit-test-credentialed";

void RegisterCredentialedFileIO() {
  FileIORegistry::Register(
      std::string(kCredentialedFileIO),
      {.create = [](const std::unordered_map<std::string, std::string>&)
           -> Result<std::unique_ptr<FileIO>> {
        return std::make_unique<CredentialedFileIO>();
      }});
  installed_provider = nullptr;
}

// Plan response that vends credentials for the scan.
constexpr std::string_view kPlanWithCredentialsResponse = R"({
  "status": "completed",
  "plan-id": "plan-1",
  "storage-credentials": [{"prefix": "s3://bucket", "config": {"k": "v"}}]
})";

}  // namespace

// --------------------------------------------------------------------------
// Helper JSON bodies for LoadTable GET responses
// --------------------------------------------------------------------------

// Minimal table metadata blob (no snapshots, no partitions).
constexpr std::string_view kBaseMetadataJson =
    R"("metadata-location":"s3://bucket/metadata/v1.json","metadata":{"format-version":2,"table-uuid":"test-uuid","location":"s3://bucket/test","last-sequence-number":0,"last-updated-ms":0,"last-column-id":1,"schemas":[{"type":"struct","schema-id":1,"fields":[{"id":1,"name":"id","type":"int","required":true}]}],"current-schema-id":1,"partition-specs":[{"spec-id":0,"fields":[]}],"default-spec-id":0,"last-partition-id":0,"sort-orders":[{"order-id":0,"fields":[]}],"default-sort-order-id":0,"properties":{}})";

// LoadTable response where server config requests server-side scan planning.
const std::string kLoadTableServerScanResponse =
    std::string(R"({"config":{"scan-planning-mode":"server"},)") +
    std::string(kBaseMetadataJson) + "}";

// LoadTable response with no scan-planning-mode set (defaults to client).
const std::string kLoadTableDefaultResponse =
    std::string("{") + std::string(kBaseMetadataJson) + "}";

// UpdateTable (commit) response.
const std::string kCommitTableResponse =
    std::string("{") + std::string(kBaseMetadataJson) + "}";

// --------------------------------------------------------------------------
// Use the regular catalog initialization with a mock HTTP client.
// --------------------------------------------------------------------------
class RestCatalogLoadTableTest : public ::testing::Test {
 protected:
  void TearDown() override { installed_provider.reset(); }

  void SetUp() override {
    mock_client_ = std::make_shared<MockHttpClient>();
    FileIORegistry::Register(
        "rest-catalog-unit-test-noop",
        {.create = [](const std::unordered_map<std::string, std::string>&)
             -> Result<std::unique_ptr<FileIO>> {
          return std::make_unique<NoOpFileIO>();
        }});

    identifier_ = TableIdentifier{.ns = Namespace{{"default"}}, .name = "my_table"};

    all_plan_endpoints_ = {Endpoint::LoadTable(),      Endpoint::UpdateTable(),
                           Endpoint::PlanTableScan(),  Endpoint::FetchPlanningResult(),
                           Endpoint::CancelPlanning(), Endpoint::FetchScanTasks()};

    no_plan_endpoint_set_ = {Endpoint::LoadTable()};
  }

  // Builds a RestCatalog with the given supported endpoints and an optional
  // client-side scan-planning-mode setting.
  Result<std::shared_ptr<RestCatalog>> MakeCatalog(
      const std::unordered_set<Endpoint>& endpoints,
      const std::string& client_scan_mode = "",
      std::unordered_map<std::string, std::string> extra_props = {}) {
    std::unordered_map<std::string, std::string> props = std::move(extra_props);
    props["uri"] = "http://test-server";
    props.try_emplace("io-impl", "rest-catalog-unit-test-noop");
    if (!client_scan_mode.empty()) {
      props["scan-planning-mode"] = client_scan_mode;
    }
    CatalogConfig server_config{.endpoints = {endpoints.begin(), endpoints.end()}};
    EXPECT_CALL(*mock_client_, Get(::testing::EndsWith("/v1/config"), _, _, _, _))
        .WillOnce(Return(HttpResponse(200, ToJson(server_config).dump())));
    auto config = RestCatalogProperties::FromMap(std::move(props));
    return RestCatalog::Make(config,
                             [client = mock_client_](const auto&) { return client; });
  }

  std::shared_ptr<MockHttpClient> mock_client_;
  TableIdentifier identifier_;
  std::unordered_set<Endpoint> all_plan_endpoints_;
  std::unordered_set<Endpoint> no_plan_endpoint_set_;
};

class RestCatalogLabelsTest : public RestCatalogLoadTableTest,
                              public ::testing::WithParamInterface<bool> {
 protected:
  std::string LabelsResponse() const {
    auto response = nlohmann::json::parse(kLoadTableDefaultResponse);
    response["config"]["scan-planning-mode"] = GetParam() ? "server" : "client";
    response["labels"] = nlohmann::json::parse(
        R"({"object-labels":{"owner":"team-a"},"fields":[{"field-id":1,"labels":{"classification":"pii"}}]})");
    return response.dump();
  }

  Result<std::shared_ptr<Catalog>> MakeLabelsCatalog() {
    auto endpoints = all_plan_endpoints_;
    endpoints.insert(Endpoint::CreateTable());
    endpoints.insert(Endpoint::RegisterTable());
    ICEBERG_ASSIGN_OR_RAISE(auto catalog, MakeCatalog(endpoints));
    return catalog->AsCatalog();
  }

  void CheckLabels(const std::shared_ptr<Table>& table) const {
    ASSERT_NE(table, nullptr);
    EXPECT_EQ(table->labels(), labels_);
    EXPECT_EQ(std::dynamic_pointer_cast<RestTable>(table) != nullptr, GetParam());
  }

  const Labels labels_{
      .object_labels = {{"owner", "team-a"}},
      .fields = {{.field_id = 1, .labels = {{"classification", "pii"}}}}};
};

TEST_P(RestCatalogLabelsTest, LoadExposesLabels) {
  EXPECT_CALL(*mock_client_, Get(::testing::EndsWith("/tables/my_table"), _, _, _, _))
      .WillOnce(Return(HttpResponse(200, LabelsResponse())));
  ICEBERG_UNWRAP_OR_FAIL(auto catalog, MakeLabelsCatalog());
  ICEBERG_UNWRAP_OR_FAIL(auto table, catalog->LoadTable(identifier_));

  CheckLabels(table);
}

TEST_P(RestCatalogLabelsTest, CreateExposesLabels) {
  EXPECT_CALL(*mock_client_,
              Post(::testing::EndsWith("/namespaces/default/tables"), _, _, _, _))
      .WillOnce(Return(HttpResponse(200, LabelsResponse())));
  ICEBERG_UNWRAP_OR_FAIL(auto catalog, MakeLabelsCatalog());
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32())}, 1);
  ICEBERG_UNWRAP_OR_FAIL(auto table,
                         catalog->CreateTable(identifier_, schema, /*spec=*/nullptr,
                                              /*order=*/nullptr, /*location=*/"",
                                              /*properties=*/{}));

  CheckLabels(table);
}

TEST_P(RestCatalogLabelsTest, RegisterExposesLabels) {
  EXPECT_CALL(*mock_client_,
              Post(::testing::EndsWith("/namespaces/default/register"), _, _, _, _))
      .WillOnce(Return(HttpResponse(200, LabelsResponse())));
  ICEBERG_UNWRAP_OR_FAIL(auto catalog, MakeLabelsCatalog());
  ICEBERG_UNWRAP_OR_FAIL(
      auto table, catalog->RegisterTable(identifier_, "s3://bucket/metadata/v1.json"));

  CheckLabels(table);
}

INSTANTIATE_TEST_SUITE_P(PlanningModes, RestCatalogLabelsTest, ::testing::Bool(),
                         [](const ::testing::TestParamInfo<bool>& info) {
                           return info.param ? "Server" : "Client";
                         });

TEST_F(RestCatalogLoadTableTest, InitializationPropagatesClientFactoryErrors) {
  auto config = RestCatalogProperties::FromMap(
      {{"uri", "http://test-server"}, {"io-impl", "rest-catalog-unit-test-noop"}});
  for (int fail_on_call : {1, 2}) {
    SCOPED_TRACE(fail_on_call);
    if (fail_on_call == 1) {
      EXPECT_CALL(*mock_client_, Get(_, _, _, _, _)).Times(0);
    } else {
      EXPECT_CALL(*mock_client_, Get(::testing::EndsWith("/v1/config"), _, _, _, _))
          .WillOnce(Return(HttpResponse(200, ToJson(CatalogConfig{}).dump())));
    }
    int calls = 0;
    auto result = RestCatalog::Make(
        config, [&](const RestCatalogProperties&) -> Result<std::shared_ptr<HttpClient>> {
          if (++calls == fail_on_call) {
            return IOError("HTTP client setup failed");
          }
          return mock_client_;
        });
    ASSERT_THAT(result, IsError(ErrorKind::kIOError));
    EXPECT_EQ(result.error().message, "HTTP client setup failed");
    EXPECT_EQ(calls, fail_on_call);
    ASSERT_TRUE(::testing::Mock::VerifyAndClearExpectations(mock_client_.get()));
  }
}

TEST_F(RestCatalogLoadTableTest, OAuthRefreshRunsOnAuthExecutorWhenEnabled) {
  for (bool refresh : {false, true}) {
    SCOPED_TRACE(::testing::Message() << "token-refresh-enabled=" << refresh);
    test::MockExecutor executor;
    auto& submits = EXPECT_CALL(executor, Submit).Times(refresh ? 1 : 0);
    if (refresh) {
      submits.WillOnce([](ExecutorTask task) -> Status {
        std::move(task)();
        return {};
      });
    }
    auto config = RestCatalogProperties::FromMap(
        {{"uri", "http://test-server"},
         {"io-impl", "rest-catalog-unit-test-noop"},
         {"credential", "client:secret"},
         {"token-refresh-enabled", refresh ? "true" : "false"}});
    auto& tokens =
        EXPECT_CALL(*mock_client_, PostForm(_, _, _, _, _)).Times(refresh ? 2 : 1);
    tokens.WillOnce(Return(HttpResponse(
        200, R"({"access_token":"initial","token_type":"bearer","expires_in":0})")));
    if (refresh) {
      tokens.WillOnce(Return(
          HttpResponse(200, R"({"access_token":"updated","token_type":"bearer"})")));
    }
    EXPECT_CALL(*mock_client_, Get(::testing::EndsWith("/v1/config"), _, _, _, _))
        .WillOnce(Return(HttpResponse(200, ToJson(CatalogConfig{}).dump())));
    ICEBERG_UNWRAP_OR_FAIL(
        auto root,
        RestCatalog::Make(
            config, [client = mock_client_](const auto&) { return client; }, &executor));

    // Let any immediately due catalog refresh dispatch first.
    ASSERT_TRUE(test::WaitForSharedTimer());

    EXPECT_CALL(*mock_client_, Get(::testing::EndsWith("/namespaces"), _, _, _, _))
        .WillOnce([&](const auto&, const auto&, const auto&, const auto&,
                      auth::AuthSession& session) -> Result<HttpResponse> {
          ICEBERG_ASSIGN_OR_RAISE(auto request, session.Authenticate({}));
          EXPECT_EQ(request.headers.at("Authorization"),
                    refresh ? "Bearer updated" : "Bearer initial");
          return HttpResponse(200, R"({"namespaces":[]})");
        });
    ICEBERG_UNWRAP_OR_FAIL(auto catalog, root->AsCatalog());
    ASSERT_THAT(catalog->ListNamespaces({}), IsOk());
    ASSERT_TRUE(::testing::Mock::VerifyAndClearExpectations(mock_client_.get()));
  }
}

TEST_F(RestCatalogLoadTableTest, InitializationUsesMergedServerConfig) {
  CatalogConfig server_config{
      .defaults = {{"header.X-Default", "default"}, {"header.X-Shared", "default"}},
      .overrides = {{"prefix", "catalog"},
                    {"snapshot-loading-mode", "REFS"},
                    {"scan-planning-mode", "server"},
                    {"header.X-Shared", "server"}},
      .endpoints = {all_plan_endpoints_.begin(), all_plan_endpoints_.end()}};
  EXPECT_CALL(
      *mock_client_,
      Get(::testing::EndsWith("/v1/config"),
          ::testing::Contains(::testing::Pair("warehouse", "client-warehouse")), _, _, _))
      .WillOnce(Return(HttpResponse(200, ToJson(server_config).dump())));
  EXPECT_CALL(*mock_client_,
              Get(::testing::EndsWith("/v1/catalog/namespaces/default/tables/my_table"),
                  ::testing::Contains(::testing::Pair("snapshots", "refs")), _, _, _))
      .WillOnce(Return(HttpResponse(200, kLoadTableDefaultResponse)));
  EXPECT_CALL(
      *mock_client_,
      Post(::testing::EndsWith("/v1/catalog/namespaces/default/tables/my_table/plan"), _,
           _, _, _))
      .WillOnce(Return(HttpResponse(200, R"({"status":"completed"})")));

  auto config =
      RestCatalogProperties::FromMap({{"uri", "http://test-server/"},
                                      {"warehouse", "client-warehouse"},
                                      {"io-impl", "rest-catalog-unit-test-noop"},
                                      {"scan-planning-mode", "client"},
                                      {"header.X-Shared", "client"}});
  std::vector<RestCatalogProperties> client_configs;
  ICEBERG_UNWRAP_OR_FAIL(
      auto root, RestCatalog::Make(config, [&](const RestCatalogProperties& properties) {
        client_configs.push_back(properties);
        return mock_client_;
      }));
  ASSERT_EQ(client_configs.size(), 2);
  EXPECT_EQ(client_configs[0].ExtractHeaders().at("X-Shared"), "client");
  EXPECT_FALSE(client_configs[0].ExtractHeaders().contains("X-Default"));
  EXPECT_EQ(client_configs[1].ExtractHeaders().at("X-Shared"), "server");
  EXPECT_EQ(client_configs[1].ExtractHeaders().at("X-Default"), "default");
  EXPECT_EQ(client_configs[1].Get(RestCatalogProperties::kPrefix), "catalog");
  ICEBERG_UNWRAP_OR_FAIL(auto catalog, root->AsCatalog());
  ICEBERG_UNWRAP_OR_FAIL(auto table, catalog->LoadTable(identifier_));
  ICEBERG_UNWRAP_OR_FAIL(auto builder, table->NewScan());
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  EXPECT_TRUE(tasks.empty());
}

// --------------------------------------------------------------------------
// "scan-planning-mode":"server" but PlanTableScan endpoint missing →
// LoadTable returns NotSupported.
// --------------------------------------------------------------------------
TEST_F(RestCatalogLoadTableTest, ServerScanWithMissingEndpointReturnsNotSupported) {
  EXPECT_CALL(*mock_client_, Get(_, _, _, _, _))
      .WillOnce(Return(HttpResponse(200, kLoadTableServerScanResponse)));

  ICEBERG_UNWRAP_OR_FAIL(auto catalog, MakeCatalog(no_plan_endpoint_set_));
  ICEBERG_UNWRAP_OR_FAIL(auto as_catalog, catalog->AsCatalog());
  auto result = as_catalog->LoadTable(identifier_);

  EXPECT_THAT(result, IsError(ErrorKind::kNotSupported));
}

// --------------------------------------------------------------------------
// A completed plan without a plan-id refreshes credentials without planId.
// --------------------------------------------------------------------------
TEST_F(RestCatalogLoadTableTest, ScanCredentialProviderWithoutPlanIdOmitsPlanId) {
  RegisterCredentialedFileIO();
  EXPECT_CALL(*mock_client_,
              Get(::testing::Not(::testing::HasSubstr("/credentials")), _, _, _, _))
      .WillOnce(Return(HttpResponse(200, kLoadTableServerScanResponse)));
  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse(
          200,
          R"({"status":"completed","storage-credentials":[{"prefix":"s3://bucket","config":{"k":"v"}}]})")));
  EXPECT_CALL(*mock_client_,
              Get(::testing::HasSubstr("/credentials"), ::testing::IsEmpty(), _, _, _))
      .WillOnce(Return(HttpResponse(
          200,
          R"({"storage-credentials":[{"prefix":"s3://bucket","config":{"k":"v2"}}]})")));

  auto endpoints = all_plan_endpoints_;
  endpoints.insert(Endpoint::TableCredentials());
  ICEBERG_UNWRAP_OR_FAIL(auto catalog,
                         MakeCatalog(endpoints, /*client_scan_mode=*/"",
                                     {{"io-impl", std::string(kCredentialedFileIO)}}));
  ICEBERG_UNWRAP_OR_FAIL(auto as_catalog, catalog->AsCatalog());
  ICEBERG_UNWRAP_OR_FAIL(auto table, as_catalog->LoadTable(identifier_));
  ICEBERG_UNWRAP_OR_FAIL(auto builder, table->NewScan());
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());

  ASSERT_NE(installed_provider, nullptr);
  ICEBERG_UNWRAP_OR_FAIL(auto refreshed, installed_provider->Load());
  ASSERT_EQ(refreshed.size(), 1);
}

// --------------------------------------------------------------------------
// Without the TableCredentials endpoint, scan credentials are not refreshed.
// --------------------------------------------------------------------------
TEST_F(RestCatalogLoadTableTest,
       ScanCredentialsWithoutCredentialsEndpointHaveNoProvider) {
  RegisterCredentialedFileIO();
  EXPECT_CALL(*mock_client_, Get(_, _, _, _, _))
      .WillOnce(Return(HttpResponse(200, kLoadTableServerScanResponse)));
  EXPECT_CALL(*mock_client_, Post(_, _, _, _, _))
      .WillOnce(Return(HttpResponse(200, std::string(kPlanWithCredentialsResponse))));

  ICEBERG_UNWRAP_OR_FAIL(auto catalog,
                         MakeCatalog(all_plan_endpoints_, /*client_scan_mode=*/"",
                                     {{"io-impl", std::string(kCredentialedFileIO)}}));
  ICEBERG_UNWRAP_OR_FAIL(auto as_catalog, catalog->AsCatalog());
  ICEBERG_UNWRAP_OR_FAIL(auto table, as_catalog->LoadTable(identifier_));
  ICEBERG_UNWRAP_OR_FAIL(auto builder, table->NewScan());
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());

  EXPECT_EQ(installed_provider, nullptr);
  auto* credentialed = scan->io()->AsSupportsStorageCredentials();
  ASSERT_NE(credentialed, nullptr);
  ASSERT_EQ(credentialed->credentials().size(), 1);
  EXPECT_EQ(credentialed->credentials()[0].config.at("k"), "v");
}

struct PlanningModeCase {
  std::string client;
  std::string table;
  bool server;
};

class RestCatalogPlanningModeTest
    : public RestCatalogLoadTableTest,
      public ::testing::WithParamInterface<PlanningModeCase> {};

TEST_P(RestCatalogPlanningModeTest, LoadAndCommitKeepTheEffectivePlanningMode) {
  const auto& config = GetParam();
  auto response = nlohmann::json::parse(kLoadTableDefaultResponse);
  if (!config.table.empty()) response["config"]["scan-planning-mode"] = config.table;
  EXPECT_CALL(*mock_client_, Get(::testing::EndsWith("/tables/my_table"), _, _, _, _))
      .WillOnce(Return(HttpResponse(200, response.dump())));
  EXPECT_CALL(*mock_client_, Post(::testing::EndsWith("/tables/my_table"), _, _, _, _))
      .WillOnce(Return(HttpResponse(200, kCommitTableResponse)));
  EXPECT_CALL(*mock_client_, Post(::testing::EndsWith("/plan"), _, _, _, _))
      .Times(config.server ? 2 : 0)
      .WillRepeatedly([](auto&&...) -> Result<HttpResponse> {
        return HttpResponse(200, R"({"status":"completed"})");
      });

  ICEBERG_UNWRAP_OR_FAIL(auto catalog, MakeCatalog(all_plan_endpoints_, config.client));
  ICEBERG_UNWRAP_OR_FAIL(auto as_catalog, catalog->AsCatalog());
  ICEBERG_UNWRAP_OR_FAIL(auto table, as_catalog->LoadTable(identifier_));
  ICEBERG_UNWRAP_OR_FAIL(auto committed,
                         table->catalog()->UpdateTable(identifier_, {}, {}));
  for (const auto& current : {table, committed}) {
    ICEBERG_UNWRAP_OR_FAIL(auto builder, current->NewScan());
    ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
    ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
    EXPECT_TRUE(tasks.empty());
  }
}

INSTANTIATE_TEST_SUITE_P(
    PlanningModes, RestCatalogPlanningModeTest,
    ::testing::Values(
        PlanningModeCase{.client = "", .table = "", .server = false},
        PlanningModeCase{.client = "server", .table = "", .server = true},
        PlanningModeCase{.client = "", .table = "server", .server = true},
        PlanningModeCase{.client = "server", .table = "client", .server = false},
        PlanningModeCase{.client = "client", .table = "server", .server = true}));

class RestCatalogScanLifetimeTest : public RestCatalogLoadTableTest,
                                    public ::testing::WithParamInterface<bool> {};

TEST_P(RestCatalogScanLifetimeTest, ScanKeepsCredentialRefreshAlive) {
  RegisterCredentialedFileIO();
  auto response = nlohmann::json::parse(kLoadTableServerScanResponse);
  auto& metadata = response["metadata"];
  metadata["current-snapshot-id"] = 200;
  metadata["last-sequence-number"] = 2;
  metadata["snapshots"] =
      nlohmann::json::array({{{"snapshot-id", 100},
                              {"sequence-number", 1},
                              {"timestamp-ms", 1},
                              {"manifest-list", "s3://bucket/manifests-1.avro"},
                              {"schema-id", 1}},
                             {{"snapshot-id", 200},
                              {"parent-snapshot-id", 100},
                              {"sequence-number", 2},
                              {"timestamp-ms", 2},
                              {"manifest-list", "s3://bucket/manifests-2.avro"},
                              {"schema-id", 1}}});
  EXPECT_CALL(*mock_client_, Get(::testing::EndsWith("/tables/my_table"), _, _, _, _))
      .WillOnce(Return(HttpResponse(200, response.dump())));
  EXPECT_CALL(*mock_client_, Post(::testing::EndsWith("/plan"), _, _, _, _))
      .WillOnce(Return(HttpResponse(200, std::string(kPlanWithCredentialsResponse))));
  EXPECT_CALL(*mock_client_,
              Get(::testing::EndsWith("/credentials"),
                  ::testing::Contains(::testing::Pair("planId", "plan-1")), _, _, _))
      .WillOnce(Return(HttpResponse(
          200,
          R"({"storage-credentials":[{"prefix":"s3://bucket","config":{"k":"v2"}}]})")));

  auto endpoints = all_plan_endpoints_;
  endpoints.insert(Endpoint::TableCredentials());
  ICEBERG_UNWRAP_OR_FAIL(
      auto catalog,
      MakeCatalog(endpoints, "", {{"io-impl", std::string(kCredentialedFileIO)}}));
  std::weak_ptr<RestCatalog> weak_catalog = catalog;
  ICEBERG_UNWRAP_OR_FAIL(auto as_catalog, catalog->AsCatalog());
  ICEBERG_UNWRAP_OR_FAIL(auto table, as_catalog->LoadTable(identifier_));
  auto exercise = [&](auto& builder) {
    ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
    builder.reset();
    table.reset();
    as_catalog.reset();
    catalog.reset();
    EXPECT_FALSE(weak_catalog.expired());

    ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
    ASSERT_NE(installed_provider, nullptr);
    auto* credentialed = scan->io()->AsSupportsStorageCredentials();
    ASSERT_NE(credentialed, nullptr);
    ASSERT_EQ(credentialed->credentials().size(), 1);
    EXPECT_EQ(credentialed->credentials()[0].config.at("k"), "v");
    ICEBERG_UNWRAP_OR_FAIL(auto refreshed, installed_provider->Load());
    ASSERT_EQ(refreshed.size(), 1);
    EXPECT_EQ(refreshed[0].config.at("k"), "v2");

    scan.reset();
    EXPECT_TRUE(weak_catalog.expired());
  };
  if (GetParam()) {
    ICEBERG_UNWRAP_OR_FAIL(auto builder, table->NewIncrementalAppendScan());
    builder->FromSnapshot(100);
    exercise(builder);
  } else {
    ICEBERG_UNWRAP_OR_FAIL(auto builder, table->NewScan());
    exercise(builder);
  }
}

INSTANTIATE_TEST_SUITE_P(ScanModes, RestCatalogScanLifetimeTest, ::testing::Bool());

// --------------------------------------------------------------------------
// Client-only properties tests
// --------------------------------------------------------------------------

class RestCatalogClientOnlyPropertiesTest : public ::testing::Test {
 protected:
  void SetUp() override {
    mock_client_ = std::make_shared<MockHttpClient>();
    FileIORegistry::Register(
        "rest-catalog-unit-test-noop",
        {.create = [](const std::unordered_map<std::string, std::string>&)
             -> Result<std::unique_ptr<FileIO>> {
          return std::make_unique<NoOpFileIO>();
        }});
  }

  // Runs RestCatalog::Make against a server that returns `server_config` from
  // /v1/config, and returns the configurations handed to the HTTP client
  // factory. The second entry is the merged configuration.
  std::vector<RestCatalogProperties> MakeWithServerConfig(
      const CatalogConfig& server_config,
      std::unordered_map<std::string, std::string> client_properties) {
    client_properties["io-impl"] = "rest-catalog-unit-test-noop";
    // These tests inspect the merged configuration only; disable refresh so no auth
    // executor is required.
    client_properties["token-refresh-enabled"] = "false";
    EXPECT_CALL(*mock_client_, Get(::testing::EndsWith("/v1/config"), _, _, _, _))
        .WillOnce(Return(HttpResponse(200, ToJson(server_config).dump())));
    EXPECT_CALL(*mock_client_, PostForm(_, _, _, _, _))
        .WillRepeatedly(
            [](const std::string&, const std::unordered_map<std::string, std::string>&,
               const std::unordered_map<std::string, std::string>&, const ErrorHandler&,
               auth::AuthSession&) -> Result<HttpResponse> {
              return HttpResponse(200, R"({"access_token":"tok","token_type":"bearer"})");
            });

    auto config = RestCatalogProperties::FromMap(std::move(client_properties));
    std::vector<RestCatalogProperties> captured;
    auto result = RestCatalog::Make(
        config, [&captured, this](const RestCatalogProperties& properties) {
          captured.push_back(properties);
          return mock_client_;
        });
    EXPECT_THAT(result, IsOk());
    return captured;
  }

  std::shared_ptr<MockHttpClient> mock_client_;
};

TEST_F(RestCatalogClientOnlyPropertiesTest,
       ServerOverrideAppliesWhenNoClientOnlyPropertiesAreSet) {
  auto configs = MakeWithServerConfig(
      CatalogConfig{.defaults = {},
                    .overrides = {{"oauth2-server-uri", "https://server.example/token"}},
                    .endpoints = {}},
      {{"uri", "https://catalog.example"}, {"credential", "client:secret"}});

  ASSERT_EQ(configs.size(), 2);
  // Upstream REST behavior is unchanged when the list is empty.
  EXPECT_EQ(
      configs[1].configs().at(std::string(auth::AuthProperties::kOAuth2ServerUri.key())),
      "https://server.example/token");
}

TEST_F(RestCatalogClientOnlyPropertiesTest,
       ClientOnlyPropertiesProtectsExplicitOAuth2ServerUri) {
  auto configs = MakeWithServerConfig(
      CatalogConfig{.defaults = {},
                    .overrides = {{"oauth2-server-uri", "https://server.example/token"}},
                    .endpoints = {}},
      {{"uri", "https://catalog.example"},
       {"credential", "client:secret"},
       {"oauth2-server-uri", "https://idp.example/token"},
       {"rest.client-only-properties", "oauth2-server-uri"}});

  ASSERT_EQ(configs.size(), 2);
  EXPECT_EQ(
      configs[1].configs().at(std::string(auth::AuthProperties::kOAuth2ServerUri.key())),
      "https://idp.example/token");
}

TEST_F(RestCatalogClientOnlyPropertiesTest,
       ClientOnlyPropertiesProtectsDefaultOAuth2ServerUriFromIndirectOverride) {
  auto configs = MakeWithServerConfig(
      CatalogConfig{.defaults = {},
                    .overrides = {{"uri", "https://collector.example"},
                                  {"oauth2-server-uri", "https://server.example/token"}},
                    .endpoints = {}},
      {{"uri", "https://catalog.example"},
       {"credential", "client:secret"},
       {"rest.client-only-properties", "oauth2-server-uri"}});

  ASSERT_EQ(configs.size(), 2);
  // The endpoint is pinned against the client URI before merging, so a server
  // override of `uri` cannot move it.
  EXPECT_EQ(
      configs[1].configs().at(std::string(auth::AuthProperties::kOAuth2ServerUri.key())),
      "https://catalog.example/v1/oauth/tokens");
  // `uri` itself is still overridable.
  EXPECT_EQ(configs[1].Get(RestCatalogProperties::kUri), "https://collector.example");
}

}  // namespace

}  // namespace iceberg::rest
