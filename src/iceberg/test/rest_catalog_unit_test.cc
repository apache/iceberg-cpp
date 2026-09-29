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

#include "iceberg/catalog/rest/rest_catalog.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/catalog/rest/catalog_properties.h"
#include "iceberg/catalog/rest/endpoint.h"
#include "iceberg/catalog/rest/error_handlers.h"
#include "iceberg/catalog/rest/http_client.h"
#include "iceberg/catalog/rest/resource_paths.h"
#include "iceberg/catalog/rest/rest_table.h"
#include "iceberg/file_io.h"
#include "iceberg/table_identifier.h"
#include "iceberg/test/matchers.h"

namespace iceberg::rest {

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

// --------------------------------------------------------------------------
// Helper JSON bodies for LoadTable GET responses
// --------------------------------------------------------------------------

// Minimal table metadata blob (no snapshots, no partitions).
constexpr std::string_view kBaseMetadataJson =
    R"("metadata-location":"s3://bucket/metadata/v1.json","metadata":{"format-version":2,"table-uuid":"test-uuid","location":"s3://bucket/test","last-sequence-number":0,"last-updated-ms":0,"last-column-id":1,"schemas":[{"type":"struct","schema-id":1,"fields":[{"id":1,"name":"id","type":"int","required":true}]}],"current-schema-id":1,"partition-specs":[{"spec-id":0,"fields":[]}],"default-spec-id":0,"last-partition-id":0,"sort-orders":[{"order-id":0,"fields":[]}],"default-sort-order-id":0,"properties":{}})";

// LoadTable response where server config requests server-side scan planning.
const std::string kLoadTableServerScanResponse =
    std::string("{\"config\":{\"scan-planning-mode\":\"server\"},") +
    std::string(kBaseMetadataJson) + "}";

// LoadTable response with no scan-planning-mode set (defaults to client).
const std::string kLoadTableDefaultResponse =
    std::string("{") + std::string(kBaseMetadataJson) + "}";

// --------------------------------------------------------------------------
// Test fixture: builds a RestCatalog via MakeForTesting so no HTTP calls
// are made during catalog construction.
// --------------------------------------------------------------------------
class RestCatalogLoadTableTest : public ::testing::Test {
 protected:
  void SetUp() override {
    mock_client_ = std::make_shared<MockHttpClient>();
    file_io_ = std::make_shared<NoOpFileIO>();

    ICEBERG_UNWRAP_OR_FAIL(paths_,
                           ResourcePaths::Make("http://test-server", /*prefix=*/"",
                                               /*namespace_separator=*/"%1F"));

    identifier_ = TableIdentifier{.ns = Namespace{{"default"}}, .name = "my_table"};

    all_plan_endpoints_ = {Endpoint::LoadTable(),         Endpoint::PlanTableScan(),
                           Endpoint::FetchPlanningResult(), Endpoint::CancelPlanning(),
                           Endpoint::FetchScanTasks()};

    no_plan_endpoint_set_ = {Endpoint::LoadTable()};
  }

  // Builds a RestCatalog with the given supported endpoints and an optional
  // client-side scan-planning-mode setting.
  Result<std::shared_ptr<RestCatalog>> MakeCatalog(
      const std::unordered_set<Endpoint>& endpoints,
      const std::string& client_scan_mode = "") {
    std::unordered_map<std::string, std::string> props{
        {"uri", "http://test-server"},
    };
    if (!client_scan_mode.empty()) {
      props["scan-planning-mode"] = client_scan_mode;
    }
    auto config = RestCatalogProperties::FromMap(props);
    return RestCatalog::MakeForTesting(std::move(config), file_io_, mock_client_, paths_,
                                       endpoints);
  }

  std::shared_ptr<MockHttpClient> mock_client_;
  std::shared_ptr<FileIO> file_io_;
  std::shared_ptr<ResourcePaths> paths_;
  TableIdentifier identifier_;
  std::unordered_set<Endpoint> all_plan_endpoints_;
  std::unordered_set<Endpoint> no_plan_endpoint_set_;
};

// --------------------------------------------------------------------------
// Table config "scan-planning-mode":"server" → LoadTable returns a RestTable.
// --------------------------------------------------------------------------
TEST_F(RestCatalogLoadTableTest, TableConfigServerScanReturnsRestTable) {
  EXPECT_CALL(*mock_client_, Get(_, _, _, _, _))
      .WillOnce(
          Return(HttpResponse::MakeForTesting(200, kLoadTableServerScanResponse)));

  ICEBERG_UNWRAP_OR_FAIL(auto catalog, MakeCatalog(all_plan_endpoints_));
  ICEBERG_UNWRAP_OR_FAIL(auto as_catalog, catalog->AsCatalog());
  ICEBERG_UNWRAP_OR_FAIL(auto table, as_catalog->LoadTable(identifier_));

  EXPECT_NE(dynamic_cast<RestTable*>(table.get()), nullptr)
      << "Expected RestTable when server config requests server-side scan planning";
}

// --------------------------------------------------------------------------
// Client config "scan-planning-mode":"server", table config absent →
// LoadTable returns a RestTable.
// --------------------------------------------------------------------------
TEST_F(RestCatalogLoadTableTest, ClientConfigServerScanReturnsRestTable) {
  EXPECT_CALL(*mock_client_, Get(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, kLoadTableDefaultResponse)));

  ICEBERG_UNWRAP_OR_FAIL(auto catalog,
                         MakeCatalog(all_plan_endpoints_, /*client_scan_mode=*/"server"));
  ICEBERG_UNWRAP_OR_FAIL(auto as_catalog, catalog->AsCatalog());
  ICEBERG_UNWRAP_OR_FAIL(auto table, as_catalog->LoadTable(identifier_));

  EXPECT_NE(dynamic_cast<RestTable*>(table.get()), nullptr)
      << "Expected RestTable when client config requests server-side scan planning";
}

// --------------------------------------------------------------------------
// "scan-planning-mode":"server" but PlanTableScan endpoint missing →
// LoadTable returns NotSupported.
// --------------------------------------------------------------------------
TEST_F(RestCatalogLoadTableTest, ServerScanWithMissingEndpointReturnsNotSupported) {
  EXPECT_CALL(*mock_client_, Get(_, _, _, _, _))
      .WillOnce(
          Return(HttpResponse::MakeForTesting(200, kLoadTableServerScanResponse)));

  ICEBERG_UNWRAP_OR_FAIL(auto catalog, MakeCatalog(no_plan_endpoint_set_));
  ICEBERG_UNWRAP_OR_FAIL(auto as_catalog, catalog->AsCatalog());
  auto result = as_catalog->LoadTable(identifier_);

  EXPECT_THAT(result, IsError(ErrorKind::kNotSupported));
}

// --------------------------------------------------------------------------
// No scan-planning-mode set anywhere → LoadTable returns a plain Table
// (not RestTable).
// --------------------------------------------------------------------------
TEST_F(RestCatalogLoadTableTest, DefaultScanModeReturnsPlainTable) {
  EXPECT_CALL(*mock_client_, Get(_, _, _, _, _))
      .WillOnce(Return(HttpResponse::MakeForTesting(200, kLoadTableDefaultResponse)));

  ICEBERG_UNWRAP_OR_FAIL(auto catalog, MakeCatalog(all_plan_endpoints_));
  ICEBERG_UNWRAP_OR_FAIL(auto as_catalog, catalog->AsCatalog());
  ICEBERG_UNWRAP_OR_FAIL(auto table, as_catalog->LoadTable(identifier_));

  EXPECT_EQ(dynamic_cast<RestTable*>(table.get()), nullptr)
      << "Expected plain Table (not RestTable) when no scan-planning-mode is configured";
}

}  // namespace iceberg::rest
