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

#include <unistd.h>

#include <chrono>
#include <memory>
#include <print>
#include <string>
#include <thread>
#include <unordered_map>

#include <arpa/inet.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <nlohmann/json.hpp>
#include <sys/socket.h>

#include "iceberg/catalog/rest/catalog_properties.h"
#include "iceberg/catalog/rest/error_handlers.h"
#include "iceberg/catalog/rest/http_client.h"
#include "iceberg/catalog/rest/json_internal.h"
#include "iceberg/partition_spec.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/sort_order.h"
#include "iceberg/table.h"
#include "iceberg/table_identifier.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/std_io.h"
#include "iceberg/test/test_resource.h"
#include "iceberg/test/util/docker_compose_util.h"

namespace iceberg::rest {

namespace {

constexpr uint16_t kRestCatalogPort = 8181;
constexpr int kMaxRetries = 60;  // Wait up to 60 seconds
constexpr int kRetryDelayMs = 1000;

constexpr std::string_view kDockerProjectName = "iceberg-rest-catalog-service";
constexpr std::string_view kCatalogName = "test_catalog";
constexpr std::string_view kWarehouseName = "default";
constexpr std::string_view kLocalhostUri = "http://localhost";

/// \brief Check if a localhost port is ready to accept connections
/// \param port Port number to check
/// \return true if the port is accessible on localhost, false otherwise
bool CheckServiceReady(uint16_t port) {
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) {
    return false;
  }

  struct timeval timeout{
      .tv_sec = 1,
      .tv_usec = 0,
  };
  setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

  sockaddr_in addr{
      .sin_family = AF_INET,
      .sin_port = htons(port),
      .sin_addr = {.s_addr = htonl(INADDR_LOOPBACK)}  // 127.0.0.1
  };
  bool result =
      (connect(sock, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0);
  close(sock);
  return result;
}

}  // namespace

/// \brief Integration test fixture for REST catalog with automatic Docker Compose setup。
class RestCatalogIntegrationTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    std::string project_name{kDockerProjectName};
    std::filesystem::path resources_dir = GetResourcePath("iceberg-rest-fixture");

    // Create and start DockerCompose
    docker_compose_ = std::make_unique<DockerCompose>(project_name, resources_dir);
    docker_compose_->Up();

    // Wait for REST catalog to be ready on localhost
    std::println("[INFO] Waiting for REST catalog to be ready at localhost:{}...",
                 kRestCatalogPort);
    for (int i = 0; i < kMaxRetries; ++i) {
      if (CheckServiceReady(kRestCatalogPort)) {
        std::println("[INFO] REST catalog is ready!");
        return;
      }
      std::println(
          "[INFO] Waiting for 1s for REST catalog to be ready... (attempt {}/{})", i + 1,
          kMaxRetries);
      std::this_thread::sleep_for(std::chrono::milliseconds(kRetryDelayMs));
    }
    throw std::runtime_error("REST catalog failed to start within {} seconds");
  }

  static void TearDownTestSuite() { docker_compose_.reset(); }

  void SetUp() override {}

  void TearDown() override {}

  // Helper function to create a REST catalog instance
  Result<std::shared_ptr<RestCatalog>> CreateCatalog() {
    auto config = RestCatalogProperties::default_properties();
    config
        ->Set(RestCatalogProperties::kUri,
              std::format("{}:{}", kLocalhostUri, kRestCatalogPort))
        .Set(RestCatalogProperties::kName, std::string(kCatalogName))
        .Set(RestCatalogProperties::kWarehouse, std::string(kWarehouseName));
    auto file_io = std::make_shared<test::StdFileIO>();
    return RestCatalog::Make(*config, std::make_shared<test::StdFileIO>());
  }

  static inline std::unique_ptr<DockerCompose> docker_compose_;
};

TEST_F(RestCatalogIntegrationTest, MakeCatalogSuccess) {
  auto catalog_result = CreateCatalog();
  ASSERT_THAT(catalog_result, IsOk());

  auto& catalog = catalog_result.value();
  EXPECT_EQ(catalog->name(), kCatalogName);
}

/// Verifies that the server's /v1/config endpoint returns a valid response
/// and that the endpoints field (if present) can be parsed correctly.
TEST_F(RestCatalogIntegrationTest, FetchServerConfigDirect) {
  // Create HTTP client and fetch config directly
  HttpClient client({});
  std::string config_url =
      std::format("{}:{}/v1/config", kLocalhostUri, kRestCatalogPort);

  auto response_result = client.Get(config_url, {}, {}, *DefaultErrorHandler::Instance());
  ASSERT_THAT(response_result, IsOk());
  auto json_result = FromJsonString(response_result->body());
  ASSERT_THAT(json_result, IsOk());
  auto& json = json_result.value();

  EXPECT_TRUE(json.contains("defaults"));
  EXPECT_TRUE(json.contains("overrides"));

  if (json.contains("endpoints")) {
    EXPECT_TRUE(json["endpoints"].is_array());

    // Parse the config to ensure all endpoints are valid
    auto config_result = CatalogConfigFromJson(json);
    ASSERT_THAT(config_result, IsOk());
    auto& config = config_result.value();
    std::println("[INFO] Server provided {} endpoints", config.endpoints.size());
    EXPECT_GT(config.endpoints.size(), 0)
        << "Server should provide at least one endpoint";
  } else {
    std::println(
        "[INFO] Server did not provide endpoints field, client will use default "
        "endpoints");
  }
}

TEST_F(RestCatalogIntegrationTest, ListNamespaces) {
  auto catalog_result = CreateCatalog();
  ASSERT_THAT(catalog_result, IsOk());
  auto& catalog = catalog_result.value();

  Namespace root{.levels = {}};
  auto result = catalog->ListNamespaces(root);
  EXPECT_THAT(result, IsOk());
  EXPECT_TRUE(result->empty());
}

TEST_F(RestCatalogIntegrationTest, CreateNamespace) {
  auto catalog_result = CreateCatalog();
  ASSERT_THAT(catalog_result, IsOk());
  auto& catalog = catalog_result.value();

  // Create a simple namespace
  Namespace ns{.levels = {"test_ns"}};
  auto status = catalog->CreateNamespace(ns, {});
  EXPECT_THAT(status, IsOk());

  // Verify it was created by listing
  Namespace root{.levels = {}};
  auto list_result = catalog->ListNamespaces(root);
  ASSERT_THAT(list_result, IsOk());
  EXPECT_EQ(list_result->size(), 1);
  EXPECT_EQ(list_result->at(0).levels, std::vector<std::string>{"test_ns"});
}

TEST_F(RestCatalogIntegrationTest, CreateNamespaceWithProperties) {
  auto catalog_result = CreateCatalog();
  ASSERT_THAT(catalog_result, IsOk());
  auto& catalog = catalog_result.value();

  // Create namespace with properties
  Namespace ns{.levels = {"test_ns_props"}};
  std::unordered_map<std::string, std::string> properties{
      {"owner", "test_user"}, {"description", "Test namespace with properties"}};
  auto status = catalog->CreateNamespace(ns, properties);
  EXPECT_THAT(status, IsOk());

  // Verify properties were set
  auto props_result = catalog->GetNamespaceProperties(ns);
  ASSERT_THAT(props_result, IsOk());
  EXPECT_EQ(props_result->at("owner"), "test_user");
  EXPECT_EQ(props_result->at("description"), "Test namespace with properties");
}

TEST_F(RestCatalogIntegrationTest, CreateNestedNamespace) {
  auto catalog_result = CreateCatalog();
  ASSERT_THAT(catalog_result, IsOk());
  auto& catalog = catalog_result.value();

  // Create parent namespace
  Namespace parent{.levels = {"parent"}};
  auto status = catalog->CreateNamespace(parent, {});
  EXPECT_THAT(status, IsOk());

  // Create nested namespace
  Namespace child{.levels = {"parent", "child"}};
  status = catalog->CreateNamespace(child, {});
  EXPECT_THAT(status, IsOk());

  // Verify nested namespace exists
  auto list_result = catalog->ListNamespaces(parent);
  ASSERT_THAT(list_result, IsOk());
  EXPECT_EQ(list_result->size(), 1);
  EXPECT_EQ(list_result->at(0).levels, (std::vector<std::string>{"parent", "child"}));
}

TEST_F(RestCatalogIntegrationTest, GetNamespaceProperties) {
  auto catalog_result = CreateCatalog();
  ASSERT_THAT(catalog_result, IsOk());
  auto& catalog = catalog_result.value();

  // Create namespace with properties
  Namespace ns{.levels = {"test_get_props"}};
  std::unordered_map<std::string, std::string> properties{{"key1", "value1"},
                                                          {"key2", "value2"}};
  auto status = catalog->CreateNamespace(ns, properties);
  EXPECT_THAT(status, IsOk());

  // Get properties
  auto props_result = catalog->GetNamespaceProperties(ns);
  ASSERT_THAT(props_result, IsOk());
  EXPECT_EQ(props_result->at("key1"), "value1");
  EXPECT_EQ(props_result->at("key2"), "value2");
}

TEST_F(RestCatalogIntegrationTest, NamespaceExists) {
  auto catalog_result = CreateCatalog();
  ASSERT_THAT(catalog_result, IsOk());
  auto& catalog = catalog_result.value();

  // Check non-existent namespace
  Namespace ns{.levels = {"non_existent"}};
  auto exists_result = catalog->NamespaceExists(ns);
  ASSERT_THAT(exists_result, IsOk());
  EXPECT_FALSE(*exists_result);

  // Create namespace
  auto status = catalog->CreateNamespace(ns, {});
  EXPECT_THAT(status, IsOk());

  // Check it now exists
  exists_result = catalog->NamespaceExists(ns);
  ASSERT_THAT(exists_result, IsOk());
  EXPECT_TRUE(*exists_result);
}

TEST_F(RestCatalogIntegrationTest, UpdateNamespaceProperties) {
  auto catalog_result = CreateCatalog();
  ASSERT_THAT(catalog_result, IsOk());
  auto& catalog = catalog_result.value();

  // Create namespace with initial properties
  Namespace ns{.levels = {"test_update"}};
  std::unordered_map<std::string, std::string> initial_props{{"key1", "value1"},
                                                             {"key2", "value2"}};
  auto status = catalog->CreateNamespace(ns, initial_props);
  EXPECT_THAT(status, IsOk());

  // Update properties: modify key1, add key3, remove key2
  std::unordered_map<std::string, std::string> updates{{"key1", "updated_value1"},
                                                       {"key3", "value3"}};
  std::unordered_set<std::string> removals{"key2"};
  status = catalog->UpdateNamespaceProperties(ns, updates, removals);
  EXPECT_THAT(status, IsOk());

  // Verify updated properties
  auto props_result = catalog->GetNamespaceProperties(ns);
  ASSERT_THAT(props_result, IsOk());
  EXPECT_EQ(props_result->at("key1"), "updated_value1");
  EXPECT_EQ(props_result->at("key3"), "value3");
  EXPECT_EQ(props_result->count("key2"), 0);  // Should be removed
}

TEST_F(RestCatalogIntegrationTest, DropNamespace) {
  auto catalog_result = CreateCatalog();
  ASSERT_THAT(catalog_result, IsOk());
  auto& catalog = catalog_result.value();

  // Create namespace
  Namespace ns{.levels = {"test_drop"}};
  auto status = catalog->CreateNamespace(ns, {});
  EXPECT_THAT(status, IsOk());

  // Verify it exists
  auto exists_result = catalog->NamespaceExists(ns);
  ASSERT_THAT(exists_result, IsOk());
  EXPECT_TRUE(*exists_result);

  // Drop namespace
  status = catalog->DropNamespace(ns);
  EXPECT_THAT(status, IsOk());

  // Verify it no longer exists
  exists_result = catalog->NamespaceExists(ns);
  ASSERT_THAT(exists_result, IsOk());
  EXPECT_FALSE(*exists_result);
}

TEST_F(RestCatalogIntegrationTest, CreateTable) {
  auto catalog_result = CreateCatalog();
  ASSERT_THAT(catalog_result, IsOk());
  auto& catalog = catalog_result.value();

  // Create nested namespace with properties
  Namespace ns{.levels = {"test_create_table", "apple", "ios"}};
  std::unordered_map<std::string, std::string> ns_properties{{"owner", "ray"},
                                                             {"community", "apache"}};

  // Create parent namespaces first
  auto status = catalog->CreateNamespace(Namespace{.levels = {"test_create_table"}}, {});
  EXPECT_THAT(status, IsOk());
  status =
      catalog->CreateNamespace(Namespace{.levels = {"test_create_table", "apple"}}, {});
  EXPECT_THAT(status, IsOk());
  status = catalog->CreateNamespace(ns, ns_properties);
  EXPECT_THAT(status, IsOk());

  // Create schema
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(1, "foo", string()),
                               SchemaField::MakeRequired(2, "bar", int32()),
                               SchemaField::MakeOptional(3, "baz", boolean())},
      /*schema_id=*/1);

  // Create partition spec and sort order (unpartitioned and unsorted)
  auto partition_spec_result = PartitionSpec::Make(PartitionSpec::kInitialSpecId, {}, 0);
  ASSERT_THAT(partition_spec_result, IsOk());
  auto partition_spec = std::shared_ptr<PartitionSpec>(std::move(*partition_spec_result));

  auto sort_order_result =
      SortOrder::Make(SortOrder::kUnsortedOrderId, std::vector<SortField>{});
  ASSERT_THAT(sort_order_result, IsOk());
  auto sort_order = std::shared_ptr<SortOrder>(std::move(*sort_order_result));

  // Create table
  TableIdentifier table_id{.ns = ns, .name = "t1"};
  std::unordered_map<std::string, std::string> table_properties;
  auto table_result = catalog->CreateTable(table_id, schema, partition_spec, sort_order,
                                           "", table_properties);
  ASSERT_THAT(table_result, IsOk());
  auto& table = table_result.value();

  // Verify table
  EXPECT_EQ(table->name().ns.levels,
            (std::vector<std::string>{"test_create_table", "apple", "ios"}));
  EXPECT_EQ(table->name().name, "t1");

  // Verify that creating the same table again fails
  auto duplicate_result = catalog->CreateTable(table_id, schema, partition_spec,
                                               sort_order, "", table_properties);
  EXPECT_THAT(duplicate_result, IsError(ErrorKind::kAlreadyExists));
  EXPECT_THAT(duplicate_result,
              HasErrorMessage("Table already exists: test_create_table.apple.ios.t1"));
}

}  // namespace iceberg::rest
