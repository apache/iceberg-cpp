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

#include <algorithm>
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
#include "iceberg/table_requirement.h"
#include "iceberg/table_update.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/std_io.h"
#include "iceberg/test/test_resource.h"
#include "iceberg/test/util/docker_compose_util.h"
#include "iceberg/transaction.h"

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

  // Helper function to create a default schema for testing
  std::shared_ptr<Schema> CreateDefaultSchema() {
    return std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                                 SchemaField::MakeOptional(2, "data", string())},
        /*schema_id=*/1);
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
  EXPECT_TRUE(exists_result.value());
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
  EXPECT_FALSE(exists_result.value());
}

TEST_F(RestCatalogIntegrationTest, DropNonExistentNamespace) {
  auto catalog_result = CreateCatalog();
  ASSERT_THAT(catalog_result, IsOk());
  auto& catalog = catalog_result.value();

  Namespace ns{.levels = {"nonexistent_namespace"}};
  auto status = catalog->DropNamespace(ns);

  // Should return NoSuchNamespace error
  EXPECT_THAT(status, IsError(ErrorKind::kNoSuchNamespace));
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

  auto schema = CreateDefaultSchema();
  auto partition_spec = PartitionSpec::Unpartitioned();
  auto sort_order = SortOrder::Unsorted();

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

TEST_F(RestCatalogIntegrationTest, ListTables) {
  auto catalog_result = CreateCatalog();
  ASSERT_THAT(catalog_result, IsOk());
  auto& catalog = catalog_result.value();

  // Create namespace
  Namespace ns{.levels = {"test_list_tables"}};
  auto status = catalog->CreateNamespace(ns, {});
  EXPECT_THAT(status, IsOk());

  // Initially no tables
  auto list_result = catalog->ListTables(ns);
  ASSERT_THAT(list_result, IsOk());
  EXPECT_TRUE(list_result.value().empty());

  // Create tables
  auto schema = CreateDefaultSchema();
  auto partition_spec = PartitionSpec::Unpartitioned();
  auto sort_order = SortOrder::Unsorted();
  std::unordered_map<std::string, std::string> table_properties;

  TableIdentifier table1{.ns = ns, .name = "table1"};
  auto create_result = catalog->CreateTable(table1, schema, partition_spec, sort_order,
                                            "", table_properties);
  ASSERT_THAT(create_result, IsOk());

  TableIdentifier table2{.ns = ns, .name = "table2"};
  create_result = catalog->CreateTable(table2, schema, partition_spec, sort_order, "",
                                       table_properties);
  ASSERT_THAT(create_result, IsOk());

  // List and varify tables
  list_result = catalog->ListTables(ns);
  ASSERT_THAT(list_result, IsOk());
  EXPECT_THAT(list_result.value(), testing::UnorderedElementsAre(
                                       testing::Field(&TableIdentifier::name, "table1"),
                                       testing::Field(&TableIdentifier::name, "table2")));
}

TEST_F(RestCatalogIntegrationTest, LoadTable) {
  auto catalog_result = CreateCatalog();
  ASSERT_THAT(catalog_result, IsOk());
  auto& catalog = catalog_result.value();

  // Create namespace and table first
  Namespace ns{.levels = {"test_load_table"}};
  auto status = catalog->CreateNamespace(ns, {});
  EXPECT_THAT(status, IsOk());

  // Create schema, partition spec, and sort order using helper functions
  auto schema = CreateDefaultSchema();
  auto partition_spec = PartitionSpec::Unpartitioned();
  auto sort_order = SortOrder::Unsorted();

  // Create table
  TableIdentifier table_id{.ns = ns, .name = "test_table"};
  std::unordered_map<std::string, std::string> table_properties{{"key1", "value1"}};
  auto create_result = catalog->CreateTable(table_id, schema, partition_spec, sort_order,
                                            "", table_properties);
  ASSERT_THAT(create_result, IsOk());

  // Load the table
  auto load_result = catalog->LoadTable(table_id);
  ASSERT_THAT(load_result, IsOk());
  auto& loaded_table = load_result.value();

  // Verify loaded table properties
  EXPECT_EQ(loaded_table->name().ns.levels, std::vector<std::string>{"test_load_table"});
  EXPECT_EQ(loaded_table->name().name, "test_table");
  EXPECT_NE(loaded_table->metadata(), nullptr);

  // Verify schema
  auto loaded_schema_result = loaded_table->schema();
  ASSERT_THAT(loaded_schema_result, IsOk());
  auto loaded_schema = loaded_schema_result.value();
  EXPECT_EQ(loaded_schema->fields().size(), 2);
  EXPECT_EQ(loaded_schema->fields()[0].name(), "id");
  EXPECT_EQ(loaded_schema->fields()[1].name(), "data");
}

TEST_F(RestCatalogIntegrationTest, DropTable) {
  auto catalog_result = CreateCatalog();
  ASSERT_THAT(catalog_result, IsOk());
  auto& catalog = catalog_result.value();

  // Create namespace and table first
  Namespace ns{.levels = {"test_drop_table"}};
  auto status = catalog->CreateNamespace(ns, {});
  EXPECT_THAT(status, IsOk());

  // Create table
  auto schema = CreateDefaultSchema();
  auto partition_spec = PartitionSpec::Unpartitioned();
  auto sort_order = SortOrder::Unsorted();

  TableIdentifier table_id{.ns = ns, .name = "table_to_drop"};
  std::unordered_map<std::string, std::string> table_properties;
  auto create_result = catalog->CreateTable(table_id, schema, partition_spec, sort_order,
                                            "", table_properties);
  ASSERT_THAT(create_result, IsOk());

  // Verify table exists
  auto load_result = catalog->TableExists(table_id);
  ASSERT_THAT(load_result, IsOk());
  EXPECT_TRUE(load_result.value());

  // Drop the table
  status = catalog->DropTable(table_id, /*purge=*/false);
  ASSERT_THAT(status, IsOk());

  // Verify table no longer exists
  load_result = catalog->TableExists(table_id);
  ASSERT_THAT(load_result, IsOk());
  EXPECT_FALSE(load_result.value());
}

TEST_F(RestCatalogIntegrationTest, RenameTable) {
  auto catalog_result = CreateCatalog();
  ASSERT_THAT(catalog_result, IsOk());
  auto& catalog = catalog_result.value();

  // Create namespace
  Namespace ns{.levels = {"test_rename_table"}};
  auto status = catalog->CreateNamespace(ns, {});
  EXPECT_THAT(status, IsOk());

  // Create table
  auto schema = CreateDefaultSchema();
  auto partition_spec = PartitionSpec::Unpartitioned();
  auto sort_order = SortOrder::Unsorted();

  TableIdentifier old_table_id{.ns = ns, .name = "old_table"};
  std::unordered_map<std::string, std::string> table_properties;
  auto table_result = catalog->CreateTable(old_table_id, schema, partition_spec,
                                           sort_order, "", table_properties);
  ASSERT_THAT(table_result, IsOk());

  // Rename table
  TableIdentifier new_table_id{.ns = ns, .name = "new_table"};
  status = catalog->RenameTable(old_table_id, new_table_id);
  ASSERT_THAT(status, IsOk());

  // Verify old table no longer exists
  auto exists_result = catalog->TableExists(old_table_id);
  ASSERT_THAT(exists_result, IsOk());
  EXPECT_FALSE(exists_result.value());

  // Verify new table exists
  exists_result = catalog->TableExists(new_table_id);
  ASSERT_THAT(exists_result, IsOk());
  EXPECT_TRUE(exists_result.value());
}

TEST_F(RestCatalogIntegrationTest, UpdateTable) {
  auto catalog_result = CreateCatalog();
  ASSERT_THAT(catalog_result, IsOk());
  auto& catalog = catalog_result.value();

  // Create namespace
  Namespace ns{.levels = {"test_update_table"}};
  auto status = catalog->CreateNamespace(ns, {});
  EXPECT_THAT(status, IsOk());

  // Create table
  auto schema = CreateDefaultSchema();
  auto partition_spec = PartitionSpec::Unpartitioned();
  auto sort_order = SortOrder::Unsorted();

  TableIdentifier table_id{.ns = ns, .name = "t1"};
  std::unordered_map<std::string, std::string> table_properties;
  auto table_result = catalog->CreateTable(table_id, schema, partition_spec, sort_order,
                                           "", table_properties);
  ASSERT_THAT(table_result, IsOk());
  auto& table = table_result.value();

  // Update table properties
  std::vector<std::unique_ptr<TableRequirement>> requirements;
  requirements.push_back(std::make_unique<table::AssertUUID>(table->uuid()));

  std::vector<std::unique_ptr<TableUpdate>> updates;
  updates.push_back(std::make_unique<table::SetProperties>(
      std::unordered_map<std::string, std::string>{{"key1", "value1"}}));

  auto update_result = catalog->UpdateTable(table_id, requirements, updates);
  ASSERT_THAT(update_result, IsOk());
  auto& updated_table = update_result.value();

  // Verify the property was set
  auto& props = updated_table->metadata()->properties.configs();
  EXPECT_EQ(props.at("key1"), "value1");
}

TEST_F(RestCatalogIntegrationTest, RegisterTable) {
  auto catalog_result = CreateCatalog();
  ASSERT_THAT(catalog_result, IsOk());
  auto& catalog = catalog_result.value();

  // Create namespace
  Namespace ns{.levels = {"test_register_table"}};
  auto status = catalog->CreateNamespace(ns, {});
  EXPECT_THAT(status, IsOk());

  // Create table
  auto schema = CreateDefaultSchema();
  auto partition_spec = PartitionSpec::Unpartitioned();
  auto sort_order = SortOrder::Unsorted();

  TableIdentifier table_id{.ns = ns, .name = "t1"};
  std::unordered_map<std::string, std::string> table_properties;
  auto table_result = catalog->CreateTable(table_id, schema, partition_spec, sort_order,
                                           "", table_properties);
  ASSERT_THAT(table_result, IsOk());
  auto& table = table_result.value();
  std::string metadata_location(table->metadata_file_location());

  // Drop table (without purge, to keep metadata file)
  status = catalog->DropTable(table_id, /*purge=*/false);
  ASSERT_THAT(status, IsOk());

  // Register table with new name
  TableIdentifier new_table_id{.ns = ns, .name = "t2"};
  auto register_result = catalog->RegisterTable(new_table_id, metadata_location);
  ASSERT_THAT(register_result, IsOk());
  auto& registered_table = register_result.value();

  EXPECT_EQ(table->metadata_file_location(), registered_table->metadata_file_location());
  EXPECT_NE(table->name(), registered_table->name());
}

TEST_F(RestCatalogIntegrationTest, StageCreateTable) {
  auto catalog_result = CreateCatalog();
  ASSERT_THAT(catalog_result, IsOk());
  auto& catalog = catalog_result.value();

  // Create namespace
  Namespace ns{.levels = {"test_stage_create"}};
  auto status = catalog->CreateNamespace(ns, {});
  EXPECT_THAT(status, IsOk());

  // Stage create table
  auto schema = CreateDefaultSchema();
  auto partition_spec = PartitionSpec::Unpartitioned();
  auto sort_order = SortOrder::Unsorted();

  TableIdentifier table_id{.ns = ns, .name = "staged_table"};
  std::unordered_map<std::string, std::string> table_properties{{"key1", "value1"}};
  auto txn_result = catalog->StageCreateTable(table_id, schema, partition_spec,
                                              sort_order, "", table_properties);
  ASSERT_THAT(txn_result, IsOk());
  auto& txn = txn_result.value();

  // Verify the staged table in transaction
  EXPECT_NE(txn->table(), nullptr);
  EXPECT_EQ(txn->table()->name(), table_id);

  // Table should NOT exist in catalog yet (staged but not committed)
  auto exists_result = catalog->TableExists(table_id);
  ASSERT_THAT(exists_result, IsOk());
  EXPECT_FALSE(exists_result.value());

  // Commit the transaction
  auto commit_result = txn->Commit();
  ASSERT_THAT(commit_result, IsOk());
  auto& committed_table = commit_result.value();

  // Verify table now exists
  exists_result = catalog->TableExists(table_id);
  ASSERT_THAT(exists_result, IsOk());
  EXPECT_TRUE(exists_result.value());

  // Verify table properties
  EXPECT_EQ(committed_table->name(), table_id);
  auto& props = committed_table->metadata()->properties.configs();
  EXPECT_EQ(props.at("key1"), "value1");
}

}  // namespace iceberg::rest
