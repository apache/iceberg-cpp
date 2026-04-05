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

#include "iceberg/catalog/rest/auth/auth_session.h"
#include "iceberg/catalog/rest/catalog_properties.h"
#include "iceberg/catalog/rest/error_handlers.h"
#include "iceberg/catalog/rest/http_client.h"
#include "iceberg/catalog/rest/json_serde_internal.h"
#include "iceberg/catalog/rest/rest_catalog.h"
#include "iceberg/partition_spec.h"
#include "iceberg/table_scan.h"
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
constexpr int kMaxRetries = 60;
constexpr int kRetryDelayMs = 1000;

constexpr std::string_view kDockerProjectName = "iceberg-rest-catalog-service";
constexpr std::string_view kCatalogName = "test_catalog";
constexpr std::string_view kWarehouseName = "default";
constexpr std::string_view kLocalhostUri = "http://localhost";

/// \brief Check if a localhost port is ready to accept connections.
bool CheckServiceReady(uint16_t port) {
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) return false;

  struct timeval timeout{
      .tv_sec = 1,
      .tv_usec = 0,
  };
  setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

  sockaddr_in addr{
      .sin_family = AF_INET,
      .sin_port = htons(port),
      .sin_addr = {.s_addr = htonl(INADDR_LOOPBACK)},
  };
  bool result =
      (connect(sock, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0);
  close(sock);
  return result;
}

std::string CatalogUri() { return std::format("{}:{}", kLocalhostUri, kRestCatalogPort); }

}  // namespace

/// \brief Integration test fixture for REST catalog with Docker Compose.
class RestCatalogIntegrationTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    docker_compose_ = std::make_unique<DockerCompose>(
        std::string{kDockerProjectName}, GetResourcePath("iceberg-rest-fixture"));
    docker_compose_->Up();

    std::println("[INFO] Waiting for REST catalog at localhost:{}...", kRestCatalogPort);
    for (int i = 0; i < kMaxRetries; ++i) {
      if (CheckServiceReady(kRestCatalogPort)) {
        std::println("[INFO] REST catalog is ready!");
        return;
      }
      std::println("[INFO] Retrying... (attempt {}/{})", i + 1, kMaxRetries);
      std::this_thread::sleep_for(std::chrono::milliseconds(kRetryDelayMs));
    }
    throw std::runtime_error("REST catalog failed to start within timeout");
  }

  static void TearDownTestSuite() { docker_compose_.reset(); }

  /// Create a catalog with default configuration.
  Result<std::shared_ptr<RestCatalog>> CreateCatalog() {
    return CreateCatalogWithProperties({});
  }

  /// Create a catalog with additional properties merged on top of defaults.
  Result<std::shared_ptr<RestCatalog>> CreateCatalogWithProperties(
      const std::unordered_map<std::string, std::string>& extra) {
    auto config = RestCatalogProperties::default_properties();
    config.Set(RestCatalogProperties::kUri, CatalogUri())
        .Set(RestCatalogProperties::kName, std::string(kCatalogName))
        .Set(RestCatalogProperties::kWarehouse, std::string(kWarehouseName));
    for (const auto& [k, v] : extra) {
      config.mutable_configs()[k] = v;
    }
    return RestCatalog::Make(config, std::make_shared<test::StdFileIO>());
  }

  /// Create a catalog configured with a specific snapshot loading mode.
  Result<std::shared_ptr<RestCatalog>> CreateCatalogWithSnapshotMode(
      const std::string& mode) {
    return CreateCatalogWithProperties(
        {{RestCatalogProperties::kSnapshotLoadingMode.key(), mode}});
  }

  /// Convenience: create a namespace and return the catalog.
  Result<std::shared_ptr<RestCatalog>> CreateCatalogAndNamespace(const Namespace& ns) {
    ICEBERG_ASSIGN_OR_RAISE(auto catalog, CreateCatalog());
    auto status = catalog->CreateNamespace(ns, {});
    if (!status.has_value()) {
      return std::unexpected(status.error());
    }
    return catalog;
  }

  /// Default two-column schema used across tests.
  static std::shared_ptr<Schema> DefaultSchema() {
    return std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                                 SchemaField::MakeOptional(2, "data", string())},
        /*schema_id=*/1);
  }

  /// Create a table with default schema and no partitioning.
  Result<std::shared_ptr<Table>> CreateDefaultTable(
      const std::shared_ptr<RestCatalog>& catalog, const TableIdentifier& table_id,
      const std::unordered_map<std::string, std::string>& props = {}) {
    return catalog->CreateTable(table_id, DefaultSchema(), PartitionSpec::Unpartitioned(),
                                SortOrder::Unsorted(), "", props);
  }

  static inline std::unique_ptr<DockerCompose> docker_compose_;
};

TEST_F(RestCatalogIntegrationTest, MakeCatalogSuccess) {
  ICEBERG_UNWRAP_OR_FAIL(auto catalog, CreateCatalog());
  EXPECT_EQ(catalog->name(), kCatalogName);
}

TEST_F(RestCatalogIntegrationTest, FetchServerConfigDirect) {
  HttpClient client({});
  auto noop_session = auth::AuthSession::MakeDefault({});
  std::string config_url = std::format("{}/v1/config", CatalogUri());

  ICEBERG_UNWRAP_OR_FAIL(const auto response,
                         client.Get(config_url, {}, /*headers=*/{},
                                    *DefaultErrorHandler::Instance(), *noop_session));
  ICEBERG_UNWRAP_OR_FAIL(auto json, FromJsonString(response.body()));

  EXPECT_TRUE(json.contains("defaults"));
  EXPECT_TRUE(json.contains("overrides"));

  if (json.contains("endpoints")) {
    EXPECT_TRUE(json["endpoints"].is_array());
    ICEBERG_UNWRAP_OR_FAIL(auto config, CatalogConfigFromJson(json));
    std::println("[INFO] Server provided {} endpoints", config.endpoints.size());
    EXPECT_GT(config.endpoints.size(), 0);
  }
}

// -- Namespace operations --

TEST_F(RestCatalogIntegrationTest, ListNamespaces) {
  ICEBERG_UNWRAP_OR_FAIL(auto catalog, CreateCatalog());
  ICEBERG_UNWRAP_OR_FAIL(auto result, catalog->ListNamespaces(Namespace{.levels = {}}));
  EXPECT_TRUE(result.empty());
}

TEST_F(RestCatalogIntegrationTest, CreateNamespace) {
  ICEBERG_UNWRAP_OR_FAIL(auto catalog, CreateCatalog());

  Namespace ns{.levels = {"test_ns"}};
  ASSERT_THAT(catalog->CreateNamespace(ns, {}), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto namespaces,
                         catalog->ListNamespaces(Namespace{.levels = {}}));
  EXPECT_EQ(namespaces.size(), 1);
  EXPECT_EQ(namespaces[0].levels, std::vector<std::string>{"test_ns"});
}

TEST_F(RestCatalogIntegrationTest, CreateNamespaceWithProperties) {
  ICEBERG_UNWRAP_OR_FAIL(auto catalog, CreateCatalog());

  Namespace ns{.levels = {"test_ns_props"}};
  std::unordered_map<std::string, std::string> properties{
      {"owner", "test_user"}, {"description", "Test namespace with properties"}};
  ASSERT_THAT(catalog->CreateNamespace(ns, properties), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto props, catalog->GetNamespaceProperties(ns));
  EXPECT_EQ(props.at("owner"), "test_user");
  EXPECT_EQ(props.at("description"), "Test namespace with properties");
}

TEST_F(RestCatalogIntegrationTest, CreateNestedNamespace) {
  ICEBERG_UNWRAP_OR_FAIL(auto catalog, CreateCatalog());

  Namespace parent{.levels = {"parent"}};
  Namespace child{.levels = {"parent", "child"}};
  ASSERT_THAT(catalog->CreateNamespace(parent, {}), IsOk());
  ASSERT_THAT(catalog->CreateNamespace(child, {}), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto children, catalog->ListNamespaces(parent));
  EXPECT_EQ(children.size(), 1);
  EXPECT_EQ(children[0].levels, (std::vector<std::string>{"parent", "child"}));
}

TEST_F(RestCatalogIntegrationTest, GetNamespaceProperties) {
  ICEBERG_UNWRAP_OR_FAIL(auto catalog, CreateCatalog());

  Namespace ns{.levels = {"test_get_props"}};
  ASSERT_THAT(catalog->CreateNamespace(ns, {{"key1", "value1"}, {"key2", "value2"}}),
              IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto props, catalog->GetNamespaceProperties(ns));
  EXPECT_EQ(props.at("key1"), "value1");
  EXPECT_EQ(props.at("key2"), "value2");
}

TEST_F(RestCatalogIntegrationTest, NamespaceExists) {
  ICEBERG_UNWRAP_OR_FAIL(auto catalog, CreateCatalog());

  Namespace ns{.levels = {"non_existent"}};
  ICEBERG_UNWRAP_OR_FAIL(auto before, catalog->NamespaceExists(ns));
  EXPECT_FALSE(before);

  ASSERT_THAT(catalog->CreateNamespace(ns, {}), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto after, catalog->NamespaceExists(ns));
  EXPECT_TRUE(after);
}

TEST_F(RestCatalogIntegrationTest, UpdateNamespaceProperties) {
  ICEBERG_UNWRAP_OR_FAIL(auto catalog, CreateCatalog());

  Namespace ns{.levels = {"test_update"}};
  ASSERT_THAT(catalog->CreateNamespace(ns, {{"key1", "value1"}, {"key2", "value2"}}),
              IsOk());

  ASSERT_THAT(catalog->UpdateNamespaceProperties(
                  ns, {{"key1", "updated_value1"}, {"key3", "value3"}}, {"key2"}),
              IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto props, catalog->GetNamespaceProperties(ns));
  EXPECT_EQ(props.at("key1"), "updated_value1");
  EXPECT_EQ(props.at("key3"), "value3");
  EXPECT_EQ(props.count("key2"), 0);
}

TEST_F(RestCatalogIntegrationTest, DropNamespace) {
  ICEBERG_UNWRAP_OR_FAIL(auto catalog, CreateCatalog());

  Namespace ns{.levels = {"test_drop"}};
  ASSERT_THAT(catalog->CreateNamespace(ns, {}), IsOk());
  ASSERT_THAT(catalog->DropNamespace(ns), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto exists, catalog->NamespaceExists(ns));
  EXPECT_FALSE(exists);
}

TEST_F(RestCatalogIntegrationTest, DropNonExistentNamespace) {
  ICEBERG_UNWRAP_OR_FAIL(auto catalog, CreateCatalog());
  EXPECT_THAT(catalog->DropNamespace(Namespace{.levels = {"nonexistent"}}),
              IsError(ErrorKind::kNoSuchNamespace));
}

// -- Table operations --

TEST_F(RestCatalogIntegrationTest, CreateTable) {
  ICEBERG_UNWRAP_OR_FAIL(auto catalog, CreateCatalog());

  // Build nested namespace hierarchy
  Namespace ns{.levels = {"test_create_table", "apple", "ios"}};
  ASSERT_THAT(catalog->CreateNamespace(Namespace{.levels = {"test_create_table"}}, {}),
              IsOk());
  ASSERT_THAT(
      catalog->CreateNamespace(Namespace{.levels = {"test_create_table", "apple"}}, {}),
      IsOk());
  ASSERT_THAT(catalog->CreateNamespace(ns, {{"owner", "ray"}, {"community", "apache"}}),
              IsOk());

  TableIdentifier table_id{.ns = ns, .name = "t1"};
  ICEBERG_UNWRAP_OR_FAIL(auto table, CreateDefaultTable(catalog, table_id));

  EXPECT_EQ(table->name().ns.levels,
            (std::vector<std::string>{"test_create_table", "apple", "ios"}));
  EXPECT_EQ(table->name().name, "t1");

  // Duplicate creation should fail
  EXPECT_THAT(CreateDefaultTable(catalog, table_id), IsError(ErrorKind::kAlreadyExists));
}

TEST_F(RestCatalogIntegrationTest, ListTables) {
  Namespace ns{.levels = {"test_list_tables"}};
  ICEBERG_UNWRAP_OR_FAIL(auto catalog, CreateCatalogAndNamespace(ns));

  ICEBERG_UNWRAP_OR_FAIL(auto empty_list, catalog->ListTables(ns));
  EXPECT_TRUE(empty_list.empty());

  TableIdentifier t1{.ns = ns, .name = "table1"};
  TableIdentifier t2{.ns = ns, .name = "table2"};
  ASSERT_THAT(CreateDefaultTable(catalog, t1), IsOk());
  ASSERT_THAT(CreateDefaultTable(catalog, t2), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto tables, catalog->ListTables(ns));
  EXPECT_THAT(tables, testing::UnorderedElementsAre(
                          testing::Field(&TableIdentifier::name, "table1"),
                          testing::Field(&TableIdentifier::name, "table2")));
}

TEST_F(RestCatalogIntegrationTest, LoadTable) {
  Namespace ns{.levels = {"test_load_table"}};
  ICEBERG_UNWRAP_OR_FAIL(auto catalog, CreateCatalogAndNamespace(ns));

  TableIdentifier table_id{.ns = ns, .name = "test_table"};
  ASSERT_THAT(CreateDefaultTable(catalog, table_id, {{"key1", "value1"}}), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto loaded, catalog->LoadTable(table_id));
  EXPECT_EQ(loaded->name().ns.levels, std::vector<std::string>{"test_load_table"});
  EXPECT_EQ(loaded->name().name, "test_table");
  EXPECT_NE(loaded->metadata(), nullptr);

  ICEBERG_UNWRAP_OR_FAIL(auto schema, loaded->schema());
  ASSERT_EQ(schema->fields().size(), 2);
  EXPECT_EQ(schema->fields()[0].name(), "id");
  EXPECT_EQ(schema->fields()[1].name(), "data");
}

TEST_F(RestCatalogIntegrationTest, DropTable) {
  Namespace ns{.levels = {"test_drop_table"}};
  ICEBERG_UNWRAP_OR_FAIL(auto catalog, CreateCatalogAndNamespace(ns));

  TableIdentifier table_id{.ns = ns, .name = "table_to_drop"};
  ASSERT_THAT(CreateDefaultTable(catalog, table_id), IsOk());

  ASSERT_THAT(catalog->DropTable(table_id, /*purge=*/false), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto exists, catalog->TableExists(table_id));
  EXPECT_FALSE(exists);
}

TEST_F(RestCatalogIntegrationTest, RenameTable) {
  Namespace ns{.levels = {"test_rename_table"}};
  ICEBERG_UNWRAP_OR_FAIL(auto catalog, CreateCatalogAndNamespace(ns));

  TableIdentifier old_id{.ns = ns, .name = "old_table"};
  TableIdentifier new_id{.ns = ns, .name = "new_table"};
  ASSERT_THAT(CreateDefaultTable(catalog, old_id), IsOk());
  ASSERT_THAT(catalog->RenameTable(old_id, new_id), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto old_exists, catalog->TableExists(old_id));
  EXPECT_FALSE(old_exists);
  ICEBERG_UNWRAP_OR_FAIL(auto new_exists, catalog->TableExists(new_id));
  EXPECT_TRUE(new_exists);
}

TEST_F(RestCatalogIntegrationTest, UpdateTable) {
  Namespace ns{.levels = {"test_update_table"}};
  ICEBERG_UNWRAP_OR_FAIL(auto catalog, CreateCatalogAndNamespace(ns));

  TableIdentifier table_id{.ns = ns, .name = "t1"};
  ICEBERG_UNWRAP_OR_FAIL(auto table, CreateDefaultTable(catalog, table_id));

  std::vector<std::unique_ptr<TableRequirement>> requirements;
  requirements.push_back(std::make_unique<table::AssertUUID>(table->uuid()));

  std::vector<std::unique_ptr<TableUpdate>> updates;
  updates.push_back(std::make_unique<table::SetProperties>(
      std::unordered_map<std::string, std::string>{{"key1", "value1"}}));

  ICEBERG_UNWRAP_OR_FAIL(auto updated,
                         catalog->UpdateTable(table_id, requirements, updates));
  EXPECT_EQ(updated->metadata()->properties.configs().at("key1"), "value1");
}

TEST_F(RestCatalogIntegrationTest, RegisterTable) {
  Namespace ns{.levels = {"test_register_table"}};
  ICEBERG_UNWRAP_OR_FAIL(auto catalog, CreateCatalogAndNamespace(ns));

  TableIdentifier table_id{.ns = ns, .name = "t1"};
  ICEBERG_UNWRAP_OR_FAIL(auto table, CreateDefaultTable(catalog, table_id));
  std::string metadata_location(table->metadata_file_location());

  ASSERT_THAT(catalog->DropTable(table_id, /*purge=*/false), IsOk());

  TableIdentifier new_id{.ns = ns, .name = "t2"};
  ICEBERG_UNWRAP_OR_FAIL(auto registered,
                         catalog->RegisterTable(new_id, metadata_location));
  EXPECT_EQ(table->metadata_file_location(), registered->metadata_file_location());
  EXPECT_NE(table->name(), registered->name());
}

TEST_F(RestCatalogIntegrationTest, StageCreateTable) {
  Namespace ns{.levels = {"test_stage_create"}};
  ICEBERG_UNWRAP_OR_FAIL(auto catalog, CreateCatalogAndNamespace(ns));

  TableIdentifier table_id{.ns = ns, .name = "staged_table"};
  ICEBERG_UNWRAP_OR_FAIL(
      auto txn,
      catalog->StageCreateTable(table_id, DefaultSchema(), PartitionSpec::Unpartitioned(),
                                SortOrder::Unsorted(), "", {{"key1", "value1"}}));

  EXPECT_EQ(txn->table()->name(), table_id);

  // Not yet visible in catalog
  ICEBERG_UNWRAP_OR_FAIL(auto before, catalog->TableExists(table_id));
  EXPECT_FALSE(before);

  ICEBERG_UNWRAP_OR_FAIL(auto committed, txn->Commit());

  ICEBERG_UNWRAP_OR_FAIL(auto after, catalog->TableExists(table_id));
  EXPECT_TRUE(after);
  EXPECT_EQ(committed->metadata()->properties.configs().at("key1"), "value1");
}

// -- Scan plan operations --

TEST_F(RestCatalogIntegrationTest, PlanTableScan) {
  Namespace ns{.levels = {"test_plan_table_scan"}};
  ICEBERG_UNWRAP_OR_FAIL(auto catalog, CreateCatalogAndNamespace(ns));
  TableIdentifier table_id{.ns = ns, .name = "scan_table"};
  ASSERT_THAT(CreateDefaultTable(catalog, table_id), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto table, catalog->LoadTable(table_id));

  internal::TableScanContext context;
  ICEBERG_UNWRAP_OR_FAIL(auto response, catalog->PlanTableScan(*table, context));
  // Empty table: no file scan tasks returned
  EXPECT_TRUE(response.file_scan_tasks.empty());
}

TEST_F(RestCatalogIntegrationTest, PlanTableScanWithContext) {
  Namespace ns{.levels = {"test_plan_scan_context"}};
  ICEBERG_UNWRAP_OR_FAIL(auto catalog, CreateCatalogAndNamespace(ns));
  TableIdentifier table_id{.ns = ns, .name = "context_scan_table"};
  ASSERT_THAT(CreateDefaultTable(catalog, table_id), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto table, catalog->LoadTable(table_id));

  internal::TableScanContext context;
  context.selected_columns = {"id", "data"};
  context.case_sensitive = true;
  ICEBERG_UNWRAP_OR_FAIL(auto response, catalog->PlanTableScan(*table, context));
  EXPECT_TRUE(response.file_scan_tasks.empty());
}

TEST_F(RestCatalogIntegrationTest, FetchPlanningResult) {
  Namespace ns{.levels = {"test_fetch_planning_result"}};
  ICEBERG_UNWRAP_OR_FAIL(auto catalog, CreateCatalogAndNamespace(ns));
  TableIdentifier table_id{.ns = ns, .name = "fetch_plan_table"};
  ASSERT_THAT(CreateDefaultTable(catalog, table_id), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto table, catalog->LoadTable(table_id));

  internal::TableScanContext context;
  ICEBERG_UNWRAP_OR_FAIL(auto plan_response, catalog->PlanTableScan(*table, context));

  if (!plan_response.plan_id.empty()) {
    // Async plan: fetch the result using the returned plan_id
    ICEBERG_UNWRAP_OR_FAIL(auto fetch_response,
                           catalog->FetchPlanningResult(*table, plan_response.plan_id));
    EXPECT_TRUE(fetch_response.file_scan_tasks.empty());
  } else {
    // Synchronous plan: file_scan_tasks already returned; verify nonexistent plan_id errors
    EXPECT_FALSE(catalog->FetchPlanningResult(*table, "nonexistent-plan-id").has_value());
  }
}

TEST_F(RestCatalogIntegrationTest, CancelPlanning) {
  Namespace ns{.levels = {"test_cancel_planning"}};
  ICEBERG_UNWRAP_OR_FAIL(auto catalog, CreateCatalogAndNamespace(ns));
  TableIdentifier table_id{.ns = ns, .name = "cancel_plan_table"};
  ASSERT_THAT(CreateDefaultTable(catalog, table_id), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto table, catalog->LoadTable(table_id));

  internal::TableScanContext context;
  ICEBERG_UNWRAP_OR_FAIL(auto plan_response, catalog->PlanTableScan(*table, context));

  if (!plan_response.plan_id.empty()) {
    // Async plan: cancel it
    ASSERT_THAT(catalog->CancelPlanning(*table, plan_response.plan_id), IsOk());
  } else {
    // Synchronous plan: verify cancelling a nonexistent plan_id returns an error
    EXPECT_FALSE(catalog->CancelPlanning(*table, "nonexistent-plan-id").has_value());
  }
}

TEST_F(RestCatalogIntegrationTest, FetchScanTasks) {
  Namespace ns{.levels = {"test_fetch_scan_tasks"}};
  ICEBERG_UNWRAP_OR_FAIL(auto catalog, CreateCatalogAndNamespace(ns));
  TableIdentifier table_id{.ns = ns, .name = "fetch_tasks_table"};
  ASSERT_THAT(CreateDefaultTable(catalog, table_id), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto table, catalog->LoadTable(table_id));

  internal::TableScanContext context;
  ICEBERG_UNWRAP_OR_FAIL(auto plan_response, catalog->PlanTableScan(*table, context));

  if (!plan_response.plan_tasks.empty()) {
    // Use the first plan task token to fetch scan tasks
    ICEBERG_UNWRAP_OR_FAIL(
        auto tasks_response,
        catalog->FetchScanTasks(*table, plan_response.plan_tasks[0]));
    EXPECT_TRUE(tasks_response.file_scan_tasks.empty());
  }
  // If synchronous plan (no plan_tasks), file_scan_tasks are already in plan_response
}

// -- Snapshot loading mode --

TEST_F(RestCatalogIntegrationTest, LoadTableWithSnapshotModeAll) {
  Namespace ns{.levels = {"test_snapshot_all"}};
  ICEBERG_UNWRAP_OR_FAIL(auto catalog, CreateCatalogWithSnapshotMode("ALL"));
  ASSERT_THAT(catalog->CreateNamespace(ns, {}), IsOk());

  TableIdentifier table_id{.ns = ns, .name = "all_mode_table"};
  ASSERT_THAT(CreateDefaultTable(catalog, table_id), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto loaded, catalog->LoadTable(table_id));
  EXPECT_NE(loaded->metadata(), nullptr);
  EXPECT_FALSE(loaded->metadata()->schemas.empty());
}

TEST_F(RestCatalogIntegrationTest, LoadTableWithSnapshotModeRefs) {
  Namespace ns{.levels = {"test_snapshot_refs"}};
  ICEBERG_UNWRAP_OR_FAIL(auto catalog, CreateCatalogWithSnapshotMode("REFS"));
  ASSERT_THAT(catalog->CreateNamespace(ns, {}), IsOk());

  TableIdentifier table_id{.ns = ns, .name = "refs_mode_table"};
  ASSERT_THAT(CreateDefaultTable(catalog, table_id), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto loaded, catalog->LoadTable(table_id));
  EXPECT_NE(loaded->metadata(), nullptr);
  EXPECT_FALSE(loaded->metadata()->schemas.empty());
}

}  // namespace iceberg::rest
