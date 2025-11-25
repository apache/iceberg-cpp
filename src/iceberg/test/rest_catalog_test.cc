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

#include <string>
#include <unordered_map>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/catalog/rest/catalog_properties.h"
#include "iceberg/table_identifier.h"
#include "iceberg/test/matchers.h"

namespace iceberg::rest {

// Test fixture for REST catalog tests, This assumes you have a local REST catalog service
// running Default configuration: http://localhost:8181.
class RestCatalogTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Default configuration for local testing
    // You can override this with environment variables if needed
    const char* uri_env = std::getenv("ICEBERG_REST_URI");
    const char* warehouse_env = std::getenv("ICEBERG_REST_WAREHOUSE");

    std::string uri = uri_env ? uri_env : "http://localhost:8181";
    std::string warehouse = warehouse_env ? warehouse_env : "default";

    config_ = RestCatalogProperties::default_properties();
    config_->Set(RestCatalogProperties::kUri, uri)
        .Set(RestCatalogProperties::kName, std::string("test_catalog"))
        .Set(RestCatalogProperties::kWarehouse, warehouse);
  }

  void TearDown() override {}

  std::unique_ptr<RestCatalogProperties> config_;
};

TEST_F(RestCatalogTest, DISABLED_MakeCatalogSuccess) {
  auto catalog_result = RestCatalog::Make(*config_);
  EXPECT_THAT(catalog_result, IsOk());

  if (catalog_result.has_value()) {
    auto& catalog = catalog_result.value();
    EXPECT_EQ(catalog->name(), "test_catalog");
  }
}

TEST_F(RestCatalogTest, DISABLED_MakeCatalogEmptyUri) {
  auto invalid_config = RestCatalogProperties::default_properties();
  invalid_config->Set(RestCatalogProperties::kUri, std::string(""));

  auto catalog_result = RestCatalog::Make(*invalid_config);
  EXPECT_THAT(catalog_result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(catalog_result, HasErrorMessage("uri"));
}

TEST_F(RestCatalogTest, DISABLED_MakeCatalogWithCustomProperties) {
  auto custom_config = RestCatalogProperties::default_properties();
  custom_config
      ->Set(RestCatalogProperties::kUri, config_->Get(RestCatalogProperties::kUri))
      .Set(RestCatalogProperties::kName, config_->Get(RestCatalogProperties::kName))
      .Set(RestCatalogProperties::kWarehouse,
           config_->Get(RestCatalogProperties::kWarehouse))
      .Set(RestCatalogProperties::Entry<std::string>{"custom_prop", ""},
           std::string("custom_value"))
      .Set(RestCatalogProperties::Entry<std::string>{"timeout", ""},
           std::string("30000"));

  auto catalog_result = RestCatalog::Make(*custom_config);
  EXPECT_THAT(catalog_result, IsOk());
}

TEST_F(RestCatalogTest, DISABLED_ListNamespaces) {
  auto catalog_result = RestCatalog::Make(*config_);
  ASSERT_THAT(catalog_result, IsOk());
  auto& catalog = catalog_result.value();

  Namespace ns{.levels = {}};
  auto result = catalog->ListNamespaces(ns);
  EXPECT_THAT(result, IsOk());
  EXPECT_FALSE(result->empty());
  EXPECT_EQ(result->front().levels, (std::vector<std::string>{"my_namespace_test2"}));
}

TEST_F(RestCatalogTest, DISABLED_CreateNamespaceNotImplemented) {
  auto catalog_result = RestCatalog::Make(*config_);
  ASSERT_THAT(catalog_result, IsOk());
  auto catalog = std::move(catalog_result.value());

  Namespace ns{.levels = {"test_namespace"}};
  std::unordered_map<std::string, std::string> props = {{"owner", "test"}};

  auto result = catalog->CreateNamespace(ns, props);
  EXPECT_THAT(result, IsError(ErrorKind::kNotImplemented));
}

TEST_F(RestCatalogTest, DISABLED_IntegrationTestFullNamespaceWorkflow) {
  auto catalog_result = RestCatalog::Make(*config_);
  ASSERT_THAT(catalog_result, IsOk());
  auto catalog = std::move(catalog_result.value());

  // 1. List initial namespaces
  Namespace root{.levels = {}};
  auto list_result1 = catalog->ListNamespaces(root);
  ASSERT_THAT(list_result1, IsOk());
  size_t initial_count = list_result1->size();

  // 2. Create a new namespace
  Namespace test_ns{.levels = {"integration_test_ns"}};
  std::unordered_map<std::string, std::string> props = {
      {"owner", "test"}, {"created_by", "rest_catalog_test"}};
  auto create_result = catalog->CreateNamespace(test_ns, props);
  EXPECT_THAT(create_result, IsOk());

  // 3. Verify namespace exists
  auto exists_result = catalog->NamespaceExists(test_ns);
  EXPECT_THAT(exists_result, HasValue(::testing::Eq(true)));

  // 4. List namespaces again (should have one more)
  auto list_result2 = catalog->ListNamespaces(root);
  ASSERT_THAT(list_result2, IsOk());
  EXPECT_EQ(list_result2->size(), initial_count + 1);

  // 5. Get namespace properties
  auto props_result = catalog->GetNamespaceProperties(test_ns);
  ASSERT_THAT(props_result, IsOk());
  EXPECT_EQ((*props_result)["owner"], "test");

  // 6. Update properties
  std::unordered_map<std::string, std::string> updates = {
      {"description", "test namespace"}};
  std::unordered_set<std::string> removals = {};
  auto update_result = catalog->UpdateNamespaceProperties(test_ns, updates, removals);
  EXPECT_THAT(update_result, IsOk());

  // 7. Verify updated properties
  auto props_result2 = catalog->GetNamespaceProperties(test_ns);
  ASSERT_THAT(props_result2, IsOk());
  EXPECT_EQ((*props_result2)["description"], "test namespace");

  // 8. Drop the namespace (cleanup)
  auto drop_result = catalog->DropNamespace(test_ns);
  EXPECT_THAT(drop_result, IsOk());

  // 9. Verify namespace no longer exists
  auto exists_result2 = catalog->NamespaceExists(test_ns);
  EXPECT_THAT(exists_result2, HasValue(::testing::Eq(false)));
}

}  // namespace iceberg::rest
