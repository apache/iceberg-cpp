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

#include <httplib.h>

#include <memory>
#include <thread>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

namespace iceberg::catalog::rest {

class RestCatalogIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    server_ = std::make_unique<httplib::Server>();
    port_ = server_->bind_to_any_port("127.0.0.1");

    server_thread_ = std::thread([this]() { server_->listen_after_bind(); });
  }

  void TearDown() override {
    server_->stop();
    if (server_thread_.joinable()) {
      server_thread_.join();
    }
  }

  std::unique_ptr<httplib::Server> server_;
  int port_ = -1;
  std::thread server_thread_;
};

TEST_F(RestCatalogIntegrationTest, DISABLED_GetConfigSuccessfully) {
  server_->Get("/v1/config", [](const httplib::Request&, httplib::Response& res) {
    res.status = 200;
    res.set_content(R"({"warehouse": "s3://test-bucket"})", "application/json");
  });

  std::string base_uri = "http://127.0.0.1:" + std::to_string(port_);
  RestCatalog catalog(base_uri);
  cpr::Response response = catalog.GetConfig();

  ASSERT_EQ(response.error.code, cpr::ErrorCode::OK);
  ASSERT_EQ(response.status_code, 200);

  auto json_body = nlohmann::json::parse(response.text);
  EXPECT_EQ(json_body["warehouse"], "s3://test-bucket");
}

TEST_F(RestCatalogIntegrationTest, DISABLED_ListNamespacesReturnsMultipleResults) {
  server_->Get("/v1/namespaces", [](const httplib::Request&, httplib::Response& res) {
    res.status = 200;
    res.set_content(R"({
         "namespaces": [
             ["accounting", "db"],
             ["production", "db"]
         ]
     })",
                    "application/json");
  });

  std::string base_uri = "http://127.0.0.1:" + std::to_string(port_);
  RestCatalog catalog(base_uri);
  cpr::Response response = catalog.ListNamespaces();

  ASSERT_EQ(response.error.code, cpr::ErrorCode::OK);
  ASSERT_EQ(response.status_code, 200);

  auto json_body = nlohmann::json::parse(response.text);
  ASSERT_TRUE(json_body.contains("namespaces"));
  EXPECT_EQ(json_body["namespaces"].size(), 2);
  EXPECT_THAT(json_body["namespaces"][0][0], "accounting");
}

TEST_F(RestCatalogIntegrationTest, DISABLED_HandlesServerError) {
  server_->Get("/v1/config", [](const httplib::Request&, httplib::Response& res) {
    res.status = 500;
    res.set_content("Internal Server Error", "text/plain");
  });

  std::string base_uri = "http://127.0.0.1:" + std::to_string(port_);
  RestCatalog catalog(base_uri);
  cpr::Response response = catalog.GetConfig();

  ASSERT_EQ(response.error.code, cpr::ErrorCode::OK);
  ASSERT_EQ(response.status_code, 500);
  ASSERT_EQ(response.text, "Internal Server Error");
}

}  // namespace iceberg::catalog::rest
