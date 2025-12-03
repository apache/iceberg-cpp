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
#include <sys/socket.h>

#include "iceberg/catalog/rest/catalog_properties.h"
#include "iceberg/result.h"
#include "iceberg/table_identifier.h"
#include "iceberg/test/matchers.h"
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
  Result<std::unique_ptr<RestCatalog>> CreateCatalog() {
    auto config = RestCatalogProperties::default_properties();
    config
        ->Set(RestCatalogProperties::kUri,
              std::format("{}:{}", kLocalhostUri, kRestCatalogPort))
        .Set(RestCatalogProperties::kName, std::string(kCatalogName))
        .Set(RestCatalogProperties::kWarehouse, std::string(kWarehouseName));
    return RestCatalog::Make(*config);
  }

  static inline std::unique_ptr<DockerCompose> docker_compose_;
};

TEST_F(RestCatalogIntegrationTest, MakeCatalogSuccess) {
  auto catalog_result = CreateCatalog();
  ASSERT_THAT(catalog_result, IsOk());

  auto& catalog = catalog_result.value();
  EXPECT_EQ(catalog->name(), kCatalogName);
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

}  // namespace iceberg::rest
