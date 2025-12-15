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

#include "iceberg/catalog/rest/endpoint.h"

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/catalog/rest/json_internal.h"
#include "iceberg/test/matchers.h"

namespace iceberg::rest {

TEST(EndpointTest, InvalidCreate) {
  // Empty path template should fail
  auto result = Endpoint::Create(HttpMethod::GET, "");
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result, HasErrorMessage("Path template cannot be empty"));
}

TEST(EndpointTest, ValidFromString) {
  auto result = Endpoint::FromString("GET /path");
  EXPECT_THAT(result, IsOk());

  auto endpoint = result.value();
  EXPECT_EQ(endpoint.method(), HttpMethod::GET);
  EXPECT_EQ(endpoint.path_template(), "/path");
}

TEST(EndpointTest, ToStringRepresentation) {
  auto endpoint1 = Endpoint::Create(HttpMethod::POST, "/path/of/resource");
  ASSERT_THAT(endpoint1, IsOk());
  EXPECT_EQ(endpoint1->ToString(), "POST /path/of/resource");

  auto endpoint2 = Endpoint::Create(HttpMethod::GET, "/");
  ASSERT_THAT(endpoint2, IsOk());
  EXPECT_EQ(endpoint2->ToString(), "GET /");

  auto endpoint3 = Endpoint::Create(HttpMethod::PUT, "/");
  ASSERT_THAT(endpoint3, IsOk());
  EXPECT_EQ(endpoint3->ToString(), "PUT /");

  auto endpoint4 = Endpoint::Create(HttpMethod::PUT, "/namespaces/{namespace}/{x}");
  ASSERT_THAT(endpoint4, IsOk());
  EXPECT_EQ(endpoint4->ToString(), "PUT /namespaces/{namespace}/{x}");
}

// Test all HTTP methods
TEST(EndpointTest, AllHttpMethods) {
  auto get = Endpoint::Create(HttpMethod::GET, "/path");
  ASSERT_THAT(get, IsOk());
  EXPECT_EQ(get->ToString(), "GET /path");

  auto post = Endpoint::Create(HttpMethod::POST, "/path");
  ASSERT_THAT(post, IsOk());
  EXPECT_EQ(post->ToString(), "POST /path");

  auto put = Endpoint::Create(HttpMethod::PUT, "/path");
  ASSERT_THAT(put, IsOk());
  EXPECT_EQ(put->ToString(), "PUT /path");

  auto del = Endpoint::Create(HttpMethod::DELETE_, "/path");
  ASSERT_THAT(del, IsOk());
  EXPECT_EQ(del->ToString(), "DELETE /path");

  auto head = Endpoint::Create(HttpMethod::HEAD, "/path");
  ASSERT_THAT(head, IsOk());
  EXPECT_EQ(head->ToString(), "HEAD /path");
}

// Test predefined namespace endpoints
TEST(EndpointTest, NamespaceEndpoints) {
  auto list_namespaces = Endpoint::ListNamespaces();
  EXPECT_EQ(list_namespaces.method(), HttpMethod::GET);
  EXPECT_EQ(list_namespaces.path_template(), "/v1/{prefix}/namespaces");
  EXPECT_EQ(list_namespaces.ToString(), "GET /v1/{prefix}/namespaces");

  auto get_namespace = Endpoint::GetNamespaceProperties();
  EXPECT_EQ(get_namespace.method(), HttpMethod::GET);
  EXPECT_EQ(get_namespace.path_template(), "/v1/{prefix}/namespaces/{namespace}");

  auto namespace_exists = Endpoint::NamespaceExists();
  EXPECT_EQ(namespace_exists.method(), HttpMethod::HEAD);
  EXPECT_EQ(namespace_exists.path_template(), "/v1/{prefix}/namespaces/{namespace}");

  auto create_namespace = Endpoint::CreateNamespace();
  EXPECT_EQ(create_namespace.method(), HttpMethod::POST);
  EXPECT_EQ(create_namespace.path_template(), "/v1/{prefix}/namespaces");

  auto update_namespace = Endpoint::UpdateNamespace();
  EXPECT_EQ(update_namespace.method(), HttpMethod::POST);
  EXPECT_EQ(update_namespace.path_template(),
            "/v1/{prefix}/namespaces/{namespace}/properties");

  auto drop_namespace = Endpoint::DropNamespace();
  EXPECT_EQ(drop_namespace.method(), HttpMethod::DELETE_);
  EXPECT_EQ(drop_namespace.path_template(), "/v1/{prefix}/namespaces/{namespace}");
}

// Test predefined table endpoints
TEST(EndpointTest, TableEndpoints) {
  auto list_tables = Endpoint::ListTables();
  EXPECT_EQ(list_tables.method(), HttpMethod::GET);
  EXPECT_EQ(list_tables.path_template(), "/v1/{prefix}/namespaces/{namespace}/tables");

  auto load_table = Endpoint::LoadTable();
  EXPECT_EQ(load_table.method(), HttpMethod::GET);
  EXPECT_EQ(load_table.path_template(),
            "/v1/{prefix}/namespaces/{namespace}/tables/{table}");

  auto table_exists = Endpoint::TableExists();
  EXPECT_EQ(table_exists.method(), HttpMethod::HEAD);
  EXPECT_EQ(table_exists.path_template(),
            "/v1/{prefix}/namespaces/{namespace}/tables/{table}");

  auto create_table = Endpoint::CreateTable();
  EXPECT_EQ(create_table.method(), HttpMethod::POST);
  EXPECT_EQ(create_table.path_template(), "/v1/{prefix}/namespaces/{namespace}/tables");

  auto update_table = Endpoint::UpdateTable();
  EXPECT_EQ(update_table.method(), HttpMethod::POST);
  EXPECT_EQ(update_table.path_template(),
            "/v1/{prefix}/namespaces/{namespace}/tables/{table}");

  auto delete_table = Endpoint::DeleteTable();
  EXPECT_EQ(delete_table.method(), HttpMethod::DELETE_);
  EXPECT_EQ(delete_table.path_template(),
            "/v1/{prefix}/namespaces/{namespace}/tables/{table}");

  auto rename_table = Endpoint::RenameTable();
  EXPECT_EQ(rename_table.method(), HttpMethod::POST);
  EXPECT_EQ(rename_table.path_template(), "/v1/{prefix}/tables/rename");

  auto register_table = Endpoint::RegisterTable();
  EXPECT_EQ(register_table.method(), HttpMethod::POST);
  EXPECT_EQ(register_table.path_template(),
            "/v1/{prefix}/namespaces/{namespace}/register");

  auto report_metrics = Endpoint::ReportMetrics();
  EXPECT_EQ(report_metrics.method(), HttpMethod::POST);
  EXPECT_EQ(report_metrics.path_template(),
            "/v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics");

  auto table_credentials = Endpoint::TableCredentials();
  EXPECT_EQ(table_credentials.method(), HttpMethod::GET);
  EXPECT_EQ(table_credentials.path_template(),
            "/v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials");
}

// Test predefined transaction endpoints
TEST(EndpointTest, TransactionEndpoints) {
  auto commit_transaction = Endpoint::CommitTransaction();
  EXPECT_EQ(commit_transaction.method(), HttpMethod::POST);
  EXPECT_EQ(commit_transaction.path_template(), "/v1/{prefix}/transactions/commit");
}

// Test endpoint equality and comparison
TEST(EndpointTest, EqualityAndComparison) {
  auto endpoint1 = Endpoint::Create(HttpMethod::GET, "/path");
  auto endpoint2 = Endpoint::Create(HttpMethod::GET, "/path");
  auto endpoint3 = Endpoint::Create(HttpMethod::POST, "/path");
  auto endpoint4 = Endpoint::Create(HttpMethod::GET, "/other");

  ASSERT_THAT(endpoint1, IsOk());
  ASSERT_THAT(endpoint2, IsOk());
  ASSERT_THAT(endpoint3, IsOk());
  ASSERT_THAT(endpoint4, IsOk());

  // Equality
  EXPECT_EQ(*endpoint1, *endpoint2);
  EXPECT_NE(*endpoint1, *endpoint3);
  EXPECT_NE(*endpoint1, *endpoint4);

  // Comparison (for ordering)
  EXPECT_LT(*endpoint1, *endpoint3);  // GET < POST
  EXPECT_LT(*endpoint4, *endpoint1);  // "/other" < "/path" lexicographically
}

// Test string serialization (endpoints are represented as strings)
TEST(EndpointTest, ToStringFormat) {
  auto endpoint1 = Endpoint::Create(HttpMethod::GET, "/v1/{prefix}/namespaces");
  ASSERT_THAT(endpoint1, IsOk());
  EXPECT_EQ(endpoint1->ToString(), "GET /v1/{prefix}/namespaces");

  auto endpoint2 = Endpoint::Create(HttpMethod::POST, "/v1/{prefix}/tables");
  ASSERT_THAT(endpoint2, IsOk());
  EXPECT_EQ(endpoint2->ToString(), "POST /v1/{prefix}/tables");

  // Test with all HTTP methods
  auto endpoint3 = Endpoint::Create(HttpMethod::DELETE_, "/path");
  ASSERT_THAT(endpoint3, IsOk());
  EXPECT_EQ(endpoint3->ToString(), "DELETE /path");

  auto endpoint4 = Endpoint::Create(HttpMethod::PUT, "/path");
  ASSERT_THAT(endpoint4, IsOk());
  EXPECT_EQ(endpoint4->ToString(), "PUT /path");

  auto endpoint5 = Endpoint::Create(HttpMethod::HEAD, "/path");
  ASSERT_THAT(endpoint5, IsOk());
  EXPECT_EQ(endpoint5->ToString(), "HEAD /path");
}

// Test string deserialization
TEST(EndpointTest, FromStringParsing) {
  auto result1 = Endpoint::FromString("GET /v1/{prefix}/namespaces");
  ASSERT_THAT(result1, IsOk());
  EXPECT_EQ(result1->method(), HttpMethod::GET);
  EXPECT_EQ(result1->path_template(), "/v1/{prefix}/namespaces");

  auto result2 = Endpoint::FromString("POST /v1/{prefix}/namespaces/{namespace}/tables");
  ASSERT_THAT(result2, IsOk());
  EXPECT_EQ(result2->method(), HttpMethod::POST);
  EXPECT_EQ(result2->path_template(), "/v1/{prefix}/namespaces/{namespace}/tables");

  // Test all HTTP methods
  auto result3 = Endpoint::FromString("DELETE /path");
  ASSERT_THAT(result3, IsOk());
  EXPECT_EQ(result3->method(), HttpMethod::DELETE_);

  auto result4 = Endpoint::FromString("PUT /path");
  ASSERT_THAT(result4, IsOk());
  EXPECT_EQ(result4->method(), HttpMethod::PUT);

  auto result5 = Endpoint::FromString("HEAD /path");
  ASSERT_THAT(result5, IsOk());
  EXPECT_EQ(result5->method(), HttpMethod::HEAD);
}

// Test string parsing with invalid inputs
TEST(EndpointTest, FromStringInvalid) {
  // Invalid endpoint format should fail - missing space
  auto result1 = Endpoint::FromString("/path/without/method");
  EXPECT_THAT(result1, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result1,
              HasErrorMessage("Invalid endpoint format (must consist of two elements "
                              "separated by a single space)"));

  // Invalid HTTP method should fail
  auto result2 = Endpoint::FromString("INVALID /path");
  EXPECT_THAT(result2, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result2, HasErrorMessage("Invalid HTTP method"));

  // Invalid endpoint format - extra element after path
  auto result3 = Endpoint::FromString("GET /path INVALID");
  EXPECT_THAT(result3, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result3,
              HasErrorMessage("Invalid endpoint format (must consist of two elements "
                              "separated by a single space)"));
}

// Test string round-trip
TEST(EndpointTest, StringRoundTrip) {
  // Create various endpoints and verify they survive string round-trip
  std::vector<Endpoint> endpoints = {
      Endpoint::ListNamespaces(),  Endpoint::GetNamespaceProperties(),
      Endpoint::CreateNamespace(), Endpoint::LoadTable(),
      Endpoint::CreateTable(),     Endpoint::DeleteTable(),
  };

  for (const auto& original : endpoints) {
    // Serialize to string
    std::string str = original.ToString();

    // Deserialize from string
    auto deserialized = Endpoint::FromString(str);
    ASSERT_THAT(deserialized, IsOk());

    // Verify they are equal
    EXPECT_EQ(original, *deserialized);
    EXPECT_EQ(original.ToString(), deserialized->ToString());
  }
}

}  // namespace iceberg::rest
