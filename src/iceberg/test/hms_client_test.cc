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

#include "iceberg/catalog/hive/hms_client.h"

#include <gtest/gtest.h>

#include "iceberg/catalog/hive/hive_catalog_properties.h"
#include "iceberg/result.h"

namespace iceberg::hive {

TEST(ParseHmsUrisTest, SingleHostPort) {
  auto result = ParseHmsUris("localhost:9083");
  ASSERT_TRUE(result.has_value()) << result.error().message;
  ASSERT_EQ(result->size(), 1);
  EXPECT_EQ((*result)[0].host, "localhost");
  EXPECT_EQ((*result)[0].port, 9083);
}

TEST(ParseHmsUrisTest, ThriftSchemePrefixStripped) {
  auto result = ParseHmsUris("thrift://hms.example.com:9084");
  ASSERT_TRUE(result.has_value()) << result.error().message;
  ASSERT_EQ(result->size(), 1);
  EXPECT_EQ((*result)[0].host, "hms.example.com");
  EXPECT_EQ((*result)[0].port, 9084);
}

TEST(ParseHmsUrisTest, DefaultsToHmsPortWhenAbsent) {
  auto result = ParseHmsUris("hms.example.com");
  ASSERT_TRUE(result.has_value()) << result.error().message;
  ASSERT_EQ(result->size(), 1);
  EXPECT_EQ((*result)[0].host, "hms.example.com");
  EXPECT_EQ((*result)[0].port, 9083);
}

TEST(ParseHmsUrisTest, DefaultsToHmsPortWhenColonOnly) {
  auto result = ParseHmsUris("hms.example.com:");
  ASSERT_TRUE(result.has_value()) << result.error().message;
  ASSERT_EQ((*result)[0].port, 9083);
}

TEST(ParseHmsUrisTest, MultipleHaEndpoints) {
  auto result = ParseHmsUris("thrift://h1:9083,h2:9084,h3");
  ASSERT_TRUE(result.has_value()) << result.error().message;
  ASSERT_EQ(result->size(), 3);
  EXPECT_EQ((*result)[0].host, "h1");
  EXPECT_EQ((*result)[0].port, 9083);
  EXPECT_EQ((*result)[1].host, "h2");
  EXPECT_EQ((*result)[1].port, 9084);
  EXPECT_EQ((*result)[2].host, "h3");
  EXPECT_EQ((*result)[2].port, 9083);
}

TEST(ParseHmsUrisTest, WhitespaceAroundEndpointsTolerated) {
  auto result = ParseHmsUris(" thrift://h1:9083 , h2:9084 ");
  ASSERT_TRUE(result.has_value()) << result.error().message;
  ASSERT_EQ(result->size(), 2);
  EXPECT_EQ((*result)[0].host, "h1");
  EXPECT_EQ((*result)[1].host, "h2");
}

TEST(ParseHmsUrisTest, EmptyUriFails) {
  EXPECT_FALSE(ParseHmsUris("").has_value());
  EXPECT_FALSE(ParseHmsUris("   ").has_value());
}

TEST(ParseHmsUrisTest, EmptyHostFails) {
  EXPECT_FALSE(ParseHmsUris(":9083").has_value());
  EXPECT_FALSE(ParseHmsUris("thrift://:9083").has_value());
}

TEST(ParseHmsUrisTest, NonNumericPortFails) {
  EXPECT_FALSE(ParseHmsUris("localhost:abc").has_value());
}

TEST(ParseHmsUrisTest, OutOfRangePortFails) {
  EXPECT_FALSE(ParseHmsUris("localhost:0").has_value());
  EXPECT_FALSE(ParseHmsUris("localhost:65536").has_value());
  EXPECT_FALSE(ParseHmsUris("localhost:99999").has_value());
}

TEST(ParseHmsUrisTest, TrailingCharsAfterPortFails) {
  EXPECT_FALSE(ParseHmsUris("localhost:9083abc").has_value());
  EXPECT_FALSE(ParseHmsUris("thrift://host:9083/path").has_value());
}

TEST(ParseHmsUrisTest, EmptySegmentInListFails) {
  EXPECT_FALSE(ParseHmsUris("h1:9083,,h2:9084").has_value());
  EXPECT_FALSE(ParseHmsUris("h1:9083,").has_value());
}

TEST(HmsClientConnectTest, MissingUriIsInvalidArgument) {
  auto config = HiveCatalogProperties::default_properties();
  auto client = HmsClient::Connect(config);
  ASSERT_FALSE(client.has_value());
  EXPECT_EQ(client.error().kind, ErrorKind::kInvalidArgument);
}

TEST(HmsClientConnectTest, BadUriIsInvalidArgument) {
  auto config = HiveCatalogProperties::FromMap(
      {{std::string(HiveCatalogProperties::kUri.key()), "thrift://:9083"}});
  auto client = HmsClient::Connect(config);
  ASSERT_FALSE(client.has_value());
  EXPECT_EQ(client.error().kind, ErrorKind::kInvalidArgument);
}

TEST(HmsClientConnectTest, BadTransportIsInvalidArgument) {
  auto config = HiveCatalogProperties::FromMap(
      {{std::string(HiveCatalogProperties::kUri.key()), "localhost:9083"},
       {std::string(HiveCatalogProperties::kThriftTransport.key()), "bogus"}});
  auto client = HmsClient::Connect(config);
  ASSERT_FALSE(client.has_value());
  EXPECT_EQ(client.error().kind, ErrorKind::kInvalidArgument);
}

TEST(HmsClientConnectTest, MultipleEndpointsRejectedUntilFailover) {
  auto config = HiveCatalogProperties::FromMap(
      {{std::string(HiveCatalogProperties::kUri.key()), "h1:9083,h2:9084"}});
  auto client = HmsClient::Connect(config);
  ASSERT_FALSE(client.has_value());
  EXPECT_EQ(client.error().kind, ErrorKind::kInvalidArgument);
}

TEST(HmsClientConnectTest, UnreachableHmsIsIoError) {
  // Port 1 is privileged; nothing should be listening. We assert that the
  // failure surfaces as IOError rather than a Thrift C++ exception.
  auto config = HiveCatalogProperties::FromMap(
      {{std::string(HiveCatalogProperties::kUri.key()), "127.0.0.1:1"},
       {std::string(HiveCatalogProperties::kConnectTimeoutMs.key()), "200"}});
  auto client = HmsClient::Connect(config);
  ASSERT_FALSE(client.has_value());
  EXPECT_EQ(client.error().kind, ErrorKind::kIOError);
}

}  // namespace iceberg::hive
