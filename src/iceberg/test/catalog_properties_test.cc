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

#include "iceberg/catalog/rest/catalog_properties.h"

#include <gtest/gtest.h>

#include "iceberg/test/matchers.h"

namespace iceberg::rest {

TEST(ScanPlanningModeTest, MissingKeyReturnsNullopt) {
  std::unordered_map<std::string, std::string> config;
  auto result = RestCatalogProperties::ScanPlanningModeFrom(config);
  ASSERT_THAT(result, IsOk());
  EXPECT_FALSE(result->has_value());
}

TEST(ScanPlanningModeTest, ClientLowercaseReturnsKClient) {
  auto result = RestCatalogProperties::ScanPlanningModeFrom(
      {{"scan-planning-mode", "client"}});
  ASSERT_THAT(result, IsOk());
  ASSERT_TRUE(result->has_value());
  EXPECT_EQ(**result, ScanPlanningMode::kClient);
}

TEST(ScanPlanningModeTest, ServerLowercaseReturnsKServer) {
  auto result = RestCatalogProperties::ScanPlanningModeFrom(
      {{"scan-planning-mode", "server"}});
  ASSERT_THAT(result, IsOk());
  ASSERT_TRUE(result->has_value());
  EXPECT_EQ(**result, ScanPlanningMode::kServer);
}

TEST(ScanPlanningModeTest, ClientUppercaseReturnsKClient) {
  auto result = RestCatalogProperties::ScanPlanningModeFrom(
      {{"scan-planning-mode", "CLIENT"}});
  ASSERT_THAT(result, IsOk());
  ASSERT_TRUE(result->has_value());
  EXPECT_EQ(**result, ScanPlanningMode::kClient);
}

TEST(ScanPlanningModeTest, ServerUppercaseReturnsKServer) {
  auto result = RestCatalogProperties::ScanPlanningModeFrom(
      {{"scan-planning-mode", "SERVER"}});
  ASSERT_THAT(result, IsOk());
  ASSERT_TRUE(result->has_value());
  EXPECT_EQ(**result, ScanPlanningMode::kServer);
}

TEST(ScanPlanningModeTest, InvalidValueReturnsError) {
  auto result = RestCatalogProperties::ScanPlanningModeFrom(
      {{"scan-planning-mode", "invalid"}});
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
}

TEST(ScanPlanningModeTest, OtherKeysAreIgnored) {
  auto result = RestCatalogProperties::ScanPlanningModeFrom(
      {{"other-key", "server"}, {"scan-planning-mode", "client"}});
  ASSERT_THAT(result, IsOk());
  ASSERT_TRUE(result->has_value());
  EXPECT_EQ(**result, ScanPlanningMode::kClient);
}

}  // namespace iceberg::rest
