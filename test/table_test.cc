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

#include "iceberg/table.h"

#include <filesystem>
#include <fstream>
#include <optional>
#include <sstream>
#include <string>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
#include "table_test_helper.h"

namespace iceberg {

namespace {

class TableTest : public ::testing::Test {
 protected:
  void SetUp() override {}
};

}  // namespace

TEST_F(TableTest, TableSchemaV1Test) {
  std::unique_ptr<TableMetadata> metadata;
  ASSERT_NO_FATAL_FAILURE(
      TableTestHelper::ReadTableMetadata("TableMetadataV1Valid.json", &metadata));

  StaticTable table("test_table_v1", std::move(metadata));
  ASSERT_EQ(table.name(), "test_table_v1");
  auto schema = table.schema();
  ASSERT_TRUE(schema != nullptr);
  ASSERT_EQ(schema->fields().size(), 3);
  auto schemas = table.schemas();
  ASSERT_TRUE(schemas.empty());

  auto spec = table.spec();
  ASSERT_TRUE(spec != nullptr);
  auto specs = table.specs();
  ASSERT_EQ(1UL, specs.size());

  auto sort_order = table.sort_order();
  ASSERT_TRUE(sort_order != nullptr);
  auto sort_orders = table.sort_orders();
  ASSERT_EQ(1UL, sort_orders.size());

  auto location = table.location();
  ASSERT_EQ(location, "s3://bucket/test/location");

  auto snapshots = table.snapshots();
  ASSERT_TRUE(snapshots.empty());
}

TEST_F(TableTest, TableSchemaV2Test) {
  std::unique_ptr<TableMetadata> metadata;
  ASSERT_NO_FATAL_FAILURE(
      TableTestHelper::ReadTableMetadata("TableMetadataV2Valid.json", &metadata));

  StaticTable table("test_table_v2", std::move(metadata));
  ASSERT_EQ(table.name(), "test_table_v2");
  auto schema = table.schema();
  ASSERT_TRUE(schema != nullptr);
  ASSERT_EQ(schema->fields().size(), 3);
  auto schemas = table.schemas();
  ASSERT_FALSE(schemas.empty());

  auto spec = table.spec();
  ASSERT_TRUE(spec != nullptr);
  auto specs = table.specs();
  ASSERT_EQ(1UL, specs.size());

  auto sort_order = table.sort_order();
  ASSERT_TRUE(sort_order != nullptr);
  auto sort_orders = table.sort_orders();
  ASSERT_EQ(1UL, sort_orders.size());

  auto location = table.location();
  ASSERT_EQ(location, "s3://bucket/test/location");

  auto snapshots = table.snapshots();
  ASSERT_EQ(2UL, snapshots.size());
  auto snapshot = table.current_snapshot();
  ASSERT_TRUE(snapshot != nullptr);
}

}  // namespace iceberg
