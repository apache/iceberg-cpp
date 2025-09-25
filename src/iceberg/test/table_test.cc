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
#include "test_common.h"

namespace iceberg {

TEST(Table, TableV1) {
  std::unique_ptr<TableMetadata> metadata;
  ASSERT_NO_FATAL_FAILURE(ReadTableMetadata("TableMetadataV1Valid.json", &metadata));
  TableIdentifier tableIdent{.ns = {}, .name = "test_table_v1"};
  Table table(tableIdent, std::move(metadata), "s3://bucket/test/location/meta/", nullptr,
              nullptr);
  ASSERT_EQ(table.name().name, "test_table_v1");

  // Check table schema
  auto schema = table.schema();
  ASSERT_TRUE(schema.has_value());
  ASSERT_EQ(schema.value()->fields().size(), 3);
  auto schemas = table.schemas();
  ASSERT_TRUE(schemas->empty());

  // Check table spec
  auto spec = table.spec();
  ASSERT_TRUE(spec.has_value());
  auto specs = table.specs();
  ASSERT_EQ(1UL, specs->size());

  // Check table sort_order
  auto sort_order = table.sort_order();
  ASSERT_TRUE(sort_order.has_value());
  auto sort_orders = table.sort_orders();
  ASSERT_EQ(1UL, sort_orders->size());

  // Check table location
  auto location = table.location();
  ASSERT_EQ(location, "s3://bucket/test/location");

  // Check table snapshots
  auto snapshots = table.snapshots();
  ASSERT_TRUE(snapshots.empty());

  auto io = table.io();
  ASSERT_TRUE(io == nullptr);
}

TEST(Table, TableV2) {
  std::unique_ptr<TableMetadata> metadata;
  ASSERT_NO_FATAL_FAILURE(ReadTableMetadata("TableMetadataV2Valid.json", &metadata));
  TableIdentifier tableIdent{.ns = {}, .name = "test_table_v2"};

  Table table(tableIdent, std::move(metadata), "s3://bucket/test/location/meta/", nullptr,
              nullptr);
  ASSERT_EQ(table.name().name, "test_table_v2");

  // Check table schema
  auto schema = table.schema();
  ASSERT_TRUE(schema.has_value());
  ASSERT_EQ(schema.value()->fields().size(), 3);
  auto schemas = table.schemas();
  ASSERT_FALSE(schemas->empty());

  // Check partition spec
  auto spec = table.spec();
  ASSERT_TRUE(spec.has_value());
  auto specs = table.specs();
  ASSERT_EQ(1UL, specs->size());

  // Check sort order
  auto sort_order = table.sort_order();
  ASSERT_TRUE(sort_order.has_value());
  auto sort_orders = table.sort_orders();
  ASSERT_EQ(1UL, sort_orders->size());

  // Check table location
  auto location = table.location();
  ASSERT_EQ(location, "s3://bucket/test/location");

  // Check snapshot
  auto snapshots = table.snapshots();
  ASSERT_EQ(2UL, snapshots.size());
  auto snapshot = table.current_snapshot();
  ASSERT_TRUE(snapshot.has_value());
  snapshot = table.SnapshotById(snapshot.value()->snapshot_id);
  ASSERT_TRUE(snapshot.has_value());
  auto invalid_snapshot_id = 9999;
  snapshot = table.SnapshotById(invalid_snapshot_id);
  ASSERT_FALSE(snapshot.has_value());
}

}  // namespace iceberg
