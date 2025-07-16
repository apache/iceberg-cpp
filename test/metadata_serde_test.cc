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

#include <filesystem>
#include <fstream>
#include <optional>
#include <string>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/partition_field.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/snapshot.h"
#include "iceberg/sort_field.h"
#include "iceberg/sort_order.h"
#include "iceberg/table_metadata.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"
#include "test_common.h"

namespace iceberg {

namespace {

class MetadataSerdeTest : public ::testing::Test {
 protected:
  void SetUp() override {}
};

}  // namespace

TEST_F(MetadataSerdeTest, DeserializeV1Valid) {
  std::unique_ptr<TableMetadata> metadata;
  ASSERT_NO_FATAL_FAILURE(ReadTableMetadata("TableMetadataV1Valid.json", &metadata));

  EXPECT_EQ(metadata->format_version, 1);
  EXPECT_EQ(metadata->table_uuid, "d20125c8-7284-442c-9aea-15fee620737c");
  EXPECT_EQ(metadata->location, "s3://bucket/test/location");
  EXPECT_EQ(metadata->last_updated_ms.time_since_epoch().count(), 1602638573874);
  EXPECT_EQ(metadata->last_column_id, 3);
  EXPECT_EQ(metadata->current_snapshot_id, -1);

  // Compare schema
  EXPECT_EQ(metadata->current_schema_id, std::nullopt);
  std::vector<SchemaField> schema_fields;
  schema_fields.emplace_back(/*field_id=*/1, "x", iceberg::int64(),
                             /*optional=*/false);
  schema_fields.emplace_back(/*field_id=*/2, "y", iceberg::int64(),
                             /*optional=*/false);
  schema_fields.emplace_back(/*field_id=*/3, "z", iceberg::int64(),
                             /*optional=*/false);
  auto expected_schema =
      std::make_shared<Schema>(schema_fields, /*schema_id=*/std::nullopt);
  auto schema = metadata->Schema();
  ASSERT_TRUE(schema.has_value());
  EXPECT_EQ(*(schema.value().get()), *expected_schema);

  // Compare partition spec
  std::vector<PartitionField> partition_fields;
  partition_fields.emplace_back(/*source_id=*/1, /*field_id=*/1000, /*name=*/"x",
                                Transform::Identity());
  auto expected_spec =
      std::make_shared<PartitionSpec>(expected_schema, /*spec_id=*/0, partition_fields);
  auto partition_spec = metadata->PartitionSpec();
  ASSERT_TRUE(partition_spec.has_value());
  EXPECT_EQ(*(partition_spec.value().get()), *expected_spec);
  auto snapshot = metadata->Snapshot();
  ASSERT_FALSE(snapshot.has_value());
}

TEST_F(MetadataSerdeTest, DeserializeV2Valid) {
  std::unique_ptr<TableMetadata> metadata;
  ASSERT_NO_FATAL_FAILURE(ReadTableMetadata("TableMetadataV2Valid.json", &metadata));

  EXPECT_EQ(metadata->format_version, 2);
  EXPECT_EQ(metadata->table_uuid, "9c12d441-03fe-4693-9a96-a0705ddf69c1");
  EXPECT_EQ(metadata->location, "s3://bucket/test/location");
  EXPECT_EQ(metadata->last_updated_ms.time_since_epoch().count(), 1602638573590);
  EXPECT_EQ(metadata->last_column_id, 3);

  // Compare schema
  EXPECT_EQ(metadata->current_schema_id, 1);
  std::vector<SchemaField> schema_fields;
  schema_fields.emplace_back(/*field_id=*/1, "x", iceberg::int64(),
                             /*optional=*/false);
  schema_fields.emplace_back(/*field_id=*/2, "y", iceberg::int64(),
                             /*optional=*/false);
  schema_fields.emplace_back(/*field_id=*/3, "z", iceberg::int64(),
                             /*optional=*/false);
  auto expected_schema = std::make_shared<Schema>(schema_fields, /*schema_id=*/1);
  auto schema = metadata->Schema();
  ASSERT_TRUE(schema.has_value());
  EXPECT_EQ(*(schema.value().get()), *expected_schema);

  // schema with ID 1
  auto schema_v1 = metadata->SchemaById(1);
  ASSERT_TRUE(schema_v1.has_value());
  EXPECT_EQ(*(schema_v1.value().get()), *expected_schema);

  // schema with ID 0
  auto expected_schema_v0 = std::make_shared<Schema>(
      std::vector<SchemaField>{schema_fields.at(0)}, /*schema_id=*/0);
  auto schema_v0 = metadata->SchemaById(0);
  ASSERT_TRUE(schema_v0.has_value());
  EXPECT_EQ(*(schema_v0.value().get()), *expected_schema_v0);

  // Compare partition spec
  EXPECT_EQ(metadata->default_spec_id, 0);
  std::vector<PartitionField> partition_fields;
  partition_fields.emplace_back(/*source_id=*/1, /*field_id=*/1000, /*name=*/"x",
                                Transform::Identity());
  auto expected_spec = std::make_shared<PartitionSpec>(expected_schema, /*spec_id=*/0,
                                                       std::move(partition_fields));
  auto partition_spec = metadata->PartitionSpec();
  ASSERT_TRUE(partition_spec.has_value());
  EXPECT_EQ(*(partition_spec.value().get()), *expected_spec);

  // Compare sort order
  EXPECT_EQ(metadata->default_sort_order_id, 3);
  std::vector<SortField> sort_fields;
  sort_fields.emplace_back(/*source_id=*/2, Transform::Identity(),
                           SortDirection::kAscending, NullOrder::kFirst);
  sort_fields.emplace_back(/*source_id=*/3, Transform::Bucket(4),
                           SortDirection::kDescending, NullOrder::kLast);
  auto expected_sort_order =
      std::make_shared<SortOrder>(/*order_id=*/3, std::move(sort_fields));
  auto sort_order = metadata->SortOrder();
  ASSERT_TRUE(sort_order.has_value());
  EXPECT_EQ(*(sort_order.value().get()), *expected_sort_order);

  // Compare snapshot
  EXPECT_EQ(metadata->current_snapshot_id, 3055729675574597004);
  auto snapshot = metadata->Snapshot();
  ASSERT_TRUE(snapshot.has_value());
  EXPECT_EQ(snapshot.value()->snapshot_id, 3055729675574597004);

  // Compare snapshots
  std::vector<Snapshot> expected_snapshots{
      {
          .snapshot_id = 3051729675574597004,
          .sequence_number = 0,
          .timestamp_ms = TimePointMsFromUnixMs(1515100955770).value(),
          .manifest_list = "s3://a/b/1.avro",
          .summary = {{"operation", "append"}},
      },
      {
          .snapshot_id = 3055729675574597004,
          .parent_snapshot_id = 3051729675574597004,
          .sequence_number = 1,
          .timestamp_ms = TimePointMsFromUnixMs(1555100955770).value(),
          .manifest_list = "s3://a/b/2.avro",
          .summary = {{"operation", "append"}},
          .schema_id = 1,
      }};
  EXPECT_EQ(metadata->snapshots.size(), expected_snapshots.size());
  for (size_t i = 0; i < expected_snapshots.size(); ++i) {
    EXPECT_EQ(*metadata->snapshots[i], expected_snapshots[i]);
  }

  // snapshot with ID 3051729675574597004
  auto snapshot_v0 = metadata->SnapshotById(3051729675574597004);
  ASSERT_TRUE(snapshot_v0.has_value());
  EXPECT_EQ(*snapshot_v0.value(), expected_snapshots[0]);

  // snapshot with ID 3055729675574597004
  auto snapshot_v1 = metadata->SnapshotById(3055729675574597004);
  ASSERT_TRUE(snapshot_v1.has_value());
  EXPECT_EQ(*snapshot_v1.value(), expected_snapshots[1]);

  // Compare snapshot logs
  std::vector<SnapshotLogEntry> expected_snapshot_log{
      {
          .timestamp_ms = TimePointMsFromUnixMs(1515100955770).value(),
          .snapshot_id = 3051729675574597004,
      },
      {
          .timestamp_ms = TimePointMsFromUnixMs(1555100955770).value(),
          .snapshot_id = 3055729675574597004,
      }};
  EXPECT_EQ(metadata->snapshot_log.size(), 2);
  for (size_t i = 0; i < expected_snapshots.size(); ++i) {
    EXPECT_EQ(metadata->snapshot_log[i], expected_snapshot_log[i]);
  }
}

}  // namespace iceberg
