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

#include <optional>
#include <string>

#include <gtest/gtest.h>

#include "iceberg/partition_field.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/snapshot.h"
#include "iceberg/sort_field.h"
#include "iceberg/sort_order.h"
#include "iceberg/statistics_file.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/test_resource.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"

namespace iceberg {

namespace {

void ReadTableMetadataExpectError(const std::string& file_name,
                                  const std::string& expected_error_substr) {
  auto result = ReadTableMetadataFromResource(file_name);
  ASSERT_FALSE(result.has_value()) << "Expected parsing to fail for " << file_name;
  EXPECT_THAT(result, HasErrorMessage(expected_error_substr));
}

void AssertSchema(const TableMetadata& metadata, const Schema& expected_schema) {
  auto schema = metadata.Schema();
  ASSERT_TRUE(schema.has_value());
  EXPECT_EQ(*(schema.value().get()), expected_schema);
}

void AssertSchemaById(const TableMetadata& metadata, int32_t schema_id,
                      const Schema& expected_schema) {
  auto schema = metadata.SchemaById(schema_id);
  ASSERT_TRUE(schema.has_value());
  EXPECT_EQ(*(schema.value().get()), expected_schema);
}

void AssertPartitionSpec(const TableMetadata& metadata,
                         const PartitionSpec& expected_spec) {
  auto partition_spec = metadata.PartitionSpec();
  ASSERT_TRUE(partition_spec.has_value());
  EXPECT_EQ(*(partition_spec.value().get()), expected_spec);
}

void AssertSortOrder(const TableMetadata& metadata,
                     const SortOrder& expected_sort_order) {
  auto sort_order = metadata.SortOrder();
  ASSERT_TRUE(sort_order.has_value());
  EXPECT_EQ(*(sort_order.value().get()), expected_sort_order);
}

void AssertSnapshot(const TableMetadata& metadata, const Snapshot& expected_snapshot) {
  auto snapshot = metadata.Snapshot();
  ASSERT_TRUE(snapshot.has_value());
  EXPECT_EQ(*snapshot.value(), expected_snapshot);
}

void AssertSnapshotById(const TableMetadata& metadata, int64_t snapshot_id,
                        const Snapshot& expected_snapshot) {
  auto snapshot = metadata.SnapshotById(snapshot_id);
  ASSERT_TRUE(snapshot.has_value());
  EXPECT_EQ(*snapshot.value(), expected_snapshot);
}

}  // namespace

TEST(MetadataSerdeTest, DeserializeV1Valid) {
  ICEBERG_UNWRAP_OR_FAIL(auto metadata,
                         ReadTableMetadataFromResource("TableMetadataV1Valid.json"));

  auto expected_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "x", int64()),
                               SchemaField::MakeRequired(2, "y", int64()),
                               SchemaField::MakeRequired(3, "z", int64())});

  auto expected_spec_result = PartitionSpec::Make(
      /*spec_id=*/0,
      std::vector<PartitionField>{PartitionField(/*source_id=*/1, /*field_id=*/1000, "x",
                                                 Transform::Identity())});
  ASSERT_TRUE(expected_spec_result.has_value());
  auto expected_spec =
      std::shared_ptr<PartitionSpec>(std::move(expected_spec_result.value()));

  TableMetadata expected{
      .format_version = 1,
      .table_uuid = "d20125c8-7284-442c-9aea-15fee620737c",
      .location = "s3://bucket/test/location",
      .last_sequence_number = 0,
      .last_updated_ms = TimePointMsFromUnixMs(1602638573874).value(),
      .last_column_id = 3,
      .schemas = {expected_schema},
      .current_schema_id = Schema::kInitialSchemaId,
      .partition_specs = {expected_spec},
      .default_spec_id = 0,
      .last_partition_id = 1000,
      .current_snapshot_id = -1,
      .sort_orders = {SortOrder::Unsorted()},
      .default_sort_order_id = 0,
      .next_row_id = 0,
  };

  ASSERT_EQ(*metadata, expected);
  AssertSchema(*metadata, *expected_schema);
  AssertPartitionSpec(*metadata, *expected_spec);
  ASSERT_FALSE(metadata->Snapshot().has_value());
}

TEST(MetadataSerdeTest, DeserializeV2Valid) {
  ICEBERG_UNWRAP_OR_FAIL(auto metadata,
                         ReadTableMetadataFromResource("TableMetadataV2Valid.json"));

  auto expected_schema_1 = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField(/*field_id=*/1, "x", int64(),
                                           /*optional=*/false)},
      /*schema_id=*/0);

  auto expected_schema_2 = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "x", int64()),
                               SchemaField::MakeRequired(2, "y", int64()),
                               SchemaField::MakeRequired(3, "z", int64())},
      /*schema_id=*/1,
      /*identifier_field_ids=*/std::vector<int32_t>{1, 2});

  auto expected_spec_result = PartitionSpec::Make(
      /*spec_id=*/0,
      std::vector<PartitionField>{PartitionField(/*source_id=*/1, /*field_id=*/1000, "x",
                                                 Transform::Identity())});
  ASSERT_TRUE(expected_spec_result.has_value());
  auto expected_spec =
      std::shared_ptr<PartitionSpec>(std::move(expected_spec_result.value()));

  ICEBERG_UNWRAP_OR_FAIL(
      auto sort_order,
      SortOrder::Make(*expected_schema_2,
                      /*order_id=*/3,
                      std::vector<SortField>{
                          SortField(/*source_id=*/2, Transform::Identity(),
                                    SortDirection::kAscending, NullOrder::kFirst),
                          SortField(/*source_id=*/3, Transform::Bucket(4),
                                    SortDirection::kDescending, NullOrder::kLast)}));

  auto expected_sort_order = std::shared_ptr<SortOrder>(std::move(sort_order));

  auto expected_snapshot_1 = std::make_shared<Snapshot>(Snapshot{
      .snapshot_id = 3051729675574597004,
      .sequence_number = 0,
      .timestamp_ms = TimePointMsFromUnixMs(1515100955770).value(),
      .manifest_list = "s3://a/b/1.avro",
      .summary = {{"operation", "append"}},
  });

  auto expected_snapshot_2 = std::make_shared<Snapshot>(Snapshot{
      .snapshot_id = 3055729675574597004,
      .parent_snapshot_id = 3051729675574597004,
      .sequence_number = 1,
      .timestamp_ms = TimePointMsFromUnixMs(1555100955770).value(),
      .manifest_list = "s3://a/b/2.avro",
      .summary = {{"operation", "append"}},
      .schema_id = 1,
  });

  TableMetadata expected{
      .format_version = 2,
      .table_uuid = "9c12d441-03fe-4693-9a96-a0705ddf69c1",
      .location = "s3://bucket/test/location",
      .last_sequence_number = 34,
      .last_updated_ms = TimePointMsFromUnixMs(1602638573590).value(),
      .last_column_id = 3,
      .schemas = {expected_schema_1, expected_schema_2},
      .current_schema_id = 1,
      .partition_specs = {expected_spec},
      .default_spec_id = 0,
      .last_partition_id = 1000,
      .current_snapshot_id = 3055729675574597004,
      .snapshots = {expected_snapshot_1, expected_snapshot_2},
      .snapshot_log = {SnapshotLogEntry{
                           .timestamp_ms = TimePointMsFromUnixMs(1515100955770).value(),
                           .snapshot_id = 3051729675574597004},
                       SnapshotLogEntry{
                           .timestamp_ms = TimePointMsFromUnixMs(1555100955770).value(),
                           .snapshot_id = 3055729675574597004}},
      .sort_orders = {expected_sort_order},
      .default_sort_order_id = 3,
      .refs = {{"main", std::make_shared<SnapshotRef>(
                            SnapshotRef{.snapshot_id = 3055729675574597004,
                                        .retention = SnapshotRef::Branch{}})}},
      .next_row_id = 0,
  };

  ASSERT_EQ(*metadata, expected);
  AssertSchema(*metadata, *expected_schema_2);
  AssertSchemaById(*metadata, 0, *expected_schema_1);
  AssertSchemaById(*metadata, 1, *expected_schema_2);
  AssertPartitionSpec(*metadata, *expected_spec);
  AssertSortOrder(*metadata, *expected_sort_order);
  AssertSnapshot(*metadata, *expected_snapshot_2);
  AssertSnapshotById(*metadata, 3051729675574597004, *expected_snapshot_1);
  AssertSnapshotById(*metadata, 3055729675574597004, *expected_snapshot_2);
}

TEST(MetadataSerdeTest, DeserializeV2ValidMinimal) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto metadata, ReadTableMetadataFromResource("TableMetadataV2ValidMinimal.json"));

  auto expected_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "x", int64()),
                               SchemaField::MakeRequired(2, "y", int64(), "comment"),
                               SchemaField::MakeRequired(3, "z", int64())},
      /*schema_id=*/0);

  auto expected_spec_result = PartitionSpec::Make(
      /*spec_id=*/0,
      std::vector<PartitionField>{PartitionField(/*source_id=*/1, /*field_id=*/1000, "x",
                                                 Transform::Identity())});
  ASSERT_TRUE(expected_spec_result.has_value());
  auto expected_spec =
      std::shared_ptr<PartitionSpec>(std::move(expected_spec_result.value()));

  ICEBERG_UNWRAP_OR_FAIL(
      auto sort_order,
      SortOrder::Make(*expected_schema,
                      /*order_id=*/3,
                      std::vector<SortField>{
                          SortField(/*source_id=*/2, Transform::Identity(),
                                    SortDirection::kAscending, NullOrder::kFirst),
                          SortField(/*source_id=*/3, Transform::Bucket(4),
                                    SortDirection::kDescending, NullOrder::kLast),
                      }));

  auto expected_sort_order = std::shared_ptr<SortOrder>(std::move(sort_order));

  TableMetadata expected{
      .format_version = 2,
      .table_uuid = "9c12d441-03fe-4693-9a96-a0705ddf69c1",
      .location = "s3://bucket/test/location",
      .last_sequence_number = 34,
      .last_updated_ms = TimePointMsFromUnixMs(1602638573590).value(),
      .last_column_id = 3,
      .schemas = {expected_schema},
      .current_schema_id = 0,
      .partition_specs = {expected_spec},
      .default_spec_id = 0,
      .last_partition_id = 1000,
      .current_snapshot_id = -1,
      .sort_orders = {expected_sort_order},
      .default_sort_order_id = 3,
      .next_row_id = 0,
  };

  ASSERT_EQ(*metadata, expected);
  AssertSchema(*metadata, *expected_schema);
  AssertPartitionSpec(*metadata, *expected_spec);
  AssertSortOrder(*metadata, *expected_sort_order);
  ASSERT_FALSE(metadata->Snapshot().has_value());
}

TEST(MetadataSerdeTest, DeserializeStatisticsFiles) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto metadata, ReadTableMetadataFromResource("TableMetadataStatisticsFiles.json"));

  auto expected_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField(/*field_id=*/1, "x", int64(),
                                           /*optional=*/false)},
      /*schema_id=*/0);

  auto expected_spec_result =
      PartitionSpec::Make(/*spec_id=*/0, std::vector<PartitionField>{});
  ASSERT_TRUE(expected_spec_result.has_value());
  auto expected_spec =
      std::shared_ptr<PartitionSpec>(std::move(expected_spec_result.value()));

  auto expected_snapshot = std::make_shared<Snapshot>(Snapshot{
      .snapshot_id = 3055729675574597004,
      .sequence_number = 1,
      .timestamp_ms = TimePointMsFromUnixMs(1555100955770).value(),
      .manifest_list = "s3://a/b/2.avro",
      .summary = {{"operation", "append"}},
      .schema_id = 0,
  });

  auto expected_stats_file = std::make_shared<StatisticsFile>(StatisticsFile{
      .snapshot_id = 3055729675574597004,
      .path = "s3://a/b/stats.puffin",
      .file_size_in_bytes = 413,
      .file_footer_size_in_bytes = 42,
      .blob_metadata =
          {
              BlobMetadata{
                  .type = "ndv",
                  .source_snapshot_id = 3055729675574597004,
                  .source_snapshot_sequence_number = 1,
                  .fields = {1},
                  .properties = {},
              },
          },
  });

  TableMetadata expected{
      .format_version = 2,
      .table_uuid = "9c12d441-03fe-4693-9a96-a0705ddf69c1",
      .location = "s3://bucket/test/location",
      .last_sequence_number = 34,
      .last_updated_ms = TimePointMsFromUnixMs(1602638573590).value(),
      .last_column_id = 3,
      .schemas = {expected_schema},
      .current_schema_id = 0,
      .partition_specs = {expected_spec},
      .default_spec_id = 0,
      .last_partition_id = 1000,
      .properties = {},
      .current_snapshot_id = 3055729675574597004,
      .snapshots = {expected_snapshot},
      .snapshot_log = {},
      .metadata_log = {},
      .sort_orders = {SortOrder::Unsorted()},
      .default_sort_order_id = 0,
      .refs = {{"main", std::make_shared<SnapshotRef>(
                            SnapshotRef{.snapshot_id = 3055729675574597004,
                                        .retention = SnapshotRef::Branch{}})}},
      .statistics = {expected_stats_file},
      .partition_statistics = {},
      .next_row_id = 0,
  };

  ASSERT_EQ(*metadata, expected);
}

TEST(MetadataSerdeTest, DeserializePartitionStatisticsFiles) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto metadata,
      ReadTableMetadataFromResource("TableMetadataPartitionStatisticsFiles.json"));

  TableMetadata expected{
      .format_version = 2,
      .table_uuid = "9c12d441-03fe-4693-9a96-a0705ddf69c1",
      .location = "s3://bucket/test/location",
      .last_sequence_number = 34,
      .last_updated_ms = TimePointMsFromUnixMs(1602638573590).value(),
      .last_column_id = 3,
      .schemas = {std::make_shared<Schema>(
          std::vector<SchemaField>{SchemaField(/*field_id=*/1, "x", int64(),
                                               /*optional=*/false)},
          /*schema_id=*/0)},
      .current_schema_id = 0,
      .partition_specs = {PartitionSpec::Unpartitioned()},
      .default_spec_id = 0,
      .last_partition_id = 1000,
      .properties = {},
      .current_snapshot_id = 3055729675574597004,
      .snapshots = {std::make_shared<Snapshot>(Snapshot{
          .snapshot_id = 3055729675574597004,
          .sequence_number = 1,
          .timestamp_ms = TimePointMsFromUnixMs(1555100955770).value(),
          .manifest_list = "s3://a/b/2.avro",
          .summary = {{"operation", "append"}},
          .schema_id = 0,
      })},
      .snapshot_log = {},
      .metadata_log = {},
      .sort_orders = {SortOrder::Unsorted()},
      .default_sort_order_id = 0,
      .refs = {{"main", std::make_shared<SnapshotRef>(
                            SnapshotRef{.snapshot_id = 3055729675574597004,
                                        .retention = SnapshotRef::Branch{}})}},
      .statistics = {},
      .partition_statistics = {std::make_shared<PartitionStatisticsFile>(
          PartitionStatisticsFile{.snapshot_id = 3055729675574597004,
                                  .path = "s3://a/b/partition-stats.parquet",
                                  .file_size_in_bytes = 43})},
      .next_row_id = 0,
  };

  ASSERT_EQ(*metadata, expected);
}

TEST(MetadataSerdeTest, DeserializeUnsupportedVersion) {
  ReadTableMetadataExpectError("TableMetadataUnsupportedVersion.json",
                               "Cannot read unsupported version");
}

TEST(MetadataSerdeTest, DeserializeV1MissingSchemaType) {
  ReadTableMetadataExpectError("TableMetadataV1MissingSchemaType.json", "Missing 'type'");
}

TEST(MetadataSerdeTest, DeserializeV2CurrentSchemaNotFound) {
  ReadTableMetadataExpectError("TableMetadataV2CurrentSchemaNotFound.json",
                               "Cannot find schema with current-schema-id");
}

TEST(MetadataSerdeTest, DeserializeV2MissingLastPartitionId) {
  ReadTableMetadataExpectError("TableMetadataV2MissingLastPartitionId.json",
                               "last-partition-id must exist");
}

TEST(MetadataSerdeTest, DeserializeV2MissingPartitionSpecs) {
  ReadTableMetadataExpectError("TableMetadataV2MissingPartitionSpecs.json",
                               "partition-specs must exist");
}

TEST(MetadataSerdeTest, DeserializeV2MissingSchemas) {
  ReadTableMetadataExpectError("TableMetadataV2MissingSchemas.json",
                               "schemas must exist");
}

TEST(MetadataSerdeTest, DeserializeV2MissingSortOrder) {
  ReadTableMetadataExpectError("TableMetadataV2MissingSortOrder.json",
                               "sort-orders must exist");
}

}  // namespace iceberg
