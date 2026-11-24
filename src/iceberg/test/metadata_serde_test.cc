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

#include <cstdint>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/expression/literal.h"
#include "iceberg/json_serde_internal.h"
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

nlohmann::json HistoricalSortOrderWithDroppedFieldMetadataJson(
    int32_t default_sort_order_id) {
  nlohmann::json metadata_json = R"({
    "format-version": 2,
    "table-uuid": "test-uuid-1234",
    "location": "s3://bucket/test",
    "last-sequence-number": 0,
    "last-updated-ms": 0,
    "last-column-id": 2,
    "schemas": [
      {
        "type": "struct",
        "schema-id": 1,
        "fields": [
          {"id": 1, "name": "id", "type": "int", "required": true}
        ]
      }
    ],
    "current-schema-id": 1,
    "partition-specs": [{"spec-id": 0, "fields": []}],
    "default-spec-id": 0,
    "last-partition-id": 999,
    "sort-orders": [
      {"order-id": 1, "fields": [
        {"transform": "identity", "source-id": 1, "direction": "asc", "null-order": "nulls-first"},
        {"transform": "identity", "source-id": 2, "direction": "asc", "null-order": "nulls-first"}
      ]},
      {"order-id": 2, "fields": [
        {"transform": "identity", "source-id": 1, "direction": "asc", "null-order": "nulls-first"}
      ]}
    ],
    "properties": {},
    "current-snapshot-id": null,
    "refs": {},
    "snapshots": [],
    "statistics": [],
    "partition-statistics": [],
    "snapshot-log": [],
    "metadata-log": []
  })"_json;
  metadata_json["default-sort-order-id"] = default_sort_order_id;
  return metadata_json;
}

nlohmann::json V4MetadataJson() {
  std::ifstream file(GetResourcePath("TableMetadataV4ValidMinimal.json"));
  return nlohmann::json::parse(file);
}

nlohmann::json RootManifestSnapshotJson() {
  return nlohmann::json::parse(R"({
    "snapshot-id": 1,
    "sequence-number": 1,
    "timestamp-ms": 1000,
    "root-manifest": "metadata/root.avro",
    "summary": {"operation": "append"},
    "schema-id": 0,
    "first-row-id": 0,
    "added-rows": 10
  })");
}

}  // namespace

TEST(MetadataSerdeTest, DeserializeV1Valid) {
  ICEBERG_UNWRAP_OR_FAIL(auto metadata,
                         ReadTableMetadataFromResource("TableMetadataV1Valid.json"));

  auto expected_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "x", int64()),
                               SchemaField::MakeRequired(2, "y", int64(), "comment"),
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
      .last_updated_ms = TimePointMsFromUnixMs(1602638573874),
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

  ICEBERG_UNWRAP_OR_FAIL(
      std::shared_ptr<Schema> expected_schema_2,
      Schema::Make(
          std::vector<SchemaField>{SchemaField::MakeRequired(1, "x", int64()),
                                   SchemaField::MakeRequired(2, "y", int64(), "comment"),
                                   SchemaField::MakeRequired(3, "z", int64())},
          /*schema_id=*/1,
          /*identifier_field_ids=*/std::vector<int32_t>{1, 2}));

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
      .timestamp_ms = TimePointMsFromUnixMs(1515100955770),
      .manifest_list = "s3://a/b/1.avro",
      .summary = {{"operation", "append"}},
  });

  auto expected_snapshot_2 = std::make_shared<Snapshot>(Snapshot{
      .snapshot_id = 3055729675574597004,
      .parent_snapshot_id = 3051729675574597004,
      .sequence_number = 1,
      .timestamp_ms = TimePointMsFromUnixMs(1555100955770),
      .manifest_list = "s3://a/b/2.avro",
      .summary = {{"operation", "append"}},
      .schema_id = 1,
  });

  TableMetadata expected{
      .format_version = 2,
      .table_uuid = "9c12d441-03fe-4693-9a96-a0705ddf69c1",
      .location = "s3://bucket/test/location",
      .last_sequence_number = 34,
      .last_updated_ms = TimePointMsFromUnixMs(1602638573590),
      .last_column_id = 3,
      .schemas = {expected_schema_1, expected_schema_2},
      .current_schema_id = 1,
      .partition_specs = {expected_spec},
      .default_spec_id = 0,
      .last_partition_id = 1000,
      .current_snapshot_id = 3055729675574597004,
      .snapshots = {expected_snapshot_1, expected_snapshot_2},
      .snapshot_log = {SnapshotLogEntry{
                           .timestamp_ms = TimePointMsFromUnixMs(1515100955770),
                           .snapshot_id = 3051729675574597004},
                       SnapshotLogEntry{
                           .timestamp_ms = TimePointMsFromUnixMs(1555100955770),
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
      .last_updated_ms = TimePointMsFromUnixMs(1602638573590),
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

TEST(MetadataSerdeTest, RejectsFractionalSchemaAndDefaultSpecIds) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto metadata, ReadTableMetadataFromResource("TableMetadataV2ValidMinimal.json"));
  ICEBERG_UNWRAP_OR_FAIL(auto valid_json, ToJson(*metadata));
  ASSERT_THAT(TableMetadataFromJson(valid_json), IsOk());

  for (const auto* key : {"current-schema-id", "default-spec-id"}) {
    SCOPED_TRACE(key);
    auto invalid_json = valid_json;
    invalid_json[key] = 0.5;

    auto result = TableMetadataFromJson(invalid_json);
    EXPECT_THAT(result, IsError(ErrorKind::kJsonParseError));
    EXPECT_THAT(result, HasErrorMessage(key));
    EXPECT_THAT(result, HasErrorMessage("must be an integer"));
  }
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
      .timestamp_ms = TimePointMsFromUnixMs(1555100955770),
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
      .last_updated_ms = TimePointMsFromUnixMs(1602638573590),
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
      .last_updated_ms = TimePointMsFromUnixMs(1602638573590),
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
          .timestamp_ms = TimePointMsFromUnixMs(1555100955770),
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

TEST(MetadataSerdeTest, V3DefaultValuesRoundTrip) {
  // Full TableMetadata path: a v3 schema field's initial/write defaults parse correctly
  // (the v3 gate in Schema::Validate is satisfied) and survive a ToJson/FromJson round
  // trip. The fixture's field is `x: long` with initial-default 1 and write-default 1.
  ICEBERG_UNWRAP_OR_FAIL(
      auto metadata, ReadTableMetadataFromResource("TableMetadataV3ValidMinimal.json"));
  auto schema_result = metadata->Schema();
  ASSERT_TRUE(schema_result.has_value());
  const auto& field = schema_result.value()->fields()[0];
  ASSERT_NE(field.initial_default(), nullptr);
  EXPECT_EQ(*field.initial_default(), Literal::Long(1));
  ASSERT_NE(field.write_default(), nullptr);
  EXPECT_EQ(*field.write_default(), Literal::Long(1));

  ICEBERG_UNWRAP_OR_FAIL(auto json, ToJson(*metadata));
  ICEBERG_UNWRAP_OR_FAIL(auto reparsed, TableMetadataFromJson(json));
  EXPECT_EQ(*reparsed, *metadata);
}

TEST(MetadataSerdeTest, V4RoundTrip) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto metadata, ReadTableMetadataFromResource("TableMetadataV4ValidMinimal.json"));
  EXPECT_EQ(metadata->format_version, 4);
  EXPECT_EQ(metadata->next_row_id, 0);
  EXPECT_TRUE(metadata->snapshots.empty());
  EXPECT_FALSE(metadata->HasRootManifest());
  EXPECT_FALSE(metadata->table_uuid.empty());
  ICEBERG_UNWRAP_OR_FAIL(auto json, ToJson(*metadata));
  EXPECT_EQ(json["format-version"], 4);
  EXPECT_EQ(json["next-row-id"], 0);
  ICEBERG_UNWRAP_OR_FAIL(auto parsed, TableMetadataFromJson(json));
  EXPECT_EQ(*metadata, *parsed);
}

TEST(MetadataSerdeTest, SerializationRejectsNullSnapshots) {
  auto snapshot_json = RootManifestSnapshotJson();
  snapshot_json.erase("root-manifest");
  snapshot_json["manifest-list"] = "s3://bucket/metadata/legacy.avro";
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, SnapshotFromJson(snapshot_json));
  auto valid = std::shared_ptr<Snapshot>(std::move(snapshot));
  for (int version : {1, 2, 3, 4}) {
    SCOPED_TRACE(version);
    auto json = V4MetadataJson();
    json["format-version"] = version;
    ICEBERG_UNWRAP_OR_FAIL(auto metadata, TableMetadataFromJson(json));
    for (bool null_first : {false, true}) {
      metadata->snapshots = null_first
                                ? std::vector<std::shared_ptr<Snapshot>>{nullptr, valid}
                                : std::vector<std::shared_ptr<Snapshot>>{valid, nullptr};
      EXPECT_FALSE(metadata->HasRootManifest());
      auto result = ToJson(*metadata);
      EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
      EXPECT_THAT(result, HasErrorMessage("Cannot write null snapshot"));
    }
  }
}

TEST(MetadataSerdeTest, V4OptionalLocation) {
  for (bool explicit_null : {false, true}) {
    auto json = V4MetadataJson();
    if (explicit_null) {
      json["location"] = nullptr;
    } else {
      json.erase("location");
    }
    ICEBERG_UNWRAP_OR_FAIL(auto parsed, TableMetadataFromJson(json));
    EXPECT_TRUE(parsed->location.empty());
    ICEBERG_UNWRAP_OR_FAIL(auto serialized, ToJson(*parsed));
    EXPECT_FALSE(serialized.contains("location"));
    for (int version : {1, 2, 3}) {
      json["format-version"] = version;
      EXPECT_FALSE(TableMetadataFromJson(json).has_value());
    }
  }
}

TEST(MetadataSerdeTest, V4RejectsRelativeLocationAndMissingRequiredFields) {
  auto json = V4MetadataJson();
  json["location"] = "relative/table";
  EXPECT_THAT(TableMetadataFromJson(json), IsError(ErrorKind::kJsonParseError));
  EXPECT_THAT(TableMetadataFromJson(json), HasErrorMessage("location must be absolute"));
  ICEBERG_UNWRAP_OR_FAIL(
      auto metadata, ReadTableMetadataFromResource("TableMetadataV4ValidMinimal.json"));
  metadata->location = "relative/table";
  EXPECT_THAT(metadata->ValidateLocation(), IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(ToJson(*metadata), IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(ToJson(*metadata), HasErrorMessage("location must be absolute"));
  json = V4MetadataJson();
  json.erase("next-row-id");
  EXPECT_FALSE(TableMetadataFromJson(json).has_value());
}

TEST(MetadataSerdeTest, V4RootManifestRoundTrip) {
  auto json = V4MetadataJson();
  json["snapshots"] = nlohmann::json::array({RootManifestSnapshotJson()});
  for (int64_t sequence : {0, 1}) {
    for (int64_t next_row_id : {0, 9, 10}) {
      if (sequence == 1 && next_row_id == 10) {
        continue;
      }
      SCOPED_TRACE(sequence);
      SCOPED_TRACE(next_row_id);
      auto invalid = json;
      invalid["last-sequence-number"] = sequence;
      invalid["next-row-id"] = next_row_id;
      auto result = TableMetadataFromJson(invalid);
      EXPECT_THAT(result, IsError(ErrorKind::kJsonParseError));
      EXPECT_THAT(result, HasErrorMessage(sequence == 0 ? "last-sequence-number"
                                                        : "next-row-id"));
    }
  }
  // Counters may retain allocations from newer snapshots that have expired.
  for (int64_t sequence : {1, 2}) {
    for (int64_t next_row_id : {10, 20}) {
      SCOPED_TRACE(sequence);
      SCOPED_TRACE(next_row_id);
      json["last-sequence-number"] = sequence;
      json["next-row-id"] = next_row_id;
      ICEBERG_UNWRAP_OR_FAIL(auto metadata, TableMetadataFromJson(json));
      ASSERT_EQ(metadata->snapshots.size(), 1);
      EXPECT_TRUE(metadata->HasRootManifest());
      auto snapshot = metadata->snapshots.front();
      EXPECT_EQ(snapshot->root_manifest, "metadata/root.avro");
      EXPECT_TRUE(snapshot->manifest_list.empty());
      ICEBERG_UNWRAP_OR_FAIL(auto serialized, ToJson(*metadata));
      EXPECT_EQ(serialized["snapshots"][0], RootManifestSnapshotJson());
      EXPECT_FALSE(serialized["snapshots"][0].contains("manifest-list"));
      ICEBERG_UNWRAP_OR_FAIL(auto parsed, TableMetadataFromJson(serialized));
      EXPECT_EQ(parsed->snapshots.front()->root_manifest, snapshot->root_manifest);
      EXPECT_EQ(parsed->last_sequence_number, sequence);
      EXPECT_EQ(parsed->next_row_id, next_row_id);
    }
  }
}

TEST(MetadataSerdeTest, V4RejectsInvalidSnapshotRoots) {
  for (const auto* field :
       {"root-manifest", "sequence-number", "first-row-id", "added-rows", "summary"}) {
    auto json = RootManifestSnapshotJson();
    json.erase(field);
    auto table_json = V4MetadataJson();
    table_json["snapshots"] = nlohmann::json::array({json});
    EXPECT_FALSE(TableMetadataFromJson(table_json).has_value()) << field;
  }
}

TEST(MetadataSerdeTest, SnapshotValidationUsesTableFormatVersion) {
  for (int8_t version : {1, 2, 3, 4}) {
    SCOPED_TRACE(version);
    auto json = V4MetadataJson();
    json["format-version"] = version;
    json["last-sequence-number"] = 1;
    json["next-row-id"] = 10;
    json["snapshots"] = nlohmann::json::array({RootManifestSnapshotJson()});
    auto root = TableMetadataFromJson(json);
    if (version < TableMetadata::kMinFormatVersionRootManifest) {
      EXPECT_THAT(root, IsError(ErrorKind::kJsonParseError));
      EXPECT_THAT(
          root,
          HasErrorMessage("requires format version " +
                          std::to_string(TableMetadata::kMinFormatVersionRootManifest)));
    } else {
      EXPECT_THAT(root, IsOk());
    }
    auto legacy = RootManifestSnapshotJson();
    legacy.erase("root-manifest");
    legacy["manifest-list"] = "legacy.avro";
    json["snapshots"] = nlohmann::json::array({legacy});
    EXPECT_THAT(TableMetadataFromJson(json), IsOk());
  }
}

TEST(MetadataSerdeTest, V4RejectsWritingIdentifierFieldsWithoutMigration) {
  for (bool historical_schema : {false, true}) {
    ICEBERG_UNWRAP_OR_FAIL(
        auto metadata, ReadTableMetadataFromResource("TableMetadataV4ValidMinimal.json"));
    ICEBERG_UNWRAP_OR_FAIL(
        auto schema, Schema::Make({SchemaField::MakeRequired(1, "id", int64())},
                                  historical_schema ? 1 : 0, std::vector<int32_t>{1}));
    if (historical_schema) {
      metadata->schemas.push_back(std::move(schema));
    } else {
      metadata->schemas[0] = std::move(schema);
    }
    EXPECT_THAT(ToJson(*metadata), IsError(ErrorKind::kNotSupported));
    ASSERT_EQ(metadata->schemas.back()->IdentifierFieldIds(), std::vector<int32_t>{1});
  }
}

TEST(MetadataSerdeTest, V4IgnoresLegacyIdentifiersAndRejectsUnimplementedConstraints) {
  auto json = V4MetadataJson();
  json["schemas"][0]["identifier-field-ids"] = {999};
  ICEBERG_UNWRAP_OR_FAIL(auto metadata, TableMetadataFromJson(json));
  ICEBERG_UNWRAP_OR_FAIL(auto schema, metadata->Schema());
  EXPECT_TRUE(schema->IdentifierFieldIds().empty());
  ICEBERG_UNWRAP_OR_FAIL(auto serialized, ToJson(*metadata));
  EXPECT_FALSE(serialized["schemas"][0].contains("identifier-field-ids"));
  for (const auto* field : {"constraints", "last-constraint-id"}) {
    json = V4MetadataJson();
    json[field] = field == std::string_view("constraints")
                      ? nlohmann::json::array({{{"constraint-id", 1}}})
                      : nlohmann::json(1);
    EXPECT_THAT(TableMetadataFromJson(json), IsError(ErrorKind::kNotSupported));
  }
  for (const auto& malformed_schema : std::vector<nlohmann::json>{nullptr, 1, "schema"}) {
    json = V4MetadataJson();
    json["schemas"] = nlohmann::json::array({malformed_schema});
    EXPECT_FALSE(TableMetadataFromJson(json).has_value());
  }
}

TEST(MetadataSerdeTest, V4ConstraintFieldsRequireTheirDeclaredJsonTypes) {
  auto json = V4MetadataJson();
  json["constraints"] = nlohmann::json::array();
  json["last-constraint-id"] = 0;
  EXPECT_THAT(TableMetadataFromJson(json), IsOk());
  for (const auto* field : {"constraints", "last-constraint-id"}) {
    auto nullable = json;
    nullable[field] = nullptr;
    EXPECT_THAT(TableMetadataFromJson(nullable), IsOk());
  }
  for (const auto& value : std::vector<nlohmann::json>{"constraints", true, 0, 0.0,
                                                       nlohmann::json::object()}) {
    SCOPED_TRACE(value.dump());
    json = V4MetadataJson();
    json["constraints"] = value;
    auto result = TableMetadataFromJson(json);
    EXPECT_THAT(result, IsError(ErrorKind::kJsonParseError));
    EXPECT_THAT(result, HasErrorMessage("constraints"));
  }
  for (const auto& value : std::vector<nlohmann::json>{
           "0", true, 0.0, 1.5, 2147483648LL, -2147483649LL, 18446744073709551615ULL,
           nlohmann::json::array(), nlohmann::json::object()}) {
    SCOPED_TRACE(value.dump());
    json = V4MetadataJson();
    json["last-constraint-id"] = value;
    auto result = TableMetadataFromJson(json);
    EXPECT_THAT(result, IsError(ErrorKind::kJsonParseError));
    EXPECT_THAT(result, HasErrorMessage("last-constraint-id"));
  }
}

TEST(MetadataSerdeTest, V4RejectsIncompleteSnapshotWrites) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto metadata, ReadTableMetadataFromResource("TableMetadataV4ValidMinimal.json"));
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, SnapshotFromJson(RootManifestSnapshotJson()));
  snapshot->first_row_id.reset();
  metadata->snapshots.push_back(std::move(snapshot));
  EXPECT_THAT(ToJson(*metadata), HasErrorMessage("first-row-id"));
}

TEST(MetadataSerdeTest, MissingOrNullUuidUsesTheDefaultForEveryVersion) {
  for (int version : {1, 2, 3, 4}) {
    auto json = V4MetadataJson();
    json["format-version"] = version;
    for (bool explicit_null : {false, true}) {
      if (explicit_null) {
        json["table-uuid"] = nullptr;
      } else {
        json.erase("table-uuid");
      }
      ICEBERG_UNWRAP_OR_FAIL(auto metadata, TableMetadataFromJson(json));
      EXPECT_TRUE(metadata->table_uuid.empty());
    }
  }
}

TEST(MetadataSerdeTest, V4RootOperationValidationIsSharedAcrossMetadataPaths) {
  for (const auto& operation :
       std::vector<std::optional<std::string>>{std::nullopt, "", "invalid"}) {
    auto json = RootManifestSnapshotJson();
    if (operation.has_value()) {
      json["summary"]["operation"] = operation.value();
    } else {
      json["summary"].erase("operation");
    }
    auto table_json = V4MetadataJson();
    table_json["snapshots"] = nlohmann::json::array({json});
    EXPECT_THAT(TableMetadataFromJson(table_json), HasErrorMessage("valid operation"));

    ICEBERG_UNWRAP_OR_FAIL(auto snapshot, SnapshotFromJson(RootManifestSnapshotJson()));
    if (operation.has_value()) {
      snapshot->summary["operation"] = operation.value();
    } else {
      snapshot->summary.erase("operation");
    }
    EXPECT_THAT(snapshot->ValidateForFormatVersion(4),
                IsError(ErrorKind::kValidationFailed));
    ICEBERG_UNWRAP_OR_FAIL(
        auto base, ReadTableMetadataFromResource("TableMetadataV4ValidMinimal.json"));
    auto builder = TableMetadataBuilder::BuildFrom(base.get());
    builder->AddSnapshot(std::make_shared<Snapshot>(*snapshot));
    EXPECT_THAT(builder->Build(), HasErrorMessage("valid operation"));
    base->snapshots.push_back(std::move(snapshot));
    EXPECT_THAT(ToJson(*base), HasErrorMessage("valid operation"));
  }
}

TEST(MetadataSerdeTest, DeserializeUnsupportedVersion) {
  ReadTableMetadataExpectError("TableMetadataUnsupportedVersion.json",
                               "Cannot read unsupported version");
}

TEST(MetadataSerdeTest, DeserializeRejectsUnknownSchemaBeforeFormatV3) {
  auto v1_metadata_json = nlohmann::json::parse(R"({
    "format-version": 1,
    "location": "s3://bucket/test/location",
    "last-column-id": 1,
    "last-updated-ms": 1602638573874,
    "schema": {
      "type": "struct",
      "schema-id": 0,
      "fields": [
        {"id": 1, "name": "mystery", "type": "unknown", "required": false}
      ]
    },
    "partition-spec": []
  })");

  auto result = TableMetadataFromJson(v1_metadata_json);
  ASSERT_THAT(result, IsError(ErrorKind::kInvalidSchema));
  EXPECT_THAT(result, HasErrorMessage(
                          "Invalid type for mystery: unknown is not supported until v3"));

  auto v2_metadata_json = nlohmann::json::parse(R"({
    "format-version": 2,
    "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
    "location": "s3://bucket/test/location",
    "last-sequence-number": 0,
    "last-column-id": 1,
    "last-updated-ms": 1602638573874,
    "schemas": [
      {
        "type": "struct",
        "schema-id": 0,
        "fields": [
          {"id": 1, "name": "mystery", "type": "unknown", "required": false}
        ]
      }
    ],
    "current-schema-id": 0,
    "partition-specs": [{"spec-id": 0, "fields": []}],
    "default-spec-id": 0,
    "last-partition-id": 999,
    "sort-orders": [{"order-id": 0, "fields": []}],
    "default-sort-order-id": 0
  })");

  result = TableMetadataFromJson(v2_metadata_json);
  ASSERT_THAT(result, IsError(ErrorKind::kInvalidSchema));
  EXPECT_THAT(result, HasErrorMessage(
                          "Invalid type for mystery: unknown is not supported until v3"));
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

TEST(MetadataSerdeTest, DeserializeHistoricalSortOrderWithDroppedField) {
  auto metadata =
      TableMetadataFromJson(HistoricalSortOrderWithDroppedFieldMetadataJson(2));
  ASSERT_THAT(metadata, IsOk());
  ASSERT_EQ(metadata.value()->sort_orders.size(), 2);
  EXPECT_EQ(metadata.value()->sort_orders[0]->order_id(), 1);
  ASSERT_EQ(metadata.value()->sort_orders[0]->fields().size(), 2);
  EXPECT_EQ(metadata.value()->sort_orders[0]->fields()[0].source_id(), 1);
  EXPECT_EQ(metadata.value()->sort_orders[0]->fields()[1].source_id(), 2);
  EXPECT_EQ(metadata.value()->sort_orders[1]->order_id(), 2);
}

TEST(MetadataSerdeTest, DeserializeDefaultSortOrderWithDroppedFieldFails) {
  auto metadata =
      TableMetadataFromJson(HistoricalSortOrderWithDroppedFieldMetadataJson(1));
  ASSERT_THAT(metadata, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(metadata, HasErrorMessage("Cannot find source column for sort field"));
}

TEST(MetadataSerdeTest, EncryptionKeysRoundTrip) {
  nlohmann::json metadata_json = R"({
    "format-version": 2,
    "table-uuid": "test-uuid-1234",
    "location": "s3://bucket/test",
    "last-sequence-number": 0,
    "last-updated-ms": 0,
    "last-column-id": 1,
    "schemas": [
      {
        "type": "struct",
        "schema-id": 0,
        "fields": [
          {"id": 1, "name": "id", "type": "int", "required": true}
        ]
      }
    ],
    "current-schema-id": 0,
    "partition-specs": [{"spec-id": 0, "fields": []}],
    "default-spec-id": 0,
    "last-partition-id": 999,
    "sort-orders": [{"order-id": 0, "fields": []}],
    "default-sort-order-id": 0,
    "properties": {},
    "current-snapshot-id": null,
    "refs": {},
    "snapshots": [],
    "statistics": [],
    "partition-statistics": [],
    "snapshot-log": [],
    "metadata-log": [],
    "encryption-keys": [
      {
        "key-id": "key-1",
        "encrypted-key-metadata": "c2VjcmV0LWtleS1tZXRhZGF0YQ==",
        "encrypted-by-id": "kek-1",
        "properties": {"scope": "table"}
      }
    ]
  })"_json;

  auto metadata = TableMetadataFromJson(metadata_json);
  ASSERT_THAT(metadata, IsOk());
  ASSERT_EQ(metadata.value()->encryption_keys.size(), 1);
  EXPECT_EQ(metadata.value()->encryption_keys[0].key_id, "key-1");
  EXPECT_EQ(metadata.value()->encryption_keys[0].encrypted_key_metadata,
            "secret-key-metadata");

  ICEBERG_UNWRAP_OR_FAIL(auto serialized, ToJson(*metadata.value()));
  ASSERT_TRUE(serialized.contains("encryption-keys"));
  EXPECT_EQ(serialized["encryption-keys"], metadata_json["encryption-keys"]);
}

}  // namespace iceberg
