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

#include "iceberg/json_internal.h"

#include <memory>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/name_mapping.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/sort_field.h"
#include "iceberg/sort_order.h"
#include "iceberg/table_requirement.h"
#include "iceberg/table_update.h"
#include "iceberg/test/matchers.h"
#include "iceberg/transform.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep
#include "iceberg/util/macros.h"     // IWYU pragma: keep
#include "iceberg/util/timepoint.h"

namespace iceberg {

namespace {
// Specialized FromJson helper based on type
template <typename T>
Result<std::unique_ptr<T>> FromJsonHelper(const nlohmann::json& json);

template <>
Result<std::unique_ptr<SortField>> FromJsonHelper(const nlohmann::json& json) {
  return SortFieldFromJson(json);
}

template <>
Result<std::unique_ptr<PartitionField>> FromJsonHelper(const nlohmann::json& json) {
  return PartitionFieldFromJson(json);
}

template <>
Result<std::unique_ptr<SnapshotRef>> FromJsonHelper(const nlohmann::json& json) {
  return SnapshotRefFromJson(json);
}

template <>
Result<std::unique_ptr<Snapshot>> FromJsonHelper(const nlohmann::json& json) {
  return SnapshotFromJson(json);
}

template <>
Result<std::unique_ptr<NameMapping>> FromJsonHelper(const nlohmann::json& json) {
  return NameMappingFromJson(json);
}

// Helper function to reduce duplication in testing
template <typename T>
void TestJsonConversion(const T& obj, const nlohmann::json& expected_json) {
  auto json = ToJson(obj);
  EXPECT_EQ(expected_json, json) << "JSON conversion mismatch.";

  // Specialize FromJson based on type (T)
  auto obj_ex = FromJsonHelper<T>(expected_json);
  EXPECT_TRUE(obj_ex.has_value()) << "Failed to deserialize JSON.";
  EXPECT_EQ(obj, *obj_ex.value()) << "Deserialized object mismatch.";
}

}  // namespace

TEST(JsonInternalTest, SortField) {
  auto identity_transform = Transform::Identity();

  // Test for SortField with ascending order
  SortField sort_field_asc(5, identity_transform, SortDirection::kAscending,
                           NullOrder::kFirst);
  nlohmann::json expected_asc =
      R"({"transform":"identity","source-id":5,"direction":"asc","null-order":"nulls-first"})"_json;
  TestJsonConversion(sort_field_asc, expected_asc);

  // Test for SortField with descending order
  SortField sort_field_desc(7, identity_transform, SortDirection::kDescending,
                            NullOrder::kLast);
  nlohmann::json expected_desc =
      R"({"transform":"identity","source-id":7,"direction":"desc","null-order":"nulls-last"})"_json;
  TestJsonConversion(sort_field_desc, expected_desc);
}

TEST(JsonInternalTest, SortOrder) {
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField(5, "region", string(), false),
                               SchemaField(7, "ts", int64(), false)},
      /*schema_id=*/100);
  auto identity_transform = Transform::Identity();
  SortField st_ts(5, identity_transform, SortDirection::kAscending, NullOrder::kFirst);
  SortField st_bar(7, identity_transform, SortDirection::kDescending, NullOrder::kLast);
  ICEBERG_UNWRAP_OR_FAIL(auto sort_order, SortOrder::Make(*schema, 100, {st_ts, st_bar}));
  nlohmann::json expected_sort_order =
      R"({"order-id":100,"fields":[
          {"transform":"identity","source-id":5,"direction":"asc","null-order":"nulls-first"},
          {"transform":"identity","source-id":7,"direction":"desc","null-order":"nulls-last"}]})"_json;

  auto json = ToJson(*sort_order);
  EXPECT_EQ(expected_sort_order, json) << "JSON conversion mismatch.";

  // Specialize FromJson based on type (T)
  ICEBERG_UNWRAP_OR_FAIL(auto obj_ex, SortOrderFromJson(expected_sort_order, schema));
  EXPECT_EQ(*sort_order, *obj_ex) << "Deserialized object mismatch.";
}

TEST(JsonInternalTest, PartitionField) {
  auto identity_transform = Transform::Identity();
  PartitionField field(3, 101, "region", identity_transform);
  nlohmann::json expected_json =
      R"({"source-id":3,"field-id":101,"transform":"identity","name":"region"})"_json;
  TestJsonConversion(field, expected_json);
}

TEST(JsonInternalTest, PartitionFieldFromJsonMissingField) {
  nlohmann::json invalid_json =
      R"({"field-id":101,"transform":"identity","name":"region"})"_json;
  // missing source-id

  auto result = PartitionFieldFromJson(invalid_json);
  EXPECT_FALSE(result.has_value());
  EXPECT_THAT(result, IsError(ErrorKind::kJsonParseError));
  EXPECT_THAT(result, HasErrorMessage("Missing 'source-id'"));
}

TEST(JsonInternalTest, PartitionSpec) {
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField(3, "region", string(), false),
                               SchemaField(5, "ts", int64(), false)},
      /*schema_id=*/100);
  auto identity_transform = Transform::Identity();
  ICEBERG_UNWRAP_OR_FAIL(
      auto spec,
      PartitionSpec::Make(*schema, 1,
                          {PartitionField(3, 101, "region", identity_transform),
                           PartitionField(5, 102, "ts", identity_transform)},
                          false));
  auto json = ToJson(*spec);
  nlohmann::json expected_json = R"({"spec-id": 1,
                                     "fields": [
                                       {"source-id": 3,
                                        "field-id": 101,
                                        "transform": "identity",
                                        "name": "region"},
                                       {"source-id": 5,
                                        "field-id": 102,
                                        "transform": "identity",
                                        "name": "ts"}]})"_json;

  EXPECT_EQ(json, expected_json);

  auto parsed_spec_result = PartitionSpecFromJson(schema, json, 1);
  ASSERT_TRUE(parsed_spec_result.has_value()) << parsed_spec_result.error().message;
  EXPECT_EQ(*spec, *parsed_spec_result.value());
}

TEST(JsonInternalTest, SnapshotRefBranch) {
  SnapshotRef ref(1234567890, SnapshotRef::Branch{.min_snapshots_to_keep = 10,
                                                  .max_snapshot_age_ms = 123456789,
                                                  .max_ref_age_ms = 987654321});

  // Create a JSON object with the expected values
  nlohmann::json expected_json =
      R"({"snapshot-id":1234567890,
          "type":"branch",
          "min-snapshots-to-keep":10,
          "max-snapshot-age-ms":123456789,
          "max-ref-age-ms":987654321})"_json;

  TestJsonConversion(ref, expected_json);
}

TEST(JsonInternalTest, SnapshotRefTag) {
  SnapshotRef ref(9876543210, SnapshotRef::Tag{.max_ref_age_ms = 54321});

  // Create a JSON object with the expected values
  nlohmann::json expected_json =
      R"({"snapshot-id":9876543210,
          "type":"tag",
          "max-ref-age-ms":54321})"_json;

  TestJsonConversion(ref, expected_json);
}

TEST(JsonInternalTest, Snapshot) {
  std::unordered_map<std::string, std::string> summary = {
      {SnapshotSummaryFields::kOperation, DataOperation::kAppend},
      {SnapshotSummaryFields::kAddedDataFiles, "50"}};

  Snapshot snapshot{.snapshot_id = 1234567890,
                    .parent_snapshot_id = 9876543210,
                    .sequence_number = 99,
                    .timestamp_ms = TimePointMsFromUnixMs(1234567890123).value(),
                    .manifest_list = "/path/to/manifest_list",
                    .summary = summary,
                    .schema_id = 42};

  // Create a JSON object with the expected values
  nlohmann::json expected_json =
      R"({"snapshot-id":1234567890,
          "parent-snapshot-id":9876543210,
          "sequence-number":99,
          "timestamp-ms":1234567890123,
          "manifest-list":"/path/to/manifest_list",
          "summary":{
            "operation":"append",
            "added-data-files":"50"
          },
          "schema-id":42})"_json;

  TestJsonConversion(snapshot, expected_json);
}

// FIXME: disable it for now since Iceberg Spark plugin generates
// custom summary keys.
TEST(JsonInternalTest, DISABLED_SnapshotFromJsonWithInvalidSummary) {
  nlohmann::json invalid_json =
      R"({"snapshot-id":1234567890,
          "parent-snapshot-id":9876543210,
          "sequence-number":99,
          "timestamp-ms":1234567890123,
          "manifest-list":"/path/to/manifest_list",
          "summary":{
            "invalid-field":"value"
          },
          "schema-id":42})"_json;
  // malformed summary field

  auto result = SnapshotFromJson(invalid_json);
  ASSERT_FALSE(result.has_value());

  EXPECT_THAT(result, IsError(ErrorKind::kJsonParseError));
  EXPECT_THAT(result, HasErrorMessage("Invalid snapshot summary field"));
}

TEST(JsonInternalTest, SnapshotFromJsonSummaryWithNoOperation) {
  nlohmann::json snapshot_json =
      R"({"snapshot-id":1234567890,
          "parent-snapshot-id":9876543210,
          "sequence-number":99,
          "timestamp-ms":1234567890123,
          "manifest-list":"/path/to/manifest_list",
          "summary":{
            "added-data-files":"50"
          },
          "schema-id":42})"_json;

  auto result = SnapshotFromJson(snapshot_json);
  ASSERT_TRUE(result.has_value());

  ASSERT_EQ(result.value()->operation(), DataOperation::kOverwrite);
}

TEST(JsonInternalTest, NameMapping) {
  auto mapping = NameMapping::Make(
      {MappedField{.names = {"id"}, .field_id = 1},
       MappedField{.names = {"data"}, .field_id = 2},
       MappedField{.names = {"location"},
                   .field_id = 3,
                   .nested_mapping = MappedFields::Make(
                       {MappedField{.names = {"latitude"}, .field_id = 4},
                        MappedField{.names = {"longitude"}, .field_id = 5}})}});

  nlohmann::json expected_json =
      R"([
        {"field-id": 1, "names": ["id"]},
        {"field-id": 2, "names": ["data"]},
        {"field-id": 3, "names": ["location"], "fields": [
          {"field-id": 4, "names": ["latitude"]},
          {"field-id": 5, "names": ["longitude"]}
        ]}
      ])"_json;

  TestJsonConversion(*mapping, expected_json);
}

// TableUpdate JSON Serialization/Deserialization Tests
TEST(TableUpdateJsonTest, AssignUUIDToJson) {
  std::string uuid = "550e8400-e29b-41d4-a716-446655440000";
  table::AssignUUID update(uuid);
  nlohmann::json expected =
      R"({"action":"assign-uuid","uuid":"550e8400-e29b-41d4-a716-446655440000"})"_json;

  auto json = ToJson(update);
  EXPECT_EQ(json, expected) << "AssignUUID should convert to the correct JSON value";
}

TEST(TableUpdateJsonTest, AssignUUIDFromJson) {
  std::string uuid = "550e8400-e29b-41d4-a716-446655440000";
  nlohmann::json json =
      R"({"action":"assign-uuid","uuid":"550e8400-e29b-41d4-a716-446655440000"})"_json;
  table::AssignUUID expected(uuid);

  auto parsed = TableUpdateFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(parsed.value()->kind(), TableUpdate::Kind::kAssignUUID);
  auto* actual = dynamic_cast<table::AssignUUID*>(parsed.value().get());
  EXPECT_EQ(actual->uuid(), expected.uuid()) << "UUID should parse correctly from JSON";
}

TEST(TableUpdateJsonTest, UpgradeFormatVersionToJson) {
  int32_t format_version = 2;
  table::UpgradeFormatVersion update(format_version);
  nlohmann::json expected =
      R"({"action":"upgrade-format-version","format-version":2})"_json;

  auto json = ToJson(update);
  EXPECT_EQ(json, expected)
      << "UpgradeFormatVersion should convert to the correct JSON value";
}

TEST(TableUpdateJsonTest, UpgradeFormatVersionFromJson) {
  int32_t format_version = 2;
  nlohmann::json json = R"({"action":"upgrade-format-version","format-version":2})"_json;
  table::UpgradeFormatVersion expected(format_version);

  auto parsed = TableUpdateFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(parsed.value()->kind(), TableUpdate::Kind::kUpgradeFormatVersion);
  auto* actual = dynamic_cast<table::UpgradeFormatVersion*>(parsed.value().get());
  EXPECT_EQ(actual->format_version(), expected.format_version())
      << "Format version should parse correctly from JSON";
}

TEST(TableUpdateJsonTest, AddSchemaToJson) {
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField(1, "id", int64(), false),
                               SchemaField(2, "name", string(), true)},
      /*schema_id=*/1);
  table::AddSchema update(schema, 2);

  auto json = ToJson(update);
  EXPECT_EQ(json["action"], "add-schema");
  EXPECT_EQ(json["last-column-id"], 2);
  EXPECT_TRUE(json.contains("schema")) << "AddSchema should include schema in JSON";
}

TEST(TableUpdateJsonTest, AddSchemaFromJson) {
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField(1, "id", int64(), false),
                               SchemaField(2, "name", string(), true)},
      /*schema_id=*/1);
  table::AddSchema expected(schema, 2);
  auto json = ToJson(expected);

  auto parsed = TableUpdateFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(parsed.value()->kind(), TableUpdate::Kind::kAddSchema);
  auto* actual = dynamic_cast<table::AddSchema*>(parsed.value().get());
  EXPECT_EQ(actual->last_column_id(), expected.last_column_id())
      << "Last column ID should parse correctly from JSON";
  EXPECT_EQ(actual->schema()->schema_id(), schema->schema_id())
      << "Schema ID should parse correctly from JSON";
  EXPECT_EQ(actual->schema()->fields().size(), 2)
      << "Schema fields should parse correctly from JSON";
}

TEST(TableUpdateJsonTest, SetCurrentSchemaToJson) {
  int32_t schema_id = 1;
  table::SetCurrentSchema update(schema_id);
  nlohmann::json expected = R"({"action":"set-current-schema","schema-id":1})"_json;

  auto json = ToJson(update);
  EXPECT_EQ(json, expected)
      << "SetCurrentSchema should convert to the correct JSON value";
}

TEST(TableUpdateJsonTest, SetCurrentSchemaFromJson) {
  int32_t schema_id = 1;
  nlohmann::json json = R"({"action":"set-current-schema","schema-id":1})"_json;
  table::SetCurrentSchema expected(schema_id);

  auto parsed = TableUpdateFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(parsed.value()->kind(), TableUpdate::Kind::kSetCurrentSchema);
  auto* actual = dynamic_cast<table::SetCurrentSchema*>(parsed.value().get());
  EXPECT_EQ(actual->schema_id(), expected.schema_id())
      << "Schema ID should parse correctly from JSON";
}

TEST(TableUpdateJsonTest, AddPartitionSpecRequiresSchema) {
  nlohmann::json json = R"({"action":"add-spec","spec":{"spec-id":1,"fields":[]}})"_json;

  auto result = TableUpdateFromJson(json);
  EXPECT_THAT(result, IsError(ErrorKind::kNotSupported))
      << "AddPartitionSpec without schema should fail";
}

TEST(TableUpdateJsonTest, AddPartitionSpecWithSchema) {
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField(1, "id", int64(), false),
                               SchemaField(2, "region", string(), true)},
      /*schema_id=*/1);

  nlohmann::json json = R"({
    "action": "add-spec",
    "spec": {
      "spec-id": 1,
      "fields": [
        {"source-id": 2, "field-id": 1000, "transform": "identity", "name": "region_partition"}
      ]
    }
  })"_json;

  auto result = TableUpdateFromJson(json, schema);
  ASSERT_THAT(result, IsOk());
  EXPECT_EQ(result.value()->kind(), TableUpdate::Kind::kAddPartitionSpec);
  auto* parsed = dynamic_cast<table::AddPartitionSpec*>(result.value().get());
  EXPECT_EQ(parsed->spec()->spec_id(), 1) << "Spec ID should parse correctly from JSON";
  EXPECT_EQ(parsed->spec()->fields().size(), 1)
      << "Spec fields should parse correctly from JSON";
}

TEST(TableUpdateJsonTest, SetDefaultPartitionSpecToJson) {
  int32_t spec_id = 2;
  table::SetDefaultPartitionSpec update(spec_id);
  nlohmann::json expected = R"({"action":"set-default-spec","spec-id":2})"_json;

  auto json = ToJson(update);
  EXPECT_EQ(json, expected)
      << "SetDefaultPartitionSpec should convert to the correct JSON value";
}

TEST(TableUpdateJsonTest, SetDefaultPartitionSpecFromJson) {
  int32_t spec_id = 2;
  nlohmann::json json = R"({"action":"set-default-spec","spec-id":2})"_json;
  table::SetDefaultPartitionSpec expected(spec_id);

  auto parsed = TableUpdateFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(parsed.value()->kind(), TableUpdate::Kind::kSetDefaultPartitionSpec);
  auto* actual = dynamic_cast<table::SetDefaultPartitionSpec*>(parsed.value().get());
  EXPECT_EQ(actual->spec_id(), expected.spec_id())
      << "Spec ID should parse correctly from JSON";
}

TEST(TableUpdateJsonTest, RemovePartitionSpecsToJson) {
  std::vector<int32_t> spec_ids = {1, 2, 3};
  table::RemovePartitionSpecs update(spec_ids);
  nlohmann::json expected =
      R"({"action":"remove-partition-specs","spec-ids":[1,2,3]})"_json;

  auto json = ToJson(update);
  EXPECT_EQ(json, expected)
      << "RemovePartitionSpecs should convert to the correct JSON value";
}

TEST(TableUpdateJsonTest, RemovePartitionSpecsFromJson) {
  std::vector<int32_t> spec_ids = {1, 2, 3};
  nlohmann::json json = R"({"action":"remove-partition-specs","spec-ids":[1,2,3]})"_json;
  table::RemovePartitionSpecs expected(spec_ids);

  auto parsed = TableUpdateFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(parsed.value()->kind(), TableUpdate::Kind::kRemovePartitionSpecs);
  auto* actual = dynamic_cast<table::RemovePartitionSpecs*>(parsed.value().get());
  EXPECT_EQ(actual->spec_ids(), expected.spec_ids())
      << "Spec IDs should parse correctly from JSON";
}

TEST(TableUpdateJsonTest, RemoveSchemasToJson) {
  std::unordered_set<int32_t> schema_ids = {1, 2};
  table::RemoveSchemas update(schema_ids);
  nlohmann::json expected = R"({"action":"remove-schemas","schema-ids":[1,2]})"_json;

  auto json = ToJson(update);
  EXPECT_EQ(json["action"], "remove-schemas");
}

TEST(TableUpdateJsonTest, RemoveSchemasFromJson) {
  std::unordered_set<int32_t> schema_ids = {1, 2};
  nlohmann::json json = R"({"action":"remove-schemas","schema-ids":[1,2]})"_json;
  table::RemoveSchemas expected(schema_ids);

  auto parsed = TableUpdateFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(parsed.value()->kind(), TableUpdate::Kind::kRemoveSchemas);
  auto* actual = dynamic_cast<table::RemoveSchemas*>(parsed.value().get());
  EXPECT_EQ(actual->schema_ids(), expected.schema_ids())
      << "Schema IDs should parse correctly from JSON";
}

TEST(TableUpdateJsonTest, AddSortOrderRequiresSchema) {
  nlohmann::json json =
      R"({"action":"add-sort-order","sort-order":{"order-id":1,"fields":[]}})"_json;

  auto result = TableUpdateFromJson(json);
  EXPECT_THAT(result, IsError(ErrorKind::kNotSupported))
      << "AddSortOrder without schema should fail";
}

TEST(TableUpdateJsonTest, SetDefaultSortOrderToJson) {
  int32_t sort_order_id = 1;
  table::SetDefaultSortOrder update(sort_order_id);
  nlohmann::json expected =
      R"({"action":"set-default-sort-order","sort-order-id":1})"_json;

  auto json = ToJson(update);
  EXPECT_EQ(json, expected)
      << "SetDefaultSortOrder should convert to the correct JSON value";
}

TEST(TableUpdateJsonTest, SetDefaultSortOrderFromJson) {
  int32_t sort_order_id = 1;
  nlohmann::json json = R"({"action":"set-default-sort-order","sort-order-id":1})"_json;
  table::SetDefaultSortOrder expected(sort_order_id);

  auto parsed = TableUpdateFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(parsed.value()->kind(), TableUpdate::Kind::kSetDefaultSortOrder);
  auto* actual = dynamic_cast<table::SetDefaultSortOrder*>(parsed.value().get());
  EXPECT_EQ(actual->sort_order_id(), expected.sort_order_id())
      << "Sort order ID should parse correctly from JSON";
}

TEST(TableUpdateJsonTest, AddSnapshotToJson) {
  auto snapshot = std::make_shared<Snapshot>(
      Snapshot{.snapshot_id = 123456789,
               .parent_snapshot_id = 987654321,
               .sequence_number = 5,
               .timestamp_ms = TimePointMsFromUnixMs(1234567890000).value(),
               .manifest_list = "/path/to/manifest-list.avro",
               .summary = {{SnapshotSummaryFields::kOperation, DataOperation::kAppend}},
               .schema_id = 1});
  table::AddSnapshot update(snapshot);

  auto json = ToJson(update);
  EXPECT_EQ(json["action"], "add-snapshot");
  EXPECT_TRUE(json.contains("snapshot")) << "AddSnapshot should include snapshot in JSON";
}

TEST(TableUpdateJsonTest, AddSnapshotFromJson) {
  auto snapshot = std::make_shared<Snapshot>(
      Snapshot{.snapshot_id = 123456789,
               .parent_snapshot_id = 987654321,
               .sequence_number = 5,
               .timestamp_ms = TimePointMsFromUnixMs(1234567890000).value(),
               .manifest_list = "/path/to/manifest-list.avro",
               .summary = {{SnapshotSummaryFields::kOperation, DataOperation::kAppend}},
               .schema_id = 1});
  table::AddSnapshot expected(snapshot);
  auto json = ToJson(expected);

  auto parsed = TableUpdateFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(parsed.value()->kind(), TableUpdate::Kind::kAddSnapshot);
  auto* actual = dynamic_cast<table::AddSnapshot*>(parsed.value().get());
  EXPECT_EQ(actual->snapshot()->snapshot_id, snapshot->snapshot_id)
      << "Snapshot ID should parse correctly from JSON";
}

TEST(TableUpdateJsonTest, RemoveSnapshotsToJson) {
  std::vector<int64_t> snapshot_ids = {111, 222, 333};
  table::RemoveSnapshots update(snapshot_ids);
  nlohmann::json expected =
      R"({"action":"remove-snapshots","snapshot-ids":[111,222,333]})"_json;

  auto json = ToJson(update);
  EXPECT_EQ(json, expected) << "RemoveSnapshots should convert to the correct JSON value";
}

TEST(TableUpdateJsonTest, RemoveSnapshotsFromJson) {
  std::vector<int64_t> snapshot_ids = {111, 222, 333};
  nlohmann::json json =
      R"({"action":"remove-snapshots","snapshot-ids":[111,222,333]})"_json;
  table::RemoveSnapshots expected(snapshot_ids);

  auto parsed = TableUpdateFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(parsed.value()->kind(), TableUpdate::Kind::kRemoveSnapshots);
  auto* actual = dynamic_cast<table::RemoveSnapshots*>(parsed.value().get());
  EXPECT_EQ(actual->snapshot_ids(), expected.snapshot_ids())
      << "Snapshot IDs should parse correctly from JSON";
}

TEST(TableUpdateJsonTest, RemoveSnapshotRefToJson) {
  std::string ref_name = "my-branch";
  table::RemoveSnapshotRef update(ref_name);
  nlohmann::json expected =
      R"({"action":"remove-snapshot-ref","ref-name":"my-branch"})"_json;

  auto json = ToJson(update);
  EXPECT_EQ(json, expected)
      << "RemoveSnapshotRef should convert to the correct JSON value";
}

TEST(TableUpdateJsonTest, RemoveSnapshotRefFromJson) {
  std::string ref_name = "my-branch";
  nlohmann::json json = R"({"action":"remove-snapshot-ref","ref-name":"my-branch"})"_json;
  table::RemoveSnapshotRef expected(ref_name);

  auto parsed = TableUpdateFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(parsed.value()->kind(), TableUpdate::Kind::kRemoveSnapshotRef);
  auto* actual = dynamic_cast<table::RemoveSnapshotRef*>(parsed.value().get());
  EXPECT_EQ(actual->ref_name(), expected.ref_name())
      << "Ref name should parse correctly from JSON";
}

TEST(TableUpdateJsonTest, SetSnapshotRefBranchToJson) {
  table::SetSnapshotRef update("main", 123456789, SnapshotRefType::kBranch, 5, 86400000,
                               604800000);

  auto json = ToJson(update);
  EXPECT_EQ(json["action"], "set-snapshot-ref");
  EXPECT_EQ(json["ref-name"], "main");
  EXPECT_EQ(json["snapshot-id"], 123456789);
  EXPECT_EQ(json["type"], "branch");
}

TEST(TableUpdateJsonTest, SetSnapshotRefBranchFromJson) {
  table::SetSnapshotRef expected("main", 123456789, SnapshotRefType::kBranch, 5, 86400000,
                                 604800000);
  auto json = ToJson(expected);

  auto parsed = TableUpdateFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(parsed.value()->kind(), TableUpdate::Kind::kSetSnapshotRef);
  auto* actual = dynamic_cast<table::SetSnapshotRef*>(parsed.value().get());
  EXPECT_EQ(actual->ref_name(), expected.ref_name())
      << "Ref name should parse correctly from JSON";
  EXPECT_EQ(actual->snapshot_id(), expected.snapshot_id())
      << "Snapshot ID should parse correctly from JSON";
  EXPECT_EQ(actual->type(), expected.type()) << "Type should parse correctly from JSON";
  EXPECT_EQ(actual->min_snapshots_to_keep(), expected.min_snapshots_to_keep())
      << "Min snapshots to keep should parse correctly from JSON";
}

TEST(TableUpdateJsonTest, SetSnapshotRefTagToJson) {
  table::SetSnapshotRef update("release-1.0", 987654321, SnapshotRefType::kTag);

  auto json = ToJson(update);
  EXPECT_EQ(json["action"], "set-snapshot-ref");
  EXPECT_EQ(json["type"], "tag");
}

TEST(TableUpdateJsonTest, SetSnapshotRefTagFromJson) {
  table::SetSnapshotRef expected("release-1.0", 987654321, SnapshotRefType::kTag);
  auto json = ToJson(expected);

  auto parsed = TableUpdateFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(parsed.value()->kind(), TableUpdate::Kind::kSetSnapshotRef);
  auto* actual = dynamic_cast<table::SetSnapshotRef*>(parsed.value().get());
  EXPECT_EQ(actual->type(), SnapshotRefType::kTag)
      << "Type should parse correctly as tag from JSON";
}

TEST(TableUpdateJsonTest, SetPropertiesToJson) {
  std::unordered_map<std::string, std::string> props = {{"key1", "value1"},
                                                        {"key2", "value2"}};
  table::SetProperties update(props);

  auto json = ToJson(update);
  EXPECT_EQ(json["action"], "set-properties");
  EXPECT_TRUE(json.contains("updates")) << "SetProperties should include updates in JSON";
}

TEST(TableUpdateJsonTest, SetPropertiesFromJson) {
  std::unordered_map<std::string, std::string> props = {{"key1", "value1"},
                                                        {"key2", "value2"}};
  table::SetProperties expected(props);
  auto json = ToJson(expected);

  auto parsed = TableUpdateFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(parsed.value()->kind(), TableUpdate::Kind::kSetProperties);
  auto* actual = dynamic_cast<table::SetProperties*>(parsed.value().get());
  EXPECT_EQ(actual->updated(), expected.updated())
      << "Properties should parse correctly from JSON";
}

TEST(TableUpdateJsonTest, RemovePropertiesToJson) {
  std::unordered_set<std::string> props = {"key1", "key2"};
  table::RemoveProperties update(props);

  auto json = ToJson(update);
  EXPECT_EQ(json["action"], "remove-properties");
  EXPECT_TRUE(json.contains("removals"))
      << "RemoveProperties should include removals in JSON";
}

TEST(TableUpdateJsonTest, RemovePropertiesFromJson) {
  std::unordered_set<std::string> props = {"key1", "key2"};
  table::RemoveProperties expected(props);
  auto json = ToJson(expected);

  auto parsed = TableUpdateFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(parsed.value()->kind(), TableUpdate::Kind::kRemoveProperties);
  auto* actual = dynamic_cast<table::RemoveProperties*>(parsed.value().get());
  EXPECT_EQ(actual->removed(), expected.removed())
      << "Removed properties should parse correctly from JSON";
}

TEST(TableUpdateJsonTest, SetLocationToJson) {
  std::string location = "s3://bucket/warehouse/table";
  table::SetLocation update(location);
  nlohmann::json expected =
      R"({"action":"set-location","location":"s3://bucket/warehouse/table"})"_json;

  auto json = ToJson(update);
  EXPECT_EQ(json, expected) << "SetLocation should convert to the correct JSON value";
}

TEST(TableUpdateJsonTest, SetLocationFromJson) {
  std::string location = "s3://bucket/warehouse/table";
  nlohmann::json json =
      R"({"action":"set-location","location":"s3://bucket/warehouse/table"})"_json;
  table::SetLocation expected(location);

  auto parsed = TableUpdateFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(parsed.value()->kind(), TableUpdate::Kind::kSetLocation);
  auto* actual = dynamic_cast<table::SetLocation*>(parsed.value().get());
  EXPECT_EQ(actual->location(), expected.location())
      << "Location should parse correctly from JSON";
}

TEST(TableUpdateJsonTest, UnknownAction) {
  nlohmann::json json = R"({"action":"unknown-action"})"_json;
  auto result = TableUpdateFromJson(json);
  EXPECT_THAT(result, IsError(ErrorKind::kJsonParseError));
  EXPECT_THAT(result, HasErrorMessage("Unknown table update action"));
}

// TableRequirement JSON Serialization/Deserialization Tests
TEST(TableRequirementJsonTest, AssertDoesNotExistToJson) {
  table::AssertDoesNotExist req;
  nlohmann::json expected = R"({"type":"assert-create"})"_json;

  auto json = ToJson(req);
  EXPECT_EQ(json, expected)
      << "AssertDoesNotExist should convert to the correct JSON value";
}

TEST(TableRequirementJsonTest, AssertDoesNotExistFromJson) {
  nlohmann::json json = R"({"type":"assert-create"})"_json;
  table::AssertDoesNotExist expected;

  auto parsed = TableRequirementFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(parsed.value()->kind(), TableRequirement::Kind::kAssertDoesNotExist);
}

TEST(TableRequirementJsonTest, AssertUUIDToJson) {
  std::string uuid = "550e8400-e29b-41d4-a716-446655440000";
  table::AssertUUID req(uuid);
  nlohmann::json expected =
      R"({"type":"assert-table-uuid","uuid":"550e8400-e29b-41d4-a716-446655440000"})"_json;

  auto json = ToJson(req);
  EXPECT_EQ(json, expected) << "AssertUUID should convert to the correct JSON value";
}

TEST(TableRequirementJsonTest, AssertUUIDFromJson) {
  std::string uuid = "550e8400-e29b-41d4-a716-446655440000";
  nlohmann::json json =
      R"({"type":"assert-table-uuid","uuid":"550e8400-e29b-41d4-a716-446655440000"})"_json;
  table::AssertUUID expected(uuid);

  auto parsed = TableRequirementFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(parsed.value()->kind(), TableRequirement::Kind::kAssertUUID);
  auto* actual = dynamic_cast<table::AssertUUID*>(parsed.value().get());
  EXPECT_EQ(*actual, expected) << "UUID should parse correctly from JSON";
}

TEST(TableRequirementJsonTest, AssertRefSnapshotIDToJson) {
  std::string ref_name = "main";
  int64_t snapshot_id = 123456789;
  table::AssertRefSnapshotID req(ref_name, snapshot_id);
  nlohmann::json expected =
      R"({"type":"assert-ref-snapshot-id","ref-name":"main","snapshot-id":123456789})"_json;

  auto json = ToJson(req);
  EXPECT_EQ(json, expected)
      << "AssertRefSnapshotID should convert to the correct JSON value";
}

TEST(TableRequirementJsonTest, AssertRefSnapshotIDFromJson) {
  std::string ref_name = "main";
  int64_t snapshot_id = 123456789;
  nlohmann::json json =
      R"({"type":"assert-ref-snapshot-id","ref-name":"main","snapshot-id":123456789})"_json;
  table::AssertRefSnapshotID expected(ref_name, snapshot_id);

  auto parsed = TableRequirementFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(parsed.value()->kind(), TableRequirement::Kind::kAssertRefSnapshotID);
  auto* actual = dynamic_cast<table::AssertRefSnapshotID*>(parsed.value().get());
  EXPECT_EQ(*actual, expected) << "AssertRefSnapshotID should parse correctly from JSON";
}

TEST(TableRequirementJsonTest, AssertRefSnapshotIDToJsonWithNull) {
  std::string ref_name = "main";
  table::AssertRefSnapshotID req(ref_name, std::nullopt);

  auto json = ToJson(req);
  EXPECT_EQ(json["snapshot-id"], nullptr);
}

TEST(TableRequirementJsonTest, AssertRefSnapshotIDFromJsonWithNull) {
  std::string ref_name = "main";
  nlohmann::json json =
      R"({"type":"assert-ref-snapshot-id","ref-name":"main","snapshot-id":null})"_json;
  table::AssertRefSnapshotID expected(ref_name, std::nullopt);

  auto parsed = TableRequirementFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  auto* actual = dynamic_cast<table::AssertRefSnapshotID*>(parsed.value().get());
  EXPECT_EQ(*actual, expected) << "AssertRefSnapshotID with null should parse correctly";
}

TEST(TableRequirementJsonTest, AssertLastAssignedFieldIdToJson) {
  int32_t last_assigned_field_id = 100;
  table::AssertLastAssignedFieldId req(last_assigned_field_id);
  nlohmann::json expected =
      R"({"type":"assert-last-assigned-field-id","last-assigned-field-id":100})"_json;

  auto json = ToJson(req);
  EXPECT_EQ(json, expected)
      << "AssertLastAssignedFieldId should convert to the correct JSON value";
}

TEST(TableRequirementJsonTest, AssertLastAssignedFieldIdFromJson) {
  int32_t last_assigned_field_id = 100;
  nlohmann::json json =
      R"({"type":"assert-last-assigned-field-id","last-assigned-field-id":100})"_json;
  table::AssertLastAssignedFieldId expected(last_assigned_field_id);

  auto parsed = TableRequirementFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(parsed.value()->kind(), TableRequirement::Kind::kAssertLastAssignedFieldId);
  auto* actual = dynamic_cast<table::AssertLastAssignedFieldId*>(parsed.value().get());
  EXPECT_EQ(*actual, expected)
      << "AssertLastAssignedFieldId should parse correctly from JSON";
}

TEST(TableRequirementJsonTest, AssertCurrentSchemaIDToJson) {
  int32_t schema_id = 1;
  table::AssertCurrentSchemaID req(schema_id);
  nlohmann::json expected =
      R"({"type":"assert-current-schema-id","current-schema-id":1})"_json;

  auto json = ToJson(req);
  EXPECT_EQ(json, expected)
      << "AssertCurrentSchemaID should convert to the correct JSON value";
}

TEST(TableRequirementJsonTest, AssertCurrentSchemaIDFromJson) {
  int32_t schema_id = 1;
  nlohmann::json json =
      R"({"type":"assert-current-schema-id","current-schema-id":1})"_json;
  table::AssertCurrentSchemaID expected(schema_id);

  auto parsed = TableRequirementFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(parsed.value()->kind(), TableRequirement::Kind::kAssertCurrentSchemaID);
  auto* actual = dynamic_cast<table::AssertCurrentSchemaID*>(parsed.value().get());
  EXPECT_EQ(*actual, expected)
      << "AssertCurrentSchemaID should parse correctly from JSON";
}

TEST(TableRequirementJsonTest, AssertLastAssignedPartitionIdToJson) {
  int32_t last_assigned_partition_id = 1000;
  table::AssertLastAssignedPartitionId req(last_assigned_partition_id);
  nlohmann::json expected =
      R"({"type":"assert-last-assigned-partition-id","last-assigned-partition-id":1000})"_json;

  auto json = ToJson(req);
  EXPECT_EQ(json, expected)
      << "AssertLastAssignedPartitionId should convert to the correct JSON value";
}

TEST(TableRequirementJsonTest, AssertLastAssignedPartitionIdFromJson) {
  int32_t last_assigned_partition_id = 1000;
  nlohmann::json json =
      R"({"type":"assert-last-assigned-partition-id","last-assigned-partition-id":1000})"_json;
  table::AssertLastAssignedPartitionId expected(last_assigned_partition_id);

  auto parsed = TableRequirementFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(parsed.value()->kind(),
            TableRequirement::Kind::kAssertLastAssignedPartitionId);
  auto* actual =
      dynamic_cast<table::AssertLastAssignedPartitionId*>(parsed.value().get());
  EXPECT_EQ(*actual, expected)
      << "AssertLastAssignedPartitionId should parse correctly from JSON";
}

TEST(TableRequirementJsonTest, AssertDefaultSpecIDToJson) {
  int32_t spec_id = 0;
  table::AssertDefaultSpecID req(spec_id);
  nlohmann::json expected =
      R"({"type":"assert-default-spec-id","default-spec-id":0})"_json;

  auto json = ToJson(req);
  EXPECT_EQ(json, expected)
      << "AssertDefaultSpecID should convert to the correct JSON value";
}

TEST(TableRequirementJsonTest, AssertDefaultSpecIDFromJson) {
  int32_t spec_id = 0;
  nlohmann::json json = R"({"type":"assert-default-spec-id","default-spec-id":0})"_json;
  table::AssertDefaultSpecID expected(spec_id);

  auto parsed = TableRequirementFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(parsed.value()->kind(), TableRequirement::Kind::kAssertDefaultSpecID);
  auto* actual = dynamic_cast<table::AssertDefaultSpecID*>(parsed.value().get());
  EXPECT_EQ(*actual, expected) << "AssertDefaultSpecID should parse correctly from JSON";
}

TEST(TableRequirementJsonTest, AssertDefaultSortOrderIDToJson) {
  int32_t sort_order_id = 0;
  table::AssertDefaultSortOrderID req(sort_order_id);
  nlohmann::json expected =
      R"({"type":"assert-default-sort-order-id","default-sort-order-id":0})"_json;

  auto json = ToJson(req);
  EXPECT_EQ(json, expected)
      << "AssertDefaultSortOrderID should convert to the correct JSON value";
}

TEST(TableRequirementJsonTest, AssertDefaultSortOrderIDFromJson) {
  int32_t sort_order_id = 0;
  nlohmann::json json =
      R"({"type":"assert-default-sort-order-id","default-sort-order-id":0})"_json;
  table::AssertDefaultSortOrderID expected(sort_order_id);

  auto parsed = TableRequirementFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(parsed.value()->kind(), TableRequirement::Kind::kAssertDefaultSortOrderID);
  auto* actual = dynamic_cast<table::AssertDefaultSortOrderID*>(parsed.value().get());
  EXPECT_EQ(*actual, expected)
      << "AssertDefaultSortOrderID should parse correctly from JSON";
}

TEST(TableRequirementJsonTest, UnknownType) {
  nlohmann::json json = R"({"type":"unknown-type"})"_json;
  auto result = TableRequirementFromJson(json);
  EXPECT_THAT(result, IsError(ErrorKind::kJsonParseError));
  EXPECT_THAT(result, HasErrorMessage("Unknown table requirement type"));
}

}  // namespace iceberg
