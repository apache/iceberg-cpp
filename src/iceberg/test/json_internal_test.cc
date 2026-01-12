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

  ASSERT_EQ(result.value()->Operation(), DataOperation::kOverwrite);
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
TEST(JsonInternalTest, TableUpdateAssignUUID) {
  table::AssignUUID update("550e8400-e29b-41d4-a716-446655440000");
  nlohmann::json expected =
      R"({"action":"assign-uuid","uuid":"550e8400-e29b-41d4-a716-446655440000"})"_json;

  EXPECT_EQ(ToJson(update), expected);
  auto parsed = TableUpdateFromJson(expected);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(*internal::checked_cast<table::AssignUUID*>(parsed.value().get()), update);
}

TEST(JsonInternalTest, TableUpdateUpgradeFormatVersion) {
  table::UpgradeFormatVersion update(2);
  nlohmann::json expected =
      R"({"action":"upgrade-format-version","format-version":2})"_json;

  EXPECT_EQ(ToJson(update), expected);
  auto parsed = TableUpdateFromJson(expected);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(*internal::checked_cast<table::UpgradeFormatVersion*>(parsed.value().get()),
            update);
}

TEST(JsonInternalTest, TableUpdateAddSchema) {
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField(1, "id", int64(), false),
                               SchemaField(2, "name", string(), true)},
      /*schema_id=*/1);
  table::AddSchema update(schema, 2);

  auto json = ToJson(update);
  EXPECT_EQ(json["action"], "add-schema");
  EXPECT_EQ(json["last-column-id"], 2);
  EXPECT_TRUE(json.contains("schema"));

  auto parsed = TableUpdateFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  auto* actual = internal::checked_cast<table::AddSchema*>(parsed.value().get());
  EXPECT_EQ(actual->last_column_id(), update.last_column_id());
  EXPECT_EQ(*actual->schema(), *update.schema());
}

TEST(JsonInternalTest, TableUpdateSetCurrentSchema) {
  table::SetCurrentSchema update(1);
  nlohmann::json expected = R"({"action":"set-current-schema","schema-id":1})"_json;

  EXPECT_EQ(ToJson(update), expected);
  auto parsed = TableUpdateFromJson(expected);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(*internal::checked_cast<table::SetCurrentSchema*>(parsed.value().get()),
            update);
}

TEST(JsonInternalTest, TableUpdateSetDefaultPartitionSpec) {
  table::SetDefaultPartitionSpec update(2);
  nlohmann::json expected = R"({"action":"set-default-spec","spec-id":2})"_json;

  EXPECT_EQ(ToJson(update), expected);
  auto parsed = TableUpdateFromJson(expected);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(
      *internal::checked_cast<table::SetDefaultPartitionSpec*>(parsed.value().get()),
      update);
}

TEST(JsonInternalTest, TableUpdateRemovePartitionSpecs) {
  table::RemovePartitionSpecs update({1, 2, 3});
  nlohmann::json expected =
      R"({"action":"remove-partition-specs","spec-ids":[1,2,3]})"_json;

  EXPECT_EQ(ToJson(update), expected);
  auto parsed = TableUpdateFromJson(expected);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(*internal::checked_cast<table::RemovePartitionSpecs*>(parsed.value().get()),
            update);
}

TEST(JsonInternalTest, TableUpdateRemoveSchemas) {
  table::RemoveSchemas update({1, 2});

  auto json = ToJson(update);
  EXPECT_EQ(json["action"], "remove-schemas");
  EXPECT_THAT(json["schema-ids"].get<std::vector<int32_t>>(),
              testing::UnorderedElementsAre(1, 2));

  auto parsed = TableUpdateFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(*internal::checked_cast<table::RemoveSchemas*>(parsed.value().get()), update);
}

TEST(JsonInternalTest, TableUpdateSetDefaultSortOrder) {
  table::SetDefaultSortOrder update(1);
  nlohmann::json expected =
      R"({"action":"set-default-sort-order","sort-order-id":1})"_json;

  EXPECT_EQ(ToJson(update), expected);
  auto parsed = TableUpdateFromJson(expected);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(*internal::checked_cast<table::SetDefaultSortOrder*>(parsed.value().get()),
            update);
}

TEST(JsonInternalTest, TableUpdateAddSnapshot) {
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
  EXPECT_TRUE(json.contains("snapshot"));

  auto parsed = TableUpdateFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  auto* actual = internal::checked_cast<table::AddSnapshot*>(parsed.value().get());
  EXPECT_EQ(*actual->snapshot(), *update.snapshot());
}

TEST(JsonInternalTest, TableUpdateRemoveSnapshots) {
  table::RemoveSnapshots update({111, 222, 333});
  nlohmann::json expected =
      R"({"action":"remove-snapshots","snapshot-ids":[111,222,333]})"_json;

  EXPECT_EQ(ToJson(update), expected);
  auto parsed = TableUpdateFromJson(expected);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(*internal::checked_cast<table::RemoveSnapshots*>(parsed.value().get()),
            update);
}

TEST(JsonInternalTest, TableUpdateRemoveSnapshotRef) {
  table::RemoveSnapshotRef update("my-branch");
  nlohmann::json expected =
      R"({"action":"remove-snapshot-ref","ref-name":"my-branch"})"_json;

  EXPECT_EQ(ToJson(update), expected);
  auto parsed = TableUpdateFromJson(expected);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(*internal::checked_cast<table::RemoveSnapshotRef*>(parsed.value().get()),
            update);
}

TEST(JsonInternalTest, TableUpdateSetSnapshotRefBranch) {
  table::SetSnapshotRef update("main", 123456789, SnapshotRefType::kBranch, 5, 86400000,
                               604800000);

  auto json = ToJson(update);
  EXPECT_EQ(json["action"], "set-snapshot-ref");
  EXPECT_EQ(json["ref-name"], "main");
  EXPECT_EQ(json["snapshot-id"], 123456789);
  EXPECT_EQ(json["type"], "branch");

  auto parsed = TableUpdateFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(*internal::checked_cast<table::SetSnapshotRef*>(parsed.value().get()),
            update);
}

TEST(JsonInternalTest, TableUpdateSetSnapshotRefTag) {
  table::SetSnapshotRef update("release-1.0", 987654321, SnapshotRefType::kTag);

  auto json = ToJson(update);
  EXPECT_EQ(json["action"], "set-snapshot-ref");
  EXPECT_EQ(json["type"], "tag");

  auto parsed = TableUpdateFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(*internal::checked_cast<table::SetSnapshotRef*>(parsed.value().get()),
            update);
}

TEST(JsonInternalTest, TableUpdateSetProperties) {
  table::SetProperties update({{"key1", "value1"}, {"key2", "value2"}});

  auto json = ToJson(update);
  EXPECT_EQ(json["action"], "set-properties");
  EXPECT_TRUE(json.contains("updates"));

  auto parsed = TableUpdateFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(*internal::checked_cast<table::SetProperties*>(parsed.value().get()), update);
}

TEST(JsonInternalTest, TableUpdateRemoveProperties) {
  table::RemoveProperties update({"key1", "key2"});

  auto json = ToJson(update);
  EXPECT_EQ(json["action"], "remove-properties");
  EXPECT_TRUE(json.contains("removals"));

  auto parsed = TableUpdateFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(*internal::checked_cast<table::RemoveProperties*>(parsed.value().get()),
            update);
}

TEST(JsonInternalTest, TableUpdateSetLocation) {
  table::SetLocation update("s3://bucket/warehouse/table");
  nlohmann::json expected =
      R"({"action":"set-location","location":"s3://bucket/warehouse/table"})"_json;

  EXPECT_EQ(ToJson(update), expected);
  auto parsed = TableUpdateFromJson(expected);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(*internal::checked_cast<table::SetLocation*>(parsed.value().get()), update);
}

TEST(JsonInternalTest, TableUpdateUnknownAction) {
  nlohmann::json json = R"({"action":"unknown-action"})"_json;
  auto result = TableUpdateFromJson(json);
  EXPECT_THAT(result, IsError(ErrorKind::kJsonParseError));
  EXPECT_THAT(result, HasErrorMessage("Unknown table update action"));
}

// TableRequirement JSON Serialization/Deserialization Tests
TEST(TableRequirementJsonTest, TableRequirementAssertDoesNotExist) {
  table::AssertDoesNotExist req;
  nlohmann::json expected = R"({"type":"assert-create"})"_json;

  EXPECT_EQ(ToJson(req), expected);
  auto parsed = TableRequirementFromJson(expected);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(parsed.value()->kind(), TableRequirement::Kind::kAssertDoesNotExist);
}

TEST(TableRequirementJsonTest, TableRequirementAssertUUID) {
  table::AssertUUID req("550e8400-e29b-41d4-a716-446655440000");
  nlohmann::json expected =
      R"({"type":"assert-table-uuid","uuid":"550e8400-e29b-41d4-a716-446655440000"})"_json;

  EXPECT_EQ(ToJson(req), expected);
  auto parsed = TableRequirementFromJson(expected);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(*internal::checked_cast<table::AssertUUID*>(parsed.value().get()), req);
}

TEST(TableRequirementJsonTest, TableRequirementAssertRefSnapshotID) {
  table::AssertRefSnapshotID req("main", 123456789);
  nlohmann::json expected =
      R"({"type":"assert-ref-snapshot-id","ref-name":"main","snapshot-id":123456789})"_json;

  EXPECT_EQ(ToJson(req), expected);
  auto parsed = TableRequirementFromJson(expected);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(*internal::checked_cast<table::AssertRefSnapshotID*>(parsed.value().get()),
            req);
}

TEST(TableRequirementJsonTest, TableRequirementAssertRefSnapshotIDWithNull) {
  table::AssertRefSnapshotID req("main", std::nullopt);
  nlohmann::json expected =
      R"({"type":"assert-ref-snapshot-id","ref-name":"main","snapshot-id":null})"_json;

  EXPECT_EQ(ToJson(req), expected);
  auto parsed = TableRequirementFromJson(expected);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(*internal::checked_cast<table::AssertRefSnapshotID*>(parsed.value().get()),
            req);
}

TEST(TableRequirementJsonTest, TableRequirementAssertLastAssignedFieldId) {
  table::AssertLastAssignedFieldId req(100);
  nlohmann::json expected =
      R"({"type":"assert-last-assigned-field-id","last-assigned-field-id":100})"_json;

  EXPECT_EQ(ToJson(req), expected);
  auto parsed = TableRequirementFromJson(expected);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(
      *internal::checked_cast<table::AssertLastAssignedFieldId*>(parsed.value().get()),
      req);
}

TEST(TableRequirementJsonTest, TableRequirementAssertCurrentSchemaID) {
  table::AssertCurrentSchemaID req(1);
  nlohmann::json expected =
      R"({"type":"assert-current-schema-id","current-schema-id":1})"_json;

  EXPECT_EQ(ToJson(req), expected);
  auto parsed = TableRequirementFromJson(expected);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(*internal::checked_cast<table::AssertCurrentSchemaID*>(parsed.value().get()),
            req);
}

TEST(TableRequirementJsonTest, TableRequirementAssertLastAssignedPartitionId) {
  table::AssertLastAssignedPartitionId req(1000);
  nlohmann::json expected =
      R"({"type":"assert-last-assigned-partition-id","last-assigned-partition-id":1000})"_json;

  EXPECT_EQ(ToJson(req), expected);
  auto parsed = TableRequirementFromJson(expected);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(*internal::checked_cast<table::AssertLastAssignedPartitionId*>(
                parsed.value().get()),
            req);
}

TEST(TableRequirementJsonTest, TableRequirementAssertDefaultSpecID) {
  table::AssertDefaultSpecID req(0);
  nlohmann::json expected =
      R"({"type":"assert-default-spec-id","default-spec-id":0})"_json;

  EXPECT_EQ(ToJson(req), expected);
  auto parsed = TableRequirementFromJson(expected);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(*internal::checked_cast<table::AssertDefaultSpecID*>(parsed.value().get()),
            req);
}

TEST(TableRequirementJsonTest, TableRequirementAssertDefaultSortOrderID) {
  table::AssertDefaultSortOrderID req(0);
  nlohmann::json expected =
      R"({"type":"assert-default-sort-order-id","default-sort-order-id":0})"_json;

  EXPECT_EQ(ToJson(req), expected);
  auto parsed = TableRequirementFromJson(expected);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(
      *internal::checked_cast<table::AssertDefaultSortOrderID*>(parsed.value().get()),
      req);
}

TEST(TableRequirementJsonTest, TableRequirementUnknownType) {
  nlohmann::json json = R"({"type":"unknown-type"})"_json;
  auto result = TableRequirementFromJson(json);
  EXPECT_THAT(result, IsError(ErrorKind::kJsonParseError));
  EXPECT_THAT(result, HasErrorMessage("Unknown table requirement type"));
}

}  // namespace iceberg
