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

#include <array>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/expression/expressions.h"
#include "iceberg/expression/json_serde_internal.h"
#include "iceberg/expression/literal.h"
#include "iceberg/file_format.h"
#include "iceberg/json_serde_internal.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/name_mapping.h"
#include "iceberg/partition_spec.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/snapshot.h"
#include "iceberg/sort_field.h"
#include "iceberg/sort_order.h"
#include "iceberg/statistics_file.h"
#include "iceberg/table_requirement.h"
#include "iceberg/table_scan.h"
#include "iceberg/table_update.h"
#include "iceberg/test/matchers.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"
#include "iceberg/util/base64.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep
#include "iceberg/util/macros.h"     // IWYU pragma: keep
#include "iceberg/util/timepoint.h"
#include "iceberg/util/uuid.h"

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

template <>
Result<std::unique_ptr<SchemaField>> FromJsonHelper(const nlohmann::json& json) {
  return FieldFromJson(json);
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

// ToJson(SchemaField) returns Result, so it cannot use the shared
// TestJsonConversion helper. Unwrap the serialized json before comparing and
// round-tripping.
void TestSchemaFieldJsonConversion(const SchemaField& field,
                                   const nlohmann::json& expected_json) {
  ICEBERG_UNWRAP_OR_FAIL(auto json, ToJson(field));
  EXPECT_EQ(expected_json, json) << "JSON conversion mismatch.";

  auto obj_ex = FieldFromJson(expected_json);
  EXPECT_TRUE(obj_ex.has_value()) << "Failed to deserialize JSON.";
  EXPECT_EQ(field, *obj_ex.value()) << "Deserialized object mismatch.";
}

}  // namespace

// Pins the wire format produced by ToJson(SchemaField) / FieldFromJson for
// `initial-default` and `write-default`, including the absence of the keys when a
// field carries no defaults.
TEST(JsonInternalTest, SchemaFieldDefaultValues) {
  // Both defaults present.
  SchemaField with_both(/*field_id=*/1, "id", int32(), /*optional=*/false, /*doc=*/{},
                        std::make_shared<const Literal>(Literal::Int(42)),
                        std::make_shared<const Literal>(Literal::Int(7)));
  TestSchemaFieldJsonConversion(
      with_both,
      R"({"id":1,"name":"id","required":true,"type":"int","initial-default":42,"write-default":7})"_json);

  // Only an initial-default; write-default must not appear in the JSON.
  SchemaField initial_only(/*field_id=*/2, "name", string(), /*optional=*/true,
                           /*doc=*/{},
                           std::make_shared<const Literal>(Literal::String("n/a")),
                           /*write_default=*/nullptr);
  TestSchemaFieldJsonConversion(
      initial_only,
      R"({"id":2,"name":"name","required":false,"type":"string","initial-default":"n/a"})"_json);

  // No defaults; neither key may appear.
  SchemaField no_defaults(/*field_id=*/3, "plain", int32(), /*optional=*/false);
  TestSchemaFieldJsonConversion(
      no_defaults, R"({"id":3,"name":"plain","required":true,"type":"int"})"_json);
}

// Round-trips a field carrying both defaults through ToJson -> FieldFromJson for
// every primitive type, exercising the per-type single-value serialization the
// default path reuses (date/timestamp/decimal/uuid/binary have non-trivial wire
// encodings).
TEST(JsonInternalTest, SchemaFieldDefaultValuesRoundTripAllTypes) {
  ICEBERG_UNWRAP_OR_FAIL(auto uuid_value,
                         Uuid::FromString("f79c3e09-677c-4bbd-a479-3f349cb785e7"));
  std::vector<std::pair<std::shared_ptr<Type>, Literal>> cases;
  cases.emplace_back(boolean(), Literal::Boolean(true));
  cases.emplace_back(int32(), Literal::Int(-7));
  cases.emplace_back(int64(), Literal::Long(1234567890123LL));
  cases.emplace_back(float32(), Literal::Float(1.5f));
  cases.emplace_back(float64(), Literal::Double(2.5));
  cases.emplace_back(date(), Literal::Date(19738));
  cases.emplace_back(time(), Literal::Time(43200000000LL));
  cases.emplace_back(timestamp(), Literal::Timestamp(1719446400000000LL));
  cases.emplace_back(timestamp_tz(), Literal::TimestampTz(1719446400000000LL));
  cases.emplace_back(timestamp_ns(), Literal::TimestampNs(1719446400000000123LL));
  cases.emplace_back(timestamptz_ns(), Literal::TimestampTzNs(1719446400000000123LL));
  cases.emplace_back(string(), Literal::String("hello"));
  cases.emplace_back(decimal(9, 2), Literal::Decimal(12345, 9, 2));
  cases.emplace_back(fixed(3), Literal::Fixed({0x01, 0x02, 0x03}));
  cases.emplace_back(binary(), Literal::Binary({0xDE, 0xAD, 0xBE, 0xEF}));
  cases.emplace_back(uuid(), Literal::UUID(uuid_value));

  int32_t field_id = 1;
  for (const auto& [type, literal] : cases) {
    SchemaField field(field_id++, "f", type, /*optional=*/false, /*doc=*/{},
                      std::make_shared<const Literal>(literal),
                      std::make_shared<const Literal>(literal));
    ICEBERG_UNWRAP_OR_FAIL(auto json, ToJson(field));
    ICEBERG_UNWRAP_OR_FAIL(auto parsed, FieldFromJson(json));
    EXPECT_EQ(field, *parsed) << "round-trip mismatch for type " << type->ToString()
                              << ", json=" << json.dump();
  }
}

// The spec only permits UTC offsets for timestamptz / timestamptz_ns default values.
// A non-UTC offset (which the shared parser would silently normalize) must be rejected,
// while the UTC form is accepted.
TEST(JsonInternalTest, SchemaFieldRejectsNonUtcTimestamptzDefault) {
  auto non_utc = nlohmann::json::parse(
      R"({"id":1,"name":"ts","required":true,"type":"timestamptz","initial-default":"2024-06-27T05:00:00+05:00"})");
  EXPECT_FALSE(FieldFromJson(non_utc).has_value());

  auto non_utc_ns = nlohmann::json::parse(
      R"({"id":1,"name":"ts","required":true,"type":"timestamptz_ns","write-default":"2024-06-27T05:00:00-08:00"})");
  EXPECT_FALSE(FieldFromJson(non_utc_ns).has_value());

  auto utc = nlohmann::json::parse(
      R"({"id":1,"name":"ts","required":true,"type":"timestamptz","initial-default":"2024-06-27T00:00:00+00:00"})");
  EXPECT_TRUE(FieldFromJson(utc).has_value());
}

TEST(JsonInternalTest, SchemaIdsRejectInvalidJsonIntegers) {
  const nlohmann::json valid_schema = R"({
    "type": "struct",
    "schema-id": 10,
    "identifier-field-ids": [20],
    "fields": [
      {"id": 20, "name": "id", "required": true, "type": "int"},
      {
        "id": 30,
        "name": "items",
        "required": false,
        "type": {
          "type": "list",
          "element-id": 40,
          "element-required": false,
          "element": {
            "type": "map",
            "key-id": 50,
            "key": "string",
            "value-id": 60,
            "value": "long",
            "value-required": false
          }
        }
      }
    ]
  })"_json;

  struct IdPath {
    nlohmann::json::json_pointer pointer;
    std::string_view key;
  };
  const std::array<IdPath, 6> id_paths = {{
      {nlohmann::json::json_pointer("/schema-id"), "schema-id"},
      {nlohmann::json::json_pointer("/identifier-field-ids/0"), "identifier-field-ids"},
      {nlohmann::json::json_pointer("/fields/0/id"), "id"},
      {nlohmann::json::json_pointer("/fields/1/type/element-id"), "element-id"},
      {nlohmann::json::json_pointer("/fields/1/type/element/key-id"), "key-id"},
      {nlohmann::json::json_pointer("/fields/1/type/element/value-id"), "value-id"},
  }};
  const std::array<std::pair<nlohmann::json, std::string_view>, 3> invalid_ids = {{
      {nlohmann::json(1.5), "must be an integer"},
      {nlohmann::json(true), "must be an integer"},
      {nlohmann::json(2147483648ULL), "out of range"},
  }};

  for (const auto& id_path : id_paths) {
    for (const auto& [invalid_id, expected_error] : invalid_ids) {
      SCOPED_TRACE(id_path.pointer.to_string() + "=" + invalid_id.dump());
      auto invalid_schema = valid_schema;
      invalid_schema[id_path.pointer] = invalid_id;

      auto result = SchemaFromJson(invalid_schema);
      EXPECT_THAT(result, IsError(ErrorKind::kJsonParseError));
      EXPECT_THAT(result, HasErrorMessage(id_path.key));
      EXPECT_THAT(result, HasErrorMessage(expected_error));
    }
  }
}

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

TEST(JsonInternalTest, SortOrderFromJson) {
  auto identity_transform = Transform::Identity();
  SortField st1(5, identity_transform, SortDirection::kAscending, NullOrder::kFirst);
  SortField st2(7, identity_transform, SortDirection::kDescending, NullOrder::kLast);
  ICEBERG_UNWRAP_OR_FAIL(auto sort_order, SortOrder::Make(100, {st1, st2}));

  auto json = ToJson(*sort_order);
  ICEBERG_UNWRAP_OR_FAIL(auto parsed, SortOrderFromJson(json));
  EXPECT_EQ(*sort_order, *parsed);
}

TEST(JsonInternalTest, PartitionSpecFromJson) {
  auto identity_transform = Transform::Identity();
  ICEBERG_UNWRAP_OR_FAIL(
      auto spec,
      PartitionSpec::Make(1, {PartitionField(3, 101, "region", identity_transform),
                              PartitionField(5, 102, "ts", identity_transform)}));

  auto json = ToJson(*spec);
  ICEBERG_UNWRAP_OR_FAIL(auto parsed, PartitionSpecFromJson(json));
  EXPECT_EQ(*spec, *parsed);
}

TEST(JsonInternalTest, PartitionIdsRejectInvalidJsonIntegers) {
  const nlohmann::json valid_spec = R"({
    "spec-id": 10,
    "fields": [{
      "source-id": 20,
      "field-id": 1000,
      "transform": "identity",
      "name": "id"
    }]
  })"_json;
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField(20, "id", int32(), false)},
      /*schema_id=*/10);

  struct IdPath {
    nlohmann::json::json_pointer pointer;
    std::string_view key;
  };
  const std::array<IdPath, 3> id_paths = {{
      {nlohmann::json::json_pointer("/spec-id"), "spec-id"},
      {nlohmann::json::json_pointer("/fields/0/source-id"), "source-id"},
      {nlohmann::json::json_pointer("/fields/0/field-id"), "field-id"},
  }};
  const std::array<std::pair<nlohmann::json, std::string_view>, 3> invalid_ids = {{
      {nlohmann::json(1.5), "must be an integer"},
      {nlohmann::json(true), "must be an integer"},
      {nlohmann::json(2147483648ULL), "out of range"},
  }};

  for (const auto& id_path : id_paths) {
    for (const auto& [invalid_id, expected_error] : invalid_ids) {
      SCOPED_TRACE(id_path.pointer.to_string() + "=" + invalid_id.dump());
      auto invalid_spec = valid_spec;
      invalid_spec[id_path.pointer] = invalid_id;

      auto unbound_result = PartitionSpecFromJson(invalid_spec);
      EXPECT_THAT(unbound_result, IsError(ErrorKind::kJsonParseError));
      EXPECT_THAT(unbound_result, HasErrorMessage(id_path.key));
      EXPECT_THAT(unbound_result, HasErrorMessage(expected_error));

      auto bound_result = PartitionSpecFromJson(schema, invalid_spec, 10);
      EXPECT_THAT(bound_result, IsError(ErrorKind::kJsonParseError));
      EXPECT_THAT(bound_result, HasErrorMessage(id_path.key));
      EXPECT_THAT(bound_result, HasErrorMessage(expected_error));
    }
  }

  auto legacy_field = valid_spec["fields"][0];
  legacy_field.erase("field-id");
  ICEBERG_UNWRAP_OR_FAIL(
      auto parsed_legacy_field,
      PartitionFieldFromJson(legacy_field, /*allow_field_id_missing=*/true));
  EXPECT_EQ(parsed_legacy_field->field_id(), SchemaField::kInvalidFieldId);

  for (const auto& [invalid_id, expected_error] : invalid_ids) {
    SCOPED_TRACE("legacy field-id=" + invalid_id.dump());
    legacy_field["field-id"] = invalid_id;
    auto result = PartitionFieldFromJson(legacy_field, /*allow_field_id_missing=*/true);
    EXPECT_THAT(result, IsError(ErrorKind::kJsonParseError));
    EXPECT_THAT(result, HasErrorMessage("field-id"));
    EXPECT_THAT(result, HasErrorMessage(expected_error));
  }
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
                    .timestamp_ms = TimePointMsFromUnixMs(1234567890123),
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

TEST(JsonInternalTest, SnapshotRowLineageSerializesTopLevelFields) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto snapshot,
      Snapshot::Make(/*sequence_number=*/99, /*snapshot_id=*/1234567890,
                     /*parent_snapshot_id=*/9876543210,
                     TimePointMsFromUnixMs(1234567890123), DataOperation::kAppend,
                     {{SnapshotSummaryFields::kAddedDataFiles, "50"}},
                     /*schema_id=*/42, "/path/to/manifest_list",
                     /*first_row_id=*/100, /*added_rows=*/25));

  auto json = ToJson(*snapshot);
  EXPECT_EQ(json["first-row-id"], 100);
  EXPECT_EQ(json["added-rows"], 25);
  EXPECT_FALSE(json["summary"].contains("first-row-id"));
  EXPECT_FALSE(json["summary"].contains("added-rows"));
}

TEST(JsonInternalTest, SnapshotFromJsonReadsTopLevelRowLineageFields) {
  nlohmann::json snapshot_json =
      R"({"snapshot-id":1234567890,
          "parent-snapshot-id":9876543210,
          "sequence-number":99,
          "timestamp-ms":1234567890123,
          "manifest-list":"/path/to/manifest_list",
          "summary":{
            "operation":"append",
            "added-data-files":"50",
            "first-row-id":"101",
            "added-rows":"26"
          },
          "schema-id":42,
          "first-row-id":100,
          "added-rows":25})"_json;

  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, SnapshotFromJson(snapshot_json));
  ICEBERG_UNWRAP_OR_FAIL(auto first_row_id, snapshot->FirstRowId());
  ICEBERG_UNWRAP_OR_FAIL(auto added_rows, snapshot->AddedRows());
  EXPECT_EQ(first_row_id, 100);
  EXPECT_EQ(added_rows, 25);

  auto json = ToJson(*snapshot);
  EXPECT_EQ(json["first-row-id"], 100);
  EXPECT_EQ(json["added-rows"], 25);
  EXPECT_EQ(json["summary"]["first-row-id"], "101");
  EXPECT_EQ(json["summary"]["added-rows"], "26");
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

  ICEBERG_UNWRAP_OR_FAIL(auto json, ToJson(update));
  EXPECT_EQ(json, expected);
  auto parsed = TableUpdateFromJson(expected);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(*internal::checked_cast<table::AssignUUID*>(parsed.value().get()), update);
}

TEST(JsonInternalTest, TableUpdateUpgradeFormatVersion) {
  table::UpgradeFormatVersion update(2);
  nlohmann::json expected =
      R"({"action":"upgrade-format-version","format-version":2})"_json;

  ICEBERG_UNWRAP_OR_FAIL(auto json, ToJson(update));
  EXPECT_EQ(json, expected);
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

  ICEBERG_UNWRAP_OR_FAIL(auto json, ToJson(update));
  EXPECT_EQ(json["action"], "add-schema");
  EXPECT_EQ(json["last-column-id"], 2);
  EXPECT_TRUE(json.contains("schema"));

  auto parsed = TableUpdateFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  auto* actual = internal::checked_cast<table::AddSchema*>(parsed.value().get());
  EXPECT_EQ(actual->last_column_id(), update.last_column_id());
  EXPECT_EQ(*actual->schema(), *update.schema());
}

TEST(JsonInternalTest, TableUpdateAddSchemaWithoutDeprecatedLastColumnId) {
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField(1, "id", int64(), false),
                               SchemaField(3, "name", string(), true)},
      /*schema_id=*/1);
  ICEBERG_UNWRAP_OR_FAIL(auto schema_json, ToJson(*schema));
  nlohmann::json json = {
      {"action", "add-schema"},
      {"schema", schema_json},
  };

  auto parsed = TableUpdateFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  auto* actual = internal::checked_cast<table::AddSchema*>(parsed.value().get());
  EXPECT_EQ(*actual->schema(), *schema);
  EXPECT_EQ(actual->last_column_id(), 3);
}

TEST(JsonInternalTest, TableUpdateSetCurrentSchema) {
  table::SetCurrentSchema update(1);
  nlohmann::json expected = R"({"action":"set-current-schema","schema-id":1})"_json;

  ICEBERG_UNWRAP_OR_FAIL(auto json, ToJson(update));
  EXPECT_EQ(json, expected);
  auto parsed = TableUpdateFromJson(expected);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(*internal::checked_cast<table::SetCurrentSchema*>(parsed.value().get()),
            update);
}

TEST(JsonInternalTest, TableUpdateAddPartitionSpec) {
  auto identity_transform = Transform::Identity();
  ICEBERG_UNWRAP_OR_FAIL(
      auto spec,
      PartitionSpec::Make(1, {PartitionField(3, 101, "region", identity_transform)}));
  table::AddPartitionSpec update(std::move(spec));

  ICEBERG_UNWRAP_OR_FAIL(auto json, ToJson(update));
  EXPECT_EQ(json["action"], "add-spec");
  EXPECT_TRUE(json.contains("spec"));

  auto parsed = TableUpdateFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  auto* actual = internal::checked_cast<table::AddPartitionSpec*>(parsed.value().get());
  EXPECT_EQ(*actual->spec(), *update.spec());
}

TEST(JsonInternalTest, TableUpdateSetDefaultPartitionSpec) {
  table::SetDefaultPartitionSpec update(2);
  nlohmann::json expected = R"({"action":"set-default-spec","spec-id":2})"_json;

  ICEBERG_UNWRAP_OR_FAIL(auto json, ToJson(update));
  EXPECT_EQ(json, expected);
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

  ICEBERG_UNWRAP_OR_FAIL(auto json, ToJson(update));
  EXPECT_EQ(json, expected);
  auto parsed = TableUpdateFromJson(expected);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(*internal::checked_cast<table::RemovePartitionSpecs*>(parsed.value().get()),
            update);
}

TEST(JsonInternalTest, TableUpdateRemoveSchemas) {
  table::RemoveSchemas update({1, 2});

  ICEBERG_UNWRAP_OR_FAIL(auto json, ToJson(update));
  EXPECT_EQ(json["action"], "remove-schemas");
  EXPECT_THAT(json["schema-ids"].get<std::vector<int32_t>>(),
              testing::UnorderedElementsAre(1, 2));

  auto parsed = TableUpdateFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(*internal::checked_cast<table::RemoveSchemas*>(parsed.value().get()), update);
}

TEST(JsonInternalTest, TableUpdateAddSortOrder) {
  auto identity_transform = Transform::Identity();
  SortField st(5, identity_transform, SortDirection::kAscending, NullOrder::kFirst);
  ICEBERG_UNWRAP_OR_FAIL(auto sort_order, SortOrder::Make(1, {st}));
  table::AddSortOrder update(std::move(sort_order));

  ICEBERG_UNWRAP_OR_FAIL(auto json, ToJson(update));
  EXPECT_EQ(json["action"], "add-sort-order");
  EXPECT_TRUE(json.contains("sort-order"));

  auto parsed = TableUpdateFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  auto* actual = internal::checked_cast<table::AddSortOrder*>(parsed.value().get());
  EXPECT_EQ(*actual->sort_order(), *update.sort_order());
}

TEST(JsonInternalTest, TableUpdateSetDefaultSortOrder) {
  table::SetDefaultSortOrder update(1);
  nlohmann::json expected =
      R"({"action":"set-default-sort-order","sort-order-id":1})"_json;

  ICEBERG_UNWRAP_OR_FAIL(auto json, ToJson(update));
  EXPECT_EQ(json, expected);
  auto parsed = TableUpdateFromJson(expected);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(*internal::checked_cast<table::SetDefaultSortOrder*>(parsed.value().get()),
            update);
}

TEST(JsonInternalTest, TableUpdateAddSnapshot) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto snapshot_unique,
      Snapshot::Make(/*sequence_number=*/5, /*snapshot_id=*/123456789,
                     /*parent_snapshot_id=*/987654321,
                     TimePointMsFromUnixMs(1234567890000), DataOperation::kAppend,
                     /*summary=*/{}, /*schema_id=*/1, "/path/to/manifest-list.avro",
                     /*first_row_id=*/100, /*added_rows=*/25));
  std::shared_ptr<Snapshot> snapshot(std::move(snapshot_unique));
  table::AddSnapshot update(snapshot);

  ICEBERG_UNWRAP_OR_FAIL(auto json, ToJson(update));
  EXPECT_EQ(json["action"], "add-snapshot");
  EXPECT_TRUE(json.contains("snapshot"));
  EXPECT_EQ(json["snapshot"]["first-row-id"], 100);
  EXPECT_EQ(json["snapshot"]["added-rows"], 25);

  auto parsed = TableUpdateFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  auto* actual = internal::checked_cast<table::AddSnapshot*>(parsed.value().get());
  EXPECT_EQ(*actual->snapshot(), *update.snapshot());
}

TEST(JsonInternalTest, TableUpdateRemoveSnapshots) {
  table::RemoveSnapshots update({111, 222, 333});
  nlohmann::json expected =
      R"({"action":"remove-snapshots","snapshot-ids":[111,222,333]})"_json;

  ICEBERG_UNWRAP_OR_FAIL(auto json, ToJson(update));
  EXPECT_EQ(json, expected);
  auto parsed = TableUpdateFromJson(expected);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(*internal::checked_cast<table::RemoveSnapshots*>(parsed.value().get()),
            update);
}

TEST(JsonInternalTest, TableUpdateRemoveSnapshotRef) {
  table::RemoveSnapshotRef update("my-branch");
  nlohmann::json expected =
      R"({"action":"remove-snapshot-ref","ref-name":"my-branch"})"_json;

  ICEBERG_UNWRAP_OR_FAIL(auto json, ToJson(update));
  EXPECT_EQ(json, expected);
  auto parsed = TableUpdateFromJson(expected);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(*internal::checked_cast<table::RemoveSnapshotRef*>(parsed.value().get()),
            update);
}

TEST(JsonInternalTest, TableUpdateSetSnapshotRefBranch) {
  table::SetSnapshotRef update("main", 123456789, SnapshotRefType::kBranch, 5, 86400000,
                               604800000);

  ICEBERG_UNWRAP_OR_FAIL(auto json, ToJson(update));
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

  ICEBERG_UNWRAP_OR_FAIL(auto json, ToJson(update));
  EXPECT_EQ(json["action"], "set-snapshot-ref");
  EXPECT_EQ(json["type"], "tag");

  auto parsed = TableUpdateFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(*internal::checked_cast<table::SetSnapshotRef*>(parsed.value().get()),
            update);
}

TEST(JsonInternalTest, TableUpdateSetProperties) {
  table::SetProperties update({{"key1", "value1"}, {"key2", "value2"}});

  ICEBERG_UNWRAP_OR_FAIL(auto json, ToJson(update));
  EXPECT_EQ(json["action"], "set-properties");
  EXPECT_TRUE(json.contains("updates"));

  auto parsed = TableUpdateFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(*internal::checked_cast<table::SetProperties*>(parsed.value().get()), update);
}

TEST(JsonInternalTest, TableUpdateSetPropertiesLegacyUpdatedField) {
  nlohmann::json json =
      R"({"action":"set-properties","updated":{"key1":"value1","key2":"value2"}})"_json;

  auto parsed = TableUpdateFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  table::SetProperties expected({{"key1", "value1"}, {"key2", "value2"}});
  EXPECT_EQ(*internal::checked_cast<table::SetProperties*>(parsed.value().get()),
            expected);
}

TEST(JsonInternalTest, TableUpdateSetPropertiesMissingCanonicalField) {
  auto parsed = TableUpdateFromJson(R"({"action":"set-properties"})"_json);
  EXPECT_THAT(parsed, IsError(ErrorKind::kJsonParseError));
  EXPECT_THAT(parsed, HasErrorMessage("Missing 'updates'"));
}

TEST(JsonInternalTest, TableUpdateRemoveProperties) {
  table::RemoveProperties update({"key1", "key2"});

  ICEBERG_UNWRAP_OR_FAIL(auto json, ToJson(update));
  EXPECT_EQ(json["action"], "remove-properties");
  EXPECT_TRUE(json.contains("removals"));

  auto parsed = TableUpdateFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(*internal::checked_cast<table::RemoveProperties*>(parsed.value().get()),
            update);
}

TEST(JsonInternalTest, TableUpdateRemovePropertiesLegacyRemovedField) {
  nlohmann::json json =
      R"({"action":"remove-properties","removed":["key1","key2"]})"_json;

  auto parsed = TableUpdateFromJson(json);
  ASSERT_THAT(parsed, IsOk());
  table::RemoveProperties expected({"key1", "key2"});
  EXPECT_EQ(*internal::checked_cast<table::RemoveProperties*>(parsed.value().get()),
            expected);
}

TEST(JsonInternalTest, TableUpdateRemovePropertiesMissingCanonicalField) {
  auto parsed = TableUpdateFromJson(R"({"action":"remove-properties"})"_json);
  EXPECT_THAT(parsed, IsError(ErrorKind::kJsonParseError));
  EXPECT_THAT(parsed, HasErrorMessage("Missing 'removals'"));
}

TEST(JsonInternalTest, TableUpdateSetLocation) {
  table::SetLocation update("s3://bucket/warehouse/table");
  nlohmann::json expected =
      R"({"action":"set-location","location":"s3://bucket/warehouse/table"})"_json;

  ICEBERG_UNWRAP_OR_FAIL(auto json, ToJson(update));
  EXPECT_EQ(json, expected);
  auto parsed = TableUpdateFromJson(expected);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(*internal::checked_cast<table::SetLocation*>(parsed.value().get()), update);
}

TEST(JsonInternalTest, TableUpdateSetStatistics) {
  auto stats_file = std::make_shared<StatisticsFile>();
  stats_file->snapshot_id = 123456789;
  stats_file->path = "s3://bucket/warehouse/table/metadata/stats-123456789.puffin";
  stats_file->file_size_in_bytes = 1024;
  stats_file->file_footer_size_in_bytes = 128;
  stats_file->blob_metadata = {BlobMetadata{.type = "ndv",
                                            .source_snapshot_id = 123456789,
                                            .source_snapshot_sequence_number = 1,
                                            .fields = {1, 2},
                                            .properties = {{"prop1", "value1"}}}};

  table::SetStatistics update(stats_file);
  nlohmann::json expected = R"({
    "action": "set-statistics",
    "statistics": {
      "snapshot-id": 123456789,
      "statistics-path": "s3://bucket/warehouse/table/metadata/stats-123456789.puffin",
      "file-size-in-bytes": 1024,
      "file-footer-size-in-bytes": 128,
      "blob-metadata": [{
        "type": "ndv",
        "snapshot-id": 123456789,
        "sequence-number": 1,
        "fields": [1, 2],
        "properties": {"prop1": "value1"}
      }]
    }
  })"_json;

  ICEBERG_UNWRAP_OR_FAIL(auto json, ToJson(update));
  EXPECT_EQ(json, expected);
  auto parsed = TableUpdateFromJson(expected);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(*internal::checked_cast<table::SetStatistics*>(parsed.value().get()), update);
}

TEST(JsonInternalTest, TableUpdateRemoveStatistics) {
  table::RemoveStatistics update(123456789);
  nlohmann::json expected =
      R"({"action":"remove-statistics","snapshot-id":123456789})"_json;

  ICEBERG_UNWRAP_OR_FAIL(auto json, ToJson(update));
  EXPECT_EQ(json, expected);
  auto parsed = TableUpdateFromJson(expected);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(*internal::checked_cast<table::RemoveStatistics*>(parsed.value().get()),
            update);
}

TEST(JsonInternalTest, TableUpdateSetPartitionStatistics) {
  auto partition_stats_file = std::make_shared<PartitionStatisticsFile>();
  partition_stats_file->snapshot_id = 123456789;
  partition_stats_file->path =
      "s3://bucket/warehouse/table/metadata/partition-stats-123456789.parquet";
  partition_stats_file->file_size_in_bytes = 2048;

  table::SetPartitionStatistics update(partition_stats_file);
  nlohmann::json expected = R"({
    "action": "set-partition-statistics",
    "partition-statistics": {
      "snapshot-id": 123456789,
      "statistics-path": "s3://bucket/warehouse/table/metadata/partition-stats-123456789.parquet",
      "file-size-in-bytes": 2048
    }
  })"_json;

  ICEBERG_UNWRAP_OR_FAIL(auto json, ToJson(update));
  EXPECT_EQ(json, expected);
  auto parsed = TableUpdateFromJson(expected);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(*internal::checked_cast<table::SetPartitionStatistics*>(parsed.value().get()),
            update);
}

TEST(JsonInternalTest, TableUpdateRemovePartitionStatistics) {
  table::RemovePartitionStatistics update(123456789);
  nlohmann::json expected =
      R"({"action":"remove-partition-statistics","snapshot-id":123456789})"_json;

  ICEBERG_UNWRAP_OR_FAIL(auto json, ToJson(update));
  EXPECT_EQ(json, expected);
  auto parsed = TableUpdateFromJson(expected);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(
      *internal::checked_cast<table::RemovePartitionStatistics*>(parsed.value().get()),
      update);
}

TEST(JsonInternalTest, TableUpdateAddEncryptionKey) {
  EncryptedKey key{
      .key_id = "key-1",
      .encrypted_key_metadata = "secret-key-metadata",
      .encrypted_by_id = "kek-1",
      .properties = {{"scope", "table"}},
  };
  table::AddEncryptionKey update(key);

  nlohmann::json expected = {
      {"action", "add-encryption-key"},
      {"encryption-key",
       {{"key-id", "key-1"},
        {"encrypted-key-metadata", Base64::Encode("secret-key-metadata")},
        {"encrypted-by-id", "kek-1"},
        {"properties", {{"scope", "table"}}}}},
  };

  ICEBERG_UNWRAP_OR_FAIL(auto json, ToJson(update));
  EXPECT_EQ(json, expected);
  auto parsed = TableUpdateFromJson(expected);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(*internal::checked_cast<table::AddEncryptionKey*>(parsed.value().get()),
            update);
}

TEST(JsonInternalTest, TableUpdateAddEncryptionKeyRejectsNonObjectKey) {
  nlohmann::json json = R"({"action":"add-encryption-key","encryption-key":null})"_json;

  auto parsed = TableUpdateFromJson(json);
  EXPECT_THAT(parsed, IsError(ErrorKind::kJsonParseError));
  EXPECT_THAT(parsed, HasErrorMessage("Invalid encryption key"));
}

TEST(JsonInternalTest, TableUpdateRemoveEncryptionKey) {
  table::RemoveEncryptionKey update("key-1");
  nlohmann::json expected = R"({"action":"remove-encryption-key","key-id":"key-1"})"_json;

  ICEBERG_UNWRAP_OR_FAIL(auto json, ToJson(update));
  EXPECT_EQ(json, expected);
  auto parsed = TableUpdateFromJson(expected);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(*internal::checked_cast<table::RemoveEncryptionKey*>(parsed.value().get()),
            update);
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
      R"({"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":123456789})"_json;

  EXPECT_EQ(ToJson(req), expected);
  auto parsed = TableRequirementFromJson(expected);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(*internal::checked_cast<table::AssertRefSnapshotID*>(parsed.value().get()),
            req);
}

TEST(TableRequirementJsonTest, TableRequirementAssertRefSnapshotIDWithNull) {
  table::AssertRefSnapshotID req("main", std::nullopt);
  nlohmann::json expected =
      R"({"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":null})"_json;

  EXPECT_EQ(ToJson(req), expected);
  auto parsed = TableRequirementFromJson(expected);
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(*internal::checked_cast<table::AssertRefSnapshotID*>(parsed.value().get()),
            req);
}

TEST(TableRequirementJsonTest, TableRequirementAssertRefSnapshotIDRejectsRefName) {
  nlohmann::json legacy =
      R"({"type":"assert-ref-snapshot-id","ref-name":"main","snapshot-id":123456789})"_json;
  auto result = TableRequirementFromJson(legacy);
  EXPECT_THAT(result, IsError(ErrorKind::kJsonParseError));
  EXPECT_THAT(result, HasErrorMessage("Missing 'ref'"));
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

class FileScanTaskJsonTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ICEBERG_UNWRAP_OR_FAIL(
        auto spec,
        PartitionSpec::Make(schema_, /*spec_id=*/0,
                            {PartitionField(1, 1000, "id", Transform::Identity())},
                            /*allow_missing_fields=*/false));
    spec_ = std::shared_ptr<PartitionSpec>(std::move(spec));
    specs_.emplace(spec_->spec_id(), spec_);
  }

  DataFile DataFileForTask() const {
    DataFile data_file;
    data_file.content = DataFile::Content::kData;
    data_file.file_path = "/path/to/data.parquet";
    data_file.file_format = FileFormatType::kParquet;
    data_file.partition_spec_id = 0;
    data_file.partition = PartitionValues({Literal::Int(7)});
    data_file.record_count = 1;
    data_file.file_size_in_bytes = 10;
    data_file.lower_bounds = {{1, {0x01, 0x00, 0x00, 0x00}}};
    data_file.upper_bounds = {{1, {0x05, 0x00, 0x00, 0x00}}};
    data_file.key_metadata = {0x0A, 0x0B};
    data_file.sort_order_id = 0;
    return data_file;
  }

  DataFile DeleteFileForTask() const {
    DataFile delete_file;
    delete_file.content = DataFile::Content::kPositionDeletes;
    delete_file.file_path = "/path/to/delete.parquet";
    delete_file.file_format = FileFormatType::kParquet;
    delete_file.partition_spec_id = 0;
    delete_file.partition = PartitionValues({Literal::Int(7)});
    delete_file.record_count = 1;
    delete_file.file_size_in_bytes = 4;
    return delete_file;
  }

  nlohmann::json JavaGolden() const {
    return R"({
      "task-type": "file-scan-task",
      "schema": {
        "type": "struct",
        "schema-id": 0,
        "fields": [
          {"id": 1, "name": "id", "required": true, "type": "int"}
        ]
      },
      "spec": {
        "spec-id": 0,
        "fields": [
          {"source-id": 1, "field-id": 1000, "name": "id", "transform": "identity"}
        ]
      },
      "data-file": {
        "spec-id": 0,
        "content": "data",
        "file-path": "/path/to/data.parquet",
        "file-format": "parquet",
        "partition": [7],
        "file-size-in-bytes": 10,
        "record-count": 1,
        "lower-bounds": {"keys": [1], "values": ["01000000"]},
        "upper-bounds": {"keys": [1], "values": ["05000000"]},
        "key-metadata": "0A0B",
        "sort-order-id": 0
      },
      "start": 0,
      "length": 10,
      "delete-files": [
        {
          "spec-id": 0,
          "content": "position-deletes",
          "file-path": "/path/to/delete.parquet",
          "file-format": "parquet",
          "partition": [7],
          "file-size-in-bytes": 4,
          "record-count": 1
        }
      ],
      "residual-filter": true
    })"_json;
  }

  Schema schema_{{SchemaField::MakeRequired(1, "id", int32())}, 0};
  std::shared_ptr<PartitionSpec> spec_;
  std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> specs_;
};

TEST_F(FileScanTaskJsonTest, SerializesJavaGolden) {
  FileScanTask task(std::make_shared<DataFile>(DataFileForTask()),
                    {std::make_shared<DataFile>(DeleteFileForTask())},
                    Expressions::AlwaysTrue());

  ICEBERG_UNWRAP_OR_FAIL(auto json, ToJson(task, specs_, schema_));
  EXPECT_EQ(json, JavaGolden());
}

TEST_F(FileScanTaskJsonTest, ParsesJavaGoldenWithoutExternalSchemaOrSpec) {
  ICEBERG_UNWRAP_OR_FAIL(auto task, FileScanTaskFromJson(JavaGolden()));

  ASSERT_NE(task->data_file(), nullptr);
  EXPECT_EQ(*task->data_file(), DataFileForTask());
  ASSERT_EQ(task->delete_files().size(), 1U);
  ASSERT_NE(task->delete_files()[0], nullptr);
  EXPECT_EQ(*task->delete_files()[0], DeleteFileForTask());
  ASSERT_NE(task->residual_filter(), nullptr);
  ICEBERG_UNWRAP_OR_FAIL(auto residual, ToJson(*task->residual_filter()));
  EXPECT_EQ(residual, true);
}

TEST_F(FileScanTaskJsonTest, SerializesEmptyDeleteFilesArray) {
  FileScanTask task(std::make_shared<DataFile>(DataFileForTask()));

  ICEBERG_UNWRAP_OR_FAIL(auto json, ToJson(task, specs_, schema_));
  EXPECT_EQ(json["delete-files"], nlohmann::json::array());
  EXPECT_EQ(json["start"], 0);
  EXPECT_EQ(json["length"], 10);
}

TEST_F(FileScanTaskJsonTest, RequiresJavaCoreFields) {
  for (std::string_view field : {"schema", "spec", "start", "length"}) {
    for (bool use_null : {false, true}) {
      SCOPED_TRACE(std::string(field) + (use_null ? " null" : " missing"));
      auto json = JavaGolden();
      if (use_null) {
        json[field] = nullptr;
      } else {
        json.erase(field);
      }

      auto result = FileScanTaskFromJson(json);
      EXPECT_THAT(result, IsError(ErrorKind::kJsonParseError));
      EXPECT_THAT(result, HasErrorMessage(field));
    }
  }
}

TEST_F(FileScanTaskJsonTest, RejectsSplitTasksUsingExactOrCondition) {
  struct Case {
    int64_t start;
    int64_t length;
    bool supported;
  };
  for (const auto& test_case :
       {Case{0, 10, true}, Case{1, 10, false}, Case{0, 9, false}}) {
    SCOPED_TRACE(testing::Message()
                 << "start=" << test_case.start << ", length=" << test_case.length);
    auto json = JavaGolden();
    json["start"] = test_case.start;
    json["length"] = test_case.length;

    auto result = FileScanTaskFromJson(json);
    if (test_case.supported) {
      EXPECT_THAT(result, IsOk());
    } else {
      EXPECT_THAT(result, IsError(ErrorKind::kNotSupported));
      EXPECT_THAT(result, HasErrorMessage("Split FileScanTask is not supported"));
    }
  }
}

TEST_F(FileScanTaskJsonTest, RejectsFractionalIntegerFields) {
  for (std::string_view field :
       {"start", "length", "spec-id", "file-size-in-bytes", "record-count",
        "sort-order-id", "first-row-id", "content-offset", "content-size-in-bytes"}) {
    SCOPED_TRACE(field);
    auto json = JavaGolden();
    if (field == "start" || field == "length") {
      json[field] = 0.5;
    } else {
      json["data-file"][field] = 0.5;
    }

    auto result = FileScanTaskFromJson(json);
    EXPECT_THAT(result, IsError(ErrorKind::kJsonParseError));
    EXPECT_THAT(result, HasErrorMessage(field));
  }

  for (std::string_view field : {"split-offsets", "equality-ids"}) {
    SCOPED_TRACE(field);
    auto json = JavaGolden();
    json["data-file"][field] = {0.5};

    auto result = FileScanTaskFromJson(json);
    EXPECT_THAT(result, IsError(ErrorKind::kJsonParseError));
    EXPECT_THAT(result, HasErrorMessage(field));
  }

  auto scalar_map = JavaGolden();
  scalar_map["data-file"]["column-sizes"] = {{"keys", {1}}, {"values", {0.5}}};
  EXPECT_THAT(FileScanTaskFromJson(scalar_map), IsError(ErrorKind::kJsonParseError));

  auto bytes_map = JavaGolden();
  bytes_map["data-file"]["lower-bounds"] = {{"keys", {0.5}}, {"values", {"00"}}};
  EXPECT_THAT(FileScanTaskFromJson(bytes_map), IsError(ErrorKind::kJsonParseError));
}

TEST_F(FileScanTaskJsonTest, DoesNotTreatOffsetAsStartAlias) {
  auto json = JavaGolden();
  json.erase("start");
  json["offset"] = 0;

  auto result = FileScanTaskFromJson(json);
  EXPECT_THAT(result, IsError(ErrorKind::kJsonParseError));
  EXPECT_THAT(result, HasErrorMessage("start"));
}

TEST_F(FileScanTaskJsonTest, ValidatesTaskTypeWhenPresent) {
  auto json = JavaGolden();
  json["task-type"] = "FILE-SCAN-TASK";
  EXPECT_THAT(FileScanTaskFromJson(json), IsOk());

  json["task-type"] = nullptr;
  EXPECT_THAT(FileScanTaskFromJson(json), IsOk());

  json.erase("task-type");
  EXPECT_THAT(FileScanTaskFromJson(json), IsOk());

  json["task-type"] = "data-task";
  auto result = FileScanTaskFromJson(json);
  EXPECT_THAT(result, IsError(ErrorKind::kJsonParseError));
  EXPECT_THAT(result, HasErrorMessage("task type"));
}

TEST_F(FileScanTaskJsonTest, ParsesPartitionObjectsByFieldId) {
  auto json = JavaGolden();
  json["data-file"]["partition"] = {{"1000", 7}};

  ICEBERG_UNWRAP_OR_FAIL(auto task, FileScanTaskFromJson(json));
  ASSERT_EQ(task->data_file()->partition.num_fields(), 1U);
  EXPECT_EQ(task->data_file()->partition.values()[0], Literal::Int(7));
}

TEST_F(FileScanTaskJsonTest, ParsesNullPartitionValues) {
  const std::vector<nlohmann::json> null_partitions = {
      nlohmann::json::object(), {{"1000", nullptr}}, nlohmann::json::array({nullptr})};
  for (const auto& partition : null_partitions) {
    SCOPED_TRACE(partition.dump());
    auto json = JavaGolden();
    json["data-file"]["partition"] = partition;

    ICEBERG_UNWRAP_OR_FAIL(auto task, FileScanTaskFromJson(json));
    ASSERT_EQ(task->data_file()->partition.num_fields(), 1U);
    const auto& value = task->data_file()->partition.values()[0];
    EXPECT_TRUE(value.IsNull());
    ASSERT_NE(value.type(), nullptr);
    EXPECT_EQ(value.type()->type_id(), TypeId::kInt);
  }
}

TEST_F(FileScanTaskJsonTest, AcceptsMissingPartitionLikeJava) {
  auto json = JavaGolden();
  json["data-file"].erase("partition");

  ICEBERG_UNWRAP_OR_FAIL(auto task, FileScanTaskFromJson(json));
  EXPECT_EQ(task->data_file()->partition.num_fields(), 0U);
}

TEST_F(FileScanTaskJsonTest, AcceptsNullMetricMapsLikeJava) {
  auto json = JavaGolden();
  for (std::string_view field : {"column-sizes", "value-counts", "null-value-counts",
                                 "nan-value-counts", "lower-bounds", "upper-bounds"}) {
    json["data-file"][field] = nullptr;
  }

  EXPECT_THAT(FileScanTaskFromJson(json), IsOk());
}

TEST_F(FileScanTaskJsonTest, RejectsDuplicateMetricMapKeys) {
  for (std::string_view field : {"column-sizes", "lower-bounds"}) {
    SCOPED_TRACE(field);
    auto json = JavaGolden();
    json["data-file"][field] = {
        {"keys", {1, 1}},
        {"values", field == "lower-bounds" ? nlohmann::json({"00", "01"})
                                           : nlohmann::json({1, 2})}};

    auto result = FileScanTaskFromJson(json);
    EXPECT_THAT(result, IsError(ErrorKind::kJsonParseError));
    EXPECT_THAT(result, HasErrorMessage("duplicate key"));
  }
}

TEST_F(FileScanTaskJsonTest, AcceptsLegacyContentEnumNames) {
  auto json = JavaGolden();
  json["data-file"]["content"] = "DATA";
  json["delete-files"][0]["content"] = "POSITION_DELETES";

  EXPECT_THAT(FileScanTaskFromJson(json), IsOk());
}

TEST_F(FileScanTaskJsonTest, RejectsInvalidContentRolesWhenParsing) {
  auto invalid_data = JavaGolden();
  invalid_data["data-file"]["content"] = "position-deletes";
  auto data_result = FileScanTaskFromJson(invalid_data);
  EXPECT_THAT(data_result, IsError(ErrorKind::kJsonParseError));
  EXPECT_THAT(data_result, HasErrorMessage("data-file"));

  auto invalid_delete = JavaGolden();
  invalid_delete["delete-files"][0]["content"] = "data";
  auto delete_result = FileScanTaskFromJson(invalid_delete);
  EXPECT_THAT(delete_result, IsError(ErrorKind::kJsonParseError));
  EXPECT_THAT(delete_result, HasErrorMessage("delete-file"));
}

TEST_F(FileScanTaskJsonTest, RejectsInvalidContentRolesWhenSerializing) {
  auto invalid_data = DataFileForTask();
  invalid_data.content = DataFile::Content::kPositionDeletes;
  FileScanTask data_task(std::make_shared<DataFile>(std::move(invalid_data)));
  auto data_result = ToJson(data_task, specs_, schema_);
  EXPECT_THAT(data_result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(data_result, HasErrorMessage("data-file"));

  auto invalid_delete = DeleteFileForTask();
  invalid_delete.content = DataFile::Content::kData;
  FileScanTask delete_task(std::make_shared<DataFile>(DataFileForTask()),
                           {std::make_shared<DataFile>(std::move(invalid_delete))});
  auto delete_result = ToJson(delete_task, specs_, schema_);
  EXPECT_THAT(delete_result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(delete_result, HasErrorMessage("delete-file"));
}

TEST_F(FileScanTaskJsonTest, RejectsInvalidPartitionValueTypeWhenSerializing) {
  auto data_file = DataFileForTask();
  data_file.partition = PartitionValues({Literal::String("not-an-int")});
  FileScanTask task(std::make_shared<DataFile>(std::move(data_file)));

  auto result = ToJson(task, specs_, schema_);
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(result, HasErrorMessage("partition value type"));
}

TEST_F(FileScanTaskJsonTest, RejectsNullDeleteEntries) {
  auto json = JavaGolden();
  json["delete-files"][0] = nullptr;
  auto parse_result = FileScanTaskFromJson(json);
  EXPECT_THAT(parse_result, IsError(ErrorKind::kJsonParseError));

  FileScanTask task(std::make_shared<DataFile>(DataFileForTask()), {nullptr});
  auto serialize_result = ToJson(task, specs_, schema_);
  EXPECT_THAT(serialize_result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(serialize_result, HasErrorMessage("null"));
}

TEST_F(FileScanTaskJsonTest, RejectsExplicitNullDeleteFilesAndResidual) {
  for (std::string_view field : {"delete-files", "residual-filter"}) {
    SCOPED_TRACE(field);
    auto json = JavaGolden();
    json[field] = nullptr;

    auto result = FileScanTaskFromJson(json);
    EXPECT_THAT(result, IsError(ErrorKind::kJsonParseError));
    EXPECT_THAT(result, HasErrorMessage(field));
  }
}

TEST_F(FileScanTaskJsonTest, DefaultsMissingResidualToAlwaysTrue) {
  auto json = JavaGolden();
  json.erase("residual-filter");

  ICEBERG_UNWRAP_OR_FAIL(auto task, FileScanTaskFromJson(json));
  ASSERT_NE(task->residual_filter(), nullptr);
  ICEBERG_UNWRAP_OR_FAIL(auto residual, ToJson(*task->residual_filter()));
  EXPECT_EQ(residual, true);
}

TEST_F(FileScanTaskJsonTest, RejectsRestDeleteFileReferences) {
  auto json = JavaGolden();
  json["delete-file-references"] = nlohmann::json::array({0});

  auto result = FileScanTaskFromJson(json);
  EXPECT_THAT(result, IsError(ErrorKind::kJsonParseError));
  EXPECT_THAT(result, HasErrorMessage("delete-file-references"));
}

}  // namespace iceberg
