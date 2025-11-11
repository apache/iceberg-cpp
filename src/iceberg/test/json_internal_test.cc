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
      std::vector<SchemaField>{SchemaField(5, "region", iceberg::string(), false),
                               SchemaField(7, "ts", iceberg::int64(), false)},
      /*schema_id=*/100);
  auto identity_transform = Transform::Identity();
  SortField st_ts(5, identity_transform, SortDirection::kAscending, NullOrder::kFirst);
  SortField st_bar(7, identity_transform, SortDirection::kDescending, NullOrder::kLast);
  ICEBERG_UNWRAP_OR_FAIL(auto sort_order, SortOrder::Make(100, {st_ts, st_bar}));
  EXPECT_TRUE(sort_order->Validate(*schema));
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

TEST(JsonPartitionTest, PartitionFieldFromJsonMissingField) {
  nlohmann::json invalid_json =
      R"({"field-id":101,"transform":"identity","name":"region"})"_json;
  // missing source-id

  auto result = PartitionFieldFromJson(invalid_json);
  EXPECT_FALSE(result.has_value());
  EXPECT_THAT(result, IsError(ErrorKind::kJsonParseError));
  EXPECT_THAT(result, HasErrorMessage("Missing 'source-id'"));
}

TEST(JsonPartitionTest, PartitionSpec) {
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField(3, "region", string(), false),
                               SchemaField(5, "ts", int64(), false)},
      /*schema_id=*/100);
  auto identity_transform = Transform::Identity();
  PartitionSpec spec(1, {PartitionField(3, 101, "region", identity_transform),
                         PartitionField(5, 102, "ts", identity_transform)});
  auto json = ToJson(spec);
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

  auto parsed_spec_result = PartitionSpecFromJson(schema, json);
  ASSERT_TRUE(parsed_spec_result.has_value()) << parsed_spec_result.error().message;
  EXPECT_EQ(spec, *parsed_spec_result.value());
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

}  // namespace iceberg
