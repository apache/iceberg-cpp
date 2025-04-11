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

#include <format>
#include <memory>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/sort_field.h"
#include "iceberg/sort_order.h"
#include "iceberg/transform.h"
#include "iceberg/util/formatter.h"

namespace iceberg {

namespace {
// Specialized FromJson helper based on type
template <typename T>
expected<std::unique_ptr<T>, Error> FromJsonHelper(const nlohmann::json& json);

template <>
expected<std::unique_ptr<SortField>, Error> FromJsonHelper(const nlohmann::json& json) {
  return SortFieldFromJson(json);
}

template <>
expected<std::unique_ptr<SortOrder>, Error> FromJsonHelper(const nlohmann::json& json) {
  return SortOrderFromJson(json);
}

template <>
expected<std::unique_ptr<PartitionField>, Error> FromJsonHelper(
    const nlohmann::json& json) {
  return PartitionFieldFromJson(json);
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
  auto identity_transform = std::make_shared<IdentityTransformFunction>();

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
  auto identity_transform = std::make_shared<IdentityTransformFunction>();
  SortField st_ts(5, identity_transform, SortDirection::kAscending, NullOrder::kFirst);
  SortField st_bar(7, identity_transform, SortDirection::kDescending, NullOrder::kLast);
  SortOrder sort_order(100, {st_ts, st_bar});

  nlohmann::json expected_sort_order =
      R"({"order-id":100,"fields":[
          {"transform":"identity","source-id":5,"direction":"asc","null-order":"nulls-first"},
          {"transform":"identity","source-id":7,"direction":"desc","null-order":"nulls-last"}]})"_json;

  TestJsonConversion(sort_order, expected_sort_order);
}

TEST(JsonInternalTest, PartitionField) {
  auto identity_transform = std::make_shared<IdentityTransformFunction>();
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
  EXPECT_EQ(result.error().kind, ErrorKind::kJsonParseError);
}

TEST(JsonPartitionTest, PartitionSpec) {
  auto schema = std::make_shared<Schema>(
      100, std::vector<SchemaField>{
               SchemaField(3, "region", std::make_shared<StringType>(), false),
               SchemaField(5, "ts", std::make_shared<LongType>(), false)});

  auto identity_transform = std::make_shared<IdentityTransformFunction>();
  PartitionSpec spec(schema, 1,
                     {PartitionField(3, 101, "region", identity_transform),
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

}  // namespace iceberg
