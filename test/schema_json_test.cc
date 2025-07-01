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

#include <memory>
#include <string>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/json_internal.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/type.h"

namespace iceberg {

struct SchemaJsonParam {
  std::string json;
  std::shared_ptr<Type> type;
};

class TypeJsonTest : public ::testing::TestWithParam<SchemaJsonParam> {};

TEST_P(TypeJsonTest, SingleTypeRoundTrip) {
  // To Json
  const auto& param = GetParam();
  auto json = ToJson(*param.type).dump();
  ASSERT_EQ(param.json, json);

  // From Json
  auto type_result = TypeFromJson(nlohmann::json::parse(param.json));
  ASSERT_TRUE(type_result.has_value()) << "Failed to deserialize " << param.json
                                       << " with error " << type_result.error().message;
  auto type = std::move(type_result.value());
  ASSERT_EQ(*param.type, *type);
}

INSTANTIATE_TEST_SUITE_P(
    JsonSerailization, TypeJsonTest,
    ::testing::Values(
        SchemaJsonParam{.json = "\"boolean\"", .type = iceberg::boolean()},
        SchemaJsonParam{.json = "\"int\"", .type = iceberg::int32()},
        SchemaJsonParam{.json = "\"long\"", .type = iceberg::int64()},
        SchemaJsonParam{.json = "\"float\"", .type = iceberg::float32()},
        SchemaJsonParam{.json = "\"double\"", .type = iceberg::float64()},
        SchemaJsonParam{.json = "\"string\"", .type = iceberg::string()},
        SchemaJsonParam{.json = "\"binary\"", .type = iceberg::binary()},
        SchemaJsonParam{.json = "\"uuid\"", .type = iceberg::uuid()},
        SchemaJsonParam{.json = "\"fixed[8]\"", .type = iceberg::fixed(8)},
        SchemaJsonParam{.json = "\"decimal(10,2)\"", .type = iceberg::decimal(10, 2)},
        SchemaJsonParam{.json = "\"date\"", .type = iceberg::date()},
        SchemaJsonParam{.json = "\"time\"", .type = iceberg::time()},
        SchemaJsonParam{.json = "\"timestamp\"", .type = iceberg::timestamp()},
        SchemaJsonParam{.json = "\"timestamptz\"",
                        .type = std::make_shared<TimestampTzType>()},
        SchemaJsonParam{
            .json =
                R"({"element":"string","element-id":3,"element-required":true,"type":"list"})",
            .type = std::make_shared<ListType>(
                SchemaField::MakeRequired(3, "element", iceberg::string()))},
        SchemaJsonParam{
            .json =
                R"({"key":"string","key-id":4,"type":"map","value":"double","value-id":5,"value-required":false})",
            .type = std::make_shared<MapType>(
                SchemaField::MakeRequired(4, "key", iceberg::string()),
                SchemaField::MakeOptional(5, "value", iceberg::float64()))},
        SchemaJsonParam{
            .json =
                R"({"fields":[{"id":1,"name":"id","required":true,"type":"int"},{"id":2,"name":"name","required":false,"type":"string"}],"type":"struct"})",
            .type = std::make_shared<StructType>(std::vector<SchemaField>{
                SchemaField::MakeRequired(1, "id", iceberg::int32()),
                SchemaField::MakeOptional(2, "name", iceberg::string())})}));

TEST(TypeJsonTest, FromJsonWithSpaces) {
  auto fixed_json = R"("fixed[ 8 ]")";
  auto fixed_result = TypeFromJson(nlohmann::json::parse(fixed_json));
  ASSERT_TRUE(fixed_result.has_value());
  ASSERT_EQ(fixed_result.value()->type_id(), TypeId::kFixed);
  auto fixed = dynamic_cast<FixedType*>(fixed_result.value().get());
  ASSERT_NE(fixed, nullptr);
  ASSERT_EQ(fixed->length(), 8);

  auto decimal_json = "\"decimal( 10, 2 )\"";
  auto decimal_result = TypeFromJson(nlohmann::json::parse(decimal_json));
  ASSERT_TRUE(decimal_result.has_value());
  ASSERT_EQ(decimal_result.value()->type_id(), TypeId::kDecimal);
  auto decimal = dynamic_cast<DecimalType*>(decimal_result.value().get());
  ASSERT_NE(decimal, nullptr);
  ASSERT_EQ(decimal->precision(), 10);
  ASSERT_EQ(decimal->scale(), 2);
}

TEST(SchemaJsonTest, RoundTrip) {
  constexpr std::string_view json =
      R"({"fields":[{"id":1,"name":"id","required":true,"type":"int"},{"id":2,"name":"name","required":false,"type":"string"}],"schema-id":1,"type":"struct"})";

  auto from_json_result = SchemaFromJson(nlohmann::json::parse(json));
  ASSERT_TRUE(from_json_result.has_value());
  auto schema = std::move(from_json_result.value());
  ASSERT_EQ(schema->fields().size(), 2);
  ASSERT_EQ(schema->schema_id(), 1);

  auto field1 = schema->fields()[0];
  ASSERT_EQ(field1.field_id(), 1);
  ASSERT_EQ(field1.name(), "id");
  ASSERT_EQ(field1.type()->type_id(), TypeId::kInt);
  ASSERT_FALSE(field1.optional());

  auto field2 = schema->fields()[1];
  ASSERT_EQ(field2.field_id(), 2);
  ASSERT_EQ(field2.name(), "name");
  ASSERT_EQ(field2.type()->type_id(), TypeId::kString);
  ASSERT_TRUE(field2.optional());

  auto dumped_json = ToJson(*schema).dump();
  ASSERT_EQ(dumped_json, json);
}

}  // namespace iceberg
