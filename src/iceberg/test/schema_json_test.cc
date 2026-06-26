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

#include "iceberg/expression/literal.h"
#include "iceberg/json_serde_internal.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/test/matchers.h"
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
  ICEBERG_UNWRAP_OR_FAIL(auto json, ToJson(*param.type));
  ASSERT_EQ(param.json, json.dump());

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
        SchemaJsonParam{.json = "\"unknown\"", .type = iceberg::unknown()},
        SchemaJsonParam{.json = "\"fixed[8]\"", .type = iceberg::fixed(8)},
        SchemaJsonParam{.json = "\"decimal(10,2)\"", .type = iceberg::decimal(10, 2)},
        SchemaJsonParam{.json = "\"date\"", .type = iceberg::date()},
        SchemaJsonParam{.json = "\"time\"", .type = iceberg::time()},
        SchemaJsonParam{.json = "\"timestamp\"", .type = iceberg::timestamp()},
        SchemaJsonParam{.json = "\"timestamptz\"",
                        .type = std::make_shared<TimestampTzType>()},
        SchemaJsonParam{.json = "\"timestamp_ns\"", .type = iceberg::timestamp_ns()},
        SchemaJsonParam{.json = "\"timestamptz_ns\"", .type = iceberg::timestamptz_ns()},
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

  ICEBERG_UNWRAP_OR_FAIL(auto schema_json, ToJson(*schema));
  ASSERT_EQ(schema_json.dump(), json);
}

TEST(SchemaJsonTest, FieldWithDefaultValuesRoundTrip) {
  constexpr std::string_view json =
      R"({"fields":[{"id":1,"initial-default":42,"name":"id","required":true,"type":"int","write-default":7},{"id":2,"initial-default":"n/a","name":"name","required":false,"type":"string"}],"schema-id":1,"type":"struct"})";

  ICEBERG_UNWRAP_OR_FAIL(auto schema, SchemaFromJson(nlohmann::json::parse(json)));
  ASSERT_EQ(schema->fields().size(), 2);

  const auto& field1 = schema->fields()[0];
  ASSERT_TRUE(field1.initial_default().has_value());
  ASSERT_EQ(field1.initial_default()->get(), Literal::Int(42));
  ASSERT_TRUE(field1.write_default().has_value());
  ASSERT_EQ(field1.write_default()->get(), Literal::Int(7));

  const auto& field2 = schema->fields()[1];
  ASSERT_TRUE(field2.initial_default().has_value());
  ASSERT_EQ(field2.initial_default()->get(), Literal::String("n/a"));
  ASSERT_FALSE(field2.write_default().has_value());

  ICEBERG_UNWRAP_OR_FAIL(auto schema_json, ToJson(*schema));
  ASSERT_EQ(schema_json.dump(), json);
}

TEST(SchemaJsonTest, FieldWithMismatchedDefaultValueFails) {
  constexpr std::string_view json =
      R"({"fields":[{"id":1,"initial-default":"oops","name":"id","required":true,"type":"int"}],"schema-id":1,"type":"struct"})";

  auto result = SchemaFromJson(nlohmann::json::parse(json));
  ASSERT_FALSE(result.has_value());
}

TEST(SchemaJsonTest, NestedFieldWithDefaultValuesRoundTrip) {
  constexpr std::string_view json =
      R"({"fields":[{"id":1,"name":"person","required":true,"type":{"fields":[{"id":2,"initial-default":18,"name":"age","required":true,"type":"int","write-default":21}],"type":"struct"}}],"schema-id":1,"type":"struct"})";

  ICEBERG_UNWRAP_OR_FAIL(auto schema, SchemaFromJson(nlohmann::json::parse(json)));
  const auto& person = schema->fields()[0];
  const auto& nested = dynamic_cast<const StructType&>(*person.type()).fields()[0];
  ASSERT_TRUE(nested.initial_default().has_value());
  ASSERT_EQ(nested.initial_default()->get(), Literal::Int(18));
  ASSERT_TRUE(nested.write_default().has_value());
  ASSERT_EQ(nested.write_default()->get(), Literal::Int(21));

  ICEBERG_UNWRAP_OR_FAIL(auto schema_json, ToJson(*schema));
  ASSERT_EQ(schema_json.dump(), json);
}

TEST(SchemaJsonTest, UnknownFieldRoundTrip) {
  constexpr std::string_view json =
      R"({"fields":[{"id":1,"name":"mystery","required":false,"type":"unknown"}],"schema-id":1,"type":"struct"})";

  ICEBERG_UNWRAP_OR_FAIL(auto schema, SchemaFromJson(nlohmann::json::parse(json)));
  ASSERT_EQ(schema->fields().size(), 1);

  const auto& field = schema->fields()[0];
  ASSERT_EQ(field.field_id(), 1);
  ASSERT_EQ(field.name(), "mystery");
  ASSERT_EQ(field.type()->type_id(), TypeId::kUnknown);
  ASSERT_TRUE(field.optional());
  ICEBERG_UNWRAP_OR_FAIL(auto schema_json, ToJson(*schema));
  ASSERT_EQ(schema_json.dump(), json);
}

TEST(SchemaJsonTest, NestedUnknownFieldsRoundTrip) {
  constexpr std::string_view json =
      R"({
        "fields": [
          {
            "id": 1,
            "name": "profile",
            "required": false,
            "type": {
              "fields": [
                {"id": 2, "name": "mystery", "required": false, "type": "unknown"}
              ],
              "type": "struct"
            }
          },
          {
            "id": 3,
            "name": "mysteries",
            "required": false,
            "type": {
              "element": "unknown",
              "element-id": 4,
              "element-required": false,
              "type": "list"
            }
          },
          {
            "id": 5,
            "name": "properties",
            "required": false,
            "type": {
              "key": "string",
              "key-id": 6,
              "type": "map",
              "value": "unknown",
              "value-id": 7,
              "value-required": false
            }
          }
        ],
        "schema-id": 1,
        "type": "struct"
      })";
  const auto parsed_json = nlohmann::json::parse(json);

  ICEBERG_UNWRAP_OR_FAIL(auto schema, SchemaFromJson(parsed_json));
  ASSERT_EQ(schema->fields().size(), 3);

  const auto* profile = dynamic_cast<const StructType*>(schema->fields()[0].type().get());
  ASSERT_NE(profile, nullptr);
  ASSERT_EQ(profile->fields().size(), 1);
  ASSERT_EQ(profile->fields()[0].type()->type_id(), TypeId::kUnknown);
  ASSERT_TRUE(profile->fields()[0].optional());

  const auto* mysteries = dynamic_cast<const ListType*>(schema->fields()[1].type().get());
  ASSERT_NE(mysteries, nullptr);
  ASSERT_EQ(mysteries->fields()[0].type()->type_id(), TypeId::kUnknown);
  ASSERT_TRUE(mysteries->fields()[0].optional());

  const auto* properties = dynamic_cast<const MapType*>(schema->fields()[2].type().get());
  ASSERT_NE(properties, nullptr);
  ASSERT_EQ(properties->value().type()->type_id(), TypeId::kUnknown);
  ASSERT_TRUE(properties->value().optional());

  ICEBERG_UNWRAP_OR_FAIL(auto schema_json, ToJson(*schema));
  ASSERT_EQ(schema_json, parsed_json);
}

TEST(SchemaJsonTest, IdentifierFieldIds) {
  // Test schema with identifier-field-ids
  constexpr std::string_view json_with_identifier_str =
      R"({"fields":[{"id":1,"name":"id","required":true,"type":"long"},
                    {"id":2,"name":"data","required":false,"type":"string"}],
          "identifier-field-ids":[1],
          "schema-id":1,
          "type":"struct"})";

  auto json_with_identifiers = nlohmann::json::parse(json_with_identifier_str);
  ICEBERG_UNWRAP_OR_FAIL(auto schema_with_identifers,
                         SchemaFromJson(json_with_identifiers));
  ASSERT_EQ(schema_with_identifers->fields().size(), 2);
  ASSERT_EQ(schema_with_identifers->schema_id(), 1);
  ASSERT_EQ(schema_with_identifers->IdentifierFieldIds().size(), 1);
  ASSERT_EQ(schema_with_identifers->IdentifierFieldIds()[0], 1);
  ICEBERG_UNWRAP_OR_FAIL(auto json_with_identifiers_out, ToJson(*schema_with_identifers));
  ASSERT_EQ(json_with_identifiers_out, json_with_identifiers);

  // Test schema without identifier-field-ids
  constexpr std::string_view json_without_identifiers_str =
      R"({"fields":[{"id":1,"name":"id","required":true,"type":"int"},
                    {"id":2,"name":"name","required":false,"type":"string"}],
          "schema-id":1,
          "type":"struct"})";

  auto json_without_identifiers = nlohmann::json::parse(json_without_identifiers_str);
  ICEBERG_UNWRAP_OR_FAIL(auto schema_without_identifiers,
                         SchemaFromJson(json_without_identifiers));
  ASSERT_TRUE(schema_without_identifiers->IdentifierFieldIds().empty());
  ICEBERG_UNWRAP_OR_FAIL(auto json_without_identifiers_out,
                         ToJson(*schema_without_identifiers));
  ASSERT_EQ(json_without_identifiers_out, json_without_identifiers);

  // Test schema with multiple identifier fields
  constexpr std::string_view json_multi_identifiers_str =
      R"({"fields":[{"id":1,"name":"user_id","required":true,"type":"long"},
                    {"id":2,"name":"org_id","required":true,"type":"long"},
                    {"id":3,"name":"data","required":false,"type":"string"}],
          "identifier-field-ids":[1,2],
          "schema-id":2,
          "type":"struct"})";
  auto json_multi_identifiers = nlohmann::json::parse(json_multi_identifiers_str);
  ICEBERG_UNWRAP_OR_FAIL(auto schema_multi_identifiers,
                         SchemaFromJson(json_multi_identifiers));
  ASSERT_EQ(schema_multi_identifiers->IdentifierFieldIds().size(), 2);
  ASSERT_EQ(schema_multi_identifiers->IdentifierFieldIds()[0], 1);
  ASSERT_EQ(schema_multi_identifiers->IdentifierFieldIds()[1], 2);
  ICEBERG_UNWRAP_OR_FAIL(auto json_multi_identifiers_out,
                         ToJson(*schema_multi_identifiers));
  ASSERT_EQ(json_multi_identifiers_out, json_multi_identifiers);
}

}  // namespace iceberg
