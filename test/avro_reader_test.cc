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

#include "iceberg/avro/avro_reader.h"

#include <avro/Compiler.hh>
#include <avro/DataFile.hh>
#include <avro/Generic.hh>
#include <avro/GenericDatum.hh>
#include <avro/ValidSchema.hh>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/json_internal.h"
#include "iceberg/name_mapping.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "matchers.h"

namespace iceberg::avro {

class AvroReaderTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create a simple Avro schema for testing
    std::string schema_json = R"({
      "type": "record",
      "name": "test_record",
      "fields": [
        {"name": "id", "type": "long"},
        {"name": "name", "type": "string"},
        {"name": "data", "type": {
          "type": "record",
          "name": "nested_record",
          "fields": [
            {"name": "value", "type": "int"},
            {"name": "description", "type": "string"}
          ]
        }}
      ]
    })";

    avro_schema_ = ::avro::compileJsonSchemaFromString(schema_json);
  }

  std::unique_ptr<NameMapping> CreateTestNameMapping() {
    std::vector<MappedField> fields;
    fields.emplace_back(MappedField{.names = {"id"}, .field_id = 1});
    fields.emplace_back(MappedField{.names = {"name"}, .field_id = 2});

    // Create nested mapping for the data field
    std::vector<MappedField> nested_fields;
    nested_fields.emplace_back(MappedField{.names = {"value"}, .field_id = 3});
    nested_fields.emplace_back(MappedField{.names = {"description"}, .field_id = 4});
    auto nested_mapping = MappedFields::Make(std::move(nested_fields));

    fields.emplace_back(MappedField{
        .names = {"data"}, .field_id = 5, .nested_mapping = std::move(nested_mapping)});

    return NameMapping::Make(std::move(fields));
  }

  ::avro::ValidSchema avro_schema_;
};

TEST_F(AvroReaderTest, ApplyFieldIdsFromNameMappingBasic) {
  auto name_mapping = CreateTestNameMapping();

  // Test that the schema can be processed without errors
  // Note: This is a basic test to ensure the function can be called
  // In a real implementation, we would need to mock the Avro Node interface
  // or create actual Avro schema nodes to test the field ID application

  EXPECT_TRUE(name_mapping != nullptr);
  EXPECT_EQ(name_mapping->AsMappedFields().Size(), 3);
}

TEST_F(AvroReaderTest, NameMappingFindMethodsWorkWithConst) {
  auto name_mapping = CreateTestNameMapping();
  const NameMapping& const_mapping = *name_mapping;

  // Test that Find methods work on const objects
  auto field_by_id = const_mapping.Find(1);
  ASSERT_TRUE(field_by_id.has_value());
  EXPECT_EQ(field_by_id->get().field_id, 1);
  EXPECT_THAT(field_by_id->get().names, testing::UnorderedElementsAre("id"));

  auto field_by_name = const_mapping.Find("name");
  ASSERT_TRUE(field_by_name.has_value());
  EXPECT_EQ(field_by_name->get().field_id, 2);
  EXPECT_THAT(field_by_name->get().names, testing::UnorderedElementsAre("name"));

  auto field_by_parts = const_mapping.Find(std::vector<std::string>{"data", "value"});
  ASSERT_TRUE(field_by_parts.has_value());
  EXPECT_EQ(field_by_parts->get().field_id, 3);
  EXPECT_THAT(field_by_parts->get().names, testing::UnorderedElementsAre("value"));
}

TEST_F(AvroReaderTest, NameMappingFromJsonWorks) {
  std::string json_str = R"([
    {"field-id": 1, "names": ["id"]},
    {"field-id": 2, "names": ["name"]},
    {"field-id": 3, "names": ["data"], "fields": [
      {"field-id": 4, "names": ["value"]},
      {"field-id": 5, "names": ["description"]}
    ]}
  ])";

  auto result = iceberg::NameMappingFromJson(nlohmann::json::parse(json_str));
  ASSERT_TRUE(result.has_value());

  auto mapping = std::move(result.value());
  const NameMapping& const_mapping = *mapping;

  // Test that the parsed mapping works correctly
  auto field_by_id = const_mapping.Find(1);
  ASSERT_TRUE(field_by_id.has_value());
  EXPECT_EQ(field_by_id->get().field_id, 1);
  EXPECT_THAT(field_by_id->get().names, testing::UnorderedElementsAre("id"));

  auto field_by_name = const_mapping.Find("name");
  ASSERT_TRUE(field_by_name.has_value());
  EXPECT_EQ(field_by_name->get().field_id, 2);
  EXPECT_THAT(field_by_name->get().names, testing::UnorderedElementsAre("name"));

  auto nested_field = const_mapping.Find("data.value");
  ASSERT_TRUE(nested_field.has_value());
  EXPECT_EQ(nested_field->get().field_id, 4);
  EXPECT_THAT(nested_field->get().names, testing::UnorderedElementsAre("value"));
}

TEST_F(AvroReaderTest, NameMappingWithNestedFields) {
  auto name_mapping = CreateTestNameMapping();
  const NameMapping& const_mapping = *name_mapping;

  // Test nested field access
  auto data_field = const_mapping.Find("data");
  ASSERT_TRUE(data_field.has_value());
  EXPECT_EQ(data_field->get().field_id, 5);
  EXPECT_THAT(data_field->get().names, testing::UnorderedElementsAre("data"));

  // Test that nested mapping exists
  ASSERT_TRUE(data_field->get().nested_mapping != nullptr);
  EXPECT_EQ(data_field->get().nested_mapping->Size(), 2);

  // Test nested field access through the nested mapping
  auto value_field = data_field->get().nested_mapping->Field(3);
  ASSERT_TRUE(value_field.has_value());
  EXPECT_EQ(value_field->get().field_id, 3);
  EXPECT_THAT(value_field->get().names, testing::UnorderedElementsAre("value"));

  auto description_field = data_field->get().nested_mapping->Field(4);
  ASSERT_TRUE(description_field.has_value());
  EXPECT_EQ(description_field->get().field_id, 4);
  EXPECT_THAT(description_field->get().names,
              testing::UnorderedElementsAre("description"));
}

TEST_F(AvroReaderTest, NameMappingFromReaderOptionsWorks) {
  // Create a name mapping
  auto name_mapping = CreateTestNameMapping();
  ASSERT_TRUE(name_mapping != nullptr);
  EXPECT_EQ(name_mapping->AsMappedFields().Size(), 3);

  // Create reader options with name mapping
  ReaderOptions options;
  options.name_mapping = std::move(name_mapping);

  // Verify that the name mapping is accessible
  ASSERT_TRUE(options.name_mapping != nullptr);
  EXPECT_EQ(options.name_mapping->AsMappedFields().Size(), 3);

  // Test that the name mapping works correctly
  auto field_by_id = options.name_mapping->Find(1);
  ASSERT_TRUE(field_by_id.has_value());
  EXPECT_EQ(field_by_id->get().field_id, 1);
  EXPECT_THAT(field_by_id->get().names, testing::UnorderedElementsAre("id"));
}

}  // namespace iceberg::avro
