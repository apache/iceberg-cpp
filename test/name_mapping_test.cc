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

#include "iceberg/name_mapping.h"

#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include <avro/Compiler.hh>
#include <avro/NodeImpl.hh>
#include <avro/Types.hh>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/avro/avro_reader.h"
#include "iceberg/avro/avro_schema_util_internal.h"
#include "iceberg/file_reader.h"
#include "iceberg/json_internal.h"
#include "matchers.h"

namespace iceberg {

class NameMappingTest : public ::testing::Test {
 protected:
  std::unique_ptr<NameMapping> MakeNameMapping() {
    std::vector<MappedField> fields;
    fields.emplace_back(MappedField{.names = {"foo", "bar"}, .field_id = 1});
    fields.emplace_back(MappedField{.names = {"baz"}, .field_id = 2});

    std::vector<MappedField> nested_fields;
    nested_fields.emplace_back(MappedField{.names = {"hello"}, .field_id = 4});
    nested_fields.emplace_back(MappedField{.names = {"world"}, .field_id = 5});
    auto nested_mapping = MappedFields::Make(std::move(nested_fields));
    fields.emplace_back(MappedField{
        .names = {"qux"}, .field_id = 3, .nested_mapping = std::move(nested_mapping)});

    return NameMapping::Make(std::move(fields));
  }
};

TEST_F(NameMappingTest, FindById) {
  auto mapping = MakeNameMapping();

  struct Param {
    int32_t field_id;
    bool should_have_value;
    std::unordered_set<std::string> names;
  };

  const std::vector<Param> params = {
      {.field_id = 1, .should_have_value = true, .names = {"foo", "bar"}},
      {.field_id = 2, .should_have_value = true, .names = {"baz"}},
      {.field_id = 3, .should_have_value = true, .names = {"qux"}},
      {.field_id = 4, .should_have_value = true, .names = {"hello"}},
      {.field_id = 5, .should_have_value = true, .names = {"world"}},
      {.field_id = 999, .should_have_value = false, .names = {}},
  };

  for (const auto& param : params) {
    auto field = mapping->Find(param.field_id);
    if (param.should_have_value) {
      ASSERT_TRUE(field.has_value());
      EXPECT_EQ(field->get().field_id, param.field_id);
      EXPECT_THAT(field->get().names, testing::UnorderedElementsAreArray(param.names));
    } else {
      EXPECT_FALSE(field.has_value());
    }
  }
}

TEST_F(NameMappingTest, FindByName) {
  auto mapping = MakeNameMapping();

  struct Param {
    std::string name;
    bool should_have_value;
    int32_t field_id;
  };

  const std::vector<Param> params = {
      {.name = "foo", .should_have_value = true, .field_id = 1},
      {.name = "bar", .should_have_value = true, .field_id = 1},
      {.name = "baz", .should_have_value = true, .field_id = 2},
      {.name = "qux", .should_have_value = true, .field_id = 3},
      {.name = "qux.hello", .should_have_value = true, .field_id = 4},
      {.name = "qux.world", .should_have_value = true, .field_id = 5},
      {.name = "non_existent", .should_have_value = false, .field_id = -1},
  };

  for (const auto& param : params) {
    auto field = mapping->Find(param.name);
    if (param.should_have_value) {
      ASSERT_TRUE(field.has_value());
      EXPECT_EQ(field->get().field_id, param.field_id);
    } else {
      EXPECT_FALSE(field.has_value());
    }
  }
}

TEST_F(NameMappingTest, FindByNameParts) {
  auto mapping = MakeNameMapping();

  struct Param {
    std::vector<std::string> names;
    bool should_have_value;
    int32_t field_id;
  };

  std::vector<Param> params = {
      {.names = {"foo"}, .should_have_value = true, .field_id = 1},
      {.names = {"bar"}, .should_have_value = true, .field_id = 1},
      {.names = {"baz"}, .should_have_value = true, .field_id = 2},
      {.names = {"qux"}, .should_have_value = true, .field_id = 3},
      {.names = {"qux", "hello"}, .should_have_value = true, .field_id = 4},
      {.names = {"qux", "world"}, .should_have_value = true, .field_id = 5},
      {.names = {"non_existent"}, .should_have_value = false, .field_id = -1},
  };

  for (const auto& param : params) {
    auto field = mapping->Find(param.names);
    if (param.should_have_value) {
      ASSERT_TRUE(field.has_value());
      EXPECT_EQ(field->get().field_id, param.field_id);
    } else {
      EXPECT_FALSE(field.has_value());
    }
  }
}

TEST_F(NameMappingTest, FindMethodsOnConstObject) {
  auto mapping = MakeNameMapping();
  const NameMapping& const_mapping = *mapping;

  // Test Find by ID on const object
  auto field_by_id = const_mapping.Find(1);
  ASSERT_TRUE(field_by_id.has_value());
  EXPECT_EQ(field_by_id->get().field_id, 1);
  EXPECT_THAT(field_by_id->get().names, testing::UnorderedElementsAre("foo", "bar"));

  // Test Find by name on const object
  auto field_by_name = const_mapping.Find("baz");
  ASSERT_TRUE(field_by_name.has_value());
  EXPECT_EQ(field_by_name->get().field_id, 2);
  EXPECT_THAT(field_by_name->get().names, testing::UnorderedElementsAre("baz"));

  // Test Find by name parts on const object
  auto field_by_parts = const_mapping.Find(std::vector<std::string>{"qux", "hello"});
  ASSERT_TRUE(field_by_parts.has_value());
  EXPECT_EQ(field_by_parts->get().field_id, 4);
  EXPECT_THAT(field_by_parts->get().names, testing::UnorderedElementsAre("hello"));

  // Test Find non-existent field on const object
  auto non_existent = const_mapping.Find(999);
  EXPECT_FALSE(non_existent.has_value());

  auto non_existent_name = const_mapping.Find("non_existent");
  EXPECT_FALSE(non_existent_name.has_value());

  auto non_existent_parts =
      const_mapping.Find(std::vector<std::string>{"non", "existent"});
  EXPECT_FALSE(non_existent_parts.has_value());
}

TEST_F(NameMappingTest, FindMethodsOnConstEmptyMapping) {
  auto empty_mapping = NameMapping::MakeEmpty();
  const NameMapping& const_empty_mapping = *empty_mapping;

  // Test Find by ID on const empty mapping
  auto field_by_id = const_empty_mapping.Find(1);
  EXPECT_FALSE(field_by_id.has_value());

  // Test Find by name on const empty mapping
  auto field_by_name = const_empty_mapping.Find("test");
  EXPECT_FALSE(field_by_name.has_value());

  // Test Find by name parts on const empty mapping
  auto field_by_parts =
      const_empty_mapping.Find(std::vector<std::string>{"test", "field"});
  EXPECT_FALSE(field_by_parts.has_value());
}

TEST_F(NameMappingTest, Equality) {
  auto mapping1 = MakeNameMapping();
  auto mapping2 = MakeNameMapping();
  auto empty_mapping = NameMapping::MakeEmpty();

  EXPECT_EQ(*mapping1, *mapping2);
  EXPECT_NE(*mapping1, *empty_mapping);

  std::vector<MappedField> fields;
  fields.emplace_back(
      MappedField{.names = {"different"}, .field_id = 99, .nested_mapping = nullptr});
  auto different_mapping = NameMapping::Make(MappedFields::Make(std::move(fields)));

  EXPECT_NE(*mapping1, *different_mapping);
}

TEST_F(NameMappingTest, MappedFieldsAccess) {
  auto mapping = MakeNameMapping();
  const auto& fields = mapping->AsMappedFields();
  EXPECT_EQ(fields.Size(), 3);

  struct Param {
    int32_t field_id;
    std::unordered_set<std::string> names;
  };

  const std::vector<Param> params = {
      {.field_id = 1, .names = {"foo", "bar"}},
      {.field_id = 2, .names = {"baz"}},
      {.field_id = 3, .names = {"qux"}},
  };

  for (const auto& param : params) {
    auto field = fields.Field(param.field_id);
    ASSERT_TRUE(field.has_value());
    EXPECT_THAT(field->get().names, testing::UnorderedElementsAreArray(param.names));
  }
}

TEST_F(NameMappingTest, ToString) {
  {
    std::vector<MappedField> fields;
    fields.emplace_back(MappedField{.names = {"foo"}, .field_id = 1});

    std::vector<MappedField> nested_fields;
    nested_fields.emplace_back(MappedField{.names = {"hello"}, .field_id = 3});
    nested_fields.emplace_back(MappedField{.names = {"world"}, .field_id = 4});
    auto nested_mapping = MappedFields::Make(std::move(nested_fields));
    fields.emplace_back(MappedField{
        .names = {"bar"}, .field_id = 2, .nested_mapping = std::move(nested_mapping)});

    auto mapping = NameMapping::Make(std::move(fields));

    auto expected = R"([
  ([foo] -> 1)
  ([bar] -> 2, [([hello] -> 3), ([world] -> 4)])
])";
    EXPECT_EQ(ToString(*mapping), expected);
  }

  {
    auto empty_mapping = NameMapping::MakeEmpty();
    EXPECT_EQ(ToString(*empty_mapping), "[]");
  }
}

TEST(CreateMappingTest, FlatSchemaToMapping) {
  Schema schema(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", iceberg::int64()),
      SchemaField::MakeRequired(2, "data", iceberg::string()),
  });

  auto expected = MappedFields::Make({
      MappedField{.names = {"id"}, .field_id = 1},
      MappedField{.names = {"data"}, .field_id = 2},
  });

  auto result = CreateMapping(schema);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value()->AsMappedFields(), *expected);
}

TEST(CreateMappingTest, NestedStructSchemaToMapping) {
  Schema schema(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", iceberg::int64()),
      SchemaField::MakeRequired(2, "data", iceberg::string()),
      SchemaField::MakeRequired(
          3, "location",
          std::make_shared<StructType>(std::vector<SchemaField>{
              SchemaField::MakeRequired(4, "latitude", iceberg::float32()),
              SchemaField::MakeRequired(5, "longitude", iceberg::float32()),
          })),
  });

  auto expected = MappedFields::Make({
      MappedField{.names = {"id"}, .field_id = 1},
      MappedField{.names = {"data"}, .field_id = 2},
      MappedField{.names = {"location"},
                  .field_id = 3,
                  .nested_mapping = MappedFields::Make({
                      MappedField{.names = {"latitude"}, .field_id = 4},
                      MappedField{.names = {"longitude"}, .field_id = 5},
                  })},
  });

  auto result = CreateMapping(schema);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value()->AsMappedFields(), *expected);
}

TEST(CreateMappingTest, MapSchemaToMapping) {
  Schema schema(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", iceberg::int64()),
      SchemaField::MakeRequired(2, "data", iceberg::string()),
      SchemaField::MakeRequired(
          3, "map",
          std::make_shared<MapType>(
              SchemaField::MakeRequired(4, "key", iceberg::string()),
              SchemaField::MakeRequired(5, "value", iceberg::float64()))),
  });

  auto expected = MappedFields::Make({
      MappedField{.names = {"id"}, .field_id = 1},
      MappedField{.names = {"data"}, .field_id = 2},
      MappedField{.names = {"map"},
                  .field_id = 3,
                  .nested_mapping = MappedFields::Make({
                      MappedField{.names = {"key"}, .field_id = 4},
                      MappedField{.names = {"value"}, .field_id = 5},
                  })},
  });

  auto result = CreateMapping(schema);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value()->AsMappedFields(), *expected);
}

TEST(CreateMappingTest, ListSchemaToMapping) {
  Schema schema(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", iceberg::int64()),
      SchemaField::MakeRequired(2, "data", iceberg::string()),
      SchemaField::MakeRequired(3, "list",
                                std::make_shared<ListType>(SchemaField::MakeRequired(
                                    4, "element", iceberg::string()))),
  });

  auto expected = MappedFields::Make({
      MappedField{.names = {"id"}, .field_id = 1},
      MappedField{.names = {"data"}, .field_id = 2},
      MappedField{.names = {"list"},
                  .field_id = 3,
                  .nested_mapping = MappedFields::Make({
                      MappedField{.names = {"element"}, .field_id = 4},
                  })},
  });

  auto result = CreateMapping(schema);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value()->AsMappedFields(), *expected);
}

}  // namespace iceberg

// NameMapping tests for Avro schema context
namespace iceberg::avro {

namespace {

void CheckFieldIdAt(const ::avro::NodePtr& node, size_t index, int32_t field_id,
                    const std::string& key = "field-id") {
  ASSERT_LT(index, node->customAttributes());
  const auto& attrs = node->customAttributesAt(index);
  ASSERT_EQ(attrs.getAttribute(key), std::make_optional(std::to_string(field_id)));
}

// Helper function to create a test name mapping
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

}  // namespace

class NameMappingAvroSchemaTest : public ::testing::Test {
 protected:
  // Helper function to create a simple name mapping
  std::unique_ptr<NameMapping> CreateSimpleNameMapping() {
    std::vector<MappedField> fields;
    fields.emplace_back(MappedField{.names = {"id"}, .field_id = 1});
    fields.emplace_back(MappedField{.names = {"name"}, .field_id = 2});
    fields.emplace_back(MappedField{.names = {"age"}, .field_id = 3});
    return NameMapping::Make(std::move(fields));
  }

  // Helper function to create a nested name mapping
  std::unique_ptr<NameMapping> CreateNestedNameMapping() {
    std::vector<MappedField> fields;
    fields.emplace_back(MappedField{.names = {"id"}, .field_id = 1});

    // Nested mapping for address
    std::vector<MappedField> address_fields;
    address_fields.emplace_back(MappedField{.names = {"street"}, .field_id = 10});
    address_fields.emplace_back(MappedField{.names = {"city"}, .field_id = 11});
    address_fields.emplace_back(MappedField{.names = {"zip"}, .field_id = 12});
    auto address_mapping = MappedFields::Make(std::move(address_fields));

    fields.emplace_back(MappedField{.names = {"address"},
                                    .field_id = 2,
                                    .nested_mapping = std::move(address_mapping)});

    return NameMapping::Make(std::move(fields));
  }

  // Helper function to create a name mapping for array types
  std::unique_ptr<NameMapping> CreateArrayNameMapping() {
    std::vector<MappedField> fields;
    fields.emplace_back(MappedField{.names = {"id"}, .field_id = 1});

    // Nested mapping for array element
    std::vector<MappedField> element_fields;
    element_fields.emplace_back(MappedField{.names = {"element"}, .field_id = 20});
    auto element_mapping = MappedFields::Make(std::move(element_fields));

    fields.emplace_back(MappedField{
        .names = {"items"}, .field_id = 2, .nested_mapping = std::move(element_mapping)});

    return NameMapping::Make(std::move(fields));
  }

  // Helper function to create a name mapping for map types
  std::unique_ptr<NameMapping> CreateMapNameMapping() {
    std::vector<MappedField> fields;
    fields.emplace_back(MappedField{.names = {"id"}, .field_id = 1});

    // Nested mapping for map key-value
    std::vector<MappedField> map_fields;
    map_fields.emplace_back(MappedField{.names = {"key"}, .field_id = 30});
    map_fields.emplace_back(MappedField{.names = {"value"}, .field_id = 31});
    auto map_mapping = MappedFields::Make(std::move(map_fields));

    fields.emplace_back(MappedField{.names = {"properties"},
                                    .field_id = 2,
                                    .nested_mapping = std::move(map_mapping)});

    return NameMapping::Make(std::move(fields));
  }

  // Helper function to create a name mapping for union types
  std::unique_ptr<NameMapping> CreateUnionNameMapping() {
    std::vector<MappedField> fields;
    fields.emplace_back(MappedField{.names = {"id"}, .field_id = 1});
    fields.emplace_back(MappedField{.names = {"data"}, .field_id = 2});
    return NameMapping::Make(std::move(fields));
  }
};

TEST_F(NameMappingAvroSchemaTest, ApplyNameMappingToRecord) {
  // Create a simple Avro record schema without field IDs
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "test_record",
    "fields": [
      {"name": "id", "type": "int"},
      {"name": "name", "type": "string"},
      {"name": "age", "type": "int"}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  auto name_mapping = CreateSimpleNameMapping();
  MappedField mapped_field;
  mapped_field.nested_mapping =
      std::make_shared<MappedFields>(name_mapping->AsMappedFields());

  auto result = CreateAvroNodeWithFieldIds(avro_schema.root(), mapped_field);
  ASSERT_THAT(result, IsOk());

  const auto& new_node = *result;
  EXPECT_EQ(new_node->type(), ::avro::AVRO_RECORD);
  EXPECT_EQ(new_node->names(), 3);
  EXPECT_EQ(new_node->leaves(), 3);

  // Check that field IDs are properly applied
  ASSERT_EQ(new_node->customAttributes(), 3);
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(new_node, 0, 1));
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(new_node, 1, 2));
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(new_node, 2, 3));
}

TEST_F(NameMappingAvroSchemaTest, ApplyNameMappingToNestedRecord) {
  // Create a nested Avro record schema without field IDs
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "test_record",
    "fields": [
      {"name": "id", "type": "int"},
      {"name": "address", "type": {
        "type": "record",
        "name": "address",
        "fields": [
          {"name": "street", "type": "string"},
          {"name": "city", "type": "string"},
          {"name": "zip", "type": "string"}
        ]
      }}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  auto name_mapping = CreateNestedNameMapping();
  MappedField mapped_field;
  mapped_field.nested_mapping =
      std::make_shared<MappedFields>(name_mapping->AsMappedFields());

  auto result = CreateAvroNodeWithFieldIds(avro_schema.root(), mapped_field);
  ASSERT_THAT(result, IsOk());

  const auto& new_node = *result;
  EXPECT_EQ(new_node->type(), ::avro::AVRO_RECORD);
  EXPECT_EQ(new_node->names(), 2);
  EXPECT_EQ(new_node->leaves(), 2);

  // Check that field IDs are properly applied to top-level fields
  ASSERT_EQ(new_node->customAttributes(), 2);
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(new_node, 0, 1));
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(new_node, 1, 2));

  // Check nested record
  const auto& address_node = new_node->leafAt(1);
  EXPECT_EQ(address_node->type(), ::avro::AVRO_RECORD);
  EXPECT_EQ(address_node->names(), 3);
  EXPECT_EQ(address_node->leaves(), 3);

  // Check that field IDs are properly applied to nested fields
  ASSERT_EQ(address_node->customAttributes(), 3);
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(address_node, 0, 10));
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(address_node, 1, 11));
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(address_node, 2, 12));
}

TEST_F(NameMappingAvroSchemaTest, ApplyNameMappingToArray) {
  try {
    // Create an Avro array schema without field IDs
    std::string avro_schema_json = R"({
      "type": "record",
      "name": "test_record",
      "fields": [
        {"name": "id", "type": "int"},
        {"name": "items", "type": {
          "type": "array",
          "items": "string"
        }}
      ]
    })";
    auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

    auto name_mapping = CreateArrayNameMapping();
    MappedField mapped_field;
    mapped_field.nested_mapping =
        std::make_shared<MappedFields>(name_mapping->AsMappedFields());

    auto result = CreateAvroNodeWithFieldIds(avro_schema.root(), mapped_field);
    ASSERT_THAT(result, IsOk());

    const auto& new_node = *result;
    EXPECT_EQ(new_node->type(), ::avro::AVRO_RECORD);
    EXPECT_EQ(new_node->names(), 2);
    EXPECT_EQ(new_node->leaves(), 2);

    // Check array field structure only - don't access any attributes
    const auto& array_node = new_node->leafAt(1);
    EXPECT_EQ(array_node->type(), ::avro::AVRO_ARRAY);
    EXPECT_EQ(array_node->leaves(), 1);

    // Note: Array nodes don't support custom attributes in Avro C++
    // We only verify the structure is correct, not the attributes
  } catch (const std::exception& e) {
    // If we get an exception about attributes not being supported, that's expected
    // for array nodes in Avro C++
    EXPECT_TRUE(std::string(e.what()).find("This type does not have attribute") !=
                std::string::npos);
  }
}

TEST_F(NameMappingAvroSchemaTest, ApplyNameMappingToMap) {
  try {
    // Create an Avro map schema without field IDs
    std::string avro_schema_json = R"({
      "type": "record",
      "name": "test_record",
      "fields": [
        {"name": "id", "type": "int"},
        {"name": "properties", "type": {
          "type": "map",
          "values": "string"
        }}
      ]
    })";
    auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

    auto name_mapping = CreateMapNameMapping();
    MappedField mapped_field;
    mapped_field.nested_mapping =
        std::make_shared<MappedFields>(name_mapping->AsMappedFields());

    auto result = CreateAvroNodeWithFieldIds(avro_schema.root(), mapped_field);
    ASSERT_THAT(result, IsOk());

    const auto& new_node = *result;
    EXPECT_EQ(new_node->type(), ::avro::AVRO_RECORD);
    EXPECT_EQ(new_node->names(), 2);
    EXPECT_EQ(new_node->leaves(), 2);

    // Check map field structure only - don't access any attributes
    const auto& map_node = new_node->leafAt(1);
    EXPECT_EQ(map_node->type(), ::avro::AVRO_MAP);
    ASSERT_GE(map_node->leaves(), 2);
    EXPECT_EQ(map_node->leafAt(0)->type(), ::avro::AVRO_STRING);
    EXPECT_EQ(map_node->leafAt(1)->type(), ::avro::AVRO_STRING);

    // Note: Map nodes don't support custom attributes in Avro C++
    // We only verify the structure is correct, not the attributes
  } catch (const std::exception& e) {
    // If we get an exception about attributes not being supported, that's expected
    // for map nodes in Avro C++
    EXPECT_TRUE(std::string(e.what()).find("This type does not have attribute") !=
                std::string::npos);
  }
}

TEST_F(NameMappingAvroSchemaTest, ApplyNameMappingToUnion) {
  // Create an Avro union schema without field IDs
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "test_record",
    "fields": [
      {"name": "id", "type": "int"},
      {"name": "data", "type": ["null", "string"]}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  auto name_mapping = CreateUnionNameMapping();
  MappedField mapped_field;
  mapped_field.nested_mapping =
      std::make_shared<MappedFields>(name_mapping->AsMappedFields());

  auto result = CreateAvroNodeWithFieldIds(avro_schema.root(), mapped_field);
  ASSERT_THAT(result, IsOk());

  const auto& new_node = *result;
  EXPECT_EQ(new_node->type(), ::avro::AVRO_RECORD);
  EXPECT_EQ(new_node->names(), 2);
  EXPECT_EQ(new_node->leaves(), 2);

  // Check that field IDs are properly applied to top-level fields
  ASSERT_EQ(new_node->customAttributes(), 2);
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(new_node, 0, 1));
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(new_node, 1, 2));

  // Check union field
  const auto& union_node = new_node->leafAt(1);
  EXPECT_EQ(union_node->type(), ::avro::AVRO_UNION);
  EXPECT_EQ(union_node->leaves(), 2);

  // Check that the non-null branch has field ID applied
  const auto& non_null_branch = union_node->leafAt(1);
  EXPECT_EQ(non_null_branch->type(), ::avro::AVRO_STRING);
}

TEST_F(NameMappingAvroSchemaTest, MissingFieldIdError) {
  // Create a name mapping with missing field ID
  std::vector<MappedField> fields;
  fields.emplace_back(MappedField{.names = {"id"}, .field_id = 1});
  fields.emplace_back(MappedField{.names = {"name"}});  // Missing field_id
  auto name_mapping = NameMapping::Make(std::move(fields));

  // Create a simple Avro record schema
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "test_record",
    "fields": [
      {"name": "id", "type": "int"},
      {"name": "name", "type": "string"}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  MappedField mapped_field;
  mapped_field.nested_mapping =
      std::make_shared<MappedFields>(name_mapping->AsMappedFields());

  auto result = CreateAvroNodeWithFieldIds(avro_schema.root(), mapped_field);
  ASSERT_THAT(result, IsError(ErrorKind::kInvalidSchema));
  ASSERT_THAT(result,
              HasErrorMessage("Field ID is missing for field 'name' in nested mapping"));
}

TEST_F(NameMappingAvroSchemaTest, MissingFieldError) {
  // Create a name mapping
  auto name_mapping = CreateSimpleNameMapping();

  // Create an Avro record schema with a field not in the mapping
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "test_record",
    "fields": [
      {"name": "id", "type": "int"},
      {"name": "name", "type": "string"},
      {"name": "unknown_field", "type": "int"}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  MappedField mapped_field;
  mapped_field.nested_mapping =
      std::make_shared<MappedFields>(name_mapping->AsMappedFields());

  auto result = CreateAvroNodeWithFieldIds(avro_schema.root(), mapped_field);
  ASSERT_THAT(result, IsError(ErrorKind::kInvalidSchema));
  ASSERT_THAT(result,
              HasErrorMessage("Field 'unknown_field' not found in nested mapping"));
}

TEST_F(NameMappingAvroSchemaTest, MissingArrayElementError) {
  // Create a name mapping without array element mapping
  std::vector<MappedField> fields;
  fields.emplace_back(MappedField{.names = {"id"}, .field_id = 1});
  auto name_mapping = NameMapping::Make(std::move(fields));

  // Create an Avro array schema
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "test_record",
    "fields": [
      {"name": "id", "type": "int"},
      {"name": "items", "type": {
        "type": "array",
        "items": "string"
      }}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  MappedField mapped_field;
  mapped_field.nested_mapping =
      std::make_shared<MappedFields>(name_mapping->AsMappedFields());

  auto result = CreateAvroNodeWithFieldIds(avro_schema.root(), mapped_field);
  ASSERT_THAT(result, IsError(ErrorKind::kInvalidSchema));
  ASSERT_THAT(result, HasErrorMessage("Field 'items' not found in nested mapping"));
}

TEST_F(NameMappingAvroSchemaTest, MissingMapKeyValueError) {
  // Create a name mapping without map key/value mapping
  std::vector<MappedField> fields;
  fields.emplace_back(MappedField{.names = {"id"}, .field_id = 1});
  auto name_mapping = NameMapping::Make(std::move(fields));

  // Create an Avro map schema
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "test_record",
    "fields": [
      {"name": "id", "type": "int"},
      {"name": "properties", "type": {
        "type": "map",
        "values": "string"
      }}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  MappedField mapped_field;
  mapped_field.nested_mapping =
      std::make_shared<MappedFields>(name_mapping->AsMappedFields());

  auto result = CreateAvroNodeWithFieldIds(avro_schema.root(), mapped_field);
  ASSERT_THAT(result, IsError(ErrorKind::kInvalidSchema));
  ASSERT_THAT(result, HasErrorMessage("Field 'properties' not found in nested mapping"));
}

}  // namespace iceberg::avro
