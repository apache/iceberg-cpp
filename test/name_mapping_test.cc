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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

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
