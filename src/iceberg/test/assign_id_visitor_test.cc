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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"
#include "iceberg/util/type_util.h"

namespace iceberg {

namespace {

Schema CreateFlatSchema() {
  return Schema({
      SchemaField::MakeRequired(/*field_id=*/10, "id", iceberg::int64()),
      SchemaField::MakeOptional(/*field_id=*/20, "name", iceberg::string()),
      SchemaField::MakeOptional(/*field_id=*/30, "age", iceberg::int32()),
      SchemaField::MakeRequired(/*field_id=*/40, "data", iceberg::float64()),
  });
}

std::shared_ptr<Type> CreateListOfStruct() {
  return std::make_shared<ListType>(SchemaField::MakeOptional(
      /*field_id=*/101, "element",
      std::make_shared<StructType>(std::vector<SchemaField>{
          SchemaField::MakeOptional(/*field_id=*/102, "x", iceberg::int32()),
          SchemaField::MakeRequired(/*field_id=*/103, "y", iceberg::string()),
      })));
}

std::shared_ptr<Type> CreateMapWithStructValue() {
  return std::make_shared<MapType>(
      SchemaField::MakeRequired(/*field_id=*/201, "key", iceberg::string()),
      SchemaField::MakeRequired(
          /*field_id=*/202, "value",
          std::make_shared<StructType>(std::vector<SchemaField>{
              SchemaField::MakeRequired(/*field_id=*/203, "id", iceberg::int64()),
              SchemaField::MakeOptional(/*field_id=*/204, "name", iceberg::string()),
          })));
}

std::shared_ptr<Type> CreateNestedStruct() {
  return std::make_shared<StructType>(std::vector<SchemaField>{
      SchemaField::MakeRequired(/*field_id=*/301, "outer_id", iceberg::int64()),
      SchemaField::MakeRequired(
          /*field_id=*/302, "nested",
          std::make_shared<StructType>(std::vector<SchemaField>{
              SchemaField::MakeOptional(/*field_id=*/303, "inner_id", iceberg::int32()),
              SchemaField::MakeRequired(/*field_id=*/304, "inner_name",
                                        iceberg::string()),
          })),
  });
}

Result<std::unique_ptr<Schema>> CreateNestedSchema(
    std::vector<int32_t> identifier_field_ids = {}) {
  return Schema::Make(
      {
          SchemaField::MakeRequired(/*field_id=*/10, "id", iceberg::int64()),
          SchemaField::MakeOptional(/*field_id=*/20, "list", CreateListOfStruct()),
          SchemaField::MakeOptional(/*field_id=*/30, "map", CreateMapWithStructValue()),
          SchemaField::MakeRequired(/*field_id=*/40, "struct", CreateNestedStruct()),
      },
      Schema::kInitialSchemaId, std::move(identifier_field_ids));
}

}  // namespace

TEST(AssignFreshIdVisitorTest, FlatSchema) {
  Schema schema = CreateFlatSchema();

  std::atomic<int32_t> id = 0;
  auto next_id = [&id]() { return ++id; };
  ICEBERG_UNWRAP_OR_FAIL(auto fresh_schema,
                         AssignFreshIds(Schema::kInitialSchemaId, schema, next_id));

  ASSERT_EQ(fresh_schema->fields().size(), schema.fields().size());
  EXPECT_EQ(Schema(
                {
                    SchemaField::MakeRequired(/*field_id=*/1, "id", iceberg::int64()),
                    SchemaField::MakeOptional(/*field_id=*/2, "name", iceberg::string()),
                    SchemaField::MakeOptional(/*field_id=*/3, "age", iceberg::int32()),
                    SchemaField::MakeRequired(/*field_id=*/4, "data", iceberg::float64()),
                },
                Schema::kInitialSchemaId),
            *fresh_schema);
}

TEST(AssignFreshIdVisitorTest, NestedSchema) {
  ICEBERG_UNWRAP_OR_FAIL(auto schema, CreateNestedSchema());
  std::atomic<int32_t> id = 0;
  auto next_id = [&id]() { return ++id; };
  ICEBERG_UNWRAP_OR_FAIL(auto fresh_schema,
                         AssignFreshIds(Schema::kInitialSchemaId, *schema, next_id));

  ASSERT_EQ(4, fresh_schema->fields().size());
  for (int32_t i = 0; i < fresh_schema->fields().size(); ++i) {
    EXPECT_EQ(i + 1, fresh_schema->fields()[i].field_id());
  }

  auto list_field = fresh_schema->fields()[1];
  auto list_type = std::dynamic_pointer_cast<ListType>(list_field.type());
  ASSERT_TRUE(list_type);
  auto list_element_field = list_type->fields()[0];
  EXPECT_EQ(5, list_element_field.field_id());
  auto list_element_type =
      std::dynamic_pointer_cast<StructType>(list_element_field.type());
  ASSERT_TRUE(list_element_type);
  EXPECT_EQ(StructType(std::vector<SchemaField>{
                SchemaField::MakeOptional(/*field_id=*/6, "x", iceberg::int32()),
                SchemaField::MakeRequired(/*field_id=*/7, "y", iceberg::string()),
            }),
            *list_element_type);

  auto map_field = fresh_schema->fields()[2];
  auto map_type = std::dynamic_pointer_cast<MapType>(map_field.type());
  ASSERT_TRUE(map_type);
  EXPECT_EQ(8, map_type->fields()[0].field_id());
  auto map_value_field = map_type->fields()[1];
  EXPECT_EQ(9, map_value_field.field_id());
  auto map_value_type = std::dynamic_pointer_cast<StructType>(map_value_field.type());
  ASSERT_TRUE(map_value_type);
  EXPECT_EQ(StructType(std::vector<SchemaField>{
                SchemaField::MakeRequired(/*field_id=*/10, "id", iceberg::int64()),
                SchemaField::MakeOptional(/*field_id=*/11, "name", iceberg::string()),
            }),
            *map_value_type);

  auto struct_field = fresh_schema->fields()[3];
  auto struct_type = std::dynamic_pointer_cast<StructType>(struct_field.type());
  ASSERT_TRUE(struct_type);

  auto expect_nested_struct_type = std::make_shared<StructType>(std::vector<SchemaField>{
      SchemaField::MakeOptional(/*field_id=*/14, "inner_id", iceberg::int32()),
      SchemaField::MakeRequired(/*field_id=*/15, "inner_name", iceberg::string()),
  });
  EXPECT_EQ(StructType(std::vector<SchemaField>{
                SchemaField::MakeRequired(/*field_id=*/12, "outer_id", iceberg::int64()),
                SchemaField::MakeRequired(
                    /*field_id=*/13, "nested", expect_nested_struct_type)}),
            *struct_type);

  auto nested_struct_field = struct_type->fields()[1];
  auto nested_struct_type =
      std::dynamic_pointer_cast<StructType>(nested_struct_field.type());
  ASSERT_TRUE(nested_struct_type);
  EXPECT_EQ(*expect_nested_struct_type, *nested_struct_type);
}

TEST(AssignFreshIdVisitorTest, RefreshIdentifierId) {
  int32_t id = 0;
  auto next_id = [&id]() { return ++id; };

  ICEBERG_UNWRAP_OR_FAIL(auto schema, CreateNestedSchema({10, 301}));
  ICEBERG_UNWRAP_OR_FAIL(auto fresh_schema,
                         AssignFreshIds(Schema::kInitialSchemaId, *schema, next_id));
  EXPECT_THAT(fresh_schema->IdentifierFieldIds(), testing::ElementsAre(1, 12));
  ICEBERG_UNWRAP_OR_FAIL(auto identifier_field_names,
                         fresh_schema->IdentifierFieldNames());
  EXPECT_THAT(identifier_field_names, testing::ElementsAre("id", "struct.outer_id"));
}

}  // namespace iceberg
