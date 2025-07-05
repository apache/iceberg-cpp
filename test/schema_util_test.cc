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

#include "iceberg/schema_util.h"

#include <memory>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/metadata_columns.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/type.h"
#include "matchers.h"

namespace iceberg {

namespace {

// Helper function to check if a field projection is of the expected kind
void AssertProjectedField(const FieldProjection& projection, size_t expected_index) {
  ASSERT_EQ(projection.kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.from), expected_index);
}

Schema CreateFlatSchema() {
  return Schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", iceberg::int64()),
      SchemaField::MakeOptional(/*field_id=*/2, "name", iceberg::string()),
      SchemaField::MakeOptional(/*field_id=*/3, "age", iceberg::int32()),
      SchemaField::MakeRequired(/*field_id=*/4, "data", iceberg::float64()),
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

std::shared_ptr<Type> CreateListOfList() {
  return std::make_shared<ListType>(SchemaField::MakeRequired(
      /*field_id=*/401, "element",
      std::make_shared<ListType>(SchemaField::MakeOptional(
          /*field_id=*/402, "element", iceberg::float64()))));
}

std::shared_ptr<Type> CreateMapOfList() {
  return std::make_shared<MapType>(
      SchemaField::MakeRequired(/*field_id=*/501, "key", iceberg::string()),
      SchemaField::MakeRequired(
          /*field_id=*/502, "value",
          std::make_shared<ListType>(SchemaField::MakeOptional(
              /*field_id=*/503, "element", iceberg::int32()))));
}

}  // namespace

TEST(SchemaUtilTest, ProjectIdenticalSchemas) {
  Schema schema = CreateFlatSchema();

  auto projection_result = Project(schema, schema, /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 4);
  for (size_t i = 0; i < projection.fields.size(); ++i) {
    AssertProjectedField(projection.fields[i], i);
  }
}

TEST(SchemaUtilTest, ProjectSubsetSchema) {
  Schema source_schema = CreateFlatSchema();
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", iceberg::int64()),
      SchemaField::MakeOptional(/*field_id=*/3, "age", iceberg::int32()),
  });

  auto projection_result =
      Project(expected_schema, source_schema, /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 2);
  AssertProjectedField(projection.fields[0], 0);
  AssertProjectedField(projection.fields[1], 2);
}

TEST(SchemaUtilTest, ProjectWithPruning) {
  Schema source_schema = CreateFlatSchema();
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", iceberg::int64()),
      SchemaField::MakeOptional(/*field_id=*/3, "age", iceberg::int32()),
  });

  auto projection_result = Project(expected_schema, source_schema, /*prune_source=*/true);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 2);
  AssertProjectedField(projection.fields[0], 0);
  AssertProjectedField(projection.fields[1], 1);
}

TEST(SchemaUtilTest, ProjectMissingOptionalField) {
  Schema source_schema = CreateFlatSchema();
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", iceberg::int64()),
      SchemaField::MakeOptional(/*field_id=*/2, "name", iceberg::string()),
      SchemaField::MakeOptional(/*field_id=*/10, "extra", iceberg::string()),
  });

  auto projection_result = Project(expected_schema, source_schema, false);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 3);

  AssertProjectedField(projection.fields[0], 0);
  AssertProjectedField(projection.fields[1], 1);
  ASSERT_EQ(projection.fields[2].kind, FieldProjection::Kind::kNull);
}

TEST(SchemaUtilTest, ProjectMissingRequiredField) {
  Schema source_schema = CreateFlatSchema();
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", iceberg::int64()),
      SchemaField::MakeOptional(/*field_id=*/2, "name", iceberg::string()),
      SchemaField::MakeRequired(/*field_id=*/10, "extra", iceberg::string()),
  });

  auto projection_result =
      Project(expected_schema, source_schema, /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsError(ErrorKind::kInvalidSchema));
  ASSERT_THAT(projection_result, HasErrorMessage("Missing required field"));
}

TEST(SchemaUtilTest, ProjectMetadataColumn) {
  Schema source_schema = CreateFlatSchema();
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", iceberg::int64()),
      MetadataColumns::kFilePath,
  });

  auto projection_result =
      Project(expected_schema, source_schema, /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 2);
  AssertProjectedField(projection.fields[0], 0);
  ASSERT_EQ(projection.fields[1].kind, FieldProjection::Kind::kMetadata);
}

TEST(SchemaUtilTest, ProjectSchemaEvolutionIntToLong) {
  Schema source_schema(
      {SchemaField::MakeRequired(/*field_id=*/1, "id", iceberg::int32())});
  Schema expected_schema(
      {SchemaField::MakeRequired(/*field_id=*/1, "id", iceberg::int64())});

  auto projection_result =
      Project(expected_schema, source_schema, /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 1);
  AssertProjectedField(projection.fields[0], 0);
}

TEST(SchemaUtilTest, ProjectSchemaEvolutionFloatToDouble) {
  Schema source_schema(
      {SchemaField::MakeOptional(/*field_id=*/2, "value", iceberg::float32())});
  Schema expected_schema(
      {SchemaField::MakeOptional(/*field_id=*/2, "value", iceberg::float64())});

  auto projection_result =
      Project(expected_schema, source_schema, /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 1);
  AssertProjectedField(projection.fields[0], 0);
}

TEST(SchemaUtilTest, ProjectSchemaEvolutionDecimalCompatible) {
  Schema source_schema(
      {SchemaField::MakeOptional(/*field_id=*/2, "value", iceberg::decimal(9, 2))});
  Schema expected_schema({SchemaField::MakeOptional(
      /*field_id=*/2, "value", iceberg::decimal(18, 2))});

  auto projection_result =
      Project(expected_schema, source_schema, /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 1);
  AssertProjectedField(projection.fields[0], 0);
}

TEST(SchemaUtilTest, ProjectSchemaEvolutionDecimalIncompatible) {
  Schema source_schema(
      {SchemaField::MakeOptional(/*field_id=*/2, "value", iceberg::decimal(9, 2))});
  Schema expected_schema({SchemaField::MakeOptional(
      /*field_id=*/2, "value", iceberg::decimal(18, 3))});

  auto projection_result =
      Project(expected_schema, source_schema, /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsError(ErrorKind::kNotSupported));
  ASSERT_THAT(projection_result, HasErrorMessage("Cannot read"));
}

TEST(SchemaUtilTest, ProjectSchemaEvolutionIncompatibleTypes) {
  Schema source_schema(
      {SchemaField::MakeOptional(/*field_id=*/1, "value", iceberg::string())});
  Schema expected_schema(
      {SchemaField::MakeOptional(/*field_id=*/1, "value", iceberg::int32())});

  auto projection_result =
      Project(expected_schema, source_schema, /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsError(ErrorKind::kNotSupported));
  ASSERT_THAT(projection_result, HasErrorMessage("Cannot read"));
}

TEST(SchemaUtilTest, ProjectNestedStructures) {
  Schema schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", iceberg::int64()),
      SchemaField::MakeOptional(/*field_id=*/2, "name", iceberg::string()),
      SchemaField::MakeOptional(
          /*field_id=*/3, "address",
          std::make_shared<StructType>(std::vector<SchemaField>{
              SchemaField::MakeOptional(/*field_id=*/101, "street", iceberg::string()),
              SchemaField::MakeOptional(/*field_id=*/102, "city", iceberg::string()),
          })),
  });

  auto projection_result = Project(schema, schema, /*prune_source=*/true);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 3);

  // Verify top-level fields are projected correctly
  AssertProjectedField(projection.fields[0], 0);  // id
  AssertProjectedField(projection.fields[1], 1);  // name
  AssertProjectedField(projection.fields[2], 2);  // address

  // Verify struct field has children correctly mapped
  ASSERT_EQ(projection.fields[2].children.size(), 2);
  AssertProjectedField(projection.fields[2].children[0], 0);  // address.street
  AssertProjectedField(projection.fields[2].children[1], 1);  // address.city
}

TEST(SchemaUtilTest, ProjectSubsetNestedFields) {
  Schema source_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", iceberg::int64()),
      SchemaField::MakeOptional(/*field_id=*/2, "name", iceberg::string()),
      SchemaField::MakeOptional(
          /*field_id=*/3, "address",
          std::make_shared<StructType>(std::vector<SchemaField>{
              SchemaField::MakeOptional(/*field_id=*/101, "street", iceberg::string()),
              SchemaField::MakeOptional(/*field_id=*/102, "city", iceberg::string()),
              SchemaField::MakeOptional(/*field_id=*/103, "zip", iceberg::string()),
          })),
  });

  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", iceberg::int64()),
      SchemaField::MakeOptional(
          /*field_id=*/3, "address",
          std::make_shared<StructType>(std::vector<SchemaField>{
              SchemaField::MakeOptional(/*field_id=*/102, "city", iceberg::string()),
              SchemaField::MakeOptional(/*field_id=*/101, "street", iceberg::string()),
          })),
  });

  auto projection_result = Project(expected_schema, source_schema, /*prune_source=*/true);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 2);

  // Verify top-level fields are projected correctly
  AssertProjectedField(projection.fields[0], 0);  // id
  AssertProjectedField(projection.fields[1], 1);  // address

  // Verify struct field has children correctly mapped
  ASSERT_EQ(projection.fields[1].children.size(), 2);
  AssertProjectedField(projection.fields[1].children[0], 1);  // address.city
  AssertProjectedField(projection.fields[1].children[1], 0);  // address.street
}

TEST(SchemaUtilTest, ProjectListOfStruct) {
  Schema source_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", iceberg::int64()),
      SchemaField::MakeOptional(/*field_id=*/2, "items", CreateListOfStruct()),
  });

  // Identity projection
  for (const auto& prune_source : {true, false}) {
    auto projection_result = Project(source_schema, source_schema, prune_source);
    ASSERT_THAT(projection_result, IsOk());

    const auto& projection = *projection_result;
    ASSERT_EQ(projection.fields.size(), 2);
    AssertProjectedField(projection.fields[0], 0);  // id
    AssertProjectedField(projection.fields[1], 1);  // items

    // Verify list field has children correctly mapped
    ASSERT_EQ(projection.fields[1].children.size(), 1);
    const auto& struct_projection = projection.fields[1].children[0];
    ASSERT_EQ(struct_projection.children.size(), 2);
    AssertProjectedField(struct_projection.children[0], 0);  // item.element.x
    AssertProjectedField(struct_projection.children[1], 1);  // item.element.y
  }

  // Subset of field selection with list of struct
  {
    Schema expected_schema({
        SchemaField::MakeOptional(
            /*field_id=*/2, "items",
            std::make_shared<ListType>(SchemaField::MakeOptional(
                /*field_id=*/101, "element",
                std::make_shared<StructType>(
                    std::vector<SchemaField>{SchemaField::MakeRequired(
                        /*field_id=*/103, "y", iceberg::string())})))),
    });

    auto projection_result =
        Project(expected_schema, source_schema, /*prune_source=*/true);
    ASSERT_THAT(projection_result, IsOk());

    const auto& projection = *projection_result;
    ASSERT_EQ(projection.fields.size(), 1);
    AssertProjectedField(projection.fields[0], 0);  // items

    // Verify list field has children correctly mapped
    ASSERT_EQ(projection.fields[0].children.size(), 1);
    const auto& struct_projection = projection.fields[0].children[0];
    ASSERT_EQ(struct_projection.children.size(), 1);
    AssertProjectedField(struct_projection.children[0], 0);  // item.element.y
  }
}

TEST(SchemaUtilTest, ProjectMapWithStructValue) {
  Schema source_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", iceberg::int64()),
      SchemaField::MakeOptional(/*field_id=*/2, "attributes", CreateMapWithStructValue()),
  });
  Schema expected_schema({
      SchemaField::MakeOptional(/*field_id=*/2, "attributes", CreateMapWithStructValue()),
  });

  auto projection_result = Project(expected_schema, source_schema, /*prune_source=*/true);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 1);
  AssertProjectedField(projection.fields[0], 0);

  ASSERT_EQ(projection.fields[0].children.size(), 2);
  AssertProjectedField(projection.fields[0].children[0], 0);  // attributes.value.id
  AssertProjectedField(projection.fields[0].children[1], 1);  // attributes.value.name
}

TEST(SchemaUtilTest, ProjectComplexMixedTypes) {
  Schema source_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", iceberg::int64()),
      SchemaField::MakeOptional(/*field_id=*/2, "lists", CreateListOfStruct()),
      SchemaField::MakeRequired(/*field_id=*/3, "mappings", CreateMapWithStructValue()),
      SchemaField::MakeOptional(/*field_id=*/4, "nested", CreateNestedStruct()),
  });
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", iceberg::int64()),
      SchemaField::MakeRequired(/*field_id=*/3, "mappings", CreateMapWithStructValue()),
  });

  // Test with prune_source = true
  auto projection_result = Project(expected_schema, source_schema, /*prune_source=*/true);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 2);
  AssertProjectedField(projection.fields[0], 0);  // id
  AssertProjectedField(projection.fields[1], 1);  // mappings

  // Test with prune_source = false
  auto unpruned_result = Project(expected_schema, source_schema, /*prune_source=*/false);
  ASSERT_THAT(unpruned_result, IsOk());

  const auto& unpruned_projection = *unpruned_result;
  ASSERT_EQ(unpruned_projection.fields.size(), 2);
  AssertProjectedField(unpruned_projection.fields[0], 0);  // id
  AssertProjectedField(unpruned_projection.fields[1], 2);  // mappings
}

TEST(SchemaUtilTest, ProjectIncompatibleNestedTypes) {
  Schema source_schema({
      SchemaField::MakeOptional(/*field_id=*/1, "data", CreateListOfStruct()),
  });
  Schema incompatible_schema({
      SchemaField::MakeOptional(/*field_id=*/1, "data", CreateMapWithStructValue()),
  });

  auto projection_result =
      Project(incompatible_schema, source_schema, /*prune_source=*/false);
  ASSERT_FALSE(projection_result.has_value());
  ASSERT_THAT(projection_result, IsError(ErrorKind::kInvalidSchema));
  ASSERT_THAT(projection_result, HasErrorMessage("Expected map"));
  ASSERT_THAT(projection_result, HasErrorMessage("but got list"));
}

}  // namespace iceberg
