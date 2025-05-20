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

// Helper function to create a standard source schema for testing
Schema CreateSourceSchema() {
  return Schema({
      SchemaField(/*field_id=*/1, "id", std::make_shared<LongType>(),
                  /*optional=*/false),
      SchemaField(/*field_id=*/2, "name", std::make_shared<StringType>(),
                  /*optional=*/true),
      SchemaField(/*field_id=*/3, "age", std::make_shared<IntType>(),
                  /*optional=*/true),
      SchemaField(/*field_id=*/4, "data", std::make_shared<DoubleType>(),
                  /*optional=*/false),
  });
}

}  // namespace

TEST(SchemaUtilTest, ProjectIdenticalSchemas) {
  Schema schema = CreateSourceSchema();

  auto projection_result = Project(schema, schema, /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 4);

  for (size_t i = 0; i < projection.fields.size(); ++i) {
    AssertProjectedField(projection.fields[i], i);
  }
}

TEST(SchemaUtilTest, ProjectSubsetSchema) {
  Schema source_schema = CreateSourceSchema();
  Schema expected_schema({
      SchemaField(/*field_id=*/1, "id", std::make_shared<LongType>(),
                  /*optional=*/false),
      SchemaField(/*field_id=*/3, "age", std::make_shared<IntType>(),
                  /*optional=*/true),
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
  Schema source_schema = CreateSourceSchema();
  Schema expected_schema({
      SchemaField(/*field_id=*/1, "id", std::make_shared<LongType>(),
                  /*optional=*/false),
      SchemaField(/*field_id=*/3, "age", std::make_shared<IntType>(),
                  /*optional=*/true),
  });

  auto projection_result = Project(expected_schema, source_schema, /*prune_source=*/true);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 2);

  AssertProjectedField(projection.fields[0], 0);
  AssertProjectedField(projection.fields[1], 1);
}

TEST(SchemaUtilTest, ProjectMissingOptionalField) {
  Schema source_schema = CreateSourceSchema();
  Schema expected_schema({
      SchemaField(/*field_id=*/1, "id", std::make_shared<LongType>(),
                  /*optional=*/false),
      SchemaField(/*field_id=*/2, "name", std::make_shared<StringType>(),
                  /*optional=*/true),
      SchemaField(/*field_id=*/10, "extra", std::make_shared<StringType>(),
                  /*optional=*/true),
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
  Schema source_schema = CreateSourceSchema();
  Schema expected_schema({
      SchemaField(/*field_id=*/1, "id", std::make_shared<LongType>(),
                  /*optional=*/false),
      SchemaField(/*field_id=*/2, "name", std::make_shared<StringType>(),
                  /*optional=*/true),
      SchemaField(/*field_id=*/10, "extra", std::make_shared<StringType>(),
                  /*optional=*/false),
  });

  auto projection_result =
      Project(expected_schema, source_schema, /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsError(ErrorKind::kInvalidSchema));
  ASSERT_THAT(projection_result, HasErrorMessage("Missing required field"));
}

TEST(SchemaUtilTest, ProjectMetadataColumn) {
  Schema source_schema = CreateSourceSchema();
  Schema expected_schema({
      SchemaField(/*field_id=*/1, "id", std::make_shared<LongType>(),
                  /*optional=*/false),
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
  Schema source_schema({SchemaField(/*field_id=*/1, "id", std::make_shared<IntType>(),
                                    /*optional=*/false)});
  Schema expected_schema({SchemaField(/*field_id=*/1, "id", std::make_shared<LongType>(),
                                      /*optional=*/false)});

  auto projection_result =
      Project(expected_schema, source_schema, /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 1);
  AssertProjectedField(projection.fields[0], 0);
}

TEST(SchemaUtilTest, ProjectSchemaEvolutionFloatToDouble) {
  Schema source_schema(
      {SchemaField(/*field_id=*/2, "value", std::make_shared<FloatType>(),
                   /*optional=*/true)});
  Schema expected_schema(
      {SchemaField(/*field_id=*/2, "value", std::make_shared<DoubleType>(),
                   /*optional=*/true)});

  auto projection_result =
      Project(expected_schema, source_schema, /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 1);
  AssertProjectedField(projection.fields[0], 0);
}

TEST(SchemaUtilTest, ProjectSchemaEvolutionDecimalCompatible) {
  Schema source_schema(
      {SchemaField(/*field_id=*/2, "value", std::make_shared<DecimalType>(9, 2),
                   /*optional=*/true)});
  Schema expected_schema(
      {SchemaField(/*field_id=*/2, "value", std::make_shared<DecimalType>(18, 2),
                   /*optional=*/true)});

  auto projection_result =
      Project(expected_schema, source_schema, /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 1);
  AssertProjectedField(projection.fields[0], 0);
}

TEST(SchemaUtilTest, ProjectSchemaEvolutionDecimalIncompatible) {
  Schema source_schema(
      {SchemaField(/*field_id=*/2, "value", std::make_shared<DecimalType>(9, 2),
                   /*optional=*/true)});
  Schema expected_schema(
      {SchemaField(/*field_id=*/2, "value", std::make_shared<DecimalType>(18, 3),
                   /*optional=*/true)});

  auto projection_result =
      Project(expected_schema, source_schema, /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsError(ErrorKind::kNotSupported));
  ASSERT_THAT(projection_result, HasErrorMessage("Cannot read"));
}

TEST(SchemaUtilTest, ProjectSchemaEvolutionIncompatibleTypes) {
  Schema source_schema(
      {SchemaField(/*field_id=*/1, "value", std::make_shared<StringType>(),
                   /*optional=*/true)});
  Schema expected_schema(
      {SchemaField(/*field_id=*/1, "value", std::make_shared<IntType>(),
                   /*optional=*/true)});

  auto projection_result =
      Project(expected_schema, source_schema, /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsError(ErrorKind::kNotSupported));
  ASSERT_THAT(projection_result, HasErrorMessage("Cannot read"));
}

TEST(SchemaUtilTest, ProjectNestedStructures) {
  Schema schema(
      {SchemaField(/*field_id=*/1, "id", std::make_shared<LongType>(),
                   /*optional=*/false),
       SchemaField(/*field_id=*/2, "name", std::make_shared<StringType>(),
                   /*optional=*/true),
       SchemaField(
           /*field_id=*/3, "address",
           std::make_shared<StructType>(std::vector<SchemaField>{
               SchemaField(/*field_id=*/101, "street", std::make_shared<StringType>(),
                           /*optional=*/true),
               SchemaField(/*field_id=*/102, "city", std::make_shared<StringType>(),
                           /*optional=*/true),
           }),
           /*optional=*/true)});

  auto projection_result = Project(schema, schema, /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 3);

  AssertProjectedField(projection.fields[0], 0);
  AssertProjectedField(projection.fields[1], 1);
  AssertProjectedField(projection.fields[2], 2);

  ASSERT_EQ(projection.fields[2].children.size(), 1);
  ASSERT_EQ(projection.fields[2].children[0].children.size(), 2);

  const auto& struct_projection = projection.fields[2].children[0];
  AssertProjectedField(struct_projection.children[0], 0);
  AssertProjectedField(struct_projection.children[1], 1);
}

TEST(SchemaUtilTest, ProjectSubsetNestedFields) {
  Schema source_schema(
      {SchemaField(/*field_id=*/1, "id", std::make_shared<LongType>(),
                   /*optional=*/false),
       SchemaField(/*field_id=*/2, "name", std::make_shared<StringType>(),
                   /*optional=*/true),
       SchemaField(
           /*field_id=*/3, "address",
           std::make_shared<StructType>(std::vector<SchemaField>{
               SchemaField(/*field_id=*/101, "street", std::make_shared<StringType>(),
                           /*optional=*/true),
               SchemaField(/*field_id=*/102, "city", std::make_shared<StringType>(),
                           /*optional=*/true),
               SchemaField(/*field_id=*/103, "zip", std::make_shared<StringType>(),
                           /*optional=*/true),
           }),
           /*optional=*/true)});

  Schema expected_schema(
      {SchemaField(/*field_id=*/1, "id", std::make_shared<LongType>(),
                   /*optional=*/false),
       SchemaField(
           /*field_id=*/3, "address",
           std::make_shared<StructType>(std::vector<SchemaField>{
               SchemaField(/*field_id=*/102, "city", std::make_shared<StringType>(),
                           /*optional=*/true),
               SchemaField(/*field_id=*/101, "street", std::make_shared<StringType>(),
                           /*optional=*/true),
           }),
           /*optional=*/true)});

  auto projection_result = Project(expected_schema, source_schema, /*prune_source=*/true);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 2);

  AssertProjectedField(projection.fields[0], 0);
  AssertProjectedField(projection.fields[1], 1);

  ASSERT_EQ(projection.fields[1].children.size(), 1);
  ASSERT_EQ(projection.fields[1].children[0].children.size(), 2);

  const auto& struct_projection = projection.fields[1].children[0];
  AssertProjectedField(struct_projection.children[0], 1);
  AssertProjectedField(struct_projection.children[1], 0);
}

}  // namespace iceberg
