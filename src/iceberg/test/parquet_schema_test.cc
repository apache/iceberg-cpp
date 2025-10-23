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

#include <arrow/type.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/schema.h>
#include <parquet/schema.h>
#include <parquet/types.h>

#include "iceberg/metadata_columns.h"
#include "iceberg/parquet/parquet_schema_util_internal.h"
#include "iceberg/schema.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"

namespace iceberg::parquet {

namespace {

constexpr std::string_view kParquetFieldIdKey = "PARQUET:field_id";

::parquet::schema::NodePtr MakeInt32Node(const std::string& name, int field_id = -1,
                                         bool optional = true) {
  return ::parquet::schema::PrimitiveNode::Make(
      name, optional ? ::parquet::Repetition::OPTIONAL : ::parquet::Repetition::REQUIRED,
      ::parquet::LogicalType::None(), ::parquet::Type::INT32, /*primitive_length=*/-1,
      field_id);
}

::parquet::schema::NodePtr MakeInt64Node(const std::string& name, int field_id = -1,
                                         bool optional = true) {
  return ::parquet::schema::PrimitiveNode::Make(
      name, optional ? ::parquet::Repetition::OPTIONAL : ::parquet::Repetition::REQUIRED,
      ::parquet::LogicalType::None(), ::parquet::Type::INT64, /*primitive_length=*/-1,
      field_id);
}

::parquet::schema::NodePtr MakeStringNode(const std::string& name, int field_id = -1,
                                          bool optional = true) {
  return ::parquet::schema::PrimitiveNode::Make(
      name, optional ? ::parquet::Repetition::OPTIONAL : ::parquet::Repetition::REQUIRED,
      ::parquet::LogicalType::String(), ::parquet::Type::BYTE_ARRAY,
      /*primitive_length=*/-1, field_id);
}

::parquet::schema::NodePtr MakeDoubleNode(const std::string& name, int field_id = -1,
                                          bool optional = true) {
  return ::parquet::schema::PrimitiveNode::Make(
      name, optional ? ::parquet::Repetition::OPTIONAL : ::parquet::Repetition::REQUIRED,
      ::parquet::LogicalType::None(), ::parquet::Type::DOUBLE, /*primitive_length=*/-1,
      field_id);
}

::parquet::schema::NodePtr MakeFloatNode(const std::string& name, int field_id = -1,
                                         bool optional = true) {
  return ::parquet::schema::PrimitiveNode::Make(
      name, optional ? ::parquet::Repetition::OPTIONAL : ::parquet::Repetition::REQUIRED,
      ::parquet::LogicalType::None(), ::parquet::Type::FLOAT, /*primitive_length=*/-1,
      field_id);
}

::parquet::schema::NodePtr MakeGroupNode(const std::string& name,
                                         const ::parquet::schema::NodeVector& fields,
                                         int field_id = -1, bool optional = true) {
  return ::parquet::schema::GroupNode::Make(
      name, optional ? ::parquet::Repetition::OPTIONAL : ::parquet::Repetition::REQUIRED,
      fields, /*logical_type=*/nullptr, field_id);
}

::parquet::schema::NodePtr MakeListNode(const std::string& name,
                                        const ::parquet::schema::NodePtr& element_node,
                                        int field_id = -1, bool optional = true) {
  auto list_group = ::parquet::schema::GroupNode::Make(
      "element", ::parquet::Repetition::REPEATED, {element_node});
  return ::parquet::schema::GroupNode::Make(
      name, optional ? ::parquet::Repetition::OPTIONAL : ::parquet::Repetition::REQUIRED,
      {list_group}, ::parquet::LogicalType::List(), field_id);
}

::parquet::schema::NodePtr MakeMapNode(const std::string& name,
                                       const ::parquet::schema::NodePtr& key_node,
                                       const ::parquet::schema::NodePtr& value_node,
                                       int field_id = -1, bool optional = true) {
  auto key_value_group = ::parquet::schema::GroupNode::Make(
      "key_value", ::parquet::Repetition::REPEATED, {key_node, value_node});
  return ::parquet::schema::GroupNode::Make(
      name, optional ? ::parquet::Repetition::OPTIONAL : ::parquet::Repetition::REQUIRED,
      {key_value_group}, ::parquet::LogicalType::Map(), field_id);
}

// Helper to create SchemaManifest from Parquet schema
::parquet::arrow::SchemaManifest MakeSchemaManifest(
    const ::parquet::schema::NodePtr& parquet_schema) {
  auto parquet_schema_descriptor = std::make_shared<::parquet::SchemaDescriptor>();
  parquet_schema_descriptor->Init(parquet_schema);

  auto properties = ::parquet::default_arrow_reader_properties();
  properties.set_arrow_extensions_enabled(true);

  ::parquet::arrow::SchemaManifest manifest;
  auto status = ::parquet::arrow::SchemaManifest::Make(parquet_schema_descriptor.get(),
                                                       /*key_value_metadata=*/nullptr,
                                                       properties, &manifest);
  if (!status.ok()) {
    throw std::runtime_error("Failed to create SchemaManifest: " + status.ToString());
  }
  return manifest;
}

#define ASSERT_PROJECTED_FIELD(field_projection, index)                \
  ASSERT_EQ(field_projection.kind, FieldProjection::Kind::kProjected); \
  ASSERT_EQ(std::get<1>(field_projection.from), index);

#define ASSERT_PROJECTED_NULL_FIELD(field_projection) \
  ASSERT_EQ(field_projection.kind, FieldProjection::Kind::kNull);

}  // namespace

TEST(HasFieldIdsTest, PrimitiveNode) {
  EXPECT_FALSE(HasFieldIds(MakeInt32Node("test_field")));
  EXPECT_TRUE(HasFieldIds(MakeInt32Node("test_field", /*field_id=*/1)));
  EXPECT_FALSE(HasFieldIds(MakeInt32Node("test_field", /*field_id=*/-1)));
}

// NOLINTBEGIN(clang-analyzer-cplusplus.NewDeleteLeaks)
TEST(HasFieldIdsTest, GroupNode) {
  EXPECT_FALSE(
      HasFieldIds(MakeGroupNode("group_without_field_id", {
                                                              MakeInt32Node("c1"),
                                                              MakeInt32Node("c2"),
                                                          })));
  EXPECT_TRUE(HasFieldIds(
      MakeGroupNode("group_with_full_field_id", {
                                                    MakeInt32Node("c1", /*field_id=*/1),
                                                    MakeInt32Node("c2", /*field_id=*/2),
                                                })));
  EXPECT_TRUE(HasFieldIds(MakeGroupNode("group_with_partial_field_id",
                                        {
                                            MakeInt32Node("c1", /*field_id=*/1),
                                            MakeInt32Node("c2"),
                                        })));
}
// NOLINTEND(clang-analyzer-cplusplus.NewDeleteLeaks)

TEST(ParquetSchemaProjectionTest, ProjectIdenticalSchemas) {
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", iceberg::int64()),
      SchemaField::MakeOptional(/*field_id=*/2, "name", iceberg::string()),
      SchemaField::MakeOptional(/*field_id=*/3, "age", iceberg::int32()),
      SchemaField::MakeRequired(/*field_id=*/4, "data", iceberg::float64()),
  });

  auto parquet_schema = MakeGroupNode(
      "iceberg_schema",
      {MakeInt64Node("id", /*field_id=*/1), MakeStringNode("name", /*field_id=*/2),
       MakeInt32Node("age", /*field_id=*/3), MakeDoubleNode("data", /*field_id=*/4)});

  auto schema_manifest = MakeSchemaManifest(parquet_schema);
  auto projection_result = Project(expected_schema, schema_manifest);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 4);
  for (size_t i = 0; i < projection.fields.size(); ++i) {
    ASSERT_PROJECTED_FIELD(projection.fields[i], i);
  }

  ASSERT_EQ(SelectedColumnIndices(projection), std::vector<int32_t>({0, 1, 2, 3}));
}

TEST(ParquetSchemaProjectionTest, ProjectSubsetSchema) {
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", iceberg::int64()),
      SchemaField::MakeOptional(/*field_id=*/3, "age", iceberg::int32()),
  });

  auto parquet_schema = MakeGroupNode(
      "iceberg_schema",
      {MakeInt64Node("id", /*field_id=*/1), MakeStringNode("name", /*field_id=*/2),
       MakeInt32Node("age", /*field_id=*/3), MakeDoubleNode("data", /*field_id=*/4)});

  auto schema_manifest = MakeSchemaManifest(parquet_schema);
  auto projection_result = Project(expected_schema, schema_manifest);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 2);
  ASSERT_PROJECTED_FIELD(projection.fields[0], 0);
  ASSERT_PROJECTED_FIELD(projection.fields[1], 1);

  ASSERT_EQ(SelectedColumnIndices(projection), std::vector<int32_t>({0, 2}));
}

TEST(ParquetSchemaProjectionTest, ProjectMissingOptionalField) {
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", iceberg::int64()),
      SchemaField::MakeOptional(/*field_id=*/2, "name", iceberg::string()),
      SchemaField::MakeOptional(/*field_id=*/10, "extra", iceberg::string()),
  });

  auto parquet_schema = MakeGroupNode(
      "iceberg_schema",
      {MakeInt64Node("id", /*field_id=*/1), MakeStringNode("name", /*field_id=*/2)});

  auto schema_manifest = MakeSchemaManifest(parquet_schema);
  auto projection_result = Project(expected_schema, schema_manifest);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 3);
  ASSERT_PROJECTED_FIELD(projection.fields[0], 0);
  ASSERT_PROJECTED_FIELD(projection.fields[1], 1);
  ASSERT_PROJECTED_NULL_FIELD(projection.fields[2]);

  ASSERT_EQ(SelectedColumnIndices(projection), std::vector<int32_t>({0, 1}));
}

TEST(ParquetSchemaProjectionTest, ProjectMissingRequiredField) {
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", iceberg::int64()),
      SchemaField::MakeOptional(/*field_id=*/2, "name", iceberg::string()),
      SchemaField::MakeRequired(/*field_id=*/10, "extra", iceberg::string()),
  });

  auto parquet_schema = MakeGroupNode(
      "iceberg_schema",
      {MakeInt64Node("id", /*field_id=*/1), MakeStringNode("name", /*field_id=*/2)});

  auto schema_manifest = MakeSchemaManifest(parquet_schema);
  auto projection_result = Project(expected_schema, schema_manifest);
  ASSERT_THAT(projection_result, IsError(ErrorKind::kInvalidSchema));
  ASSERT_THAT(projection_result, HasErrorMessage("Missing required field"));
}

TEST(ParquetSchemaProjectionTest, ProjectMetadataColumn) {
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", iceberg::int64()),
      MetadataColumns::kFilePath,
  });

  auto parquet_schema =
      MakeGroupNode("iceberg_schema", {MakeInt64Node("id", /*field_id=*/1)});

  auto schema_manifest = MakeSchemaManifest(parquet_schema);
  auto projection_result = Project(expected_schema, schema_manifest);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 2);
  ASSERT_PROJECTED_FIELD(projection.fields[0], 0);
  ASSERT_EQ(projection.fields[1].kind, FieldProjection::Kind::kMetadata);

  ASSERT_EQ(SelectedColumnIndices(projection), std::vector<int32_t>({0}));
}

TEST(ParquetSchemaProjectionTest, ProjectSchemaEvolutionIntToLong) {
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", iceberg::int64()),
  });

  auto parquet_schema =
      MakeGroupNode("iceberg_schema", {MakeInt32Node("id", /*field_id=*/1)});

  auto schema_manifest = MakeSchemaManifest(parquet_schema);
  auto projection_result = Project(expected_schema, schema_manifest);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 1);
  ASSERT_PROJECTED_FIELD(projection.fields[0], 0);
}

TEST(ParquetSchemaProjectionTest, ProjectSchemaEvolutionFloatToDouble) {
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "value", iceberg::float64()),
  });

  auto parquet_schema =
      MakeGroupNode("iceberg_schema", {MakeFloatNode("value", /*field_id=*/1)});

  auto schema_manifest = MakeSchemaManifest(parquet_schema);
  auto projection_result = Project(expected_schema, schema_manifest);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 1);
  ASSERT_PROJECTED_FIELD(projection.fields[0], 0);
}

TEST(ParquetSchemaProjectionTest, ProjectSchemaEvolutionIncompatibleTypes) {
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "value", iceberg::int32()),
  });

  auto parquet_schema =
      MakeGroupNode("iceberg_schema", {MakeStringNode("value", /*field_id=*/1)});

  auto schema_manifest = MakeSchemaManifest(parquet_schema);
  auto projection_result = Project(expected_schema, schema_manifest);
  ASSERT_THAT(projection_result, IsError(ErrorKind::kInvalidSchema));
  ASSERT_THAT(projection_result, HasErrorMessage("Cannot read Iceberg type"));
}

TEST(ParquetSchemaProjectionTest, ProjectNestedStructures) {
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", iceberg::int64()),
      SchemaField::MakeOptional(
          /*field_id=*/3, "address",
          std::make_shared<StructType>(std::vector<SchemaField>{
              SchemaField::MakeOptional(/*field_id=*/101, "street", iceberg::string()),
              SchemaField::MakeOptional(/*field_id=*/102, "city", iceberg::string()),
          })),
  });

  auto parquet_schema = MakeGroupNode(
      "iceberg_schema",
      {
          MakeInt64Node("id", /*field_id=*/1),
          MakeListNode("address", MakeStringNode("street", /*field_id=*/100),
                       /*field_id=*/2),
          MakeGroupNode("address",
                        {MakeStringNode("street", /*field_id=*/101),
                         MakeStringNode("city", /*field_id=*/102)},
                        /*field_id=*/3),
      });

  auto schema_manifest = MakeSchemaManifest(parquet_schema);
  auto projection_result = Project(expected_schema, schema_manifest);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 2);
  ASSERT_PROJECTED_FIELD(projection.fields[0], 0);
  ASSERT_PROJECTED_FIELD(projection.fields[1], 1);

  ASSERT_EQ(projection.fields[1].children.size(), 2);
  ASSERT_PROJECTED_FIELD(projection.fields[1].children[0], 0);
  ASSERT_PROJECTED_FIELD(projection.fields[1].children[1], 1);

  ASSERT_EQ(SelectedColumnIndices(projection), std::vector<int32_t>({0, 2, 3}));
}

TEST(ParquetSchemaProjectionTest, ProjectListType) {
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", iceberg::int64()),
      SchemaField::MakeOptional(
          /*field_id=*/2, "numbers",
          std::make_shared<ListType>(SchemaField::MakeOptional(
              /*field_id=*/101, "element", iceberg::int32()))),
  });

  auto parquet_schema = MakeGroupNode(
      "iceberg_schema",
      {
          MakeInt64Node("id", /*field_id=*/1),
          MakeListNode("numbers", MakeInt32Node("element", /*field_id=*/101),
                       /*field_id=*/2),
      });

  auto schema_manifest = MakeSchemaManifest(parquet_schema);
  auto projection_result = Project(expected_schema, schema_manifest);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 2);
  ASSERT_PROJECTED_FIELD(projection.fields[0], 0);
  ASSERT_PROJECTED_FIELD(projection.fields[1], 1);

  ASSERT_EQ(projection.fields[1].children.size(), 1);
  ASSERT_PROJECTED_FIELD(projection.fields[1].children[0], 0);

  ASSERT_EQ(SelectedColumnIndices(projection), std::vector<int32_t>({0, 1}));
}

TEST(ParquetSchemaProjectionTest, ProjectMapType) {
  Schema expected_schema({
      SchemaField::MakeOptional(
          /*field_id=*/1, "counts",
          std::make_shared<MapType>(
              SchemaField::MakeRequired(/*field_id=*/101, "key", iceberg::string()),
              SchemaField::MakeOptional(/*field_id=*/102, "value", iceberg::int32()))),
  });

  auto parquet_schema = MakeGroupNode(
      "iceberg_schema",
      {
          MakeMapNode("counts",
                      MakeStringNode("key", /*field_id=*/101, /*optional=*/false),
                      MakeInt32Node("value", /*field_id=*/102), /*field_id=*/1),
      });

  auto schema_manifest = MakeSchemaManifest(parquet_schema);
  auto projection_result = Project(expected_schema, schema_manifest);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 1);
  ASSERT_PROJECTED_FIELD(projection.fields[0], 0);

  ASSERT_EQ(projection.fields[0].children.size(), 2);
  ASSERT_PROJECTED_FIELD(projection.fields[0].children[0], 0);
  ASSERT_PROJECTED_FIELD(projection.fields[0].children[1], 1);

  ASSERT_EQ(SelectedColumnIndices(projection), std::vector<int32_t>({0, 1}));
}

TEST(ParquetSchemaProjectionTest, ProjectListOfStruct) {
  Schema expected_schema({
      SchemaField::MakeOptional(
          /*field_id=*/1, "items",
          std::make_shared<ListType>(SchemaField::MakeOptional(
              /*field_id=*/101, "element",
              std::make_shared<StructType>(std::vector<SchemaField>{
                  SchemaField::MakeRequired(/*field_id=*/104, "z", iceberg::int32()),
                  SchemaField::MakeOptional(/*field_id=*/102, "x", iceberg::int32()),
              })))),
  });

  auto parquet_schema =
      MakeGroupNode("iceberg_schema",
                    {
                        MakeListNode("items",
                                     MakeGroupNode("element",
                                                   {MakeInt32Node("x", /*field_id=*/102),
                                                    MakeInt32Node("y", /*field_id=*/103),
                                                    MakeInt32Node("z", /*field_id=*/104),
                                                    MakeInt32Node("m", /*field_id=*/105)},
                                                   /*field_id=*/101),
                                     /*field_id=*/1),
                    });

  auto schema_manifest = MakeSchemaManifest(parquet_schema);
  auto projection_result = Project(expected_schema, schema_manifest);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 1);
  ASSERT_PROJECTED_FIELD(projection.fields[0], 0);

  // Verify list element struct is properly projected
  ASSERT_EQ(projection.fields[0].children.size(), 1);
  const auto& element_proj = projection.fields[0].children[0];
  ASSERT_EQ(element_proj.children.size(), 2);
  ASSERT_PROJECTED_FIELD(element_proj.children[0], 1);
  ASSERT_PROJECTED_FIELD(element_proj.children[1], 0);

  ASSERT_EQ(SelectedColumnIndices(projection), std::vector<int32_t>({0, 2}));
}

TEST(ParquetSchemaProjectionTest, ProjectDecimalType) {
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "value", iceberg::decimal(18, 2)),
  });

  auto decimal_node = ::parquet::schema::PrimitiveNode::Make(
      "value", ::parquet::Repetition::REQUIRED, ::parquet::LogicalType::Decimal(9, 2),
      ::parquet::Type::FIXED_LEN_BYTE_ARRAY, /*primitive_length=*/4, /*field_id=*/1);
  auto parquet_schema = MakeGroupNode("iceberg_schema", {decimal_node});

  auto schema_manifest = MakeSchemaManifest(parquet_schema);
  auto projection_result = Project(expected_schema, schema_manifest);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 1);
  ASSERT_PROJECTED_FIELD(projection.fields[0], 0);
}

TEST(ParquetSchemaProjectionTest, ProjectDecimalIncompatible) {
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "value", iceberg::decimal(18, 3)),
  });

  auto decimal_node = ::parquet::schema::PrimitiveNode::Make(
      "value", ::parquet::Repetition::REQUIRED, ::parquet::LogicalType::Decimal(9, 2),
      ::parquet::Type::FIXED_LEN_BYTE_ARRAY, /*primitive_length=*/4, /*field_id=*/1);
  auto parquet_schema = MakeGroupNode("iceberg_schema", {decimal_node});

  auto schema_manifest = MakeSchemaManifest(parquet_schema);
  auto projection_result = Project(expected_schema, schema_manifest);
  ASSERT_THAT(projection_result, IsError(ErrorKind::kInvalidSchema));
  ASSERT_THAT(projection_result, HasErrorMessage("Cannot read"));
}

TEST(ParquetSchemaProjectionTest, ProjectDuplicateFieldIds) {
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", iceberg::int64()),
      SchemaField::MakeOptional(/*field_id=*/2, "name", iceberg::string()),
  });

  auto parquet_schema = MakeGroupNode(
      "iceberg_schema", {
                            MakeInt64Node("id", /*field_id=*/1),
                            MakeStringNode("name", /*field_id=*/1)  // Duplicate field ID
                        });

  auto schema_manifest = MakeSchemaManifest(parquet_schema);
  auto projection_result = Project(expected_schema, schema_manifest);
  ASSERT_THAT(projection_result, IsError(ErrorKind::kInvalidSchema));
  ASSERT_THAT(projection_result, HasErrorMessage("Duplicate field id"));
}

TEST(ParquetSchemaProjectionTest, ProjectPrimitiveType) {
  struct TestCase {
    std::shared_ptr<Type> iceberg_type;
    ::parquet::Type::type parquet_type;
    std::shared_ptr<const ::parquet::LogicalType> parquet_logical_type;
    int32_t primitive_length = -1;
  };

  std::vector<TestCase> test_cases = {
      TestCase{.iceberg_type = float64(), .parquet_type = ::parquet::Type::DOUBLE},
      TestCase{.iceberg_type = float32(), .parquet_type = ::parquet::Type::FLOAT},
      TestCase{.iceberg_type = int64(), .parquet_type = ::parquet::Type::INT64},
      TestCase{.iceberg_type = int32(), .parquet_type = ::parquet::Type::INT32},
      TestCase{.iceberg_type = string(),
               .parquet_type = ::parquet::Type::BYTE_ARRAY,
               .parquet_logical_type = ::parquet::LogicalType::String()},
      TestCase{.iceberg_type = binary(), .parquet_type = ::parquet::Type::BYTE_ARRAY},
      TestCase{.iceberg_type = boolean(), .parquet_type = ::parquet::Type::BOOLEAN},
      TestCase{.iceberg_type = date(),
               .parquet_type = ::parquet::Type::INT32,
               .parquet_logical_type = ::parquet::LogicalType::Date()},
      TestCase{
          .iceberg_type = time(),
          .parquet_type = ::parquet::Type::INT64,
          .parquet_logical_type = ::parquet::LogicalType::Time(
              /*is_adjusted_to_utc=*/true, ::parquet::LogicalType::TimeUnit::MICROS)},
      TestCase{
          .iceberg_type = timestamp(),
          .parquet_type = ::parquet::Type::INT64,
          .parquet_logical_type = ::parquet::LogicalType::Timestamp(
              /*is_adjusted_to_utc=*/false, ::parquet::LogicalType::TimeUnit::MICROS)},
      TestCase{
          .iceberg_type = timestamp_tz(),
          .parquet_type = ::parquet::Type::INT64,
          .parquet_logical_type = ::parquet::LogicalType::Timestamp(
              /*is_adjusted_to_utc=*/true, ::parquet::LogicalType::TimeUnit::MICROS)},
      TestCase{.iceberg_type = decimal(4, 2),
               .parquet_type = ::parquet::Type::INT32,
               .parquet_logical_type = ::parquet::LogicalType::Decimal(4, 2)},
      TestCase{.iceberg_type = decimal(38, 18),
               .parquet_type = ::parquet::Type::FIXED_LEN_BYTE_ARRAY,
               .parquet_logical_type = ::parquet::LogicalType::Decimal(38, 18),
               .primitive_length = 16},
      TestCase{.iceberg_type = uuid(),
               .parquet_type = ::parquet::Type::FIXED_LEN_BYTE_ARRAY,
               .parquet_logical_type = ::parquet::LogicalType::UUID(),
               .primitive_length = 16},
      TestCase{.iceberg_type = fixed(8),
               .parquet_type = ::parquet::Type::FIXED_LEN_BYTE_ARRAY,
               .primitive_length = 8}};

  for (const auto& test_case : test_cases) {
    Schema expected_schema({SchemaField::MakeRequired(/*field_id=*/1, "test_field",
                                                      test_case.iceberg_type)});
    auto parquet_schema = MakeGroupNode(
        "iceberg_schema",
        {::parquet::schema::PrimitiveNode::Make(
            "test_field", ::parquet::Repetition::REQUIRED, test_case.parquet_logical_type,
            test_case.parquet_type, test_case.primitive_length,
            /*field_id=*/1)});

    auto schema_manifest = MakeSchemaManifest(parquet_schema);
    auto projection_result = Project(expected_schema, schema_manifest);
    ASSERT_THAT(projection_result, IsOk());

    const auto& projection = *projection_result;
    ASSERT_EQ(projection.fields.size(), 1);
    ASSERT_PROJECTED_FIELD(projection.fields[0], 0);
  }
}

TEST(ParquetSchemaProjectionTest, UnsuportedProjection) {
  struct TestCase {
    std::shared_ptr<Type> iceberg_type;
    ::parquet::Type::type parquet_type;
    std::shared_ptr<const ::parquet::LogicalType> parquet_logical_type;
    int32_t primitive_length = -1;
  };

  std::vector<TestCase> test_cases = {
      TestCase{.iceberg_type = float32(), .parquet_type = ::parquet::Type::DOUBLE},
      TestCase{.iceberg_type = int32(), .parquet_type = ::parquet::Type::INT64},
      TestCase{.iceberg_type = date(), .parquet_type = ::parquet::Type::INT32},
      TestCase{.iceberg_type = time(),
               .parquet_type = ::parquet::Type::INT64,
               .parquet_logical_type = ::parquet::LogicalType::Time(
                   /*is_adjusted_to_utc=*/true, ::parquet::LogicalType::TimeUnit::NANOS)},
      TestCase{
          .iceberg_type = timestamp(),
          .parquet_type = ::parquet::Type::INT64,
          .parquet_logical_type = ::parquet::LogicalType::Timestamp(
              /*is_adjusted_to_utc=*/false, ::parquet::LogicalType::TimeUnit::NANOS)},
      TestCase{.iceberg_type = timestamp_tz(),
               .parquet_type = ::parquet::Type::INT64,
               .parquet_logical_type = ::parquet::LogicalType::Timestamp(
                   /*is_adjusted_to_utc=*/true, ::parquet::LogicalType::TimeUnit::NANOS)},
      TestCase{.iceberg_type = decimal(4, 2),
               .parquet_type = ::parquet::Type::INT32,
               .parquet_logical_type = ::parquet::LogicalType::Decimal(4, 1)},
      TestCase{.iceberg_type = fixed(8),
               .parquet_type = ::parquet::Type::FIXED_LEN_BYTE_ARRAY,
               .primitive_length = 4}};

  for (const auto& test_case : test_cases) {
    Schema expected_schema({SchemaField::MakeRequired(/*field_id=*/1, "test_field",
                                                      test_case.iceberg_type)});
    auto parquet_schema = MakeGroupNode(
        "iceberg_schema",
        {::parquet::schema::PrimitiveNode::Make(
            "test_field", ::parquet::Repetition::REQUIRED, test_case.parquet_logical_type,
            test_case.parquet_type, test_case.primitive_length,
            /*field_id=*/1)});

    auto schema_manifest = MakeSchemaManifest(parquet_schema);
    auto projection_result = Project(expected_schema, schema_manifest);
    ASSERT_THAT(projection_result, HasErrorMessage("Cannot read"));
  }
}

}  // namespace iceberg::parquet
