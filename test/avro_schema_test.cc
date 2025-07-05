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

#include <string>

#include <avro/Compiler.hh>
#include <avro/NodeImpl.hh>
#include <avro/Types.hh>
#include <gtest/gtest.h>

#include "iceberg/avro/avro_schema_util_internal.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/schema.h"
#include "matchers.h"

namespace iceberg::avro {

namespace {

void CheckCustomLogicalType(const ::avro::NodePtr& node, const std::string& type_name) {
  EXPECT_EQ(node->logicalType().type(), ::avro::LogicalType::CUSTOM);
  ASSERT_TRUE(node->logicalType().customLogicalType() != nullptr);
  EXPECT_EQ(node->logicalType().customLogicalType()->name(), type_name);
}

void CheckFieldIdAt(const ::avro::NodePtr& node, size_t index, int32_t field_id,
                    const std::string& key = "field-id") {
  ASSERT_LT(index, node->customAttributes());
  const auto& attrs = node->customAttributesAt(index);
  ASSERT_EQ(attrs.getAttribute(key), std::make_optional(std::to_string(field_id)));
}

}  // namespace

TEST(ToAvroNodeVisitorTest, BooleanType) {
  ::avro::NodePtr node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(BooleanType{}, &node), IsOk());
  EXPECT_EQ(node->type(), ::avro::AVRO_BOOL);
}

TEST(ToAvroNodeVisitorTest, IntType) {
  ::avro::NodePtr node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(IntType{}, &node), IsOk());
  EXPECT_EQ(node->type(), ::avro::AVRO_INT);
}

TEST(ToAvroNodeVisitorTest, LongType) {
  ::avro::NodePtr node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(LongType{}, &node), IsOk());
  EXPECT_EQ(node->type(), ::avro::AVRO_LONG);
}

TEST(ToAvroNodeVisitorTest, FloatType) {
  ::avro::NodePtr node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(FloatType{}, &node), IsOk());
  EXPECT_EQ(node->type(), ::avro::AVRO_FLOAT);
}

TEST(ToAvroNodeVisitorTest, DoubleType) {
  ::avro::NodePtr node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(DoubleType{}, &node), IsOk());
  EXPECT_EQ(node->type(), ::avro::AVRO_DOUBLE);
}

TEST(ToAvroNodeVisitorTest, DecimalType) {
  ::avro::NodePtr node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(DecimalType{10, 2}, &node), IsOk());
  EXPECT_EQ(node->type(), ::avro::AVRO_FIXED);
  EXPECT_EQ(node->logicalType().type(), ::avro::LogicalType::DECIMAL);

  EXPECT_EQ(node->logicalType().precision(), 10);
  EXPECT_EQ(node->logicalType().scale(), 2);
  EXPECT_EQ(node->name().simpleName(), "decimal_10_2");
}

TEST(ToAvroNodeVisitorTest, DateType) {
  ::avro::NodePtr node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(DateType{}, &node), IsOk());
  EXPECT_EQ(node->type(), ::avro::AVRO_INT);
  EXPECT_EQ(node->logicalType().type(), ::avro::LogicalType::DATE);
}

TEST(ToAvroNodeVisitorTest, TimeType) {
  ::avro::NodePtr node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(TimeType{}, &node), IsOk());
  EXPECT_EQ(node->type(), ::avro::AVRO_LONG);
  EXPECT_EQ(node->logicalType().type(), ::avro::LogicalType::TIME_MICROS);
}

TEST(ToAvroNodeVisitorTest, TimestampType) {
  ::avro::NodePtr node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(TimestampType{}, &node), IsOk());
  EXPECT_EQ(node->type(), ::avro::AVRO_LONG);
  EXPECT_EQ(node->logicalType().type(), ::avro::LogicalType::TIMESTAMP_MICROS);

  ASSERT_EQ(node->customAttributes(), 1);
  EXPECT_EQ(node->customAttributesAt(0).getAttribute("adjust-to-utc"), "false");
}

TEST(ToAvroNodeVisitorTest, TimestampTzType) {
  ::avro::NodePtr node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(TimestampTzType{}, &node), IsOk());
  EXPECT_EQ(node->type(), ::avro::AVRO_LONG);
  EXPECT_EQ(node->logicalType().type(), ::avro::LogicalType::TIMESTAMP_MICROS);

  ASSERT_EQ(node->customAttributes(), 1);
  EXPECT_EQ(node->customAttributesAt(0).getAttribute("adjust-to-utc"), "true");
}

TEST(ToAvroNodeVisitorTest, StringType) {
  ::avro::NodePtr node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(StringType{}, &node), IsOk());
  EXPECT_EQ(node->type(), ::avro::AVRO_STRING);
}

TEST(ToAvroNodeVisitorTest, UuidType) {
  ::avro::NodePtr node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(UuidType{}, &node), IsOk());
  EXPECT_EQ(node->type(), ::avro::AVRO_FIXED);
  EXPECT_EQ(node->logicalType().type(), ::avro::LogicalType::UUID);

  EXPECT_EQ(node->fixedSize(), 16);
  EXPECT_EQ(node->name().fullname(), "uuid_fixed");
}

TEST(ToAvroNodeVisitorTest, FixedType) {
  ::avro::NodePtr node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(FixedType{20}, &node), IsOk());

  EXPECT_EQ(node->type(), ::avro::AVRO_FIXED);
  EXPECT_EQ(node->fixedSize(), 20);
  EXPECT_EQ(node->name().fullname(), "fixed_20");
}

TEST(ToAvroNodeVisitorTest, BinaryType) {
  ::avro::NodePtr node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(BinaryType{}, &node), IsOk());
  EXPECT_EQ(node->type(), ::avro::AVRO_BYTES);
}

TEST(ToAvroNodeVisitorTest, StructType) {
  StructType struct_type{{SchemaField{/*field_id=*/1, "bool_field", iceberg::boolean(),
                                      /*optional=*/false},
                          SchemaField{/*field_id=*/2, "int_field", iceberg::int32(),
                                      /*optional=*/true}}};

  ::avro::NodePtr node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(struct_type, &node), IsOk());
  EXPECT_EQ(node->type(), ::avro::AVRO_RECORD);

  ASSERT_EQ(node->names(), 2);
  EXPECT_EQ(node->nameAt(0), "bool_field");
  EXPECT_EQ(node->nameAt(1), "int_field");

  ASSERT_EQ(node->customAttributes(), 2);
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(node, /*index=*/0, /*field_id=*/1));
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(node, /*index=*/1, /*field_id=*/2));

  ASSERT_EQ(node->leaves(), 2);
  ASSERT_EQ(node->leafAt(0)->type(), ::avro::AVRO_BOOL);
  ASSERT_EQ(node->leafAt(1)->type(), ::avro::AVRO_UNION);
  ASSERT_EQ(node->leafAt(1)->leaves(), 2);
  EXPECT_EQ(node->leafAt(1)->leafAt(0)->type(), ::avro::AVRO_NULL);
  EXPECT_EQ(node->leafAt(1)->leafAt(1)->type(), ::avro::AVRO_INT);
}

TEST(ToAvroNodeVisitorTest, ListType) {
  ListType list_type{SchemaField{/*field_id=*/5, "element", iceberg::string(),
                                 /*optional=*/true}};

  ::avro::NodePtr node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(list_type, &node), IsOk());
  EXPECT_EQ(node->type(), ::avro::AVRO_ARRAY);

  ASSERT_EQ(node->customAttributes(), 1);
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(node, /*index=*/0, /*field_id=*/5,
                                         /*key=*/"element-id"));

  ASSERT_EQ(node->leaves(), 1);
  EXPECT_EQ(node->leafAt(0)->type(), ::avro::AVRO_UNION);
  ASSERT_EQ(node->leafAt(0)->leaves(), 2);
  EXPECT_EQ(node->leafAt(0)->leafAt(0)->type(), ::avro::AVRO_NULL);
  EXPECT_EQ(node->leafAt(0)->leafAt(1)->type(), ::avro::AVRO_STRING);
}

TEST(ToAvroNodeVisitorTest, MapTypeWithStringKey) {
  MapType map_type{SchemaField{/*field_id=*/10, "key", iceberg::string(),
                               /*optional=*/false},
                   SchemaField{/*field_id=*/11, "value", iceberg::int32(),
                               /*optional=*/false}};

  ::avro::NodePtr node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(map_type, &node), IsOk());
  EXPECT_EQ(node->type(), ::avro::AVRO_MAP);

  ASSERT_GT(node->customAttributes(), 0);
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(node, /*index=*/0, /*field_id=*/10,
                                         /*key=*/"key-id"));
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(node, /*index=*/0, /*field_id=*/11,
                                         /*key=*/"value-id"));

  ASSERT_EQ(node->leaves(), 2);
  EXPECT_EQ(node->leafAt(0)->type(), ::avro::AVRO_STRING);
  EXPECT_EQ(node->leafAt(1)->type(), ::avro::AVRO_INT);
}

TEST(ToAvroNodeVisitorTest, MapTypeWithNonStringKey) {
  MapType map_type{SchemaField{/*field_id=*/10, "key", iceberg::int32(),
                               /*optional=*/false},
                   SchemaField{/*field_id=*/11, "value", iceberg::string(),
                               /*optional=*/false}};

  ::avro::NodePtr node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(map_type, &node), IsOk());
  EXPECT_EQ(node->type(), ::avro::AVRO_ARRAY);
  CheckCustomLogicalType(node, "map");

  ASSERT_EQ(node->leaves(), 1);
  auto record_node = node->leafAt(0);
  ASSERT_EQ(record_node->type(), ::avro::AVRO_RECORD);
  EXPECT_EQ(record_node->name().fullname(), "k10_v11");

  ASSERT_EQ(record_node->customAttributes(), 2);
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(record_node, /*index=*/0, /*field_id=*/10));
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(record_node, /*index=*/1, /*field_id=*/11));

  ASSERT_EQ(record_node->names(), 2);
  EXPECT_EQ(record_node->nameAt(0), "key");
  EXPECT_EQ(record_node->nameAt(1), "value");

  ASSERT_EQ(record_node->leaves(), 2);
  EXPECT_EQ(record_node->leafAt(0)->type(), ::avro::AVRO_INT);
  EXPECT_EQ(record_node->leafAt(1)->type(), ::avro::AVRO_STRING);
}

TEST(ToAvroNodeVisitorTest, InvalidMapKeyType) {
  MapType map_type{SchemaField{/*field_id=*/1, "key", iceberg::string(),
                               /*optional=*/true},
                   SchemaField{/*field_id=*/2, "value", iceberg::string(),
                               /*optional=*/false}};

  ::avro::NodePtr node;
  auto status = ToAvroNodeVisitor{}.Visit(map_type, &node);
  EXPECT_THAT(status, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(status, HasErrorMessage("Map key `key` must be required"));
}

TEST(ToAvroNodeVisitorTest, NestedTypes) {
  auto inner_struct = std::make_shared<StructType>(std::vector<SchemaField>{
      SchemaField{/*field_id=*/2, "string_field", iceberg::string(),
                  /*optional=*/false},
      SchemaField{/*field_id=*/3, "int_field", iceberg::int32(),
                  /*optional=*/true}});
  auto inner_list = std::make_shared<ListType>(SchemaField{/*field_id=*/5, "element",
                                                           iceberg::float64(),
                                                           /*optional=*/false});
  StructType root_struct{{SchemaField{/*field_id=*/1, "struct_field", inner_struct,
                                      /*optional=*/false},
                          SchemaField{/*field_id=*/4, "list_field", inner_list,
                                      /*optional=*/true}}};

  ::avro::NodePtr root_node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(root_struct, &root_node), IsOk());
  EXPECT_EQ(root_node->type(), ::avro::AVRO_RECORD);

  ASSERT_EQ(root_node->names(), 2);
  EXPECT_EQ(root_node->nameAt(0), "struct_field");
  EXPECT_EQ(root_node->nameAt(1), "list_field");

  ASSERT_EQ(root_node->customAttributes(), 2);
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(root_node, /*index=*/0, /*field_id=*/1));
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(root_node, /*index=*/1, /*field_id=*/4));

  // Check struct field
  auto struct_node = root_node->leafAt(0);
  ASSERT_EQ(struct_node->type(), ::avro::AVRO_RECORD);
  ASSERT_EQ(struct_node->names(), 2);
  EXPECT_EQ(struct_node->nameAt(0), "string_field");
  EXPECT_EQ(struct_node->nameAt(1), "int_field");

  ASSERT_EQ(struct_node->customAttributes(), 2);
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(struct_node, /*index=*/0, /*field_id=*/2));
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(struct_node, /*index=*/1, /*field_id=*/3));

  ASSERT_EQ(struct_node->leaves(), 2);
  EXPECT_EQ(struct_node->leafAt(0)->type(), ::avro::AVRO_STRING);
  EXPECT_EQ(struct_node->leafAt(1)->type(), ::avro::AVRO_UNION);
  ASSERT_EQ(struct_node->leafAt(1)->leaves(), 2);
  EXPECT_EQ(struct_node->leafAt(1)->leafAt(0)->type(), ::avro::AVRO_NULL);
  EXPECT_EQ(struct_node->leafAt(1)->leafAt(1)->type(), ::avro::AVRO_INT);

  // Check list field
  auto list_union_node = root_node->leafAt(1);
  ASSERT_EQ(list_union_node->type(), ::avro::AVRO_UNION);
  ASSERT_EQ(list_union_node->leaves(), 2);
  EXPECT_EQ(list_union_node->leafAt(0)->type(), ::avro::AVRO_NULL);
  EXPECT_EQ(list_union_node->leafAt(1)->type(), ::avro::AVRO_ARRAY);

  auto list_node = list_union_node->leafAt(1);
  ASSERT_EQ(list_node->type(), ::avro::AVRO_ARRAY);

  ASSERT_EQ(list_node->customAttributes(), 1);
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(list_node, /*index=*/0, /*field_id=*/5,
                                         /*key=*/"element-id"));

  ASSERT_EQ(list_node->leaves(), 1);
  EXPECT_EQ(list_node->leafAt(0)->type(), ::avro::AVRO_DOUBLE);
}

TEST(HasIdVisitorTest, HasNoIds) {
  HasIdVisitor visitor;
  EXPECT_THAT(visitor.Visit(::avro::compileJsonSchemaFromString("\"string\"")), IsOk());
  EXPECT_TRUE(visitor.HasNoIds());
  EXPECT_FALSE(visitor.AllHaveIds());
}

TEST(HasIdVisitorTest, RecordWithFieldIds) {
  const std::string schema_json = R"({
    "type": "record",
    "name": "test_record",
    "fields": [
      {"name": "int_field", "type": "int", "field-id": 1},
      {"name": "string_field", "type": "string", "field-id": 2}
    ]
  })";

  HasIdVisitor visitor;
  EXPECT_THAT(visitor.Visit(::avro::compileJsonSchemaFromString(schema_json)), IsOk());
  EXPECT_FALSE(visitor.HasNoIds());
  EXPECT_TRUE(visitor.AllHaveIds());
}

TEST(HasIdVisitorTest, RecordWithMissingFieldIds) {
  const std::string schema_json = R"({
    "type": "record",
    "name": "test_record",
    "fields": [
      {"name": "int_field", "type": "int", "field-id": 1},
      {"name": "string_field", "type": "string"}
    ]
  })";

  HasIdVisitor visitor;
  EXPECT_THAT(visitor.Visit(::avro::compileJsonSchemaFromString(schema_json)), IsOk());
  EXPECT_FALSE(visitor.HasNoIds());
  EXPECT_FALSE(visitor.AllHaveIds());
}

TEST(HasIdVisitorTest, ArrayWithElementId) {
  const std::string schema_json = R"({
    "type": "array",
    "items": "int",
    "element-id": 5
  })";

  HasIdVisitor visitor;
  EXPECT_THAT(visitor.Visit(::avro::compileJsonSchemaFromString(schema_json)), IsOk());
  EXPECT_FALSE(visitor.HasNoIds());
  EXPECT_TRUE(visitor.AllHaveIds());
}

TEST(HasIdVisitorTest, ArrayWithoutElementId) {
  const std::string schema_json = R"({
    "type": "array",
    "items": "int"
  })";

  HasIdVisitor visitor;
  EXPECT_THAT(visitor.Visit(::avro::compileJsonSchemaFromString(schema_json)), IsOk());
  EXPECT_FALSE(visitor.HasNoIds());
  EXPECT_FALSE(visitor.AllHaveIds());
}

TEST(HasIdVisitorTest, MapWithIds) {
  const std::string schema_json = R"({
    "type": "map",
    "values": "int",
    "key-id": 10,
    "value-id": 11
  })";

  HasIdVisitor visitor;
  EXPECT_THAT(visitor.Visit(::avro::compileJsonSchemaFromString(schema_json)), IsOk());
  EXPECT_FALSE(visitor.HasNoIds());
  EXPECT_TRUE(visitor.AllHaveIds());
}

TEST(HasIdVisitorTest, MapWithPartialIds) {
  const std::string schema_json = R"({
    "type": "map",
    "values": "int",
    "key-id": 10
  })";

  HasIdVisitor visitor;
  EXPECT_THAT(visitor.Visit(::avro::compileJsonSchemaFromString(schema_json)), IsOk());
  EXPECT_FALSE(visitor.HasNoIds());
  EXPECT_FALSE(visitor.AllHaveIds());
}

TEST(HasIdVisitorTest, UnionType) {
  const std::string schema_json = R"([
    "null",
    {
      "type": "record",
      "name": "record_in_union",
      "fields": [
        {"name": "int_field", "type": "int", "field-id": 1}
      ]
    }
  ])";

  HasIdVisitor visitor;
  EXPECT_THAT(visitor.Visit(::avro::compileJsonSchemaFromString(schema_json)), IsOk());
  EXPECT_FALSE(visitor.HasNoIds());
  EXPECT_TRUE(visitor.AllHaveIds());
}

TEST(HasIdVisitorTest, ComplexNestedSchema) {
  const std::string schema_json = R"({
    "type": "record",
    "name": "root",
    "fields": [
      {
        "name": "string_field",
        "type": "string",
        "field-id": 1
      },
      {
        "name": "record_field",
        "type": {
          "type": "record",
          "name": "nested",
          "fields": [
            {
              "name": "int_field",
              "type": "int",
              "field-id": 3
            }
          ]
        },
        "field-id": 2
      },
      {
        "name": "array_field",
        "type": {
          "type": "array",
          "items": "double",
          "element-id": 5
        },
        "field-id": 4
      }
    ]
  })";

  HasIdVisitor visitor;
  EXPECT_THAT(visitor.Visit(::avro::compileJsonSchemaFromString(schema_json)), IsOk());
  EXPECT_FALSE(visitor.HasNoIds());
  EXPECT_TRUE(visitor.AllHaveIds());
}

TEST(HasIdVisitorTest, ArrayBackedMapWithIds) {
  const std::string schema_json = R"({
    "type": "array",
    "items": {
      "type": "record",
      "name": "key_value",
      "fields": [
        {"name": "key", "type": "int", "field-id": 10},
        {"name": "value", "type": "string", "field-id": 11}
      ]
    },
    "logicalType": "map"
  })";

  HasIdVisitor visitor;
  EXPECT_THAT(visitor.Visit(::avro::compileJsonSchemaFromString(schema_json)), IsOk());
  EXPECT_FALSE(visitor.HasNoIds());
  EXPECT_TRUE(visitor.AllHaveIds());
}

TEST(HasIdVisitorTest, ArrayBackedMapWithPartialIds) {
  const std::string schema_json = R"({
    "type": "array",
    "items": {
      "type": "record",
      "name": "key_value",
      "fields": [
        {"name": "key", "type": "int", "field-id": 10},
        {"name": "value", "type": "string"}
      ]
    },
    "logicalType": "map"
  })";

  HasIdVisitor visitor;
  EXPECT_THAT(visitor.Visit(::avro::compileJsonSchemaFromString(schema_json)), IsOk());
  EXPECT_FALSE(visitor.HasNoIds());
  EXPECT_FALSE(visitor.AllHaveIds());
}

TEST(AvroSchemaProjectionTest, ProjectIdenticalSchemas) {
  // Create an iceberg schema
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", iceberg::int64()),
      SchemaField::MakeOptional(/*field_id=*/2, "name", iceberg::string()),
      SchemaField::MakeOptional(/*field_id=*/3, "age", iceberg::int32()),
      SchemaField::MakeRequired(/*field_id=*/4, "data", iceberg::float64()),
  });

  // Create equivalent avro schema
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "iceberg_schema",
    "fields": [
      {"name": "id", "type": "long", "field-id": 1},
      {"name": "name", "type": ["null", "string"], "field-id": 2},
      {"name": "age", "type": ["null", "int"], "field-id": 3},
      {"name": "data", "type": "double", "field-id": 4}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  auto projection_result =
      Project(expected_schema, avro_schema.root(), /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 4);
  for (size_t i = 0; i < projection.fields.size(); ++i) {
    ASSERT_EQ(projection.fields[i].kind, FieldProjection::Kind::kProjected);
    ASSERT_EQ(std::get<1>(projection.fields[i].from), i);
  }
}

TEST(AvroSchemaProjectionTest, ProjectSubsetSchema) {
  // Create a subset iceberg schema
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", iceberg::int64()),
      SchemaField::MakeOptional(/*field_id=*/3, "age", iceberg::int32()),
  });

  // Create full avro schema
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "iceberg_schema",
    "fields": [
      {"name": "id", "type": "long", "field-id": 1},
      {"name": "name", "type": ["null", "string"], "field-id": 2},
      {"name": "age", "type": ["null", "int"], "field-id": 3},
      {"name": "data", "type": "double", "field-id": 4}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  auto projection_result =
      Project(expected_schema, avro_schema.root(), /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 2);
  ASSERT_EQ(projection.fields[0].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.fields[0].from), 0);
  ASSERT_EQ(projection.fields[1].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.fields[1].from), 2);
}

TEST(AvroSchemaProjectionTest, ProjectWithPruning) {
  // Create a subset iceberg schema
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", iceberg::int64()),
      SchemaField::MakeOptional(/*field_id=*/3, "age", iceberg::int32()),
  });

  // Create full avro schema
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "iceberg_schema",
    "fields": [
      {"name": "id", "type": "long", "field-id": 1},
      {"name": "name", "type": ["null", "string"], "field-id": 2},
      {"name": "age", "type": ["null", "int"], "field-id": 3},
      {"name": "data", "type": "double", "field-id": 4}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  auto projection_result =
      Project(expected_schema, avro_schema.root(), /*prune_source=*/true);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 2);
  ASSERT_EQ(projection.fields[0].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.fields[0].from), 0);
  ASSERT_EQ(projection.fields[1].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.fields[1].from), 1);
}

TEST(AvroSchemaProjectionTest, ProjectMissingOptionalField) {
  // Create iceberg schema with an extra optional field
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", iceberg::int64()),
      SchemaField::MakeOptional(/*field_id=*/2, "name", iceberg::string()),
      SchemaField::MakeOptional(/*field_id=*/10, "extra", iceberg::string()),
  });

  // Create avro schema without the extra field
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "iceberg_schema",
    "fields": [
      {"name": "id", "type": "long", "field-id": 1},
      {"name": "name", "type": ["null", "string"], "field-id": 2}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  auto projection_result =
      Project(expected_schema, avro_schema.root(), /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 3);
  ASSERT_EQ(projection.fields[0].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.fields[0].from), 0);
  ASSERT_EQ(projection.fields[1].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.fields[1].from), 1);
  ASSERT_EQ(projection.fields[2].kind, FieldProjection::Kind::kNull);
}

TEST(AvroSchemaProjectionTest, ProjectMissingRequiredField) {
  // Create iceberg schema with a required field that's missing from the avro schema
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", iceberg::int64()),
      SchemaField::MakeOptional(/*field_id=*/2, "name", iceberg::string()),
      SchemaField::MakeRequired(/*field_id=*/10, "extra", iceberg::string()),
  });

  std::string avro_schema_json = R"({
    "type": "record",
    "name": "iceberg_schema",
    "fields": [
      {"name": "id", "type": "long", "field-id": 1},
      {"name": "name", "type": ["null", "string"], "field-id": 2}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  auto projection_result =
      Project(expected_schema, avro_schema.root(), /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsError(ErrorKind::kInvalidSchema));
  ASSERT_THAT(projection_result, HasErrorMessage("Missing required field"));
}

TEST(AvroSchemaProjectionTest, ProjectMetadataColumn) {
  // Create iceberg schema with a metadata column
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", iceberg::int64()),
      MetadataColumns::kFilePath,
  });

  std::string avro_schema_json = R"({
    "type": "record",
    "name": "iceberg_schema",
    "fields": [
      {"name": "id", "type": "long", "field-id": 1}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  auto projection_result =
      Project(expected_schema, avro_schema.root(), /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 2);
  ASSERT_EQ(projection.fields[0].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.fields[0].from), 0);
  ASSERT_EQ(projection.fields[1].kind, FieldProjection::Kind::kMetadata);
}

TEST(AvroSchemaProjectionTest, ProjectSchemaEvolutionIntToLong) {
  // Create iceberg schema expecting a long
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", iceberg::int64()),
  });

  // Create avro schema with an int
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "iceberg_schema",
    "fields": [
      {"name": "id", "type": "int", "field-id": 1}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  auto projection_result =
      Project(expected_schema, avro_schema.root(), /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 1);
  ASSERT_EQ(projection.fields[0].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.fields[0].from), 0);
}

TEST(AvroSchemaProjectionTest, ProjectSchemaEvolutionFloatToDouble) {
  // Create iceberg schema expecting a double
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "value", iceberg::float64()),
  });

  // Create avro schema with a float
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "iceberg_schema",
    "fields": [
      {"name": "value", "type": "float", "field-id": 1}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  auto projection_result =
      Project(expected_schema, avro_schema.root(), /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 1);
  ASSERT_EQ(projection.fields[0].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.fields[0].from), 0);
}

TEST(AvroSchemaProjectionTest, ProjectSchemaEvolutionIncompatibleTypes) {
  // Create iceberg schema expecting an int
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "value", iceberg::int32()),
  });

  // Create avro schema with a string
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "iceberg_schema",
    "fields": [
      {"name": "value", "type": "string", "field-id": 1}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  auto projection_result =
      Project(expected_schema, avro_schema.root(), /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsError(ErrorKind::kInvalidSchema));
  ASSERT_THAT(projection_result, HasErrorMessage("Cannot read"));
}

TEST(AvroSchemaProjectionTest, ProjectNestedStructures) {
  // Create iceberg schema with nested struct
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", iceberg::int64()),
      SchemaField::MakeOptional(
          /*field_id=*/3, "address",
          std::make_shared<StructType>(std::vector<SchemaField>{
              SchemaField::MakeOptional(/*field_id=*/101, "street", iceberg::string()),
              SchemaField::MakeOptional(/*field_id=*/102, "city", iceberg::string()),
          })),
  });

  // Create equivalent avro schema
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "iceberg_schema",
    "fields": [
      {"name": "id", "type": "long", "field-id": 1},
      {"name": "address", "type": ["null", {
        "type": "record",
        "name": "address_record",
        "fields": [
          {"name": "street", "type": ["null", "string"], "field-id": 101},
          {"name": "city", "type": ["null", "string"], "field-id": 102}
        ]
      }], "field-id": 3}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  auto projection_result =
      Project(expected_schema, avro_schema.root(), /*prune_source=*/true);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 2);
  ASSERT_EQ(projection.fields[0].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.fields[0].from), 0);
  ASSERT_EQ(projection.fields[1].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.fields[1].from), 1);

  // Verify struct field has children correctly mapped
  ASSERT_EQ(projection.fields[1].children.size(), 2);
  ASSERT_EQ(projection.fields[1].children[0].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.fields[1].children[0].from), 0);
  ASSERT_EQ(projection.fields[1].children[1].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.fields[1].children[1].from), 1);
}

TEST(AvroSchemaProjectionTest, ProjectListType) {
  // Create iceberg schema with a list
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "id", iceberg::int64()),
      SchemaField::MakeOptional(
          /*field_id=*/2, "numbers",
          std::make_shared<ListType>(SchemaField::MakeOptional(
              /*field_id=*/101, "element", iceberg::int32()))),
  });

  // Create equivalent avro schema
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "iceberg_schema",
    "fields": [
      {"name": "id", "type": "long", "field-id": 1},
      {"name": "numbers", "type": ["null", {
        "type": "array",
        "items": ["null", "int"],
        "element-id": 101
      }], "field-id": 2}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  auto projection_result =
      Project(expected_schema, avro_schema.root(), /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 2);
  ASSERT_EQ(projection.fields[0].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.fields[0].from), 0);
  ASSERT_EQ(projection.fields[1].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.fields[1].from), 1);
}

TEST(AvroSchemaProjectionTest, ProjectMapType) {
  // Create iceberg schema with a string->int map
  Schema expected_schema({
      SchemaField::MakeOptional(
          /*field_id=*/1, "counts",
          std::make_shared<MapType>(
              SchemaField::MakeRequired(/*field_id=*/101, "key", iceberg::string()),
              SchemaField::MakeOptional(/*field_id=*/102, "value", iceberg::int32()))),
  });

  // Create equivalent avro schema
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "iceberg_schema",
    "fields": [
      {"name": "counts", "type": ["null", {
        "type": "map",
        "values": ["null", "int"],
        "key-id": 101,
        "value-id": 102
      }], "field-id": 1}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  auto projection_result =
      Project(expected_schema, avro_schema.root(), /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 1);
  ASSERT_EQ(projection.fields[0].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.fields[0].from), 0);
  ASSERT_EQ(projection.fields[0].children.size(), 2);
}

TEST(AvroSchemaProjectionTest, ProjectMapTypeWithNonStringKey) {
  // Create iceberg schema with an int->string map
  Schema expected_schema({
      SchemaField::MakeOptional(
          /*field_id=*/1, "counts",
          std::make_shared<MapType>(
              SchemaField::MakeRequired(/*field_id=*/101, "key", iceberg::int32()),
              SchemaField::MakeOptional(/*field_id=*/102, "value", iceberg::string()))),
  });

  // Create equivalent avro schema (using array-backed map for non-string keys)
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "iceberg_schema",
    "fields": [
      {"name": "counts", "type": ["null", {
        "type": "array",
        "items": {
          "type": "record",
          "name": "key_value",
          "fields": [
            {"name": "key", "type": "int", "field-id": 101},
            {"name": "value", "type": ["null", "string"], "field-id": 102}
          ]
        },
        "logicalType": "map"
      }], "field-id": 1}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  auto projection_result =
      Project(expected_schema, avro_schema.root(), /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 1);
  ASSERT_EQ(projection.fields[0].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.fields[0].from), 0);
  ASSERT_EQ(projection.fields[0].children.size(), 2);
}

TEST(AvroSchemaProjectionTest, ProjectListOfStruct) {
  // Create iceberg schema with list of struct
  Schema expected_schema({
      SchemaField::MakeOptional(
          /*field_id=*/1, "items",
          std::make_shared<ListType>(SchemaField::MakeOptional(
              /*field_id=*/101, "element",
              std::make_shared<StructType>(std::vector<SchemaField>{
                  SchemaField::MakeOptional(/*field_id=*/102, "x", iceberg::int32()),
                  SchemaField::MakeRequired(/*field_id=*/103, "y", iceberg::string()),
              })))),
  });

  // Create equivalent avro schema
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "iceberg_schema",
    "fields": [
      {"name": "items", "type": ["null", {
        "type": "array",
        "items": ["null", {
          "type": "record",
          "name": "element_record",
          "fields": [
            {"name": "x", "type": ["null", "int"], "field-id": 102},
            {"name": "y", "type": "string", "field-id": 103}
          ]
        }],
        "element-id": 101
      }], "field-id": 1}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  auto projection_result =
      Project(expected_schema, avro_schema.root(), /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 1);
  ASSERT_EQ(projection.fields[0].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.fields[0].from), 0);

  // Verify list element struct is properly projected
  ASSERT_EQ(projection.fields[0].children.size(), 1);
  const auto& element_proj = projection.fields[0].children[0];
  ASSERT_EQ(element_proj.children.size(), 2);
  ASSERT_EQ(element_proj.children[0].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(element_proj.children[0].from), 0);
  ASSERT_EQ(element_proj.children[1].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(element_proj.children[1].from), 1);
}

TEST(AvroSchemaProjectionTest, ProjectDecimalType) {
  // Create iceberg schema with decimal
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "value", iceberg::decimal(18, 2)),
  });

  // Create avro schema with decimal
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "iceberg_schema",
    "fields": [
      {
        "name": "value",
        "type": {
          "type": "fixed",
          "name": "decimal_9_2",
          "size": 4,
          "logicalType": "decimal",
          "precision": 9,
          "scale": 2
        },
        "field-id": 1
      }
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  auto projection_result =
      Project(expected_schema, avro_schema.root(), /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsOk());

  const auto& projection = *projection_result;
  ASSERT_EQ(projection.fields.size(), 1);
  ASSERT_EQ(projection.fields[0].kind, FieldProjection::Kind::kProjected);
  ASSERT_EQ(std::get<1>(projection.fields[0].from), 0);
}

TEST(AvroSchemaProjectionTest, ProjectDecimalIncompatible) {
  // Create iceberg schema with decimal having different scale
  Schema expected_schema({
      SchemaField::MakeRequired(/*field_id=*/1, "value", iceberg::decimal(18, 3)),
  });

  // Create avro schema with decimal
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "iceberg_schema",
    "fields": [
      {
        "name": "value",
        "type": {
          "type": "fixed",
          "name": "decimal_9_2",
          "size": 4,
          "logicalType": "decimal",
          "precision": 9,
          "scale": 2
        },
        "field-id": 1
      }
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  auto projection_result =
      Project(expected_schema, avro_schema.root(), /*prune_source=*/false);
  ASSERT_THAT(projection_result, IsError(ErrorKind::kInvalidSchema));
  ASSERT_THAT(projection_result, HasErrorMessage("Cannot read"));
}

}  // namespace iceberg::avro
