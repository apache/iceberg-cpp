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
#include "iceberg/name_mapping.h"
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

// Helper function to check if a custom attribute exists for a field name preservation
void CheckIcebergFieldName(const ::avro::NodePtr& node, size_t index,
                           const std::string& original_name) {
  ASSERT_LT(index, node->customAttributes());
  const auto& attrs = node->customAttributesAt(index);
  ASSERT_EQ(attrs.getAttribute("iceberg-field-name"), std::make_optional(original_name));
}

}  // namespace

TEST(ValidAvroNameTest, ValidNames) {
  // Valid field names should return true
  EXPECT_TRUE(ValidAvroName("valid_field"));
  EXPECT_TRUE(ValidAvroName("field123"));
  EXPECT_TRUE(ValidAvroName("_private"));
  EXPECT_TRUE(ValidAvroName("CamelCase"));
  EXPECT_TRUE(ValidAvroName("field_with_underscores"));
}

TEST(ValidAvroNameTest, InvalidNames) {
  // Names starting with numbers should return false
  EXPECT_FALSE(ValidAvroName("123field"));
  EXPECT_FALSE(ValidAvroName("0value"));

  // Names with special characters should return false
  EXPECT_FALSE(ValidAvroName("field-name"));
  EXPECT_FALSE(ValidAvroName("field.name"));
  EXPECT_FALSE(ValidAvroName("field name"));
  EXPECT_FALSE(ValidAvroName("field@name"));
  EXPECT_FALSE(ValidAvroName("field#name"));
}

TEST(ValidAvroNameTest, EmptyName) {
  // Empty name should return false
  EXPECT_FALSE(ValidAvroName(""));
}

TEST(SanitizeFieldNameTest, ValidFieldNames) {
  // Valid field names should remain unchanged
  EXPECT_EQ(SanitizeFieldName("valid_field"), "valid_field");
  EXPECT_EQ(SanitizeFieldName("field123"), "field123");
  EXPECT_EQ(SanitizeFieldName("_private"), "_private");
  EXPECT_EQ(SanitizeFieldName("CamelCase"), "CamelCase");
  EXPECT_EQ(SanitizeFieldName("field_with_underscores"), "field_with_underscores");
}

TEST(SanitizeFieldNameTest, InvalidFieldNames) {
  // Field names starting with numbers should be prefixed with underscore
  EXPECT_EQ(SanitizeFieldName("123field"), "_123field");
  EXPECT_EQ(SanitizeFieldName("0value"), "_0value");

  // Field names with special characters should be encoded with hex values
  EXPECT_EQ(SanitizeFieldName("field-name"), "field_x2Dname");
  EXPECT_EQ(SanitizeFieldName("field.name"), "field_x2Ename");
  EXPECT_EQ(SanitizeFieldName("field name"), "field_x20name");
  EXPECT_EQ(SanitizeFieldName("field@name"), "field_x40name");
  EXPECT_EQ(SanitizeFieldName("field#name"), "field_x23name");

  // Complex field names with multiple issues
  EXPECT_EQ(SanitizeFieldName("1field-with.special@chars"),
            "_1field_x2Dwith_x2Especial_x40chars");
  EXPECT_EQ(SanitizeFieldName("user-email"), "user_x2Demail");
}

TEST(SanitizeFieldNameTest, EdgeCases) {
  // Empty field name
  EXPECT_EQ(SanitizeFieldName(""), "");

  // Field name with only special characters
  EXPECT_EQ(SanitizeFieldName("@#$"), "_x40_x23_x24");

  // Field name starting with special character
  EXPECT_EQ(SanitizeFieldName("-field"), "_x2Dfield");
  EXPECT_EQ(SanitizeFieldName(".field"), "_x2Efield");
}

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

TEST(ToAvroNodeVisitorTest, StructTypeWithFieldNames) {
  StructType struct_type{
      {SchemaField{/*field_id=*/1, "user-name", iceberg::string(),
                   /*optional=*/false},
       SchemaField{/*field_id=*/2, "valid_field", iceberg::string(),
                   /*optional=*/false},
       SchemaField{/*field_id=*/3, "email.address", iceberg::string(),
                   /*optional=*/true},
       SchemaField{/*field_id=*/4, "AnotherField", iceberg::int32(),
                   /*optional=*/true},
       SchemaField{/*field_id=*/5, "123field", iceberg::int32(),
                   /*optional=*/false},
       SchemaField{/*field_id=*/6, "field with spaces", iceberg::boolean(),
                   /*optional=*/true}}};
  ::avro::NodePtr node;
  EXPECT_THAT(ToAvroNodeVisitor{}.Visit(struct_type, &node), IsOk());
  EXPECT_EQ(node->type(), ::avro::AVRO_RECORD);

  ASSERT_EQ(node->names(), 6);

  EXPECT_EQ(node->nameAt(0), "user_x2Dname");  // "user-name" -> "user_x2Dname"
  EXPECT_EQ(node->nameAt(2),
            "email_x2Eaddress");            // "email.address" -> "email_x2Eaddress"
  EXPECT_EQ(node->nameAt(4), "_123field");  // "123field" -> "_123field"
  EXPECT_EQ(
      node->nameAt(5),
      "field_x20with_x20spaces");  // "field with spaces" -> "field_x20with_x20spaces"

  EXPECT_EQ(node->nameAt(1), "valid_field");
  EXPECT_EQ(node->nameAt(3), "AnotherField");

  ASSERT_EQ(node->customAttributes(), 6);
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(node, /*index=*/0, /*field_id=*/1));
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(node, /*index=*/1, /*field_id=*/2));
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(node, /*index=*/2, /*field_id=*/3));
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(node, /*index=*/3, /*field_id=*/4));
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(node, /*index=*/4, /*field_id=*/5));
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(node, /*index=*/5, /*field_id=*/6));

  const auto& attrs1 = node->customAttributesAt(1);  // valid_field
  const auto& attrs3 = node->customAttributesAt(3);  // AnotherField
  EXPECT_FALSE(attrs1.getAttribute("iceberg-field-name").has_value());
  EXPECT_FALSE(attrs3.getAttribute("iceberg-field-name").has_value());

  ASSERT_NO_FATAL_FAILURE(
      CheckIcebergFieldName(node, /*index=*/0, /*original_name=*/"user-name"));
  ASSERT_NO_FATAL_FAILURE(
      CheckIcebergFieldName(node, /*index=*/2, /*original_name=*/"email.address"));
  ASSERT_NO_FATAL_FAILURE(
      CheckIcebergFieldName(node, /*index=*/4, /*original_name=*/"123field"));
  ASSERT_NO_FATAL_FAILURE(
      CheckIcebergFieldName(node, /*index=*/5, /*original_name=*/"field with spaces"));
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

// NameMapping tests for Avro schema context
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

  // Helper function to create a name mapping for complex map types
  // (array<struct<key,value>>)
  std::unique_ptr<NameMapping> CreateComplexMapNameMapping() {
    std::vector<MappedField> fields;
    fields.emplace_back(MappedField{.names = {"id"}, .field_id = 1});

    // Nested mapping for array element (struct<key,value>)
    std::vector<MappedField> element_fields;
    element_fields.emplace_back(MappedField{.names = {"key"}, .field_id = 40});
    element_fields.emplace_back(MappedField{.names = {"value"}, .field_id = 41});
    auto element_mapping = MappedFields::Make(std::move(element_fields));

    // Nested mapping for array
    std::vector<MappedField> array_fields;
    array_fields.emplace_back(MappedField{.names = {"element"},
                                          .field_id = 50,
                                          .nested_mapping = std::move(element_mapping)});
    auto array_mapping = MappedFields::Make(std::move(array_fields));

    fields.emplace_back(MappedField{
        .names = {"entries"}, .field_id = 2, .nested_mapping = std::move(array_mapping)});

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

  auto result = MakeAvroNodeWithFieldIds(avro_schema.root(), *name_mapping);
  ASSERT_THAT(result, IsOk());

  const auto& node = *result;
  EXPECT_EQ(node->type(), ::avro::AVRO_RECORD);
  EXPECT_EQ(node->names(), 3);
  EXPECT_EQ(node->leaves(), 3);

  // Check that field IDs are properly applied
  ASSERT_EQ(node->customAttributes(), 3);
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(node, /*index=*/0, /*field_id=*/1));
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(node, /*index=*/1, /*field_id=*/2));
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(node, /*index=*/2, /*field_id=*/3));
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

  auto result = MakeAvroNodeWithFieldIds(avro_schema.root(), *name_mapping);
  ASSERT_THAT(result, IsOk());

  const auto& node = *result;
  EXPECT_EQ(node->type(), ::avro::AVRO_RECORD);
  EXPECT_EQ(node->names(), 2);
  EXPECT_EQ(node->leaves(), 2);

  // Check that field IDs are properly applied to top-level fields
  ASSERT_EQ(node->customAttributes(), 2);
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(node, /*index=*/0, /*field_id=*/1));
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(node, /*index=*/1, /*field_id=*/2));

  // Check nested record
  const auto& address_node = node->leafAt(1);
  EXPECT_EQ(address_node->type(), ::avro::AVRO_RECORD);
  EXPECT_EQ(address_node->names(), 3);
  EXPECT_EQ(address_node->leaves(), 3);

  // Check that field IDs are properly applied to nested fields
  ASSERT_EQ(address_node->customAttributes(), 3);
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(address_node, /*index=*/0, /*field_id=*/10));
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(address_node, /*index=*/1, /*field_id=*/11));
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(address_node, /*index=*/2, /*field_id=*/12));
}

TEST_F(NameMappingAvroSchemaTest, ApplyNameMappingToArray) {
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

  auto result = MakeAvroNodeWithFieldIds(avro_schema.root(), *name_mapping);
  ASSERT_THAT(result, IsOk());

  const auto& new_node = *result;
  EXPECT_EQ(new_node->type(), ::avro::AVRO_RECORD);
  EXPECT_EQ(new_node->names(), 2);
  EXPECT_EQ(new_node->leaves(), 2);

  // Check that field IDs are properly applied to top-level fields
  ASSERT_EQ(new_node->customAttributes(), 2);
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(new_node, /*index=*/0, /*field_id=*/1));
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(new_node, /*index=*/1, /*field_id=*/2));

  // Check array field structure and element field ID
  const auto& array_node = new_node->leafAt(1);
  EXPECT_EQ(array_node->type(), ::avro::AVRO_ARRAY);
  EXPECT_EQ(array_node->leaves(), 1);

  // Check that array element has field ID applied
  const auto& element_node = array_node->leafAt(0);
  EXPECT_EQ(element_node->type(), ::avro::AVRO_STRING);
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(array_node, /*index=*/0, /*field_id=*/20,
                                         /*key=*/"element-id"));
}

TEST_F(NameMappingAvroSchemaTest, ApplyNameMappingToMap) {
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

  auto result = MakeAvroNodeWithFieldIds(avro_schema.root(), *name_mapping);
  ASSERT_THAT(result, IsOk());

  const auto& new_node = *result;
  EXPECT_EQ(new_node->type(), ::avro::AVRO_RECORD);
  EXPECT_EQ(new_node->names(), 2);
  EXPECT_EQ(new_node->leaves(), 2);

  // Check that field IDs are properly applied to top-level fields
  ASSERT_EQ(new_node->customAttributes(), 2);
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(new_node, /*index=*/0, /*field_id=*/1));
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(new_node, /*index=*/1, /*field_id=*/2));

  // Check map field structure and key-value field IDs
  const auto& map_node = new_node->leafAt(1);
  EXPECT_EQ(map_node->type(), ::avro::AVRO_MAP);
  ASSERT_GE(map_node->leaves(), 2);
  EXPECT_EQ(map_node->leafAt(0)->type(), ::avro::AVRO_STRING);
  EXPECT_EQ(map_node->leafAt(1)->type(), ::avro::AVRO_STRING);
  ASSERT_EQ(map_node->customAttributes(), 2);
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(map_node, /*index=*/0, /*field_id=*/30,
                                         /*key=*/"key-id"));
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(map_node, /*index=*/1, /*field_id=*/31,
                                         /*key=*/"value-id"));
}

TEST_F(NameMappingAvroSchemaTest, ApplyNameMappingToComplexMap) {
  // Create an Avro schema for complex map (array<struct<key,value>>) without field IDs
  // This represents a map where key is not a string type
  std::string avro_schema_json = R"({
    "type": "record",
    "name": "test_record",
    "fields": [
      {"name": "id", "type": "int"},
      {"name": "entries", "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "entry",
          "fields": [
            {"name": "key", "type": "int"},
            {"name": "value", "type": "string"}
          ]
        }
      }}
    ]
  })";
  auto avro_schema = ::avro::compileJsonSchemaFromString(avro_schema_json);

  auto name_mapping = CreateComplexMapNameMapping();

  auto result = MakeAvroNodeWithFieldIds(avro_schema.root(), *name_mapping);
  ASSERT_THAT(result, IsOk());

  const auto& new_node = *result;
  EXPECT_EQ(new_node->type(), ::avro::AVRO_RECORD);
  EXPECT_EQ(new_node->names(), 2);
  EXPECT_EQ(new_node->leaves(), 2);

  // Check that field IDs are properly applied to top-level fields
  ASSERT_EQ(new_node->customAttributes(), 2);
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(new_node, /*index=*/0, /*field_id=*/1));
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(new_node, /*index=*/1, /*field_id=*/2));

  // Check array field structure (representing the map)
  const auto& array_node = new_node->leafAt(1);
  EXPECT_EQ(array_node->type(), ::avro::AVRO_ARRAY);
  EXPECT_EQ(array_node->leaves(), 1);

  // Check array element (struct<key,value>)
  const auto& element_node = array_node->leafAt(0);
  EXPECT_EQ(element_node->type(), ::avro::AVRO_RECORD);
  EXPECT_EQ(element_node->names(), 2);
  EXPECT_EQ(element_node->leaves(), 2);

  // Check that field IDs are properly applied to struct fields
  ASSERT_EQ(element_node->customAttributes(), 2);
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(element_node, /*index=*/0, /*field_id=*/40));
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(element_node, /*index=*/1, /*field_id=*/41));

  // Check key and value types
  EXPECT_EQ(element_node->leafAt(0)->type(), ::avro::AVRO_INT);
  EXPECT_EQ(element_node->leafAt(1)->type(), ::avro::AVRO_STRING);
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

  auto result = MakeAvroNodeWithFieldIds(avro_schema.root(), *name_mapping);
  ASSERT_THAT(result, IsOk());

  const auto& new_node = *result;
  EXPECT_EQ(new_node->type(), ::avro::AVRO_RECORD);
  EXPECT_EQ(new_node->names(), 2);
  EXPECT_EQ(new_node->leaves(), 2);

  // Check that field IDs are properly applied to top-level fields
  ASSERT_EQ(new_node->customAttributes(), 2);
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(new_node, /*index=*/0, /*field_id=*/1));
  ASSERT_NO_FATAL_FAILURE(CheckFieldIdAt(new_node, /*index=*/1, /*field_id=*/2));

  // Check union field
  const auto& union_node = new_node->leafAt(1);
  EXPECT_EQ(union_node->type(), ::avro::AVRO_UNION);
  EXPECT_EQ(union_node->leaves(), 2);

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

  auto result = MakeAvroNodeWithFieldIds(avro_schema.root(), *name_mapping);
  ASSERT_THAT(result, IsError(ErrorKind::kInvalidSchema));
}
}  // namespace iceberg::avro
