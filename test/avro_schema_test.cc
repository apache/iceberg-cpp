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

#include <avro/Types.hh>
#include <gtest/gtest.h>

#include "iceberg/avro/avro_schema_util_internal.h"
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

// FIXME: https://issues.apache.org/jira/browse/AVRO-4140
TEST(ToAvroNodeVisitorTest, DISABLED_UuidType) {
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
  StructType struct_type{
      {SchemaField{/*field_id=*/1, "bool_field", std::make_shared<BooleanType>(),
                   /*optional=*/false},
       SchemaField{/*field_id=*/2, "int_field", std::make_shared<IntType>(),
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
  ListType list_type{SchemaField{/*field_id=*/5, "element",
                                 std::make_shared<StringType>(),
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
  MapType map_type{SchemaField{/*field_id=*/10, "key", std::make_shared<StringType>(),
                               /*optional=*/false},
                   SchemaField{/*field_id=*/11, "value", std::make_shared<IntType>(),
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
  MapType map_type{SchemaField{/*field_id=*/10, "key", std::make_shared<IntType>(),
                               /*optional=*/false},
                   SchemaField{/*field_id=*/11, "value", std::make_shared<StringType>(),
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
  MapType map_type{SchemaField{/*field_id=*/1, "key", std::make_shared<StringType>(),
                               /*optional=*/true},
                   SchemaField{/*field_id=*/2, "value", std::make_shared<StringType>(),
                               /*optional=*/false}};

  ::avro::NodePtr node;
  auto status = ToAvroNodeVisitor{}.Visit(map_type, &node);
  EXPECT_THAT(status, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(status, HasErrorMessage("Map key `key` must be required"));
}

TEST(ToAvroNodeVisitorTest, NestedTypes) {
  auto inner_struct = std::make_shared<StructType>(std::vector<SchemaField>{
      SchemaField{/*field_id=*/2, "string_field", std::make_shared<StringType>(),
                  /*optional=*/false},
      SchemaField{/*field_id=*/3, "int_field", std::make_shared<IntType>(),
                  /*optional=*/true}});
  auto inner_list = std::make_shared<ListType>(SchemaField{/*field_id=*/5, "element",
                                                           std::make_shared<DoubleType>(),
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

}  // namespace iceberg::avro
