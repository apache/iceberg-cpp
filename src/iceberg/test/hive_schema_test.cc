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

#include "iceberg/catalog/hive/hive_schema.h"

#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/type.h"

namespace iceberg::hive {

namespace {

// Convenience: convert a primitive Iceberg type to its Hive DDL string,
// asserting success.
std::string ToHive(const std::shared_ptr<Type>& type) {
  auto result = TypeToHiveString(*type);
  EXPECT_TRUE(result.has_value())
      << "TypeToHiveString failed: " << (result ? "" : result.error().message);
  return result.value_or("");
}

}  // namespace

TEST(TypeToHiveStringTest, PrimitivesMapToHiveScalarTypes) {
  EXPECT_EQ(ToHive(boolean()), "boolean");
  EXPECT_EQ(ToHive(int32()), "int");
  EXPECT_EQ(ToHive(int64()), "bigint");
  EXPECT_EQ(ToHive(float32()), "float");
  EXPECT_EQ(ToHive(float64()), "double");
  EXPECT_EQ(ToHive(date()), "date");
  EXPECT_EQ(ToHive(timestamp()), "timestamp");
}

TEST(TypeToHiveStringTest, TimeStringUuidAllMapToString) {
  EXPECT_EQ(ToHive(time()), "string");
  EXPECT_EQ(ToHive(string()), "string");
  EXPECT_EQ(ToHive(uuid()), "string");
}

TEST(TypeToHiveStringTest, BinaryAndFixedMapToBinary) {
  EXPECT_EQ(ToHive(binary()), "binary");
  EXPECT_EQ(ToHive(fixed(16)), "binary");
}

TEST(TypeToHiveStringTest, DecimalIncludesPrecisionAndScale) {
  EXPECT_EQ(ToHive(decimal(10, 2)), "decimal(10,2)");
  EXPECT_EQ(ToHive(decimal(38, 9)), "decimal(38,9)");
}

TEST(TypeToHiveStringTest, TimestampTzMapsToTimestampWithLocalTimeZone) {
  // Hive's TIMESTAMPLOCALTZ, matching what iceberg-java emits on Hive 3+.
  // A plain `timestamp` here would make every engine reading through HMS
  // see a tz-adjusted column as naive.
  EXPECT_EQ(ToHive(timestamp_tz()), "timestamp with local time zone");
}

TEST(TypeToHiveStringTest, StructFlattensFieldList) {
  StructType struct_type(
      {SchemaField(1, "a", int32(), true), SchemaField(2, "b", string(), true)});
  auto result = TypeToHiveString(struct_type);
  ASSERT_TRUE(result.has_value()) << result.error().message;
  EXPECT_EQ(*result, "struct<a:int,b:string>");
}

TEST(TypeToHiveStringTest, ListWrapsElement) {
  ListType list_type(1, int32(), true);
  auto result = TypeToHiveString(list_type);
  ASSERT_TRUE(result.has_value()) << result.error().message;
  EXPECT_EQ(*result, "array<int>");
}

TEST(TypeToHiveStringTest, MapEmitsKeyAndValue) {
  MapType map_type(SchemaField(1, "key", string(), false),
                   SchemaField(2, "value", int32(), true));
  auto result = TypeToHiveString(map_type);
  ASSERT_TRUE(result.has_value()) << result.error().message;
  EXPECT_EQ(*result, "map<string,int>");
}

TEST(TypeToHiveStringTest, NestedMapInsideMap) {
  // map<string, map<string, int>>
  auto inner_map = std::make_shared<MapType>(SchemaField(3, "key", string(), false),
                                             SchemaField(4, "value", int32(), true));
  MapType outer_map(SchemaField(1, "key", string(), false),
                    SchemaField(2, "value", inner_map, true));
  auto result = TypeToHiveString(outer_map);
  ASSERT_TRUE(result.has_value()) << result.error().message;
  EXPECT_EQ(*result, "map<string,map<string,int>>");
}

TEST(TypeToHiveStringTest, StructInsideListEmitsArrayOfStruct) {
  auto struct_type = std::make_shared<StructType>(std::vector<SchemaField>{
      SchemaField(2, "name", string(), true), SchemaField(3, "age", int32(), true)});
  ListType list_type(SchemaField(1, "element", struct_type, true));
  auto result = TypeToHiveString(list_type);
  ASSERT_TRUE(result.has_value()) << result.error().message;
  EXPECT_EQ(*result, "array<struct<name:string,age:int>>");
}

TEST(TypeToHiveStringTest, NestedTimestampTzKeepsTheMultiWordTypeName) {
  // The Hive type name contains spaces, and it stays spelled out inside a
  // nested type. Java composes struct fields the same way -- via Hive's
  // own TypeInfo round-trip -- so Hive's type parser accepts this form.
  StructType struct_type(
      {SchemaField(1, "a", int32(), true), SchemaField(2, "b", timestamp_tz(), true)});
  auto result = TypeToHiveString(struct_type);
  ASSERT_TRUE(result.has_value()) << result.error().message;
  EXPECT_EQ(*result, "struct<a:int,b:timestamp with local time zone>");
}

TEST(SchemaToHiveColumnsTest, FlatSchemaProducesOneColumnPerField) {
  Schema schema(
      {SchemaField(1, "id", int64(), true), SchemaField(2, "name", string(), true)},
      /*schema_id=*/100);

  auto columns = SchemaToHiveColumns(schema);
  ASSERT_TRUE(columns.has_value()) << columns.error().message;
  ASSERT_EQ(columns->size(), 2);

  EXPECT_EQ((*columns)[0].name, "id");
  EXPECT_EQ((*columns)[0].type_string, "bigint");
  EXPECT_TRUE((*columns)[0].comment.empty());

  EXPECT_EQ((*columns)[1].name, "name");
  EXPECT_EQ((*columns)[1].type_string, "string");
}

TEST(SchemaToHiveColumnsTest, NestedFieldRendersAsFlattenedTypeString) {
  auto address = std::make_shared<StructType>(std::vector<SchemaField>{
      SchemaField(10, "street", string(), true), SchemaField(11, "zip", int32(), true)});
  Schema schema(
      {SchemaField(1, "id", int64(), true), SchemaField(2, "address", address, true)},
      /*schema_id=*/200);

  auto columns = SchemaToHiveColumns(schema);
  ASSERT_TRUE(columns.has_value()) << columns.error().message;
  ASSERT_EQ(columns->size(), 2);
  EXPECT_EQ((*columns)[1].name, "address");
  EXPECT_EQ((*columns)[1].type_string, "struct<street:string,zip:int>");
}

TEST(SchemaToHiveColumnsTest, FieldDocBecomesColumnComment) {
  SchemaField id_field(1, "id", int64(), true, "Synthetic primary key.");
  Schema schema({std::move(id_field)}, /*schema_id=*/1);

  auto columns = SchemaToHiveColumns(schema);
  ASSERT_TRUE(columns.has_value()) << columns.error().message;
  ASSERT_EQ(columns->size(), 1);
  EXPECT_EQ((*columns)[0].comment, "Synthetic primary key.");
}

TEST(SchemaToHiveColumnsTest, TimestampTzColumnIsTzAdjustedInTheDdl) {
  // Distinguishing the two timestamp types is the whole point: a schema
  // carrying both must not collapse them onto the same Hive type.
  Schema schema({SchemaField(1, "ts", timestamp_tz(), true),
                 SchemaField(2, "naive", timestamp(), true)},
                /*schema_id=*/1);
  auto columns = SchemaToHiveColumns(schema);
  ASSERT_TRUE(columns.has_value()) << columns.error().message;
  ASSERT_EQ(columns->size(), 2);
  EXPECT_EQ((*columns)[0].name, "ts");
  EXPECT_EQ((*columns)[0].type_string, "timestamp with local time zone");
  EXPECT_EQ((*columns)[1].name, "naive");
  EXPECT_EQ((*columns)[1].type_string, "timestamp");
}

}  // namespace iceberg::hive
