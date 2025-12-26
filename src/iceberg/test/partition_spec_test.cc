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

#include "iceberg/partition_spec.h"

#include <format>
#include <memory>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/json_internal.h"
#include "iceberg/partition_field.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/test/matchers.h"
#include "iceberg/transform.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep

namespace iceberg {

TEST(PartitionSpecTest, Basics) {
  {
    SchemaField field1(5, "ts", timestamp(), true);
    SchemaField field2(7, "bar", string(), true);

    auto identity_transform = Transform::Identity();
    PartitionField pt_field1(5, 1000, "day", identity_transform);
    PartitionField pt_field2(5, 1001, "hour", identity_transform);
    ICEBERG_UNWRAP_OR_FAIL(auto spec, PartitionSpec::Make(100, {pt_field1, pt_field2}));
    ASSERT_EQ(*spec, *spec);
    ASSERT_EQ(100, spec->spec_id());
    std::span<const PartitionField> fields = spec->fields();
    ASSERT_EQ(2, fields.size());
    ASSERT_EQ(pt_field1, fields[0]);
    ASSERT_EQ(pt_field2, fields[1]);
    auto spec_str =
        "partition_spec[spec_id<100>,\n  day (1000 identity(5))\n  hour (1001 "
        "identity(5))\n]";
    EXPECT_EQ(spec_str, spec->ToString());
    EXPECT_EQ(spec_str, std::format("{}", *spec));
  }
}

TEST(PartitionSpecTest, Equality) {
  SchemaField field1(5, "ts", timestamp(), true);
  SchemaField field2(7, "bar", string(), true);
  auto identity_transform = Transform::Identity();
  PartitionField pt_field1(5, 1000, "day", identity_transform);
  PartitionField pt_field2(7, 1001, "hour", identity_transform);
  PartitionField pt_field3(7, 1001, "hour", identity_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec1, PartitionSpec::Make(100, {pt_field1, pt_field2}));
  ICEBERG_UNWRAP_OR_FAIL(auto spec2, PartitionSpec::Make(101, {pt_field1, pt_field2}));
  ICEBERG_UNWRAP_OR_FAIL(auto spec3, PartitionSpec::Make(101, {pt_field1}));
  ICEBERG_UNWRAP_OR_FAIL(auto spec4, PartitionSpec::Make(101, {pt_field3, pt_field1}));
  ICEBERG_UNWRAP_OR_FAIL(auto spec5, PartitionSpec::Make(100, {pt_field1, pt_field2}));
  ICEBERG_UNWRAP_OR_FAIL(auto spec6, PartitionSpec::Make(100, {pt_field2, pt_field1}));

  ASSERT_EQ(*spec1, *spec1);
  ASSERT_NE(*spec1, *spec2);
  ASSERT_NE(*spec2, *spec1);
  ASSERT_NE(*spec1, *spec3);
  ASSERT_NE(*spec3, *spec1);
  ASSERT_NE(*spec1, *spec4);
  ASSERT_NE(*spec4, *spec1);
  ASSERT_EQ(*spec1, *spec5);
  ASSERT_EQ(*spec5, *spec1);
  ASSERT_NE(*spec1, *spec6);
  ASSERT_NE(*spec6, *spec1);
}

TEST(PartitionSpecTest, PartitionSchemaTest) {
  SchemaField field1(5, "ts", timestamp(), true);
  SchemaField field2(7, "bar", string(), true);
  Schema schema({field1, field2}, 100);
  auto identity_transform = Transform::Identity();
  PartitionField pt_field1(5, 1000, "day", identity_transform);
  PartitionField pt_field2(7, 1001, "hour", identity_transform);
  ICEBERG_UNWRAP_OR_FAIL(auto spec, PartitionSpec::Make(100, {pt_field1, pt_field2}));
  ICEBERG_UNWRAP_OR_FAIL(auto partition_type, spec->PartitionType(schema));
  ASSERT_EQ(2, partition_type->fields().size());
  EXPECT_EQ(pt_field1.name(), partition_type->fields()[0].name());
  EXPECT_EQ(pt_field1.field_id(), partition_type->fields()[0].field_id());
  EXPECT_EQ(pt_field2.name(), partition_type->fields()[1].name());
  EXPECT_EQ(pt_field2.field_id(), partition_type->fields()[1].field_id());
}

TEST(PartitionSpecTest, PartitionTypeTest) {
  nlohmann::json json = R"(
  {
  "spec-id": 1,
  "fields": [ {
      "source-id": 4,
      "field-id": 1000,
      "name": "__ts_day",
      "transform": "day"
      }, {
      "source-id": 1,
      "field-id": 1001,
      "name": "__id_bucket",
      "transform": "bucket[16]"
      }, {
      "source-id": 2,
      "field-id": 1002,
      "name": "__id_truncate",
      "transform": "truncate[4]"
      } ]
  })"_json;

  auto field1 = SchemaField::MakeRequired(1, "id", int32());
  auto field2 = SchemaField::MakeRequired(2, "name", string());
  auto field3 = SchemaField::MakeRequired(3, "ts", timestamp());
  auto field4 = SchemaField::MakeRequired(4, "ts_day", timestamp());
  auto field5 = SchemaField::MakeRequired(5, "id_bucket", int32());
  auto field6 = SchemaField::MakeRequired(6, "id_truncate", int32());
  auto const schema = std::make_shared<Schema>(
      std::vector<SchemaField>{field1, field2, field3, field4, field5, field6},
      Schema::kInitialSchemaId);

  ICEBERG_UNWRAP_OR_FAIL(auto parsed_spec, PartitionSpecFromJson(schema, json, 1));
  ICEBERG_UNWRAP_OR_FAIL(auto partition_type, parsed_spec->PartitionType(*schema));

  SchemaField pt_field1(1000, "__ts_day", date(), true);
  SchemaField pt_field2(1001, "__id_bucket", int32(), true);
  SchemaField pt_field3(1002, "__id_truncate", string(), true);

  ASSERT_EQ(3, partition_type->fields().size());

  EXPECT_EQ(pt_field1, partition_type->fields()[0]);
  EXPECT_EQ(pt_field2, partition_type->fields()[1]);
  EXPECT_EQ(pt_field3, partition_type->fields()[2]);
}

TEST(PartitionSpecTest, InvalidTransformForType) {
  // Test Day transform on string type (should fail)
  auto field_string = SchemaField::MakeRequired(6, "s", string());
  Schema schema_string({field_string}, Schema::kInitialSchemaId);

  PartitionField pt_field_invalid(6, 1005, "s_day", Transform::Day());
  auto result = PartitionSpec::Make(schema_string, 1, {pt_field_invalid}, false);
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result, HasErrorMessage("Invalid source type"));

  // Test that Void transform does not error out even with incompatible types
  // Void transform is used for V1 partition-spec when a field gets deleted
  PartitionField pt_field_void(6, 1006, "s_void", Transform::Void());
  auto result_void = PartitionSpec::Make(schema_string, 1, {pt_field_void}, false);
  EXPECT_THAT(result_void, IsOk());
}

TEST(PartitionSpecTest, SourceIdNotFound) {
  auto field1 = SchemaField::MakeRequired(1, "id", int64());
  auto field2 = SchemaField::MakeRequired(2, "ts", timestamp());
  Schema schema({field1, field2}, Schema::kInitialSchemaId);

  // Try to create partition field with source ID 99 which doesn't exist
  PartitionField pt_field_invalid(99, 1000, "Test", Transform::Identity());

  auto result = PartitionSpec::Make(schema, 1, {pt_field_invalid}, false);
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result, HasErrorMessage("Cannot find source column for partition field"));
}

TEST(PartitionSpecTest, AllowMissingFields) {
  auto field1 = SchemaField::MakeRequired(1, "id", int64());
  auto field2 = SchemaField::MakeRequired(2, "ts", timestamp());
  Schema schema({field1, field2}, Schema::kInitialSchemaId);

  // Create partition field with source ID 99 which doesn't exist
  PartitionField pt_field_missing(99, 1000, "Test", Transform::Identity());

  // Without allow_missing_fields, this should fail
  auto result_no_allow =
      PartitionSpec::Make(schema, 1, {pt_field_missing}, false, std::nullopt);
  EXPECT_THAT(result_no_allow, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result_no_allow,
              HasErrorMessage("Cannot find source column for partition field"));

  // With allow_missing_fields, this should succeed (e.g., for evolved schemas where
  // source field was dropped)
  auto result_allow =
      PartitionSpec::Make(schema, 1, {pt_field_missing}, true, std::nullopt);
  EXPECT_THAT(result_allow, IsOk());
}

TEST(PartitionSpecTest, PartitionFieldInStruct) {
  auto field1 = SchemaField::MakeRequired(1, "id", int64());
  auto field2 = SchemaField::MakeRequired(2, "ts", timestamp());
  Schema base_schema({field1, field2}, Schema::kInitialSchemaId);

  auto struct_type =
      std::make_shared<StructType>(std::vector<SchemaField>{field1, field2});
  auto outer_struct = SchemaField::MakeRequired(11, "MyStruct", struct_type);

  Schema schema({outer_struct}, Schema::kInitialSchemaId);
  PartitionField pt_field(1, 1000, "id_partition", Transform::Identity());

  EXPECT_THAT(PartitionSpec::Make(schema, 1, {pt_field}, false), IsOk());
}

TEST(PartitionSpecTest, PartitionFieldInStructInStruct) {
  auto field1 = SchemaField::MakeRequired(1, "id", int64());
  auto field2 = SchemaField::MakeRequired(2, "ts", timestamp());

  auto inner_struct =
      std::make_shared<StructType>(std::vector<SchemaField>{field1, field2});
  auto inner_field = SchemaField::MakeRequired(11, "Inner", inner_struct);
  auto outer_struct = std::make_shared<StructType>(std::vector<SchemaField>{inner_field});
  SchemaField outer_field(12, "Outer", outer_struct, true);

  Schema schema({outer_field}, Schema::kInitialSchemaId);
  PartitionField pt_field(1, 1000, "id_partition", Transform::Identity());
  EXPECT_THAT(PartitionSpec::Make(schema, 1, {pt_field}, false), IsOk());
}

TEST(PartitionSpecTest, PartitionFieldInList) {
  auto list_type = std::make_shared<ListType>(1, int32(), /*element_required=*/false);
  auto list_field = SchemaField::MakeRequired(2, "MyList", list_type);
  Schema schema({list_field}, Schema::kInitialSchemaId);

  // Try to partition on the list element field (field ID 1 is the element)
  PartitionField pt_field(1, 1000, "element_partition", Transform::Identity());

  auto result = PartitionSpec::Make(schema, 1, {pt_field}, false);
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result, HasErrorMessage("Invalid partition field parent"));
}

TEST(PartitionSpecTest, PartitionFieldInStructInList) {
  auto struct_in_list = std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField(1, "Foo", int32(), true)});
  auto list_type = std::make_shared<ListType>(2, struct_in_list,
                                              /*element_required=*/false);
  auto list_field = SchemaField::MakeRequired(3, "MyList", list_type);

  Schema schema({list_field}, Schema::kInitialSchemaId);
  PartitionField pt_field(1, 1000, "foo_partition", Transform::Identity());

  auto result = PartitionSpec::Make(schema, 1, {pt_field}, false);
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result, HasErrorMessage("Invalid partition field parent"));
}

TEST(PartitionSpecTest, PartitionFieldInMap) {
  auto key_field = SchemaField::MakeRequired(1, "key", int32());
  auto value_field = SchemaField::MakeRequired(2, "value", int32());
  auto map_type = std::make_shared<MapType>(key_field, value_field);
  auto map_field = SchemaField::MakeRequired(3, "MyMap", map_type);

  Schema schema({map_field}, Schema::kInitialSchemaId);
  PartitionField pt_field_key(1, 1000, "key_partition", Transform::Identity());

  auto result_key = PartitionSpec::Make(schema, 1, {pt_field_key}, false);
  EXPECT_THAT(result_key, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result_key, HasErrorMessage("Invalid partition field parent"));

  PartitionField pt_field_value(2, 1001, "value_partition", Transform::Identity());

  auto result_value = PartitionSpec::Make(schema, 1, {pt_field_value}, false);
  EXPECT_THAT(result_value, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result_value, HasErrorMessage("Invalid partition field parent"));
}

TEST(PartitionSpecTest, PartitionFieldInStructInMap) {
  auto struct_key = std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField(1, "Foo", int32(), true)});
  auto struct_value = std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField(2, "Bar", int32(), true)});

  auto key_field = SchemaField::MakeRequired(3, "key", struct_key);
  auto value_field = SchemaField::MakeRequired(4, "value", struct_value);
  auto map_type = std::make_shared<MapType>(key_field, value_field);
  auto map_field = SchemaField::MakeRequired(5, "MyMap", map_type);

  Schema schema({map_field}, Schema::kInitialSchemaId);
  PartitionField pt_field_key(1, 1000, "foo_partition", Transform::Identity());

  auto result_key = PartitionSpec::Make(schema, 1, {pt_field_key}, false);
  EXPECT_THAT(result_key, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result_key, HasErrorMessage("Invalid partition field parent"));

  PartitionField pt_field_value(2, 1001, "bar_partition", Transform::Identity());

  auto result_value = PartitionSpec::Make(schema, 1, {pt_field_value}, false);
  EXPECT_THAT(result_value, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result_value, HasErrorMessage("Invalid partition field parent"));
}

}  // namespace iceberg
