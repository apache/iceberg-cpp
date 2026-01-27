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

#include "iceberg/json_serde_internal.h"
#include "iceberg/partition_field.h"
#include "iceberg/row/partition_values.h"
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

TEST(PartitionSpecTest, ValidateRedundantPartitionsExactDuplicates) {
  // Create a schema with different field types
  auto ts_field = SchemaField::MakeRequired(1, "ts", timestamp());
  auto id_field = SchemaField::MakeRequired(2, "id", int64());
  Schema schema({ts_field, id_field}, Schema::kInitialSchemaId);

  // Test: exact duplicate transforms on same field (same dedup name)
  {
    PartitionField field1(1, 1000, "ts_day_1_0", Transform::Day());
    PartitionField field2(1, 1001, "ts_day_1_1", Transform::Day());

    auto result = PartitionSpec::Make(schema, 1, {field1, field2}, false);
    EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
    EXPECT_THAT(result, HasErrorMessage("Cannot add redundant partition"));
    EXPECT_THAT(result, HasErrorMessage("conflicts with"));
  }

  // Test: same bucket size on same field (redundant)
  {
    PartitionField bucket1(2, 1000, "id_bucket_16_2_0", Transform::Bucket(16));
    PartitionField bucket2(2, 1001, "id_bucket_16_2_1", Transform::Bucket(16));

    auto result = PartitionSpec::Make(schema, 1, {bucket1, bucket2}, false);
    EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
    EXPECT_THAT(result, HasErrorMessage("Cannot add redundant partition"));
  }

  // Test: same truncate width on same field (redundant)
  {
    auto name_field = SchemaField::MakeRequired(3, "name", string());
    Schema schema_with_string({name_field}, Schema::kInitialSchemaId);

    PartitionField truncate1(3, 1000, "name_trunc_4_3_1", Transform::Truncate(4));
    PartitionField truncate2(3, 1001, "name_trunc_4_3_2", Transform::Truncate(4));

    auto result =
        PartitionSpec::Make(schema_with_string, 1, {truncate1, truncate2}, false);
    EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
    EXPECT_THAT(result, HasErrorMessage("Cannot add redundant partition"));
  }
}

TEST(PartitionSpecTest, ValidateRedundantPartitionsAllowedCases) {
  // Create a schema with different field types
  auto ts_field = SchemaField::MakeRequired(1, "ts", timestamp());
  auto id_field = SchemaField::MakeRequired(2, "id", int64());
  auto name_field = SchemaField::MakeRequired(3, "name", string());
  Schema schema({ts_field, id_field, name_field}, Schema::kInitialSchemaId);

  // Test: different bucket sizes on same field (allowed - different dedup names)
  {
    PartitionField bucket16(2, 1000, "id_bucket_16_2", Transform::Bucket(16));
    PartitionField bucket32(2, 1001, "id_bucket_32_2", Transform::Bucket(32));

    auto result = PartitionSpec::Make(schema, 1, {bucket16, bucket32}, false);
    EXPECT_THAT(result, IsOk());
  }

  // Test: different truncate widths on same field (allowed - different dedup names)
  {
    PartitionField truncate4(3, 1000, "name_trunc_4_3", Transform::Truncate(4));
    PartitionField truncate8(3, 1001, "name_trunc_8_3", Transform::Truncate(8));

    auto result = PartitionSpec::Make(schema, 1, {truncate4, truncate8}, false);
    EXPECT_THAT(result, IsOk());
  }

  // Test: same transforms on different fields (allowed)
  {
    PartitionField ts_day(1, 1000, "ts_day_1", Transform::Day());
    PartitionField id_bucket(2, 1001, "id_bucket_2", Transform::Bucket(16));

    auto result = PartitionSpec::Make(schema, 1, {ts_day, id_bucket}, false);
    EXPECT_THAT(result, IsOk());
  }

  // Test: different transforms on same field (allowed if dedup names differ)
  {
    PartitionField ts_day(1, 1000, "ts_day_1", Transform::Day());
    PartitionField ts_month(1, 1001, "ts_month_1", Transform::Month());

    // This should be allowed since Day and Month have different dedup names
    // The Java logic only checks for exact dedup name matches
    auto result = PartitionSpec::Make(schema, 1, {ts_day, ts_month}, false);
    EXPECT_THAT(result, IsOk());
  }

  // Test: single partition field (no redundancy possible)
  {
    PartitionField single_field(1, 1000, "ts_year_1", Transform::Year());

    auto result = PartitionSpec::Make(schema, 1, {single_field}, false);
    EXPECT_THAT(result, IsOk());
  }
}

TEST(PartitionSpecTest, ValidateRedundantPartitionsIdentityTransforms) {
  // Create a schema with different field types
  auto id_field = SchemaField::MakeRequired(1, "id", int64());
  auto name_field = SchemaField::MakeRequired(2, "name", string());
  Schema schema({id_field, name_field}, Schema::kInitialSchemaId);

  // Test: multiple identity transforms on same field (redundant)
  {
    PartitionField identity1(1, 1000, "id_1_0", Transform::Identity());
    PartitionField identity2(1, 1001, "id_1_1", Transform::Identity());

    auto result = PartitionSpec::Make(schema, 1, {identity1, identity2}, false);
    EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
    EXPECT_THAT(result, HasErrorMessage("Cannot add redundant partition"));
  }

  // Test: identity transforms on different fields (allowed)
  {
    PartitionField id_identity(1, 1000, "id_1", Transform::Identity());
    PartitionField name_identity(2, 1001, "name_2", Transform::Identity());

    auto result = PartitionSpec::Make(schema, 1, {id_identity, name_identity}, false);
    EXPECT_THAT(result, IsOk());
  }
}

TEST(PartitionSpecTest, PartitionPath) {
  // Create a schema with different field types
  auto id_field = SchemaField::MakeRequired(1, "id", int64());
  auto name_field = SchemaField::MakeRequired(2, "name", string());
  auto ts_field = SchemaField::MakeRequired(3, "ts", timestamp());
  Schema schema({id_field, name_field, ts_field}, Schema::kInitialSchemaId);

  // Create partition fields
  PartitionField id_field_partition(1, 1000, "id_partition", Transform::Identity());
  PartitionField name_field_partition(2, 1001, "name_partition", Transform::Identity());
  PartitionField ts_field_partition(3, 1002, "ts_partition", Transform::Day());

  // Create partition spec
  ICEBERG_UNWRAP_OR_FAIL(
      auto spec,
      PartitionSpec::Make(schema, 1,
                          {id_field_partition, name_field_partition, ts_field_partition},
                          false));

  {
    // Invalid partition values
    PartitionValues part_data({Literal::Int(123)});
    auto result = spec->PartitionPath(part_data);
    EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
    EXPECT_THAT(result, HasErrorMessage("Partition spec and data mismatch"));
  }

  {
    // Normal partition values
    PartitionValues part_data(
        {Literal::Int(123), Literal::String("val2"), Literal::Date(19489)});
    ICEBERG_UNWRAP_OR_FAIL(auto path, spec->PartitionPath(part_data));
    std::string expected = "id_partition=123/name_partition=val2/ts_partition=2023-05-12";
    EXPECT_EQ(expected, path);
  }

  {
    // Partition values with special characters
    PartitionValues part_data(
        {Literal::Int(123), Literal::String("val#2"), Literal::Date(19489)});
    ICEBERG_UNWRAP_OR_FAIL(auto path, spec->PartitionPath(part_data));
    std::string expected =
        "id_partition=123/name_partition=val%232/ts_partition=2023-05-12";
    EXPECT_EQ(expected, path);
  }
}

}  // namespace iceberg
