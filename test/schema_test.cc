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

#include "iceberg/schema.h"

#include <format>
#include <memory>
#include <thread>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/result.h"
#include "iceberg/schema_field.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep
#include "matchers.h"

TEST(SchemaTest, Basics) {
  {
    iceberg::SchemaField field1(5, "foo", iceberg::int32(), true);
    iceberg::SchemaField field2(7, "bar", iceberg::string(), true);
    iceberg::Schema schema({field1, field2}, 100);
    ASSERT_EQ(schema, schema);
    ASSERT_EQ(100, schema.schema_id());
    std::span<const iceberg::SchemaField> fields = schema.fields();
    ASSERT_EQ(2, fields.size());
    ASSERT_EQ(field1, fields[0]);
    ASSERT_EQ(field2, fields[1]);
    ASSERT_THAT(schema.GetFieldById(5), ::testing::Optional(field1));
    ASSERT_THAT(schema.GetFieldById(7), ::testing::Optional(field2));
    ASSERT_THAT(schema.GetFieldByIndex(0), ::testing::Optional(field1));
    ASSERT_THAT(schema.GetFieldByIndex(1), ::testing::Optional(field2));
    ASSERT_THAT(schema.GetFieldByName("foo"), ::testing::Optional(field1));
    ASSERT_THAT(schema.GetFieldByName("bar"), ::testing::Optional(field2));

    ASSERT_EQ(std::nullopt, schema.GetFieldById(0));
    auto result = schema.GetFieldByIndex(2);
    ASSERT_THAT(result, IsError(iceberg::ErrorKind::kInvalidArgument));
    ASSERT_THAT(result,
                iceberg::HasErrorMessage("Invalid index 2 to get field from struct"));
    result = schema.GetFieldByIndex(-1);
    ASSERT_THAT(result, IsError(iceberg::ErrorKind::kInvalidArgument));
    ASSERT_THAT(result,
                iceberg::HasErrorMessage("Invalid index -1 to get field from struct"));
    ASSERT_EQ(std::nullopt, schema.GetFieldByName("element"));
  }
}

TEST(SchemaTest, Equality) {
  iceberg::SchemaField field1(5, "foo", iceberg::int32(), true);
  iceberg::SchemaField field2(7, "bar", iceberg::string(), true);
  iceberg::SchemaField field3(5, "foobar", iceberg::int32(), true);
  iceberg::Schema schema1({field1, field2}, 100);
  iceberg::Schema schema2({field1, field2}, 101);
  iceberg::Schema schema3({field1}, 101);
  iceberg::Schema schema4({field3, field2}, 101);
  iceberg::Schema schema5({field1, field2}, 100);

  ASSERT_EQ(schema1, schema1);
  ASSERT_NE(schema1, schema2);
  ASSERT_NE(schema2, schema1);
  ASSERT_NE(schema1, schema3);
  ASSERT_NE(schema3, schema1);
  ASSERT_NE(schema1, schema4);
  ASSERT_NE(schema4, schema1);
  ASSERT_EQ(schema1, schema5);
  ASSERT_EQ(schema5, schema1);
}

class BasicShortNameTest : public ::testing::Test {
 protected:
  void SetUp() override {
    field1_ = std::make_unique<iceberg::SchemaField>(1, "Foo", iceberg::int32(), true);
    field2_ = std::make_unique<iceberg::SchemaField>(2, "Bar", iceberg::string(), true);
    field3_ = std::make_unique<iceberg::SchemaField>(3, "Foobar", iceberg::int32(), true);

    auto structtype = std::make_shared<iceberg::StructType>(
        std::vector<iceberg::SchemaField>{*field1_, *field2_, *field3_});

    field4_ = std::make_unique<iceberg::SchemaField>(4, "element", structtype, false);

    auto listype = std::make_shared<iceberg::ListType>(*field4_);

    field5_ = std::make_unique<iceberg::SchemaField>(5, "key", iceberg::int32(), false);
    field6_ = std::make_unique<iceberg::SchemaField>(6, "value", listype, false);

    auto maptype = std::make_shared<iceberg::MapType>(*field5_, *field6_);

    field7_ = std::make_unique<iceberg::SchemaField>(7, "Value", maptype, false);

    schema_ =
        std::make_unique<iceberg::Schema>(std::vector<iceberg::SchemaField>{*field7_}, 1);
  }

  std::unique_ptr<iceberg::Schema> schema_;
  std::unique_ptr<iceberg::SchemaField> field1_;
  std::unique_ptr<iceberg::SchemaField> field2_;
  std::unique_ptr<iceberg::SchemaField> field3_;
  std::unique_ptr<iceberg::SchemaField> field4_;
  std::unique_ptr<iceberg::SchemaField> field5_;
  std::unique_ptr<iceberg::SchemaField> field6_;
  std::unique_ptr<iceberg::SchemaField> field7_;
};

TEST_F(BasicShortNameTest, TestFindById) {
  ASSERT_THAT(schema_->FindFieldById(7), ::testing::Optional(*field7_));
  ASSERT_THAT(schema_->FindFieldById(6), ::testing::Optional(*field6_));
  ASSERT_THAT(schema_->FindFieldById(5), ::testing::Optional(*field5_));
  ASSERT_THAT(schema_->FindFieldById(4), ::testing::Optional(*field4_));
  ASSERT_THAT(schema_->FindFieldById(3), ::testing::Optional(*field3_));
  ASSERT_THAT(schema_->FindFieldById(2), ::testing::Optional(*field2_));
  ASSERT_THAT(schema_->FindFieldById(1), ::testing::Optional(*field1_));

  ASSERT_THAT(schema_->FindFieldById(10), ::testing::Optional(std::nullopt));
}

TEST_F(BasicShortNameTest, TestFindByName) {
  ASSERT_THAT(schema_->FindFieldByName("Value"), ::testing::Optional(*field7_));
  ASSERT_THAT(schema_->FindFieldByName("Value.value"), ::testing::Optional(*field6_));
  ASSERT_THAT(schema_->FindFieldByName("Value.key"), ::testing::Optional(*field5_));
  ASSERT_THAT(schema_->FindFieldByName("Value.value.element"),
              ::testing::Optional(*field4_));
  ASSERT_THAT(schema_->FindFieldByName("Value.value.element.Foobar"),
              ::testing::Optional(*field3_));
  ASSERT_THAT(schema_->FindFieldByName("Value.value.element.Bar"),
              ::testing::Optional(*field2_));
  ASSERT_THAT(schema_->FindFieldByName("Value.value.element.Foo"),
              ::testing::Optional(*field1_));

  ASSERT_THAT(schema_->FindFieldByName("Value.value.element.FoO"),
              ::testing::Optional(std::nullopt));
}

TEST_F(BasicShortNameTest, TestFindByNameCaseInsensitive) {
  ASSERT_THAT(schema_->FindFieldByName("vALue", false), ::testing::Optional(*field7_));
  ASSERT_THAT(schema_->FindFieldByName("vALue.VALUE", false),
              ::testing::Optional(*field6_));
  ASSERT_THAT(schema_->FindFieldByName("valUe.kEy", false),
              ::testing::Optional(*field5_));
  ASSERT_THAT(schema_->FindFieldByName("vaLue.vAlue.elEment", false),
              ::testing::Optional(*field4_));
  ASSERT_THAT(schema_->FindFieldByName("vaLue.vAlue.eLement.fOObar", false),
              ::testing::Optional(*field3_));
  ASSERT_THAT(schema_->FindFieldByName("valUe.vaLUe.elemEnt.Bar", false),
              ::testing::Optional(*field2_));
  ASSERT_THAT(schema_->FindFieldByName("valUe.valUe.ELEMENT.FOO", false),
              ::testing::Optional(*field1_));
  ASSERT_THAT(schema_->FindFieldByName("valUe.valUe.ELEMENT.FO", false),
              ::testing::Optional(std::nullopt));
}

TEST_F(BasicShortNameTest, TestFindByShortNameCaseInsensitive) {
  ASSERT_THAT(schema_->FindFieldByName("vaLue.value.FOO", false),
              ::testing::Optional(*field1_));
  ASSERT_THAT(schema_->FindFieldByName("Value.value.Bar", false),
              ::testing::Optional(*field2_));
  ASSERT_THAT(schema_->FindFieldByName("Value.value.FooBAR", false),
              ::testing::Optional(*field3_));
  ASSERT_THAT(schema_->FindFieldByName("Value.value.FooBAR.a", false),
              ::testing::Optional(std::nullopt));
}

class ComplexShortNameTest : public ::testing::Test {
 protected:
  void SetUp() override {
    field1_ = std::make_unique<iceberg::SchemaField>(1, "Foo", iceberg::int32(), true);
    field2_ = std::make_unique<iceberg::SchemaField>(2, "Bar", iceberg::string(), true);
    field3_ = std::make_unique<iceberg::SchemaField>(3, "Foobar", iceberg::int32(), true);

    auto structtype = std::make_shared<iceberg::StructType>(
        std::vector<iceberg::SchemaField>{*field1_, *field2_, *field3_});

    field4_ = std::make_unique<iceberg::SchemaField>(4, "element", structtype, false);

    auto listype = std::make_shared<iceberg::ListType>(*field4_);

    field5_ =
        std::make_unique<iceberg::SchemaField>(5, "First_child", iceberg::int32(), false);
    field6_ = std::make_unique<iceberg::SchemaField>(6, "Second_child", listype, false);

    auto structtype2 = std::make_shared<iceberg::StructType>(
        std::vector<iceberg::SchemaField>{*field5_, *field6_});

    field7_ = std::make_unique<iceberg::SchemaField>(7, "key", iceberg::int32(), false);
    field8_ = std::make_unique<iceberg::SchemaField>(8, "value", structtype2, false);

    auto maptype = std::make_shared<iceberg::MapType>(*field7_, *field8_);

    field9_ = std::make_unique<iceberg::SchemaField>(9, "Map", maptype, false);

    schema_ =
        std::make_unique<iceberg::Schema>(std::vector<iceberg::SchemaField>{*field9_}, 1);
  }

  std::unique_ptr<iceberg::Schema> schema_;
  std::unique_ptr<iceberg::SchemaField> field1_;
  std::unique_ptr<iceberg::SchemaField> field2_;
  std::unique_ptr<iceberg::SchemaField> field3_;
  std::unique_ptr<iceberg::SchemaField> field4_;
  std::unique_ptr<iceberg::SchemaField> field5_;
  std::unique_ptr<iceberg::SchemaField> field6_;
  std::unique_ptr<iceberg::SchemaField> field7_;
  std::unique_ptr<iceberg::SchemaField> field8_;
  std::unique_ptr<iceberg::SchemaField> field9_;
};

TEST_F(ComplexShortNameTest, TestFindById) {
  ASSERT_THAT(schema_->FindFieldById(9), ::testing::Optional(*field9_));
  ASSERT_THAT(schema_->FindFieldById(8), ::testing::Optional(*field8_));
  ASSERT_THAT(schema_->FindFieldById(7), ::testing::Optional(*field7_));
  ASSERT_THAT(schema_->FindFieldById(6), ::testing::Optional(*field6_));
  ASSERT_THAT(schema_->FindFieldById(5), ::testing::Optional(*field5_));
  ASSERT_THAT(schema_->FindFieldById(4), ::testing::Optional(*field4_));
  ASSERT_THAT(schema_->FindFieldById(3), ::testing::Optional(*field3_));
  ASSERT_THAT(schema_->FindFieldById(2), ::testing::Optional(*field2_));
  ASSERT_THAT(schema_->FindFieldById(1), ::testing::Optional(*field1_));

  ASSERT_THAT(schema_->FindFieldById(0), ::testing::Optional(std::nullopt));
}

TEST_F(ComplexShortNameTest, TestFindByName) {
  ASSERT_THAT(schema_->FindFieldByName("Map"), ::testing::Optional(*field9_));
  ASSERT_THAT(schema_->FindFieldByName("Map.value"), ::testing::Optional(*field8_));
  ASSERT_THAT(schema_->FindFieldByName("Map.key"), ::testing::Optional(*field7_));
  ASSERT_THAT(schema_->FindFieldByName("Map.value.Second_child"),
              ::testing::Optional(*field6_));
  ASSERT_THAT(schema_->FindFieldByName("Map.value.First_child"),
              ::testing::Optional(*field5_));
  ASSERT_THAT(schema_->FindFieldByName("Map.value.Second_child.element"),
              ::testing::Optional(*field4_));
  ASSERT_THAT(schema_->FindFieldByName("Map.value.Second_child.element.Foobar"),
              ::testing::Optional(*field3_));
  ASSERT_THAT(schema_->FindFieldByName("Map.value.Second_child.element.Bar"),
              ::testing::Optional(*field2_));
  ASSERT_THAT(schema_->FindFieldByName("Map.value.Second_child.element.Foo"),
              ::testing::Optional(*field1_));
  ASSERT_THAT(schema_->FindFieldByName("Map.value.Second_child.element.Fooo"),
              ::testing::Optional(std::nullopt));
}

TEST_F(ComplexShortNameTest, TestFindByNameCaseInsensitive) {
  ASSERT_THAT(schema_->FindFieldByName("map", false), ::testing::Optional(*field9_));
  ASSERT_THAT(schema_->FindFieldByName("map.vALUE", false),
              ::testing::Optional(*field8_));
  ASSERT_THAT(schema_->FindFieldByName("map.Key", false), ::testing::Optional(*field7_));
  ASSERT_THAT(schema_->FindFieldByName("map.Value.second_Child", false),
              ::testing::Optional(*field6_));
  ASSERT_THAT(schema_->FindFieldByName("map.Value.first_chIld", false),
              ::testing::Optional(*field5_));
  ASSERT_THAT(schema_->FindFieldByName("map.Value.second_child.Element", false),
              ::testing::Optional(*field4_));
  ASSERT_THAT(schema_->FindFieldByName("map.Value.second_child.Element.foobar", false),
              ::testing::Optional(*field3_));
  ASSERT_THAT(schema_->FindFieldByName("map.VaLue.second_child.Element.bar", false),
              ::testing::Optional(*field2_));
  ASSERT_THAT(schema_->FindFieldByName("map.value.Second_child.Element.foo", false),
              ::testing::Optional(*field1_));
  ASSERT_THAT(schema_->FindFieldByName("map.value.Second_child.Element.fooo", false),
              ::testing::Optional(std::nullopt));
}

TEST_F(ComplexShortNameTest, TestFindByShortName) {
  ASSERT_THAT(schema_->FindFieldByName("Map.Second_child"),
              ::testing::Optional(*field6_));
  ASSERT_THAT(schema_->FindFieldByName("Map.First_child"), ::testing::Optional(*field5_));
  ASSERT_THAT(schema_->FindFieldByName("Map.Second_child.Foobar"),
              ::testing::Optional(*field3_));
  ASSERT_THAT(schema_->FindFieldByName("Map.Second_child.Bar"),
              ::testing::Optional(*field2_));
  ASSERT_THAT(schema_->FindFieldByName("Map.Second_child.Foo"),
              ::testing::Optional(*field1_));
  ASSERT_THAT(schema_->FindFieldByName("Map.Second_child.aaa"),
              ::testing::Optional(std::nullopt));
}

TEST_F(ComplexShortNameTest, TestFindByShortNameCaseInsensitive) {
  ASSERT_THAT(schema_->FindFieldByName("map.second_child", false),
              ::testing::Optional(*field6_));
  ASSERT_THAT(schema_->FindFieldByName("map.first_child", false),
              ::testing::Optional(*field5_));
  ASSERT_THAT(schema_->FindFieldByName("map.second_child.foobar", false),
              ::testing::Optional(*field3_));
  ASSERT_THAT(schema_->FindFieldByName("map.second_child.bar", false),
              ::testing::Optional(*field2_));
  ASSERT_THAT(schema_->FindFieldByName("map.second_child.foo", false),
              ::testing::Optional(*field1_));
  ASSERT_THAT(schema_->FindFieldByName("Map.Second_child.aaa", false),
              ::testing::Optional(std::nullopt));
}

class ComplexMapStructShortNameTest : public ::testing::Test {
 protected:
  void SetUp() override {
    exp_inner_key_key_ =
        std::make_unique<iceberg::SchemaField>(10, "inner_key", iceberg::int32(), false);
    exp_inner_key_value_ = std::make_unique<iceberg::SchemaField>(
        11, "inner_value", iceberg::int32(), false);
    auto inner_struct_type_key_ = std::make_shared<iceberg::StructType>(
        std::vector<iceberg::SchemaField>{*exp_inner_key_key_, *exp_inner_key_value_});

    exp_inner_value_k_ =
        std::make_unique<iceberg::SchemaField>(12, "inner_k", iceberg::int32(), false);
    exp_inner_value_v_ =
        std::make_unique<iceberg::SchemaField>(13, "inner_v", iceberg::int32(), false);
    auto inner_struct_type_value_ = std::make_shared<iceberg::StructType>(
        std::vector<iceberg::SchemaField>{*exp_inner_value_k_, *exp_inner_value_v_});

    exp_key_struct_key_ =
        std::make_unique<iceberg::SchemaField>(14, "key", iceberg::int32(), false);
    exp_key_struct_value_ = std::make_unique<iceberg::SchemaField>(
        15, "value", inner_struct_type_key_, false);
    auto key_struct_type_ = std::make_shared<iceberg::StructType>(
        std::vector<iceberg::SchemaField>{*exp_key_struct_key_, *exp_key_struct_value_});

    exp_value_struct_key_ =
        std::make_unique<iceberg::SchemaField>(16, "key", iceberg::int32(), false);
    exp_value_struct_value_ = std::make_unique<iceberg::SchemaField>(
        17, "value", inner_struct_type_value_, false);
    auto value_struct_type_ =
        std::make_shared<iceberg::StructType>(std::vector<iceberg::SchemaField>{
            *exp_value_struct_key_, *exp_value_struct_value_});

    exp_map_key_ =
        std::make_unique<iceberg::SchemaField>(18, "key", key_struct_type_, false);
    exp_map_value_ =
        std::make_unique<iceberg::SchemaField>(19, "value", value_struct_type_, false);
    auto map_type_ = std::make_shared<iceberg::MapType>(*exp_map_key_, *exp_map_value_);

    exp_field_a_ = std::make_unique<iceberg::SchemaField>(20, "a", map_type_, false);

    schema_ = std::make_unique<iceberg::Schema>(
        std::vector<iceberg::SchemaField>{*exp_field_a_}, 1);
  }

  std::unique_ptr<iceberg::Schema> schema_;
  std::unique_ptr<iceberg::SchemaField> exp_inner_key_key_;
  std::unique_ptr<iceberg::SchemaField> exp_inner_key_value_;
  std::unique_ptr<iceberg::SchemaField> exp_inner_value_k_;
  std::unique_ptr<iceberg::SchemaField> exp_inner_value_v_;
  std::unique_ptr<iceberg::SchemaField> exp_key_struct_key_;
  std::unique_ptr<iceberg::SchemaField> exp_key_struct_value_;
  std::unique_ptr<iceberg::SchemaField> exp_value_struct_key_;
  std::unique_ptr<iceberg::SchemaField> exp_value_struct_value_;
  std::unique_ptr<iceberg::SchemaField> exp_map_key_;
  std::unique_ptr<iceberg::SchemaField> exp_map_value_;
  std::unique_ptr<iceberg::SchemaField> exp_field_a_;
};

TEST_F(ComplexMapStructShortNameTest, TestFindById) {
  ASSERT_THAT(schema_->FindFieldById(20), ::testing::Optional(*exp_field_a_));
  ASSERT_THAT(schema_->FindFieldById(19), ::testing::Optional(*exp_map_value_));
  ASSERT_THAT(schema_->FindFieldById(18), ::testing::Optional(*exp_map_key_));
  ASSERT_THAT(schema_->FindFieldById(17), ::testing::Optional(*exp_value_struct_value_));
  ASSERT_THAT(schema_->FindFieldById(16), ::testing::Optional(*exp_value_struct_key_));
  ASSERT_THAT(schema_->FindFieldById(15), ::testing::Optional(*exp_key_struct_value_));
  ASSERT_THAT(schema_->FindFieldById(14), ::testing::Optional(*exp_key_struct_key_));
  ASSERT_THAT(schema_->FindFieldById(13), ::testing::Optional(*exp_inner_value_v_));
  ASSERT_THAT(schema_->FindFieldById(12), ::testing::Optional(*exp_inner_value_k_));
  ASSERT_THAT(schema_->FindFieldById(11), ::testing::Optional(*exp_inner_key_value_));
  ASSERT_THAT(schema_->FindFieldById(10), ::testing::Optional(*exp_inner_key_key_));
}

TEST_F(ComplexMapStructShortNameTest, TestFindByName) {
  ASSERT_THAT(schema_->FindFieldByName("a"), ::testing::Optional(*exp_field_a_));
  ASSERT_THAT(schema_->FindFieldByName("a.key"), ::testing::Optional(*exp_map_key_));
  ASSERT_THAT(schema_->FindFieldByName("a.value"), ::testing::Optional(*exp_map_value_));
  ASSERT_THAT(schema_->FindFieldByName("a.key.key"),
              ::testing::Optional(*exp_key_struct_key_));
  ASSERT_THAT(schema_->FindFieldByName("a.key.value"),
              ::testing::Optional(*exp_key_struct_value_));
  ASSERT_THAT(schema_->FindFieldByName("a.key.value.inner_key"),
              ::testing::Optional(*exp_inner_key_key_));
  ASSERT_THAT(schema_->FindFieldByName("a.key.value.inner_value"),
              ::testing::Optional(*exp_inner_key_value_));
  ASSERT_THAT(schema_->FindFieldByName("a.value.key"),
              ::testing::Optional(*exp_value_struct_key_));
  ASSERT_THAT(schema_->FindFieldByName("a.value.value"),
              ::testing::Optional(*exp_value_struct_value_));
  ASSERT_THAT(schema_->FindFieldByName("a.value.value.inner_k"),
              ::testing::Optional(*exp_inner_value_k_));
  ASSERT_THAT(schema_->FindFieldByName("a.value.value.inner_v"),
              ::testing::Optional(*exp_inner_value_v_));
}

TEST_F(ComplexMapStructShortNameTest, TestFindByNameCaseInsensitive) {
  ASSERT_THAT(schema_->FindFieldByName("A", false), ::testing::Optional(*exp_field_a_));
  ASSERT_THAT(schema_->FindFieldByName("A.KEY", false),
              ::testing::Optional(*exp_map_key_));
  ASSERT_THAT(schema_->FindFieldByName("A.VALUE", false),
              ::testing::Optional(*exp_map_value_));
  ASSERT_THAT(schema_->FindFieldByName("A.KEY.KEY", false),
              ::testing::Optional(*exp_key_struct_key_));
  ASSERT_THAT(schema_->FindFieldByName("A.KEY.VALUE", false),
              ::testing::Optional(*exp_key_struct_value_));
  ASSERT_THAT(schema_->FindFieldByName("A.KEY.VALUE.INNER_KEY", false),
              ::testing::Optional(*exp_inner_key_key_));
  ASSERT_THAT(schema_->FindFieldByName("A.KEY.VALUE.INNER_VALUE", false),
              ::testing::Optional(*exp_inner_key_value_));
  ASSERT_THAT(schema_->FindFieldByName("A.VALUE.KEY", false),
              ::testing::Optional(*exp_value_struct_key_));
  ASSERT_THAT(schema_->FindFieldByName("A.VALUE.VALUE", false),
              ::testing::Optional(*exp_value_struct_value_));
  ASSERT_THAT(schema_->FindFieldByName("A.VALUE.VALUE.INNER_K", false),
              ::testing::Optional(*exp_inner_value_k_));
  ASSERT_THAT(schema_->FindFieldByName("A.VALUE.VALUE.INNER_V", false),
              ::testing::Optional(*exp_inner_value_v_));
}

TEST_F(ComplexMapStructShortNameTest, TestInvalidPaths) {
  ASSERT_THAT(schema_->FindFieldByName("a.invalid"), ::testing::Optional(std::nullopt));
  ASSERT_THAT(schema_->FindFieldByName("a.key.invalid"),
              ::testing::Optional(std::nullopt));
  ASSERT_THAT(schema_->FindFieldByName("a.value.invalid"),
              ::testing::Optional(std::nullopt));
  ASSERT_THAT(schema_->FindFieldByName("A.KEY.VALUE.INVALID", false),
              ::testing::Optional(std::nullopt));
}

TEST(SchemaTest, DuplicatePathErrorCaseSensitive) {
  auto nested_b = std::make_unique<iceberg::SchemaField>(2, "b", iceberg::int32(), false);
  auto nested_struct =
      std::make_shared<iceberg::StructType>(std::vector<iceberg::SchemaField>{*nested_b});
  auto a = std::make_unique<iceberg::SchemaField>(1, "a", nested_struct, false);
  auto duplicate_ab =
      std::make_unique<iceberg::SchemaField>(3, "a.b", iceberg::int32(), false);
  auto schema = std::make_unique<iceberg::Schema>(
      std::vector<iceberg::SchemaField>{*a, *duplicate_ab}, 1);

  auto result = schema->FindFieldByName("a.b", /*case_sensitive=*/true);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().kind, iceberg::ErrorKind::kInvalidSchema);
  EXPECT_THAT(result.error().message,
              ::testing::HasSubstr("Duplicate path found: a.b, prev id: 2, curr id: 3"));
}

TEST(SchemaTest, DuplicatePathErrorCaseInsensitive) {
  auto nested_b = std::make_unique<iceberg::SchemaField>(2, "B", iceberg::int32(), false);
  auto nested_struct =
      std::make_shared<iceberg::StructType>(std::vector<iceberg::SchemaField>{*nested_b});
  auto a = std::make_unique<iceberg::SchemaField>(1, "A", nested_struct, false);
  auto duplicate_ab =
      std::make_unique<iceberg::SchemaField>(3, "a.b", iceberg::int32(), false);
  auto schema = std::make_unique<iceberg::Schema>(
      std::vector<iceberg::SchemaField>{*a, *duplicate_ab}, 1);

  auto result = schema->FindFieldByName("A.B", /*case_sensitive=*/false);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().kind, iceberg::ErrorKind::kInvalidSchema);
  EXPECT_THAT(result.error().message,
              ::testing::HasSubstr("Duplicate path found: a.b, prev id: 2, curr id: 3"));
}

TEST(SchemaTest, NestedDuplicateFieldIdError) {
  // Outer struct with field ID 1
  auto outer_field =
      std::make_unique<iceberg::SchemaField>(1, "outer", iceberg::int32(), true);

  // Inner struct with duplicate field ID 1
  auto inner_field =
      std::make_unique<iceberg::SchemaField>(1, "inner", iceberg::string(), true);
  auto inner_struct = std::make_shared<iceberg::StructType>(
      std::vector<iceberg::SchemaField>{*inner_field});

  // Nested field with inner struct
  auto nested_field =
      std::make_unique<iceberg::SchemaField>(2, "nested", inner_struct, true);

  // Schema with outer and nested fields
  auto schema = std::make_unique<iceberg::Schema>(
      std::vector<iceberg::SchemaField>{*outer_field, *nested_field}, 1);

  // Attempt to find a field, which should trigger duplicate ID detection
  auto result = schema->FindFieldById(1);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().kind, iceberg::ErrorKind::kInvalidSchema);
  EXPECT_THAT(result.error().message,
              ::testing::HasSubstr("Duplicate field id found: 1"));
}

namespace {

struct TestFieldFactory {
  static std::unique_ptr<iceberg::SchemaField> Id(int32_t field_id = 1,
                                                  bool optional = false) {
    return std::make_unique<iceberg::SchemaField>(field_id, "id", iceberg::int32(),
                                                  optional);
  }

  static std::unique_ptr<iceberg::SchemaField> Name(int32_t field_id = 2,
                                                    bool optional = true) {
    return std::make_unique<iceberg::SchemaField>(field_id, "name", iceberg::string(),
                                                  optional);
  }

  static std::unique_ptr<iceberg::SchemaField> Age(int32_t field_id = 3,
                                                   bool optional = true) {
    return std::make_unique<iceberg::SchemaField>(field_id, "age", iceberg::int32(),
                                                  optional);
  }

  static std::unique_ptr<iceberg::SchemaField> Email(int32_t field_id = 4,
                                                     bool optional = true) {
    return std::make_unique<iceberg::SchemaField>(field_id, "email", iceberg::string(),
                                                  optional);
  }

  static std::unique_ptr<iceberg::SchemaField> Street(int32_t field_id = 11,
                                                      bool optional = true) {
    return std::make_unique<iceberg::SchemaField>(field_id, "street", iceberg::string(),
                                                  optional);
  }

  static std::unique_ptr<iceberg::SchemaField> City(int32_t field_id = 12,
                                                    bool optional = true) {
    return std::make_unique<iceberg::SchemaField>(field_id, "city", iceberg::string(),
                                                  optional);
  }

  static std::unique_ptr<iceberg::SchemaField> Zip(int32_t field_id = 13,
                                                   bool optional = true) {
    return std::make_unique<iceberg::SchemaField>(field_id, "zip", iceberg::int32(),
                                                  optional);
  }

  static std::unique_ptr<iceberg::SchemaField> Theme(int32_t field_id = 21,
                                                     bool optional = true) {
    return std::make_unique<iceberg::SchemaField>(field_id, "theme", iceberg::string(),
                                                  optional);
  }

  static std::unique_ptr<iceberg::SchemaField> Key(int32_t field_id = 31,
                                                   bool optional = false) {
    return std::make_unique<iceberg::SchemaField>(field_id, "key", iceberg::int32(),
                                                  optional);
  }

  static std::unique_ptr<iceberg::SchemaField> Value(int32_t field_id = 32,
                                                     bool optional = false) {
    return std::make_unique<iceberg::SchemaField>(field_id, "value", iceberg::string(),
                                                  optional);
  }

  static std::unique_ptr<iceberg::SchemaField> Element(int32_t field_id = 41,
                                                       bool optional = false) {
    return std::make_unique<iceberg::SchemaField>(field_id, "element", iceberg::string(),
                                                  optional);
  }
};

struct TestSchemaFactory {
  static std::shared_ptr<iceberg::Schema> BasicSchema(int32_t schema_id = 100) {
    auto field1 = TestFieldFactory::Id(1, false);
    auto field2 = TestFieldFactory::Name(2, true);
    auto field3 = TestFieldFactory::Age(3, true);
    auto field4 = TestFieldFactory::Email(4, true);
    return std::make_shared<iceberg::Schema>(
        std::vector<iceberg::SchemaField>{*field1, *field2, *field3, *field4}, schema_id);
  }

  static std::shared_ptr<iceberg::Schema> AddressSchema(int32_t schema_id = 200) {
    auto street = TestFieldFactory::Street(11, true);
    auto city = TestFieldFactory::City(12, true);
    auto zip = TestFieldFactory::Zip(13, true);
    auto address_type = std::make_shared<iceberg::StructType>(
        std::vector<iceberg::SchemaField>{*street, *city, *zip});
    auto address_field =
        std::make_unique<iceberg::SchemaField>(14, "address", address_type, true);
    auto id_field = TestFieldFactory::Id(15, false);
    auto name_field = TestFieldFactory::Name(16, true);
    return std::make_shared<iceberg::Schema>(
        std::vector<iceberg::SchemaField>{*id_field, *name_field, *address_field},
        schema_id);
  }

  static std::shared_ptr<iceberg::Schema> NestedUserSchema(int32_t schema_id = 300) {
    auto street = TestFieldFactory::Street(11, true);
    auto city = TestFieldFactory::City(12, true);
    auto address_type = std::make_shared<iceberg::StructType>(
        std::vector<iceberg::SchemaField>{*street, *city});
    auto address_field =
        std::make_unique<iceberg::SchemaField>(13, "address", address_type, true);

    auto name_field = TestFieldFactory::Name(14, true);
    auto user_type = std::make_shared<iceberg::StructType>(
        std::vector<iceberg::SchemaField>{*name_field, *address_field});
    auto user_field = std::make_unique<iceberg::SchemaField>(15, "user", user_type, true);

    auto id_field = TestFieldFactory::Id(16, false);
    return std::make_shared<iceberg::Schema>(
        std::vector<iceberg::SchemaField>{*id_field, *user_field}, schema_id);
  }

  static std::shared_ptr<iceberg::Schema> MultiLevelSchema(int32_t schema_id = 400) {
    auto name_field = TestFieldFactory::Name(21, true);
    auto age_field = TestFieldFactory::Age(22, true);
    auto profile_type = std::make_shared<iceberg::StructType>(
        std::vector<iceberg::SchemaField>{*name_field, *age_field});
    auto profile_field =
        std::make_unique<iceberg::SchemaField>(23, "profile", profile_type, true);

    auto theme_field = TestFieldFactory::Theme(24, true);
    auto settings_type = std::make_shared<iceberg::StructType>(
        std::vector<iceberg::SchemaField>{*theme_field});
    auto settings_field =
        std::make_unique<iceberg::SchemaField>(25, "settings", settings_type, true);

    auto user_type = std::make_shared<iceberg::StructType>(
        std::vector<iceberg::SchemaField>{*profile_field, *settings_field});
    auto user_field = std::make_unique<iceberg::SchemaField>(26, "user", user_type, true);

    auto id_field = TestFieldFactory::Id(27, false);
    return std::make_shared<iceberg::Schema>(
        std::vector<iceberg::SchemaField>{*id_field, *user_field}, schema_id);
  }

  static std::shared_ptr<iceberg::Schema> ListSchema(int32_t schema_id = 500) {
    auto list_element = TestFieldFactory::Element(41, false);
    auto list_type = std::make_shared<iceberg::ListType>(*list_element);
    auto tags_field = std::make_unique<iceberg::SchemaField>(42, "tags", list_type, true);

    auto name_field = TestFieldFactory::Name(43, true);
    auto age_field = TestFieldFactory::Age(44, true);
    auto user_type = std::make_shared<iceberg::StructType>(
        std::vector<iceberg::SchemaField>{*name_field, *age_field});
    auto user_field = std::make_unique<iceberg::SchemaField>(45, "user", user_type, true);

    auto id_field = TestFieldFactory::Id(46, false);
    return std::make_shared<iceberg::Schema>(
        std::vector<iceberg::SchemaField>{*id_field, *tags_field, *user_field},
        schema_id);
  }

  static std::shared_ptr<iceberg::Schema> MapSchema(int32_t schema_id = 100) {
    auto key_field = TestFieldFactory::Key(31, false);
    auto value_field = TestFieldFactory::Value(32, false);
    auto map_type = std::make_shared<iceberg::MapType>(*key_field, *value_field);
    auto map_field =
        std::make_unique<iceberg::SchemaField>(33, "map_field", map_type, true);
    return std::make_shared<iceberg::Schema>(
        std::vector<iceberg::SchemaField>{*map_field}, schema_id);
  }

  static std::shared_ptr<iceberg::Schema> ListWithStructElementSchema(
      int32_t schema_id = 100) {
    auto name_field = TestFieldFactory::Name(51, true);
    auto age_field = TestFieldFactory::Age(52, true);
    auto struct_type = std::make_shared<iceberg::StructType>(
        std::vector<iceberg::SchemaField>{*name_field, *age_field});
    auto element_field =
        std::make_unique<iceberg::SchemaField>(53, "element", struct_type, false);
    auto list_type = std::make_shared<iceberg::ListType>(*element_field);
    auto list_field =
        std::make_unique<iceberg::SchemaField>(54, "list_field", list_type, true);
    return std::make_shared<iceberg::Schema>(
        std::vector<iceberg::SchemaField>{*list_field}, schema_id);
  }

  static std::shared_ptr<iceberg::Schema> ListOfMapSchema(int32_t schema_id = 100) {
    auto map_key_field = TestFieldFactory::Key(61, false);
    auto map_value_field1 = std::make_unique<iceberg::SchemaField>(
        62, "value_name", iceberg::string(), false);
    auto map_value_field2 =
        std::make_unique<iceberg::SchemaField>(63, "value_age", iceberg::int32(), false);
    auto map_value_struct = std::make_shared<iceberg::StructType>(
        std::vector<iceberg::SchemaField>{*map_value_field1, *map_value_field2});
    auto map_value_field =
        std::make_unique<iceberg::SchemaField>(64, "value", map_value_struct, false);
    auto map_type = std::make_shared<iceberg::MapType>(*map_key_field, *map_value_field);
    auto list_element =
        std::make_unique<iceberg::SchemaField>(65, "element", map_type, false);
    auto list_type = std::make_shared<iceberg::ListType>(*list_element);
    auto list_field =
        std::make_unique<iceberg::SchemaField>(66, "list_field", list_type, true);
    return std::make_shared<iceberg::Schema>(
        std::vector<iceberg::SchemaField>{*list_field}, schema_id);
  }

  static std::shared_ptr<iceberg::Schema> ComplexMapSchema(int32_t schema_id = 100) {
    auto key_field1 =
        std::make_unique<iceberg::SchemaField>(71, "key_id", iceberg::int32(), false);
    auto key_field2 =
        std::make_unique<iceberg::SchemaField>(72, "key_name", iceberg::string(), false);
    auto key_struct = std::make_shared<iceberg::StructType>(
        std::vector<iceberg::SchemaField>{*key_field1, *key_field2});
    auto key_field = std::make_unique<iceberg::SchemaField>(73, "key", key_struct, false);

    auto value_field1 =
        std::make_unique<iceberg::SchemaField>(74, "value_id", iceberg::int32(), false);
    auto value_field2 = std::make_unique<iceberg::SchemaField>(75, "value_name",
                                                               iceberg::string(), false);
    auto value_struct = std::make_shared<iceberg::StructType>(
        std::vector<iceberg::SchemaField>{*value_field1, *value_field2});
    auto value_field =
        std::make_unique<iceberg::SchemaField>(76, "value", value_struct, false);

    auto map_type = std::make_shared<iceberg::MapType>(*key_field, *value_field);
    auto map_field =
        std::make_unique<iceberg::SchemaField>(77, "map_field", map_type, true);
    return std::make_shared<iceberg::Schema>(
        std::vector<iceberg::SchemaField>{*map_field}, schema_id);
  }
};

}  // namespace

struct SelectTestParam {
  std::string test_name;
  std::function<std::shared_ptr<iceberg::Schema>()> create_schema;
  std::vector<std::string> select_fields;
  std::function<std::shared_ptr<iceberg::Schema>()> expected_schema;
  bool should_succeed;
  std::string expected_error_message;
  bool case_sensitive = true;
};

class SelectParamTest : public ::testing::TestWithParam<SelectTestParam> {};

TEST_P(SelectParamTest, SelectFields) {
  const auto& param = GetParam();
  auto input_schema = param.create_schema();
  auto result = input_schema->Select(param.select_fields, param.case_sensitive);

  if (param.should_succeed) {
    ASSERT_TRUE(result.has_value());
    auto actual_schema = std::move(result.value());
    auto expected_schema = param.expected_schema();
    ASSERT_EQ(*actual_schema, *expected_schema);
  } else {
    ASSERT_FALSE(result.has_value());
    ASSERT_THAT(result, iceberg::IsError(iceberg::ErrorKind::kInvalidArgument));
    ASSERT_THAT(result, iceberg::HasErrorMessage(param.expected_error_message));
  }
}

INSTANTIATE_TEST_SUITE_P(
    SelectTestCases, SelectParamTest,
    ::testing::Values(
        SelectTestParam{
            .test_name = "SelectAllColumns",
            .create_schema = []() { return TestSchemaFactory::BasicSchema(100); },
            .select_fields = {"*"},
            .expected_schema = []() { return TestSchemaFactory::BasicSchema(100); },
            .should_succeed = true},

        SelectTestParam{
            .test_name = "SelectSingleField",
            .create_schema = []() { return TestSchemaFactory::BasicSchema(100); },
            .select_fields = {"name"},
            .expected_schema =
                []() {
                  auto field = TestFieldFactory::Name(2, true);
                  return std::make_shared<iceberg::Schema>(
                      std::vector<iceberg::SchemaField>{*field}, 100);
                },
            .should_succeed = true},

        SelectTestParam{
            .test_name = "SelectMultipleFields",
            .create_schema = []() { return TestSchemaFactory::BasicSchema(100); },
            .select_fields = {"id", "name", "age"},
            .expected_schema =
                []() {
                  auto field1 = TestFieldFactory::Id(1, false);
                  auto field2 = TestFieldFactory::Name(2, true);
                  auto field3 = TestFieldFactory::Age(3, true);
                  return std::make_shared<iceberg::Schema>(
                      std::vector<iceberg::SchemaField>{*field1, *field2, *field3}, 100);
                },
            .should_succeed = true},

        SelectTestParam{
            .test_name = "SelectNonExistentField",
            .create_schema = []() { return TestSchemaFactory::BasicSchema(100); },
            .select_fields = {"nonexistent"},
            .expected_schema =
                []() {
                  return std::make_shared<iceberg::Schema>(
                      std::vector<iceberg::SchemaField>{}, 100);
                },
            .should_succeed = true},

        SelectTestParam{
            .test_name = "SelectCaseSensitive",
            .create_schema = []() { return TestSchemaFactory::BasicSchema(100); },
            .select_fields = {"Name"},  // case-sensitive
            .expected_schema =
                []() {
                  return std::make_shared<iceberg::Schema>(
                      std::vector<iceberg::SchemaField>{}, 100);
                },
            .should_succeed = true},

        SelectTestParam{
            .test_name = "SelectCaseInsensitive",
            .create_schema = []() { return TestSchemaFactory::BasicSchema(100); },
            .select_fields = {"Name"},  // case-insensitive
            .expected_schema =
                []() {
                  auto field = TestFieldFactory::Name(2, true);
                  return std::make_shared<iceberg::Schema>(
                      std::vector<iceberg::SchemaField>{*field}, 100);
                },
            .should_succeed = true,
            .case_sensitive = false}));

INSTANTIATE_TEST_SUITE_P(
    SelectNestedTestCases, SelectParamTest,
    ::testing::Values(
        SelectTestParam{
            .test_name = "SelectTopLevelFields",
            .create_schema = []() { return TestSchemaFactory::AddressSchema(200); },
            .select_fields = {"id", "name"},
            .expected_schema =
                []() {
                  auto field1 = TestFieldFactory::Id(15, false);
                  auto field2 = TestFieldFactory::Name(16, true);
                  return std::make_shared<iceberg::Schema>(
                      std::vector<iceberg::SchemaField>{*field1, *field2}, 200);
                },
            .should_succeed = true},

        SelectTestParam{
            .test_name = "SelectNestedField",
            .create_schema = []() { return TestSchemaFactory::AddressSchema(200); },
            .select_fields = {"address.street"},
            .expected_schema =
                []() {
                  auto street_field = TestFieldFactory::Street(11, true);
                  auto address_type = std::make_shared<iceberg::StructType>(
                      std::vector<iceberg::SchemaField>{*street_field});
                  auto address_field = std::make_unique<iceberg::SchemaField>(
                      14, "address", address_type, true);
                  return std::make_shared<iceberg::Schema>(
                      std::vector<iceberg::SchemaField>{*address_field}, 200);
                },
            .should_succeed = true}));

INSTANTIATE_TEST_SUITE_P(
    SelectMultiLevelTestCases, SelectParamTest,
    ::testing::Values(
        SelectTestParam{
            .test_name = "SelectTopLevelAndNestedFields",
            .create_schema = []() { return TestSchemaFactory::NestedUserSchema(300); },
            .select_fields = {"id", "user.name", "user.address.street"},
            .expected_schema =
                []() {
                  auto id_field = TestFieldFactory::Id(16, false);
                  auto name_field = TestFieldFactory::Name(14, true);
                  auto street_field = TestFieldFactory::Street(11, true);
                  auto address_type = std::make_shared<iceberg::StructType>(
                      std::vector<iceberg::SchemaField>{*street_field});
                  auto address_field = std::make_unique<iceberg::SchemaField>(
                      13, "address", address_type, true);
                  auto user_type = std::make_shared<iceberg::StructType>(
                      std::vector<iceberg::SchemaField>{*name_field, *address_field});
                  auto user_field =
                      std::make_unique<iceberg::SchemaField>(15, "user", user_type, true);
                  return std::make_shared<iceberg::Schema>(
                      std::vector<iceberg::SchemaField>{*id_field, *user_field}, 300);
                },
            .should_succeed = true},

        SelectTestParam{
            .test_name = "SelectNestedFieldsAtDifferentLevels",
            .create_schema = []() { return TestSchemaFactory::MultiLevelSchema(400); },
            .select_fields = {"user.profile.name", "user.settings.theme"},
            .expected_schema =
                []() {
                  auto name_field = TestFieldFactory::Name(21, true);
                  auto profile_type = std::make_shared<iceberg::StructType>(
                      std::vector<iceberg::SchemaField>{*name_field});
                  auto profile_field = std::make_unique<iceberg::SchemaField>(
                      23, "profile", profile_type, true);

                  auto theme_field = TestFieldFactory::Theme(24, true);
                  auto settings_type = std::make_shared<iceberg::StructType>(
                      std::vector<iceberg::SchemaField>{*theme_field});
                  auto settings_field = std::make_unique<iceberg::SchemaField>(
                      25, "settings", settings_type, true);

                  auto user_type = std::make_shared<iceberg::StructType>(
                      std::vector<iceberg::SchemaField>{*profile_field, *settings_field});
                  auto user_field =
                      std::make_unique<iceberg::SchemaField>(26, "user", user_type, true);
                  return std::make_shared<iceberg::Schema>(
                      std::vector<iceberg::SchemaField>{*user_field}, 400);
                },
            .should_succeed = true},

        SelectTestParam{
            .test_name = "SelectListAndNestedFields",
            .create_schema = []() { return TestSchemaFactory::ListSchema(500); },
            .select_fields = {"id", "user.name"},
            .expected_schema =
                []() {
                  auto id_field = TestFieldFactory::Id(46, false);
                  auto name_field = TestFieldFactory::Name(43, true);
                  auto user_type = std::make_shared<iceberg::StructType>(
                      std::vector<iceberg::SchemaField>{*name_field});
                  auto user_field =
                      std::make_unique<iceberg::SchemaField>(45, "user", user_type, true);
                  return std::make_shared<iceberg::Schema>(
                      std::vector<iceberg::SchemaField>{*id_field, *user_field}, 500);
                },
            .should_succeed = true}));

struct ProjectTestParam {
  std::string test_name;
  std::function<std::shared_ptr<iceberg::Schema>()> create_schema;
  std::unordered_set<int32_t> selected_ids;
  std::function<std::shared_ptr<iceberg::Schema>()> expected_schema;
  bool should_succeed;
  std::string expected_error_message;
};

class ProjectParamTest : public ::testing::TestWithParam<ProjectTestParam> {};

TEST_P(ProjectParamTest, ProjectFields) {
  const auto& param = GetParam();
  auto input_schema = param.create_schema();
  auto selected_ids = param.selected_ids;
  auto result = input_schema->Project(selected_ids);

  if (param.should_succeed) {
    ASSERT_TRUE(result.has_value());
    auto actual_schema = std::move(result.value());
    auto expected_schema = param.expected_schema();
    ASSERT_EQ(*actual_schema, *expected_schema);
  } else {
    ASSERT_FALSE(result.has_value());
    ASSERT_THAT(result, iceberg::IsError(iceberg::ErrorKind::kInvalidArgument));
    ASSERT_THAT(result, iceberg::HasErrorMessage(param.expected_error_message));
  }
}

INSTANTIATE_TEST_SUITE_P(
    ProjectTestCases, ProjectParamTest,
    ::testing::Values(
        ProjectTestParam{
            .test_name = "ProjectAllFields",
            .create_schema = []() { return TestSchemaFactory::BasicSchema(100); },
            .selected_ids = {1, 2, 3, 4},
            .expected_schema = []() { return TestSchemaFactory::BasicSchema(100); },
            .should_succeed = true},

        ProjectTestParam{
            .test_name = "ProjectSingleField",
            .create_schema = []() { return TestSchemaFactory::BasicSchema(100); },
            .selected_ids = {2},
            .expected_schema =
                []() {
                  auto field = TestFieldFactory::Name(2, true);
                  return std::make_shared<iceberg::Schema>(
                      std::vector<iceberg::SchemaField>{*field}, 100);
                },
            .should_succeed = true},

        ProjectTestParam{
            .test_name = "ProjectNonExistentFieldId",
            .create_schema = []() { return TestSchemaFactory::BasicSchema(100); },
            .selected_ids = {999},
            .expected_schema =
                []() {
                  return std::make_shared<iceberg::Schema>(
                      std::vector<iceberg::SchemaField>{}, 100);
                },
            .should_succeed = true},

        ProjectTestParam{
            .test_name = "ProjectEmptySelection",
            .create_schema = []() { return TestSchemaFactory::BasicSchema(100); },
            .selected_ids = {},
            .expected_schema =
                []() {
                  return std::make_shared<iceberg::Schema>(
                      std::vector<iceberg::SchemaField>{}, 100);
                },
            .should_succeed = true}));

INSTANTIATE_TEST_SUITE_P(
    ProjectNestedTestCases, ProjectParamTest,
    ::testing::Values(ProjectTestParam{
        .test_name = "ProjectNestedStructField",
        .create_schema = []() { return TestSchemaFactory::AddressSchema(200); },
        .selected_ids = {11},
        .expected_schema =
            []() {
              auto street_field = TestFieldFactory::Street(11, true);
              auto address_type = std::make_shared<iceberg::StructType>(
                  std::vector<iceberg::SchemaField>{*street_field});
              auto address_field = std::make_unique<iceberg::SchemaField>(
                  14, "address", address_type, true);
              return std::make_shared<iceberg::Schema>(
                  std::vector<iceberg::SchemaField>{*address_field}, 200);
            },
        .should_succeed = true}));

INSTANTIATE_TEST_SUITE_P(
    ProjectMultiLevelTestCases, ProjectParamTest,
    ::testing::Values(
        ProjectTestParam{
            .test_name = "ProjectTopLevelAndNestedFields",
            .create_schema = []() { return TestSchemaFactory::NestedUserSchema(300); },
            .selected_ids = {16, 14, 11},
            .expected_schema =
                []() {
                  auto id_field = TestFieldFactory::Id(16, false);
                  auto name_field = TestFieldFactory::Name(14, true);
                  auto street_field = TestFieldFactory::Street(11, true);
                  auto address_type = std::make_shared<iceberg::StructType>(
                      std::vector<iceberg::SchemaField>{*street_field});
                  auto address_field = std::make_unique<iceberg::SchemaField>(
                      13, "address", address_type, true);
                  auto user_type = std::make_shared<iceberg::StructType>(
                      std::vector<iceberg::SchemaField>{*name_field, *address_field});
                  auto user_field =
                      std::make_unique<iceberg::SchemaField>(15, "user", user_type, true);
                  return std::make_shared<iceberg::Schema>(
                      std::vector<iceberg::SchemaField>{*id_field, *user_field}, 300);
                },
            .should_succeed = true},

        ProjectTestParam{
            .test_name = "ProjectNestedFieldsAtDifferentLevels",
            .create_schema = []() { return TestSchemaFactory::MultiLevelSchema(400); },
            .selected_ids = {21, 24},
            .expected_schema =
                []() {
                  auto name_field = TestFieldFactory::Name(21, true);
                  auto profile_type = std::make_shared<iceberg::StructType>(
                      std::vector<iceberg::SchemaField>{*name_field});
                  auto profile_field = std::make_unique<iceberg::SchemaField>(
                      23, "profile", profile_type, true);

                  auto theme_field = TestFieldFactory::Theme(24, true);
                  auto settings_type = std::make_shared<iceberg::StructType>(
                      std::vector<iceberg::SchemaField>{*theme_field});
                  auto settings_field = std::make_unique<iceberg::SchemaField>(
                      25, "settings", settings_type, true);

                  auto user_type = std::make_shared<iceberg::StructType>(
                      std::vector<iceberg::SchemaField>{*profile_field, *settings_field});
                  auto user_field =
                      std::make_unique<iceberg::SchemaField>(26, "user", user_type, true);
                  return std::make_shared<iceberg::Schema>(
                      std::vector<iceberg::SchemaField>{*user_field}, 400);
                },
            .should_succeed = true},

        ProjectTestParam{
            .test_name = "ProjectListAndNestedFields",
            .create_schema = []() { return TestSchemaFactory::ListSchema(500); },
            .selected_ids = {46, 43},
            .expected_schema =
                []() {
                  auto id_field = TestFieldFactory::Id(46, false);
                  auto name_field = TestFieldFactory::Name(43, true);
                  auto user_type = std::make_shared<iceberg::StructType>(
                      std::vector<iceberg::SchemaField>{*name_field});
                  auto user_field =
                      std::make_unique<iceberg::SchemaField>(45, "user", user_type, true);
                  return std::make_shared<iceberg::Schema>(
                      std::vector<iceberg::SchemaField>{*id_field, *user_field}, 500);
                },
            .should_succeed = true}));

INSTANTIATE_TEST_SUITE_P(
    ProjectMapErrorTestCases, ProjectParamTest,
    ::testing::Values(ProjectTestParam{
        .test_name = "ProjectMapWithOnlyKey",
        .create_schema = []() { return TestSchemaFactory::MapSchema(100); },
        .selected_ids = {31},  // Only select key field, not value field
        .expected_schema = []() { return nullptr; },
        .should_succeed = false,
        .expected_error_message = "Cannot project Map without value field"}));

INSTANTIATE_TEST_SUITE_P(
    ProjectListAndMapTestCases, ProjectParamTest,
    ::testing::Values(
        ProjectTestParam{
            .test_name = "ProjectListElement",
            .create_schema =
                []() { return TestSchemaFactory::ListWithStructElementSchema(100); },
            .selected_ids = {51},  // Only select name field from list element
            .expected_schema =
                []() {
                  auto name_field = TestFieldFactory::Name(51, true);
                  auto struct_type = std::make_shared<iceberg::StructType>(
                      std::vector<iceberg::SchemaField>{*name_field});
                  auto element_field = std::make_unique<iceberg::SchemaField>(
                      53, "element", struct_type, false);
                  auto list_type = std::make_shared<iceberg::ListType>(*element_field);
                  auto list_field = std::make_unique<iceberg::SchemaField>(
                      54, "list_field", list_type, true);
                  return std::make_shared<iceberg::Schema>(
                      std::vector<iceberg::SchemaField>{*list_field}, 100);
                },
            .should_succeed = true},

        ProjectTestParam{
            .test_name = "ProjectListOfMap",
            .create_schema = []() { return TestSchemaFactory::ListOfMapSchema(100); },
            .selected_ids = {61, 62},
            .expected_schema =
                []() {
                  auto map_key_field = TestFieldFactory::Key(61, false);
                  auto map_value_field1 = std::make_unique<iceberg::SchemaField>(
                      62, "value_name", iceberg::string(), false);
                  auto map_value_struct = std::make_shared<iceberg::StructType>(
                      std::vector<iceberg::SchemaField>{*map_value_field1});
                  auto map_value_field = std::make_unique<iceberg::SchemaField>(
                      64, "value", map_value_struct, false);
                  auto map_type = std::make_shared<iceberg::MapType>(*map_key_field,
                                                                     *map_value_field);
                  auto list_element = std::make_unique<iceberg::SchemaField>(
                      65, "element", map_type, false);
                  auto list_type = std::make_shared<iceberg::ListType>(*list_element);
                  auto list_field = std::make_unique<iceberg::SchemaField>(
                      66, "list_field", list_type, true);
                  return std::make_shared<iceberg::Schema>(
                      std::vector<iceberg::SchemaField>{*list_field}, 100);
                },
            .should_succeed = true},

        ProjectTestParam{
            .test_name = "ProjectMapKeyAndValue",
            .create_schema = []() { return TestSchemaFactory::ComplexMapSchema(100); },
            .selected_ids = {71, 74},
            .expected_schema =
                []() {
                  auto key_field1 = std::make_unique<iceberg::SchemaField>(
                      71, "key_id", iceberg::int32(), false);
                  auto key_struct = std::make_shared<iceberg::StructType>(
                      std::vector<iceberg::SchemaField>{*key_field1});
                  auto key_field = std::make_unique<iceberg::SchemaField>(
                      73, "key", key_struct, false);

                  auto value_field1 = std::make_unique<iceberg::SchemaField>(
                      74, "value_id", iceberg::int32(), false);
                  auto value_struct = std::make_shared<iceberg::StructType>(
                      std::vector<iceberg::SchemaField>{*value_field1});
                  auto value_field = std::make_unique<iceberg::SchemaField>(
                      76, "value", value_struct, false);

                  auto map_type =
                      std::make_shared<iceberg::MapType>(*key_field, *value_field);
                  auto map_field = std::make_unique<iceberg::SchemaField>(77, "map_field",
                                                                          map_type, true);
                  return std::make_shared<iceberg::Schema>(
                      std::vector<iceberg::SchemaField>{*map_field}, 100);
                },
            .should_succeed = true},

        ProjectTestParam{
            .test_name = "ProjectEmptyResult",
            .create_schema = []() { return TestSchemaFactory::BasicSchema(100); },
            .selected_ids = {999},  // Select non-existent field
            .expected_schema =
                []() {
                  return std::make_shared<iceberg::Schema>(
                      std::vector<iceberg::SchemaField>{}, 100);
                },
            .should_succeed = true}));
