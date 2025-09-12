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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/result.h"
#include "iceberg/schema_field.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep
#include "matchers.h"

template <typename... Args>
std::shared_ptr<iceberg::StructType> MakeStructType(Args... args) {
  return std::make_shared<iceberg::StructType>(
      std::vector<iceberg::SchemaField>{args...});
}

template <typename... Args>
std::shared_ptr<iceberg::Schema> MakeSchema(Args... args) {
  return std::make_shared<iceberg::Schema>(std::vector<iceberg::SchemaField>{args...},
                                           std::nullopt);
}

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

namespace TestFields {
iceberg::SchemaField Id() { return {1, "id", iceberg::int32(), true}; }
iceberg::SchemaField Name() { return {2, "name", iceberg::string(), false}; }
iceberg::SchemaField Age() { return {3, "age", iceberg::int32(), true}; }
iceberg::SchemaField Email() { return {4, "email", iceberg::string(), true}; }
iceberg::SchemaField Street() { return {11, "street", iceberg::string(), true}; }
iceberg::SchemaField City() { return {12, "city", iceberg::string(), true}; }
iceberg::SchemaField Zip() { return {13, "zip", iceberg::int32(), true}; }
iceberg::SchemaField Theme() { return {24, "theme", iceberg::string(), true}; }
iceberg::SchemaField Key() { return {31, "key", iceberg::int32(), false}; }
iceberg::SchemaField Value() { return {32, "value", iceberg::string(), false}; }
iceberg::SchemaField Element() { return {41, "element", iceberg::string(), false}; }

}  // namespace TestFields

struct TestSchemaFactory {
  static std::shared_ptr<iceberg::Schema> BasicSchema(int32_t schema_id = 100) {
    return MakeSchema(TestFields::Id(), TestFields::Name(), TestFields::Age(),
                      TestFields::Email());
  }

  static std::shared_ptr<iceberg::Schema> AddressSchema(int32_t schema_id = 200) {
    auto address_type =
        MakeStructType(TestFields::Street(), TestFields::City(), TestFields::Zip());
    auto address_field = iceberg::SchemaField{14, "address", address_type, true};
    return MakeSchema(TestFields::Id(), TestFields::Name(), address_field);
  }

  static std::shared_ptr<iceberg::Schema> NestedUserSchema(int32_t schema_id = 300) {
    auto address_type = MakeStructType(TestFields::Street(), TestFields::City());
    auto address_field = iceberg::SchemaField{16, "address", address_type, true};
    auto user_type = MakeStructType(TestFields::Name(), address_field);
    auto user_field = iceberg::SchemaField{17, "user", user_type, true};
    return MakeSchema(TestFields::Id(), user_field);
  }

  static std::shared_ptr<iceberg::Schema> MultiLevelSchema(int32_t schema_id = 400) {
    auto profile_type = MakeStructType(TestFields::Name(), TestFields::Age());
    auto profile_field = iceberg::SchemaField{23, "profile", profile_type, true};

    auto settings_type = MakeStructType(TestFields::Theme());
    auto settings_field = iceberg::SchemaField{25, "settings", settings_type, true};

    auto user_type = MakeStructType(profile_field, settings_field);
    auto user_field = iceberg::SchemaField{26, "user", user_type, true};

    return MakeSchema(TestFields::Id(), user_field);
  }

  static std::shared_ptr<iceberg::Schema> ListSchema(int32_t schema_id = 500) {
    auto list_type = std::make_shared<iceberg::ListType>(TestFields::Element());
    auto tags_field = iceberg::SchemaField{42, "tags", list_type, true};

    auto user_type = MakeStructType(TestFields::Name(), TestFields::Age());
    auto user_field = iceberg::SchemaField{45, "user", user_type, true};

    return MakeSchema(TestFields::Id(), tags_field, user_field);
  }

  static std::shared_ptr<iceberg::Schema> MapSchema(int32_t schema_id = 600) {
    auto map_type =
        std::make_shared<iceberg::MapType>(TestFields::Key(), TestFields::Value());
    auto map_field = iceberg::SchemaField{33, "map_field", map_type, true};
    return MakeSchema(map_field);
  }

  static std::shared_ptr<iceberg::Schema> ListWithStructElementSchema(
      int32_t schema_id = 700) {
    auto struct_type = MakeStructType(TestFields::Name(), TestFields::Age());
    auto element_field = iceberg::SchemaField{53, "element", struct_type, false};
    auto list_type = std::make_shared<iceberg::ListType>(element_field);
    auto list_field = iceberg::SchemaField{54, "list_field", list_type, true};
    return MakeSchema(list_field);
  }

  static std::shared_ptr<iceberg::Schema> ListOfMapSchema(int32_t schema_id = 800) {
    auto map_value_struct = MakeStructType(TestFields::Name(), TestFields::Age());
    auto map_value_field = iceberg::SchemaField{64, "value", map_value_struct, false};
    auto map_type =
        std::make_shared<iceberg::MapType>(TestFields::Key(), map_value_field);
    auto list_element = iceberg::SchemaField{65, "element", map_type, false};
    auto list_type = std::make_shared<iceberg::ListType>(list_element);
    auto list_field = iceberg::SchemaField{66, "list_field", list_type, true};
    return MakeSchema(list_field);
  }

  static std::shared_ptr<iceberg::Schema> ComplexMapSchema(int32_t schema_id = 900) {
    auto key_id_field = iceberg::SchemaField{71, "id", iceberg::int32(), false};
    auto key_name_field = iceberg::SchemaField{72, "name", iceberg::string(), false};
    auto key_struct = MakeStructType(key_id_field, key_name_field);
    auto key_field = iceberg::SchemaField{73, "key", key_struct, false};

    auto value_id_field = iceberg::SchemaField{74, "id", iceberg::int32(), false};
    auto value_name_field = iceberg::SchemaField{75, "name", iceberg::string(), false};
    auto value_struct = MakeStructType(value_id_field, value_name_field);
    auto value_field = iceberg::SchemaField{76, "value", value_struct, false};

    auto map_type = std::make_shared<iceberg::MapType>(key_field, value_field);
    auto map_field = iceberg::SchemaField{77, "map_field", map_type, true};
    return MakeSchema(map_field);
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

void CompareSchema(std::shared_ptr<iceberg::Schema> actual_schema,
                   std::shared_ptr<iceberg::Schema> expected_schema) {
  ASSERT_EQ(actual_schema->fields().size(), expected_schema->fields().size());
  for (int i = 0; i < actual_schema->fields().size(); i++) {
    ASSERT_EQ(actual_schema->fields()[i], expected_schema->fields()[i]);
  }
}

TEST_P(SelectParamTest, SelectFields) {
  const auto& param = GetParam();
  auto input_schema = param.create_schema();
  auto result = input_schema->Select(param.select_fields, param.case_sensitive);

  if (param.should_succeed) {
    ASSERT_TRUE(result.has_value());
    CompareSchema(std::move(result.value()), param.expected_schema());
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
            .expected_schema = []() { return MakeSchema(TestFields::Name()); },
            .should_succeed = true},

        SelectTestParam{
            .test_name = "SelectMultipleFields",
            .create_schema = []() { return TestSchemaFactory::BasicSchema(100); },
            .select_fields = {"id", "name", "age"},
            .expected_schema =
                []() {
                  return MakeSchema(TestFields::Id(), TestFields::Name(),
                                    TestFields::Age());
                },
            .should_succeed = true},

        SelectTestParam{
            .test_name = "SelectNonExistentField",
            .create_schema = []() { return TestSchemaFactory::BasicSchema(100); },
            .select_fields = {"nonexistent"},
            .expected_schema = []() { return MakeSchema(); },
            .should_succeed = true},

        SelectTestParam{
            .test_name = "SelectCaseSensitive",
            .create_schema = []() { return TestSchemaFactory::BasicSchema(100); },
            .select_fields = {"Name"},  // case-sensitive
            .expected_schema = []() { return MakeSchema(); },
            .should_succeed = true},

        SelectTestParam{
            .test_name = "SelectCaseInsensitive",
            .create_schema = []() { return TestSchemaFactory::BasicSchema(100); },
            .select_fields = {"Name"},  // case-insensitive
            .expected_schema = []() { return MakeSchema(TestFields::Name()); },
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
                []() { return MakeSchema(TestFields::Id(), TestFields::Name()); },
            .should_succeed = true},

        SelectTestParam{
            .test_name = "SelectNestedField",
            .create_schema = []() { return TestSchemaFactory::AddressSchema(200); },
            .select_fields = {"address.street"},
            .expected_schema =
                []() {
                  auto address_type = MakeStructType(TestFields::Street());
                  auto address_field =
                      iceberg::SchemaField{14, "address", address_type, true};
                  return MakeSchema(address_field);
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
                  auto address_type = MakeStructType(TestFields::Street());
                  auto address_field =
                      iceberg::SchemaField{16, "address", address_type, true};
                  auto user_type = MakeStructType(TestFields::Name(), address_field);
                  auto user_field = iceberg::SchemaField{17, "user", user_type, true};
                  return MakeSchema(TestFields::Id(), user_field);
                },
            .should_succeed = true},

        SelectTestParam{
            .test_name = "SelectNestedFieldsAtDifferentLevels",
            .create_schema = []() { return TestSchemaFactory::MultiLevelSchema(400); },
            .select_fields = {"user.profile.name", "user.settings.theme"},
            .expected_schema =
                []() {
                  auto profile_type = MakeStructType(TestFields::Name());
                  auto profile_field =
                      iceberg::SchemaField{23, "profile", profile_type, true};

                  auto settings_type = MakeStructType(TestFields::Theme());
                  auto settings_field =
                      iceberg::SchemaField{25, "settings", settings_type, true};

                  auto user_type = MakeStructType(profile_field, settings_field);
                  auto user_field = iceberg::SchemaField{26, "user", user_type, true};
                  return MakeSchema(user_field);
                },
            .should_succeed = true},

        SelectTestParam{
            .test_name = "SelectListAndNestedFields",
            .create_schema = []() { return TestSchemaFactory::ListSchema(500); },
            .select_fields = {"id", "user.name"},
            .expected_schema =
                []() {
                  auto user_type = MakeStructType(TestFields::Name());
                  auto user_field = iceberg::SchemaField{45, "user", user_type, true};
                  return MakeSchema(TestFields::Id(), user_field);
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
    CompareSchema(std::move(result.value()), param.expected_schema());
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
            .expected_schema = []() { return MakeSchema(TestFields::Name()); },
            .should_succeed = true},

        ProjectTestParam{
            .test_name = "ProjectNonExistentFieldId",
            .create_schema = []() { return TestSchemaFactory::BasicSchema(100); },
            .selected_ids = {999},
            .expected_schema = []() { return MakeSchema(); },
            .should_succeed = true},

        ProjectTestParam{
            .test_name = "ProjectEmptySelection",
            .create_schema = []() { return TestSchemaFactory::BasicSchema(100); },
            .selected_ids = {},
            .expected_schema = []() { return MakeSchema(); },
            .should_succeed = true}));

INSTANTIATE_TEST_SUITE_P(
    ProjectNestedTestCases, ProjectParamTest,
    ::testing::Values(ProjectTestParam{
        .test_name = "ProjectNestedStructField",
        .create_schema = []() { return TestSchemaFactory::AddressSchema(200); },
        .selected_ids = {11},
        .expected_schema =
            []() {
              auto address_type = MakeStructType(TestFields::Street());
              auto address_field =
                  iceberg::SchemaField{14, "address", address_type, true};
              return MakeSchema(address_field);
            },
        .should_succeed = true}));

INSTANTIATE_TEST_SUITE_P(
    ProjectMultiLevelTestCases, ProjectParamTest,
    ::testing::Values(
        ProjectTestParam{
            .test_name = "ProjectTopLevelAndNestedFields",
            .create_schema = []() { return TestSchemaFactory::NestedUserSchema(300); },
            .selected_ids = {1, 2, 11},
            .expected_schema =
                []() {
                  auto address_type = MakeStructType(TestFields::Street());
                  auto address_field =
                      iceberg::SchemaField{16, "address", address_type, true};
                  auto user_type = MakeStructType(TestFields::Name(), address_field);
                  auto user_field = iceberg::SchemaField{17, "user", user_type, true};
                  return MakeSchema(TestFields::Id(), user_field);
                },
            .should_succeed = true},

        ProjectTestParam{
            .test_name = "ProjectNestedFieldsAtDifferentLevels",
            .create_schema = []() { return TestSchemaFactory::MultiLevelSchema(400); },
            .selected_ids = {2, 24},
            .expected_schema =
                []() {
                  auto profile_type = MakeStructType(TestFields::Name());
                  auto profile_field =
                      iceberg::SchemaField{23, "profile", profile_type, true};

                  auto settings_type = MakeStructType(TestFields::Theme());
                  auto settings_field =
                      iceberg::SchemaField{25, "settings", settings_type, true};

                  auto user_type = MakeStructType(profile_field, settings_field);
                  auto user_field = iceberg::SchemaField{26, "user", user_type, true};
                  return MakeSchema(user_field);
                },
            .should_succeed = true},

        ProjectTestParam{
            .test_name = "ProjectListAndNestedFields",
            .create_schema = []() { return TestSchemaFactory::ListSchema(500); },
            .selected_ids = {1, 2},
            .expected_schema =
                []() {
                  auto user_type = MakeStructType(TestFields::Name());
                  auto user_field = iceberg::SchemaField{45, "user", user_type, true};
                  return MakeSchema(TestFields::Id(), user_field);
                },
            .should_succeed = true}));

INSTANTIATE_TEST_SUITE_P(
    ProjectMapErrorTestCases, ProjectParamTest,
    ::testing::Values(ProjectTestParam{
        .test_name = "ProjectMapWithOnlyKey",
        .create_schema = []() { return TestSchemaFactory::MapSchema(600); },
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
                []() { return TestSchemaFactory::ListWithStructElementSchema(700); },
            .selected_ids = {2},  // Only select name field from list element
            .expected_schema =
                []() {
                  auto struct_type = MakeStructType(TestFields::Name());
                  auto element_field =
                      iceberg::SchemaField{53, "element", struct_type, false};
                  auto list_type = std::make_shared<iceberg::ListType>(element_field);
                  auto list_field =
                      iceberg::SchemaField{54, "list_field", list_type, true};
                  return MakeSchema(list_field);
                },
            .should_succeed = true},

        ProjectTestParam{
            .test_name = "ProjectListOfMap",
            .create_schema = []() { return TestSchemaFactory::ListOfMapSchema(800); },
            .selected_ids = {2, 3},
            .expected_schema =
                []() {
                  auto map_value_struct =
                      MakeStructType(TestFields::Name(), TestFields::Age());
                  auto map_value_field =
                      iceberg::SchemaField{64, "value", map_value_struct, false};
                  auto map_type = std::make_shared<iceberg::MapType>(TestFields::Key(),
                                                                     map_value_field);
                  auto list_element =
                      iceberg::SchemaField{65, "element", map_type, false};
                  auto list_type = std::make_shared<iceberg::ListType>(list_element);
                  auto list_field =
                      iceberg::SchemaField{66, "list_field", list_type, true};
                  return MakeSchema(list_field);
                },
            .should_succeed = true},

        ProjectTestParam{
            .test_name = "ProjectMapKeyAndValue",
            .create_schema = []() { return TestSchemaFactory::ComplexMapSchema(900); },
            .selected_ids = {71, 74},
            .expected_schema =
                []() {
                  auto key_id_field =
                      iceberg::SchemaField{71, "id", iceberg::int32(), false};
                  auto key_struct = MakeStructType(key_id_field);
                  auto key_field = iceberg::SchemaField{73, "key", key_struct, false};

                  auto value_id_field =
                      iceberg::SchemaField{74, "id", iceberg::int32(), false};
                  auto value_struct = MakeStructType(value_id_field);
                  auto value_field =
                      iceberg::SchemaField{76, "value", value_struct, false};

                  auto map_type =
                      std::make_shared<iceberg::MapType>(key_field, value_field);
                  auto map_field = iceberg::SchemaField{77, "map_field", map_type, true};
                  return MakeSchema(map_field);
                },
            .should_succeed = true},

        ProjectTestParam{
            .test_name = "ProjectEmptyResult",
            .create_schema = []() { return TestSchemaFactory::BasicSchema(100); },
            .selected_ids = {999},  // Select non-existent field
            .expected_schema = []() { return MakeSchema(); },
            .should_succeed = true}));
