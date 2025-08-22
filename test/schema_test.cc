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

#include "iceberg/schema_field.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep

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
    ASSERT_EQ(std::nullopt, schema.GetFieldByIndex(2));
    ASSERT_EQ(std::nullopt, schema.GetFieldByIndex(-1));
    ASSERT_EQ(std::nullopt, schema.GetFieldByName("element"));
  }
  ASSERT_THAT(
      []() {
        iceberg::SchemaField field1(5, "foo", iceberg::int32(), true);
        iceberg::SchemaField field2(5, "bar", iceberg::string(), true);
        iceberg::Schema schema({field1, field2}, 100);
      },
      ::testing::ThrowsMessage<std::runtime_error>(
          ::testing::HasSubstr("duplicate field ID 5")));
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

class NestedTypeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    field1_ = iceberg::SchemaField(1, "Foo", iceberg::int32(), true);
    field2_ = iceberg::SchemaField(2, "Bar", iceberg::string(), true);
    field3_ = iceberg::SchemaField(3, "Foobar", iceberg::int32(), true);

    iceberg::StructType structtype =
        iceberg::StructType(std::vector<iceberg::SchemaField>{
            field1_.value(), field2_.value(), field3_.value()});

    auto listype = iceberg::ListType(iceberg::SchemaField::MakeRequired(
        4, "element", std::make_shared<iceberg::StructType>(structtype)));

    auto maptype =
        iceberg::MapType(iceberg::SchemaField::MakeRequired(5, "key", iceberg::int32()),
                         iceberg::SchemaField::MakeRequired(
                             6, "value", std::make_shared<iceberg::ListType>(listype)));

    field4_ = iceberg::SchemaField::MakeRequired(
        4, "element", std::make_shared<iceberg::StructType>(structtype));
    field5_ = iceberg::SchemaField::MakeRequired(5, "key", iceberg::int32());
    field6_ = iceberg::SchemaField::MakeRequired(
        6, "value", std::make_shared<iceberg::ListType>(listype));
    field7_ = iceberg::SchemaField::MakeRequired(
        7, "Value", std::make_shared<iceberg::MapType>(maptype));

    schema_ = std::make_shared<iceberg::Schema>(
        std::vector<iceberg::SchemaField>{field7_.value()}, 1);
  }

  std::shared_ptr<iceberg::Schema> schema_;
  std::optional<iceberg::SchemaField> field1_;
  std::optional<iceberg::SchemaField> field2_;
  std::optional<iceberg::SchemaField> field3_;
  std::optional<iceberg::SchemaField> field4_;
  std::optional<iceberg::SchemaField> field5_;
  std::optional<iceberg::SchemaField> field6_;
  std::optional<iceberg::SchemaField> field7_;
};

TEST_F(NestedTypeTest, TestFindById) {
  ASSERT_THAT(schema_->FindFieldById(7), ::testing::Optional(field7_));
  ASSERT_THAT(schema_->FindFieldById(6), ::testing::Optional(field6_));
  ASSERT_THAT(schema_->FindFieldById(5), ::testing::Optional(field5_));
  ASSERT_THAT(schema_->FindFieldById(4), ::testing::Optional(field4_));
  ASSERT_THAT(schema_->FindFieldById(3), ::testing::Optional(field3_));
  ASSERT_THAT(schema_->FindFieldById(2), ::testing::Optional(field2_));
  ASSERT_THAT(schema_->FindFieldById(1), ::testing::Optional(field1_));

  ASSERT_THAT(schema_->FindFieldById(10), ::testing::Optional(std::nullopt));
}

TEST_F(NestedTypeTest, TestFindByName) {
  ASSERT_THAT(schema_->FindFieldByName("Value"), ::testing::Optional(field7_));
  ASSERT_THAT(schema_->FindFieldByName("Value.value"), ::testing::Optional(field6_));
  ASSERT_THAT(schema_->FindFieldByName("Value.key"), ::testing::Optional(field5_));
  ASSERT_THAT(schema_->FindFieldByName("Value.value.element"),
              ::testing::Optional(field4_));
  ASSERT_THAT(schema_->FindFieldByName("Value.value.element.Foobar"),
              ::testing::Optional(field3_));
  ASSERT_THAT(schema_->FindFieldByName("Value.value.element.Bar"),
              ::testing::Optional(field2_));
  ASSERT_THAT(schema_->FindFieldByName("Value.value.element.Foo"),
              ::testing::Optional(field1_));

  ASSERT_THAT(schema_->FindFieldByName("Value.value.element.FoO"),
              ::testing::Optional(std::nullopt));
}

TEST_F(NestedTypeTest, TestFindByNameCaseInsensitive) {
  ASSERT_THAT(schema_->FindFieldByName("vALue", false), ::testing::Optional(field7_));
  ASSERT_THAT(schema_->FindFieldByName("vALue.VALUE", false),
              ::testing::Optional(field6_));
  ASSERT_THAT(schema_->FindFieldByName("valUe.kEy", false), ::testing::Optional(field5_));
  ASSERT_THAT(schema_->FindFieldByName("vaLue.vAlue.elEment", false),
              ::testing::Optional(field4_));
  ASSERT_THAT(schema_->FindFieldByName("vaLue.vAlue.eLement.fOObar", false),
              ::testing::Optional(field3_));
  ASSERT_THAT(schema_->FindFieldByName("valUe.vaLUe.elemEnt.Bar", false),
              ::testing::Optional(field2_));
  ASSERT_THAT(schema_->FindFieldByName("valUe.valUe.ELEMENT.FOO", false),
              ::testing::Optional(field1_));
  ASSERT_THAT(schema_->FindFieldByName("valUe.valUe.ELEMENT.FO", false),
              ::testing::Optional(std::nullopt));
}

TEST_F(NestedTypeTest, TestFindByShortNameCaseInsensitive) {
  ASSERT_THAT(schema_->FindFieldByName("vaLue.value.FOO", false),
              ::testing::Optional(field1_));
  ASSERT_THAT(schema_->FindFieldByName("Value.value.Bar", false),
              ::testing::Optional(field2_));
  ASSERT_THAT(schema_->FindFieldByName("Value.value.FooBAR", false),
              ::testing::Optional(field3_));
  ASSERT_THAT(schema_->FindFieldByName("Value.value.FooBAR.a", false),
              ::testing::Optional(std::nullopt));
}

class NestType2Test : public ::testing::Test {
 protected:
  void SetUp() override {
    field1_ = iceberg::SchemaField(1, "Foo", iceberg::int32(), true);
    field2_ = iceberg::SchemaField(2, "Bar", iceberg::string(), true);
    field3_ = iceberg::SchemaField(3, "Foobar", iceberg::int32(), true);

    iceberg::StructType structtype =
        iceberg::StructType({field1_.value(), field2_.value(), field3_.value()});

    field4_ = iceberg::SchemaField::MakeRequired(
        4, "element", std::make_shared<iceberg::StructType>(structtype));
    auto listype = iceberg::ListType(field4_.value());

    iceberg::StructType structtype2 = iceberg::StructType(
        {iceberg::SchemaField::MakeRequired(5, "First_child", iceberg::int32()),
         iceberg::SchemaField::MakeRequired(
             6, "Second_child", std::make_shared<iceberg::ListType>(listype))});

    auto maptype = iceberg::MapType(
        iceberg::SchemaField::MakeRequired(7, "key", iceberg::int32()),
        iceberg::SchemaField::MakeRequired(
            8, "value", std::make_shared<iceberg::StructType>(structtype2)));

    field5_ = iceberg::SchemaField::MakeRequired(5, "First_child", iceberg::int32());
    field6_ = iceberg::SchemaField::MakeRequired(
        6, "Second_child", std::make_shared<iceberg::ListType>(listype));
    field7_ = iceberg::SchemaField::MakeRequired(7, "key", iceberg::int32());
    field8_ = iceberg::SchemaField::MakeRequired(
        8, "value", std::make_shared<iceberg::StructType>(structtype2));
    field9_ = iceberg::SchemaField::MakeRequired(
        9, "Map", std::make_shared<iceberg::MapType>(maptype));

    schema_ = std::make_shared<iceberg::Schema>(
        std::vector<iceberg::SchemaField>{field9_.value()}, 1);
  }

  std::shared_ptr<iceberg::Schema> schema_;
  std::optional<iceberg::SchemaField> field1_;
  std::optional<iceberg::SchemaField> field2_;
  std::optional<iceberg::SchemaField> field3_;
  std::optional<iceberg::SchemaField> field4_;
  std::optional<iceberg::SchemaField> field5_;
  std::optional<iceberg::SchemaField> field6_;
  std::optional<iceberg::SchemaField> field7_;
  std::optional<iceberg::SchemaField> field8_;
  std::optional<iceberg::SchemaField> field9_;
};

TEST_F(NestType2Test, TestFindById) {
  ASSERT_THAT(schema_->FindFieldById(9), ::testing::Optional(field9_));
  ASSERT_THAT(schema_->FindFieldById(8), ::testing::Optional(field8_));
  ASSERT_THAT(schema_->FindFieldById(7), ::testing::Optional(field7_));
  ASSERT_THAT(schema_->FindFieldById(6), ::testing::Optional(field6_));
  ASSERT_THAT(schema_->FindFieldById(5), ::testing::Optional(field5_));
  ASSERT_THAT(schema_->FindFieldById(4), ::testing::Optional(field4_));
  ASSERT_THAT(schema_->FindFieldById(3), ::testing::Optional(field3_));
  ASSERT_THAT(schema_->FindFieldById(2), ::testing::Optional(field2_));
  ASSERT_THAT(schema_->FindFieldById(1), ::testing::Optional(field1_));

  ASSERT_THAT(schema_->FindFieldById(0), ::testing::Optional(std::nullopt));
}

TEST_F(NestType2Test, TestFindByName) {
  ASSERT_THAT(schema_->FindFieldByName("Map"), ::testing::Optional(field9_));
  ASSERT_THAT(schema_->FindFieldByName("Map.value"), ::testing::Optional(field8_));
  ASSERT_THAT(schema_->FindFieldByName("Map.key"), ::testing::Optional(field7_));
  ASSERT_THAT(schema_->FindFieldByName("Map.value.Second_child"),
              ::testing::Optional(field6_));
  ASSERT_THAT(schema_->FindFieldByName("Map.value.First_child"),
              ::testing::Optional(field5_));
  ASSERT_THAT(schema_->FindFieldByName("Map.value.Second_child.element"),
              ::testing::Optional(field4_));
  ASSERT_THAT(schema_->FindFieldByName("Map.value.Second_child.element.Foobar"),
              ::testing::Optional(field3_));
  ASSERT_THAT(schema_->FindFieldByName("Map.value.Second_child.element.Bar"),
              ::testing::Optional(field2_));
  ASSERT_THAT(schema_->FindFieldByName("Map.value.Second_child.element.Foo"),
              ::testing::Optional(field1_));
  ASSERT_THAT(schema_->FindFieldByName("Map.value.Second_child.element.Fooo"),
              ::testing::Optional(std::nullopt));
}

TEST_F(NestType2Test, TestFindByNameCaseInsensitive) {
  ASSERT_THAT(schema_->FindFieldByName("map", false), ::testing::Optional(field9_));
  ASSERT_THAT(schema_->FindFieldByName("map.vALUE", false), ::testing::Optional(field8_));
  ASSERT_THAT(schema_->FindFieldByName("map.Key", false), ::testing::Optional(field7_));
  ASSERT_THAT(schema_->FindFieldByName("map.Value.second_Child", false),
              ::testing::Optional(field6_));
  ASSERT_THAT(schema_->FindFieldByName("map.Value.first_chIld", false),
              ::testing::Optional(field5_));
  ASSERT_THAT(schema_->FindFieldByName("map.Value.second_child.Element", false),
              ::testing::Optional(field4_));
  ASSERT_THAT(schema_->FindFieldByName("map.Value.second_child.Element.foobar", false),
              ::testing::Optional(field3_));
  ASSERT_THAT(schema_->FindFieldByName("map.VaLue.second_child.Element.bar", false),
              ::testing::Optional(field2_));
  ASSERT_THAT(schema_->FindFieldByName("map.value.Second_child.Element.foo", false),
              ::testing::Optional(field1_));
  ASSERT_THAT(schema_->FindFieldByName("map.value.Second_child.Element.fooo", false),
              ::testing::Optional(std::nullopt));
}

TEST_F(NestType2Test, TestFindByShortName) {
  ASSERT_THAT(schema_->FindFieldByName("Map.Second_child"), ::testing::Optional(field6_));
  ASSERT_THAT(schema_->FindFieldByName("Map.First_child"), ::testing::Optional(field5_));
  ASSERT_THAT(schema_->FindFieldByName("Map.Second_child.Foobar"),
              ::testing::Optional(field3_));
  ASSERT_THAT(schema_->FindFieldByName("Map.Second_child.Bar"),
              ::testing::Optional(field2_));
  ASSERT_THAT(schema_->FindFieldByName("Map.Second_child.Foo"),
              ::testing::Optional(field1_));
  ASSERT_THAT(schema_->FindFieldByName("Map.Second_child.aaa"),
              ::testing::Optional(std::nullopt));
}

TEST_F(NestType2Test, TestFindByShortNameCaseInsensitive) {
  ASSERT_THAT(schema_->FindFieldByName("map.second_child", false),
              ::testing::Optional(field6_));
  ASSERT_THAT(schema_->FindFieldByName("map.first_child", false),
              ::testing::Optional(field5_));
  ASSERT_THAT(schema_->FindFieldByName("map.second_child.foobar", false),
              ::testing::Optional(field3_));
  ASSERT_THAT(schema_->FindFieldByName("map.second_child.bar", false),
              ::testing::Optional(field2_));
  ASSERT_THAT(schema_->FindFieldByName("map.second_child.foo", false),
              ::testing::Optional(field1_));
  ASSERT_THAT(schema_->FindFieldByName("Map.Second_child.aaa", false),
              ::testing::Optional(std::nullopt));
}

TEST_F(NestType2Test, TestMapKey) {
  auto field1_ = iceberg::SchemaField(1, "Foo", iceberg::int32(), true);
  auto field2_ = iceberg::SchemaField(2, "Bar", iceberg::string(), true);
  auto field3_ = iceberg::SchemaField(3, "Foobar", iceberg::int32(), true);

  iceberg::StructType structtype = iceberg::StructType({field1_, field2_, field3_});

  auto field4_ = iceberg::SchemaField::MakeRequired(
      4, "element", std::make_shared<iceberg::StructType>(structtype));

  iceberg::MapType maptype =
      iceberg::MapType(iceberg::SchemaField::MakeRequired(
                           5, "key", std::make_shared<iceberg::StructType>(structtype)),
                       iceberg::SchemaField::MakeRequired(6, "value", iceberg::int32()));

  auto field5_ = iceberg::SchemaField::MakeRequired(
      5, "key", std::make_shared<iceberg::StructType>(structtype));
  auto field6_ = iceberg::SchemaField::MakeRequired(6, "value", iceberg::int32());

  auto field7_ = iceberg::SchemaField::MakeRequired(
      7, "Map", std::make_shared<iceberg::MapType>(maptype));

  iceberg::Schema schema({field7_}, 1);

  ASSERT_THAT(schema.FindFieldByName("Map.key.Foo"), ::testing::Optional(field1_));
  ASSERT_THAT(schema.FindFieldByName("Map.Foo"), ::testing::Optional(std::nullopt));
}
