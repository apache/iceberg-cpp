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

#include "iceberg/schema_field.h"

#include <format>
#include <memory>

#include <gtest/gtest.h>

#include "iceberg/type.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep

namespace iceberg {

TEST(SchemaFieldTest, Basics) {
  {
    SchemaField field(1, "foo", int32(), false);
    EXPECT_EQ(1, field.field_id());
    EXPECT_EQ("foo", field.name());
    EXPECT_EQ(TypeId::kInt, field.type()->type_id());
    EXPECT_FALSE(field.optional());
    EXPECT_EQ("foo (1): int (required)", field.ToString());
    EXPECT_EQ("foo (1): int (required)", std::format("{}", field));
  }
  {
    SchemaField field =
        SchemaField::MakeOptional(2, "foo bar", std::make_shared<FixedType>(10));
    EXPECT_EQ(2, field.field_id());
    EXPECT_EQ("foo bar", field.name());
    EXPECT_EQ(FixedType(10), *field.type());
    EXPECT_TRUE(field.optional());
    EXPECT_EQ("foo bar (2): fixed(10) (optional)", field.ToString());
    EXPECT_EQ("foo bar (2): fixed(10) (optional)", std::format("{}", field));
  }
  {
    SchemaField field =
        SchemaField::MakeRequired(2, "foo bar", std::make_shared<FixedType>(10));
    EXPECT_EQ(2, field.field_id());
    EXPECT_EQ("foo bar", field.name());
    EXPECT_EQ(FixedType(10), *field.type());
    EXPECT_FALSE(field.optional());
    EXPECT_EQ("foo bar (2): fixed(10) (required)", field.ToString());
    EXPECT_EQ("foo bar (2): fixed(10) (required)", std::format("{}", field));
  }
}

TEST(SchemaFieldTest, Equality) {
  SchemaField field1(1, "foo", int32(), false);
  SchemaField field2(2, "foo", int32(), false);
  SchemaField field3(1, "bar", int32(), false);
  SchemaField field4(1, "foo", int64(), false);
  SchemaField field5(1, "foo", int32(), true);
  SchemaField field6(1, "foo", int32(), false);

  ASSERT_EQ(field1, field1);
  ASSERT_NE(field1, field2);
  ASSERT_NE(field2, field1);
  ASSERT_NE(field1, field3);
  ASSERT_NE(field3, field2);
  ASSERT_NE(field1, field4);
  ASSERT_NE(field4, field1);
  ASSERT_NE(field1, field5);
  ASSERT_NE(field5, field1);
  ASSERT_EQ(field1, field6);
  ASSERT_EQ(field6, field1);
}

TEST(SchemaFieldTest, WithDoc) {
  {
    SchemaField field(/*field_id=*/1, /*name=*/"foo", int32(),
                      /*optional=*/false, /*doc=*/"Field documentation");
    EXPECT_EQ(1, field.field_id());
    EXPECT_EQ("foo", field.name());
    EXPECT_EQ(TypeId::kInt, field.type()->type_id());
    EXPECT_FALSE(field.optional());
    EXPECT_EQ("Field documentation", field.doc());
    EXPECT_EQ("foo (1): int (required) - Field documentation", field.ToString());
  }
  {
    SchemaField field = SchemaField::MakeOptional(
        /*field_id=*/2, /*name=*/"bar",
        /*type=*/std::make_shared<FixedType>(10),
        /*doc=*/"Field with 10 bytes");
    EXPECT_EQ(2, field.field_id());
    EXPECT_EQ("bar", field.name());
    EXPECT_EQ(FixedType(10), *field.type());
    EXPECT_TRUE(field.optional());
    EXPECT_EQ("Field with 10 bytes", field.doc());
    EXPECT_EQ("bar (2): fixed(10) (optional) - Field with 10 bytes", field.ToString());
  }
}

}  // namespace iceberg
