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
#include <limits>
#include <memory>

#include <gtest/gtest.h>

#include "iceberg/expression/literal.h"
#include "iceberg/result.h"
#include "iceberg/test/matchers.h"
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

TEST(SchemaFieldTest, WithFieldMetadata) {
  auto initial_default = std::make_shared<const Literal>(Literal::Int(1));
  auto write_default = std::make_shared<const Literal>(Literal::Int(2));
  SchemaField field(1, "foo", int32(), false, "doc", initial_default, write_default);

  auto renamed = field.WithName("bar");
  EXPECT_EQ(renamed.name(), "bar");
  EXPECT_EQ(renamed.field_id(), field.field_id());
  EXPECT_EQ(renamed.type(), field.type());
  EXPECT_EQ(renamed.doc(), field.doc());
  EXPECT_EQ(renamed.initial_default(), field.initial_default());
  EXPECT_EQ(renamed.write_default(), field.write_default());

  auto retyped = field.WithType(int64());
  EXPECT_EQ(retyped.type(), int64());
  EXPECT_EQ(retyped.name(), field.name());
  EXPECT_EQ(retyped.initial_default(), field.initial_default());
  EXPECT_EQ(retyped.write_default(), field.write_default());

  auto documented = field.WithDoc("new doc");
  EXPECT_EQ(documented.doc(), "new doc");
  EXPECT_EQ(documented.name(), field.name());
  EXPECT_EQ(documented.initial_default(), field.initial_default());
  EXPECT_EQ(documented.write_default(), field.write_default());

  auto new_initial = std::make_shared<const Literal>(Literal::Int(3));
  auto with_initial = field.WithInitialDefault(new_initial);
  EXPECT_EQ(with_initial.initial_default(), new_initial);
  EXPECT_EQ(with_initial.write_default(), field.write_default());

  auto new_write = std::make_shared<const Literal>(Literal::Int(4));
  auto with_write = field.WithWriteDefault(new_write);
  EXPECT_EQ(with_write.initial_default(), field.initial_default());
  EXPECT_EQ(with_write.write_default(), new_write);
  EXPECT_EQ(field.name(), "foo");
  EXPECT_EQ(*field.write_default(), Literal::Int(2));
}

TEST(SchemaFieldTest, CastDefaultValue) {
  {
    auto result = SchemaField::CastDefaultValue(Literal::Int(5), int64());
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), Literal::Long(5));
  }
  {
    auto result =
        SchemaField::CastDefaultValue(Literal::Decimal(1234, 9, 2), decimal(18, 2));
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), Literal::Decimal(1234, 18, 2));
  }
  {
    auto result =
        SchemaField::CastDefaultValue(Literal::Decimal(1234, 9, 3), decimal(18, 2));
    EXPECT_FALSE(result.has_value());
  }
  {
    auto result =
        SchemaField::CastDefaultValue(Literal::Decimal(1234567, 9, 2), decimal(4, 2));
    EXPECT_FALSE(result.has_value());
  }
}

TEST(SchemaFieldTest, ValidateRejectsNonFiniteFloatingDefault) {
  // NaN / infinity cannot be represented in JSON (the serializer emits `null`, which
  // reads back as an absent default), so a non-finite floating default must be rejected.
  SchemaField nan_field(/*field_id=*/1, /*name=*/"f", float32(),
                        /*optional=*/true, /*doc=*/"",
                        std::make_shared<const Literal>(
                            Literal::Float(std::numeric_limits<float>::quiet_NaN())));
  EXPECT_THAT(nan_field.Validate(), IsError(ErrorKind::kInvalidSchema));
  EXPECT_THAT(nan_field.Validate(), HasErrorMessage("must be finite"));

  SchemaField inf_field(/*field_id=*/2, /*name=*/"d", float64(),
                        /*optional=*/true, /*doc=*/"",
                        std::make_shared<const Literal>(
                            Literal::Double(std::numeric_limits<double>::infinity())));
  EXPECT_THAT(inf_field.Validate(), IsError(ErrorKind::kInvalidSchema));

  SchemaField neg_inf_field(/*field_id=*/3, /*name=*/"d2", float64(),
                            /*optional=*/true, /*doc=*/"",
                            std::make_shared<const Literal>(Literal::Double(
                                -std::numeric_limits<double>::infinity())));
  EXPECT_THAT(neg_inf_field.Validate(), IsError(ErrorKind::kInvalidSchema));
}

TEST(SchemaFieldTest, ValidateAcceptsFiniteFloatingDefault) {
  SchemaField field(/*field_id=*/1, /*name=*/"f", float32(),
                    /*optional=*/true, /*doc=*/"",
                    std::make_shared<const Literal>(Literal::Float(1.5f)));
  EXPECT_THAT(field.Validate(), IsOk());
}

}  // namespace iceberg
