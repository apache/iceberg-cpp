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

#include "iceberg/type.h"

#include <format>
#include <memory>
#include <stdexcept>
#include <string>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/exception.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep

struct TypeTestCase {
  /// Test case name, must be safe for Googletest (alphanumeric + underscore)
  std::string name;
  std::shared_ptr<iceberg::Type> type;
  iceberg::TypeId type_id;
  bool primitive;
  std::string repr;
};

std::string TypeTestCaseToString(const ::testing::TestParamInfo<TypeTestCase>& info) {
  return info.param.name;
}

class TypeTest : public ::testing::TestWithParam<TypeTestCase> {};

TEST_P(TypeTest, TypeId) {
  const auto& test_case = GetParam();
  ASSERT_EQ(test_case.type_id, test_case.type->type_id());
}

TEST_P(TypeTest, IsPrimitive) {
  const auto& test_case = GetParam();
  if (test_case.primitive) {
    ASSERT_TRUE(test_case.type->is_primitive());
    ASSERT_FALSE(test_case.type->is_nested());

    const auto* primitive =
        dynamic_cast<const iceberg::PrimitiveType*>(test_case.type.get());
    ASSERT_NE(nullptr, primitive);
  }
}

TEST_P(TypeTest, IsNested) {
  const auto& test_case = GetParam();
  if (!test_case.primitive) {
    ASSERT_FALSE(test_case.type->is_primitive());
    ASSERT_TRUE(test_case.type->is_nested());

    const auto* nested = dynamic_cast<const iceberg::NestedType*>(test_case.type.get());
    ASSERT_NE(nullptr, nested);
  }
}

TEST_P(TypeTest, ReflexiveEquality) {
  const auto& test_case = GetParam();
  ASSERT_EQ(*test_case.type, *test_case.type);
}

TEST_P(TypeTest, ToString) {
  const auto& test_case = GetParam();
  ASSERT_EQ(test_case.repr, test_case.type->ToString());
}

TEST_P(TypeTest, StdFormat) {
  const auto& test_case = GetParam();
  ASSERT_EQ(test_case.repr, std::format("{}", *test_case.type));
}

const static std::array<TypeTestCase, 16> kPrimitiveTypes = {{
    {
        .name = "boolean",
        .type = std::make_shared<iceberg::BooleanType>(),
        .type_id = iceberg::TypeId::kBoolean,
        .primitive = true,
        .repr = "boolean",
    },
    {
        .name = "int",
        .type = std::make_shared<iceberg::IntType>(),
        .type_id = iceberg::TypeId::kInt,
        .primitive = true,
        .repr = "int",
    },
    {
        .name = "long",
        .type = std::make_shared<iceberg::LongType>(),
        .type_id = iceberg::TypeId::kLong,
        .primitive = true,
        .repr = "long",
    },
    {
        .name = "float",
        .type = std::make_shared<iceberg::FloatType>(),
        .type_id = iceberg::TypeId::kFloat,
        .primitive = true,
        .repr = "float",
    },
    {
        .name = "double",
        .type = std::make_shared<iceberg::DoubleType>(),
        .type_id = iceberg::TypeId::kDouble,
        .primitive = true,
        .repr = "double",
    },
    {
        .name = "decimal9_2",
        .type = std::make_shared<iceberg::DecimalType>(9, 2),
        .type_id = iceberg::TypeId::kDecimal,
        .primitive = true,
        .repr = "decimal(9, 2)",
    },
    {
        .name = "decimal38_10",
        .type = std::make_shared<iceberg::DecimalType>(38, 10),
        .type_id = iceberg::TypeId::kDecimal,
        .primitive = true,
        .repr = "decimal(38, 10)",
    },
    {
        .name = "date",
        .type = std::make_shared<iceberg::DateType>(),
        .type_id = iceberg::TypeId::kDate,
        .primitive = true,
        .repr = "date",
    },
    {
        .name = "time",
        .type = std::make_shared<iceberg::TimeType>(),
        .type_id = iceberg::TypeId::kTime,
        .primitive = true,
        .repr = "time",
    },
    {
        .name = "timestamp",
        .type = std::make_shared<iceberg::TimestampType>(),
        .type_id = iceberg::TypeId::kTimestamp,
        .primitive = true,
        .repr = "timestamp",
    },
    {
        .name = "timestamptz",
        .type = std::make_shared<iceberg::TimestampTzType>(),
        .type_id = iceberg::TypeId::kTimestampTz,
        .primitive = true,
        .repr = "timestamptz",
    },
    {
        .name = "binary",
        .type = std::make_shared<iceberg::BinaryType>(),
        .type_id = iceberg::TypeId::kBinary,
        .primitive = true,
        .repr = "binary",
    },
    {
        .name = "string",
        .type = std::make_shared<iceberg::StringType>(),
        .type_id = iceberg::TypeId::kString,
        .primitive = true,
        .repr = "string",
    },
    {
        .name = "fixed10",
        .type = std::make_shared<iceberg::FixedType>(10),
        .type_id = iceberg::TypeId::kFixed,
        .primitive = true,
        .repr = "fixed(10)",
    },
    {
        .name = "fixed255",
        .type = std::make_shared<iceberg::FixedType>(255),
        .type_id = iceberg::TypeId::kFixed,
        .primitive = true,
        .repr = "fixed(255)",
    },
    {
        .name = "uuid",
        .type = std::make_shared<iceberg::UuidType>(),
        .type_id = iceberg::TypeId::kUuid,
        .primitive = true,
        .repr = "uuid",
    },
}};

const static std::array<TypeTestCase, 4> kNestedTypes = {{
    {
        .name = "list_int",
        .type = std::make_shared<iceberg::ListType>(
            1, std::make_shared<iceberg::IntType>(), true),
        .type_id = iceberg::TypeId::kList,
        .primitive = false,
        .repr = "list<element (1): int (optional)>",
    },
    {
        .name = "list_list_int",
        .type = std::make_shared<iceberg::ListType>(
            1,
            std::make_shared<iceberg::ListType>(2, std::make_shared<iceberg::IntType>(),
                                                true),
            false),
        .type_id = iceberg::TypeId::kList,
        .primitive = false,
        .repr = "list<element (1): list<element (2): int (optional)> (required)>",
    },
    {
        .name = "map_int_string",
        .type = std::make_shared<iceberg::MapType>(
            iceberg::SchemaField::MakeRequired(1, "key",
                                               std::make_shared<iceberg::LongType>()),
            iceberg::SchemaField::MakeRequired(2, "value",
                                               std::make_shared<iceberg::StringType>())),
        .type_id = iceberg::TypeId::kMap,
        .primitive = false,
        .repr = "map<key (1): long (required): value (2): string (required)>",
    },
    {
        .name = "struct",
        .type = std::make_shared<iceberg::StructType>(std::vector<iceberg::SchemaField>{
            iceberg::SchemaField::MakeRequired(1, "foo",
                                               std::make_shared<iceberg::LongType>()),
            iceberg::SchemaField::MakeOptional(2, "bar",
                                               std::make_shared<iceberg::StringType>()),
        }),
        .type_id = iceberg::TypeId::kStruct,
        .primitive = false,
        .repr = R"(struct<
  foo (1): long (required)
  bar (2): string (optional)
>)",
    },
}};

INSTANTIATE_TEST_SUITE_P(Primitive, TypeTest, ::testing::ValuesIn(kPrimitiveTypes),
                         TypeTestCaseToString);

INSTANTIATE_TEST_SUITE_P(Nested, TypeTest, ::testing::ValuesIn(kNestedTypes),
                         TypeTestCaseToString);

TEST(TypeTest, Equality) {
  std::vector<std::shared_ptr<iceberg::Type>> alltypes;
  for (const auto& test_case : kPrimitiveTypes) {
    alltypes.push_back(test_case.type);
  }
  for (const auto& test_case : kNestedTypes) {
    alltypes.push_back(test_case.type);
  }

  for (size_t i = 0; i < alltypes.size(); i++) {
    for (size_t j = 0; j < alltypes.size(); j++) {
      SCOPED_TRACE(std::format("{} == {}", *alltypes[i], *alltypes[j]));

      if (i == j) {
        ASSERT_EQ(*alltypes[i], *alltypes[j]);
      } else {
        ASSERT_NE(*alltypes[i], *alltypes[j]);
      }
    }
  }
}

TEST(TypeTest, Decimal) {
  {
    iceberg::DecimalType decimal(38, 2);
    ASSERT_EQ(38, decimal.precision());
    ASSERT_EQ(2, decimal.scale());
  }
  {
    iceberg::DecimalType decimal(10, -10);
    ASSERT_EQ(10, decimal.precision());
    ASSERT_EQ(-10, decimal.scale());
  }
  ASSERT_THAT([]() { iceberg::DecimalType decimal(-1, 10); },
              ::testing::ThrowsMessage<iceberg::IcebergError>(
                  ::testing::HasSubstr("precision must be in [0, 38], was -1")));

  ASSERT_THAT([]() { iceberg::DecimalType decimal(39, 10); },
              ::testing::ThrowsMessage<iceberg::IcebergError>(
                  ::testing::HasSubstr("precision must be in [0, 38], was 39")));
}

TEST(TypeTest, Fixed) {
  {
    iceberg::FixedType fixed(0);
    ASSERT_EQ(0, fixed.length());
  }
  {
    iceberg::FixedType fixed(1);
    ASSERT_EQ(1, fixed.length());
  }
  {
    iceberg::FixedType fixed(127);
    ASSERT_EQ(127, fixed.length());
  }
  ASSERT_THAT([]() { iceberg::FixedType decimal(-1); },
              ::testing::ThrowsMessage<iceberg::IcebergError>(
                  ::testing::HasSubstr("length must be >= 0, was -1")));
}

TEST(TypeTest, List) {
  {
    iceberg::SchemaField field(5, "element", std::make_shared<iceberg::IntType>(), true);
    iceberg::ListType list(field);
    std::span<const iceberg::SchemaField> fields = list.fields();
    ASSERT_EQ(1, fields.size());
    ASSERT_EQ(field, fields[0]);
    ASSERT_THAT(list.GetFieldById(5), ::testing::Optional(field));
    ASSERT_THAT(list.GetFieldByIndex(0), ::testing::Optional(field));
    ASSERT_THAT(list.GetFieldByName("element"), ::testing::Optional(field));

    ASSERT_EQ(std::nullopt, list.GetFieldById(0));
    ASSERT_EQ(std::nullopt, list.GetFieldByIndex(1));
    ASSERT_EQ(std::nullopt, list.GetFieldByIndex(-1));
    ASSERT_EQ(std::nullopt, list.GetFieldByName("foo"));
  }
  ASSERT_THAT(
      []() {
        iceberg::ListType list(iceberg::SchemaField(
            1, "wrongname", std::make_shared<iceberg::BooleanType>(), true));
      },
      ::testing::ThrowsMessage<iceberg::IcebergError>(
          ::testing::HasSubstr("child field name should be 'element', was 'wrongname'")));
}

TEST(TypeTest, Map) {
  {
    iceberg::SchemaField key(5, "key", std::make_shared<iceberg::IntType>(), true);
    iceberg::SchemaField value(7, "value", std::make_shared<iceberg::StringType>(), true);
    iceberg::MapType map(key, value);
    std::span<const iceberg::SchemaField> fields = map.fields();
    ASSERT_EQ(2, fields.size());
    ASSERT_EQ(key, fields[0]);
    ASSERT_EQ(value, fields[1]);
    ASSERT_THAT(map.GetFieldById(5), ::testing::Optional(key));
    ASSERT_THAT(map.GetFieldById(7), ::testing::Optional(value));
    ASSERT_THAT(map.GetFieldByIndex(0), ::testing::Optional(key));
    ASSERT_THAT(map.GetFieldByIndex(1), ::testing::Optional(value));
    ASSERT_THAT(map.GetFieldByName("key"), ::testing::Optional(key));
    ASSERT_THAT(map.GetFieldByName("value"), ::testing::Optional(value));

    ASSERT_EQ(std::nullopt, map.GetFieldById(0));
    ASSERT_EQ(std::nullopt, map.GetFieldByIndex(2));
    ASSERT_EQ(std::nullopt, map.GetFieldByIndex(-1));
    ASSERT_EQ(std::nullopt, map.GetFieldByName("element"));
  }
  ASSERT_THAT(
      []() {
        iceberg::SchemaField key(5, "notkey", std::make_shared<iceberg::IntType>(), true);
        iceberg::SchemaField value(7, "value", std::make_shared<iceberg::StringType>(),
                                   true);
        iceberg::MapType map(key, value);
      },
      ::testing::ThrowsMessage<iceberg::IcebergError>(
          ::testing::HasSubstr("key field name should be 'key', was 'notkey'")));
  ASSERT_THAT(
      []() {
        iceberg::SchemaField key(5, "key", std::make_shared<iceberg::IntType>(), true);
        iceberg::SchemaField value(7, "notvalue", std::make_shared<iceberg::StringType>(),
                                   true);
        iceberg::MapType map(key, value);
      },
      ::testing::ThrowsMessage<iceberg::IcebergError>(
          ::testing::HasSubstr("value field name should be 'value', was 'notvalue'")));
}

TEST(TypeTest, Struct) {
  {
    iceberg::SchemaField field1(5, "foo", std::make_shared<iceberg::IntType>(), true);
    iceberg::SchemaField field2(7, "bar", std::make_shared<iceberg::StringType>(), true);
    iceberg::StructType struct_({field1, field2});
    std::span<const iceberg::SchemaField> fields = struct_.fields();
    ASSERT_EQ(2, fields.size());
    ASSERT_EQ(field1, fields[0]);
    ASSERT_EQ(field2, fields[1]);
    ASSERT_THAT(struct_.GetFieldById(5), ::testing::Optional(field1));
    ASSERT_THAT(struct_.GetFieldById(7), ::testing::Optional(field2));
    ASSERT_THAT(struct_.GetFieldByIndex(0), ::testing::Optional(field1));
    ASSERT_THAT(struct_.GetFieldByIndex(1), ::testing::Optional(field2));
    ASSERT_THAT(struct_.GetFieldByName("foo"), ::testing::Optional(field1));
    ASSERT_THAT(struct_.GetFieldByName("bar"), ::testing::Optional(field2));

    ASSERT_EQ(std::nullopt, struct_.GetFieldById(0));
    ASSERT_EQ(std::nullopt, struct_.GetFieldByIndex(2));
    ASSERT_EQ(std::nullopt, struct_.GetFieldByIndex(-1));
    ASSERT_EQ(std::nullopt, struct_.GetFieldByName("element"));
  }
  ASSERT_THAT(
      []() {
        iceberg::SchemaField field1(5, "foo", std::make_shared<iceberg::IntType>(), true);
        iceberg::SchemaField field2(5, "bar", std::make_shared<iceberg::StringType>(),
                                    true);
        iceberg::StructType struct_({field1, field2});
      },
      ::testing::ThrowsMessage<iceberg::IcebergError>(
          ::testing::HasSubstr("duplicate field ID 5")));
}
