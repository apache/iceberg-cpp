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
#include <string>

#include <gtest/gtest.h>

#include "gtest/gtest.h"
#include "iceberg/util/formatter.h"

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

const static TypeTestCase kPrimitiveTypes[] = {
    {
        .name = "boolean",
        .type = std::make_shared<iceberg::BooleanType>(),
        .type_id = iceberg::TypeId::kBoolean,
        .primitive = true,
        .repr = "boolean",
    },
    {
        .name = "int32",
        .type = std::make_shared<iceberg::Int32Type>(),
        .type_id = iceberg::TypeId::kInt32,
        .primitive = true,
        .repr = "int32",
    },
    {
        .name = "int64",
        .type = std::make_shared<iceberg::Int64Type>(),
        .type_id = iceberg::TypeId::kInt64,
        .primitive = true,
        .repr = "int64",
    },
    {
        .name = "float32",
        .type = std::make_shared<iceberg::Float32Type>(),
        .type_id = iceberg::TypeId::kFloat32,
        .primitive = true,
        .repr = "float32",
    },
    {
        .name = "float64",
        .type = std::make_shared<iceberg::Float64Type>(),
        .type_id = iceberg::TypeId::kFloat64,
        .primitive = true,
        .repr = "float64",
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
};

const static TypeTestCase kNestedTypes[] = {
    {
        .name = "list_int",
        .type = std::make_shared<iceberg::ListType>(
            1, std::make_shared<iceberg::Int32Type>(), true),
        .type_id = iceberg::TypeId::kList,
        .primitive = false,
        .repr = "list<element (1): int32>",
    },
    {
        .name = "list_list_int",
        .type = std::make_shared<iceberg::ListType>(
            1,
            std::make_shared<iceberg::ListType>(2, std::make_shared<iceberg::Int32Type>(),
                                                true),
            false),
        .type_id = iceberg::TypeId::kList,
        .primitive = false,
        .repr = "list<element (1): list<element (2): int32> (required)>",
    },
    {
        .name = "map_int_string",
        .type = std::make_shared<iceberg::MapType>(
            iceberg::SchemaField::MakeRequired(1, "key",
                                               std::make_shared<iceberg::Int64Type>()),
            iceberg::SchemaField::MakeRequired(2, "value",
                                               std::make_shared<iceberg::StringType>())),
        .type_id = iceberg::TypeId::kMap,
        .primitive = false,
        .repr = "map<key (1): int64 (required): value (2): string (required)>",
    },
    {
        .name = "struct",
        .type = std::make_shared<iceberg::StructType>(std::vector<iceberg::SchemaField>{
            iceberg::SchemaField::MakeRequired(1, "foo",
                                               std::make_shared<iceberg::Int64Type>()),
            iceberg::SchemaField::MakeOptional(2, "bar",
                                               std::make_shared<iceberg::StringType>()),
        }),
        .type_id = iceberg::TypeId::kStruct,
        .primitive = false,
        .repr = R"(struct<
  foo (1): int64 (required)
  bar (2): string
>)",
    },
};

INSTANTIATE_TEST_SUITE_P(Primitive, TypeTest, ::testing::ValuesIn(kPrimitiveTypes),
                         TypeTestCaseToString);

INSTANTIATE_TEST_SUITE_P(Nested, TypeTest, ::testing::ValuesIn(kNestedTypes),
                         TypeTestCaseToString);
