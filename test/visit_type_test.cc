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

#include "iceberg/util/visit_type.h"

#include <sstream>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <iceberg/type.h>

#include "gmock/gmock.h"
#include "iceberg/result.h"
#include "matchers.h"

namespace iceberg {

namespace {

class TypeNameVisitor {
 public:
  Status Visit(const Type& type, std::ostringstream& oss) {
    oss << type.ToString();
    return {};
  }
};

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

}  // namespace

class TypeTest : public ::testing::TestWithParam<TypeTestCase> {};

INSTANTIATE_TEST_SUITE_P(Primitive, TypeTest, ::testing::ValuesIn(kPrimitiveTypes),
                         TypeTestCaseToString);

INSTANTIATE_TEST_SUITE_P(Nested, TypeTest, ::testing::ValuesIn(kNestedTypes),
                         TypeTestCaseToString);

TEST_P(TypeTest, VisitTypePrintToString) {
  TypeNameVisitor visitor;
  std::ostringstream oss;
  const auto& test_case = GetParam();
  ASSERT_THAT(VisitTypeInline(*test_case.type, &visitor, oss), IsOk());
  ASSERT_EQ(oss.str(), test_case.repr);
}

TEST_P(TypeTest, VisitTypeReturnNestedTypeId) {
  auto visitor = [&](auto&& type) -> Result<TypeId> {
    using Type = std::decay_t<decltype(type)>;
    // Check if the type is a nested type
    if constexpr (std::is_base_of_v<NestedType, Type>) {
      return type.type_id();
    } else {
      return NotImplemented("Type is not a nested type");
    }
  };

  const auto& test_case = GetParam();
  auto result = VisitType(*test_case.type, visitor);

  if (test_case.primitive) {
    ASSERT_THAT(result, IsError(ErrorKind::kNotImplemented));
    ASSERT_THAT(result, HasErrorMessage("Type is not a nested type"));
  } else {
    ASSERT_THAT(result, IsOk());
    ASSERT_EQ(result.value(), test_case.type_id);
  }
}

}  // namespace iceberg
