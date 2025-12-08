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
#include <thread>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/exception.h"
#include "iceberg/test/matchers.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep
#include "iceberg/util/type_util.h"

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
        .type = iceberg::boolean(),
        .type_id = iceberg::TypeId::kBoolean,
        .primitive = true,
        .repr = "boolean",
    },
    {
        .name = "int",
        .type = iceberg::int32(),
        .type_id = iceberg::TypeId::kInt,
        .primitive = true,
        .repr = "int",
    },
    {
        .name = "long",
        .type = iceberg::int64(),
        .type_id = iceberg::TypeId::kLong,
        .primitive = true,
        .repr = "long",
    },
    {
        .name = "float",
        .type = iceberg::float32(),
        .type_id = iceberg::TypeId::kFloat,
        .primitive = true,
        .repr = "float",
    },
    {
        .name = "double",
        .type = iceberg::float64(),
        .type_id = iceberg::TypeId::kDouble,
        .primitive = true,
        .repr = "double",
    },
    {
        .name = "decimal9_2",
        .type = iceberg::decimal(9, 2),
        .type_id = iceberg::TypeId::kDecimal,
        .primitive = true,
        .repr = "decimal(9, 2)",
    },
    {
        .name = "decimal38_10",
        .type = iceberg::decimal(38, 10),
        .type_id = iceberg::TypeId::kDecimal,
        .primitive = true,
        .repr = "decimal(38, 10)",
    },
    {
        .name = "date",
        .type = iceberg::date(),
        .type_id = iceberg::TypeId::kDate,
        .primitive = true,
        .repr = "date",
    },
    {
        .name = "time",
        .type = iceberg::time(),
        .type_id = iceberg::TypeId::kTime,
        .primitive = true,
        .repr = "time",
    },
    {
        .name = "timestamp",
        .type = iceberg::timestamp(),
        .type_id = iceberg::TypeId::kTimestamp,
        .primitive = true,
        .repr = "timestamp",
    },
    {
        .name = "timestamptz",
        .type = iceberg::timestamp_tz(),
        .type_id = iceberg::TypeId::kTimestampTz,
        .primitive = true,
        .repr = "timestamptz",
    },
    {
        .name = "binary",
        .type = iceberg::binary(),
        .type_id = iceberg::TypeId::kBinary,
        .primitive = true,
        .repr = "binary",
    },
    {
        .name = "string",
        .type = iceberg::string(),
        .type_id = iceberg::TypeId::kString,
        .primitive = true,
        .repr = "string",
    },
    {
        .name = "fixed10",
        .type = iceberg::fixed(10),
        .type_id = iceberg::TypeId::kFixed,
        .primitive = true,
        .repr = "fixed(10)",
    },
    {
        .name = "fixed255",
        .type = iceberg::fixed(255),
        .type_id = iceberg::TypeId::kFixed,
        .primitive = true,
        .repr = "fixed(255)",
    },
    {
        .name = "uuid",
        .type = iceberg::uuid(),
        .type_id = iceberg::TypeId::kUuid,
        .primitive = true,
        .repr = "uuid",
    },
}};

const static std::array<TypeTestCase, 4> kNestedTypes = {{
    {
        .name = "list_int",
        .type = std::make_shared<iceberg::ListType>(1, iceberg::int32(), true),
        .type_id = iceberg::TypeId::kList,
        .primitive = false,
        .repr = "list<element (1): int (optional)>",
    },
    {
        .name = "list_list_int",
        .type = std::make_shared<iceberg::ListType>(
            1, std::make_shared<iceberg::ListType>(2, iceberg::int32(), true), false),
        .type_id = iceberg::TypeId::kList,
        .primitive = false,
        .repr = "list<element (1): list<element (2): int (optional)> (required)>",
    },
    {
        .name = "map_int_string",
        .type = std::make_shared<iceberg::MapType>(
            iceberg::SchemaField::MakeRequired(1, "key", iceberg::int64()),
            iceberg::SchemaField::MakeRequired(2, "value", iceberg::string())),
        .type_id = iceberg::TypeId::kMap,
        .primitive = false,
        .repr = "map<key (1): long (required): value (2): string (required)>",
    },
    {
        .name = "struct",
        .type = std::make_shared<iceberg::StructType>(std::vector<iceberg::SchemaField>{
            iceberg::SchemaField::MakeRequired(1, "foo", iceberg::int64()),
            iceberg::SchemaField::MakeOptional(2, "bar", iceberg::string()),
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
    iceberg::SchemaField field(5, "element", iceberg::int32(), true);
    iceberg::ListType list(field);
    std::span<const iceberg::SchemaField> fields = list.fields();
    ASSERT_EQ(1, fields.size());
    ASSERT_EQ(field, fields[0]);
    auto result = list.GetFieldByIndex(5);
    ASSERT_THAT(result, IsError(iceberg::ErrorKind::kInvalidArgument));
    ASSERT_THAT(result,
                iceberg::HasErrorMessage("Invalid index 5 to get field from list"));
    ASSERT_THAT(list.GetFieldByIndex(0), ::testing::Optional(field));
    ASSERT_THAT(list.GetFieldByName("element"), ::testing::Optional(field));

    ASSERT_EQ(std::nullopt, list.GetFieldById(0));
    result = list.GetFieldByIndex(1);
    ASSERT_THAT(result, IsError(iceberg::ErrorKind::kInvalidArgument));
    ASSERT_THAT(result,
                iceberg::HasErrorMessage("Invalid index 1 to get field from list"));
    result = list.GetFieldByIndex(-1);
    ASSERT_THAT(result, IsError(iceberg::ErrorKind::kInvalidArgument));
    ASSERT_THAT(result,
                iceberg::HasErrorMessage("Invalid index -1 to get field from list"));
    ASSERT_EQ(std::nullopt, list.GetFieldByName("foo"));
  }
  ASSERT_THAT(
      []() {
        iceberg::ListType list(
            iceberg::SchemaField(1, "wrongname", iceberg::boolean(), true));
      },
      ::testing::ThrowsMessage<iceberg::IcebergError>(
          ::testing::HasSubstr("child field name should be 'element', was 'wrongname'")));
}

TEST(TypeTest, Map) {
  {
    iceberg::SchemaField key(5, "key", iceberg::int32(), true);
    iceberg::SchemaField value(7, "value", iceberg::string(), true);
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
    auto result = map.GetFieldByIndex(2);
    ASSERT_THAT(result, IsError(iceberg::ErrorKind::kInvalidArgument));
    ASSERT_THAT(result,
                iceberg::HasErrorMessage("Invalid index 2 to get field from map"));
    result = map.GetFieldByIndex(-1);
    ASSERT_THAT(result, IsError(iceberg::ErrorKind::kInvalidArgument));
    ASSERT_THAT(result,
                iceberg::HasErrorMessage("Invalid index -1 to get field from map"));
    ASSERT_EQ(std::nullopt, map.GetFieldByName("element"));
  }
  ASSERT_THAT(
      []() {
        iceberg::SchemaField key(5, "notkey", iceberg::int32(), true);
        iceberg::SchemaField value(7, "value", iceberg::string(), true);
        iceberg::MapType map(key, value);
      },
      ::testing::ThrowsMessage<iceberg::IcebergError>(
          ::testing::HasSubstr("key field name should be 'key', was 'notkey'")));
  ASSERT_THAT(
      []() {
        iceberg::SchemaField key(5, "key", iceberg::int32(), true);
        iceberg::SchemaField value(7, "notvalue", iceberg::string(), true);
        iceberg::MapType map(key, value);
      },
      ::testing::ThrowsMessage<iceberg::IcebergError>(
          ::testing::HasSubstr("value field name should be 'value', was 'notvalue'")));
}

TEST(TypeTest, Struct) {
  {
    iceberg::SchemaField field1(5, "foo", iceberg::int32(), true);
    iceberg::SchemaField field2(7, "bar", iceberg::string(), true);
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
    auto result = struct_.GetFieldByIndex(2);
    ASSERT_THAT(result, IsError(iceberg::ErrorKind::kInvalidArgument));
    ASSERT_THAT(result,
                iceberg::HasErrorMessage("Invalid index 2 to get field from struct"));
    result = struct_.GetFieldByIndex(-1);
    ASSERT_THAT(result, IsError(iceberg::ErrorKind::kInvalidArgument));
    ASSERT_THAT(result,
                iceberg::HasErrorMessage("Invalid index -1 to get field from struct"));
    ASSERT_EQ(std::nullopt, struct_.GetFieldByName("element"));
  }
}

TEST(TypeTest, StructTypeGetFieldByName) {
  iceberg::SchemaField field1(1, "Foo", iceberg::int32(), true);
  iceberg::SchemaField field2(2, "Bar", iceberg::string(), false);
  iceberg::StructType struct_({field1, field2});

  // Case-sensitive: exact match
  ASSERT_THAT(struct_.GetFieldByName("Foo"), ::testing::Optional(field1));
  ASSERT_THAT(struct_.GetFieldByName("foo"), ::testing::Eq(std::nullopt));

  // Case-insensitive
  ASSERT_THAT(struct_.GetFieldByName("foo", false), ::testing::Optional(field1));
  ASSERT_THAT(struct_.GetFieldByName("fOO", false), ::testing::Optional(field1));
  ASSERT_THAT(struct_.GetFieldByName("FOO", false), ::testing::Optional(field1));
  ASSERT_THAT(struct_.GetFieldByName("bar", false), ::testing::Optional(field2));
  ASSERT_THAT(struct_.GetFieldByName("BaR", false), ::testing::Optional(field2));
  ASSERT_THAT(struct_.GetFieldByName("BAR", false), ::testing::Optional(field2));
  ASSERT_THAT(struct_.GetFieldByName("baz", false), ::testing::Eq(std::nullopt));
}

TEST(TypeTest, ListTypeGetFieldByName) {
  iceberg::SchemaField element(1, "element", iceberg::int32(), true);
  iceberg::ListType list(element);

  // Case-sensitive: exact match
  ASSERT_THAT(list.GetFieldByName("element"), ::testing::Optional(element));
  ASSERT_THAT(list.GetFieldByName("Element"), ::testing::Eq(std::nullopt));

  // Case-insensitive
  ASSERT_THAT(list.GetFieldByName("element", false), ::testing::Optional(element));
  ASSERT_THAT(list.GetFieldByName("Element", false), ::testing::Optional(element));
  ASSERT_THAT(list.GetFieldByName("ELEMENT", false), ::testing::Optional(element));
  ASSERT_THAT(list.GetFieldByName("eLeMeNt", false), ::testing::Optional(element));
  ASSERT_THAT(list.GetFieldByName("foo", false), ::testing::Eq(std::nullopt));
}

TEST(TypeTest, MapTypeGetFieldByName) {
  iceberg::SchemaField key(1, "key", iceberg::int32(), true);
  iceberg::SchemaField value(2, "value", iceberg::string(), false);
  iceberg::MapType map(key, value);

  // Case-sensitive: exact match
  ASSERT_THAT(map.GetFieldByName("key"), ::testing::Optional(key));
  ASSERT_THAT(map.GetFieldByName("Key"), ::testing::Eq(std::nullopt));
  ASSERT_THAT(map.GetFieldByName("value"), ::testing::Optional(value));
  ASSERT_THAT(map.GetFieldByName("Value"), ::testing::Eq(std::nullopt));

  // Case-insensitive
  ASSERT_THAT(map.GetFieldByName("Key", false), ::testing::Optional(key));
  ASSERT_THAT(map.GetFieldByName("KEY", false), ::testing::Optional(key));
  ASSERT_THAT(map.GetFieldByName("kEy", false), ::testing::Optional(key));
  ASSERT_THAT(map.GetFieldByName("value", false), ::testing::Optional(value));
  ASSERT_THAT(map.GetFieldByName("Value", false), ::testing::Optional(value));
  ASSERT_THAT(map.GetFieldByName("VALUE", false), ::testing::Optional(value));
  ASSERT_THAT(map.GetFieldByName("vAlUe", false), ::testing::Optional(value));
  ASSERT_THAT(map.GetFieldByName("foo", false), ::testing::Eq(std::nullopt));
}

TEST(TypeTest, StructDuplicateId) {
  iceberg::SchemaField field1(5, "foo", iceberg::int32(), true);
  iceberg::SchemaField field2(5, "bar", iceberg::string(), true);
  iceberg::StructType struct_({field1, field2});

  auto result = struct_.GetFieldById(5);
  ASSERT_FALSE(result.has_value());
  ASSERT_THAT(result, IsError(iceberg::ErrorKind::kInvalidSchema));
  ASSERT_THAT(result,
              iceberg::HasErrorMessage(
                  "Duplicate field id found: 5 (prev name: foo, curr name: bar)"));
}

TEST(TypeTest, StructDuplicateName) {
  iceberg::SchemaField field1(1, "foo", iceberg::int32(), true);
  iceberg::SchemaField field2(2, "foo", iceberg::string(), true);
  iceberg::StructType struct_({field1, field2});

  auto result = struct_.GetFieldByName("foo", true);
  ASSERT_FALSE(result.has_value());
  ASSERT_THAT(result, IsError(iceberg::ErrorKind::kInvalidSchema));
  ASSERT_THAT(result, iceberg::HasErrorMessage(
                          "Duplicate field name found: foo (prev id: 1, curr id: 2)"));
}

TEST(TypeTest, StructDuplicateLowerCaseName) {
  iceberg::SchemaField field1(1, "Foo", iceberg::int32(), true);
  iceberg::SchemaField field2(2, "foo", iceberg::string(), true);
  iceberg::StructType struct_({field1, field2});

  auto result = struct_.GetFieldByName("foo", false);
  ASSERT_FALSE(result.has_value());
  ASSERT_THAT(result, IsError(iceberg::ErrorKind::kInvalidSchema));
  ASSERT_THAT(result,
              iceberg::HasErrorMessage(
                  "Duplicate lowercase field name found: foo (prev id: 1, curr id: 2)"));
}

// Thread safety tests for StructType Lazy Init
class StructTypeThreadSafetyTest : public ::testing::Test {
 protected:
  void SetUp() override {
    field1_ = std::make_unique<iceberg::SchemaField>(1, "id", iceberg::int32(), true);
    field2_ = std::make_unique<iceberg::SchemaField>(2, "name", iceberg::string(), true);
    field3_ = std::make_unique<iceberg::SchemaField>(3, "age", iceberg::int32(), true);

    struct_type_ = std::make_unique<iceberg::StructType>(
        std::vector<iceberg::SchemaField>{*field1_, *field2_, *field3_});
  }

  std::unique_ptr<iceberg::StructType> struct_type_;
  std::unique_ptr<iceberg::SchemaField> field1_;
  std::unique_ptr<iceberg::SchemaField> field2_;
  std::unique_ptr<iceberg::SchemaField> field3_;
};

TEST_F(StructTypeThreadSafetyTest, ConcurrentGetFieldById) {
  const int num_threads = 10;
  const int iterations_per_thread = 100;
  std::vector<std::thread> threads;

  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([this, iterations_per_thread]() {
      for (int j = 0; j < iterations_per_thread; ++j) {
        ASSERT_THAT(struct_type_->GetFieldById(1), ::testing::Optional(*field1_));
        ASSERT_THAT(struct_type_->GetFieldById(999), ::testing::Optional(std::nullopt));
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }
}

TEST_F(StructTypeThreadSafetyTest, ConcurrentGetFieldByName) {
  const int num_threads = 10;
  const int iterations_per_thread = 100;
  std::vector<std::thread> threads;

  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([this, iterations_per_thread]() {
      for (int j = 0; j < iterations_per_thread; ++j) {
        ASSERT_THAT(struct_type_->GetFieldByName("id", true),
                    ::testing::Optional(*field1_));
        ASSERT_THAT(struct_type_->GetFieldByName("NAME", false),
                    ::testing::Optional(*field2_));
        ASSERT_THAT(struct_type_->GetFieldByName("noexist", false),
                    ::testing::Optional(std::nullopt));
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }
}

TEST_F(StructTypeThreadSafetyTest, MixedConcurrentOperations) {
  const int num_threads = 8;
  const int iterations_per_thread = 50;
  std::vector<std::thread> threads;

  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([this, iterations_per_thread, i]() {
      for (int j = 0; j < iterations_per_thread; ++j) {
        if (i % 4 == 0) {
          ASSERT_THAT(struct_type_->GetFieldById(1), ::testing::Optional(*field1_));
        } else if (i % 4 == 1) {
          ASSERT_THAT(struct_type_->GetFieldByName("name", true),
                      ::testing::Optional(*field2_));
        } else if (i % 4 == 2) {
          ASSERT_THAT(struct_type_->GetFieldByName("AGE", false),
                      ::testing::Optional(*field3_));
        } else {
          ASSERT_THAT(struct_type_->GetFieldById(2), ::testing::Optional(*field2_));
          ASSERT_THAT(struct_type_->GetFieldByName("id", true),
                      ::testing::Optional(*field1_));
          ASSERT_THAT(struct_type_->GetFieldByName("age", false),
                      ::testing::Optional(*field3_));
        }
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }
}

TEST(TypeTest, IndexParents) {
  auto inner_preferences =
      std::make_shared<iceberg::StructType>(std::vector<iceberg::SchemaField>{
          iceberg::SchemaField::MakeRequired(12, "feature3", iceberg::boolean()),
          iceberg::SchemaField::MakeOptional(13, "feature4", iceberg::boolean()),
      });

  auto preferences =
      std::make_shared<iceberg::StructType>(std::vector<iceberg::SchemaField>{
          iceberg::SchemaField::MakeRequired(6, "feature1", iceberg::boolean()),
          iceberg::SchemaField::MakeOptional(7, "feature2", iceberg::boolean()),
          iceberg::SchemaField::MakeOptional(8, "inner_preferences", inner_preferences),
      });

  auto locations_key_struct =
      std::make_shared<iceberg::StructType>(std::vector<iceberg::SchemaField>{
          iceberg::SchemaField::MakeRequired(20, "address", iceberg::string()),
          iceberg::SchemaField::MakeRequired(21, "city", iceberg::string()),
          iceberg::SchemaField::MakeRequired(22, "state", iceberg::string()),
          iceberg::SchemaField::MakeRequired(23, "zip", iceberg::int32()),
      });

  auto locations_value_struct =
      std::make_shared<iceberg::StructType>(std::vector<iceberg::SchemaField>{
          iceberg::SchemaField::MakeRequired(14, "lat", iceberg::float32()),
          iceberg::SchemaField::MakeRequired(15, "long", iceberg::float32()),
      });

  auto locations = iceberg::SchemaField::MakeRequired(
      4, "locations",
      std::make_shared<iceberg::MapType>(
          iceberg::SchemaField::MakeRequired(9, "key", locations_key_struct),
          iceberg::SchemaField::MakeRequired(10, "value", locations_value_struct)));

  auto points_struct =
      std::make_shared<iceberg::StructType>(std::vector<iceberg::SchemaField>{
          iceberg::SchemaField::MakeRequired(16, "x", iceberg::int64()),
          iceberg::SchemaField::MakeRequired(17, "y", iceberg::int64()),
      });

  auto points = iceberg::SchemaField::MakeOptional(
      5, "points",
      std::make_shared<iceberg::ListType>(
          iceberg::SchemaField::MakeOptional(11, "element", points_struct)));

  auto root_struct = iceberg::StructType(std::vector<iceberg::SchemaField>{
      iceberg::SchemaField::MakeRequired(1, "id", iceberg::int32()),
      iceberg::SchemaField::MakeOptional(2, "data", iceberg::string()),
      iceberg::SchemaField::MakeOptional(3, "preferences", preferences),
      locations,
      points,
  });

  std::unordered_map<int32_t, int32_t> parent_index = iceberg::IndexParents(root_struct);

  // Verify top-level fields have no parent
  ASSERT_EQ(parent_index.find(1), parent_index.end());
  ASSERT_EQ(parent_index.find(2), parent_index.end());
  ASSERT_EQ(parent_index.find(3), parent_index.end());
  ASSERT_EQ(parent_index.find(4), parent_index.end());
  ASSERT_EQ(parent_index.find(5), parent_index.end());

  // Verify struct field parents
  ASSERT_EQ(parent_index[6], 3);
  ASSERT_EQ(parent_index[7], 3);
  ASSERT_EQ(parent_index[8], 3);
  ASSERT_EQ(parent_index[12], 8);
  ASSERT_EQ(parent_index[13], 8);

  // Verify map field parents
  ASSERT_EQ(parent_index[9], 4);
  ASSERT_EQ(parent_index[10], 4);
  ASSERT_EQ(parent_index[20], 9);
  ASSERT_EQ(parent_index[21], 9);
  ASSERT_EQ(parent_index[22], 9);
  ASSERT_EQ(parent_index[23], 9);
  ASSERT_EQ(parent_index[14], 10);
  ASSERT_EQ(parent_index[15], 10);

  // Verify list field parents
  ASSERT_EQ(parent_index[11], 5);
  ASSERT_EQ(parent_index[16], 11);
  ASSERT_EQ(parent_index[17], 11);

  ASSERT_EQ(parent_index.size(), 16);
}
