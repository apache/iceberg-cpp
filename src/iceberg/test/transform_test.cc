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

#include "iceberg/transform.h"

#include <format>
#include <memory>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/expression/literal.h"
#include "iceberg/type.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep
#include "matchers.h"

namespace iceberg {

TEST(TransformTest, Transform) {
  auto transform = Transform::Identity();
  EXPECT_EQ(TransformType::kIdentity, transform->transform_type());
  EXPECT_EQ("identity", transform->ToString());
  EXPECT_EQ("identity", std::format("{}", *transform));

  auto source_type = iceberg::string();
  auto identity_transform = transform->Bind(source_type);
  ASSERT_TRUE(identity_transform);
}

TEST(TransformFunctionTest, CreateBucketTransform) {
  constexpr int32_t bucket_count = 8;
  auto transform = Transform::Bucket(bucket_count);
  EXPECT_EQ("bucket[8]", transform->ToString());
  EXPECT_EQ("bucket[8]", std::format("{}", *transform));

  const auto transformPtr = transform->Bind(iceberg::string());
  ASSERT_TRUE(transformPtr);
  EXPECT_EQ(transformPtr.value()->transform_type(), TransformType::kBucket);
}

TEST(TransformFunctionTest, CreateTruncateTransform) {
  constexpr int32_t width = 16;
  auto transform = Transform::Truncate(width);
  EXPECT_EQ("truncate[16]", transform->ToString());
  EXPECT_EQ("truncate[16]", std::format("{}", *transform));

  auto transformPtr = transform->Bind(iceberg::string());
  EXPECT_EQ(transformPtr.value()->transform_type(), TransformType::kTruncate);
}
TEST(TransformFromStringTest, PositiveCases) {
  struct Case {
    std::string str;
    TransformType type;
    std::optional<int32_t> param;
  };

  const std::vector<Case> cases = {
      {.str = "identity", .type = TransformType::kIdentity, .param = std::nullopt},
      {.str = "year", .type = TransformType::kYear, .param = std::nullopt},
      {.str = "month", .type = TransformType::kMonth, .param = std::nullopt},
      {.str = "day", .type = TransformType::kDay, .param = std::nullopt},
      {.str = "hour", .type = TransformType::kHour, .param = std::nullopt},
      {.str = "void", .type = TransformType::kVoid, .param = std::nullopt},
      {.str = "bucket[16]", .type = TransformType::kBucket, .param = 16},
      {.str = "truncate[32]", .type = TransformType::kTruncate, .param = 32},
  };
  for (const auto& c : cases) {
    auto result = TransformFromString(c.str);
    ASSERT_TRUE(result.has_value()) << "Failed to parse: " << c.str;

    const auto& transform = result.value();
    EXPECT_EQ(transform->transform_type(), c.type);
    if (c.param.has_value()) {
      EXPECT_EQ(transform->ToString(),
                std::format("{}[{}]", TransformTypeToString(c.type), *c.param));
    } else {
      EXPECT_EQ(transform->ToString(), TransformTypeToString(c.type));
    }
  }
}

TEST(TransformFromStringTest, NegativeCases) {
  constexpr std::array<std::string_view, 6> invalid_cases = {
      "bucket",           // missing param
      "bucket[]",         // empty param
      "bucket[abc]",      // invalid number
      "unknown",          // unsupported transform
      "bucket[16",        // missing closing bracket
      "truncate[1]extra"  // extra characters
  };

  for (const auto& str : invalid_cases) {
    auto result = TransformFromString(str);
    EXPECT_FALSE(result.has_value()) << "Unexpected success for: " << str;
    EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  }
}

TEST(TransformResultTypeTest, PositiveCases) {
  struct Case {
    std::string str;
    std::shared_ptr<Type> source_type;
    std::shared_ptr<Type> expected_result_type;
  };

  const std::vector<Case> cases = {
      {.str = "identity",
       .source_type = iceberg::string(),
       .expected_result_type = iceberg::string()},
      {.str = "year",
       .source_type = iceberg::timestamp(),
       .expected_result_type = iceberg::int32()},
      {.str = "month",
       .source_type = iceberg::timestamp(),
       .expected_result_type = iceberg::int32()},
      {.str = "day",
       .source_type = iceberg::timestamp(),
       .expected_result_type = iceberg::int32()},
      {.str = "hour",
       .source_type = iceberg::timestamp(),
       .expected_result_type = iceberg::int32()},
      {.str = "void",
       .source_type = iceberg::string(),
       .expected_result_type = iceberg::string()},
      {.str = "bucket[16]",
       .source_type = iceberg::string(),
       .expected_result_type = iceberg::int32()},
      {.str = "truncate[32]",
       .source_type = iceberg::string(),
       .expected_result_type = iceberg::string()},
  };

  for (const auto& c : cases) {
    auto result = TransformFromString(c.str);
    ASSERT_TRUE(result.has_value()) << "Failed to parse: " << c.str;

    const auto& transform = result.value();
    const auto transformPtr = transform->Bind(c.source_type);
    ASSERT_TRUE(transformPtr.has_value()) << "Failed to bind: " << c.str;

    auto result_type = transformPtr.value()->ResultType();
    EXPECT_EQ(result_type->type_id(), c.expected_result_type->type_id())
        << "Unexpected result type for: " << c.str;
  }
}

TEST(TransformResultTypeTest, NegativeCases) {
  struct Case {
    std::string str;
    std::shared_ptr<Type> source_type;
  };

  const std::vector<Case> cases = {
      {.str = "identity", .source_type = nullptr},
      {.str = "year", .source_type = iceberg::string()},
      {.str = "month", .source_type = iceberg::string()},
      {.str = "day", .source_type = iceberg::string()},
      {.str = "hour", .source_type = iceberg::string()},
      {.str = "void", .source_type = nullptr},
      {.str = "bucket[16]", .source_type = iceberg::float32()},
      {.str = "truncate[32]", .source_type = iceberg::float64()}};

  for (const auto& c : cases) {
    auto result = TransformFromString(c.str);
    ASSERT_TRUE(result.has_value()) << "Failed to parse: " << c.str;

    const auto& transform = result.value();
    auto transformPtr = transform->Bind(c.source_type);

    ASSERT_THAT(transformPtr, IsError(ErrorKind::kNotSupported));
  }
}

TEST(TransformLiteralTest, IdentityTransform) {
  struct Case {
    std::shared_ptr<Type> source_type;
    Literal source;
    Literal expected;
  };

  const std::vector<Case> cases = {
      {.source_type = iceberg::boolean(),
       .source = Literal::Boolean(true),
       .expected = Literal::Boolean(true)},
      {.source_type = iceberg::int32(),
       .source = Literal::Int(42),
       .expected = Literal::Int(42)},
      {.source_type = iceberg::int32(),
       .source = Literal::Date(30000),
       .expected = Literal::Date(30000)},
      {.source_type = iceberg::int64(),
       .source = Literal::Long(1234567890),
       .expected = Literal::Long(1234567890)},
      {.source_type = iceberg::timestamp(),
       .source = Literal::Timestamp(1622547800000000),
       .expected = Literal::Timestamp(1622547800000000)},
      {.source_type = iceberg::timestamp_tz(),
       .source = Literal::TimestampTz(1622547800000000),
       .expected = Literal::TimestampTz(1622547800000000)},
      {.source_type = iceberg::float32(),
       .source = Literal::Float(3.14),
       .expected = Literal::Float(3.14)},
      {.source_type = iceberg::float64(),
       .source = Literal::Double(1.23e-5),
       .expected = Literal::Double(1.23e-5)},
      {.source_type = iceberg::string(),
       .source = Literal::String("Hello, World!"),
       .expected = Literal::String("Hello, World!")},
      {.source_type = iceberg::binary(),
       .source = Literal::Binary({0x01, 0x02, 0x03}),
       .expected = Literal::Binary({0x01, 0x02, 0x03})},
  };

  for (const auto& c : cases) {
    auto transform = Transform::Identity();
    auto transformPtr = transform->Bind(c.source_type);
    ASSERT_TRUE(transformPtr.has_value()) << "Failed to bind identity transform";

    auto result = transformPtr.value()->Transform(c.source);
    ASSERT_TRUE(result.has_value())
        << "Failed to transform literal: " << c.source.ToString();

    EXPECT_EQ(result.value(), c.expected)
        << "Unexpected result for source: " << c.source.ToString();
  }
}

TEST(TransformLiteralTest, BucketTransform) {
  constexpr int32_t num_buckets = 4;
  auto transform = Transform::Bucket(num_buckets);

  struct Case {
    std::shared_ptr<Type> source_type;
    Literal source;
    Literal expected;
  };

  const std::vector<Case> cases = {
      {.source_type = iceberg::int32(),
       .source = Literal::Int(42),
       .expected = Literal::Int(3)},
      {.source_type = iceberg::date(),
       .source = Literal::Date(30000),
       .expected = Literal::Int(2)},
      {.source_type = iceberg::int64(),
       .source = Literal::Long(1234567890),
       .expected = Literal::Int(3)},
      {.source_type = iceberg::timestamp(),
       .source = Literal::Timestamp(1622547800000000),
       .expected = Literal::Int(1)},
      {.source_type = iceberg::timestamp_tz(),
       .source = Literal::TimestampTz(1622547800000000),
       .expected = Literal::Int(1)},
      {.source_type = iceberg::string(),
       .source = Literal::String("test"),
       .expected = Literal::Int(3)},
  };

  for (const auto& c : cases) {
    auto transformPtr = transform->Bind(c.source_type);
    ASSERT_TRUE(transformPtr.has_value()) << "Failed to bind bucket transform";
    auto result = transformPtr.value()->Transform(c.source);
    ASSERT_TRUE(result.has_value())
        << "Failed to transform literal: " << c.source.ToString();

    EXPECT_EQ(result.value(), c.expected)
        << "Unexpected result for source: " << c.source.ToString();
  }
}

TEST(TransformLiteralTest, TruncateTransform) {
  struct Case {
    std::shared_ptr<Type> source_type;
    int32_t width;
    Literal source;
    Literal expected;
  };

  const std::vector<Case> cases = {
      {.source_type = iceberg::int32(),
       .width = 5,
       .source = Literal::Int(123456),
       .expected = Literal::Int(123455)},
      {.source_type = iceberg::string(),
       .width = 5,
       .source = Literal::String("Hello, World!"),
       .expected = Literal::String("Hello")},
      {.source_type = iceberg::string(),
       .width = 5,
       .source = Literal::String("😜🧐🤔🤪🥳😵‍💫😂"),
       // Truncate to 5 utf-8 code points
       .expected = Literal::String("😜🧐🤔🤪🥳")},
      {.source_type = iceberg::string(),
       .width = 8,
       .source = Literal::String("a😜b🧐c🤔d🤪e🥳"),
       .expected = Literal::String("a😜b🧐c🤔d🤪")},
      {.source_type = iceberg::binary(),
       .width = 5,
       .source = Literal::Binary({0x01, 0x02, 0x03, 0x04, 0x05, 0x06}),
       .expected = Literal::Binary({0x01, 0x02, 0x03, 0x04, 0x05})},
  };

  for (const auto& c : cases) {
    auto transform = Transform::Truncate(c.width);
    auto transformPtr = transform->Bind(c.source_type);
    ASSERT_TRUE(transformPtr.has_value()) << "Failed to bind truncate transform";
    auto result = transformPtr.value()->Transform(c.source);
    ASSERT_TRUE(result.has_value())
        << "Failed to transform literal: " << c.source.ToString();

    EXPECT_EQ(result.value(), c.expected)
        << "Unexpected result for source: " << c.source.ToString();
  }
}

TEST(TransformLiteralTest, YearTransform) {
  auto transform = Transform::Year();

  struct Case {
    std::shared_ptr<Type> source_type;
    Literal source;
    Literal expected;
  };

  const std::vector<Case> cases = {
      {.source_type = iceberg::timestamp(),
       // 2021-06-01T11:43:20Z
       .source = Literal::Timestamp(1622547800000000),
       .expected = Literal::Int(2021)},
      {.source_type = iceberg::timestamp_tz(),
       .source = Literal::TimestampTz(1622547800000000),
       .expected = Literal::Int(2021)},
      {.source_type = iceberg::date(),
       .source = Literal::Date(30000),
       .expected = Literal::Int(2052)},
  };

  for (const auto& c : cases) {
    auto transformPtr = transform->Bind(c.source_type);
    ASSERT_TRUE(transformPtr.has_value()) << "Failed to bind year transform";
    auto result = transformPtr.value()->Transform(c.source);
    ASSERT_TRUE(result.has_value())
        << "Failed to transform literal: " << c.source.ToString();

    EXPECT_EQ(result.value(), c.expected)
        << "Unexpected result for source: " << c.source.ToString();
  }
}

TEST(TransformLiteralTest, MonthTransform) {
  auto transform = Transform::Month();

  struct Case {
    std::shared_ptr<Type> source_type;
    Literal source;
    Literal expected;
  };

  const std::vector<Case> cases = {
      {.source_type = iceberg::timestamp(),
       .source = Literal::Timestamp(1622547800000000),
       .expected = Literal::Int(617)},
      {.source_type = iceberg::timestamp_tz(),
       .source = Literal::TimestampTz(1622547800000000),
       .expected = Literal::Int(617)},
      {.source_type = iceberg::date(),
       .source = Literal::Date(30000),
       .expected = Literal::Int(985)},
  };

  for (const auto& c : cases) {
    auto transformPtr = transform->Bind(c.source_type);
    ASSERT_TRUE(transformPtr.has_value()) << "Failed to bind month transform";
    auto result = transformPtr.value()->Transform(c.source);
    ASSERT_TRUE(result.has_value())
        << "Failed to transform literal: " << c.source.ToString();

    EXPECT_EQ(result.value(), c.expected)
        << "Unexpected result for source: " << c.source.ToString();
  }
}

TEST(TransformFunctionTransformTest, DayTransform) {
  auto transform = Transform::Day();

  struct Case {
    std::shared_ptr<Type> source_type;
    Literal source;
    Literal expected;
  };

  const std::vector<Case> cases = {
      {.source_type = iceberg::timestamp(),
       .source = Literal::Timestamp(1622547800000000),
       .expected = Literal::Int(18779)},
      {.source_type = iceberg::timestamp_tz(),
       .source = Literal::TimestampTz(1622547800000000),
       .expected = Literal::Int(18779)},
      {.source_type = iceberg::date(),
       .source = Literal::Date(30000),
       .expected = Literal::Int(30000)},
  };

  for (const auto& c : cases) {
    auto transformPtr = transform->Bind(c.source_type);
    ASSERT_TRUE(transformPtr.has_value()) << "Failed to bind day transform";
    auto result = transformPtr.value()->Transform(c.source);
    ASSERT_TRUE(result.has_value())
        << "Failed to transform literal: " << c.source.ToString();

    EXPECT_EQ(result.value(), c.expected)
        << "Unexpected result for source: " << c.source.ToString();
  }
}

TEST(TransformLiteralTest, HourTransform) {
  auto transform = Transform::Hour();

  struct Case {
    std::shared_ptr<Type> source_type;
    Literal source;
    Literal expected;
  };

  const std::vector<Case> cases = {
      {.source_type = iceberg::timestamp(),
       .source = Literal::Timestamp(1622547800000000),
       .expected = Literal::Int(450707)},
      {.source_type = iceberg::timestamp_tz(),
       .source = Literal::TimestampTz(1622547800000000),
       .expected = Literal::Int(450707)},
  };

  for (const auto& c : cases) {
    auto transformPtr = transform->Bind(c.source_type);
    ASSERT_TRUE(transformPtr.has_value()) << "Failed to bind hour transform";
    auto result = transformPtr.value()->Transform(c.source);
    ASSERT_TRUE(result.has_value())
        << "Failed to transform literal: " << c.source.ToString();

    EXPECT_EQ(result.value(), c.expected)
        << "Unexpected result for source: " << c.source.ToString();
  }
}

TEST(TransformLiteralTest, VoidTransform) {
  auto transform = Transform::Void();

  struct Case {
    std::shared_ptr<Type> source_type;
    Literal source;
  };

  const std::vector<Case> cases = {
      {.source_type = iceberg::boolean(), .source = Literal::Boolean(true)},
      {.source_type = iceberg::int32(), .source = Literal::Int(42)},
      {.source_type = iceberg::date(), .source = Literal::Date(30000)},
      {.source_type = iceberg::int64(), .source = Literal::Long(1234567890)},
      {.source_type = iceberg::timestamp(),
       .source = Literal::Timestamp(1622547800000000)},
      {.source_type = iceberg::timestamp_tz(),
       .source = Literal::TimestampTz(1622547800000000)},
      {.source_type = iceberg::float32(), .source = Literal::Float(3.14)},
      {.source_type = iceberg::float64(), .source = Literal::Double(1.23e-5)},
      {.source_type = iceberg::string(), .source = Literal::String("Hello, World!")},
      {.source_type = iceberg::binary(), .source = Literal::Binary({0x01, 0x02, 0x03})},
  };

  for (const auto& c : cases) {
    auto transformPtr = transform->Bind(c.source_type);
    ASSERT_TRUE(transformPtr.has_value()) << "Failed to bind void transform";
    auto result = transformPtr.value()->Transform(c.source);
    EXPECT_TRUE(result->IsNull())
        << "Expected void transform to return null type for source: "
        << c.source.ToString();
    EXPECT_EQ(result->type()->type_id(), c.source_type->type_id())
        << "Expected void transform to return same type as source for: "
        << c.source.ToString();
  }
}

TEST(TransformLiteralTest, NullLiteral) {
  struct Case {
    std::string str;
    std::shared_ptr<Type> source_type;
    Literal source;
    std::shared_ptr<Type> expected_result_type;
  };

  const std::vector<Case> cases = {
      {.str = "identity",
       .source_type = iceberg::string(),
       .source = Literal::Null(iceberg::string()),
       .expected_result_type = iceberg::string()},
      {.str = "year",
       .source_type = iceberg::timestamp(),
       .source = Literal::Null(iceberg::timestamp()),
       .expected_result_type = iceberg::int32()},
      {.str = "month",
       .source_type = iceberg::timestamp(),
       .source = Literal::Null(iceberg::timestamp()),
       .expected_result_type = iceberg::int32()},
      {.str = "day",
       .source_type = iceberg::timestamp(),
       .source = Literal::Null(iceberg::timestamp()),
       .expected_result_type = iceberg::int32()},
      {.str = "hour",
       .source_type = iceberg::timestamp(),
       .source = Literal::Null(iceberg::timestamp()),
       .expected_result_type = iceberg::int32()},
      {.str = "void",
       .source_type = iceberg::string(),
       .source = Literal::Null(iceberg::string()),
       .expected_result_type = iceberg::string()},
      {.str = "bucket[16]",
       .source_type = iceberg::string(),
       .source = Literal::Null(iceberg::string()),
       .expected_result_type = iceberg::int32()},
      {.str = "truncate[32]",
       .source_type = iceberg::string(),
       .source = Literal::Null(iceberg::string()),
       .expected_result_type = iceberg::string()},
  };

  for (const auto& c : cases) {
    auto result = TransformFromString(c.str);
    ASSERT_TRUE(result.has_value()) << "Failed to parse: " << c.str;

    const auto& transform = result.value();
    const auto transformPtr = transform->Bind(c.source_type);
    ASSERT_TRUE(transformPtr.has_value()) << "Failed to bind: " << c.str;

    auto transform_result = transformPtr.value()->Transform(c.source);
    EXPECT_TRUE(transform_result->IsNull())
        << "Expected void transform to return null type for source: "
        << c.source.ToString();
    EXPECT_EQ(transform_result->type()->type_id(), c.expected_result_type->type_id())
        << "Expected void transform to return same type as source for: "
        << c.source.ToString();
  }
}

}  // namespace iceberg
