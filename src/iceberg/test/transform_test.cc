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
#include <string>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/expression/literal.h"
#include "iceberg/transform_function.h"
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

// Parameterized tests for transform functions
struct TransformParam {
  std::string str;
  // The integer parameter associated with the transform.
  int32_t param;
  std::shared_ptr<Type> source_type;
  Literal source;
  Literal expected;
};

class TransformLiteralTest : public ::testing::TestWithParam<TransformParam> {};

TEST_P(TransformLiteralTest, IdentityTransform) {
  const auto& param = GetParam();

  auto transform = Transform::Identity();
  auto transformPtr = transform->Bind(param.source_type);
  ASSERT_TRUE(transformPtr.has_value()) << "Failed to bind identity transform";

  auto result = transformPtr.value()->Transform(param.source);
  ASSERT_TRUE(result.has_value())
      << "Failed to transform literal: " << param.source.ToString();

  EXPECT_EQ(result.value(), param.expected)
      << "Unexpected result for source: " << param.source.ToString();
}

INSTANTIATE_TEST_SUITE_P(
    IdentityTransformTests, TransformLiteralTest,
    ::testing::Values(
        TransformParam{.str = "BooleanTrue",
                       .source_type = iceberg::boolean(),
                       .source = Literal::Boolean(true),
                       .expected = Literal::Boolean(true)},
        TransformParam{.str = "BooleanFalse",
                       .source_type = iceberg::boolean(),
                       .source = Literal::Boolean(false),
                       .expected = Literal::Boolean(false)},
        TransformParam{.str = "Int32",
                       .source_type = iceberg::int32(),
                       .source = Literal::Int(42),
                       .expected = Literal::Int(42)},
        TransformParam{.str = "Date",
                       .source_type = iceberg::int32(),
                       .source = Literal::Date(30000),
                       .expected = Literal::Date(30000)},
        TransformParam{.str = "Int64",
                       .source_type = iceberg::int64(),
                       .source = Literal::Long(1234567890),
                       .expected = Literal::Long(1234567890)},
        TransformParam{.str = "Timestamp",
                       .source_type = iceberg::timestamp(),
                       .source = Literal::Timestamp(1622547800000000),
                       .expected = Literal::Timestamp(1622547800000000)},
        TransformParam{.str = "TimestampTz",
                       .source_type = iceberg::timestamp_tz(),
                       .source = Literal::TimestampTz(1622547800000000),
                       .expected = Literal::TimestampTz(1622547800000000)},
        TransformParam{.str = "Float",
                       .source_type = iceberg::float32(),
                       .source = Literal::Float(3.14),
                       .expected = Literal::Float(3.14)},
        TransformParam{.str = "Double",
                       .source_type = iceberg::float64(),
                       .source = Literal::Double(1.23e-5),
                       .expected = Literal::Double(1.23e-5)},
        TransformParam{.str = "Decimal",
                       .source_type = iceberg::decimal(10, 2),
                       .source = Literal::Decimal(123456, 10, 2),
                       .expected = Literal::Decimal(123456, 10, 2)},
        TransformParam{.str = "String",
                       .source_type = iceberg::string(),
                       .source = Literal::String("Hello, World!"),
                       .expected = Literal::String("Hello, World!")},
        TransformParam{
            .str = "Uuid",
            .source_type = iceberg::uuid(),
            .source = Literal::UUID(
                Uuid::FromString("123e4567-e89b-12d3-a456-426614174000").value()),
            .expected = Literal::UUID(
                Uuid::FromString("123e4567-e89b-12d3-a456-426614174000").value())},
        TransformParam{.str = "Binary",
                       .source_type = iceberg::binary(),
                       .source = Literal::Binary({0x01, 0x02, 0x03}),
                       .expected = Literal::Binary({0x01, 0x02, 0x03})},
        TransformParam{.str = "Fixed",
                       .source_type = iceberg::fixed(3),
                       .source = Literal::Fixed({0x01, 0x02, 0x03}),
                       .expected = Literal::Fixed({0x01, 0x02, 0x03})}),
    [](const ::testing::TestParamInfo<TransformParam>& info) { return info.param.str; });

class BucketTransformTest : public ::testing::TestWithParam<TransformParam> {};

TEST_P(BucketTransformTest, BucketTransform) {
  constexpr int32_t num_buckets = 4;
  auto transform = Transform::Bucket(num_buckets);

  const auto& param = GetParam();
  auto transformPtr = transform->Bind(param.source_type);
  ASSERT_TRUE(transformPtr.has_value()) << "Failed to bind bucket transform";
  auto result = transformPtr.value()->Transform(param.source);
  ASSERT_TRUE(result.has_value())
      << "Failed to transform literal: " << param.source.ToString();

  EXPECT_EQ(result.value(), param.expected)
      << "Unexpected result for source: " << param.source.ToString();
}

INSTANTIATE_TEST_SUITE_P(
    BucketTransformTests, BucketTransformTest,
    ::testing::Values(
        TransformParam{.str = "Int32",
                       .source_type = iceberg::int32(),
                       .source = Literal::Int(34),
                       .expected = Literal::Int(3)},
        TransformParam{.str = "Int64",
                       .source_type = iceberg::int64(),
                       .source = Literal::Long(34),
                       .expected = Literal::Int(3)},
        TransformParam{.str = "Decimal",
                       // 14.20
                       .source_type = iceberg::decimal(4, 2),
                       .source = Literal::Decimal(1420, 4, 2),
                       .expected = Literal::Int(3)},
        TransformParam{.str = "Date",
                       // 2017-11-16
                       .source_type = iceberg::date(),
                       .source = Literal::Date(17486),
                       .expected = Literal::Int(2)},
        TransformParam{.str = "Time",
                       // 22:31:08 in microseconds
                       .source_type = iceberg::time(),
                       .source = Literal::Time(81068000000),
                       .expected = Literal::Int(3)},
        TransformParam{.str = "Timestamp",
                       // 2017-11-16T22:31:08 in microseconds
                       .source_type = iceberg::timestamp(),
                       .source = Literal::Timestamp(1510871468000000),
                       .expected = Literal::Int(3)},
        TransformParam{.str = "TimestampTz",
                       // 2017-11-16T22:31:08.000001 in microseconds
                       .source_type = iceberg::timestamp_tz(),
                       .source = Literal::TimestampTz(1510871468000001),
                       .expected = Literal::Int(2)},
        TransformParam{.str = "String",
                       .source_type = iceberg::string(),
                       .source = Literal::String("iceberg"),
                       .expected = Literal::Int(1)},
        TransformParam{
            .str = "Uuid",
            .source_type = iceberg::uuid(),
            .source = Literal::UUID(
                Uuid::FromString("f79c3e09-677c-4bbd-a479-3f349cb785e7").value()),
            .expected = Literal::Int(0)},
        TransformParam{.str = "Fixed",
                       .source_type = iceberg::fixed(4),
                       .source = Literal::Fixed({0, 1, 2, 3}),
                       .expected = Literal::Int(1)},
        TransformParam{.str = "Binary",
                       .source_type = iceberg::binary(),
                       .source = Literal::Binary({0, 1, 2, 3}),
                       .expected = Literal::Int(1)}),
    [](const ::testing::TestParamInfo<TransformParam>& info) { return info.param.str; });

class TruncateTransformTest : public ::testing::TestWithParam<TransformParam> {};

TEST_P(TruncateTransformTest, TruncateTransform) {
  const auto& param = GetParam();
  auto transform = Transform::Truncate(param.param);
  auto transformPtr = transform->Bind(param.source_type);
  ASSERT_TRUE(transformPtr.has_value()) << "Failed to bind truncate transform";
  auto result = transformPtr.value()->Transform(param.source);
  ASSERT_TRUE(result.has_value())
      << "Failed to transform literal: " << param.source.ToString();

  EXPECT_EQ(result.value(), param.expected)
      << "Unexpected result for source: " << param.source.ToString();
}

INSTANTIATE_TEST_SUITE_P(
    TruncateTransformTests, TruncateTransformTest,
    ::testing::Values(
        TransformParam{.str = "Int32",
                       .param = 5,
                       .source_type = iceberg::int32(),
                       .source = Literal::Int(123456),
                       .expected = Literal::Int(123455)},
        TransformParam{.str = "Int64",
                       .param = 10,
                       .source_type = iceberg::int64(),
                       .source = Literal::Long(-1),
                       .expected = Literal::Long(-10)},
        TransformParam{.str = "Decimal",
                       .param = 50,
                       .source_type = iceberg::decimal(5, 2),
                       .source = Literal::Decimal(12345, 5, 2),
                       .expected = Literal::Decimal(12300, 5, 2)},
        TransformParam{.str = "StringShort",
                       .param = 5,
                       .source_type = iceberg::string(),
                       .source = Literal::String("Hello, World!"),
                       .expected = Literal::String("Hello")},
        TransformParam{.str = "StringEmoji",
                       .param = 5,
                       .source_type = iceberg::string(),
                       .source = Literal::String("😜🧐🤔🤪🥳😵‍💫😂"),
                       .expected = Literal::String("😜🧐🤔🤪🥳")},
        TransformParam{.str = "StringMixed",
                       .param = 8,
                       .source_type = iceberg::string(),
                       .source = Literal::String("a😜b🧐c🤔d🤪e🥳"),
                       .expected = Literal::String("a😜b🧐c🤔d🤪")},
        TransformParam{.str = "Binary",
                       .param = 5,
                       .source_type = iceberg::binary(),
                       .source = Literal::Binary({0x01, 0x02, 0x03, 0x04, 0x05, 0x06}),
                       .expected = Literal::Binary({0x01, 0x02, 0x03, 0x04, 0x05})}),
    [](const ::testing::TestParamInfo<TransformParam>& info) { return info.param.str; });

class YearTransformTest : public ::testing::TestWithParam<TransformParam> {};

TEST_P(YearTransformTest, YearTransform) {
  auto transform = Transform::Year();
  const auto& param = GetParam();

  auto transformPtr = transform->Bind(param.source_type);
  ASSERT_TRUE(transformPtr.has_value()) << "Failed to bind year transform";

  auto result = transformPtr.value()->Transform(param.source);
  ASSERT_TRUE(result.has_value())
      << "Failed to transform literal: " << param.source.ToString();

  EXPECT_EQ(result.value(), param.expected)
      << "Unexpected result for source: " << param.source.ToString();
}

INSTANTIATE_TEST_SUITE_P(
    YearTransformTests, YearTransformTest,
    ::testing::Values(TransformParam{.str = "Timestamp",
                                     // 2021-06-01T11:43:20Z
                                     .source_type = iceberg::timestamp(),
                                     .source = Literal::Timestamp(1622547800000000),
                                     .expected = Literal::Int(2021)},
                      TransformParam{.str = "TimestampTz",
                                     .source_type = iceberg::timestamp_tz(),
                                     .source = Literal::TimestampTz(1622547800000000),
                                     .expected = Literal::Int(2021)},
                      TransformParam{.str = "Date",
                                     .source_type = iceberg::date(),
                                     .source = Literal::Date(30000),
                                     .expected = Literal::Int(2052)}),
    [](const ::testing::TestParamInfo<TransformParam>& info) { return info.param.str; });

class MonthTransformTest : public ::testing::TestWithParam<TransformParam> {};

TEST_P(MonthTransformTest, MonthTransform) {
  auto transform = Transform::Month();
  const auto& param = GetParam();

  auto transformPtr = transform->Bind(param.source_type);
  ASSERT_TRUE(transformPtr.has_value()) << "Failed to bind month transform";

  auto result = transformPtr.value()->Transform(param.source);
  ASSERT_TRUE(result.has_value())
      << "Failed to transform literal: " << param.source.ToString();

  EXPECT_EQ(result.value(), param.expected)
      << "Unexpected result for source: " << param.source.ToString();
}

INSTANTIATE_TEST_SUITE_P(
    MonthTransformTests, MonthTransformTest,
    ::testing::Values(TransformParam{.str = "Timestamp",
                                     .source_type = iceberg::timestamp(),
                                     .source = Literal::Timestamp(1622547800000000),
                                     .expected = Literal::Int(617)},
                      TransformParam{.str = "TimestampTz",
                                     .source_type = iceberg::timestamp_tz(),
                                     .source = Literal::TimestampTz(1622547800000000),
                                     .expected = Literal::Int(617)},
                      TransformParam{.str = "Date",
                                     .source_type = iceberg::date(),
                                     .source = Literal::Date(30000),
                                     .expected = Literal::Int(985)}),
    [](const ::testing::TestParamInfo<TransformParam>& info) { return info.param.str; });

class DayTransformTest : public ::testing::TestWithParam<TransformParam> {};

TEST_P(DayTransformTest, DayTransform) {
  auto transform = Transform::Day();
  const auto& param = GetParam();

  auto transformPtr = transform->Bind(param.source_type);
  ASSERT_TRUE(transformPtr.has_value()) << "Failed to bind day transform";

  auto result = transformPtr.value()->Transform(param.source);
  ASSERT_TRUE(result.has_value())
      << "Failed to transform literal: " << param.source.ToString();

  EXPECT_EQ(result.value(), param.expected)
      << "Unexpected result for source: " << param.source.ToString();
}

INSTANTIATE_TEST_SUITE_P(
    DayTransformTests, DayTransformTest,
    ::testing::Values(TransformParam{.str = "Timestamp",
                                     .source_type = iceberg::timestamp(),
                                     .source = Literal::Timestamp(1622547800000000),
                                     .expected = Literal::Int(18779)},
                      TransformParam{.str = "TimestampTz",
                                     .source_type = iceberg::timestamp_tz(),
                                     .source = Literal::TimestampTz(1622547800000000),
                                     .expected = Literal::Int(18779)},
                      TransformParam{.str = "Date",
                                     .source_type = iceberg::date(),
                                     .source = Literal::Date(30000),
                                     .expected = Literal::Int(30000)}),
    [](const ::testing::TestParamInfo<TransformParam>& info) { return info.param.str; });

class HourTransformTest : public ::testing::TestWithParam<TransformParam> {};

TEST_P(HourTransformTest, HourTransform) {
  auto transform = Transform::Hour();
  const auto& param = GetParam();

  auto transformPtr = transform->Bind(param.source_type);
  ASSERT_TRUE(transformPtr.has_value()) << "Failed to bind hour transform";

  auto result = transformPtr.value()->Transform(param.source);
  ASSERT_TRUE(result.has_value())
      << "Failed to transform literal: " << param.source.ToString();

  EXPECT_EQ(result.value(), param.expected)
      << "Unexpected result for source: " << param.source.ToString();
}

INSTANTIATE_TEST_SUITE_P(
    HourTransformTests, HourTransformTest,
    ::testing::Values(TransformParam{.str = "Timestamp",
                                     .source_type = iceberg::timestamp(),
                                     .source = Literal::Timestamp(1622547800000000),
                                     .expected = Literal::Int(450707)},
                      TransformParam{.str = "TimestampTz",
                                     .source_type = iceberg::timestamp_tz(),
                                     .source = Literal::TimestampTz(1622547800000000),
                                     .expected = Literal::Int(450707)}),
    [](const ::testing::TestParamInfo<TransformParam>& info) { return info.param.str; });

class VoidTransformTest : public ::testing::TestWithParam<TransformParam> {};

TEST_P(VoidTransformTest, VoidTransform) {
  auto transform = Transform::Void();
  const auto& param = GetParam();

  auto transformPtr = transform->Bind(param.source_type);
  ASSERT_TRUE(transformPtr.has_value()) << "Failed to bind void transform";

  auto result = transformPtr.value()->Transform(param.source);
  EXPECT_TRUE(result->IsNull())
      << "Expected void transform to return null type for source: "
      << param.source.ToString();
  EXPECT_EQ(result->type()->type_id(), param.source_type->type_id())
      << "Expected void transform to return same type as source for: "
      << param.source.ToString();
  EXPECT_EQ(result->ToString(), param.expected.ToString())
      << "Unexpected result for source: " << param.source.ToString();
}

INSTANTIATE_TEST_SUITE_P(
    VoidTransformTests, VoidTransformTest,
    ::testing::Values(
        TransformParam{.str = "Boolean",
                       .source_type = iceberg::boolean(),
                       .source = Literal::Boolean(true),
                       .expected = Literal::Null(iceberg::boolean())},
        TransformParam{.str = "Int32",
                       .source_type = iceberg::int32(),
                       .source = Literal::Int(42),
                       .expected = Literal::Null(iceberg::int32())},
        TransformParam{.str = "Date",
                       .source_type = iceberg::date(),
                       .source = Literal::Date(30000),
                       .expected = Literal::Null(iceberg::date())},
        TransformParam{.str = "Int64",
                       .source_type = iceberg::int64(),
                       .source = Literal::Long(1234567890),
                       .expected = Literal::Null(iceberg::int64())},
        TransformParam{.str = "Timestamp",
                       .source_type = iceberg::timestamp(),
                       .source = Literal::Timestamp(1622547800000000),
                       .expected = Literal::Null(iceberg::timestamp())},
        TransformParam{.str = "TimestampTz",
                       .source_type = iceberg::timestamp_tz(),
                       .source = Literal::TimestampTz(1622547800000000),
                       .expected = Literal::Null(iceberg::timestamp_tz())},
        TransformParam{.str = "Float",
                       .source_type = iceberg::float32(),
                       .source = Literal::Float(3.14),
                       .expected = Literal::Null(iceberg::float32())},
        TransformParam{.str = "Double",
                       .source_type = iceberg::float64(),
                       .source = Literal::Double(1.23e-5),
                       .expected = Literal::Null(iceberg::float64())},
        TransformParam{.str = "Decimal",
                       .source_type = iceberg::decimal(10, 2),
                       .source = Literal::Decimal(123456, 10, 2),
                       .expected = Literal::Null(iceberg::decimal(10, 2))},
        TransformParam{.str = "String",
                       .source_type = iceberg::string(),
                       .source = Literal::String("Hello, World!"),
                       .expected = Literal::Null(iceberg::string())},
        TransformParam{
            .str = "Uuid",
            .source_type = iceberg::uuid(),
            .source = Literal::UUID(
                Uuid::FromString("123e4567-e89b-12d3-a456-426614174000").value()),
            .expected = Literal::Null(iceberg::uuid())},
        TransformParam{.str = "Binary",
                       .source_type = iceberg::binary(),
                       .source = Literal::Binary({0x01, 0x02, 0x03}),
                       .expected = Literal::Null(iceberg::binary())},
        TransformParam{.str = "Fixed",
                       .source_type = iceberg::fixed(3),
                       .source = Literal::Fixed({0x01, 0x02, 0x03}),
                       .expected = Literal::Null(iceberg::fixed(3))}),
    [](const ::testing::TestParamInfo<TransformParam>& info) { return info.param.str; });

class NullLiteralTransformTest : public ::testing::TestWithParam<TransformParam> {};

TEST_P(NullLiteralTransformTest, NullLiteralTransform) {
  const auto& param = GetParam();

  auto result = TransformFromString(param.str);
  ASSERT_TRUE(result.has_value()) << "Failed to parse: " << param.str;

  const auto& transform = result.value();
  const auto transformPtr = transform->Bind(param.source_type);
  ASSERT_TRUE(transformPtr.has_value()) << "Failed to bind: " << param.str;

  auto transform_result = transformPtr.value()->Transform(param.source);
  EXPECT_TRUE(transform_result->IsNull())
      << "Expected transform to return null type for source: " << param.source.ToString();
  EXPECT_EQ(transform_result->ToString(), param.expected.ToString())
      << "Unexpected result for source: " << param.source.ToString();
}

INSTANTIATE_TEST_SUITE_P(
    NullLiteralTransformTests, NullLiteralTransformTest,
    ::testing::Values(TransformParam{.str = "identity",
                                     .source_type = iceberg::string(),
                                     .source = Literal::Null(iceberg::string()),
                                     .expected = Literal::Null(iceberg::string())},
                      TransformParam{.str = "year",
                                     .source_type = iceberg::timestamp(),
                                     .source = Literal::Null(iceberg::timestamp()),
                                     .expected = Literal::Null(iceberg::int32())},
                      TransformParam{.str = "month",
                                     .source_type = iceberg::timestamp(),
                                     .source = Literal::Null(iceberg::timestamp()),
                                     .expected = Literal::Null(iceberg::int32())},
                      TransformParam{.str = "day",
                                     .source_type = iceberg::timestamp(),
                                     .source = Literal::Null(iceberg::timestamp()),
                                     .expected = Literal::Null(iceberg::int32())},
                      TransformParam{.str = "hour",
                                     .source_type = iceberg::timestamp(),
                                     .source = Literal::Null(iceberg::timestamp()),
                                     .expected = Literal::Null(iceberg::int32())},
                      TransformParam{.str = "void",
                                     .source_type = iceberg::string(),
                                     .source = Literal::Null(iceberg::string()),
                                     .expected = Literal::Null(iceberg::string())},
                      TransformParam{.str = "bucket[16]",
                                     .source_type = iceberg::string(),
                                     .source = Literal::Null(iceberg::string()),
                                     .expected = Literal::Null(iceberg::int32())},
                      TransformParam{.str = "truncate[32]",
                                     .source_type = iceberg::string(),
                                     .source = Literal::Null(iceberg::string()),
                                     .expected = Literal::Null(iceberg::string())}));

}  // namespace iceberg
