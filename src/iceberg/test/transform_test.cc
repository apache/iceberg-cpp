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

#include "iceberg/expression/expressions.h"
#include "iceberg/expression/literal.h"
#include "iceberg/expression/predicate.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/temporal_test_helper.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep
#include "iceberg/util/macros.h"

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
       .expected_result_type = iceberg::date()},
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
                       .source_type = iceberg::date(),
                       .source = Literal::Date(TemporalTestHelper::CreateDate(
                           {.year = 2017, .month = 11, .day = 16})),
                       .expected = Literal::Int(2)},
        TransformParam{.str = "Time",
                       .source_type = iceberg::time(),
                       .source = Literal::Time(TemporalTestHelper::CreateTime(
                           {.hour = 22, .minute = 31, .second = 8})),
                       .expected = Literal::Int(3)},
        TransformParam{.str = "Timestamp",
                       // 2017-11-16T22:31:08 in microseconds
                       .source_type = iceberg::timestamp(),
                       .source = Literal::Timestamp(
                           TemporalTestHelper::CreateTimestamp({.year = 2017,
                                                                .month = 11,
                                                                .day = 16,
                                                                .hour = 22,
                                                                .minute = 31,
                                                                .second = 8})),
                       .expected = Literal::Int(3)},
        TransformParam{
            .str = "TimestampTz",
            // 2017-11-16T14:31:08.000001-08:00 in microseconds
            .source_type = iceberg::timestamp_tz(),
            .source = Literal::TimestampTz(
                TemporalTestHelper::CreateTimestampTz({.year = 2017,
                                                       .month = 11,
                                                       .day = 16,
                                                       .hour = 14,
                                                       .minute = 31,
                                                       .second = 8,
                                                       .microsecond = 1,
                                                       .tz_offset_minutes = -480})),
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
    ::testing::Values(
        TransformParam{.str = "Timestamp",
                       // 2021-06-01T11:43:20Z
                       .source_type = iceberg::timestamp(),
                       .source = Literal::Timestamp(
                           TemporalTestHelper::CreateTimestamp({.year = 2021,
                                                                .month = 6,
                                                                .day = 1,
                                                                .hour = 11,
                                                                .minute = 43,
                                                                .second = 20})),
                       .expected = Literal::Int(2021)},
        TransformParam{
            .str = "TimestampTz",
            // 2021-01-01T07:43:20+08:00, which is 2020-12-31T23:43:20Z
            .source_type = iceberg::timestamp_tz(),
            .source = Literal::TimestampTz(
                TemporalTestHelper::CreateTimestampTz({.year = 2021,
                                                       .month = 1,
                                                       .day = 1,
                                                       .hour = 7,
                                                       .minute = 43,
                                                       .second = 20,
                                                       .tz_offset_minutes = 480})),
            .expected = Literal::Int(2020)},
        TransformParam{.str = "Date",
                       .source_type = iceberg::date(),
                       .source = Literal::Date(TemporalTestHelper::CreateDate(
                           {.year = 2052, .month = 2, .day = 20})),
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
    ::testing::Values(
        TransformParam{.str = "Timestamp",
                       .source_type = iceberg::timestamp(),
                       .source = Literal::Timestamp(
                           TemporalTestHelper::CreateTimestamp({.year = 2021,
                                                                .month = 6,
                                                                .day = 1,
                                                                .hour = 11,
                                                                .minute = 43,
                                                                .second = 20})),
                       .expected = Literal::Int(TemporalTestHelper::CreateDate(
                           {.year = 2021, .month = 6, .day = 1}))},
        TransformParam{
            .str = "TimestampTz",
            .source_type = iceberg::timestamp_tz(),
            .source = Literal::TimestampTz(
                TemporalTestHelper::CreateTimestampTz({.year = 2021,
                                                       .month = 1,
                                                       .day = 1,
                                                       .hour = 7,
                                                       .minute = 43,
                                                       .second = 20,
                                                       .tz_offset_minutes = 480})),
            .expected = Literal::Int(
                TemporalTestHelper::CreateDate({.year = 2020, .month = 12, .day = 31}))},
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

TEST(TransformPreservesOrderTest, PreservesOrder) {
  struct Case {
    std::string transform_str;
    bool expected;
  };

  const std::vector<Case> cases = {
      {.transform_str = "identity", .expected = true},
      {.transform_str = "year", .expected = true},
      {.transform_str = "month", .expected = true},
      {.transform_str = "day", .expected = true},
      {.transform_str = "hour", .expected = true},
      {.transform_str = "void", .expected = false},
      {.transform_str = "bucket[16]", .expected = false},
      {.transform_str = "truncate[32]", .expected = true},
  };

  for (const auto& c : cases) {
    auto transform = TransformFromString(c.transform_str);
    ASSERT_TRUE(transform.has_value()) << "Failed to parse: " << c.transform_str;

    EXPECT_EQ(transform.value()->PreservesOrder(), c.expected)
        << "Unexpected result for transform: " << c.transform_str;
  }
}

TEST(TransformSatisfiesOrderOfTest, SatisfiesOrderOf) {
  struct Case {
    std::string transform_str;
    std::string other_transform_str;
    bool expected;
  };

  const std::vector<Case> cases = {
      // Identity satisfies all order-preserving transforms
      {.transform_str = "identity", .other_transform_str = "identity", .expected = true},
      {.transform_str = "identity", .other_transform_str = "year", .expected = true},
      {.transform_str = "identity", .other_transform_str = "month", .expected = true},
      {.transform_str = "identity", .other_transform_str = "day", .expected = true},
      {.transform_str = "identity", .other_transform_str = "hour", .expected = true},
      {.transform_str = "identity",
       .other_transform_str = "truncate[32]",
       .expected = true},
      {.transform_str = "identity",
       .other_transform_str = "bucket[16]",
       .expected = false},

      // Truncate satisfies Truncate with smaller width
      {.transform_str = "truncate[32]",
       .other_transform_str = "truncate[16]",
       .expected = true},
      {.transform_str = "truncate[16]",
       .other_transform_str = "truncate[16]",
       .expected = true},
      {.transform_str = "truncate[16]",
       .other_transform_str = "truncate[32]",
       .expected = false},
      {.transform_str = "truncate[16]",
       .other_transform_str = "bucket[32]",
       .expected = false},

      // Hour satisfies hour, day, month, and year
      {.transform_str = "hour", .other_transform_str = "hour", .expected = true},
      {.transform_str = "hour", .other_transform_str = "day", .expected = true},
      {.transform_str = "hour", .other_transform_str = "month", .expected = true},
      {.transform_str = "hour", .other_transform_str = "year", .expected = true},
      {.transform_str = "hour", .other_transform_str = "identity", .expected = false},
      {.transform_str = "hour", .other_transform_str = "bucket[16]", .expected = false},

      // Day satisfies day, month, and year
      {.transform_str = "day", .other_transform_str = "day", .expected = true},
      {.transform_str = "day", .other_transform_str = "month", .expected = true},
      {.transform_str = "day", .other_transform_str = "year", .expected = true},
      {.transform_str = "day", .other_transform_str = "hour", .expected = false},
      {.transform_str = "day", .other_transform_str = "identity", .expected = false},

      // Month satisfies month and year
      {.transform_str = "month", .other_transform_str = "month", .expected = true},
      {.transform_str = "month", .other_transform_str = "year", .expected = true},
      {.transform_str = "month", .other_transform_str = "day", .expected = false},
      {.transform_str = "month", .other_transform_str = "hour", .expected = false},

      // Year satisfies only year
      {.transform_str = "year", .other_transform_str = "year", .expected = true},
      {.transform_str = "year", .other_transform_str = "month", .expected = false},
      {.transform_str = "year", .other_transform_str = "day", .expected = false},
      {.transform_str = "year", .other_transform_str = "hour", .expected = false},

      // Void satisfies no order-preserving transforms
      {.transform_str = "void", .other_transform_str = "identity", .expected = false},
      {.transform_str = "void", .other_transform_str = "year", .expected = false},
      {.transform_str = "void", .other_transform_str = "month", .expected = false},
      {.transform_str = "void", .other_transform_str = "day", .expected = false},
      {.transform_str = "void", .other_transform_str = "hour", .expected = false},

      // Bucket satisfies only itself
      {.transform_str = "bucket[16]",
       .other_transform_str = "bucket[16]",
       .expected = true},
      {.transform_str = "bucket[16]",
       .other_transform_str = "bucket[32]",
       .expected = false},
      {.transform_str = "bucket[16]",
       .other_transform_str = "identity",
       .expected = false},
  };

  for (const auto& c : cases) {
    auto transform = TransformFromString(c.transform_str);
    auto other_transform = TransformFromString(c.other_transform_str);

    ASSERT_TRUE(transform.has_value()) << "Failed to parse: " << c.transform_str;
    ASSERT_TRUE(other_transform.has_value())
        << "Failed to parse: " << c.other_transform_str;

    EXPECT_EQ(transform.value()->SatisfiesOrderOf(*other_transform.value()), c.expected)
        << "Unexpected result for transform: " << c.transform_str
        << " and other transform: " << c.other_transform_str;
  }
}

TEST(TransformCanTransformTest, CanTransform) {
  struct Case {
    std::string transform_str;
    std::shared_ptr<Type> source_type;
    bool expected;
  };

  const std::vector<Case> cases = {
      // Identity can transform all primitive types
      {.transform_str = "identity", .source_type = int32(), .expected = true},
      {.transform_str = "identity", .source_type = string(), .expected = true},
      {.transform_str = "identity", .source_type = boolean(), .expected = true},
      {.transform_str = "identity",
       .source_type = list(SchemaField(123, "element", int32(), false)),
       .expected = false},

      // Void can transform any type
      {.transform_str = "void", .source_type = iceberg::int32(), .expected = true},
      {.transform_str = "void",
       .source_type = iceberg::map(SchemaField(123, "key", iceberg::string(), false),
                                   SchemaField(124, "value", iceberg::int32(), true)),
       .expected = true},

      // Bucket can transform specific types
      {.transform_str = "bucket[16]", .source_type = iceberg::int32(), .expected = true},
      {.transform_str = "bucket[16]", .source_type = iceberg::string(), .expected = true},
      {.transform_str = "bucket[16]",
       .source_type = iceberg::float32(),
       .expected = false},

      // Truncate can transform specific types
      {.transform_str = "truncate[32]",
       .source_type = iceberg::int32(),
       .expected = true},
      {.transform_str = "truncate[32]",
       .source_type = iceberg::string(),
       .expected = true},
      {.transform_str = "truncate[32]",
       .source_type = iceberg::boolean(),
       .expected = false},

      // Year can transform date and timestamp types
      {.transform_str = "year", .source_type = iceberg::date(), .expected = true},
      {.transform_str = "year", .source_type = iceberg::timestamp(), .expected = true},
      {.transform_str = "year", .source_type = iceberg::string(), .expected = false},

      // Month can transform date and timestamp types
      {.transform_str = "month", .source_type = iceberg::date(), .expected = true},
      {.transform_str = "month", .source_type = iceberg::timestamp(), .expected = true},
      {.transform_str = "month", .source_type = iceberg::binary(), .expected = false},

      // Day can transform date and timestamp types
      {.transform_str = "day", .source_type = iceberg::date(), .expected = true},
      {.transform_str = "day", .source_type = iceberg::timestamp(), .expected = true},
      {.transform_str = "day", .source_type = iceberg::uuid(), .expected = false},

      // Hour can transform timestamp types
      {.transform_str = "hour", .source_type = iceberg::timestamp(), .expected = true},
      {.transform_str = "hour", .source_type = iceberg::timestamp_tz(), .expected = true},
      {.transform_str = "hour", .source_type = iceberg::int32(), .expected = false},
  };

  for (const auto& c : cases) {
    auto transform = TransformFromString(c.transform_str);
    ASSERT_TRUE(transform.has_value()) << "Failed to parse: " << c.transform_str;

    EXPECT_EQ(transform.value()->CanTransform(*c.source_type), c.expected)
        << "Unexpected result for transform: " << c.transform_str
        << " and source type: " << c.source_type->ToString();
  }
}

// Test fixture for Transform::Project tests
class TransformProjectTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create test schemas for different source types
    int_schema_ = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "key", int32()),
                                 SchemaField::MakeOptional(2, "value", int32())},
        /*schema_id=*/0);
    long_schema_ = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "value", int64())},
        /*schema_id=*/0);
    string_schema_ = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "value", string())},
        /*schema_id=*/0);
    date_schema_ = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "value", date())},
        /*schema_id=*/0);
    timestamp_schema_ = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "value", timestamp())},
        /*schema_id=*/0);
  }

  std::shared_ptr<Schema> int_schema_;
  std::shared_ptr<Schema> long_schema_;
  std::shared_ptr<Schema> string_schema_;
  std::shared_ptr<Schema> date_schema_;
  std::shared_ptr<Schema> timestamp_schema_;
};

TEST_F(TransformProjectTest, IdentityProjectEquality) {
  auto transform = Transform::Identity();

  // Test equality predicate
  auto unbound = Expressions::Equal("value", Literal::Int(100));
  ICEBERG_ASSIGN_OR_THROW(auto bound,
                          unbound->Bind(*int_schema_, /*case_sensitive=*/true));
  auto bound_pred = std::dynamic_pointer_cast<BoundPredicate>(bound);
  ASSERT_NE(bound_pred, nullptr);

  ICEBERG_ASSIGN_OR_THROW(auto projected, transform->Project("part", bound_pred));
  ASSERT_NE(projected, nullptr);
  EXPECT_EQ(projected->op(), Expression::Operation::kEq);

  auto unbound_projected =
      internal::checked_pointer_cast<UnboundPredicateImpl<BoundReference>>(
          std::move(projected));
  ASSERT_NE(unbound_projected, nullptr);
  EXPECT_EQ(unbound_projected->op(), Expression::Operation::kEq);
  EXPECT_EQ(unbound_projected->literals().size(), 1);
  EXPECT_EQ(std::get<int32_t>(unbound_projected->literals().front().value()), 100);
}

TEST_F(TransformProjectTest, IdentityProjectComparison) {
  auto transform = Transform::Identity();

  // Test less than predicate
  auto unbound_lt = Expressions::LessThan("value", Literal::Int(50));
  ICEBERG_ASSIGN_OR_THROW(auto bound_lt,
                          unbound_lt->Bind(*int_schema_, /*case_sensitive=*/true));
  auto bound_pred_lt = std::dynamic_pointer_cast<BoundPredicate>(bound_lt);
  ASSERT_NE(bound_pred_lt, nullptr);

  ICEBERG_ASSIGN_OR_THROW(auto projected_lt, transform->Project("part", bound_pred_lt));
  ASSERT_NE(projected_lt, nullptr);
  EXPECT_EQ(projected_lt->op(), Expression::Operation::kLt);

  // Test greater than or equal predicate
  auto unbound_gte = Expressions::GreaterThanOrEqual("value", Literal::Int(100));
  ICEBERG_ASSIGN_OR_THROW(auto bound_gte,
                          unbound_gte->Bind(*int_schema_, /*case_sensitive=*/true));
  auto bound_pred_gte = std::dynamic_pointer_cast<BoundPredicate>(bound_gte);
  ASSERT_NE(bound_pred_gte, nullptr);

  ICEBERG_ASSIGN_OR_THROW(auto projected_gte, transform->Project("part", bound_pred_gte));
  ASSERT_NE(projected_gte, nullptr);
  EXPECT_EQ(projected_gte->op(), Expression::Operation::kGtEq);
}

TEST_F(TransformProjectTest, IdentityProjectUnary) {
  auto transform = Transform::Identity();

  // Test IsNull predicate
  auto unbound_null = Expressions::IsNull("value");
  ICEBERG_ASSIGN_OR_THROW(auto bound_null,
                          unbound_null->Bind(*int_schema_, /*case_sensitive=*/true));
  auto bound_pred_null = std::dynamic_pointer_cast<BoundPredicate>(bound_null);
  ASSERT_NE(bound_pred_null, nullptr);

  ICEBERG_ASSIGN_OR_THROW(auto projected_null,
                          transform->Project("part", bound_pred_null));
  ASSERT_NE(projected_null, nullptr);
  EXPECT_EQ(projected_null->op(), Expression::Operation::kIsNull);
}

TEST_F(TransformProjectTest, IdentityProjectSet) {
  auto transform = Transform::Identity();

  // Test IN predicate
  auto unbound_in =
      Expressions::In("value", {Literal::Int(1), Literal::Int(2), Literal::Int(3)});
  ICEBERG_ASSIGN_OR_THROW(auto bound_in,
                          unbound_in->Bind(*int_schema_, /*case_sensitive=*/true));
  auto bound_pred_in = std::dynamic_pointer_cast<BoundPredicate>(bound_in);
  ASSERT_NE(bound_pred_in, nullptr);

  ICEBERG_ASSIGN_OR_THROW(auto projected_in, transform->Project("part", bound_pred_in));
  ASSERT_NE(projected_in, nullptr);
  EXPECT_EQ(projected_in->op(), Expression::Operation::kIn);
  auto unbound_projected =
      internal::checked_pointer_cast<UnboundPredicateImpl<BoundReference>>(
          std::move(projected_in));
  ASSERT_NE(unbound_projected, nullptr);
  EXPECT_EQ(unbound_projected->op(), Expression::Operation::kIn);
  EXPECT_EQ(unbound_projected->literals().size(), 3);
  std::vector<int32_t> values;
  for (const auto& lit : unbound_projected->literals()) {
    values.push_back(std::get<int32_t>(lit.value()));
  }
  EXPECT_THAT(values, testing::UnorderedElementsAre(1, 2, 3));
}

TEST_F(TransformProjectTest, BucketProjectEquality) {
  auto transform = Transform::Bucket(4);

  // Bucket can project equality predicates
  auto unbound = Expressions::Equal("value", Literal::Int(34));
  ICEBERG_ASSIGN_OR_THROW(auto bound,
                          unbound->Bind(*int_schema_, /*case_sensitive=*/true));
  auto bound_pred = std::dynamic_pointer_cast<BoundPredicate>(bound);
  ASSERT_NE(bound_pred, nullptr);

  ICEBERG_ASSIGN_OR_THROW(auto projected, transform->Project("part", bound_pred));
  ASSERT_NE(projected, nullptr);
  EXPECT_EQ(projected->op(), Expression::Operation::kEq);

  auto unbound_projected =
      internal::checked_pointer_cast<UnboundPredicateImpl<BoundReference>>(
          std::move(projected));
  ASSERT_NE(unbound_projected, nullptr);
  EXPECT_EQ(unbound_projected->op(), Expression::Operation::kEq);
  EXPECT_EQ(unbound_projected->literals().size(), 1);
  EXPECT_EQ(std::get<int32_t>(unbound_projected->literals().front().value()), 3);
}

TEST_F(TransformProjectTest, BucketProjectWithMatchingTransformedChild) {
  auto partition_transform = Transform::Bucket(16);

  // Create a predicate like: bucket(value, 16) = 5
  auto bucket_term = Expressions::Bucket("value", 16);
  auto unbound = Expressions::Equal<BoundTransform>(bucket_term, Literal::Int(5));
  ICEBERG_ASSIGN_OR_THROW(auto bound,
                          unbound->Bind(*int_schema_, /*case_sensitive=*/true));
  auto bound_pred = std::dynamic_pointer_cast<BoundPredicate>(bound);
  ASSERT_NE(bound_pred, nullptr);

  // The predicate's term should be a transform
  EXPECT_EQ(bound_pred->term()->kind(), Term::Kind::kTransform);

  // When the transform matches, Project should use RemoveTransform and return the
  // predicate
  ICEBERG_ASSIGN_OR_THROW(auto projected,
                          partition_transform->Project("part", bound_pred));
  ASSERT_NE(projected, nullptr);
  EXPECT_EQ(projected->op(), Expression::Operation::kEq);
  auto unbound_projected =
      internal::checked_pointer_cast<UnboundPredicateImpl<BoundReference>>(
          std::move(projected));
  ASSERT_NE(unbound_projected, nullptr);
  EXPECT_EQ(unbound_projected->op(), Expression::Operation::kEq);
  EXPECT_EQ(unbound_projected->literals().size(), 1);
  EXPECT_EQ(std::get<int32_t>(unbound_projected->literals().front().value()), 5);
}

TEST_F(TransformProjectTest, BucketProjectComparisonReturnsNull) {
  auto transform = Transform::Bucket(16);

  // Bucket cannot project comparison predicates (they return null)
  auto unbound_lt = Expressions::LessThan("value", Literal::Int(50));
  ICEBERG_ASSIGN_OR_THROW(auto bound_lt,
                          unbound_lt->Bind(*int_schema_, /*case_sensitive=*/true));
  auto bound_pred_lt = std::dynamic_pointer_cast<BoundPredicate>(bound_lt);
  ASSERT_NE(bound_pred_lt, nullptr);

  ICEBERG_ASSIGN_OR_THROW(auto projected_lt, transform->Project("part", bound_pred_lt));
  EXPECT_EQ(projected_lt, nullptr);
}

TEST_F(TransformProjectTest, BucketProjectInSet) {
  auto transform = Transform::Bucket(16);

  // Bucket can project IN predicates
  auto unbound_in =
      Expressions::In("value", {Literal::Int(1), Literal::Int(2), Literal::Int(3)});
  ICEBERG_ASSIGN_OR_THROW(auto bound_in,
                          unbound_in->Bind(*int_schema_, /*case_sensitive=*/true));
  auto bound_pred_in = std::dynamic_pointer_cast<BoundPredicate>(bound_in);
  ASSERT_NE(bound_pred_in, nullptr);

  ICEBERG_ASSIGN_OR_THROW(auto projected_in, transform->Project("part", bound_pred_in));
  ASSERT_NE(projected_in, nullptr);
  EXPECT_EQ(projected_in->op(), Expression::Operation::kIn);
}

TEST_F(TransformProjectTest, BucketProjectNotInReturnsNull) {
  auto transform = Transform::Bucket(16);

  // Bucket cannot project NOT IN predicates
  auto unbound_not_in =
      Expressions::NotIn("value", {Literal::Int(1), Literal::Int(2), Literal::Int(3)});
  ICEBERG_ASSIGN_OR_THROW(auto bound_not_in,
                          unbound_not_in->Bind(*int_schema_, /*case_sensitive=*/true));
  auto bound_pred_not_in = std::dynamic_pointer_cast<BoundPredicate>(bound_not_in);
  ASSERT_NE(bound_pred_not_in, nullptr);

  ICEBERG_ASSIGN_OR_THROW(auto projected_not_in,
                          transform->Project("part", bound_pred_not_in));
  EXPECT_EQ(projected_not_in, nullptr);
}

TEST_F(TransformProjectTest, TruncateProjectIntEquality) {
  auto transform = Transform::Truncate(10);

  // Truncate can project equality predicates
  auto unbound = Expressions::Equal("value", Literal::Int(123));
  ICEBERG_ASSIGN_OR_THROW(auto bound,
                          unbound->Bind(*int_schema_, /*case_sensitive=*/true));
  auto bound_pred = std::dynamic_pointer_cast<BoundPredicate>(bound);
  ASSERT_NE(bound_pred, nullptr);

  ICEBERG_ASSIGN_OR_THROW(auto projected, transform->Project("part", bound_pred));
  ASSERT_NE(projected, nullptr);
  EXPECT_EQ(projected->op(), Expression::Operation::kEq);

  auto unbound_projected =
      internal::checked_pointer_cast<UnboundPredicateImpl<BoundReference>>(
          std::move(projected));
  ASSERT_NE(unbound_projected, nullptr);
  EXPECT_EQ(unbound_projected->op(), Expression::Operation::kEq);
  EXPECT_EQ(unbound_projected->literals().size(), 1);
  EXPECT_EQ(std::get<int32_t>(unbound_projected->literals().front().value()), 120);
}

TEST_F(TransformProjectTest, TruncateProjectIntLessThan) {
  auto transform = Transform::Truncate(10);

  // Truncate projects LT as LTE
  auto unbound = Expressions::LessThan("value", Literal::Int(25));
  ICEBERG_ASSIGN_OR_THROW(auto bound,
                          unbound->Bind(*int_schema_, /*case_sensitive=*/true));
  auto bound_pred = std::dynamic_pointer_cast<BoundPredicate>(bound);
  ASSERT_NE(bound_pred, nullptr);

  ICEBERG_ASSIGN_OR_THROW(auto projected, transform->Project("part", bound_pred));
  ASSERT_NE(projected, nullptr);
  EXPECT_EQ(projected->op(), Expression::Operation::kLtEq);
}

TEST_F(TransformProjectTest, TruncateProjectIntGreaterThan) {
  auto transform = Transform::Truncate(10);

  // Truncate projects GT as GTE
  auto unbound = Expressions::GreaterThan("value", Literal::Int(25));
  ICEBERG_ASSIGN_OR_THROW(auto bound,
                          unbound->Bind(*int_schema_, /*case_sensitive=*/true));
  auto bound_pred = std::dynamic_pointer_cast<BoundPredicate>(bound);
  ASSERT_NE(bound_pred, nullptr);

  ICEBERG_ASSIGN_OR_THROW(auto projected, transform->Project("part", bound_pred));
  ASSERT_NE(projected, nullptr);
  EXPECT_EQ(projected->op(), Expression::Operation::kGtEq);

  auto unbound_projected =
      internal::checked_pointer_cast<UnboundPredicateImpl<BoundReference>>(
          std::move(projected));
  ASSERT_NE(unbound_projected, nullptr);
  EXPECT_EQ(unbound_projected->op(), Expression::Operation::kGtEq);
  EXPECT_EQ(unbound_projected->literals().size(), 1);
  EXPECT_EQ(std::get<int32_t>(unbound_projected->literals().front().value()), 20);
}

TEST_F(TransformProjectTest, TruncateProjectStringEquality) {
  auto transform = Transform::Truncate(5);

  auto unbound = Expressions::Equal("value", Literal::String("Hello, World!"));
  ICEBERG_ASSIGN_OR_THROW(auto bound,
                          unbound->Bind(*string_schema_, /*case_sensitive=*/true));
  auto bound_pred = std::dynamic_pointer_cast<BoundPredicate>(bound);
  ASSERT_NE(bound_pred, nullptr);

  ICEBERG_ASSIGN_OR_THROW(auto projected, transform->Project("part", bound_pred));
  ASSERT_NE(projected, nullptr);
  EXPECT_EQ(projected->op(), Expression::Operation::kEq);

  auto unbound_projected =
      internal::checked_pointer_cast<UnboundPredicateImpl<BoundReference>>(
          std::move(projected));
  ASSERT_NE(unbound_projected, nullptr);
  EXPECT_EQ(unbound_projected->op(), Expression::Operation::kEq);
  EXPECT_EQ(unbound_projected->literals().size(), 1);
  EXPECT_EQ(std::get<std::string>(unbound_projected->literals().front().value()),
            "Hello");
}

TEST_F(TransformProjectTest, TruncateProjectStringStartsWith) {
  auto transform = Transform::Truncate(5);

  // StartsWith with shorter string than width
  auto unbound_short = Expressions::StartsWith("value", "Hi");
  ICEBERG_ASSIGN_OR_THROW(auto bound_short,
                          unbound_short->Bind(*string_schema_, /*case_sensitive=*/true));
  auto bound_pred_short = std::dynamic_pointer_cast<BoundPredicate>(bound_short);
  ASSERT_NE(bound_pred_short, nullptr);

  ICEBERG_ASSIGN_OR_THROW(auto projected_short,
                          transform->Project("part", bound_pred_short));
  ASSERT_NE(projected_short, nullptr);
  EXPECT_EQ(projected_short->op(), Expression::Operation::kStartsWith);

  auto unbound_projected_short =
      internal::checked_pointer_cast<UnboundPredicateImpl<BoundReference>>(
          std::move(projected_short));
  ASSERT_NE(unbound_projected_short, nullptr);
  EXPECT_EQ(unbound_projected_short->op(), Expression::Operation::kStartsWith);
  EXPECT_EQ(unbound_projected_short->literals().size(), 1);
  EXPECT_EQ(std::get<std::string>(unbound_projected_short->literals().front().value()),
            "Hi");

  // StartsWith with string equal to width
  auto unbound_equal = Expressions::StartsWith("value", "Hello");
  ICEBERG_ASSIGN_OR_THROW(auto bound_equal,
                          unbound_equal->Bind(*string_schema_, /*case_sensitive=*/true));
  auto bound_pred_equal = std::dynamic_pointer_cast<BoundPredicate>(bound_equal);
  ASSERT_NE(bound_pred_equal, nullptr);

  ICEBERG_ASSIGN_OR_THROW(auto projected_equal,
                          transform->Project("part", bound_pred_equal));
  ASSERT_NE(projected_equal, nullptr);
  EXPECT_EQ(projected_equal->op(), Expression::Operation::kEq);

  auto unbound_projected_equal =
      internal::checked_pointer_cast<UnboundPredicateImpl<BoundReference>>(
          std::move(projected_equal));
  ASSERT_NE(unbound_projected_equal, nullptr);
  EXPECT_EQ(unbound_projected_equal->op(), Expression::Operation::kEq);
  EXPECT_EQ(unbound_projected_equal->literals().size(), 1);
  EXPECT_EQ(std::get<std::string>(unbound_projected_equal->literals().front().value()),
            "Hello");
}

TEST_F(TransformProjectTest, TruncateProjectStringStartsWithCodePointCountLessThanWidth) {
  auto transform = Transform::Truncate(5);

  // Code point count < width (multi-byte UTF-8 characters)
  // "😜🧐" has 2 code points, width is 5
  auto unbound_emoji_short = Expressions::StartsWith("value", "😜🧐");
  ICEBERG_ASSIGN_OR_THROW(
      auto bound_emoji_short,
      unbound_emoji_short->Bind(*string_schema_, /*case_sensitive=*/true));
  auto bound_pred_emoji_short =
      std::dynamic_pointer_cast<BoundPredicate>(bound_emoji_short);
  ASSERT_NE(bound_pred_emoji_short, nullptr);

  ICEBERG_ASSIGN_OR_THROW(auto projected_emoji_short,
                          transform->Project("part", bound_pred_emoji_short));
  ASSERT_NE(projected_emoji_short, nullptr);
  EXPECT_EQ(projected_emoji_short->op(), Expression::Operation::kStartsWith);

  auto unbound_projected_emoji_short =
      internal::checked_pointer_cast<UnboundPredicateImpl<BoundReference>>(
          std::move(projected_emoji_short));
  ASSERT_NE(unbound_projected_emoji_short, nullptr);
  EXPECT_EQ(unbound_projected_emoji_short->op(), Expression::Operation::kStartsWith);
  EXPECT_EQ(unbound_projected_emoji_short->literals().size(), 1);
  EXPECT_EQ(
      std::get<std::string>(unbound_projected_emoji_short->literals().front().value()),
      "😜🧐");
}

TEST_F(TransformProjectTest, TruncateProjectStringStartsWithCodePointCountEqualToWidth) {
  auto transform = Transform::Truncate(5);

  // Code point count == width (exactly 5 code points)
  // "😜🧐🤔🤪🥳" has exactly 5 code points
  auto unbound_emoji_equal = Expressions::StartsWith("value", "😜🧐🤔🤪🥳");
  ICEBERG_ASSIGN_OR_THROW(
      auto bound_emoji_equal,
      unbound_emoji_equal->Bind(*string_schema_, /*case_sensitive=*/true));
  auto bound_pred_emoji_equal =
      std::dynamic_pointer_cast<BoundPredicate>(bound_emoji_equal);
  ASSERT_NE(bound_pred_emoji_equal, nullptr);

  ICEBERG_ASSIGN_OR_THROW(auto projected_emoji_equal,
                          transform->Project("part", bound_pred_emoji_equal));
  ASSERT_NE(projected_emoji_equal, nullptr);
  EXPECT_EQ(projected_emoji_equal->op(), Expression::Operation::kEq);

  auto unbound_projected_emoji_equal =
      internal::checked_pointer_cast<UnboundPredicateImpl<BoundReference>>(
          std::move(projected_emoji_equal));
  ASSERT_NE(unbound_projected_emoji_equal, nullptr);
  EXPECT_EQ(unbound_projected_emoji_equal->op(), Expression::Operation::kEq);
  EXPECT_EQ(unbound_projected_emoji_equal->literals().size(), 1);
  EXPECT_EQ(
      std::get<std::string>(unbound_projected_emoji_equal->literals().front().value()),
      "😜🧐🤔🤪🥳");
}

TEST_F(TransformProjectTest,
       TruncateProjectStringStartsWithCodePointCountGreaterThanWidth) {
  auto transform = Transform::Truncate(5);

  // Code point count > width (truncate to 5 code points)
  // "😜🧐🤔🤪🥳😵‍💫😂" has 7 code points, should truncate to 5
  auto unbound_emoji_long =
      Expressions::StartsWith("value", "😜🧐🤔🤪🥳😵‍💫😂");
  ICEBERG_ASSIGN_OR_THROW(
      auto bound_emoji_long,
      unbound_emoji_long->Bind(*string_schema_, /*case_sensitive=*/true));
  auto bound_pred_emoji_long =
      std::dynamic_pointer_cast<BoundPredicate>(bound_emoji_long);
  ASSERT_NE(bound_pred_emoji_long, nullptr);

  ICEBERG_ASSIGN_OR_THROW(auto projected_emoji_long,
                          transform->Project("part", bound_pred_emoji_long));
  ASSERT_NE(projected_emoji_long, nullptr);
  EXPECT_EQ(projected_emoji_long->op(), Expression::Operation::kStartsWith);

  auto unbound_projected_emoji_long =
      internal::checked_pointer_cast<UnboundPredicateImpl<BoundReference>>(
          std::move(projected_emoji_long));
  ASSERT_NE(unbound_projected_emoji_long, nullptr);
  EXPECT_EQ(unbound_projected_emoji_long->op(), Expression::Operation::kStartsWith);
  EXPECT_EQ(unbound_projected_emoji_long->literals().size(), 1);
  EXPECT_EQ(
      std::get<std::string>(unbound_projected_emoji_long->literals().front().value()),
      "😜🧐🤔🤪🥳");
}

TEST_F(TransformProjectTest, TruncateProjectStringStartsWithMixedAsciiAndMultiByte) {
  auto transform = Transform::Truncate(5);

  // Mixed ASCII and multi-byte UTF-8 characters
  // "a😜b🧐c" has 5 code points (3 ASCII + 2 emojis)
  auto unbound_mixed_equal = Expressions::StartsWith("value", "a😜b🧐c");
  ICEBERG_ASSIGN_OR_THROW(
      auto bound_mixed_equal,
      unbound_mixed_equal->Bind(*string_schema_, /*case_sensitive=*/true));
  auto bound_pred_mixed_equal =
      std::dynamic_pointer_cast<BoundPredicate>(bound_mixed_equal);
  ASSERT_NE(bound_pred_mixed_equal, nullptr);

  ICEBERG_ASSIGN_OR_THROW(auto projected_mixed_equal,
                          transform->Project("part", bound_pred_mixed_equal));
  ASSERT_NE(projected_mixed_equal, nullptr);
  EXPECT_EQ(projected_mixed_equal->op(), Expression::Operation::kEq);

  auto unbound_projected_mixed_equal =
      internal::checked_pointer_cast<UnboundPredicateImpl<BoundReference>>(
          std::move(projected_mixed_equal));
  ASSERT_NE(unbound_projected_mixed_equal, nullptr);
  EXPECT_EQ(unbound_projected_mixed_equal->op(), Expression::Operation::kEq);
  EXPECT_EQ(unbound_projected_mixed_equal->literals().size(), 1);
  EXPECT_EQ(
      std::get<std::string>(unbound_projected_mixed_equal->literals().front().value()),
      "a😜b🧐c");
}

TEST_F(TransformProjectTest, TruncateProjectStringStartsWithChineseCharactersShort) {
  auto transform = Transform::Truncate(5);

  // Chinese characters (3-byte UTF-8)
  // "你好世界" has 4 code points, width is 5
  auto unbound_chinese_short = Expressions::StartsWith("value", "你好世界");
  ICEBERG_ASSIGN_OR_THROW(
      auto bound_chinese_short,
      unbound_chinese_short->Bind(*string_schema_, /*case_sensitive=*/true));
  auto bound_pred_chinese_short =
      std::dynamic_pointer_cast<BoundPredicate>(bound_chinese_short);
  ASSERT_NE(bound_pred_chinese_short, nullptr);

  ICEBERG_ASSIGN_OR_THROW(auto projected_chinese_short,
                          transform->Project("part", bound_pred_chinese_short));
  ASSERT_NE(projected_chinese_short, nullptr);
  EXPECT_EQ(projected_chinese_short->op(), Expression::Operation::kStartsWith);

  auto unbound_projected_chinese_short =
      internal::checked_pointer_cast<UnboundPredicateImpl<BoundReference>>(
          std::move(projected_chinese_short));
  ASSERT_NE(unbound_projected_chinese_short, nullptr);
  EXPECT_EQ(unbound_projected_chinese_short->op(), Expression::Operation::kStartsWith);
  EXPECT_EQ(unbound_projected_chinese_short->literals().size(), 1);
  EXPECT_EQ(
      std::get<std::string>(unbound_projected_chinese_short->literals().front().value()),
      "你好世界");
}

TEST_F(TransformProjectTest, TruncateProjectStringStartsWithChineseCharactersEqualWidth) {
  auto transform = Transform::Truncate(5);

  // Chinese characters exactly matching width
  // "你好世界好" has exactly 5 code points
  auto unbound_chinese_equal = Expressions::StartsWith("value", "你好世界好");
  ICEBERG_ASSIGN_OR_THROW(
      auto bound_chinese_equal,
      unbound_chinese_equal->Bind(*string_schema_, /*case_sensitive=*/true));
  auto bound_pred_chinese_equal =
      std::dynamic_pointer_cast<BoundPredicate>(bound_chinese_equal);
  ASSERT_NE(bound_pred_chinese_equal, nullptr);

  ICEBERG_ASSIGN_OR_THROW(auto projected_chinese_equal,
                          transform->Project("part", bound_pred_chinese_equal));
  ASSERT_NE(projected_chinese_equal, nullptr);
  EXPECT_EQ(projected_chinese_equal->op(), Expression::Operation::kEq);

  auto unbound_projected_chinese_equal =
      internal::checked_pointer_cast<UnboundPredicateImpl<BoundReference>>(
          std::move(projected_chinese_equal));
  ASSERT_NE(unbound_projected_chinese_equal, nullptr);
  EXPECT_EQ(unbound_projected_chinese_equal->op(), Expression::Operation::kEq);
  EXPECT_EQ(unbound_projected_chinese_equal->literals().size(), 1);
  EXPECT_EQ(
      std::get<std::string>(unbound_projected_chinese_equal->literals().front().value()),
      "你好世界好");
}

TEST_F(TransformProjectTest,
       TruncateProjectStringNotStartsWithCodePointCountEqualToWidth) {
  auto transform = Transform::Truncate(5);

  // NotStartsWith with code point count == width
  // Should convert to NotEq
  auto unbound_not_starts_equal = Expressions::NotStartsWith("value", "😜🧐🤔🤪🥳");
  ICEBERG_ASSIGN_OR_THROW(
      auto bound_not_starts_equal,
      unbound_not_starts_equal->Bind(*string_schema_, /*case_sensitive=*/true));
  auto bound_pred_not_starts_equal =
      std::dynamic_pointer_cast<BoundPredicate>(bound_not_starts_equal);
  ASSERT_NE(bound_pred_not_starts_equal, nullptr);

  ICEBERG_ASSIGN_OR_THROW(auto projected_not_starts_equal,
                          transform->Project("part", bound_pred_not_starts_equal));
  ASSERT_NE(projected_not_starts_equal, nullptr);
  EXPECT_EQ(projected_not_starts_equal->op(), Expression::Operation::kNotEq);

  auto unbound_projected_not_starts_equal =
      internal::checked_pointer_cast<UnboundPredicateImpl<BoundReference>>(
          std::move(projected_not_starts_equal));
  ASSERT_NE(unbound_projected_not_starts_equal, nullptr);
  EXPECT_EQ(unbound_projected_not_starts_equal->op(), Expression::Operation::kNotEq);
  EXPECT_EQ(unbound_projected_not_starts_equal->literals().size(), 1);
  EXPECT_EQ(std::get<std::string>(
                unbound_projected_not_starts_equal->literals().front().value()),
            "😜🧐🤔🤪🥳");
}

TEST_F(TransformProjectTest,
       TruncateProjectStringNotStartsWithCodePointCountLessThanWidth) {
  auto transform = Transform::Truncate(5);

  // NotStartsWith with code point count < width
  // Should remain NotStartsWith
  auto unbound_not_starts_short = Expressions::NotStartsWith("value", "😜🧐");
  ICEBERG_ASSIGN_OR_THROW(
      auto bound_not_starts_short,
      unbound_not_starts_short->Bind(*string_schema_, /*case_sensitive=*/true));
  auto bound_pred_not_starts_short =
      std::dynamic_pointer_cast<BoundPredicate>(bound_not_starts_short);
  ASSERT_NE(bound_pred_not_starts_short, nullptr);

  ICEBERG_ASSIGN_OR_THROW(auto projected_not_starts_short,
                          transform->Project("part", bound_pred_not_starts_short));
  ASSERT_NE(projected_not_starts_short, nullptr);
  EXPECT_EQ(projected_not_starts_short->op(), Expression::Operation::kNotStartsWith);

  auto unbound_projected_not_starts_short =
      internal::checked_pointer_cast<UnboundPredicateImpl<BoundReference>>(
          std::move(projected_not_starts_short));
  ASSERT_NE(unbound_projected_not_starts_short, nullptr);
  EXPECT_EQ(unbound_projected_not_starts_short->op(),
            Expression::Operation::kNotStartsWith);
  EXPECT_EQ(unbound_projected_not_starts_short->literals().size(), 1);
  EXPECT_EQ(std::get<std::string>(
                unbound_projected_not_starts_short->literals().front().value()),
            "😜🧐");
}

TEST_F(TransformProjectTest,
       TruncateProjectStringNotStartsWithCodePointCountGreaterThanWidth) {
  auto transform = Transform::Truncate(5);

  // NotStartsWith with code point count > width
  // Should return nullptr (cannot project)
  auto unbound_not_starts_long =
      Expressions::NotStartsWith("value", "😜🧐🤔🤪🥳😵‍💫😂");
  ICEBERG_ASSIGN_OR_THROW(
      auto bound_not_starts_long,
      unbound_not_starts_long->Bind(*string_schema_, /*case_sensitive=*/true));
  auto bound_pred_not_starts_long =
      std::dynamic_pointer_cast<BoundPredicate>(bound_not_starts_long);
  ASSERT_NE(bound_pred_not_starts_long, nullptr);

  ICEBERG_ASSIGN_OR_THROW(auto projected_not_starts_long,
                          transform->Project("part", bound_pred_not_starts_long));
  EXPECT_EQ(projected_not_starts_long, nullptr);
}

TEST_F(TransformProjectTest, YearProjectEquality) {
  auto transform = Transform::Year();

  // 2021-06-01 as days from epoch
  int32_t date_value =
      TemporalTestHelper::CreateDate({.year = 2021, .month = 6, .day = 1});
  auto unbound = Expressions::Equal("value", Literal::Date(date_value));
  ICEBERG_ASSIGN_OR_THROW(auto bound,
                          unbound->Bind(*date_schema_, /*case_sensitive=*/true));
  auto bound_pred = std::dynamic_pointer_cast<BoundPredicate>(bound);
  ASSERT_NE(bound_pred, nullptr);

  ICEBERG_ASSIGN_OR_THROW(auto projected, transform->Project("part", bound_pred));
  ASSERT_NE(projected, nullptr);
  EXPECT_EQ(projected->op(), Expression::Operation::kEq);
}

TEST_F(TransformProjectTest, YearProjectComparison) {
  auto transform = Transform::Year();

  int32_t date_value =
      TemporalTestHelper::CreateDate({.year = 2021, .month = 6, .day = 1});

  // LT projects to LTE
  auto unbound_lt = Expressions::LessThan("value", Literal::Date(date_value));
  ICEBERG_ASSIGN_OR_THROW(auto bound_lt,
                          unbound_lt->Bind(*date_schema_, /*case_sensitive=*/true));
  auto bound_pred_lt = std::dynamic_pointer_cast<BoundPredicate>(bound_lt);
  ASSERT_NE(bound_pred_lt, nullptr);

  ICEBERG_ASSIGN_OR_THROW(auto projected_lt, transform->Project("part", bound_pred_lt));
  ASSERT_NE(projected_lt, nullptr);
  EXPECT_EQ(projected_lt->op(), Expression::Operation::kLtEq);

  // GT projects to GTE
  auto unbound_gt = Expressions::GreaterThan("value", Literal::Date(date_value));
  ICEBERG_ASSIGN_OR_THROW(auto bound_gt,
                          unbound_gt->Bind(*date_schema_, /*case_sensitive=*/true));
  auto bound_pred_gt = std::dynamic_pointer_cast<BoundPredicate>(bound_gt);
  ASSERT_NE(bound_pred_gt, nullptr);

  ICEBERG_ASSIGN_OR_THROW(auto projected_gt, transform->Project("part", bound_pred_gt));
  ASSERT_NE(projected_gt, nullptr);
  EXPECT_EQ(projected_gt->op(), Expression::Operation::kGtEq);
}

TEST_F(TransformProjectTest, MonthProjectEquality) {
  auto transform = Transform::Month();

  int64_t ts_value =
      TemporalTestHelper::CreateTimestamp({.year = 2021, .month = 6, .day = 1});
  auto unbound = Expressions::Equal("value", Literal::Timestamp(ts_value));
  ICEBERG_ASSIGN_OR_THROW(auto bound,
                          unbound->Bind(*timestamp_schema_, /*case_sensitive=*/true));
  auto bound_pred = std::dynamic_pointer_cast<BoundPredicate>(bound);
  ASSERT_NE(bound_pred, nullptr);

  ICEBERG_ASSIGN_OR_THROW(auto projected, transform->Project("part", bound_pred));
  ASSERT_NE(projected, nullptr);
  EXPECT_EQ(projected->op(), Expression::Operation::kEq);
}

TEST_F(TransformProjectTest, DayProjectEquality) {
  auto transform = Transform::Day();

  int32_t date_value =
      TemporalTestHelper::CreateDate({.year = 2021, .month = 6, .day = 15});
  auto unbound = Expressions::Equal("value", Literal::Date(date_value));
  ICEBERG_ASSIGN_OR_THROW(auto bound,
                          unbound->Bind(*date_schema_, /*case_sensitive=*/true));
  auto bound_pred = std::dynamic_pointer_cast<BoundPredicate>(bound);
  ASSERT_NE(bound_pred, nullptr);

  ICEBERG_ASSIGN_OR_THROW(auto projected, transform->Project("part", bound_pred));
  ASSERT_NE(projected, nullptr);
  EXPECT_EQ(projected->op(), Expression::Operation::kEq);
}

TEST_F(TransformProjectTest, HourProjectEquality) {
  auto transform = Transform::Hour();

  int64_t ts_value = TemporalTestHelper::CreateTimestamp(
      {.year = 2021, .month = 6, .day = 1, .hour = 14, .minute = 30});
  auto unbound = Expressions::Equal("value", Literal::Timestamp(ts_value));
  ICEBERG_ASSIGN_OR_THROW(auto bound,
                          unbound->Bind(*timestamp_schema_, /*case_sensitive=*/true));
  auto bound_pred = std::dynamic_pointer_cast<BoundPredicate>(bound);
  ASSERT_NE(bound_pred, nullptr);

  ICEBERG_ASSIGN_OR_THROW(auto projected, transform->Project("part", bound_pred));
  ASSERT_NE(projected, nullptr);
  EXPECT_EQ(projected->op(), Expression::Operation::kEq);
}

TEST_F(TransformProjectTest, VoidProjectReturnsNull) {
  auto transform = Transform::Void();

  auto unbound = Expressions::Equal("value", Literal::Int(100));
  ICEBERG_ASSIGN_OR_THROW(auto bound,
                          unbound->Bind(*int_schema_, /*case_sensitive=*/true));
  auto bound_pred = std::dynamic_pointer_cast<BoundPredicate>(bound);
  ASSERT_NE(bound_pred, nullptr);

  // Void transform always returns null (no projection possible)
  ICEBERG_ASSIGN_OR_THROW(auto projected, transform->Project("part", bound_pred));
  EXPECT_EQ(projected, nullptr);
}

TEST_F(TransformProjectTest, TemporalProjectInSet) {
  auto transform = Transform::Year();

  int32_t date1 = TemporalTestHelper::CreateDate({.year = 2020, .month = 1, .day = 1});
  int32_t date2 = TemporalTestHelper::CreateDate({.year = 2021, .month = 6, .day = 15});
  int32_t date3 = TemporalTestHelper::CreateDate({.year = 2022, .month = 12, .day = 31});

  auto unbound_in = Expressions::In(
      "value", {Literal::Date(date1), Literal::Date(date2), Literal::Date(date3)});
  ICEBERG_ASSIGN_OR_THROW(auto bound_in,
                          unbound_in->Bind(*date_schema_, /*case_sensitive=*/true));
  auto bound_pred_in = std::dynamic_pointer_cast<BoundPredicate>(bound_in);
  ASSERT_NE(bound_pred_in, nullptr);

  ICEBERG_ASSIGN_OR_THROW(auto projected_in, transform->Project("part", bound_pred_in));
  ASSERT_NE(projected_in, nullptr);
  EXPECT_EQ(projected_in->op(), Expression::Operation::kIn);
}

TEST_F(TransformProjectTest, DayTimestampProjectionFix) {
  auto transform = Transform::Day();

  // Predicate: value < 1970-01-01 00:00:00 (0)
  // This implies value <= -1 micros.
  // day(-1 micros) = -1 day (1969-12-31).
  // If we don't fix, we project to day <= -1.
  // If we fix (for buggy writers), we project to day <= 0.
  auto unbound = Expressions::LessThan("value", Literal::Timestamp(0));

  ICEBERG_ASSIGN_OR_THROW(auto bound,
                          unbound->Bind(*timestamp_schema_, /*case_sensitive=*/true));
  auto bound_pred = std::dynamic_pointer_cast<BoundPredicate>(bound);
  ASSERT_NE(bound_pred, nullptr);

  ICEBERG_ASSIGN_OR_THROW(auto projected, transform->Project("part", bound_pred));
  ASSERT_NE(projected, nullptr);

  auto unbound_projected =
      internal::checked_pointer_cast<UnboundPredicateImpl<BoundReference>>(
          std::move(projected));
  EXPECT_EQ(unbound_projected->op(), Expression::Operation::kLtEq);
  ASSERT_EQ(unbound_projected->literals().size(), 1);
  int32_t val = std::get<int32_t>(unbound_projected->literals().front().value());
  EXPECT_EQ(val, 0) << "Expected projected value to be 0 (fix applied), but got " << val;
}

}  // namespace iceberg
