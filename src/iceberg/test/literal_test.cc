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

#include "iceberg/expression/literal.h"

#include <limits>
#include <numbers>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/test/matchers.h"
#include "iceberg/test/temporal_test_helper.h"
#include "iceberg/type.h"

namespace iceberg {

// Parameter struct for basic literal tests
struct BasicLiteralTestParam {
  std::string test_name;
  Literal literal;
  TypeId expected_type_id;
  std::string expected_string;
};

class BasicLiteralTest : public ::testing::TestWithParam<BasicLiteralTestParam> {};

TEST_P(BasicLiteralTest, BasicsTest) {
  const auto& param = GetParam();

  EXPECT_EQ(param.literal.type()->type_id(), param.expected_type_id);
  EXPECT_EQ(param.literal.ToString(), param.expected_string);
}

// Parameter struct for comparison tests
struct ComparisonLiteralTestParam {
  std::string test_name;
  Literal small_literal;
  Literal large_literal;
  Literal equal_literal;  // same as small_literal
};

class ComparisonLiteralTest
    : public ::testing::TestWithParam<ComparisonLiteralTestParam> {};

TEST_P(ComparisonLiteralTest, ComparisonTest) {
  const auto& param = GetParam();

  EXPECT_EQ(param.small_literal <=> param.equal_literal,
            std::partial_ordering::equivalent);
  EXPECT_EQ(param.small_literal <=> param.large_literal, std::partial_ordering::less);
  EXPECT_EQ(param.large_literal <=> param.small_literal, std::partial_ordering::greater);
}

// Parameter struct for cast tests
struct CastLiteralTestParam {
  std::string test_name;
  Literal source_literal;
  std::shared_ptr<PrimitiveType> target_type;
  Literal expected_literal;
};

class CastLiteralTest : public ::testing::TestWithParam<CastLiteralTestParam> {};

TEST_P(CastLiteralTest, CastTest) {
  const auto& param = GetParam();
  auto result = param.source_literal.CastTo(param.target_type);

  ASSERT_THAT(result, IsOk());
  EXPECT_EQ(*result, param.expected_literal);
}

// Cross-type comparison tests
TEST(LiteralTest, CrossTypeComparison) {
  auto int_literal = Literal::Int(42);
  auto string_literal = Literal::String("42");

  // Different types should return unordered
  EXPECT_EQ(int_literal <=> string_literal, std::partial_ordering::unordered);
}

// Overflow tests
TEST(LiteralTest, LongCastToOverflow) {
  // Test overflow cases
  auto max_long =
      Literal::Long(static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + 1);
  auto min_long =
      Literal::Long(static_cast<int64_t>(std::numeric_limits<int32_t>::min()) - 1);

  auto max_result = max_long.CastTo(int32());
  ASSERT_THAT(max_result, IsOk());
  EXPECT_TRUE(max_result->IsAboveMax());

  auto min_result = min_long.CastTo(int32());
  ASSERT_THAT(min_result, IsOk());
  EXPECT_TRUE(min_result->IsBelowMin());

  max_result = max_long.CastTo(date());
  ASSERT_THAT(max_result, IsOk());
  EXPECT_TRUE(max_result->IsAboveMax());

  min_result = min_long.CastTo(date());
  ASSERT_THAT(min_result, IsOk());
  EXPECT_TRUE(min_result->IsBelowMin());
}

TEST(LiteralTest, DoubleCastToOverflow) {
  // Test overflow cases for Double to Float
  auto max_double = Literal::Double(double{std::numeric_limits<float>::max()} * 2);
  auto min_double = Literal::Double(-double{std::numeric_limits<float>::max()} * 2);

  auto max_result = max_double.CastTo(float32());
  ASSERT_THAT(max_result, IsOk());
  EXPECT_TRUE(max_result->IsAboveMax());

  auto min_result = min_double.CastTo(float32());
  ASSERT_THAT(min_result, IsOk());
  EXPECT_TRUE(min_result->IsBelowMin());
}

// Error cases for casts
TEST(LiteralTest, CastToError) {
  std::vector<uint8_t> data = {0x01, 0x02, 0x03, 0x04};
  auto binary_literal = Literal::Binary(data);

  // Cast to Fixed with different length should fail
  EXPECT_THAT(binary_literal.CastTo(fixed(5)), IsError(ErrorKind::kInvalidArgument));

  data = {0x01, 0x02, 0x03, 0x04};
  auto fixed_literal = Literal::Fixed(data);

  // Cast to Fixed with different length should fail
  EXPECT_THAT(fixed_literal.CastTo(fixed(5)), IsError(ErrorKind::kNotSupported));
}

// Special value tests
TEST(LiteralTest, SpecialValues) {
  auto int_literal = Literal::Int(42);
  EXPECT_FALSE(int_literal.IsAboveMax());
  EXPECT_FALSE(int_literal.IsBelowMin());
}

// Float special values tests
TEST(LiteralTest, FloatSpecialValuesComparison) {
  // Create special float values
  auto neg_nan = Literal::Float(-std::numeric_limits<float>::quiet_NaN());
  auto neg_inf = Literal::Float(-std::numeric_limits<float>::infinity());
  auto neg_value = Literal::Float(-1.5f);
  auto neg_zero = Literal::Float(-0.0f);
  auto pos_zero = Literal::Float(0.0f);
  auto pos_value = Literal::Float(1.5f);
  auto pos_inf = Literal::Float(std::numeric_limits<float>::infinity());
  auto pos_nan = Literal::Float(std::numeric_limits<float>::quiet_NaN());

  // Test the ordering: -NaN < -Infinity < -value < -0 < 0 < value < Infinity < NaN
  EXPECT_EQ(neg_nan <=> neg_inf, std::partial_ordering::less);
  EXPECT_EQ(neg_inf <=> neg_value, std::partial_ordering::less);
  EXPECT_EQ(neg_value <=> neg_zero, std::partial_ordering::less);
  EXPECT_EQ(neg_zero <=> pos_zero, std::partial_ordering::less);
  EXPECT_EQ(pos_zero <=> pos_value, std::partial_ordering::less);
  EXPECT_EQ(pos_value <=> pos_inf, std::partial_ordering::less);
  EXPECT_EQ(pos_inf <=> pos_nan, std::partial_ordering::less);
}

TEST(LiteralTest, FloatNaNComparison) {
  auto nan1 = Literal::Float(std::numeric_limits<float>::quiet_NaN());
  auto nan2 = Literal::Float(std::numeric_limits<float>::quiet_NaN());
  auto signaling_nan = Literal::Float(std::numeric_limits<float>::signaling_NaN());

  // NaN should be equal to itself in strong ordering
  EXPECT_EQ(nan1 <=> nan2, std::partial_ordering::equivalent);
  EXPECT_EQ(nan1 <=> signaling_nan, std::partial_ordering::equivalent);
}

TEST(LiteralTest, FloatInfinityComparison) {
  auto neg_inf = Literal::Float(-std::numeric_limits<float>::infinity());
  auto pos_inf = Literal::Float(std::numeric_limits<float>::infinity());
  auto max_value = Literal::Float(std::numeric_limits<float>::max());
  auto min_value = Literal::Float(std::numeric_limits<float>::lowest());

  EXPECT_EQ(neg_inf <=> min_value, std::partial_ordering::less);
  EXPECT_EQ(max_value <=> pos_inf, std::partial_ordering::less);
  EXPECT_EQ(neg_inf <=> pos_inf, std::partial_ordering::less);
}

TEST(LiteralTest, FloatZeroComparison) {
  auto neg_zero = Literal::Float(-0.0f);
  auto pos_zero = Literal::Float(0.0f);

  // -0 should be less than +0
  EXPECT_EQ(neg_zero <=> pos_zero, std::partial_ordering::less);
}

// Double special values tests
TEST(LiteralTest, DoubleSpecialValuesComparison) {
  // Create special double values
  auto neg_nan = Literal::Double(-std::numeric_limits<double>::quiet_NaN());
  auto neg_inf = Literal::Double(-std::numeric_limits<double>::infinity());
  auto neg_value = Literal::Double(-1.5);
  auto neg_zero = Literal::Double(-0.0);
  auto pos_zero = Literal::Double(0.0);
  auto pos_value = Literal::Double(1.5);
  auto pos_inf = Literal::Double(std::numeric_limits<double>::infinity());
  auto pos_nan = Literal::Double(std::numeric_limits<double>::quiet_NaN());

  // Test the ordering: -NaN < -Infinity < -value < -0 < 0 < value < Infinity < NaN
  EXPECT_EQ(neg_nan <=> neg_inf, std::partial_ordering::less);
  EXPECT_EQ(neg_inf <=> neg_value, std::partial_ordering::less);
  EXPECT_EQ(neg_value <=> neg_zero, std::partial_ordering::less);
  EXPECT_EQ(neg_zero <=> pos_zero, std::partial_ordering::less);
  EXPECT_EQ(pos_zero <=> pos_value, std::partial_ordering::less);
  EXPECT_EQ(pos_value <=> pos_inf, std::partial_ordering::less);
  EXPECT_EQ(pos_inf <=> pos_nan, std::partial_ordering::less);
}

TEST(LiteralTest, DoubleNaNComparison) {
  auto nan1 = Literal::Double(std::numeric_limits<double>::quiet_NaN());
  auto nan2 = Literal::Double(std::numeric_limits<double>::quiet_NaN());
  auto signaling_nan = Literal::Double(std::numeric_limits<double>::signaling_NaN());

  // NaN should be equal to itself in strong ordering
  EXPECT_EQ(nan1 <=> nan2, std::partial_ordering::equivalent);
  EXPECT_EQ(nan1 <=> signaling_nan, std::partial_ordering::equivalent);
}

TEST(LiteralTest, DoubleInfinityComparison) {
  auto neg_inf = Literal::Double(-std::numeric_limits<double>::infinity());
  auto pos_inf = Literal::Double(std::numeric_limits<double>::infinity());
  auto max_value = Literal::Double(std::numeric_limits<double>::max());
  auto min_value = Literal::Double(std::numeric_limits<double>::lowest());

  EXPECT_EQ(neg_inf <=> min_value, std::partial_ordering::less);
  EXPECT_EQ(max_value <=> pos_inf, std::partial_ordering::less);
  EXPECT_EQ(neg_inf <=> pos_inf, std::partial_ordering::less);
}

TEST(LiteralTest, DoubleZeroComparison) {
  auto neg_zero = Literal::Double(-0.0);
  auto pos_zero = Literal::Double(0.0);

  EXPECT_EQ(neg_zero <=> pos_zero, std::partial_ordering::less);
}

TEST(LiteralTest, UuidComparison) {
  auto uuid1 = Uuid::FromString("123e4567-e89b-12d3-a456-426614174000").value();
  auto uuid2 = Uuid::FromString("123e4567-e89b-12d3-a456-426614174001").value();
  auto uuid3 = Uuid::FromString("123e4567-e89b-12d3-a456-426614174000").value();

  auto literal1 = Literal::UUID(uuid1);
  auto literal2 = Literal::UUID(uuid2);
  auto literal3 = Literal::UUID(uuid3);

  EXPECT_EQ(literal1 <=> literal3, std::partial_ordering::equivalent);
  EXPECT_EQ(literal1 <=> literal2, std::partial_ordering::unordered);
  EXPECT_EQ(literal2 <=> literal1, std::partial_ordering::unordered);
}

// Parameter struct for literal serialization and deserialization tests
struct LiteralParam {
  std::string test_name;
  std::vector<uint8_t> serialized;
  Literal value;
  std::shared_ptr<PrimitiveType> type;
};

class LiteralSerDeParam : public ::testing::TestWithParam<LiteralParam> {};

TEST_P(LiteralSerDeParam, RoundTrip) {
  const auto& param = GetParam();

  // Deserialize from bytes
  Result<Literal> literal_result = Literal::Deserialize(param.serialized, param.type);
  ASSERT_TRUE(literal_result.has_value())
      << "Deserialization failed: " << literal_result.error().message;

  // Check type and value
  EXPECT_EQ(*literal_result, param.value);

  // Serialize back to bytes
  Result<std::vector<uint8_t>> bytes_result = literal_result->Serialize();
  ASSERT_TRUE(bytes_result.has_value())
      << "Serialization failed: " << bytes_result.error().message;
  EXPECT_EQ(*bytes_result, param.serialized);

  // Deserialize again to verify idempotency
  Result<Literal> final_literal = Literal::Deserialize(*bytes_result, param.type);
  ASSERT_TRUE(final_literal.has_value())
      << "Final deserialization failed: " << final_literal.error().message;
  EXPECT_EQ(*final_literal, param.value);
}

INSTANTIATE_TEST_SUITE_P(
    BinarySerialization, LiteralSerDeParam,
    ::testing::Values(
        // Basic types
        LiteralParam{"BooleanTrue", {1}, Literal::Boolean(true), boolean()},
        LiteralParam{"BooleanFalse", {0}, Literal::Boolean(false), boolean()},

        LiteralParam{"Int", {32, 0, 0, 0}, Literal::Int(32), int32()},
        LiteralParam{
            "IntMaxValue", {255, 255, 255, 127}, Literal::Int(2147483647), int32()},
        LiteralParam{"IntMinValue", {0, 0, 0, 128}, Literal::Int(-2147483648), int32()},
        LiteralParam{"NegativeInt", {224, 255, 255, 255}, Literal::Int(-32), int32()},

        LiteralParam{"Long", {32, 0, 0, 0, 0, 0, 0, 0}, Literal::Long(32), int64()},
        LiteralParam{"LongMaxValue",
                     {255, 255, 255, 255, 255, 255, 255, 127},
                     Literal::Long(std::numeric_limits<int64_t>::max()),
                     int64()},
        LiteralParam{"LongMinValue",
                     {0, 0, 0, 0, 0, 0, 0, 128},
                     Literal::Long(std::numeric_limits<int64_t>::min()),
                     int64()},
        LiteralParam{"NegativeLong",
                     {224, 255, 255, 255, 255, 255, 255, 255},
                     Literal::Long(-32),
                     int64()},

        LiteralParam{"Float", {0, 0, 128, 63}, Literal::Float(1.0f), float32()},
        LiteralParam{"FloatNegativeInfinity",
                     {0, 0, 128, 255},
                     Literal::Float(-std::numeric_limits<float>::infinity()),
                     float32()},
        LiteralParam{"FloatMaxValue",
                     {255, 255, 127, 127},
                     Literal::Float(std::numeric_limits<float>::max()),
                     float32()},
        LiteralParam{"FloatMinValue",
                     {255, 255, 127, 255},
                     Literal::Float(std::numeric_limits<float>::lowest()),
                     float32()},

        LiteralParam{
            "Double", {0, 0, 0, 0, 0, 0, 240, 63}, Literal::Double(1.0), float64()},
        LiteralParam{"DoubleNegativeInfinity",
                     {0, 0, 0, 0, 0, 0, 240, 255},
                     Literal::Double(-std::numeric_limits<double>::infinity()),
                     float64()},
        LiteralParam{"DoubleMaxValue",
                     {255, 255, 255, 255, 255, 255, 239, 127},
                     Literal::Double(std::numeric_limits<double>::max()),
                     float64()},
        LiteralParam{"DoubleMinValue",
                     {255, 255, 255, 255, 255, 255, 239, 255},
                     Literal::Double(std::numeric_limits<double>::lowest()),
                     float64()},

        // Decimal type
        LiteralParam{"DecimalPositive",
                     {1, 226, 64},
                     Literal::Decimal(123456, 6, 2),
                     decimal(6, 2)},
        LiteralParam{"DecimalNegative",
                     {254, 29, 192},
                     Literal::Decimal(-123456, 6, 2),
                     decimal(6, 2)},
        LiteralParam{"DecimalZero", {0}, Literal::Decimal(0, 3, 0), decimal(3, 0)},

        LiteralParam{"String",
                     {105, 99, 101, 98, 101, 114, 103},
                     Literal::String("iceberg"),
                     string()},
        LiteralParam{"StringLong",
                     {65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65},
                     Literal::String("AAAAAAAAAAAAAAAA"),
                     string()},

        // Uuid type
        LiteralParam{
            "Uuid",
            {0x12, 0x3E, 0x45, 0x67, 0xE8, 0x9B, 0x12, 0xD3, 0xA4, 0x56, 0x42, 0x66, 0x14,
             0x17, 0x40, 0x00},
            Literal::UUID(
                Uuid::FromString("123e4567-e89b-12d3-a456-426614174000").value()),
            uuid()},

        LiteralParam{"BinaryData",
                     {0x01, 0x02, 0x03, 0xFF},
                     Literal::Binary({0x01, 0x02, 0x03, 0xFF}),
                     binary()},
        LiteralParam{"BinarySingleByte", {42}, Literal::Binary({42}), binary()},

        // Fixed type
        LiteralParam{"FixedLength4",
                     {0x01, 0x02, 0x03, 0x04},
                     Literal::Fixed({0x01, 0x02, 0x03, 0x04}),
                     fixed(4)},
        LiteralParam{"FixedLength8",
                     {0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00, 0x11},
                     Literal::Fixed({0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00, 0x11}),
                     fixed(8)},
        LiteralParam{"FixedLength16",
                     {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A,
                      0x0B, 0x0C, 0x0D, 0x0E, 0x0F},
                     Literal::Fixed({0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
                                     0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F}),
                     fixed(16)},
        LiteralParam{"FixedSingleByte", {0xFF}, Literal::Fixed({0xFF}), fixed(1)},

        // Temporal types
        LiteralParam{"DateEpoch", {0, 0, 0, 0}, Literal::Date(0), date()},
        LiteralParam{"DateNextDay", {1, 0, 0, 0}, Literal::Date(1), date()},
        LiteralParam{"DateY2K", {205, 42, 0, 0}, Literal::Date(10957), date()},
        LiteralParam{"DateNegative", {255, 255, 255, 255}, Literal::Date(-1), date()},

        LiteralParam{"TimeMidnight", {0, 0, 0, 0, 0, 0, 0, 0}, Literal::Time(0), time()},
        LiteralParam{"TimeNoon",
                     {128, 9, 230, 124, 10, 0, 0, 0},
                     Literal::Time(45045123456),
                     time()},
        LiteralParam{
            "TimeOneSecond", {64, 66, 15, 0, 0, 0, 0, 0}, Literal::Time(1000000), time()},

        LiteralParam{"TimestampEpoch",
                     {0, 0, 0, 0, 0, 0, 0, 0},
                     Literal::Timestamp(0),
                     timestamp()},
        LiteralParam{"TimestampOneSecond",
                     {64, 66, 15, 0, 0, 0, 0, 0},
                     Literal::Timestamp(1000000),
                     timestamp()},
        LiteralParam{"TimestampNoon2024",
                     {128, 9, 230, 124, 10, 0, 0, 0},
                     Literal::Timestamp(45045123456),
                     timestamp()},

        LiteralParam{"TimestampTzEpoch",
                     {0, 0, 0, 0, 0, 0, 0, 0},
                     Literal::TimestampTz(0),
                     timestamp_tz()},
        LiteralParam{"TimestampTzOneHour",
                     {0, 164, 147, 214, 0, 0, 0, 0},
                     Literal::TimestampTz(3600000000),
                     timestamp_tz()},

        // Empty values
        LiteralParam{"EmptyString", {}, Literal::String(""), string()},
        LiteralParam{"EmptyBinary", {}, Literal::Binary({}), binary()}),

    [](const testing::TestParamInfo<LiteralSerDeParam::ParamType>& info) {
      return info.param.test_name;
    });

TEST(LiteralSerDeTest, EmptyString) {
  auto empty_string = Literal::String("");
  auto empty_bytes = empty_string.Serialize();
  ASSERT_TRUE(empty_bytes.has_value());
  EXPECT_TRUE(empty_bytes->empty());

  auto deserialize_result = Literal::Deserialize(*empty_bytes, string());
  ASSERT_THAT(deserialize_result, IsOk());
  EXPECT_TRUE(std::get<std::string>(deserialize_result->value()).empty());
}

TEST(LiteralSerDeTest, EmptyBinary) {
  auto empty_binary = Literal::Binary({});
  auto empty_bytes = empty_binary.Serialize();
  ASSERT_TRUE(empty_bytes.has_value());
  EXPECT_TRUE(empty_bytes->empty());

  auto deserialize_result = Literal::Deserialize(*empty_bytes, binary());
  ASSERT_THAT(deserialize_result, IsOk());
  EXPECT_TRUE(std::get<std::vector<uint8_t>>(deserialize_result->value()).empty());
}

// Type promotion tests
TEST(LiteralSerDeTest, TypePromotion) {
  // 4-byte int data can be deserialized as long
  std::vector<uint8_t> int_data = {32, 0, 0, 0};
  auto long_result = Literal::Deserialize(int_data, int64());
  ASSERT_TRUE(long_result.has_value());
  EXPECT_EQ(long_result->type()->type_id(), TypeId::kLong);
  EXPECT_EQ(std::get<int64_t>(long_result->value()), 32L);

  // 4-byte float data can be deserialized as double
  std::vector<uint8_t> float_data = {0, 0, 128, 63};
  auto double_result = Literal::Deserialize(float_data, float64());
  ASSERT_TRUE(double_result.has_value());
  EXPECT_EQ(double_result->type()->type_id(), TypeId::kDouble);
  EXPECT_DOUBLE_EQ(std::get<double>(double_result->value()), 1.0);
}
// Instantiate parameterized tests

INSTANTIATE_TEST_SUITE_P(
    BasicLiteralTestCases, BasicLiteralTest,
    ::testing::Values(
        BasicLiteralTestParam{.test_name = "BooleanTrue",
                              .literal = Literal::Boolean(true),
                              .expected_type_id = TypeId::kBoolean,
                              .expected_string = "true"},
        BasicLiteralTestParam{.test_name = "BooleanFalse",
                              .literal = Literal::Boolean(false),
                              .expected_type_id = TypeId::kBoolean,
                              .expected_string = "false"},
        BasicLiteralTestParam{.test_name = "IntPositive",
                              .literal = Literal::Int(42),
                              .expected_type_id = TypeId::kInt,
                              .expected_string = "42"},
        BasicLiteralTestParam{.test_name = "IntNegative",
                              .literal = Literal::Int(-123),
                              .expected_type_id = TypeId::kInt,
                              .expected_string = "-123"},
        BasicLiteralTestParam{.test_name = "LongPositive",
                              .literal = Literal::Long(1234567890L),
                              .expected_type_id = TypeId::kLong,
                              .expected_string = "1234567890"},
        BasicLiteralTestParam{.test_name = "LongNegative",
                              .literal = Literal::Long(-9876543210L),
                              .expected_type_id = TypeId::kLong,
                              .expected_string = "-9876543210"},
        BasicLiteralTestParam{.test_name = "Float",
                              .literal = Literal::Float(3.14f),
                              .expected_type_id = TypeId::kFloat,
                              .expected_string = "3.140000"},
        BasicLiteralTestParam{.test_name = "Double",
                              .literal = Literal::Double(std::numbers::pi),
                              .expected_type_id = TypeId::kDouble,
                              .expected_string = "3.141593"},
        BasicLiteralTestParam{.test_name = "DecimalPositive",
                              .literal = Literal::Decimal(123456, 6, 2),
                              .expected_type_id = TypeId::kDecimal,
                              .expected_string = "1234.56"},
        BasicLiteralTestParam{.test_name = "DecimalNegative",
                              .literal = Literal::Decimal(-123456, 6, 2),
                              .expected_type_id = TypeId::kDecimal,
                              .expected_string = "-1234.56"},
        BasicLiteralTestParam{.test_name = "DecimalZero",
                              .literal = Literal::Decimal(0, 3, 0),
                              .expected_type_id = TypeId::kDecimal,
                              .expected_string = "0"},
        BasicLiteralTestParam{.test_name = "String",
                              .literal = Literal::String("hello world"),
                              .expected_type_id = TypeId::kString,
                              .expected_string = "\"hello world\""},
        BasicLiteralTestParam{
            .test_name = "Uuid",
            .literal = Literal::UUID(
                Uuid::FromString("123e4567-e89b-12d3-a456-426614174000").value()),
            .expected_type_id = TypeId::kUuid,
            .expected_string = "123e4567-e89b-12d3-a456-426614174000"},
        BasicLiteralTestParam{
            .test_name = "Binary",
            .literal = Literal::Binary(std::vector<uint8_t>{0x01, 0x02, 0x03, 0xFF}),
            .expected_type_id = TypeId::kBinary,
            .expected_string = "X'010203FF'"},
        BasicLiteralTestParam{
            .test_name = "Fixed",
            .literal = Literal::Fixed(std::vector<uint8_t>{0x01, 0x02, 0x03, 0xFF}),
            .expected_type_id = TypeId::kFixed,
            .expected_string = "X'010203FF'"},
        BasicLiteralTestParam{.test_name = "Date",
                              .literal = Literal::Date(19489),
                              .expected_type_id = TypeId::kDate,
                              .expected_string = "19489"},
        BasicLiteralTestParam{.test_name = "Time",
                              .literal = Literal::Time(43200000000LL),
                              .expected_type_id = TypeId::kTime,
                              .expected_string = "43200000000"},
        BasicLiteralTestParam{.test_name = "Timestamp",
                              .literal = Literal::Timestamp(1684137600000000LL),
                              .expected_type_id = TypeId::kTimestamp,
                              .expected_string = "1684137600000000"},
        BasicLiteralTestParam{.test_name = "TimestampTz",
                              .literal = Literal::TimestampTz(1684137600000000LL),
                              .expected_type_id = TypeId::kTimestampTz,
                              .expected_string = "1684137600000000"}),
    [](const ::testing::TestParamInfo<BasicLiteralTestParam>& info) {
      return info.param.test_name;
    });

INSTANTIATE_TEST_SUITE_P(
    ComparisonLiteralTestCases, ComparisonLiteralTest,
    ::testing::Values(
        ComparisonLiteralTestParam{.test_name = "Boolean",
                                   .small_literal = Literal::Boolean(false),
                                   .large_literal = Literal::Boolean(true),
                                   .equal_literal = Literal::Boolean(false)},
        ComparisonLiteralTestParam{.test_name = "Int",
                                   .small_literal = Literal::Int(10),
                                   .large_literal = Literal::Int(20),
                                   .equal_literal = Literal::Int(10)},
        ComparisonLiteralTestParam{.test_name = "Long",
                                   .small_literal = Literal::Long(100L),
                                   .large_literal = Literal::Long(200L),
                                   .equal_literal = Literal::Long(100L)},
        ComparisonLiteralTestParam{.test_name = "Float",
                                   .small_literal = Literal::Float(1.5f),
                                   .large_literal = Literal::Float(2.5f),
                                   .equal_literal = Literal::Float(1.5f)},
        ComparisonLiteralTestParam{.test_name = "Double",
                                   .small_literal = Literal::Double(1.5),
                                   .large_literal = Literal::Double(2.5),
                                   .equal_literal = Literal::Double(1.5)},
        ComparisonLiteralTestParam{.test_name = "Decimal",
                                   .small_literal = Literal::Decimal(123456, 6, 2),
                                   .large_literal = Literal::Decimal(234567, 6, 2),
                                   .equal_literal = Literal::Decimal(123456, 6, 2)},
        ComparisonLiteralTestParam{.test_name = "String",
                                   .small_literal = Literal::String("apple"),
                                   .large_literal = Literal::String("banana"),
                                   .equal_literal = Literal::String("apple")},
        ComparisonLiteralTestParam{
            .test_name = "Binary",
            .small_literal = Literal::Binary(std::vector<uint8_t>{0x01, 0x02}),
            .large_literal = Literal::Binary(std::vector<uint8_t>{0x01, 0x03}),
            .equal_literal = Literal::Binary(std::vector<uint8_t>{0x01, 0x02})},
        ComparisonLiteralTestParam{
            .test_name = "Fixed",
            .small_literal = Literal::Fixed(std::vector<uint8_t>{0x01, 0x02}),
            .large_literal = Literal::Fixed(std::vector<uint8_t>{0x01, 0x03}),
            .equal_literal = Literal::Fixed(std::vector<uint8_t>{0x01, 0x02})},
        ComparisonLiteralTestParam{.test_name = "Date",
                                   .small_literal = Literal::Date(100),
                                   .large_literal = Literal::Date(200),
                                   .equal_literal = Literal::Date(100)},
        ComparisonLiteralTestParam{.test_name = "Time",
                                   .small_literal = Literal::Time(43200000000LL),
                                   .large_literal = Literal::Time(86400000000LL),
                                   .equal_literal = Literal::Time(43200000000LL)},
        ComparisonLiteralTestParam{.test_name = "Timestamp",
                                   .small_literal = Literal::Timestamp(1000000LL),
                                   .large_literal = Literal::Timestamp(2000000LL),
                                   .equal_literal = Literal::Timestamp(1000000LL)},
        ComparisonLiteralTestParam{.test_name = "TimestampTz",
                                   .small_literal = Literal::TimestampTz(1000000LL),
                                   .large_literal = Literal::TimestampTz(2000000LL),
                                   .equal_literal = Literal::TimestampTz(1000000LL)}),
    [](const ::testing::TestParamInfo<ComparisonLiteralTestParam>& info) {
      return info.param.test_name;
    });

INSTANTIATE_TEST_SUITE_P(
    CastLiteralTestCases, CastLiteralTest,
    ::testing::Values(
        // Int cast tests
        CastLiteralTestParam{.test_name = "IntToLong",
                             .source_literal = Literal::Int(42),
                             .target_type = int64(),
                             .expected_literal = Literal::Long(42L)},
        CastLiteralTestParam{.test_name = "IntToFloat",
                             .source_literal = Literal::Int(42),
                             .target_type = float32(),
                             .expected_literal = Literal::Float(42.0f)},
        CastLiteralTestParam{.test_name = "IntToDouble",
                             .source_literal = Literal::Int(42),
                             .target_type = float64(),
                             .expected_literal = Literal::Double(42.0)},
        CastLiteralTestParam{.test_name = "IntToDate",
                             .source_literal = Literal::Int(42),
                             .target_type = date(),
                             .expected_literal = Literal::Date(42)},
        // Long cast tests
        CastLiteralTestParam{.test_name = "LongToInt",
                             .source_literal = Literal::Long(42L),
                             .target_type = int32(),
                             .expected_literal = Literal::Int(42)},
        CastLiteralTestParam{.test_name = "LongToFloat",
                             .source_literal = Literal::Long(42L),
                             .target_type = float32(),
                             .expected_literal = Literal::Float(42.0f)},
        CastLiteralTestParam{.test_name = "LongToDouble",
                             .source_literal = Literal::Long(42L),
                             .target_type = float64(),
                             .expected_literal = Literal::Double(42.0)},
        CastLiteralTestParam{.test_name = "LongToTime",
                             .source_literal = Literal::Long(42L),
                             .target_type = time(),
                             .expected_literal = Literal::Time(42L)},
        CastLiteralTestParam{.test_name = "LongToTimestamp",
                             .source_literal = Literal::Long(42L),
                             .target_type = timestamp(),
                             .expected_literal = Literal::Timestamp(42L)},
        CastLiteralTestParam{.test_name = "LongToTimestampTz",
                             .source_literal = Literal::Long(42L),
                             .target_type = timestamp_tz(),
                             .expected_literal = Literal::TimestampTz(42L)},
        CastLiteralTestParam{
            .test_name = "TimestampToDate",
            .source_literal =
                Literal::Timestamp(TemporalTestHelper::CreateTimestamp({.year = 2021,
                                                                        .month = 6,
                                                                        .day = 1,
                                                                        .hour = 11,
                                                                        .minute = 43,
                                                                        .second = 20})),
            .target_type = date(),
            .expected_literal = Literal::Date(
                TemporalTestHelper::CreateDate({.year = 2021, .month = 6, .day = 1}))},
        CastLiteralTestParam{
            .test_name = "TimestampTzToDate",
            .source_literal = Literal::TimestampTz(
                TemporalTestHelper::CreateTimestampTz({.year = 2021,
                                                       .month = 1,
                                                       .day = 1,
                                                       .hour = 7,
                                                       .minute = 43,
                                                       .second = 20,
                                                       .tz_offset_minutes = 480})),
            .target_type = date(),
            .expected_literal = Literal::Date(
                TemporalTestHelper::CreateDate({.year = 2020, .month = 12, .day = 31}))},
        CastLiteralTestParam{.test_name = "EpochToDate",
                             .source_literal = Literal::Timestamp(
                                 TemporalTestHelper::CreateTimestamp({.year = 1970,
                                                                      .month = 1,
                                                                      .day = 1,
                                                                      .hour = 0,
                                                                      .minute = 0,
                                                                      .second = 0})),
                             .target_type = date(),
                             .expected_literal = Literal::Date(0)},
        CastLiteralTestParam{.test_name = "TimestampBeforeEpochToDate",
                             .source_literal = Literal::Timestamp(
                                 TemporalTestHelper::CreateTimestamp({.year = 1969,
                                                                      .month = 12,
                                                                      .day = 31,
                                                                      .hour = 23,
                                                                      .minute = 59,
                                                                      .second = 59})),
                             .target_type = date(),
                             .expected_literal = Literal::Date(-1)},
        // Float cast tests
        CastLiteralTestParam{.test_name = "FloatToDouble",
                             .source_literal = Literal::Float(2.0f),
                             .target_type = float64(),
                             .expected_literal = Literal::Double(double{2.0f})},
        // Double cast tests
        CastLiteralTestParam{.test_name = "DoubleToFloat",
                             .source_literal = Literal::Double(2.0),
                             .target_type = float32(),
                             .expected_literal = Literal::Float(2.0f)},
        // Binary cast tests
        CastLiteralTestParam{.test_name = "BinaryToFixed",
                             .source_literal = Literal::Binary(std::vector<uint8_t>{
                                 0x01, 0x02, 0x03, 0x04}),
                             .target_type = fixed(4),
                             .expected_literal = Literal::Fixed(std::vector<uint8_t>{
                                 0x01, 0x02, 0x03, 0x04})},
        // Fixed cast tests
        CastLiteralTestParam{.test_name = "FixedToBinary",
                             .source_literal = Literal::Fixed(std::vector<uint8_t>{
                                 0x01, 0x02, 0x03, 0x04}),
                             .target_type = binary(),
                             .expected_literal = Literal::Binary(std::vector<uint8_t>{
                                 0x01, 0x02, 0x03, 0x04})},
        CastLiteralTestParam{.test_name = "FixedToFixed",
                             .source_literal = Literal::Fixed(std::vector<uint8_t>{
                                 0x01, 0x02, 0x03, 0x04}),
                             .target_type = fixed(4),
                             .expected_literal = Literal::Fixed(std::vector<uint8_t>{
                                 0x01, 0x02, 0x03, 0x04})},
        // String cast tests
        CastLiteralTestParam{
            .test_name = "StringToUuid",
            .source_literal = Literal::String("123e4567-e89b-12d3-a456-426614174000"),
            .target_type = uuid(),
            .expected_literal = Literal::UUID(
                Uuid::FromString("123e4567-e89b-12d3-a456-426614174000").value())},
        // Same type cast test
        CastLiteralTestParam{.test_name = "IntToInt",
                             .source_literal = Literal::Int(42),
                             .target_type = int32(),
                             .expected_literal = Literal::Int(42)}),
    [](const ::testing::TestParamInfo<CastLiteralTestParam>& info) {
      return info.param.test_name;
    });

}  // namespace iceberg
