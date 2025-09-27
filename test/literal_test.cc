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

#include "iceberg/type.h"
#include "matchers.h"

namespace iceberg {

// Boolean type tests
TEST(LiteralTest, BooleanBasics) {
  auto true_literal = Literal::Boolean(true);
  auto false_literal = Literal::Boolean(false);

  EXPECT_EQ(true_literal.type()->type_id(), TypeId::kBoolean);
  EXPECT_EQ(false_literal.type()->type_id(), TypeId::kBoolean);

  EXPECT_EQ(true_literal.ToString(), "true");
  EXPECT_EQ(false_literal.ToString(), "false");
}

TEST(LiteralTest, BooleanComparison) {
  auto true_literal = Literal::Boolean(true);
  auto false_literal = Literal::Boolean(false);
  auto another_true = Literal::Boolean(true);

  EXPECT_EQ(true_literal <=> another_true, std::partial_ordering::equivalent);
  EXPECT_EQ(true_literal <=> false_literal, std::partial_ordering::greater);
  EXPECT_EQ(false_literal <=> true_literal, std::partial_ordering::less);
}

// Int type tests
TEST(LiteralTest, IntBasics) {
  auto int_literal = Literal::Int(42);
  auto negative_int = Literal::Int(-123);

  EXPECT_EQ(int_literal.type()->type_id(), TypeId::kInt);
  EXPECT_EQ(negative_int.type()->type_id(), TypeId::kInt);

  EXPECT_EQ(int_literal.ToString(), "42");
  EXPECT_EQ(negative_int.ToString(), "-123");
}

TEST(LiteralTest, IntComparison) {
  auto int1 = Literal::Int(10);
  auto int2 = Literal::Int(20);
  auto int3 = Literal::Int(10);

  EXPECT_EQ(int1 <=> int3, std::partial_ordering::equivalent);
  EXPECT_EQ(int1 <=> int2, std::partial_ordering::less);
  EXPECT_EQ(int2 <=> int1, std::partial_ordering::greater);
}

TEST(LiteralTest, IntCastTo) {
  auto int_literal = Literal::Int(42);

  // Cast to Long
  auto long_result = int_literal.CastTo(iceberg::int64());
  ASSERT_THAT(long_result, IsOk());
  EXPECT_EQ(long_result->type()->type_id(), TypeId::kLong);
  EXPECT_EQ(std::get<int64_t>(long_result->value()), 42L);

  // Cast to Float
  auto float_result = int_literal.CastTo(iceberg::float32());
  ASSERT_THAT(float_result, IsOk());
  EXPECT_EQ(float_result->type()->type_id(), TypeId::kFloat);

  // Cast to Double
  auto double_result = int_literal.CastTo(iceberg::float64());
  ASSERT_THAT(double_result, IsOk());
  EXPECT_EQ(double_result->type()->type_id(), TypeId::kDouble);
}

// Long type tests
TEST(LiteralTest, LongBasics) {
  auto long_literal = Literal::Long(1234567890L);
  auto negative_long = Literal::Long(-9876543210L);

  EXPECT_EQ(long_literal.type()->type_id(), TypeId::kLong);
  EXPECT_EQ(negative_long.type()->type_id(), TypeId::kLong);

  EXPECT_EQ(long_literal.ToString(), "1234567890");
  EXPECT_EQ(negative_long.ToString(), "-9876543210");
}

TEST(LiteralTest, LongComparison) {
  auto long1 = Literal::Long(100L);
  auto long2 = Literal::Long(200L);
  auto long3 = Literal::Long(100L);

  EXPECT_EQ(long1 <=> long3, std::partial_ordering::equivalent);
  EXPECT_EQ(long1 <=> long2, std::partial_ordering::less);
  EXPECT_EQ(long2 <=> long1, std::partial_ordering::greater);
}

TEST(LiteralTest, LongCastTo) {
  auto long_literal = Literal::Long(42L);

  // Cast to Int (within range)
  auto int_result = long_literal.CastTo(iceberg::int32());
  ASSERT_THAT(int_result, IsOk());
  EXPECT_EQ(int_result->type()->type_id(), TypeId::kInt);
  EXPECT_EQ(int_result->ToString(), "42");

  // Cast to Float
  auto float_result = long_literal.CastTo(iceberg::float32());
  ASSERT_THAT(float_result, IsOk());
  EXPECT_EQ(float_result->type()->type_id(), TypeId::kFloat);

  // Cast to Double
  auto double_result = long_literal.CastTo(iceberg::float64());
  ASSERT_THAT(double_result, IsOk());
  EXPECT_EQ(double_result->type()->type_id(), TypeId::kDouble);
}

TEST(LiteralTest, LongCastToIntOverflow) {
  auto max_long =
      Literal::Long(static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + 1);
  auto min_long =
      Literal::Long(static_cast<int64_t>(std::numeric_limits<int32_t>::min()) - 1);

  auto max_result = max_long.CastTo(iceberg::int32());
  ASSERT_THAT(max_result, IsOk());
  EXPECT_TRUE(max_result->IsAboveMax());

  auto min_result = min_long.CastTo(iceberg::int32());
  ASSERT_THAT(min_result, IsOk());
  EXPECT_TRUE(min_result->IsBelowMin());
}

// Float type tests
TEST(LiteralTest, FloatBasics) {
  auto float_literal = Literal::Float(3.14f);
  auto negative_float = Literal::Float(-2.71f);

  EXPECT_EQ(float_literal.type()->type_id(), TypeId::kFloat);
  EXPECT_EQ(negative_float.type()->type_id(), TypeId::kFloat);

  EXPECT_EQ(float_literal.ToString(), "3.140000");
  EXPECT_EQ(negative_float.ToString(), "-2.710000");
}

TEST(LiteralTest, FloatComparison) {
  auto float1 = Literal::Float(1.5f);
  auto float2 = Literal::Float(2.5f);
  auto float3 = Literal::Float(1.5f);

  EXPECT_EQ(float1 <=> float3, std::partial_ordering::equivalent);
  EXPECT_EQ(float1 <=> float2, std::partial_ordering::less);
  EXPECT_EQ(float2 <=> float1, std::partial_ordering::greater);
}

TEST(LiteralTest, FloatCastTo) {
  auto float_literal = Literal::Float(3.14f);

  // Cast to Double
  auto double_result = float_literal.CastTo(iceberg::float64());
  ASSERT_THAT(double_result, IsOk());
  EXPECT_EQ(double_result->type()->type_id(), TypeId::kDouble);
}

// Double type tests
TEST(LiteralTest, DoubleBasics) {
  auto double_literal = Literal::Double(std::numbers::pi);
  auto negative_double = Literal::Double(-std::numbers::e);

  EXPECT_EQ(double_literal.type()->type_id(), TypeId::kDouble);
  EXPECT_EQ(negative_double.type()->type_id(), TypeId::kDouble);

  EXPECT_EQ(double_literal.ToString(), "3.141593");
  EXPECT_EQ(negative_double.ToString(), "-2.718282");
}

TEST(LiteralTest, DoubleComparison) {
  auto double1 = Literal::Double(1.5);
  auto double2 = Literal::Double(2.5);
  auto double3 = Literal::Double(1.5);

  EXPECT_EQ(double1 <=> double3, std::partial_ordering::equivalent);
  EXPECT_EQ(double1 <=> double2, std::partial_ordering::less);
  EXPECT_EQ(double2 <=> double1, std::partial_ordering::greater);
}

// String type tests
TEST(LiteralTest, StringBasics) {
  auto string_literal = Literal::String("hello world");
  auto empty_string = Literal::String("");

  EXPECT_EQ(string_literal.type()->type_id(), TypeId::kString);
  EXPECT_EQ(empty_string.type()->type_id(), TypeId::kString);

  EXPECT_EQ(string_literal.ToString(), "hello world");
  EXPECT_EQ(empty_string.ToString(), "");
}

TEST(LiteralTest, StringComparison) {
  auto string1 = Literal::String("apple");
  auto string2 = Literal::String("banana");
  auto string3 = Literal::String("apple");

  EXPECT_EQ(string1 <=> string3, std::partial_ordering::equivalent);
  EXPECT_EQ(string1 <=> string2, std::partial_ordering::less);
  EXPECT_EQ(string2 <=> string1, std::partial_ordering::greater);
}

// Binary type tests
TEST(LiteralTest, BinaryBasics) {
  std::vector<uint8_t> data = {0x01, 0x02, 0x03, 0xFF};
  auto binary_literal = Literal::Binary(data);
  auto empty_binary = Literal::Binary({});

  EXPECT_EQ(binary_literal.type()->type_id(), TypeId::kBinary);
  EXPECT_EQ(empty_binary.type()->type_id(), TypeId::kBinary);

  EXPECT_EQ(binary_literal.ToString(), "010203FF");
  EXPECT_EQ(empty_binary.ToString(), "");
}

TEST(LiteralTest, BinaryComparison) {
  std::vector<uint8_t> data1 = {0x01, 0x02};
  std::vector<uint8_t> data2 = {0x01, 0x03};
  std::vector<uint8_t> data3 = {0x01, 0x02};

  auto binary1 = Literal::Binary(data1);
  auto binary2 = Literal::Binary(data2);
  auto binary3 = Literal::Binary(data3);

  EXPECT_EQ(binary1 <=> binary3, std::partial_ordering::equivalent);
  EXPECT_EQ(binary1 <=> binary2, std::partial_ordering::less);
  EXPECT_EQ(binary2 <=> binary1, std::partial_ordering::greater);
}

// Cross-type comparison tests
TEST(LiteralTest, CrossTypeComparison) {
  auto int_literal = Literal::Int(42);
  auto string_literal = Literal::String("42");

  // Different types should return unordered
  EXPECT_EQ(int_literal <=> string_literal, std::partial_ordering::unordered);
}

// Special value tests
TEST(LiteralTest, SpecialValues) {
  auto int_literal = Literal::Int(42);

  EXPECT_FALSE(int_literal.IsAboveMax());
  EXPECT_FALSE(int_literal.IsBelowMin());
}

// Same type cast test
TEST(LiteralTest, SameTypeCast) {
  auto int_literal = Literal::Int(42);

  auto same_type_result = int_literal.CastTo(iceberg::int32());
  ASSERT_THAT(same_type_result, IsOk());
  EXPECT_EQ(same_type_result->type()->type_id(), TypeId::kInt);
  EXPECT_EQ(same_type_result->ToString(), "42");
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

  // -0 should be less than +0
  EXPECT_EQ(neg_zero <=> pos_zero, std::partial_ordering::less);
}

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

        LiteralParam{"String",
                     {105, 99, 101, 98, 101, 114, 103},
                     Literal::String("iceberg"),
                     string()},
        LiteralParam{"StringLong",
                     {65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65},
                     Literal::String("AAAAAAAAAAAAAAAA"),
                     string()},

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

}  // namespace iceberg
