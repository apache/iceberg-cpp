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

namespace {

// Helper function to assert that a CastTo operation succeeds and checks
// the resulting type and value.
template <typename T>
void AssertCastSucceeds(const Result<Literal>& result, TypeId expected_type_id,
                        const T& expected_value) {
  ASSERT_THAT(result, IsOk());
  EXPECT_EQ(result->type()->type_id(), expected_type_id);
  ASSERT_NO_THROW(EXPECT_EQ(std::get<T>(result->value()), expected_value))
      << "Value type mismatch in std::get. Expected type for TypeId "
      << static_cast<int>(expected_type_id);
}

}  // namespace

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
  AssertCastSucceeds(int_literal.CastTo(iceberg::int64()), TypeId::kLong,
                     static_cast<int64_t>(42));

  // Cast to Float
  AssertCastSucceeds(int_literal.CastTo(iceberg::float32()), TypeId::kFloat, 42.0f);

  // Cast to Double
  AssertCastSucceeds(int_literal.CastTo(iceberg::float64()), TypeId::kDouble, 42.0);

  // Cast to Date
  AssertCastSucceeds(int_literal.CastTo(iceberg::date()), TypeId::kDate, 42);
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
  AssertCastSucceeds(long_literal.CastTo(iceberg::int32()), TypeId::kInt, 42);

  // Cast to Float
  AssertCastSucceeds(long_literal.CastTo(iceberg::float32()), TypeId::kFloat, 42.0f);

  // Cast to Double
  AssertCastSucceeds(long_literal.CastTo(iceberg::float64()), TypeId::kDouble, 42.0);

  // Cast to Date
  AssertCastSucceeds(long_literal.CastTo(iceberg::date()), TypeId::kDate, 42);

  // Cast to Time
  AssertCastSucceeds(long_literal.CastTo(iceberg::time()), TypeId::kTime,
                     static_cast<int64_t>(42L));

  // Cast to Timestamp
  AssertCastSucceeds(long_literal.CastTo(iceberg::timestamp()), TypeId::kTimestamp,
                     static_cast<int64_t>(42L));

  // Cast to TimestampTz
  AssertCastSucceeds(long_literal.CastTo(iceberg::timestamp_tz()), TypeId::kTimestampTz,
                     static_cast<int64_t>(42L));
}

TEST(LiteralTest, LongCastToOverflow) {
  // Test overflow cases
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

  max_result = max_long.CastTo(iceberg::date());
  ASSERT_THAT(max_result, IsOk());
  EXPECT_TRUE(max_result->IsAboveMax());

  min_result = min_long.CastTo(iceberg::date());
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
  AssertCastSucceeds(float_literal.CastTo(iceberg::float64()), TypeId::kDouble,
                     static_cast<double>(3.14f));
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

TEST(LiteralTest, DoubleCastTo) {
  auto double_literal = Literal::Double(3.14);

  // Cast to Float
  AssertCastSucceeds(double_literal.CastTo(iceberg::float32()), TypeId::kFloat, 3.14f);
}

TEST(LiteralTest, DoubleCastToOverflow) {
  // Test overflow cases for Double to Float
  auto max_double =
      Literal::Double(static_cast<double>(std::numeric_limits<float>::max()) * 2);
  auto min_double =
      Literal::Double(-static_cast<double>(std::numeric_limits<float>::max()) * 2);

  auto max_result = max_double.CastTo(iceberg::float32());
  ASSERT_THAT(max_result, IsOk());
  EXPECT_TRUE(max_result->IsAboveMax());

  auto min_result = min_double.CastTo(iceberg::float32());
  ASSERT_THAT(min_result, IsOk());
  EXPECT_TRUE(min_result->IsBelowMin());
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

  EXPECT_EQ(binary_literal.ToString(), "X'010203FF'");
  EXPECT_EQ(empty_binary.ToString(), "X''");
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

// Fixed type tests
TEST(LiteralTest, FixedBasics) {
  std::vector<uint8_t> data = {0x01, 0x02, 0x03, 0xFF};
  auto fixed_literal = Literal::Fixed(data);
  auto empty_fixed = Literal::Fixed({});

  EXPECT_EQ(fixed_literal.type()->type_id(), TypeId::kFixed);
  EXPECT_EQ(empty_fixed.type()->type_id(), TypeId::kFixed);

  EXPECT_EQ(fixed_literal.ToString(), "X'010203FF'");
  EXPECT_EQ(empty_fixed.ToString(), "X''");
}

TEST(LiteralTest, FixedComparison) {
  std::vector<uint8_t> data1 = {0x01, 0x02};
  std::vector<uint8_t> data2 = {0x01, 0x03};
  std::vector<uint8_t> data3 = {0x01, 0x02};

  auto fixed1 = Literal::Fixed(data1);
  auto fixed2 = Literal::Fixed(data2);
  auto fixed3 = Literal::Fixed(data3);

  EXPECT_EQ(fixed1 <=> fixed3, std::partial_ordering::equivalent);
  EXPECT_EQ(fixed1 <=> fixed2, std::partial_ordering::less);
  EXPECT_EQ(fixed2 <=> fixed1, std::partial_ordering::greater);
}

// Date type tests
TEST(LiteralTest, DateBasics) {
  auto date_literal = Literal::Date(19489);  // May 15, 2023
  auto negative_date = Literal::Date(-1);    // December 31, 1969

  EXPECT_EQ(date_literal.type()->type_id(), TypeId::kDate);
  EXPECT_EQ(negative_date.type()->type_id(), TypeId::kDate);

  EXPECT_EQ(date_literal.ToString(), "19489");
  EXPECT_EQ(negative_date.ToString(), "-1");
}

TEST(LiteralTest, DateComparison) {
  auto date1 = Literal::Date(100);
  auto date2 = Literal::Date(200);
  auto date3 = Literal::Date(100);

  EXPECT_EQ(date1 <=> date3, std::partial_ordering::equivalent);
  EXPECT_EQ(date1 <=> date2, std::partial_ordering::less);
  EXPECT_EQ(date2 <=> date1, std::partial_ordering::greater);
}

// Time type tests
TEST(LiteralTest, TimeBasics) {
  auto time_literal = Literal::Time(43200000000LL);  // 12:00:00 in microseconds
  auto midnight = Literal::Time(0LL);

  EXPECT_EQ(time_literal.type()->type_id(), TypeId::kTime);
  EXPECT_EQ(midnight.type()->type_id(), TypeId::kTime);

  EXPECT_EQ(time_literal.ToString(), "43200000000");
  EXPECT_EQ(midnight.ToString(), "0");
}

TEST(LiteralTest, TimeComparison) {
  auto time1 = Literal::Time(43200000000LL);  // 12:00:00
  auto time2 = Literal::Time(86400000000LL);  // 24:00:00 (invalid but for testing)
  auto time3 = Literal::Time(43200000000LL);

  EXPECT_EQ(time1 <=> time3, std::partial_ordering::equivalent);
  EXPECT_EQ(time1 <=> time2, std::partial_ordering::less);
  EXPECT_EQ(time2 <=> time1, std::partial_ordering::greater);
}

// Timestamp type tests
TEST(LiteralTest, TimestampBasics) {
  auto timestamp_literal =
      Literal::Timestamp(1684137600000000LL);  // May 15, 2023 12:00:00 UTC
  auto epoch = Literal::Timestamp(0LL);

  EXPECT_EQ(timestamp_literal.type()->type_id(), TypeId::kTimestamp);
  EXPECT_EQ(epoch.type()->type_id(), TypeId::kTimestamp);

  EXPECT_EQ(timestamp_literal.ToString(), "1684137600000000");
  EXPECT_EQ(epoch.ToString(), "0");
}

TEST(LiteralTest, TimestampComparison) {
  auto timestamp1 = Literal::Timestamp(1000000LL);
  auto timestamp2 = Literal::Timestamp(2000000LL);
  auto timestamp3 = Literal::Timestamp(1000000LL);

  EXPECT_EQ(timestamp1 <=> timestamp3, std::partial_ordering::equivalent);
  EXPECT_EQ(timestamp1 <=> timestamp2, std::partial_ordering::less);
  EXPECT_EQ(timestamp2 <=> timestamp1, std::partial_ordering::greater);
}

// TimestampTz type tests
TEST(LiteralTest, TimestampTzBasics) {
  auto timestamptz_literal =
      Literal::TimestampTz(1684137600000000LL);  // May 15, 2023 12:00:00 UTC
  auto epoch = Literal::TimestampTz(0LL);

  EXPECT_EQ(timestamptz_literal.type()->type_id(), TypeId::kTimestampTz);
  EXPECT_EQ(epoch.type()->type_id(), TypeId::kTimestampTz);

  EXPECT_EQ(timestamptz_literal.ToString(), "1684137600000000");
  EXPECT_EQ(epoch.ToString(), "0");
}

TEST(LiteralTest, TimestampTzComparison) {
  auto timestamptz1 = Literal::TimestampTz(1000000LL);
  auto timestamptz2 = Literal::TimestampTz(2000000LL);
  auto timestamptz3 = Literal::TimestampTz(1000000LL);

  EXPECT_EQ(timestamptz1 <=> timestamptz3, std::partial_ordering::equivalent);
  EXPECT_EQ(timestamptz1 <=> timestamptz2, std::partial_ordering::less);
  EXPECT_EQ(timestamptz2 <=> timestamptz1, std::partial_ordering::greater);
}

TEST(LiteralTest, BinaryCastTo) {
  std::vector<uint8_t> data4 = {0x01, 0x02, 0x03, 0x04};
  auto binary_literal = Literal::Binary(data4);

  // Cast to Fixed with matching length
  AssertCastSucceeds(binary_literal.CastTo(iceberg::fixed(4)), TypeId::kFixed, data4);

  // Cast to Fixed with different length should fail
  EXPECT_THAT(binary_literal.CastTo(iceberg::fixed(5)),
              IsError(ErrorKind::kInvalidArgument));
}

TEST(LiteralTest, FixedCastTo) {
  std::vector<uint8_t> data4 = {0x01, 0x02, 0x03, 0x04};
  auto fixed_literal = Literal::Fixed(data4);

  // Cast to Binary
  AssertCastSucceeds(fixed_literal.CastTo(iceberg::binary()), TypeId::kBinary, data4);

  // Cast to Fixed with same length
  AssertCastSucceeds(fixed_literal.CastTo(iceberg::fixed(4)), TypeId::kFixed, data4);

  // Cast to Fixed with different length should fail
  EXPECT_THAT(fixed_literal.CastTo(iceberg::fixed(5)), IsError(ErrorKind::kNotSupported));
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

}  // namespace iceberg
