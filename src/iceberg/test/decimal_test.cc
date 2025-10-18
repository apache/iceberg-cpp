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
#include "iceberg/util/decimal.h"

#include <algorithm>
#include <array>
#include <bit>
#include <cstdint>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/util/int128.h"
#include "matchers.h"

namespace iceberg {

namespace {

void AssertDecimalFromString(const std::string& s, const Decimal& expected,
                             int32_t expected_precision, int32_t expected_scale) {
  int32_t precision = 0;
  int32_t scale = 0;
  auto result = Decimal::FromString(s, &precision, &scale);
  EXPECT_THAT(result, IsOk());
  const Decimal& actual = result.value();
  EXPECT_EQ(expected, actual);
  EXPECT_EQ(expected_precision, precision);
  EXPECT_EQ(expected_scale, scale);
}

}  // namespace

TEST(DecimalTest, Basics) {
  AssertDecimalFromString("234.23445", Decimal(23423445), 8, 5);

  std::string string_value("-23049223942343532412");
  Decimal result(string_value);
  Decimal expected(static_cast<int64_t>(-230492239423435324));
  ASSERT_EQ(result, expected * 100 - 12);
  ASSERT_NE(result.high(), 0);

  result = Decimal("-23049223942343.532412");
  ASSERT_EQ(result, expected * 100 - 12);
  ASSERT_NE(result.high(), 0);
}

TEST(DecimalTest, StringStartingWithSign) {
  AssertDecimalFromString("+234.567", Decimal(234567), 6, 3);
  AssertDecimalFromString("+2342394230592.232349023094",
                          Decimal("2342394230592232349023094"), 25, 12);
  AssertDecimalFromString("-234.567", Decimal("-234567"), 6, 3);
  AssertDecimalFromString("-2342394230592.232349023094",
                          Decimal("-2342394230592232349023094"), 25, 12);
}

TEST(DecimalTest, StringWithLeadingZeros) {
  AssertDecimalFromString("0000000000000000000000000000000.234", Decimal(234), 3, 3);
  AssertDecimalFromString("0000000000000000000000000000000.23400", Decimal(23400), 5, 5);
  AssertDecimalFromString("234.00", Decimal(23400), 5, 2);
  AssertDecimalFromString("234.0", Decimal(2340), 4, 1);
  AssertDecimalFromString("0000000", Decimal(0), 0, 0);
  AssertDecimalFromString("000.0000", Decimal(0), 4, 4);
  AssertDecimalFromString(".00000", Decimal(0), 5, 5);
}

TEST(DecimalTest, DecimalWithExponent) {
  AssertDecimalFromString("1E1", Decimal(10), 2, 0);
  AssertDecimalFromString("234.23445e2", Decimal(23423445), 8, 3);
  AssertDecimalFromString("234.23445e-2", Decimal(23423445), 8, 7);
  AssertDecimalFromString("234.23445E2", Decimal(23423445), 8, 3);
  AssertDecimalFromString("234.23445E-2", Decimal(23423445), 8, 7);
  AssertDecimalFromString("1.23E-8", Decimal(123), 3, 10);
}

TEST(DecimalTest, SmallValues) {
  struct TestValue {
    std::string s;
    int64_t expected;
    int32_t expected_precision;
    int32_t expected_scale;
  };

  for (const auto& tv : std::vector<TestValue>{
           {.s = "12.3", .expected = 123LL, .expected_precision = 3, .expected_scale = 1},
           {.s = "0.00123",
            .expected = 123LL,
            .expected_precision = 5,
            .expected_scale = 5},
           {.s = "1.23E-8",
            .expected = 123LL,
            .expected_precision = 3,
            .expected_scale = 10},
           {.s = "-1.23E-8",
            .expected = -123LL,
            .expected_precision = 3,
            .expected_scale = 10},
           {.s = "1.23E+3",
            .expected = 1230LL,
            .expected_precision = 4,
            .expected_scale = 0},
           {.s = "-1.23E+3",
            .expected = -1230LL,
            .expected_precision = 4,
            .expected_scale = 0},
           {.s = "1.23E+5",
            .expected = 123000LL,
            .expected_precision = 6,
            .expected_scale = 0},
           {.s = "1.2345E+7",
            .expected = 12345000LL,
            .expected_precision = 8,
            .expected_scale = 0},
           {.s = "1.23e-8",
            .expected = 123LL,
            .expected_precision = 3,
            .expected_scale = 10},
           {.s = "-1.23e-8",
            .expected = -123LL,
            .expected_precision = 3,
            .expected_scale = 10},
           {.s = "1.23e+3",
            .expected = 1230LL,
            .expected_precision = 4,
            .expected_scale = 0},
           {.s = "-1.23e+3",
            .expected = -1230LL,
            .expected_precision = 4,
            .expected_scale = 0},
           {.s = "1.23e+5",
            .expected = 123000LL,
            .expected_precision = 6,
            .expected_scale = 0},
           {.s = "1.2345e+7",
            .expected = 12345000LL,
            .expected_precision = 8,
            .expected_scale = 0}}) {
    AssertDecimalFromString(tv.s, Decimal(tv.expected), tv.expected_precision,
                            tv.expected_scale);
  }
}

TEST(DecimalTest, LargeValues) {
  const std::array<std::string, 4> string_values = {
      "99999999999999999999999999999999999999", "-99999999999999999999999999999999999999",
      "170141183460469231731687303715884105727",  // maximum positive value
      "-170141183460469231731687303715884105728"  // minimum negative value
  };

  for (const auto& s : string_values) {
    const Decimal value(s);
    const std::string printed_value = value.ToIntegerString();
    EXPECT_EQ(printed_value, s) << "Expected: " << s << ", but got: " << printed_value;
  }
}

TEST(DecimalTest, TestStringRoundTrip) {
  static constexpr std::array<uint64_t, 11> kTestBits = {
      0,
      1,
      999,
      1000,
      std::numeric_limits<int32_t>::max(),
      (1ull << 31),
      std::numeric_limits<uint32_t>::max(),
      (1ull << 32),
      std::numeric_limits<int64_t>::max(),
      (1ull << 63),
      std::numeric_limits<uint64_t>::max(),
  };
  static constexpr std::array<int32_t, 3> kScales = {0, 1, 10};
  for (uint64_t high : kTestBits) {
    for (uint64_t low : kTestBits) {
      Decimal value(high, low);
      for (int32_t scale : kScales) {
        auto result = value.ToString(scale);

        ASSERT_THAT(result, IsOk())
            << "Failed to convert Decimal to string: " << value.ToIntegerString()
            << ", scale: " << scale;

        auto round_trip = Decimal::FromString(result.value());
        ASSERT_THAT(round_trip, IsOk())
            << "Failed to convert string back to Decimal: " << result.value();

        EXPECT_EQ(value, round_trip.value())
            << "Round trip failed for value: " << value.ToIntegerString()
            << ", scale: " << scale;
      }
    }
  }
}

TEST(DecimalTest, FromStringLimits) {
  AssertDecimalFromString("1e37", Decimal(542101086242752217ULL, 68739955140067328ULL),
                          38, 0);

  AssertDecimalFromString(
      "-1e37", Decimal(17904642987466799398ULL, 18378004118569484288ULL), 38, 0);
  AssertDecimalFromString(
      "9.87e37", Decimal(5350537721215964381ULL, 15251391175463010304ULL), 38, 0);
  AssertDecimalFromString(
      "-9.87e37", Decimal(13096206352493587234ULL, 3195352898246541312ULL), 38, 0);
  AssertDecimalFromString("12345678901234567890123456789012345678",
                          Decimal(669260594276348691ULL, 14143994781733811022ULL), 38, 0);
  AssertDecimalFromString("-12345678901234567890123456789012345678",
                          Decimal(17777483479433202924ULL, 4302749291975740594ULL), 38,
                          0);

  // "9..9" (38 times)
  const auto dec38times9pos = Decimal(5421010862427522170ULL, 687399551400673279ULL);
  // "-9..9" (38 times)
  const auto dec38times9neg = Decimal(13025733211282029445ULL, 17759344522308878337ULL);

  AssertDecimalFromString("99999999999999999999999999999999999999", dec38times9pos, 38,
                          0);
  AssertDecimalFromString("-99999999999999999999999999999999999999", dec38times9neg, 38,
                          0);
  AssertDecimalFromString("9.9999999999999999999999999999999999999e37", dec38times9pos,
                          38, 0);
  AssertDecimalFromString("-9.9999999999999999999999999999999999999e37", dec38times9neg,
                          38, 0);

  // No exponent, many fractional digits
  AssertDecimalFromString("9.9999999999999999999999999999999999999", dec38times9pos, 38,
                          37);
  AssertDecimalFromString("-9.9999999999999999999999999999999999999", dec38times9neg, 38,
                          37);
  AssertDecimalFromString("0.99999999999999999999999999999999999999", dec38times9pos, 38,
                          38);
  AssertDecimalFromString("-0.99999999999999999999999999999999999999", dec38times9neg, 38,
                          38);

  // Negative exponent
  AssertDecimalFromString("1e-38", Decimal(0, 1), 1, 38);
  AssertDecimalFromString(
      "-1e-38", Decimal(18446744073709551615ULL, 18446744073709551615ULL), 1, 38);
  AssertDecimalFromString("9.99e-36", Decimal(0, 999), 3, 38);
  AssertDecimalFromString(
      "-9.99e-36", Decimal(18446744073709551615ULL, 18446744073709550617ULL), 3, 38);
  AssertDecimalFromString("987e-38", Decimal(0, 987), 3, 38);
  AssertDecimalFromString(
      "-987e-38", Decimal(18446744073709551615ULL, 18446744073709550629ULL), 3, 38);
  AssertDecimalFromString("99999999999999999999999999999999999999e-37", dec38times9pos,
                          38, 37);
  AssertDecimalFromString("-99999999999999999999999999999999999999e-37", dec38times9neg,
                          38, 37);
  AssertDecimalFromString("99999999999999999999999999999999999999e-38", dec38times9pos,
                          38, 38);
  AssertDecimalFromString("-99999999999999999999999999999999999999e-38", dec38times9neg,
                          38, 38);
}

TEST(DecimalTest, FromStringInvalid) {
  // Empty string
  auto result = Decimal::FromString("");
  ASSERT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  ASSERT_THAT(result, HasErrorMessage("Empty string is not a valid Decimal"));
  for (const auto& invalid_string :
       std::vector<std::string>{"-", "0.0.0", "0-13-32", "a", "-23092.235-",
                                "-+23092.235", "+-23092.235", "00a", "1e1a", "0.00123D/3",
                                "1.23eA8", "1.23E+3A", "-1.23E--5", "1.2345E+++07"}) {
    auto result = Decimal::FromString(invalid_string);
    ASSERT_THAT(result, IsError(ErrorKind::kInvalidArgument));
    ASSERT_THAT(result, HasErrorMessage("Invalid decimal string"));
  }

  for (const auto& invalid_string :
       std::vector<std::string>{"1e39", "-1e39", "9e39", "-9e39", "9.9e40", "-9.9e40"}) {
    auto result = Decimal::FromString(invalid_string);
    ASSERT_THAT(result, IsError(ErrorKind::kInvalidArgument));
    ASSERT_THAT(result, HasErrorMessage("scale must be in the range"));
  }
}

TEST(DecimalTest, Division) {
  const std::string expected_string_value("-23923094039234029");
  const Decimal value(expected_string_value);
  const Decimal result(value / 3);
  const Decimal expected_value("-7974364679744676");
  ASSERT_EQ(expected_value, result);
}

TEST(DecimalTest, ToString) {
  struct ToStringCase {
    int64_t test_value;
    int32_t scale;
    const char* expected_string;
  };

  for (const auto& t : std::vector<ToStringCase>{
           {.test_value = 0, .scale = -1, .expected_string = "0E+1"},
           {.test_value = 0, .scale = 0, .expected_string = "0"},
           {.test_value = 0, .scale = 1, .expected_string = "0.0"},
           {.test_value = 0, .scale = 6, .expected_string = "0.000000"},
           {.test_value = 2, .scale = 7, .expected_string = "2E-7"},
           {.test_value = 2, .scale = -1, .expected_string = "2E+1"},
           {.test_value = 2, .scale = 0, .expected_string = "2"},
           {.test_value = 2, .scale = 1, .expected_string = "0.2"},
           {.test_value = 2, .scale = 6, .expected_string = "0.000002"},
           {.test_value = -2, .scale = 7, .expected_string = "-2E-7"},
           {.test_value = -2, .scale = 7, .expected_string = "-2E-7"},
           {.test_value = -2, .scale = -1, .expected_string = "-2E+1"},
           {.test_value = -2, .scale = 0, .expected_string = "-2"},
           {.test_value = -2, .scale = 1, .expected_string = "-0.2"},
           {.test_value = -2, .scale = 6, .expected_string = "-0.000002"},
           {.test_value = -2, .scale = 7, .expected_string = "-2E-7"},
           {.test_value = 123, .scale = -3, .expected_string = "1.23E+5"},
           {.test_value = 123, .scale = -1, .expected_string = "1.23E+3"},
           {.test_value = 123, .scale = 1, .expected_string = "12.3"},
           {.test_value = 123, .scale = 0, .expected_string = "123"},
           {.test_value = 123, .scale = 5, .expected_string = "0.00123"},
           {.test_value = 123, .scale = 8, .expected_string = "0.00000123"},
           {.test_value = 123, .scale = 9, .expected_string = "1.23E-7"},
           {.test_value = 123, .scale = 10, .expected_string = "1.23E-8"},
           {.test_value = -123, .scale = -3, .expected_string = "-1.23E+5"},
           {.test_value = -123, .scale = -1, .expected_string = "-1.23E+3"},
           {.test_value = -123, .scale = 1, .expected_string = "-12.3"},
           {.test_value = -123, .scale = 0, .expected_string = "-123"},
           {.test_value = -123, .scale = 5, .expected_string = "-0.00123"},
           {.test_value = -123, .scale = 8, .expected_string = "-0.00000123"},
           {.test_value = -123, .scale = 9, .expected_string = "-1.23E-7"},
           {.test_value = -123, .scale = 10, .expected_string = "-1.23E-8"},
           {.test_value = 1000000000, .scale = -3, .expected_string = "1.000000000E+12"},
           {.test_value = 1000000000, .scale = -1, .expected_string = "1.000000000E+10"},
           {.test_value = 1000000000, .scale = 0, .expected_string = "1000000000"},
           {.test_value = 1000000000, .scale = 1, .expected_string = "100000000.0"},
           {.test_value = 1000000000, .scale = 5, .expected_string = "10000.00000"},
           {.test_value = 1000000000,
            .scale = 15,
            .expected_string = "0.000001000000000"},
           {.test_value = 1000000000, .scale = 16, .expected_string = "1.000000000E-7"},
           {.test_value = 1000000000, .scale = 17, .expected_string = "1.000000000E-8"},
           {.test_value = -1000000000,
            .scale = -3,
            .expected_string = "-1.000000000E+12"},
           {.test_value = -1000000000,
            .scale = -1,
            .expected_string = "-1.000000000E+10"},
           {.test_value = -1000000000, .scale = 0, .expected_string = "-1000000000"},
           {.test_value = -1000000000, .scale = 1, .expected_string = "-100000000.0"},
           {.test_value = -1000000000, .scale = 5, .expected_string = "-10000.00000"},
           {.test_value = -1000000000,
            .scale = 15,
            .expected_string = "-0.000001000000000"},
           {.test_value = -1000000000, .scale = 16, .expected_string = "-1.000000000E-7"},
           {.test_value = -1000000000, .scale = 17, .expected_string = "-1.000000000E-8"},
           {.test_value = 1234567890123456789LL,
            .scale = -3,
            .expected_string = "1.234567890123456789E+21"},
           {.test_value = 1234567890123456789LL,
            .scale = -1,
            .expected_string = "1.234567890123456789E+19"},
           {.test_value = 1234567890123456789LL,
            .scale = 0,
            .expected_string = "1234567890123456789"},
           {.test_value = 1234567890123456789LL,
            .scale = 1,
            .expected_string = "123456789012345678.9"},
           {.test_value = 1234567890123456789LL,
            .scale = 5,
            .expected_string = "12345678901234.56789"},
           {.test_value = 1234567890123456789LL,
            .scale = 24,
            .expected_string = "0.000001234567890123456789"},
           {.test_value = 1234567890123456789LL,
            .scale = 25,
            .expected_string = "1.234567890123456789E-7"},
           {.test_value = -1234567890123456789LL,
            .scale = -3,
            .expected_string = "-1.234567890123456789E+21"},
           {.test_value = -1234567890123456789LL,
            .scale = -1,
            .expected_string = "-1.234567890123456789E+19"},
           {.test_value = -1234567890123456789LL,
            .scale = 0,
            .expected_string = "-1234567890123456789"},
           {.test_value = -1234567890123456789LL,
            .scale = 1,
            .expected_string = "-123456789012345678.9"},
           {.test_value = -1234567890123456789LL,
            .scale = 5,
            .expected_string = "-12345678901234.56789"},
           {.test_value = -1234567890123456789LL,
            .scale = 24,
            .expected_string = "-0.000001234567890123456789"},
           {.test_value = -1234567890123456789LL,
            .scale = 25,
            .expected_string = "-1.234567890123456789E-7"},
       }) {
    const Decimal value(t.test_value);
    auto result = value.ToString(t.scale);
    ASSERT_THAT(result, IsOk())
        << "Failed to convert Decimal to string: " << value.ToIntegerString()
        << ", scale: " << t.scale;

    EXPECT_EQ(result.value(), t.expected_string)
        << "Expected: " << t.expected_string << ", but got: " << result.value();
  }
}

TEST(DecimalTest, FromBigEndian) {
  // We test out a variety of scenarios:
  //
  // * Positive values that are left shifted
  //   and filled in with the same bit pattern
  // * Negated of the positive values
  // * Complement of the positive values
  //
  // For the positive values, we can call FromBigEndian
  // with a length that is less than 16, whereas we must
  // pass all 16 bytes for the negative and complement.
  //
  // We use a number of bit patterns to increase the coverage
  // of scenarios
  constexpr int WidthMinusOne = Decimal::kByteWidth - 1;

  for (int32_t start : {1, 15, /* 00001111 */
                        85,    /* 01010101 */
                        127 /* 01111111 */}) {
    Decimal value(start);
    for (int ii = 0; ii < Decimal::kByteWidth; ++ii) {
      auto native_endian = value.ToBytes();
      if constexpr (std::endian::native == std::endian::little) {
        // convert to big endian
        std::ranges::reverse(native_endian);
      }
      // Limit the number of bytes we are passing to make
      // sure that it works correctly. That's why all of the
      // 'start' values don't have a 1 in the most significant
      // bit place
      auto result =
          Decimal::FromBigEndian(native_endian.data() + WidthMinusOne - ii, ii + 1);
      ASSERT_THAT(result, IsOk());
      const Decimal& decimal = result.value();
      EXPECT_EQ(decimal, value);

      // Negate it
      auto negated = -value;
      native_endian = negated.ToBytes();

      if constexpr (std::endian::native == std::endian::little) {
        // convert to big endian
        std::ranges::reverse(native_endian);
      }

      result = Decimal::FromBigEndian(native_endian.data() + WidthMinusOne - ii, ii + 1);
      ASSERT_THAT(result, IsOk());
      const Decimal& negated_decimal = result.value();
      EXPECT_EQ(negated_decimal, negated);

      // Take the complement
      auto complement = ~value;
      native_endian = complement.ToBytes();

      if constexpr (std::endian::native == std::endian::little) {
        // convert to big endian
        std::ranges::reverse(native_endian);
      }
      result = Decimal::FromBigEndian(native_endian.data(), Decimal::kByteWidth);
      ASSERT_THAT(result, IsOk());
      const Decimal& complement_decimal = result.value();
      EXPECT_EQ(complement_decimal, complement);

      value <<= 2;
      value += Decimal(start);
    }
  }
}

TEST(DecimalTest, FromBigEndianInvalid) {
  ASSERT_THAT(Decimal::FromBigEndian(nullptr, -1), IsError(ErrorKind::kInvalidArgument));
  ASSERT_THAT(Decimal::FromBigEndian(nullptr, Decimal::kByteWidth + 1),
              IsError(ErrorKind::kInvalidArgument));
}

TEST(DecimalTest, ToBigEndian) {
  std::vector<int64_t> high_values = {0,
                                      1,
                                      -1,
                                      INT32_MAX,
                                      INT32_MIN,
                                      static_cast<int64_t>(INT32_MAX) + 1,
                                      static_cast<int64_t>(INT32_MIN) - 1,
                                      INT64_MAX,
                                      INT64_MIN};
  std::vector<uint64_t> low_values = {0,
                                      1,
                                      255,
                                      UINT32_MAX,
                                      static_cast<uint64_t>(UINT32_MAX) + 1,
                                      static_cast<uint64_t>(UINT32_MAX) + 2,
                                      static_cast<uint64_t>(UINT32_MAX) + 3,
                                      static_cast<uint64_t>(UINT32_MAX) + 4,
                                      static_cast<uint64_t>(UINT32_MAX) + 5,
                                      static_cast<uint64_t>(UINT32_MAX) + 6,
                                      static_cast<uint64_t>(UINT32_MAX) + 7,
                                      static_cast<uint64_t>(UINT32_MAX) + 8,
                                      UINT64_MAX};

  for (int64_t high : high_values) {
    for (uint64_t low : low_values) {
      Decimal decimal(high, low);
      auto bytes = decimal.ToBigEndian();
      auto result = Decimal::FromBigEndian(bytes.data(), bytes.size());
      ASSERT_THAT(result, IsOk());
      EXPECT_EQ(result.value(), decimal);
    }
  }

  for (int128_t value : std::vector<int128_t>{-INT64_MAX, -INT32_MAX, -255, -1, 0, 1, 255,
                                              256, INT32_MAX, INT64_MAX}) {
    Decimal decimal(value);
    auto bytes = decimal.ToBigEndian();
    auto result = Decimal::FromBigEndian(bytes.data(), bytes.size());
    ASSERT_THAT(result, IsOk());
    EXPECT_EQ(result.value(), decimal);
  }
}

TEST(DecimalTestFunctionality, Multiply) {
  ASSERT_EQ(Decimal(60501), Decimal(301) * Decimal(201));
  ASSERT_EQ(Decimal(-60501), Decimal(-301) * Decimal(201));
  ASSERT_EQ(Decimal(-60501), Decimal(301) * Decimal(-201));
  ASSERT_EQ(Decimal(60501), Decimal(-301) * Decimal(-201));

  // Edge cases
  for (auto x : std::vector<int128_t>{-INT64_MAX, -INT32_MAX, 0, INT32_MAX, INT64_MAX}) {
    for (auto y :
         std::vector<int128_t>{-INT32_MAX, -32, -2, -1, 0, 1, 2, 32, INT32_MAX}) {
      Decimal decimal_x(x);
      Decimal decimal_y(y);
      Decimal result = decimal_x * decimal_y;
      EXPECT_EQ(Decimal(x * y), result) << " x: " << decimal_x << " y: " << decimal_y;
    }
  }
}

TEST(DecimalTestFunctionality, Divide) {
  ASSERT_EQ(Decimal(66), Decimal(20100) / Decimal(301));
  ASSERT_EQ(Decimal(-66), Decimal(-20100) / Decimal(301));
  ASSERT_EQ(Decimal(-66), Decimal(20100) / Decimal(-301));
  ASSERT_EQ(Decimal(66), Decimal(-20100) / Decimal(-301));

  for (auto x : std::vector<int128_t>{-INT64_MAX, -INT32_MAX, 0, INT32_MAX, INT64_MAX}) {
    for (auto y : std::vector<int128_t>{-INT32_MAX, -32, -2, -1, 1, 2, 32, INT32_MAX}) {
      Decimal decimal_x(x);
      Decimal decimal_y(y);
      Decimal result = decimal_x / decimal_y;
      EXPECT_EQ(Decimal(x / y), result) << " x: " << decimal_x << " y: " << decimal_y;
    }
  }
}

TEST(DecimalTestFunctionality, Modulo) {
  ASSERT_EQ(Decimal(234), Decimal(20100) % Decimal(301));
  ASSERT_EQ(Decimal(-234), Decimal(-20100) % Decimal(301));
  ASSERT_EQ(Decimal(234), Decimal(20100) % Decimal(-301));
  ASSERT_EQ(Decimal(-234), Decimal(-20100) % Decimal(-301));

  // Test some edge cases
  for (auto x : std::vector<int128_t>{-INT64_MAX, -INT32_MAX, 0, INT32_MAX, INT64_MAX}) {
    for (auto y : std::vector<int128_t>{-INT32_MAX, -32, -2, -1, 1, 2, 32, INT32_MAX}) {
      Decimal decimal_x(x);
      Decimal decimal_y(y);
      Decimal result = decimal_x % decimal_y;
      EXPECT_EQ(Decimal(x % y), result) << " x: " << decimal_x << " y: " << decimal_y;
    }
  }
}

TEST(DecimalTestFunctionality, Sign) {
  ASSERT_EQ(1, Decimal(999999).Sign());
  ASSERT_EQ(-1, Decimal(-999999).Sign());
  ASSERT_EQ(1, Decimal(0).Sign());
}

TEST(DecimalTestFunctionality, FitsInPrecision) {
  ASSERT_TRUE(Decimal("0").FitsInPrecision(1));
  ASSERT_TRUE(Decimal("9").FitsInPrecision(1));
  ASSERT_TRUE(Decimal("-9").FitsInPrecision(1));
  ASSERT_FALSE(Decimal("10").FitsInPrecision(1));
  ASSERT_FALSE(Decimal("-10").FitsInPrecision(1));

  ASSERT_TRUE(Decimal("0").FitsInPrecision(2));
  ASSERT_TRUE(Decimal("10").FitsInPrecision(2));
  ASSERT_TRUE(Decimal("-10").FitsInPrecision(2));
  ASSERT_TRUE(Decimal("99").FitsInPrecision(2));
  ASSERT_TRUE(Decimal("-99").FitsInPrecision(2));
  ASSERT_FALSE(Decimal("100").FitsInPrecision(2));
  ASSERT_FALSE(Decimal("-100").FitsInPrecision(2));

  std::string max_nines(Decimal::kMaxPrecision, '9');
  ASSERT_TRUE(Decimal(max_nines).FitsInPrecision(Decimal::kMaxPrecision));
  ASSERT_TRUE(Decimal("-" + max_nines).FitsInPrecision(Decimal::kMaxPrecision));

  std::string max_zeros(Decimal::kMaxPrecision, '0');
  ASSERT_FALSE(Decimal("1" + max_zeros).FitsInPrecision(Decimal::kMaxPrecision));
  ASSERT_FALSE(Decimal("-1" + max_zeros).FitsInPrecision(Decimal::kMaxPrecision));
}

TEST(DecimalTest, LeftShift) {
  auto check = [](int128_t x, uint32_t bits) {
    auto expected = Decimal(x << bits);
    auto actual = Decimal(x) << bits;
    ASSERT_EQ(actual.low(), expected.low());
    ASSERT_EQ(actual.high(), expected.high());
  };

  ASSERT_EQ(Decimal("0"), Decimal("0") << 0);
  ASSERT_EQ(Decimal("0"), Decimal("0") << 1);
  ASSERT_EQ(Decimal("0"), Decimal("0") << 63);
  ASSERT_EQ(Decimal("0"), Decimal("0") << 127);

  check(123, 0);
  check(123, 1);
  check(123, 63);
  check(123, 64);
  check(123, 120);

  ASSERT_EQ(Decimal("199999999999998"), Decimal("99999999999999") << 1);
  ASSERT_EQ(Decimal("3435973836799965640261632"), Decimal("99999999999999") << 35);
  ASSERT_EQ(Decimal("120892581961461708544797985370825293824"), Decimal("99999999999999")
                                                                    << 80);

  ASSERT_EQ(Decimal("1234567890123456789012"), Decimal("1234567890123456789012") << 0);
  ASSERT_EQ(Decimal("2469135780246913578024"), Decimal("1234567890123456789012") << 1);
  ASSERT_EQ(Decimal("88959991838777271103427858320412639232"),
            Decimal("1234567890123456789012") << 56);

  check(-123, 0);
  check(-123, 1);
  check(-123, 63);
  check(-123, 64);
  check(-123, 120);

  ASSERT_EQ(Decimal("-199999999999998"), Decimal("-99999999999999") << 1);
  ASSERT_EQ(Decimal("-3435973836799965640261632"), Decimal("-99999999999999") << 35);
  ASSERT_EQ(Decimal("-120892581961461708544797985370825293824"),
            Decimal("-99999999999999") << 80);

  ASSERT_EQ(Decimal("-1234567890123456789012"), Decimal("-1234567890123456789012") << 0);
  ASSERT_EQ(Decimal("-2469135780246913578024"), Decimal("-1234567890123456789012") << 1);
  ASSERT_EQ(Decimal("-88959991838777271103427858320412639232"),
            Decimal("-1234567890123456789012") << 56);
}

TEST(DecimalTest, RightShift) {
  ASSERT_EQ(Decimal("0"), Decimal("0") >> 0);
  ASSERT_EQ(Decimal("0"), Decimal("0") >> 1);
  ASSERT_EQ(Decimal("0"), Decimal("0") >> 63);
  ASSERT_EQ(Decimal("0"), Decimal("0") >> 127);

  ASSERT_EQ(Decimal("1"), Decimal("1") >> 0);
  ASSERT_EQ(Decimal("0"), Decimal("1") >> 1);
  ASSERT_EQ(Decimal("0"), Decimal("1") >> 63);
  ASSERT_EQ(Decimal("0"), Decimal("1") >> 127);

  ASSERT_EQ(Decimal("-1"), Decimal("-1") >> 0);
  ASSERT_EQ(Decimal("-1"), Decimal("-1") >> 1);
  ASSERT_EQ(Decimal("-1"), Decimal("-1") >> 63);
  ASSERT_EQ(Decimal("-1"), Decimal("-1") >> 127);

  ASSERT_EQ(Decimal("1096516"), Decimal("1234567890123456789012") >> 50);
  ASSERT_EQ(Decimal("66"), Decimal("1234567890123456789012") >> 64);
  ASSERT_EQ(Decimal("2"), Decimal("1234567890123456789012") >> 69);
  ASSERT_EQ(Decimal("0"), Decimal("1234567890123456789012") >> 71);
  ASSERT_EQ(Decimal("0"), Decimal("1234567890123456789012") >> 127);

  ASSERT_EQ(Decimal("-1096517"), Decimal("-1234567890123456789012") >> 50);
  ASSERT_EQ(Decimal("-67"), Decimal("-1234567890123456789012") >> 64);
  ASSERT_EQ(Decimal("-3"), Decimal("-1234567890123456789012") >> 69);
  ASSERT_EQ(Decimal("-1"), Decimal("-1234567890123456789012") >> 71);
  ASSERT_EQ(Decimal("-1"), Decimal("-1234567890123456789012") >> 127);
}

TEST(DecimalTest, Negate) {
  auto check = [](Decimal pos, Decimal neg) {
    EXPECT_EQ(-pos, neg);
    EXPECT_EQ(-neg, pos);
  };

  check(Decimal(0, 0), Decimal(0, 0));
  check(Decimal(0, 1), Decimal(-1, 0xFFFFFFFFFFFFFFFFULL));
  check(Decimal(0, 2), Decimal(-1, 0xFFFFFFFFFFFFFFFEULL));
  check(Decimal(0, 0x8000000000000000ULL), Decimal(-1, 0x8000000000000000ULL));
  check(Decimal(0, 0xFFFFFFFFFFFFFFFFULL), Decimal(-1, 1));
  check(Decimal(12, 0), Decimal(-12, 0));
  check(Decimal(12, 1), Decimal(-13, 0xFFFFFFFFFFFFFFFFULL));
  check(Decimal(12, 0xFFFFFFFFFFFFFFFFULL), Decimal(-13, 1));
}

TEST(DecimalTest, Rescale) {
  ASSERT_EQ(Decimal(11100), Decimal(111).Rescale(0, 2).value());
  ASSERT_EQ(Decimal(111), Decimal(11100).Rescale(2, 0).value());
  ASSERT_EQ(Decimal(5), Decimal(500000).Rescale(6, 1).value());
  ASSERT_EQ(Decimal(500000), Decimal(5).Rescale(1, 6).value());

  ASSERT_THAT(Decimal(5555555).Rescale(6, 1), IsError(ErrorKind::kInvalid));
}

TEST(DecimalTest, Compare) {
  // max positive unscaled value
  // 10^38 - 1 scale cause overflow
  ASSERT_EQ(Decimal::Compare(Decimal("99999999999999999999999999999999999999"),
                             Decimal("99999999999999999999999999999999999999"), 2, 3),
            std::partial_ordering::greater);
  // 10^37 - 1 scale no overflow
  ASSERT_EQ(Decimal::Compare(Decimal("9999999999999999999999999999999999999"),
                             Decimal("99999999999999999999999999999999999999"), 2, 3),
            std::partial_ordering::less);

  // min negative unscaled value
  // -10^38 + 1 scale cause overflow
  ASSERT_EQ(Decimal::Compare(Decimal("-99999999999999999999999999999999999999"),
                             Decimal("-99999999999999999999999999999999999999"), 2, 3),
            std::partial_ordering::less);
  // -10^37 + 1 scale no overflow
  ASSERT_EQ(Decimal::Compare(Decimal("-9999999999999999999999999999999999999"),
                             Decimal("-99999999999999999999999999999999999999"), 2, 3),
            std::partial_ordering::greater);

  // equal values with different scales
  ASSERT_EQ(Decimal::Compare(Decimal("123456789"), Decimal("1234567890"), 2, 3),
            std::partial_ordering::equivalent);
  ASSERT_EQ(Decimal::Compare(Decimal("-1234567890"), Decimal("-123456789"), 3, 2),
            std::partial_ordering::equivalent);

  // different values with different scales
  ASSERT_EQ(Decimal::Compare(Decimal("123456788"), Decimal("1234567890"), 2, 3),
            std::partial_ordering::less);
  ASSERT_EQ(Decimal::Compare(Decimal("-1234567890"), Decimal("-123456788"), 2, 3),
            std::partial_ordering::less);

  // different values with same scales
  ASSERT_EQ(Decimal::Compare(Decimal("123456790"), Decimal("123456789"), 2, 2),
            std::partial_ordering::greater);
  ASSERT_EQ(Decimal::Compare(Decimal("-123456790"), Decimal("-123456789"), 2, 2),
            std::partial_ordering::less);

  // different signs
  ASSERT_EQ(Decimal::Compare(Decimal("123456789"), Decimal("-123456789"), 2, 3),
            std::partial_ordering::greater);
  ASSERT_EQ(Decimal::Compare(Decimal("-123456789"), Decimal("123456789"), 2, 3),
            std::partial_ordering::less);

  // zero comparisons
  ASSERT_EQ(Decimal::Compare(Decimal("0"), Decimal("0"), 2, 3),
            std::partial_ordering::equivalent);
  ASSERT_EQ(Decimal::Compare(Decimal("0"), Decimal("123456789"), 2, 3),
            std::partial_ordering::less);
  ASSERT_EQ(Decimal::Compare(Decimal("-123456789"), Decimal("0"), 2, 3),
            std::partial_ordering::less);
}

}  // namespace iceberg
