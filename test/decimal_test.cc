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
#include "iceberg/expression/decimal.h"

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <sys/types.h>

#include "gmock/gmock.h"
#include "iceberg/util/port.h"
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

Decimal DecimalFromInt128(int128_t value) {
  return {static_cast<int64_t>(value >> 64),
          static_cast<uint64_t>(value & 0xFFFFFFFFFFFFFFFFULL)};
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
  ASSERT_THAT(result, HasErrorMessage(
                          "Decimal::FromString: empty string is not a valid Decimal"));
  for (const auto& invalid_string :
       std::vector<std::string>{"-", "0.0.0", "0-13-32", "a", "-23092.235-",
                                "-+23092.235", "+-23092.235", "00a", "1e1a", "0.00123D/3",
                                "1.23eA8", "1.23E+3A", "-1.23E--5", "1.2345E+++07"}) {
    auto result = Decimal::FromString(invalid_string);
    ASSERT_THAT(result, IsError(ErrorKind::kInvalidArgument));
    ASSERT_THAT(result, HasErrorMessage("Decimal::FromString: invalid decimal string"));
  }

  for (const auto& invalid_string :
       std::vector<std::string>{"1e39", "-1e39", "9e39", "-9e39", "9.9e40", "-9.9e40"}) {
    auto result = Decimal::FromString(invalid_string);
    ASSERT_THAT(result, IsError(ErrorKind::kInvalidArgument));
    ASSERT_THAT(result,
                HasErrorMessage("Decimal::FromString: scale must be in the range"));
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

template <typename Real>
struct FromRealTestParam {
  Real real;
  int32_t precision;
  int32_t scale;
  const char* expected_string;
};

template <typename Real>
void CheckDecimalFromReal(Real real, int32_t precision, int32_t scale,
                          const char* expected_string) {
  auto result = Decimal::FromReal(real, precision, scale);
  ASSERT_THAT(result, IsOk()) << "Failed to convert real to Decimal: " << real
                              << ", precision: " << precision << ", scale: " << scale;
  const Decimal& decimal = result.value();
  EXPECT_EQ(decimal.ToString(scale).value(), expected_string);
  const std::string expected_neg =
      (decimal) ? "-" + std::string(expected_string) : expected_string;
  auto neg_result = Decimal::FromReal(-real, precision, scale);
  ASSERT_THAT(neg_result, IsOk())
      << "Failed to convert negative real to Decimal: " << -real
      << ", precision: " << precision << ", scale: " << scale;
  const Decimal& neg_decimal = neg_result.value();
  EXPECT_EQ(neg_decimal.ToString(scale).value(), expected_neg);
}

template <typename Real>
void CheckDecimalFromRealIntegerString(Real real, int32_t precision, int32_t scale,
                                       const char* expected_string) {
  auto result = Decimal::FromReal(real, precision, scale);
  ASSERT_THAT(result, IsOk()) << "Failed to convert real to Decimal: " << real
                              << ", precision: " << precision << ", scale: " << scale;
  const Decimal& decimal = result.value();
  EXPECT_EQ(decimal.ToIntegerString(), expected_string);
  const std::string expected_neg =
      (decimal) ? "-" + std::string(expected_string) : expected_string;
  auto neg_result = Decimal::FromReal(-real, precision, scale);
  ASSERT_THAT(neg_result, IsOk())
      << "Failed to convert negative real to Decimal: " << -real
      << ", precision: " << precision << ", scale: " << scale;
  const Decimal& neg_decimal = neg_result.value();
  EXPECT_EQ(neg_decimal.ToIntegerString(), expected_neg);
}

using FromFloatTestParam = FromRealTestParam<float>;
using FromDoubleTestParam = FromRealTestParam<double>;

template <typename Real>
class TestDecimalFromReal : public ::testing::Test {
 public:
  using ParamType = FromRealTestParam<Real>;

  void TestSuccess() {
    const std::vector<ParamType> params{
        // clang-format off
        {0.0f, 1, 0, "0"},
        {0.0f, 19, 4, "0.0000"},
        {123.0f, 7, 4, "123.0000"},
        {456.78f, 7, 4, "456.7800"},
        {456.784f, 5, 2, "456.78"},
        {456.786f, 5, 2, "456.79"},
        {999.99f, 5, 2, "999.99"},
        {123.0f, 19, 0, "123"},
        {123.4f, 19, 0, "123"},
        {123.6f, 19, 0, "124"},
        // 2**62
        {4.6116860184273879e+18, 19, 0, "4611686018427387904"},
        // 2**63
        {9.2233720368547758e+18, 19, 0, "9223372036854775808"},
        // 2**64
        {1.8446744073709552e+19, 20, 0, "18446744073709551616"},
        // clang-format on
    };

    for (const ParamType& param : params) {
      CheckDecimalFromReal(param.real, param.precision, param.scale,
                           param.expected_string);
    }
  }

  void TestErrors() {
    const std::vector<ParamType> params{
        {std::numeric_limits<Real>::infinity(), Decimal::kMaxPrecision / 2, 4, ""},
        {-std::numeric_limits<Real>::infinity(), Decimal::kMaxPrecision / 2, 4, ""},
        {std::numeric_limits<Real>::quiet_NaN(), Decimal::kMaxPrecision / 2, 4, ""},
        {-std::numeric_limits<Real>::quiet_NaN(), Decimal::kMaxPrecision / 2, 4, ""},
    };

    for (const ParamType& param : params) {
      auto result = Decimal::FromReal(param.real, param.precision, param.scale);
      ASSERT_THAT(result, IsError(ErrorKind::kInvalidArgument));
    }

    const std::vector<ParamType> overflow_params{
        // Overflow errors
        {1000.0, 3, 0, ""},  {-1000.0, 3, 0, ""}, {1000.0, 5, 2, ""},
        {-1000.0, 5, 2, ""}, {999.996, 5, 2, ""}, {-999.996, 5, 2, ""},
    };
    for (const ParamType& param : overflow_params) {
      auto result = Decimal::FromReal(param.real, param.precision, param.scale);
      ASSERT_THAT(result, IsError(ErrorKind::kInvalid));
    }
  }
};

using RealTypes = ::testing::Types<float, double>;
TYPED_TEST_SUITE(TestDecimalFromReal, RealTypes);

TYPED_TEST(TestDecimalFromReal, TestSuccess) { this->TestSuccess(); }

TYPED_TEST(TestDecimalFromReal, TestErrors) { this->TestErrors(); }

TEST(TestDecimalFromReal, FromFloat) {
  const std::vector<FromFloatTestParam> params{
      // -- Stress the 24 bits of precision of a float
      // 2**63 + 2**40
      FromFloatTestParam{.real = 9.223373e+18f,
                         .precision = 19,
                         .scale = 0,
                         .expected_string = "9223373136366403584"},
      // 2**64 - 2**40
      FromFloatTestParam{.real = 1.8446743e+19f,
                         .precision = 20,
                         .scale = 0,
                         .expected_string = "18446742974197923840"},
      // 2**64 + 2**41
      FromFloatTestParam{.real = 1.8446746e+19f,
                         .precision = 20,
                         .scale = 0,
                         .expected_string = "18446746272732807168"},
      // 2**14 - 2**-10
      FromFloatTestParam{
          .real = 16383.999f, .precision = 8, .scale = 3, .expected_string = "16383.999"},
      FromFloatTestParam{16383.999f, .precision = 19, .scale = 3,
                         .expected_string = "16383.999"},
      // 1 - 2**-24
      FromFloatTestParam{.real = 0.99999994f,
                         .precision = 10,
                         .scale = 10,
                         .expected_string = "0.9999999404"},
      FromFloatTestParam{.real = 0.99999994f,
                         .precision = 15,
                         .scale = 15,
                         .expected_string = "0.999999940395355"},
      FromFloatTestParam{.real = 0.99999994f,
                         .precision = 20,
                         .scale = 20,
                         .expected_string = "0.99999994039535522461"},
      FromFloatTestParam{.real = 0.99999994f,
                         .precision = 21,
                         .scale = 21,
                         .expected_string = "0.999999940395355224609"},
      FromFloatTestParam{.real = 0.99999994f,
                         .precision = 38,
                         .scale = 38,
                         .expected_string = "0.99999994039535522460937500000000000000"},
      // -- Other cases
      // 10**38 - 2**103
      FromFloatTestParam{.real = 9.999999e+37f,
                         .precision = 38,
                         .scale = 0,
                         .expected_string = "99999986661652122824821048795547566080"},
  };

  for (const auto& param : params) {
    CheckDecimalFromReal(param.real, param.precision, param.scale, param.expected_string);
  }
}

TEST(TestDecimalFromReal, FromFloatLargeValues) {
  for (int32_t scale = -38; scale <= 38; ++scale) {
    float real = std::pow(10.0f, static_cast<float>(scale));
    CheckDecimalFromRealIntegerString(real, 1, -scale, "1");
  }

  for (int32_t scale = -37; scale <= 36; ++scale) {
    float real = 123.f * std::pow(10.f, static_cast<float>(scale));
    CheckDecimalFromRealIntegerString(real, 2, -scale - 1, "12");
    CheckDecimalFromRealIntegerString(real, 3, -scale, "123");
    CheckDecimalFromRealIntegerString(real, 4, -scale + 1, "1230");
  }
}

TEST(TestDecimalFromReal, FromDouble) {
  const std::vector<FromDoubleTestParam> params{
      // -- Stress the 53 bits of precision of a double
      // 2**63 + 2**11
      FromDoubleTestParam{.real = 9.223372036854778e+18,
                          .precision = 19,
                          .scale = 0,
                          .expected_string = "9223372036854777856"},
      // 2**64 - 2**11
      FromDoubleTestParam{.real = 1.844674407370955e+19,
                          .precision = 20,
                          .scale = 0,
                          .expected_string = "18446744073709549568"},
      // 2**64 + 2**11
      FromDoubleTestParam{.real = 1.8446744073709556e+19,
                          .precision = 20,
                          .scale = 0,
                          .expected_string = "18446744073709555712"},
      // 2**126
      FromDoubleTestParam{.real = 8.507059173023462e+37,
                          .precision = 38,
                          .scale = 0,
                          .expected_string = "85070591730234615865843651857942052864"},
      // 2**126 - 2**74
      FromDoubleTestParam{.real = 8.50705917302346e+37,
                          .precision = 38,
                          .scale = 0,
                          .expected_string = "85070591730234596976377720379361198080"},
      // 2**36 - 2**-16
      FromDoubleTestParam{.real = 68719476735.999985,
                          .precision = 11,
                          .scale = 0,
                          .expected_string = "68719476736"},
      FromDoubleTestParam{.real = 68719476735.999985,
                          .precision = 38,
                          .scale = 27,
                          .expected_string = "68719476735.999984741210937500000000000"},
      // -- Other cases
      // Almost 10**38 (minus 2**73)
      FromDoubleTestParam{.real = 9.999999999999998e+37,
                          .precision = 38,
                          .scale = 0,
                          .expected_string = "99999999999999978859343891977453174784"},
      FromDoubleTestParam{.real = 9.999999999999998e+27,
                          .precision = 38,
                          .scale = 10,
                          .expected_string = "9999999999999997384096481280.0000000000"},
      // 10**N (sometimes fits in N digits)
      FromDoubleTestParam{.real = 1e23,
                          .precision = 23,
                          .scale = 0,
                          .expected_string = "99999999999999991611392"},
      FromDoubleTestParam{.real = 1e23,
                          .precision = 24,
                          .scale = 1,
                          .expected_string = "99999999999999991611392.0"},
      FromDoubleTestParam{.real = 1e36,
                          .precision = 37,
                          .scale = 0,
                          .expected_string = "1000000000000000042420637374017961984"},
      FromDoubleTestParam{.real = 1e36,
                          .precision = 38,
                          .scale = 1,
                          .expected_string = "1000000000000000042420637374017961984.0"},
      FromDoubleTestParam{.real = 1e37,
                          .precision = 37,
                          .scale = 0,
                          .expected_string = "9999999999999999538762658202121142272"},
      FromDoubleTestParam{.real = 1e37,
                          .precision = 38,
                          .scale = 1,
                          .expected_string = "9999999999999999538762658202121142272.0"},
      FromDoubleTestParam{.real = 1e38,
                          .precision = 38,
                          .scale = 0,
                          .expected_string = "99999999999999997748809823456034029568"},
      // Hand-picked test cases that can involve precision issues.
      // More comprehensive testing is done in the PyArrow test suite.
      FromDoubleTestParam{.real = 9.223372036854778e+10,
                          .precision = 19,
                          .scale = 8,
                          .expected_string = "92233720368.54777527"},
      FromDoubleTestParam{.real = 1.8446744073709556e+15,
                          .precision = 20,
                          .scale = 4,
                          .expected_string = "1844674407370955.5000"},
      FromDoubleTestParam{.real = 999999999999999.0,
                          .precision = 16,
                          .scale = 1,
                          .expected_string = "999999999999999.0"},
      FromDoubleTestParam{.real = 9999999999999998.0,
                          .precision = 17,
                          .scale = 1,
                          .expected_string = "9999999999999998.0"},
      FromDoubleTestParam{.real = 999999999999999.9,
                          .precision = 16,
                          .scale = 1,
                          .expected_string = "999999999999999.9"},
      FromDoubleTestParam{.real = 9999999987.,
                          .precision = 38,
                          .scale = 22,
                          .expected_string = "9999999987.0000000000000000000000"},
      FromDoubleTestParam{.real = 9999999987.,
                          .precision = 38,
                          .scale = 28,
                          .expected_string = "9999999987.0000000000000000000000000000"},
      // 1 - 2**-52
      // XXX the result should be 0.99999999999999977795539507496869191527
      // but our algorithm loses the right digit.
      FromDoubleTestParam{.real = 0.9999999999999998,
                          .precision = 38,
                          .scale = 38,
                          .expected_string = "0.99999999999999977795539507496869191520"},
      FromDoubleTestParam{.real = 0.9999999999999998,
                          .precision = 20,
                          .scale = 20,
                          .expected_string = "0.99999999999999977796"},
      FromDoubleTestParam{.real = 0.9999999999999998,
                          .precision = 16,
                          .scale = 16,
                          .expected_string = "0.9999999999999998"},
  };

  for (const auto& param : params) {
    CheckDecimalFromReal(param.real, param.precision, param.scale, param.expected_string);
  }
}

TEST(TestDecimalFromReal, FromDoubleLargeValues) {
  constexpr auto kMaxScale = Decimal::kMaxScale;
  for (int32_t scale = -kMaxScale; scale <= kMaxScale; ++scale) {
    if (std::abs(1 - scale) < kMaxScale) {
      double real = std::pow(10.0, static_cast<double>(scale));
      CheckDecimalFromRealIntegerString(real, 1, -scale, "1");
    }
  }

  for (int32_t scale = -kMaxScale + 1; scale <= kMaxScale - 1; ++scale) {
    if (std::abs(4 - scale) < kMaxScale) {
      double real = 123. * std::pow(10.0, static_cast<double>(scale));
      CheckDecimalFromRealIntegerString(real, 2, -scale - 1, "12");
      CheckDecimalFromRealIntegerString(real, 3, -scale, "123");
      CheckDecimalFromRealIntegerString(real, 4, -scale + 1, "1230");
    }
  }
}

template <typename Real>
struct ToRealTestParam {
  std::string decimal_value;
  int32_t scale;
  Real expected;
};

using ToFloatTestParam = ToRealTestParam<float>;
using ToDoubleTestParam = ToRealTestParam<double>;

template <typename Real>
void CheckDecimalToReal(const std::string& decimal_value, int32_t scale, Real expected) {
  auto result = Decimal::FromString(decimal_value);
  ASSERT_THAT(result, IsOk()) << "Failed to convert string to Decimal: " << decimal_value;

  const Decimal& decimal = result.value();
  auto real_result = decimal.ToReal<Real>(scale);

  EXPECT_EQ(real_result, expected)
      << "Expected: " << expected << ", but got: " << real_result;
}

template <typename Real>
void CheckDecimalToRealWithinOneULP(const std::string& decimal_value, int32_t scale,
                                    Real expected) {
  Decimal dec(decimal_value);
  Real actual = dec.template ToReal<Real>(scale);
  ASSERT_TRUE(actual == expected || actual == std::nextafter(expected, expected + 1) ||
              actual == std::nextafter(expected, expected - 1))
      << "Decimal value: " << decimal_value << ", scale: " << scale
      << ", expected: " << expected << ", actual: " << actual;
}

template <typename Real>
void CheckDecimalToRealWithinEpsilon(const std::string& decimal_value, int32_t scale,
                                     Real epsilon, Real expected) {
  Decimal dec(decimal_value);
  Real actual = dec.template ToReal<Real>(scale);
  ASSERT_LE(std::abs(actual - expected), epsilon)
      << "Decimal value: " << decimal_value << ", scale: " << scale
      << ", expected: " << expected << ", actual: " << actual;
}

void CheckDecimalToRealApprox(const std::string& decimal_value, int32_t scale,
                              float expected) {
  Decimal dec(decimal_value);
  auto actual = dec.template ToReal<float>(scale);
  ASSERT_FLOAT_EQ(actual, expected)
      << "Decimal value: " << decimal_value << ", scale: " << scale
      << ", expected: " << expected << ", actual: " << actual;
}

void CheckDecimalToRealApprox(const std::string& decimal_value, int32_t scale,
                              double expected) {
  Decimal dec(decimal_value);
  auto actual = dec.template ToReal<double>(scale);
  ASSERT_DOUBLE_EQ(actual, expected)
      << "Decimal value: " << decimal_value << ", scale: " << scale
      << ", expected: " << expected << ", actual: " << actual;
}

template <typename Real>
class TestDecimalToReal : public ::testing::Test {
 public:
  using ParamType = ToRealTestParam<Real>;

  Real Pow2(int exp) { return std::pow(static_cast<Real>(2), static_cast<Real>(exp)); }

  Real Pow10(int exp) { return std::pow(static_cast<Real>(10), static_cast<Real>(exp)); }

  void TestSuccess() {
    const std::vector<ParamType> params{
        // clang-format off
        {"0", 0, 0.0f},
        {"0", 10, 0.0f},
        {"0", -10, 0.0f},
        {"1", 0, 1.0f},
        {"12345", 0, 12345.f},
#ifndef __MINGW32__  // MinGW has precision issues
        {"12345", 1, 1234.5f},
#endif
        {"12345", -3, 12345000.f},
        // 2**62
        {"4611686018427387904", 0, Pow2(62)},
        // 2**63 + 2**62
        {"13835058055282163712", 0, Pow2(63) + Pow2(62)},
        // 2**64 + 2**62
        {"23058430092136939520", 0, Pow2(64) + Pow2(62)},
        // 10**38 - 2**103
#ifndef __MINGW32__  // MinGW has precision issues
        {"99999989858795198174164788026374356992", 0, Pow10(38) - Pow2(103)},
#endif
        // clang-format on
    };

    for (const ParamType& param : params) {
      CheckDecimalToReal(param.decimal_value, param.scale, param.expected);
      if (param.decimal_value != "0") {
        // Check negative values as well
        CheckDecimalToReal("-" + param.decimal_value, param.scale, -param.expected);
      }
    }
  }
};

TYPED_TEST_SUITE(TestDecimalToReal, RealTypes);

TYPED_TEST(TestDecimalToReal, TestSuccess) { this->TestSuccess(); }

// Custom test for Decimal::ToReal<float>
TEST(TestDecimalToReal, ToFloatLargeValues) {
  constexpr auto max_scale = Decimal::kMaxScale;

  // Note that exact comparisons would succeed on some platforms (Linux, macOS).
  // Nevertheless, power-of-ten factors are not all exactly representable
  // in binary floating point.
  for (int32_t scale = -max_scale; scale <= max_scale; scale++) {
#ifdef _WIN32
    // MSVC gives pow(10.f, -45.f) == 0 even though 1e-45f is nonzero
    if (scale == 45) continue;
#endif
    CheckDecimalToRealApprox(
        "1", scale, std::pow(static_cast<float>(10), static_cast<float>(-scale)));
  }
  for (int32_t scale = -max_scale; scale <= max_scale - 2; scale++) {
#ifdef _WIN32
    // MSVC gives pow(10.f, -45.f) == 0 even though 1e-45f is nonzero
    if (scale == 45) continue;
#endif
    const auto factor = static_cast<float>(123);
    CheckDecimalToRealApprox(
        "123", scale,
        factor * std::pow(static_cast<float>(10), static_cast<float>(-scale)));
  }
}

TEST(TestDecimalToReal, ToFloatPrecision) {
  // 2**63 + 2**40 (exactly representable in a float's 24 bits of precision)
  CheckDecimalToReal<float>("9223373136366403584", 0, 9.223373e+18f);
  CheckDecimalToReal<float>("-9223373136366403584", 0, -9.223373e+18f);

  // 2**64 + 2**41 (exactly representable in a float)
  CheckDecimalToReal<float>("18446746272732807168", 0, 1.8446746e+19f);
  CheckDecimalToReal<float>("-18446746272732807168", 0, -1.8446746e+19f);

  // Integers are always exact
  auto scale = Decimal::kMaxScale - 1;
  std::string seven = "7.";
  seven.append(scale, '0');  // pad with trailing zeros
  CheckDecimalToReal<float>(seven, scale, 7.0f);
  CheckDecimalToReal<float>("-" + seven, scale, -7.0f);

  CheckDecimalToReal<float>("99999999999999999999.0000000000000000", 16,
                            99999999999999999999.0f);
  CheckDecimalToReal<float>("-99999999999999999999.0000000000000000", 16,
                            -99999999999999999999.0f);

  // Small fractions are within one ULP
  CheckDecimalToRealWithinOneULP<float>("9999999.9", 1, 9999999.9f);
  CheckDecimalToRealWithinOneULP<float>("-9999999.9", 1, -9999999.9f);

  CheckDecimalToRealWithinOneULP<float>("9999999.999999", 6, 9999999.999999f);
  CheckDecimalToRealWithinOneULP<float>("-9999999.999999", 6, -9999999.999999f);

  // Large fractions are within 2^-23
  constexpr float epsilon = 1.1920928955078125e-07f;  // 2^-23
  CheckDecimalToRealWithinEpsilon<float>("112334829348925.99070703983306884765625", 23,
                                         epsilon,
                                         112334829348925.99070703983306884765625f);
  CheckDecimalToRealWithinEpsilon<float>("1.987748987892758765582589910934859345", 36,
                                         epsilon,
                                         1.987748987892758765582589910934859345f);
}

// ToReal<double> tests are disabled on MinGW because of precision issues in results
#ifndef __MINGW32__

TEST(TestDecimalToReal, ToDoubleLargeValues) {
  // Note that exact comparisons would succeed on some platforms (Linux, macOS).
  // Nevertheless, power-of-ten factors are not all exactly representable
  // in binary floating point.
  constexpr auto max_scale = Decimal::kMaxScale;

  for (int32_t scale = -max_scale; scale <= max_scale; scale++) {
    CheckDecimalToRealApprox(
        "1", scale, std::pow(static_cast<double>(10), static_cast<double>(-scale)));
  }
  for (int32_t scale = -max_scale + 1; scale <= max_scale - 1; scale++) {
    const auto factor = static_cast<double>(123);
    CheckDecimalToRealApprox(
        "123", scale,
        factor * std::pow(static_cast<double>(10), static_cast<double>(-scale)));
  }
}

TEST(TestDecimalToReal, ToDoublePrecision) {
  // 2**63 + 2**11 (exactly representable in a double's 53 bits of precision)
  CheckDecimalToReal<double>("9223372036854777856", 0, 9.223372036854778e+18);
  CheckDecimalToReal<double>("-9223372036854777856", 0, -9.223372036854778e+18);
  // 2**64 - 2**11 (exactly representable in a double)
  CheckDecimalToReal<double>("18446744073709549568", 0, 1.844674407370955e+19);
  CheckDecimalToReal<double>("-18446744073709549568", 0, -1.844674407370955e+19);
  // 2**64 + 2**11 (exactly representable in a double)
  CheckDecimalToReal<double>("18446744073709555712", 0, 1.8446744073709556e+19);
  CheckDecimalToReal<double>("-18446744073709555712", 0, -1.8446744073709556e+19);

  // Almost 10**38 (minus 2**73)
  CheckDecimalToReal<double>("99999999999999978859343891977453174784", 0,
                             9.999999999999998e+37);
  CheckDecimalToReal<double>("-99999999999999978859343891977453174784", 0,
                             -9.999999999999998e+37);
  CheckDecimalToReal<double>("99999999999999978859343891977453174784", 10,
                             9.999999999999998e+27);
  CheckDecimalToReal<double>("-99999999999999978859343891977453174784", 10,
                             -9.999999999999998e+27);
  CheckDecimalToReal<double>("99999999999999978859343891977453174784", -10,
                             9.999999999999998e+47);
  CheckDecimalToReal<double>("-99999999999999978859343891977453174784", -10,
                             -9.999999999999998e+47);

  // Integers are always exact
  auto scale = Decimal::kMaxScale - 1;
  std::string seven = "7.";
  seven.append(scale, '0');
  CheckDecimalToReal<double>(seven, scale, 7.0);
  CheckDecimalToReal<double>("-" + seven, scale, -7.0);

  CheckDecimalToReal<double>("99999999999999999999.0000000000000000", 16,
                             99999999999999999999.0);
  CheckDecimalToReal<double>("-99999999999999999999.0000000000000000", 16,
                             -99999999999999999999.0);

  // Small fractions are within one ULP
  CheckDecimalToRealWithinOneULP<double>("9999999.9", 1, 9999999.9);
  CheckDecimalToRealWithinOneULP<double>("-9999999.9", 1, -9999999.9);

  CheckDecimalToRealWithinOneULP<double>("9999999.999999999999999", 15,
                                         9999999.999999999999999);
  CheckDecimalToRealWithinOneULP<double>("-9999999.999999999999999", 15,
                                         -9999999.999999999999999);
  // Large fractions are within 2^-52
  constexpr double epsilon = 2.220446049250313080847263336181640625e-16;  // 2^-52
  CheckDecimalToRealWithinEpsilon<double>("112334829348925.99070703983306884765625", 23,
                                          epsilon,
                                          112334829348925.99070703983306884765625);
  CheckDecimalToRealWithinEpsilon<double>("1.987748987892758765582589910934859345", 36,
                                          epsilon,
                                          1.987748987892758765582589910934859345);
}

#endif  // __MINGW32__

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
#if ICEBERG_LITTLE_ENDIAN
      std::ranges::reverse(native_endian);
#endif
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

#if ICEBERG_LITTLE_ENDIAN
      // convert to big endian
      std::ranges::reverse(native_endian);
#endif
      result = Decimal::FromBigEndian(native_endian.data() + WidthMinusOne - ii, ii + 1);
      ASSERT_THAT(result, IsOk());
      const Decimal& negated_decimal = result.value();
      EXPECT_EQ(negated_decimal, negated);

      // Take the complement
      auto complement = ~value;
      native_endian = complement.ToBytes();

#if ICEBERG_LITTLE_ENDIAN
      // convert to big endian
      std::ranges::reverse(native_endian);
#endif
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

TEST(DecimalTestFunctionality, Multiply) {
  ASSERT_EQ(Decimal(60501), Decimal(301) * Decimal(201));
  ASSERT_EQ(Decimal(-60501), Decimal(-301) * Decimal(201));
  ASSERT_EQ(Decimal(-60501), Decimal(301) * Decimal(-201));
  ASSERT_EQ(Decimal(60501), Decimal(-301) * Decimal(-201));

  // Edge cases
  for (auto x : std::vector<int128_t>{-INT64_MAX, -INT32_MAX, 0, INT32_MAX, INT64_MAX}) {
    for (auto y :
         std::vector<int128_t>{-INT32_MAX, -32, -2, -1, 0, 1, 2, 32, INT32_MAX}) {
      Decimal decimal_x = DecimalFromInt128(x);
      Decimal decimal_y = DecimalFromInt128(y);
      Decimal result = decimal_x * decimal_y;
      EXPECT_EQ(DecimalFromInt128(x * y), result)
          << " x: " << decimal_x << " y: " << decimal_y;
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
      Decimal decimal_x = DecimalFromInt128(x);
      Decimal decimal_y = DecimalFromInt128(y);
      Decimal result = decimal_x / decimal_y;
      EXPECT_EQ(DecimalFromInt128(x / y), result)
          << " x: " << decimal_x << " y: " << decimal_y;
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
      Decimal decimal_x = DecimalFromInt128(x);
      Decimal decimal_y = DecimalFromInt128(y);
      Decimal result = decimal_x % decimal_y;
      EXPECT_EQ(DecimalFromInt128(x % y), result)
          << " x: " << decimal_x << " y: " << decimal_y;
    }
  }
}

TEST(DecimalTestFunctionality, Sign) {
  ASSERT_EQ(1, Decimal(999999).Sign());
  ASSERT_EQ(-1, Decimal(-999999).Sign());
  ASSERT_EQ(1, Decimal(0).Sign());
}

TEST(DecimalTestFunctionality, GetWholeAndFraction) {
  Decimal value("123456");

  auto check = [value](int32_t scale, std::pair<int32_t, int32_t> expected) {
    int32_t out;
    auto result = value.GetWholeAndFraction(scale);
    ASSERT_THAT(result->first.ToInteger(&out), IsOk());
    ASSERT_EQ(expected.first, out);
    ASSERT_THAT(result->second.ToInteger(&out), IsOk());
    ASSERT_EQ(expected.second, out);
  };

  check(0, {123456, 0});
  check(1, {12345, 6});
  check(5, {1, 23456});
  check(7, {0, 123456});
}

TEST(DecimalTestFunctionality, GetWholeAndFractionNegative) {
  Decimal value("-123456");

  auto check = [value](int32_t scale, std::pair<int32_t, int32_t> expected) {
    int32_t out;
    auto result = value.GetWholeAndFraction(scale);
    ASSERT_THAT(result->first.ToInteger(&out), IsOk());
    ASSERT_EQ(expected.first, out);
    ASSERT_THAT(result->second.ToInteger(&out), IsOk());
    ASSERT_EQ(expected.second, out);
  };

  check(0, {-123456, 0});
  check(1, {-12345, -6});
  check(5, {-1, -23456});
  check(7, {0, -123456});
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
    auto expected = DecimalFromInt128(x << bits);
    auto actual = DecimalFromInt128(x) << bits;
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

}  // namespace iceberg
