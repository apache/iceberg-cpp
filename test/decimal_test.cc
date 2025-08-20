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

#include <array>
#include <cstdint>

#include <gtest/gtest.h>
#include <sys/types.h>

#include "gmock/gmock.h"
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
      ASSERT_THAT(result, IsError(ErrorKind::kOverflow));
    }
  }
};

using RealTypes = ::testing::Types<float, double>;
TYPED_TEST_SUITE(TestDecimalFromReal, RealTypes);

TYPED_TEST(TestDecimalFromReal, TestSuccess) { this->TestSuccess(); }

TYPED_TEST(TestDecimalFromReal, TestErrors) { this->TestErrors(); }

}  // namespace iceberg
