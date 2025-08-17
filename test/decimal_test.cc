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

#include <gtest/gtest.h>

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

}  // namespace iceberg
