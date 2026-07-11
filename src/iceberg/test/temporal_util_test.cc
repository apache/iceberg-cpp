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

#include "iceberg/util/temporal_util.h"

#include <limits>
#include <string>

#include <gtest/gtest.h>

#include "iceberg/test/matchers.h"

namespace iceberg {

TEST(TemporalUtilTest, ParseTimestampNs) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto nanos, TemporalUtils::ParseTimestampNs("2026-01-01T00:00:01.000001001"));
  EXPECT_EQ(nanos, 1767225601000001001L);

  ICEBERG_UNWRAP_OR_FAIL(auto pre_epoch_nanos, TemporalUtils::ParseTimestampNs(
                                                   "1969-12-31T23:59:59.123456789"));
  EXPECT_EQ(pre_epoch_nanos, -876543211);
}

TEST(TemporalUtilTest, ParseTimestampNsChecksInt64Bounds) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto max_nanos, TemporalUtils::ParseTimestampNs("2262-04-11T23:47:16.854775807"));
  EXPECT_EQ(max_nanos, std::numeric_limits<int64_t>::max());

  ICEBERG_UNWRAP_OR_FAIL(
      auto min_nanos, TemporalUtils::ParseTimestampNs("1677-09-21T00:12:43.145224192"));
  EXPECT_EQ(min_nanos, std::numeric_limits<int64_t>::min());

  EXPECT_THAT(TemporalUtils::ParseTimestampNs("2262-04-11T23:47:16.854775808"),
              IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(TemporalUtils::ParseTimestampNs("1677-09-21T00:12:43.145224191"),
              IsError(ErrorKind::kInvalidArgument));
}

TEST(TemporalUtilTest, IsUtcOffset) {
  // UTC offsets: "Z", "+00:00" and "-00:00".
  ICEBERG_UNWRAP_OR_FAIL(auto z, TemporalUtils::IsUtcOffset("2024-06-27T00:00:00Z"));
  EXPECT_TRUE(z);
  ICEBERG_UNWRAP_OR_FAIL(auto plus_zero,
                         TemporalUtils::IsUtcOffset("2024-06-27T00:00:00+00:00"));
  EXPECT_TRUE(plus_zero);
  ICEBERG_UNWRAP_OR_FAIL(auto minus_zero,
                         TemporalUtils::IsUtcOffset("2024-06-27T00:00:00-00:00"));
  EXPECT_TRUE(minus_zero);

  // Non-UTC offsets.
  ICEBERG_UNWRAP_OR_FAIL(auto plus_five,
                         TemporalUtils::IsUtcOffset("2024-06-27T05:00:00+05:00"));
  EXPECT_FALSE(plus_five);
  ICEBERG_UNWRAP_OR_FAIL(auto minus_eight,
                         TemporalUtils::IsUtcOffset("2024-06-27T00:00:00-08:00"));
  EXPECT_FALSE(minus_eight);

  // A missing or unparseable timezone suffix is an error.
  EXPECT_THAT(TemporalUtils::IsUtcOffset("2024-06-27T00:00:00"),
              IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(TemporalUtils::IsUtcOffset(""), IsError(ErrorKind::kInvalidArgument));
}

TEST(TemporalUtilTest, ParseTimestampNsRejectsMoreThanNineFractionalDigits) {
  EXPECT_THAT(TemporalUtils::ParseTimestampNs("2026-01-01T00:00:01.0000010011"),
              IsError(ErrorKind::kInvalidArgument));
}

TEST(TemporalUtilTest, ParseTimestampNsWithZone) {
  ICEBERG_UNWRAP_OR_FAIL(auto nanos, TemporalUtils::ParseTimestampNsWithZone(
                                         "2026-01-01T00:00:01.000001001+00:00"));
  EXPECT_EQ(nanos, 1767225601000001001L);
}

TEST(TemporalUtilTest, ParseTimestampNsWithZoneChecksInt64BoundsAfterOffset) {
  ICEBERG_UNWRAP_OR_FAIL(auto max_nanos, TemporalUtils::ParseTimestampNsWithZone(
                                             "2262-04-12T00:47:16.854775807+01:00"));
  EXPECT_EQ(max_nanos, std::numeric_limits<int64_t>::max());

  ICEBERG_UNWRAP_OR_FAIL(auto min_nanos, TemporalUtils::ParseTimestampNsWithZone(
                                             "1677-09-20T23:12:43.145224192-01:00"));
  EXPECT_EQ(min_nanos, std::numeric_limits<int64_t>::min());

  EXPECT_THAT(
      TemporalUtils::ParseTimestampNsWithZone("2262-04-11T23:47:16.854775807-00:01"),
      IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(
      TemporalUtils::ParseTimestampNsWithZone("1677-09-21T00:12:43.145224192+00:01"),
      IsError(ErrorKind::kInvalidArgument));
}

TEST(TemporalUtilTest, ParseTimestampNsWithZoneRejectsOffsetPastPlusMinus1800) {
  EXPECT_THAT(
      TemporalUtils::ParseTimestampNsWithZone("2026-01-01T00:00:01.000001001+18:01"),
      IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(
      TemporalUtils::ParseTimestampNsWithZone("2026-01-01T00:00:01.000001001-18:30"),
      IsError(ErrorKind::kInvalidArgument));
}

struct ParseParam {
  std::string name;
  std::string str;
  int64_t value;
  enum Kind { kDay, kTime, kTimestamp, kTimestampTz } kind;
};

class TemporalParseTest : public ::testing::TestWithParam<ParseParam> {};

TEST_P(TemporalParseTest, ParsesCorrectly) {
  const auto& param = GetParam();
  switch (param.kind) {
    case ParseParam::kDay: {
      ICEBERG_UNWRAP_OR_FAIL(auto parsed, TemporalUtils::ParseDay(param.str));
      EXPECT_EQ(parsed, static_cast<int32_t>(param.value));
      break;
    }
    case ParseParam::kTime: {
      ICEBERG_UNWRAP_OR_FAIL(auto parsed, TemporalUtils::ParseTime(param.str));
      EXPECT_EQ(parsed, param.value);
      break;
    }
    case ParseParam::kTimestamp: {
      ICEBERG_UNWRAP_OR_FAIL(auto parsed, TemporalUtils::ParseTimestamp(param.str));
      EXPECT_EQ(parsed, param.value);
      break;
    }
    case ParseParam::kTimestampTz: {
      ICEBERG_UNWRAP_OR_FAIL(auto parsed,
                             TemporalUtils::ParseTimestampWithZone(param.str));
      EXPECT_EQ(parsed, param.value);
      break;
    }
  }
}

struct ParseTimeErrorParam {
  std::string name;
  std::string str;
};

class ParseTimeErrorTest : public ::testing::TestWithParam<ParseTimeErrorParam> {};

TEST_P(ParseTimeErrorTest, ReturnsError) {
  EXPECT_THAT(TemporalUtils::ParseTime(GetParam().str),
              IsError(ErrorKind::kInvalidArgument));
}

INSTANTIATE_TEST_SUITE_P(
    TemporalUtilTest, TemporalParseTest,
    ::testing::Values(
        ParseParam{"DayEpoch", "1970-01-01", 0, ParseParam::kDay},
        ParseParam{"DayNext", "1970-01-02", 1, ParseParam::kDay},
        ParseParam{"DayBeforeEpoch", "1969-12-31", -1, ParseParam::kDay},
        ParseParam{"DayYear999", "0999-12-31", -354286, ParseParam::kDay},
        ParseParam{"DayNonLeap", "1971-01-01", 365, ParseParam::kDay},
        ParseParam{"DayY2K", "2000-01-01", 10957, ParseParam::kDay},
        ParseParam{"Day2026", "2026-01-01", 20454, ParseParam::kDay},
        ParseParam{"TimeMidnight", "00:00", 0, ParseParam::kTime},
        ParseParam{"TimeOneSec", "00:00:01", 1000000, ParseParam::kTime},
        ParseParam{"TimeMillis", "00:00:01.500", 1500000, ParseParam::kTime},
        ParseParam{"TimeOneMillis", "00:00:01.001", 1001000, ParseParam::kTime},
        ParseParam{"TimeMicros", "00:00:01.000001", 1000001, ParseParam::kTime},
        ParseParam{"TimeHourMinSec", "01:02:03", 3723000000, ParseParam::kTime},
        ParseParam{"TimeEndOfDay", "23:59:59", 86399000000, ParseParam::kTime},
        ParseParam{"TimestampEpoch", "1970-01-01T00:00:00", 0, ParseParam::kTimestamp},
        ParseParam{"TimestampOneSec", "1970-01-01T00:00:01", 1000000,
                   ParseParam::kTimestamp},
        ParseParam{"TimestampMillis", "2026-01-01T00:00:01.500", 1767225601500000L,
                   ParseParam::kTimestamp},
        ParseParam{"TimestampOneMillis", "2026-01-01T00:00:01.001", 1767225601001000L,
                   ParseParam::kTimestamp},
        ParseParam{"TimestampMicros", "2026-01-01T00:00:01.000001", 1767225601000001L,
                   ParseParam::kTimestamp},
        ParseParam{"TimestampTzEpoch", "1970-01-01T00:00:00+00:00", 0,
                   ParseParam::kTimestampTz},
        ParseParam{"TimestampTzOneSec", "1970-01-01T00:00:01+00:00", 1000000,
                   ParseParam::kTimestampTz},
        ParseParam{"TimestampTzMillis", "2026-01-01T00:00:01.500+00:00",
                   1767225601500000L, ParseParam::kTimestampTz},
        ParseParam{"TimestampTzOneMillis", "2026-01-01T00:00:01.001+00:00",
                   1767225601001000L, ParseParam::kTimestampTz},
        ParseParam{"TimestampTzMicros", "2026-01-01T00:00:01.000001+00:00",
                   1767225601000001L, ParseParam::kTimestampTz},
        ParseParam{"TimestampTzSuffixZ_Epoch", "1970-01-01T00:00:00Z", 0,
                   ParseParam::kTimestampTz},
        ParseParam{"TimestampTzSuffixZ_Millis", "2026-01-01T00:00:01.500Z",
                   1767225601500000L, ParseParam::kTimestampTz},
        ParseParam{"TimestampTzNegZero_Epoch", "1970-01-01T00:00:00-00:00", 0,
                   ParseParam::kTimestampTz},
        ParseParam{"TimestampTzNegZero_Millis", "2026-01-01T00:00:01.500-00:00",
                   1767225601500000L, ParseParam::kTimestampTz},
        ParseParam{"TimeTruncatesNanos", "00:00:01.123456789", 1123456,
                   ParseParam::kTime},
        ParseParam{"1Digit", "00:00:01.5", 1500000, ParseParam::kTime},
        ParseParam{"2Digits", "00:00:01.50", 1500000, ParseParam::kTime},
        ParseParam{"2DigitsNonZero", "00:00:01.12", 1120000, ParseParam::kTime},
        ParseParam{"4Digits", "00:00:01.0001", 1000100, ParseParam::kTime},
        ParseParam{"TimestampNoSec_Zero", "1970-01-01T00:00", 0, ParseParam::kTimestamp},
        ParseParam{"TimestampNoSec_OneMin", "1970-01-01T00:01", 60000000,
                   ParseParam::kTimestamp},
        ParseParam{"TimestampTzNoSec_Offset", "1970-01-01T00:00+00:00", 0,
                   ParseParam::kTimestampTz},
        ParseParam{"TimestampTzNoSec_OneMin", "1970-01-01T00:01+00:00", 60000000,
                   ParseParam::kTimestampTz},
        ParseParam{"TimestampTzNoSec_Z", "1970-01-01T00:00Z", 0,
                   ParseParam::kTimestampTz},
        ParseParam{"ExtendedYearPlusEpoch", "+1970-01-01", 0, ParseParam::kDay},
        ParseParam{"ExtendedYearPlus2026", "+2026-01-01", 20454, ParseParam::kDay},
        ParseParam{"ExtendedYearMinus2026", "-2026-01-01", -1459509, ParseParam::kDay},
        ParseParam{"TimestampTzPositiveOffset", "1970-01-01T05:00:00+05:00", 0,
                   ParseParam::kTimestampTz},
        ParseParam{"TimestampTzNegativeOffset", "1970-01-01T00:00:00-05:00", 18000000000,
                   ParseParam::kTimestampTz},
        ParseParam{"TimestampTzOffsetWithMillis", "2026-01-01T05:30:01.500+05:30",
                   1767225601500000L, ParseParam::kTimestampTz},
        ParseParam{"TimestampTzNegOffsetToEpoch", "1969-12-31T19:00:00-05:00", 0,
                   ParseParam::kTimestampTz},
        ParseParam{"TimestampTzNoSecWithOffset", "1970-01-01T05:30+05:30", 0,
                   ParseParam::kTimestampTz}),
    [](const ::testing::TestParamInfo<ParseParam>& info) { return info.param.name; });

INSTANTIATE_TEST_SUITE_P(
    TemporalUtilTest, ParseTimeErrorTest,
    ::testing::Values(ParseTimeErrorParam{"EmptyString", ""},
                      ParseTimeErrorParam{"TooShort1Char", "1"},
                      ParseTimeErrorParam{"TooShort2Chars", "12"},
                      ParseTimeErrorParam{"TooShort4Chars", "12:3"},
                      ParseTimeErrorParam{"MissingColon", "1200:00"},
                      ParseTimeErrorParam{"OutofRangeHours", "24:00:00"},
                      ParseTimeErrorParam{"OutofRangeMinutes", "12:60:00"},
                      ParseTimeErrorParam{"OutofRangeSeconds", "12:30:61"},
                      ParseTimeErrorParam{"SpaceInsteadOfColon", "12 30"}),
    [](const ::testing::TestParamInfo<ParseTimeErrorParam>& info) {
      return info.param.name;
    });

}  // namespace iceberg
