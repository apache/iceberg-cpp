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

#include "iceberg/util/transform_util.h"

#include <gtest/gtest.h>

#include "iceberg/test/matchers.h"

namespace iceberg {

TEST(TransformUtilTest, HumanYear) {
  EXPECT_EQ("1970", TransformUtil::HumanYear(0));
  EXPECT_EQ("1971", TransformUtil::HumanYear(1));
  EXPECT_EQ("1969", TransformUtil::HumanYear(-1));
  EXPECT_EQ("0999", TransformUtil::HumanYear(999 - 1970));
  EXPECT_EQ("2026", TransformUtil::HumanYear(56));
}

TEST(TransformUtilTest, HumanMonth) {
  // 0 is January 1970
  EXPECT_EQ("1970-01", TransformUtil::HumanMonth(0));
  // 1 is Febrary 1970
  EXPECT_EQ("1970-02", TransformUtil::HumanMonth(1));
  // -1 is December 1969
  EXPECT_EQ("1969-12", TransformUtil::HumanMonth(-1));
  // 0999-12
  EXPECT_EQ("0999-12", TransformUtil::HumanMonth(-11641));
  // 12 is January 1971
  EXPECT_EQ("1971-01", TransformUtil::HumanMonth(12));
  // 672 is December 2026-01
  EXPECT_EQ("2026-01", TransformUtil::HumanMonth(672));
}

TEST(TransformUtilTest, HumanDay) {
  // 0 is Unix epoch (1970-01-01)
  EXPECT_EQ("1970-01-01", TransformUtil::HumanDay(0));
  // 1 is 1970-01-02
  EXPECT_EQ("1970-01-02", TransformUtil::HumanDay(1));
  // -1 is 1969-12-31
  EXPECT_EQ("1969-12-31", TransformUtil::HumanDay(-1));
  // 0999-12-31
  EXPECT_EQ("0999-12-31", TransformUtil::HumanDay(-354286));
  // 365 is 1971-01-01 (non-leap year)
  EXPECT_EQ("1971-01-01", TransformUtil::HumanDay(365));
  // 20454 is 2026-01-01
  EXPECT_EQ("2026-01-01", TransformUtil::HumanDay(20454));
}

TEST(TransformUtilTest, HumanHour) {
  // 0 is Unix epoch at 00:00
  EXPECT_EQ("1970-01-01-00", TransformUtil::HumanHour(0));
  // 1 is first hour of epoch
  EXPECT_EQ("1970-01-01-01", TransformUtil::HumanHour(1));
  // -1 is previous day's last hour
  EXPECT_EQ("1969-12-31-23", TransformUtil::HumanHour(-1));
  // 999-12-31 at 23:00
  EXPECT_EQ("0999-12-31-23", TransformUtil::HumanHour(-8502841));
  // 24 is next day at 00:00
  EXPECT_EQ("1970-01-02-00", TransformUtil::HumanHour(24));
  // 490896 is 2026-01-01 at 00:00
  EXPECT_EQ("2026-01-01-00", TransformUtil::HumanHour(490896));
}

TEST(TransformUtilTest, HumanTime) {
  // Midnight
  EXPECT_EQ("00:00", TransformUtil::HumanTime(0));
  // 1 second after midnight
  EXPECT_EQ("00:00:01", TransformUtil::HumanTime(1000000));
  // 1.5 seconds after midnight
  EXPECT_EQ("00:00:01.500", TransformUtil::HumanTime(1500000));
  // 1.001 seconds after midnight
  EXPECT_EQ("00:00:01.001", TransformUtil::HumanTime(1001000));
  // 1.000001 seconds after midnight
  EXPECT_EQ("00:00:01.000001", TransformUtil::HumanTime(1000001));
  // 1 hour, 2 minutes, 3 seconds
  EXPECT_EQ("01:02:03", TransformUtil::HumanTime(3723000000));
  // 23:59:59
  EXPECT_EQ("23:59:59", TransformUtil::HumanTime(86399000000));
}

TEST(TransformUtilTest, HumanTimestamp) {
  // Unix epoch
  EXPECT_EQ("1970-01-01T00:00:00", TransformUtil::HumanTimestamp(0));
  // 1 second after epoch
  EXPECT_EQ("1970-01-01T00:00:01", TransformUtil::HumanTimestamp(1000000));
  // 1 second before epoch
  EXPECT_EQ("1969-12-31T23:59:59", TransformUtil::HumanTimestamp(-1000000));
  // 0999-12-31T23:59:59
  EXPECT_EQ("0999-12-31T23:59:59", TransformUtil::HumanTimestamp(-30610224001000000L));
  // precistion with 500 milliseconds
  EXPECT_EQ("2026-01-01T00:00:01.500", TransformUtil::HumanTimestamp(1767225601500000L));
  // precision with 1 millisecond
  EXPECT_EQ("2026-01-01T00:00:01.001", TransformUtil::HumanTimestamp(1767225601001000L));
  // precision with 1 microsecond
  EXPECT_EQ("2026-01-01T00:00:01.000001",
            TransformUtil::HumanTimestamp(1767225601000001L));
}

TEST(TransformUtilTest, HumanTimestampWithZone) {
  // Unix epoch
  EXPECT_EQ("1970-01-01T00:00:00+00:00", TransformUtil::HumanTimestampWithZone(0));
  // 1 second after epoch
  EXPECT_EQ("1970-01-01T00:00:01+00:00", TransformUtil::HumanTimestampWithZone(1000000));
  // 1 second before epoch
  EXPECT_EQ("1969-12-31T23:59:59+00:00", TransformUtil::HumanTimestampWithZone(-1000000));
  // 0999-12-31T23:59:59
  EXPECT_EQ("0999-12-31T23:59:59+00:00",
            TransformUtil::HumanTimestampWithZone(-30610224001000000L));
  // precistion with 500 milliseconds
  EXPECT_EQ("2026-01-01T00:00:01.500+00:00",
            TransformUtil::HumanTimestampWithZone(1767225601500000L));
  // precision with 1 millisecond
  EXPECT_EQ("2026-01-01T00:00:01.001+00:00",
            TransformUtil::HumanTimestampWithZone(1767225601001000L));
  // precision with 1 microsecond
  EXPECT_EQ("2026-01-01T00:00:01.000001+00:00",
            TransformUtil::HumanTimestampWithZone(1767225601000001L));
}

TEST(TransformUtilTest, Base64Encode) {
  // Empty string
  EXPECT_EQ("", TransformUtil::Base64Encode(""));

  // Single character
  EXPECT_EQ("YQ==", TransformUtil::Base64Encode("a"));
  EXPECT_EQ("YWI=", TransformUtil::Base64Encode("ab"));
  EXPECT_EQ("YWJj", TransformUtil::Base64Encode("abc"));

  // Multiple of 3 characters
  EXPECT_EQ("YWJjZGU=", TransformUtil::Base64Encode("abcde"));
  EXPECT_EQ("YWJjZGVm", TransformUtil::Base64Encode("abcdef"));

  // Common strings
  EXPECT_EQ("U29tZSBkYXRhIHdpdGggY2hhcmFjdGVycw==",
            TransformUtil::Base64Encode("Some data with characters"));
  EXPECT_EQ("aGVsbG8=", TransformUtil::Base64Encode("hello"));
  EXPECT_EQ("dGVzdCBzdHJpbmc=", TransformUtil::Base64Encode("test string"));

  // Unicode
  EXPECT_EQ("8J+EgA==", TransformUtil::Base64Encode("\xF0\x9F\x84\x80"));
  // Null byte
  EXPECT_EQ("AA==", TransformUtil::Base64Encode({"\x00", 1}));
}

struct ParseRoundTripParam {
  std::string name;
  std::string str;
  int64_t value;
  enum Kind { kDay, kTime, kTimestamp, kTimestampTz } kind;
};

class ParseRoundTripTest : public ::testing::TestWithParam<ParseRoundTripParam> {};

TEST_P(ParseRoundTripTest, RoundTrip) {
  const auto& param = GetParam();
  switch (param.kind) {
    case ParseRoundTripParam::kDay: {
      EXPECT_EQ(TransformUtil::HumanDay(static_cast<int32_t>(param.value)), param.str);
      ICEBERG_UNWRAP_OR_FAIL(auto parsed, TransformUtil::ParseDay(param.str));
      EXPECT_EQ(parsed, static_cast<int32_t>(param.value));
      break;
    }
    case ParseRoundTripParam::kTime: {
      EXPECT_EQ(TransformUtil::HumanTime(param.value), param.str);
      ICEBERG_UNWRAP_OR_FAIL(auto parsed, TransformUtil::ParseTime(param.str));
      EXPECT_EQ(parsed, param.value);
      break;
    }
    case ParseRoundTripParam::kTimestamp: {
      EXPECT_EQ(TransformUtil::HumanTimestamp(param.value), param.str);
      ICEBERG_UNWRAP_OR_FAIL(auto parsed, TransformUtil::ParseTimestamp(param.str));
      EXPECT_EQ(parsed, param.value);
      break;
    }
    case ParseRoundTripParam::kTimestampTz: {
      EXPECT_EQ(TransformUtil::HumanTimestampWithZone(param.value), param.str);
      ICEBERG_UNWRAP_OR_FAIL(auto parsed,
                             TransformUtil::ParseTimestampWithZone(param.str));
      EXPECT_EQ(parsed, param.value);
      break;
    }
  }
}

INSTANTIATE_TEST_SUITE_P(
    TransformUtilTest, ParseRoundTripTest,
    ::testing::Values(
        // Day round-trips
        ParseRoundTripParam{"DayEpoch", "1970-01-01", 0, ParseRoundTripParam::kDay},
        ParseRoundTripParam{"DayNext", "1970-01-02", 1, ParseRoundTripParam::kDay},
        ParseRoundTripParam{"DayBeforeEpoch", "1969-12-31", -1,
                            ParseRoundTripParam::kDay},
        ParseRoundTripParam{"DayYear999", "0999-12-31", -354286,
                            ParseRoundTripParam::kDay},
        ParseRoundTripParam{"DayNonLeap", "1971-01-01", 365, ParseRoundTripParam::kDay},
        ParseRoundTripParam{"DayY2K", "2000-01-01", 10957, ParseRoundTripParam::kDay},
        ParseRoundTripParam{"Day2026", "2026-01-01", 20454, ParseRoundTripParam::kDay},
        // Time round-trips
        ParseRoundTripParam{"TimeMidnight", "00:00", 0, ParseRoundTripParam::kTime},
        ParseRoundTripParam{"TimeOneSec", "00:00:01", 1000000,
                            ParseRoundTripParam::kTime},
        ParseRoundTripParam{"TimeMillis", "00:00:01.500", 1500000,
                            ParseRoundTripParam::kTime},
        ParseRoundTripParam{"TimeOneMillis", "00:00:01.001", 1001000,
                            ParseRoundTripParam::kTime},
        ParseRoundTripParam{"TimeMicros", "00:00:01.000001", 1000001,
                            ParseRoundTripParam::kTime},
        ParseRoundTripParam{"TimeHourMinSec", "01:02:03", 3723000000,
                            ParseRoundTripParam::kTime},
        ParseRoundTripParam{"TimeEndOfDay", "23:59:59", 86399000000,
                            ParseRoundTripParam::kTime},
        // Timestamp round-trips
        ParseRoundTripParam{"TimestampEpoch", "1970-01-01T00:00:00", 0,
                            ParseRoundTripParam::kTimestamp},
        ParseRoundTripParam{"TimestampOneSec", "1970-01-01T00:00:01", 1000000,
                            ParseRoundTripParam::kTimestamp},
        ParseRoundTripParam{"TimestampMillis", "2026-01-01T00:00:01.500",
                            1767225601500000L, ParseRoundTripParam::kTimestamp},
        ParseRoundTripParam{"TimestampOneMillis", "2026-01-01T00:00:01.001",
                            1767225601001000L, ParseRoundTripParam::kTimestamp},
        ParseRoundTripParam{"TimestampMicros", "2026-01-01T00:00:01.000001",
                            1767225601000001L, ParseRoundTripParam::kTimestamp},
        // TimestampTz round-trips
        ParseRoundTripParam{"TimestampTzEpoch", "1970-01-01T00:00:00+00:00", 0,
                            ParseRoundTripParam::kTimestampTz},
        ParseRoundTripParam{"TimestampTzOneSec", "1970-01-01T00:00:01+00:00", 1000000,
                            ParseRoundTripParam::kTimestampTz},
        ParseRoundTripParam{"TimestampTzMillis", "2026-01-01T00:00:01.500+00:00",
                            1767225601500000L, ParseRoundTripParam::kTimestampTz},
        ParseRoundTripParam{"TimestampTzOneMillis", "2026-01-01T00:00:01.001+00:00",
                            1767225601001000L, ParseRoundTripParam::kTimestampTz},
        ParseRoundTripParam{"TimestampTzMicros", "2026-01-01T00:00:01.000001+00:00",
                            1767225601000001L, ParseRoundTripParam::kTimestampTz}),
    [](const ::testing::TestParamInfo<ParseRoundTripParam>& info) {
      return info.param.name;
    });

}  // namespace iceberg
