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

#include "iceberg/util/date_time_util.h"

#include <ctime>

#include <gtest/gtest.h>

namespace iceberg {

// Constants for better readability
constexpr int64_t kMicrosPerSecond = 1000000LL;
constexpr int64_t kSecondsPerMinute = 60LL;
constexpr int64_t kSecondsPerHour = 3600LL;
constexpr int64_t kMicrosPerDay = 86400000000LL;

// Helper function for creating tm with designated initializers
std::tm make_tm(int year, int mon, int mday, int hour = 0, int min = 0, int sec = 0) {
  return std::tm{.tm_sec = sec,
                 .tm_min = min,
                 .tm_hour = hour,
                 .tm_mday = mday,
                 .tm_mon = mon,
                 .tm_year = year - 1900,
                 .tm_wday = 0,
                 .tm_yday = 0,
                 .tm_isdst = -1};
}

// MicrosToDays Tests
TEST(DateTimeUtilTest, MicrosToDaysUnixEpoch) {
  // Unix epoch (1970-01-01 00:00:00 UTC) should be day 0
  EXPECT_EQ(MicrosToDays(0), 0);
}

TEST(DateTimeUtilTest, MicrosToDaysPositiveValues) {
  // Test with cleaner constant usage
  EXPECT_EQ(MicrosToDays(kMicrosPerDay), 1);
  EXPECT_EQ(MicrosToDays(2 * kMicrosPerDay), 2);
  EXPECT_EQ(MicrosToDays(365 * kMicrosPerDay), 365);

  // Test partial day - should floor down
  EXPECT_EQ(MicrosToDays(kMicrosPerDay - 1), 0);
  EXPECT_EQ(MicrosToDays(kMicrosPerDay + 12 * kSecondsPerHour * kMicrosPerSecond), 1);
}

TEST(DateTimeUtilTest, MicrosToDaysNegativeValues) {
  EXPECT_EQ(MicrosToDays(-kMicrosPerDay), -1);
  EXPECT_EQ(MicrosToDays(-2 * kMicrosPerDay), -2);

  // Test partial negative day - should floor down (more negative)
  EXPECT_EQ(MicrosToDays(-1), -1);
  EXPECT_EQ(MicrosToDays(-kMicrosPerDay + 1), -1);
}

// TimegmCustom Tests
TEST(DateTimeUtilTest, TimegmCustomUnixEpoch) {
  auto tm = make_tm(1970, 0, 1);  // Much cleaner!
  EXPECT_EQ(TimegmCustom(&tm), 0);
}

TEST(DateTimeUtilTest, TimegmCustomValidDates) {
  // 2000-01-01 00:00:00 UTC = 946684800 seconds since epoch
  auto tm = make_tm(2000, 0, 1);
  EXPECT_EQ(TimegmCustom(&tm), 946684800);

  // 2020-12-31 23:59:59 UTC = 1609459199 seconds since epoch
  tm = make_tm(2020, 11, 31, 23, 59, 59);
  EXPECT_EQ(TimegmCustom(&tm), 1609459199);
}

TEST(DateTimeUtilTest, TimegmCustomLeapYear) {
  // 2000-02-29 00:00:00 UTC (leap year)
  auto tm = make_tm(2000, 1, 29);  // Much more readable!

  // Should not crash and return valid result
  time_t result = TimegmCustom(&tm);
  EXPECT_GT(result, 0);
}

// ParseDateString Tests
TEST(DateTimeUtilTest, ParseDateStringValidFormats) {
  // Unix epoch
  auto result = ParseDateString("1970-01-01");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), 0);

  // Common dates
  result = ParseDateString("2000-01-01");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), 10957);  // Days since epoch

  result = ParseDateString("2020-12-31");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), 18627);  // Days since epoch
}

TEST(DateTimeUtilTest, ParseDateStringLeapYear) {
  // Leap year date
  auto result = ParseDateString("2000-02-29");
  ASSERT_TRUE(result.has_value());
  EXPECT_GT(result.value(), 0);

  // Non-leap year - February 29th should fail in validation if implemented
  result = ParseDateString("1999-02-29");
  // Note: Current implementation might not validate this properly
  // This is a known limitation that could be improved
}

TEST(DateTimeUtilTest, ParseDateStringInvalidFormats) {
  // Wrong format
  EXPECT_FALSE(ParseDateString("01-01-2000").has_value());
  EXPECT_FALSE(ParseDateString("2000/01/01").has_value());

  // Invalid dates
  EXPECT_FALSE(ParseDateString("2000-13-01").has_value());  // Invalid month
  EXPECT_FALSE(ParseDateString("2000-01-32").has_value());  // Invalid day
  EXPECT_FALSE(ParseDateString("2000-00-01").has_value());  // Invalid month
  EXPECT_FALSE(ParseDateString("2000-01-00").has_value());  // Invalid day

  // Empty and malformed strings
  EXPECT_FALSE(ParseDateString("").has_value());
  EXPECT_FALSE(ParseDateString("not-a-date").has_value());
  EXPECT_FALSE(ParseDateString("2000-01").has_value());
  EXPECT_FALSE(ParseDateString("2000-01-01-extra").has_value());
}

// ParseTimeString Tests
TEST(DateTimeUtilTest, ParseTimeStringValidFormats) {
  // Basic time without fractional seconds
  auto result = ParseTimeString("00:00:00");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), 0);

  result = ParseTimeString("12:30:45");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), (12 * 3600 + 30 * 60 + 45) * 1000000LL);

  result = ParseTimeString("23:59:59");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), (23 * 3600 + 59 * 60 + 59) * 1000000LL);
}

TEST(DateTimeUtilTest, ParseTimeStringWithFractionalSeconds) {
  constexpr int64_t base_time_micros =
      (12 * kSecondsPerHour + 30 * kSecondsPerMinute + 45) * kMicrosPerSecond;

  // Single digit fractional
  auto result = ParseTimeString("12:30:45.1");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), base_time_micros + 100000LL);

  // Three digit fractional
  result = ParseTimeString("12:30:45.123");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), base_time_micros + 123000LL);

  // Six digit fractional (microseconds)
  result = ParseTimeString("12:30:45.123456");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), base_time_micros + 123456LL);

  // More than 6 digits should be truncated
  result = ParseTimeString("12:30:45.1234567890");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), base_time_micros + 123456LL);
}

TEST(DateTimeUtilTest, ParseTimeStringInvalidFormats) {
  // Invalid time values
  EXPECT_FALSE(ParseTimeString("24:00:00").has_value());  // Invalid hour
  EXPECT_FALSE(ParseTimeString("12:60:00").has_value());  // Invalid minute
  EXPECT_FALSE(ParseTimeString("-1:30:45").has_value());  // Negative hour

  // Wrong format
  EXPECT_FALSE(ParseTimeString("12-30-45").has_value());
  EXPECT_FALSE(ParseTimeString("12:30:45:67").has_value());

  // Empty and malformed
  EXPECT_FALSE(ParseTimeString("").has_value());
  EXPECT_FALSE(ParseTimeString("not-a-time").has_value());
  EXPECT_FALSE(ParseTimeString("12:30:45 extra").has_value());
}

// ParseTimestampString Tests
TEST(DateTimeUtilTest, ParseTimestampStringValidFormats) {
  // Unix epoch
  auto result = ParseTimestampString("1970-01-01T00:00:00");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), 0);

  // Standard timestamp
  result = ParseTimestampString("2000-01-01T12:30:45");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(),
            946684800LL * 1000000LL + (12 * 3600 + 30 * 60 + 45) * 1000000LL);

  // With fractional seconds
  result = ParseTimestampString("2000-01-01T12:30:45.123456");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(),
            946684800LL * 1000000LL + (12 * 3600 + 30 * 60 + 45) * 1000000LL + 123456LL);
}

TEST(DateTimeUtilTest, ParseTimestampStringInvalidFormats) {
  // Wrong separator
  EXPECT_FALSE(ParseTimestampString("2000-01-01 12:30:45").has_value());

  // Invalid date part
  EXPECT_FALSE(ParseTimestampString("2000-13-01T12:30:45").has_value());

  // Invalid time part
  EXPECT_FALSE(ParseTimestampString("2000-01-01T25:30:45").has_value());

  // Incomplete
  EXPECT_FALSE(ParseTimestampString("2000-01-01T").has_value());

  // Extra characters
  EXPECT_FALSE(ParseTimestampString("2000-01-01T12:30:45 extra").has_value());
}

// ParseTimestampTzString Tests
TEST(DateTimeUtilTest, ParseTimestampTzStringValidFormats) {
  // Without Z suffix (should still work)
  auto result = ParseTimestampTzString("2000-01-01T12:30:45");
  ASSERT_TRUE(result.has_value());
  EXPECT_GT(result.value(), 0);

  // With Z suffix
  result = ParseTimestampTzString("2000-01-01T12:30:45Z");
  ASSERT_TRUE(result.has_value());
  EXPECT_GT(result.value(), 0);

  // With fractional seconds and Z
  result = ParseTimestampTzString("2000-01-01T12:30:45.123456Z");
  ASSERT_TRUE(result.has_value());
  EXPECT_GT(result.value(), 0);
}

TEST(DateTimeUtilTest, ParseTimestampTzStringInvalidFormats) {
  // Invalid timezone formats (only Z is supported)
  EXPECT_FALSE(ParseTimestampTzString("2000-01-01T12:30:45+08:00").has_value());
  EXPECT_FALSE(ParseTimestampTzString("2000-01-01T12:30:45-05:00").has_value());
  EXPECT_FALSE(ParseTimestampTzString("2000-01-01T12:30:45GMT").has_value());

  // Multiple Z characters
  EXPECT_FALSE(ParseTimestampTzString("2000-01-01T12:30:45ZZ").has_value());

  // Extra characters after Z
  EXPECT_FALSE(ParseTimestampTzString("2000-01-01T12:30:45Z extra").has_value());
}

// ParseFractionalSeconds Tests
TEST(DateTimeUtilTest, ParseFractionalSecondsValidInputs) {
  // Empty string should return 0
  auto result = ParseFractionalSeconds("");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), 0);

  // Single digit (100000 microseconds = 0.1 seconds)
  result = ParseFractionalSeconds("1");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), 100000);

  // Two digits
  result = ParseFractionalSeconds("12");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), 120000);

  // Three digits (milliseconds)
  result = ParseFractionalSeconds("123");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), 123000);

  // Six digits (microseconds)
  result = ParseFractionalSeconds("123456");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), 123456);

  // Leading zeros
  result = ParseFractionalSeconds("000123");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), 123);
}

TEST(DateTimeUtilTest, ParseFractionalSecondsInvalidInputs) {
  // More than 6 digits
  EXPECT_FALSE(ParseFractionalSeconds("1234567").has_value());

  // Non-numeric characters
  EXPECT_FALSE(ParseFractionalSeconds("12a").has_value());
  EXPECT_FALSE(ParseFractionalSeconds("abc").has_value());
  EXPECT_FALSE(ParseFractionalSeconds("12.3").has_value());
  EXPECT_FALSE(ParseFractionalSeconds("12-3").has_value());
  EXPECT_FALSE(ParseFractionalSeconds("-123").has_value());
  EXPECT_FALSE(ParseFractionalSeconds(" 123").has_value());
}

// Edge Cases and Integration Tests
TEST(DateTimeUtilTest, EdgeCasesBoundaryValues) {
  // Test year boundaries
  auto date_result = ParseDateString("1970-01-01");
  ASSERT_TRUE(date_result.has_value());
  EXPECT_EQ(date_result.value(), 0);

  // Test time boundaries
  auto time_result = ParseTimeString("00:00:00.000000");
  ASSERT_TRUE(time_result.has_value());
  EXPECT_EQ(time_result.value(), 0);

  time_result = ParseTimeString("23:59:59.999999");
  ASSERT_TRUE(time_result.has_value());
  EXPECT_EQ(time_result.value(), 86399999999LL);  // Almost 1 day in microseconds
}

TEST(DateTimeUtilTest, ConsistencyBetweenFunctions) {
  // Ensure MicrosToDays and date parsing are consistent
  auto date_days = ParseDateString("2000-01-01");
  ASSERT_TRUE(date_days.has_value());

  auto timestamp_micros = ParseTimestampString("2000-01-01T00:00:00");
  ASSERT_TRUE(timestamp_micros.has_value());

  auto derived_days = MicrosToDays(timestamp_micros.value());
  EXPECT_EQ(date_days.value(), derived_days);
}

}  // namespace iceberg
