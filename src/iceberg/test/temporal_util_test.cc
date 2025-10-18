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

#include <gtest/gtest.h>

namespace iceberg {

TEST(TemporalUtilTest, CreateDate) {
  EXPECT_EQ(TemporalUtils::CreateDate({.year = 1970, .month = 1, .day = 1}), 0);
  EXPECT_EQ(TemporalUtils::CreateDate({.year = 1970, .month = 1, .day = 2}), 1);
  EXPECT_EQ(TemporalUtils::CreateDate({.year = 1969, .month = 12, .day = 31}), -1);
  EXPECT_EQ(TemporalUtils::CreateDate({.year = 2000, .month = 1, .day = 1}), 10957);
  EXPECT_EQ(TemporalUtils::CreateDate({.year = 2017, .month = 11, .day = 16}), 17486);
  EXPECT_EQ(TemporalUtils::CreateDate({.year = 2052, .month = 2, .day = 20}), 30000);
}

TEST(TemporalUtilTest, CreateTime) {
  EXPECT_EQ(TemporalUtils::CreateTime({.hour = 0, .minute = 0, .second = 0}), 0);
  EXPECT_EQ(TemporalUtils::CreateTime({.hour = 1, .minute = 0, .second = 0}),
            3600000000LL);
  EXPECT_EQ(TemporalUtils::CreateTime({.hour = 0, .minute = 1, .second = 0}), 60000000LL);
  EXPECT_EQ(TemporalUtils::CreateTime({.hour = 0, .minute = 0, .second = 1}), 1000000LL);
  EXPECT_EQ(TemporalUtils::CreateTime({.hour = 22, .minute = 31, .second = 8}),
            81068000000LL);
  EXPECT_EQ(TemporalUtils::CreateTime({.hour = 23, .minute = 59, .second = 59}),
            86399000000LL);
  EXPECT_EQ(
      TemporalUtils::CreateTime({.hour = 0, .minute = 0, .second = 0, .microsecond = 1}),
      1LL);
  EXPECT_EQ(TemporalUtils::CreateTime(
                {.hour = 0, .minute = 0, .second = 0, .microsecond = 999999}),
            999999LL);
  EXPECT_EQ(
      TemporalUtils::CreateTime({.hour = 0, .minute = 0, .second = 1, .microsecond = 1}),
      1000001LL);
  EXPECT_EQ(TemporalUtils::CreateTime(
                {.hour = 23, .minute = 59, .second = 59, .microsecond = 999999}),
            86399999999LL);
}

TEST(TemporalUtilTest, CreateTimestamp) {
  EXPECT_EQ(
      TemporalUtils::CreateTimestamp(
          {.year = 1970, .month = 1, .day = 1, .hour = 0, .minute = 0, .second = 0}),
      0LL);
  EXPECT_EQ(
      TemporalUtils::CreateTimestamp(
          {.year = 1970, .month = 1, .day = 2, .hour = 0, .minute = 0, .second = 0}),
      86400000000LL);
  EXPECT_EQ(
      TemporalUtils::CreateTimestamp(
          {.year = 1969, .month = 12, .day = 31, .hour = 23, .minute = 59, .second = 59}),
      -1000000LL);
  EXPECT_EQ(
      TemporalUtils::CreateTimestamp(
          {.year = 2000, .month = 1, .day = 1, .hour = 0, .minute = 0, .second = 0}),
      946684800000000LL);
  EXPECT_EQ(
      TemporalUtils::CreateTimestamp(
          {.year = 2017, .month = 11, .day = 16, .hour = 22, .minute = 31, .second = 8}),
      1510871468000000LL);
  EXPECT_EQ(TemporalUtils::CreateTimestamp({.year = 2017,
                                            .month = 11,
                                            .day = 16,
                                            .hour = 22,
                                            .minute = 31,
                                            .second = 8,
                                            .microsecond = 1}),
            1510871468000001LL);
  EXPECT_EQ(TemporalUtils::CreateTimestamp({.year = 2023,
                                            .month = 10,
                                            .day = 5,
                                            .hour = 15,
                                            .minute = 45,
                                            .second = 30,
                                            .microsecond = 123456}),
            1696520730123456LL);
}

TEST(TemporalUtilTest, CreateTimestampTz) {
  EXPECT_EQ(TemporalUtils::CreateTimestampTz({.year = 2017,
                                              .month = 11,
                                              .day = 16,
                                              .hour = 14,
                                              .minute = 31,
                                              .second = 8,
                                              .tz_offset_minutes = -480}),
            1510871468000000LL);
  EXPECT_EQ(TemporalUtils::CreateTimestampTz({.year = 2017,
                                              .month = 11,
                                              .day = 16,
                                              .hour = 14,
                                              .minute = 31,
                                              .second = 8,
                                              .microsecond = 1,
                                              .tz_offset_minutes = -480}),
            1510871468000001LL);
  EXPECT_EQ(TemporalUtils::CreateTimestampTz({.year = 2023,
                                              .month = 10,
                                              .day = 5,
                                              .hour = 15,
                                              .minute = 45,
                                              .second = 30,
                                              .microsecond = 123456,
                                              .tz_offset_minutes = 60}),
            1696517130123456LL);
  EXPECT_EQ(TemporalUtils::CreateTimestampTz({.year = 2023,
                                              .month = 10,
                                              .day = 5,
                                              .hour = 15,
                                              .minute = 45,
                                              .second = 30,
                                              .microsecond = 123456,
                                              .tz_offset_minutes = -60}),
            1696524330123456LL);
}

TEST(TemporalUtilTest, CreateTimestampNanos) {
  EXPECT_EQ(
      TemporalUtils::CreateTimestampNanos(
          {.year = 2017, .month = 11, .day = 16, .hour = 22, .minute = 31, .second = 8}),
      1510871468000000000LL);
  EXPECT_EQ(TemporalUtils::CreateTimestampNanos({.year = 2017,
                                                 .month = 11,
                                                 .day = 16,
                                                 .hour = 22,
                                                 .minute = 31,
                                                 .second = 8,
                                                 .nanosecond = 1}),
            1510871468000000001LL);
}

TEST(TemporalUtilTest, CreateTimestampTzNanos) {
  EXPECT_EQ(TemporalUtils::CreateTimestampTzNanos({.year = 2017,
                                                   .month = 11,
                                                   .day = 16,
                                                   .hour = 14,
                                                   .minute = 31,
                                                   .second = 8,
                                                   .tz_offset_minutes = -480}),
            1510871468000000000LL);
  EXPECT_EQ(TemporalUtils::CreateTimestampTzNanos({.year = 2017,
                                                   .month = 11,
                                                   .day = 16,
                                                   .hour = 14,
                                                   .minute = 31,
                                                   .second = 8,
                                                   .nanosecond = 1,
                                                   .tz_offset_minutes = -480}),
            1510871468000000001LL);
}

}  // namespace iceberg
