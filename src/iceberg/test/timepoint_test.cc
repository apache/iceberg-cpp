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

#include "iceberg/util/timepoint.h"

#include <chrono>

#include <gtest/gtest.h>

#include "iceberg/util/timepoint.h"

namespace iceberg {

TEST(TimePointTest, FormatTimePointMs) {
  // Unix timestamp for 2026-01-01T00:00:00 is 1767225600000
  auto time_point = TimePointMsFromUnixMs(1767225600000).value();
  EXPECT_EQ("2026-01-01T00:00:00.000", FormatTimePointMs(time_point));

  // Unix timestamp for 2026-01-01T12:20:00.123
  auto time_point2 =
      TimePointMsFromUnixMs(1767225600123 + (12 * 3600 + 20 * 60) * 1000).value();
  EXPECT_EQ("2026-01-01T12:20:00.123", FormatTimePointMs(time_point2));

  // Test with a date before 1970 (Unix epoch) - 1969-01-01T00:00:00
  // Unix timestamp for 1969-01-01T00:00:00 is -31536000000 ms from epoch
  auto time_point_before_epoch = TimePointMsFromUnixMs(-31536000000).value();
  EXPECT_EQ("1969-01-01T00:00:00.000", FormatTimePointMs(time_point_before_epoch));
}

TEST(TimePointTest, FormatUnixMicro) {
  // Test with whole seconds (micros = 0) - 2026-01-01T00:00:00.000000
  int64_t unix_micro = 1767225600000000;
  EXPECT_EQ("2026-01-01T00:00:00", FormatUnixMicro(unix_micro));

  // Test with milliseconds precision (micros ending in 000)
  int64_t unix_micro_ms = 1767225600001000;
  EXPECT_EQ("2026-01-01T00:00:00.001", FormatUnixMicro(unix_micro_ms));

  // Test with full microsecond precision
  int64_t unix_micro_full = 1767225600001234;
  EXPECT_EQ("2026-01-01T00:00:00.001234", FormatUnixMicro(unix_micro_full));

  // Test with a value that has more micros than a second
  int64_t unix_micro_over = 1767225661123456;
  EXPECT_EQ("2026-01-01T00:01:01.123456", FormatUnixMicro(unix_micro_over));

  // Test with a date before 1970
  int64_t unix_micro_before_epoch = -31536000000000;
  EXPECT_EQ("1969-01-01T00:00:00", FormatUnixMicro(unix_micro_before_epoch));
}

TEST(TimePointTest, FormatUnixMicroTz) {
  // Test with whole seconds (micros = 0)
  int64_t unix_micro = 1767225600000000;
  EXPECT_EQ("2026-01-01T00:00:00+00:00", FormatUnixMicroTz(unix_micro));

  // Test with milliseconds precision (micros ending in 000)
  int64_t unix_micro_ms = 1767225600001000;
  EXPECT_EQ("2026-01-01T00:00:00.001+00:00", FormatUnixMicroTz(unix_micro_ms));

  // Test with full microsecond precision
  int64_t unix_micro_full = 1767225600001234;
  EXPECT_EQ("2026-01-01T00:00:00.001234+00:00", FormatUnixMicroTz(unix_micro_full));

  // Test with a value that has more micros than a second
  int64_t unix_micro_over = 1767225661123456;
  EXPECT_EQ("2026-01-01T00:01:01.123456+00:00", FormatUnixMicroTz(unix_micro_over));

  // Test with a date before 1970
  int64_t unix_micro_before_epoch = -31536000000000;
  EXPECT_EQ("1969-01-01T00:00:00+00:00", FormatUnixMicroTz(unix_micro_before_epoch));
}

}  // namespace iceberg
