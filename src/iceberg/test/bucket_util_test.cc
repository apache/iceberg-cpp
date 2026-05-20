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

#include "iceberg/util/bucket_util.h"

#include <chrono>

#include <gtest/gtest.h>

#include "iceberg/expression/literal.h"
#include "iceberg/test/temporal_test_helper.h"
#include "iceberg/util/decimal.h"
#include "iceberg/util/uuid.h"

namespace iceberg {

// The following tests are from
// https://iceberg.apache.org/spec/#appendix-b-32-bit-hash-requirements
TEST(BucketUtilsTest, HashHelper) {
  // int and long
  EXPECT_EQ(BucketUtils::HashInt(34), 2017239379);
  EXPECT_EQ(BucketUtils::HashLong(34L), 2017239379);

  // decimal hash
  auto decimal = Decimal::FromString("14.20");
  ASSERT_TRUE(decimal.has_value());
  EXPECT_EQ(BucketUtils::HashBytes(decimal->ToBigEndian()), -500754589);

  // date hash
  EXPECT_EQ(BucketUtils::HashInt(
                TemporalTestHelper::CreateDate({.year = 2017, .month = 11, .day = 16})),
            -653330422);

  // time
  EXPECT_EQ(BucketUtils::HashLong(
                TemporalTestHelper::CreateTime({.hour = 22, .minute = 31, .second = 8})),
            -662762989);

  // timestamp
  // 2017-11-16T22:31:08 in microseconds
  EXPECT_EQ(
      BucketUtils::HashLong(TemporalTestHelper::CreateTimestamp(
          {.year = 2017, .month = 11, .day = 16, .hour = 22, .minute = 31, .second = 8})),
      -2047944441);

  // 2017-11-16T22:31:08.000001 in microseconds
  EXPECT_EQ(
      BucketUtils::HashLong(TemporalTestHelper::CreateTimestamp({.year = 2017,
                                                                 .month = 11,
                                                                 .day = 16,
                                                                 .hour = 22,
                                                                 .minute = 31,
                                                                 .second = 8,
                                                                 .microsecond = 1})),
      -1207196810);

  // 2017-11-16T14:31:08-08:00 in microseconds
  EXPECT_EQ(BucketUtils::HashLong(
                TemporalTestHelper::CreateTimestampTz({.year = 2017,
                                                       .month = 11,
                                                       .day = 16,
                                                       .hour = 14,
                                                       .minute = 31,
                                                       .second = 8,
                                                       .tz_offset_minutes = -480})),
            -2047944441);

  // 2017-11-16T14:31:08.000001-08:00 in microseconds
  EXPECT_EQ(BucketUtils::HashLong(
                TemporalTestHelper::CreateTimestampTz({.year = 2017,
                                                       .month = 11,
                                                       .day = 16,
                                                       .hour = 14,
                                                       .minute = 31,
                                                       .second = 8,
                                                       .microsecond = 1,
                                                       .tz_offset_minutes = -480})),
            -1207196810);

  // string
  std::string str = "iceberg";
  EXPECT_EQ(BucketUtils::HashBytes(std::span<const uint8_t>(
                reinterpret_cast<const uint8_t*>(str.data()), str.size())),
            1210000089);

  // uuid
  auto uuid = Uuid::FromString("f79c3e09-677c-4bbd-a479-3f349cb785e7");
  EXPECT_EQ(BucketUtils::HashBytes(uuid->bytes()), 1488055340);

  // fixed & binary
  std::vector<uint8_t> fixed = {0, 1, 2, 3};
  EXPECT_EQ(BucketUtils::HashBytes(fixed), -188683207);
}

TEST(BucketUtilsTest, BucketTimestampNanosMatchesMicros) {
  constexpr int32_t kNumBuckets = 1000;
  const auto ts_micros = TemporalTestHelper::CreateTimestamp({.year = 2017,
                                                              .month = 11,
                                                              .day = 16,
                                                              .hour = 22,
                                                              .minute = 31,
                                                              .second = 8,
                                                              .microsecond = 1});
  const auto ts_nanos = TemporalTestHelper::CreateTimestampNanos({.year = 2017,
                                                                  .month = 11,
                                                                  .day = 16,
                                                                  .hour = 22,
                                                                  .minute = 31,
                                                                  .second = 8,
                                                                  .nanosecond = 1000});

  const auto micros_bucket =
      BucketUtils::BucketIndex(Literal::Timestamp(ts_micros), kNumBuckets);
  const auto nanos_bucket =
      BucketUtils::BucketIndex(Literal::TimestampNs(ts_nanos), kNumBuckets);

  ASSERT_TRUE(micros_bucket.has_value());
  ASSERT_TRUE(nanos_bucket.has_value());
  EXPECT_EQ(micros_bucket.value(), nanos_bucket.value());

  const auto ts_tz_micros =
      TemporalTestHelper::CreateTimestampTz({.year = 2017,
                                             .month = 11,
                                             .day = 16,
                                             .hour = 14,
                                             .minute = 31,
                                             .second = 8,
                                             .microsecond = 1,
                                             .tz_offset_minutes = -480});
  const auto ts_tz_nanos =
      TemporalTestHelper::CreateTimestampTzNanos({.year = 2017,
                                                  .month = 11,
                                                  .day = 16,
                                                  .hour = 14,
                                                  .minute = 31,
                                                  .second = 8,
                                                  .nanosecond = 1000,
                                                  .tz_offset_minutes = -480});

  const auto tz_micros_bucket =
      BucketUtils::BucketIndex(Literal::TimestampTz(ts_tz_micros), kNumBuckets);
  const auto tz_nanos_bucket =
      BucketUtils::BucketIndex(Literal::TimestampTzNs(ts_tz_nanos), kNumBuckets);

  ASSERT_TRUE(tz_micros_bucket.has_value());
  ASSERT_TRUE(tz_nanos_bucket.has_value());
  EXPECT_EQ(tz_micros_bucket.value(), tz_nanos_bucket.value());
}

}  // namespace iceberg
