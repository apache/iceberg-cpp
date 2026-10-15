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

#include <array>
#include <chrono>
#include <cstdint>
#include <limits>
#include <numeric>
#include <type_traits>

#include <gtest/gtest.h>

#include "iceberg/expression/literal.h"
#include "iceberg/test/temporal_test_helper.h"
#include "iceberg/util/decimal.h"
#include "iceberg/util/murmurhash3_internal.h"
#include "iceberg/util/uuid.h"

namespace iceberg {

static_assert(std::is_convertible_v<decltype(&MurmurHash3_x86_32),
                                    uint32_t (*)(const void*, size_t, uint32_t)>);

// The following tests are from
// https://iceberg.apache.org/spec/#appendix-b-32-bit-hash-requirements
TEST(BucketUtilsTest, HashHelper) {
  // int and long
  EXPECT_EQ(BucketUtils::HashInt(34), 2017239379);
  EXPECT_EQ(BucketUtils::HashLong(34L), 2017239379);
  const auto bucket = BucketUtils::BucketIndex(Literal::Int(34), 100);
  ASSERT_TRUE(bucket.has_value());
  EXPECT_EQ(*bucket, 79);

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

TEST(BucketUtilsTest, BucketByteLiterals) {
  struct BucketCase {
    Literal literal;
    int32_t bucket;
  };
  const std::array<BucketCase, 3> cases = {{
      {.literal = Literal::String("iceberg"), .bucket = 89},
      {.literal = Literal::Binary({0, 1, 2, 3}), .bucket = 41},
      {.literal = Literal::Fixed({0, 1, 2, 3}), .bucket = 41},
  }};
  for (const auto& test : cases) {
    const auto bucket = BucketUtils::BucketIndex(test.literal, 100);
    ASSERT_TRUE(bucket.has_value());
    EXPECT_EQ(*bucket, test.bucket);
  }
}

TEST(BucketUtilsTest, CanonicalLittleEndianBlocks) {
  constexpr size_t kMaxLength = 33;
  constexpr size_t kAlignment = alignof(uint64_t);
  struct HashCase {
    size_t length;
    uint32_t hash;
  };
  constexpr std::array<HashCase, 14> cases = {{
      {.length = 0, .hash = 0x00000000u},
      {.length = 1, .hash = 0x514e28b7u},
      {.length = 2, .hash = 0x70e1a2c0u},
      {.length = 3, .hash = 0x51d4d0d7u},
      {.length = 4, .hash = 0xf4c0ec39u},
      {.length = 5, .hash = 0xcca4dccbu},
      {.length = 6, .hash = 0x901cc302u},
      {.length = 7, .hash = 0x8d7e4914u},
      {.length = 8, .hash = 0xd161d673u},
      {.length = 15, .hash = 0x5bd6952du},
      {.length = 16, .hash = 0x191573ddu},
      {.length = 17, .hash = 0xcbe58dc6u},
      {.length = 32, .hash = 0xcac37638u},
      {.length = kMaxLength, .hash = 0x5460867au},
  }};
  for (const auto& test : cases) {
    SCOPED_TRACE(test.length);
    for (size_t offset = 0; offset < kAlignment; ++offset) {
      SCOPED_TRACE(offset);
      alignas(uint64_t) std::array<uint8_t, kMaxLength + kAlignment - 1> input{};
      std::iota(input.begin() + offset, input.begin() + offset + test.length, uint8_t{0});
      EXPECT_EQ(static_cast<uint32_t>(
                    BucketUtils::HashBytes({input.data() + offset, test.length})),
                test.hash);
    }
  }
}

TEST(BucketUtilsTest, HashLongMatchesCanonicalBytes) {
  constexpr std::array<int64_t, 9> values = {0,
                                             34,
                                             -1,
                                             std::numeric_limits<int32_t>::min(),
                                             std::numeric_limits<int32_t>::max(),
                                             std::numeric_limits<int64_t>::min(),
                                             std::numeric_limits<int64_t>::max(),
                                             0x0123456789abcdefLL,
                                             static_cast<int64_t>(0xfedcba9876543210ULL)};
  for (const auto value : values) {
    SCOPED_TRACE(value);
    std::array<uint8_t, sizeof(value)> bytes{};
    for (size_t i = 0; i < bytes.size(); ++i) {
      bytes[i] = static_cast<uint8_t>(static_cast<uint64_t>(value) >> (i * 8));
    }
    EXPECT_EQ(BucketUtils::HashLong(value), BucketUtils::HashBytes(bytes));
  }
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

  const auto pre_epoch_micros_bucket =
      BucketUtils::BucketIndex(Literal::Timestamp(-876544), kNumBuckets);
  const auto pre_epoch_nanos_bucket =
      BucketUtils::BucketIndex(Literal::TimestampNs(-876543211), kNumBuckets);

  ASSERT_TRUE(pre_epoch_micros_bucket.has_value());
  ASSERT_TRUE(pre_epoch_nanos_bucket.has_value());
  EXPECT_EQ(pre_epoch_micros_bucket.value(), pre_epoch_nanos_bucket.value());

  const auto pre_epoch_tz_micros_bucket =
      BucketUtils::BucketIndex(Literal::TimestampTz(-876544), kNumBuckets);
  const auto pre_epoch_tz_nanos_bucket =
      BucketUtils::BucketIndex(Literal::TimestampTzNs(-876543211), kNumBuckets);

  ASSERT_TRUE(pre_epoch_tz_micros_bucket.has_value());
  ASSERT_TRUE(pre_epoch_tz_nanos_bucket.has_value());
  EXPECT_EQ(pre_epoch_tz_micros_bucket.value(), pre_epoch_tz_nanos_bucket.value());
}

}  // namespace iceberg
