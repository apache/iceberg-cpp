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
  std::chrono::sys_days sd = std::chrono::year{2017} / 11 / 16;
  std::chrono::sys_days epoch{std::chrono::year{1970} / 1 / 1};
  int32_t days = (sd - epoch).count();
  EXPECT_EQ(BucketUtils::HashInt(days), -653330422);

  // time
  // 22:31:08 in microseconds
  int64_t time_micros = (22 * 3600 + 31 * 60 + 8) * 1000000LL;
  EXPECT_EQ(BucketUtils::HashLong(time_micros), -662762989);

  // timestamp
  // 2017-11-16T22:31:08 in microseconds
  std::chrono::system_clock::time_point tp =
      std::chrono::sys_days{std::chrono::year{2017} / 11 / 16} + std::chrono::hours{22} +
      std::chrono::minutes{31} + std::chrono::seconds{8};
  int64_t timestamp_micros =
      std::chrono::duration_cast<std::chrono::microseconds>(tp.time_since_epoch())
          .count();
  EXPECT_EQ(BucketUtils::HashLong(timestamp_micros), -2047944441);
  // 2017-11-16T22:31:08.000001 in microseconds
  EXPECT_EQ(BucketUtils::HashLong(timestamp_micros + 1), -1207196810);

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

}  // namespace iceberg
