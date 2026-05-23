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

#include "iceberg/util/math_util_internal.h"

#include <limits>

#include <gtest/gtest.h>

#include "iceberg/test/matchers.h"

namespace iceberg::internal {

TEST(MathUtilInternalTest, FloorDiv) {
  EXPECT_EQ(0, FloorDiv(0, 1000));
  EXPECT_EQ(1, FloorDiv(1001, 1000));
  EXPECT_EQ(-1, FloorDiv(-1, 1000));
  EXPECT_EQ(-2, FloorDiv(-1001, 1000));
  EXPECT_EQ(1, FloorDiv(-1001, -1000));
  EXPECT_EQ(-2, FloorDiv(1001, -1000));
}

TEST(MathUtilInternalTest, MultiplyExact) {
  ICEBERG_UNWRAP_OR_FAIL(auto positive, MultiplyExact(1000, 1000));
  EXPECT_EQ(1000000, positive);

  ICEBERG_UNWRAP_OR_FAIL(auto negative, MultiplyExact(-1000, 1000));
  EXPECT_EQ(-1000000, negative);

  ICEBERG_UNWRAP_OR_FAIL(auto min_value,
                         MultiplyExact(std::numeric_limits<int64_t>::min(), 1));
  EXPECT_EQ(std::numeric_limits<int64_t>::min(), min_value);

  EXPECT_THAT(MultiplyExact(std::numeric_limits<int64_t>::max(), 2),
              IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(MultiplyExact(std::numeric_limits<int64_t>::min(), -1),
              IsError(ErrorKind::kInvalidArgument));
}

}  // namespace iceberg::internal
