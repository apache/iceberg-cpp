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

#pragma once

#include <cstdint>
#include <limits>

#include "iceberg/result.h"
#include "iceberg/util/int128.h"

namespace iceberg {

inline constexpr int64_t FloorDiv(int64_t dividend, int64_t divisor) {
  const auto quotient = dividend / divisor;
  if ((dividend ^ divisor) < 0 && quotient * divisor != dividend) {
    return quotient - 1;
  }
  return quotient;
}

inline Result<int64_t> MultiplyExact(int64_t lhs, int64_t rhs) {
  const auto result = static_cast<int128_t>(lhs) * static_cast<int128_t>(rhs);
  if (result > std::numeric_limits<int64_t>::max() ||
      result < std::numeric_limits<int64_t>::min()) [[unlikely]] {
    return InvalidArgument("Long overflow when multiplying {} by {}", lhs, rhs);
  }
  return static_cast<int64_t>(result);
}

}  // namespace iceberg
