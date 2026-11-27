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
#include <cstdint>
#include <limits>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/test/matchers.h"
#include "iceberg/util/bin_packing_internal.h"

namespace iceberg::internal::bin_packing {
namespace {

using Bins = std::vector<std::vector<int>>;
constexpr int kUnlimitedLookback = std::numeric_limits<int>::max();

Result<Bins> Pack(const std::vector<int>& items, int64_t target_weight,
                  int lookback = kUnlimitedLookback, bool largest_bin_first = false,
                  int64_t max_items_per_bin = std::numeric_limits<int64_t>::max()) {
  ListPacker<int> packer(target_weight, lookback, largest_bin_first, max_items_per_bin);
  return packer.Pack(items, [](int item) { return item; });
}

Result<Bins> PackEnd(const std::vector<int>& items, int64_t target_weight,
                     int lookback = kUnlimitedLookback) {
  ListPacker<int> packer(target_weight, lookback, /*largest_bin_first=*/false);
  return packer.PackEnd(items, [](int item) { return item; });
}

TEST(BinPackingTest, BasicBinPacking) {
  EXPECT_THAT(Pack({1, 2, 3, 4, 5}, 3),
              HasValue(::testing::Eq(Bins{{1, 2}, {3}, {4}, {5}})));
  EXPECT_THAT(Pack({1, 2, 3, 4, 5}, 5),
              HasValue(::testing::Eq(Bins{{1, 2}, {3}, {4}, {5}})));
  EXPECT_THAT(Pack({1, 2, 3, 4, 5}, 6),
              HasValue(::testing::Eq(Bins{{1, 2, 3}, {4}, {5}})));
  EXPECT_THAT(Pack({1, 2, 3, 4, 5}, 8),
              HasValue(::testing::Eq(Bins{{1, 2, 3}, {4}, {5}})));
  EXPECT_THAT(Pack({1, 2, 3, 4, 5}, 9), HasValue(::testing::Eq(Bins{{1, 2, 3}, {4, 5}})));
  EXPECT_THAT(Pack({1, 2, 3, 4, 5}, 10),
              HasValue(::testing::Eq(Bins{{1, 2, 3, 4}, {5}})));
  EXPECT_THAT(Pack({1, 2, 3, 4, 5}, 14),
              HasValue(::testing::Eq(Bins{{1, 2, 3, 4}, {5}})));
  EXPECT_THAT(Pack({1, 2, 3, 4, 5}, 15), HasValue(::testing::Eq(Bins{{1, 2, 3, 4, 5}})));
}

TEST(BinPackingTest, BasicBinPackingTargetSize) {
  EXPECT_THAT(
      Pack({1, 2, 3, 4, 5}, 3, kUnlimitedLookback, /*largest_bin_first=*/false, 2),
      HasValue(::testing::Eq(Bins{{1, 2}, {3}, {4}, {5}})));
  EXPECT_THAT(
      Pack({1, 2, 3, 4, 5}, 5, kUnlimitedLookback, /*largest_bin_first=*/false, 2),
      HasValue(::testing::Eq(Bins{{1, 2}, {3}, {4}, {5}})));
  EXPECT_THAT(
      Pack({1, 2, 3, 4, 5}, 6, kUnlimitedLookback, /*largest_bin_first=*/false, 2),
      HasValue(::testing::Eq(Bins{{1, 2}, {3}, {4}, {5}})));
  EXPECT_THAT(
      Pack({1, 2, 3, 4, 5}, 8, kUnlimitedLookback, /*largest_bin_first=*/false, 3),
      HasValue(::testing::Eq(Bins{{1, 2, 3}, {4}, {5}})));
  EXPECT_THAT(
      Pack({1, 2, 3, 4, 5}, 9, kUnlimitedLookback, /*largest_bin_first=*/false, 3),
      HasValue(::testing::Eq(Bins{{1, 2, 3}, {4, 5}})));
  EXPECT_THAT(
      Pack({1, 2, 3, 4, 5}, 10, kUnlimitedLookback, /*largest_bin_first=*/false, 3),
      HasValue(::testing::Eq(Bins{{1, 2, 3}, {4, 5}})));
  EXPECT_THAT(
      Pack({1, 2, 3, 4, 5}, 14, kUnlimitedLookback, /*largest_bin_first=*/false, 3),
      HasValue(::testing::Eq(Bins{{1, 2, 3}, {4, 5}})));
}

TEST(BinPackingTest, ReverseBinPackingSingleLookback) {
  EXPECT_THAT(PackEnd({1, 2, 3, 4, 5}, 3, 1),
              HasValue(::testing::Eq(Bins{{1, 2}, {3}, {4}, {5}})));
  EXPECT_THAT(PackEnd({1, 2, 3, 4, 5}, 4, 1),
              HasValue(::testing::Eq(Bins{{1, 2}, {3}, {4}, {5}})));
  EXPECT_THAT(PackEnd({1, 2, 3, 4, 5}, 5, 1),
              HasValue(::testing::Eq(Bins{{1}, {2, 3}, {4}, {5}})));
  EXPECT_THAT(PackEnd({1, 2, 3, 4, 5}, 6, 1),
              HasValue(::testing::Eq(Bins{{1, 2, 3}, {4}, {5}})));
  EXPECT_THAT(PackEnd({1, 2, 3, 4, 5}, 7, 1),
              HasValue(::testing::Eq(Bins{{1, 2}, {3, 4}, {5}})));
  EXPECT_THAT(PackEnd({1, 2, 3, 4, 5}, 8, 1),
              HasValue(::testing::Eq(Bins{{1, 2}, {3, 4}, {5}})));
  EXPECT_THAT(PackEnd({1, 2, 3, 4, 5}, 9, 1),
              HasValue(::testing::Eq(Bins{{1, 2, 3}, {4, 5}})));
  EXPECT_THAT(PackEnd({1, 2, 3, 4, 5}, 11, 1),
              HasValue(::testing::Eq(Bins{{1, 2, 3}, {4, 5}})));
  EXPECT_THAT(PackEnd({1, 2, 3, 4, 5}, 12, 1),
              HasValue(::testing::Eq(Bins{{1, 2}, {3, 4, 5}})));
  EXPECT_THAT(PackEnd({1, 2, 3, 4, 5}, 14, 1),
              HasValue(::testing::Eq(Bins{{1}, {2, 3, 4, 5}})));
  EXPECT_THAT(PackEnd({1, 2, 3, 4, 5}, 15, 1),
              HasValue(::testing::Eq(Bins{{1, 2, 3, 4, 5}})));
}

TEST(BinPackingTest, ReverseBinPackingUnlimitedLookback) {
  EXPECT_THAT(PackEnd({1, 2, 3, 4, 5}, 3),
              HasValue(::testing::Eq(Bins{{1, 2}, {3}, {4}, {5}})));
  EXPECT_THAT(PackEnd({1, 2, 3, 4, 5}, 4),
              HasValue(::testing::Eq(Bins{{2}, {1, 3}, {4}, {5}})));
  EXPECT_THAT(PackEnd({1, 2, 3, 4, 5}, 5),
              HasValue(::testing::Eq(Bins{{2, 3}, {1, 4}, {5}})));
  EXPECT_THAT(PackEnd({1, 2, 3, 4, 5}, 6),
              HasValue(::testing::Eq(Bins{{3}, {2, 4}, {1, 5}})));
  EXPECT_THAT(PackEnd({1, 2, 3, 4, 5}, 7),
              HasValue(::testing::Eq(Bins{{1}, {3, 4}, {2, 5}})));
  EXPECT_THAT(PackEnd({1, 2, 3, 4, 5}, 8),
              HasValue(::testing::Eq(Bins{{1, 2, 4}, {3, 5}})));
  EXPECT_THAT(PackEnd({1, 2, 3, 4, 5}, 9),
              HasValue(::testing::Eq(Bins{{1, 2, 3}, {4, 5}})));
  EXPECT_THAT(PackEnd({1, 2, 3, 4, 5}, 10),
              HasValue(::testing::Eq(Bins{{2, 3}, {1, 4, 5}})));
  EXPECT_THAT(PackEnd({1, 2, 3, 4, 5}, 11),
              HasValue(::testing::Eq(Bins{{1, 3}, {2, 4, 5}})));
  EXPECT_THAT(PackEnd({1, 2, 3, 4, 5}, 12),
              HasValue(::testing::Eq(Bins{{1, 2}, {3, 4, 5}})));
  EXPECT_THAT(PackEnd({1, 2, 3, 4, 5}, 13),
              HasValue(::testing::Eq(Bins{{2}, {1, 3, 4, 5}})));
  EXPECT_THAT(PackEnd({1, 2, 3, 4, 5}, 14),
              HasValue(::testing::Eq(Bins{{1}, {2, 3, 4, 5}})));
  EXPECT_THAT(PackEnd({1, 2, 3, 4, 5}, 15),
              HasValue(::testing::Eq(Bins{{1, 2, 3, 4, 5}})));
}

TEST(BinPackingTest, BinPackingLookBack) {
  EXPECT_THAT(Pack({5, 1, 5, 1, 5, 1}, 8),
              HasValue(::testing::Eq(Bins{{5, 1, 1, 1}, {5}, {5}})));
  EXPECT_THAT(Pack({5, 1, 5, 1, 5, 1}, 8, 2),
              HasValue(::testing::Eq(Bins{{5, 1, 1}, {5, 1}, {5}})));
  EXPECT_THAT(Pack({5, 1, 5, 1, 5, 1}, 8, 1),
              HasValue(::testing::Eq(Bins{{5, 1}, {5, 1}, {5, 1}})));
  EXPECT_THAT(Pack({36, 36, 36, 36, 65, 65, 128}, 128, 2, /*largest_bin_first=*/true),
              HasValue(::testing::Eq(Bins{{36, 36, 36}, {128}, {36, 65}, {65}})));
  EXPECT_THAT(Pack({64, 64, 128, 32, 32, 32, 32}, 128, 1, /*largest_bin_first=*/true),
              HasValue(::testing::Eq(Bins{{64, 64}, {128}, {32, 32, 32, 32}})));
}

TEST(BinPackingTest, BinWeightDoesNotOverflow) {
  const int64_t max = std::numeric_limits<int64_t>::max();
  ListPacker<int64_t> packer(max, 1, /*largest_bin_first=*/false);
  EXPECT_THAT(
      packer.Pack(std::vector<int64_t>{max, 1}, [](int64_t item) { return item; }),
      HasValue(::testing::Eq(std::vector<std::vector<int64_t>>{{max}, {1}})));
}

}  // namespace
}  // namespace iceberg::internal::bin_packing
