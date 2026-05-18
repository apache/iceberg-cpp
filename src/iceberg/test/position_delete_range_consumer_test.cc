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

#include "iceberg/deletes/position_delete_range_consumer.h"

#include <cstdint>
#include <limits>
#include <set>
#include <span>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/deletes/position_delete_index.h"
#include "iceberg/deletes/roaring_position_bitmap.h"

namespace iceberg {

namespace {

// Reference set: positions a per-pos `Delete(pos)` loop would accept.
std::set<int64_t> ExpectedValidSet(const std::vector<int64_t>& positions) {
  std::set<int64_t> expected;
  for (int64_t pos : positions) {
    if (pos >= 0 && pos <= RoaringPositionBitmap::kMaxPosition) {
      expected.insert(pos);
    }
  }
  return expected;
}

// Strict contents check: cardinality plus per-position membership.
// Weaker checks would miss divergences at the 32-bit key boundary.
void AssertMatchesBaseline(const std::vector<int64_t>& positions) {
  PositionDeleteIndex index;
  ForEachPositionDelete(std::span<const int64_t>(positions), index);
  const auto expected = ExpectedValidSet(positions);
  ASSERT_EQ(index.Cardinality(), static_cast<int64_t>(expected.size()))
      << "input size=" << positions.size();
  for (int64_t pos : expected) {
    ASSERT_TRUE(index.IsDeleted(pos)) << "missing pos=" << pos;
  }
}

}  // namespace

TEST(PositionDeleteRangeConsumerTest, EmptySpan) { AssertMatchesBaseline({}); }

TEST(PositionDeleteRangeConsumerTest, SinglePosition) { AssertMatchesBaseline({42}); }

TEST(PositionDeleteRangeConsumerTest, FullyContiguousRunBecomesSingleRange) {
  std::vector<int64_t> positions;
  for (int64_t i = 100; i < 200; ++i) {
    positions.push_back(i);
  }
  AssertMatchesBaseline(positions);
}

TEST(PositionDeleteRangeConsumerTest, AlternatingPositionsProduceNoCoalescing) {
  std::vector<int64_t> positions;
  for (int64_t i = 0; i < 50; ++i) {
    positions.push_back(i * 2);
  }
  AssertMatchesBaseline(positions);
}

TEST(PositionDeleteRangeConsumerTest, MixedShortAndLongRuns) {
  AssertMatchesBaseline({1, 2, 3, 7, 10, 11, 20, 30, 31, 32, 33, 34});
}

TEST(PositionDeleteRangeConsumerTest, UnsortedInputStillCorrect) {
  AssertMatchesBaseline({10, 5, 11, 12, 4, 13, 100});
}

TEST(PositionDeleteRangeConsumerTest, DuplicatesAreIdempotent) {
  AssertMatchesBaseline({5, 5, 5, 6, 6, 7});
}

TEST(PositionDeleteRangeConsumerTest, InvalidPositionsSilentlySkipped) {
  // Invalids at the edges, mid-run, and mixed with valid contiguous runs
  // must all be dropped without breaking coalescing around them. We stay
  // well below `kMaxPosition` to avoid forcing the bitmap to resize its
  // backing vector to ~2^31 empty containers.
  AssertMatchesBaseline({std::numeric_limits<int64_t>::min(), -5, -4, 10, 11, -999, 12,
                         13, RoaringPositionBitmap::kMaxPosition + 1,
                         std::numeric_limits<int64_t>::max()});
}

TEST(PositionDeleteRangeConsumerTest, ContiguousRunAcrossKeyBoundary) {
  // Pins `last_position + 1` and the adjacency check at a non-zero
  // high-32 key. The coalesced run must survive the key transition.
  constexpr int64_t kBoundary = int64_t{1} << 32;
  std::vector<int64_t> positions;
  for (int64_t i = kBoundary - 3; i < kBoundary + 3; ++i) {
    positions.push_back(i);
  }
  AssertMatchesBaseline(positions);
}

TEST(PositionDeleteRangeConsumerTest, DispatcherAgreesAtBothDensities) {
  // Above the sniff threshold at densities below and above the 10%
  // cutoff. We can't observe the choice directly; agreement with the
  // baseline is the contract.
  std::vector<int64_t> low_density;
  std::vector<int64_t> high_density;
  int64_t lo = 0;
  int64_t hi = 0;
  for (int64_t i = 0; i < 2'048; ++i) {
    low_density.push_back(lo);
    ++lo;
    if ((i + 1) % 20 == 0) {
      lo += 5;
    }
    high_density.push_back(hi);
    hi += ((i % 5 == 0) ? 5 : 1);
  }
  AssertMatchesBaseline(low_density);
  AssertMatchesBaseline(high_density);
}

TEST(PositionDeleteRangeConsumerTest, DispatcherSkipsSniffOnSmallInputs) {
  // Below the 64-element threshold the dispatcher bypasses the sniff.
  // Exercise both a scattered tiny input (where bulk would win at large
  // n) and a contiguous tiny input (the range path always wins).
  std::vector<int64_t> scattered;
  std::vector<int64_t> contiguous;
  for (int64_t i = 0; i < 32; ++i) {
    scattered.push_back(i * 100);
    contiguous.push_back(i);
  }
  AssertMatchesBaseline(scattered);
  AssertMatchesBaseline(contiguous);
}

TEST(PositionDeleteRangeConsumerTest, DispatcherAgreesAtThresholdBoundary) {
  // The dispatcher selects the bulk path when
  //   boundaries * 100 > (sniff - 1) * kBulkThresholdPercent
  // With `sniff = 1024` and `kBulkThresholdPercent = 10`, the cutoff is
  // 102.3 boundaries: 102 stays on coalesce, 103 flips to bulk. Both
  // inputs must still produce the same cardinality and membership as
  // the per-position baseline; this test guards against arithmetic
  // regressions around the threshold constant.
  auto build = [](int64_t target_boundaries) {
    std::vector<int64_t> positions;
    positions.reserve(1024);
    int64_t pos = 0;
    positions.push_back(pos);
    for (int64_t i = 1; i < 1024; ++i) {
      pos += (i <= target_boundaries) ? 2 : 1;
      positions.push_back(pos);
    }
    return positions;
  };
  AssertMatchesBaseline(build(/*target_boundaries=*/102));
  AssertMatchesBaseline(build(/*target_boundaries=*/103));
}

}  // namespace iceberg
