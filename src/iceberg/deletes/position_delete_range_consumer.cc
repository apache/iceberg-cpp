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

#include <algorithm>
#include <cstdint>
#include <span>
#include <vector>

#include "iceberg/deletes/position_delete_index.h"
#include "iceberg/deletes/roaring_position_bitmap.h"

namespace iceberg {

namespace {

bool IsValidPosition(int64_t pos) {
  return pos >= 0 && pos <= RoaringPositionBitmap::kMaxPosition;
}

// Unsigned subtraction so negative or wrap-around input can't
// false-positive via signed overflow.
bool IsAdjacent(int64_t prev, int64_t next) {
  return (static_cast<uint64_t>(next) - static_cast<uint64_t>(prev)) == 1;
}

// `RoaringPositionBitmap` shards positions by their high 32 bits; the
// bulk path groups by this key before flushing via `BulkAddForKey`.
int32_t HighKeyFromPosition(int64_t pos) { return static_cast<int32_t>(pos >> 32); }

// Emit `[range_start, last_position]`, collapsing singletons. Callers
// pre-filter via `IsValidPosition`, so `last_position + 1` cannot overflow.
void EmitRange(PositionDeleteIndex& target, int64_t range_start, int64_t last_position) {
  if (range_start == last_position) {
    target.Delete(range_start);
  } else {
    target.Delete(range_start, last_position + 1);
  }
}

// Emit closed-interval runs; out-of-range positions are silently skipped
// to match `Delete(pos)`.
void CoalesceIntoRanges(std::span<const int64_t> positions, PositionDeleteIndex& target) {
  const size_t n = positions.size();

  size_t i = 0;
  while (i < n && !IsValidPosition(positions[i])) {
    ++i;
  }
  if (i == n) {
    return;
  }

  int64_t range_start = positions[i];
  int64_t last_position = range_start;
  ++i;

  for (; i < n; ++i) {
    const int64_t pos = positions[i];
    if (!IsValidPosition(pos)) {
      continue;
    }
    if (!IsAdjacent(last_position, pos)) {
      EmitRange(target, range_start, last_position);
      range_start = pos;
    }
    last_position = pos;
  }

  EmitRange(target, range_start, last_position);
}

}  // namespace

void ForEachPositionDelete(std::span<const int64_t> positions,
                           PositionDeleteIndex& target) {
  if (positions.empty()) {
    return;
  }

  // Below this size the bulk path's fixed overhead beats any coalescing win.
  constexpr size_t kMinSniffSize = 64;
  if (positions.size() < kMinSniffSize) {
    CoalesceIntoRanges(positions, target);
    return;
  }

  // Bounded prefix size for the boundary-density estimate.
  constexpr size_t kSniffSize = 1024;
  // Above this boundary density take the bulk path; below it stay on coalesce.
  constexpr size_t kBulkThresholdPercent = 10;

  const size_t sniff = std::min(positions.size(), kSniffSize);
  size_t boundaries = 0;
  for (size_t i = 1; i < sniff; ++i) {
    boundaries += static_cast<size_t>(!IsAdjacent(positions[i - 1], positions[i]));
  }

  // boundaries / (sniff - 1) > kBulkThresholdPercent / 100, without FP.
  if (boundaries * 100 > (sniff - 1) * kBulkThresholdPercent) {
    // Bulk path: group by high-32-bit key, flush each group via CRoaring's
    // `addMany` (through `BulkAddForKey`). The thread-local buffer is
    // reused across calls; nested invocations on the same thread would
    // corrupt it -- see `\warning` on `ForEachPositionDelete`.
    thread_local std::vector<uint32_t> bulk_key_positions;
    const size_t n = positions.size();
    size_t i = 0;
    while (i < n) {
      while (i < n && !IsValidPosition(positions[i])) {
        ++i;
      }
      if (i == n) {
        break;
      }
      const int32_t key = HighKeyFromPosition(positions[i]);
      bulk_key_positions.clear();
      while (i < n && IsValidPosition(positions[i]) &&
             HighKeyFromPosition(positions[i]) == key) {
        bulk_key_positions.push_back(static_cast<uint32_t>(positions[i] & 0xFFFFFFFFu));
        ++i;
      }
      target.BulkAddForKey(key, bulk_key_positions.data(), bulk_key_positions.size());
    }
    return;
  }

  CoalesceIntoRanges(positions, target);
}

}  // namespace iceberg
