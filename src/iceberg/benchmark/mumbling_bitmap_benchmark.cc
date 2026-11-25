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

// Microbenchmarks for the Mumbling bitmap reader.
//
// Build with -DICEBERG_BUILD_BENCHMARKS=ON and run, for example:
//   ./build/src/iceberg/benchmark/mumbling_bitmap_benchmark --benchmark_filter=Pfor
//
// The inputs are serialized with the test encoder in
// iceberg/test/mumbling_bitmap_test_util.h, which picks PFOR bit widths the way
// the Java encoder does (checked against Java-generated fixtures in
// mumbling_bitmap_internal_test.cc).

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <random>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <benchmark/benchmark.h>

#include "iceberg/test/mumbling_bitmap_test_util.h"
#include "iceberg/util/mumbling_bitmap_internal.h"

namespace iceberg {
namespace {

using mumbling_test::BuildBitmapFromPositions;
using mumbling_test::Bytes;
using mumbling_test::EncodePfor;
using mumbling_test::kBestWidth;

constexpr int64_t kMaxPositions = MumblingBitmapView::kMaxCardinality;
constexpr uint32_t kSeed = 42;

// ---------------------------------------------------------------------------
// PFOR descriptor decoding
// ---------------------------------------------------------------------------

// Value distributions for PFOR inputs, modeled on Java's PFORRandomData.
enum class PforData {
  kConstant,         // every value is the same (one bit width of 0)
  kSmall,            // uniform 0-3
  kExceptions1Pct,   // 0-3 with 1% exceptions in 0-255
  kExceptions10Pct,  // 0-3 with 10% exceptions in 0-255
  kUniform,          // uniform 0-255 (stored as raw bytes)
};

std::vector<uint8_t> MakePforValues(PforData data, size_t count) {
  std::mt19937 rng(kSeed);
  std::uniform_int_distribution<int> small(0, 3);
  std::uniform_int_distribution<int> any(0, 255);
  std::uniform_real_distribution<double> unit(0.0, 1.0);
  std::vector<uint8_t> values(count);
  for (auto& value : values) {
    switch (data) {
      case PforData::kConstant:
        value = 5;
        break;
      case PforData::kSmall:
        value = static_cast<uint8_t>(small(rng));
        break;
      case PforData::kExceptions1Pct:
        value = static_cast<uint8_t>(unit(rng) < 0.01 ? any(rng) : small(rng));
        break;
      case PforData::kExceptions10Pct:
        value = static_cast<uint8_t>(unit(rng) < 0.10 ? any(rng) : small(rng));
        break;
      case PforData::kUniform:
        value = static_cast<uint8_t>(any(rng));
        break;
    }
  }
  return values;
}

// Arg: number of values.
void BM_PforDecode(benchmark::State& state, PforData data) {
  const auto count = static_cast<size_t>(state.range(0));
  const Bytes encoded = EncodePfor(MakePforValues(data, count), kBestWidth);
  std::vector<uint8_t> out(count);
  // Check once before timing, so a decode error isn't reported as a speedup.
  if (auto result = DecodePforBytes(encoded, out); !result.has_value()) {
    state.SkipWithError(result.error().message);
    return;
  }
  while (state.KeepRunning()) {
    auto result = DecodePforBytes(encoded, out);
    benchmark::DoNotOptimize(result);
    benchmark::DoNotOptimize(out.data());
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(count));
  state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(encoded.size()));
  state.counters["encoded_bytes"] = static_cast<double>(encoded.size());
}

void PforArgs(benchmark::internal::Benchmark* b) {
  b->ArgName("count")->Arg(256)->Arg(8192);
}

BENCHMARK_CAPTURE(BM_PforDecode, constant, PforData::kConstant)->Apply(PforArgs);
BENCHMARK_CAPTURE(BM_PforDecode, small, PforData::kSmall)->Apply(PforArgs);
BENCHMARK_CAPTURE(BM_PforDecode, exceptions_1pct, PforData::kExceptions1Pct)
    ->Apply(PforArgs);
BENCHMARK_CAPTURE(BM_PforDecode, exceptions_10pct, PforData::kExceptions10Pct)
    ->Apply(PforArgs);
BENCHMARK_CAPTURE(BM_PforDecode, uniform, PforData::kUniform)->Apply(PforArgs);

// ---------------------------------------------------------------------------
// Bitmaps
// ---------------------------------------------------------------------------

// Position layouts for bitmap inputs.
enum class Layout {
  kRandom,  // positions spread uniformly over the full range (mostly sparse)
  kRuns,    // runs of 64 consecutive positions (dense containers between gaps)
  kPrefix,  // the first N positions (all dense containers)
};

std::vector<int64_t> MakePositions(Layout layout, int64_t cardinality) {
  std::vector<int64_t> positions;
  positions.reserve(cardinality);
  switch (layout) {
    case Layout::kRandom: {
      std::mt19937_64 rng(kSeed);
      std::uniform_int_distribution<int64_t> dist(0, kMaxPositions - 1);
      std::set<int64_t> unique;
      while (std::cmp_less(unique.size(), cardinality)) {
        unique.insert(dist(rng));
      }
      positions.assign(unique.begin(), unique.end());
      break;
    }
    case Layout::kRuns: {
      // Spread the runs so that they cover the whole range when possible.
      const int64_t runs = (cardinality + 63) / 64;
      const int64_t stride = std::max<int64_t>(64, (kMaxPositions / runs) / 64 * 64);
      for (int64_t run = 0; std::cmp_less(positions.size(), cardinality); ++run) {
        for (int64_t i = 0; i < 64 && std::cmp_less(positions.size(), cardinality); ++i) {
          positions.push_back(run * stride + i);
        }
      }
      break;
    }
    case Layout::kPrefix:
      for (int64_t i = 0; i < cardinality; ++i) {
        positions.push_back(i);
      }
      break;
  }
  return positions;
}

Bytes MakeBitmapBytes(const benchmark::State& state, Layout layout) {
  return BuildBitmapFromPositions(MakePositions(layout, state.range(0)));
}

MumblingBitmapView MakeView(const Bytes& bytes) {
  auto bitmap = MumblingBitmapView::Make(bytes);
  if (!bitmap.has_value()) {
    std::abort();
  }
  return std::move(bitmap).value();
}

void BitmapArgs(benchmark::internal::Benchmark* b) {
  b->ArgName("cardinality")->Arg(1'000)->Arg(100'000);
}

// The largest bitmap: every position set.
void MaxBitmapArgs(benchmark::internal::Benchmark* b) {
  b->ArgName("cardinality")->Arg(kMaxPositions);
}

#define MUMBLING_BITMAP_BENCHMARK(fn)                                \
  BENCHMARK_CAPTURE(fn, random, Layout::kRandom)->Apply(BitmapArgs); \
  BENCHMARK_CAPTURE(fn, runs, Layout::kRuns)->Apply(BitmapArgs);     \
  BENCHMARK_CAPTURE(fn, prefix, Layout::kPrefix)->Apply(BitmapArgs); \
  BENCHMARK_CAPTURE(fn, full, Layout::kPrefix)->Apply(MaxBitmapArgs)

// Arg: cardinality. Creating a view: header checks, descriptor decoding and
// the container offset table.
void BM_Make(benchmark::State& state, Layout layout) {
  const Bytes bytes = MakeBitmapBytes(state, layout);
  // Check once before timing, so a decode error isn't reported as a speedup.
  if (auto bitmap = MumblingBitmapView::Make(bytes); !bitmap.has_value()) {
    state.SkipWithError(bitmap.error().message);
    return;
  }
  while (state.KeepRunning()) {
    auto bitmap = MumblingBitmapView::Make(bytes);
    benchmark::DoNotOptimize(bitmap);
  }
  state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(bytes.size()));
  state.counters["serialized_bytes"] = static_cast<double>(bytes.size());
}

MUMBLING_BITMAP_BENCHMARK(BM_Make);

// Arg: cardinality. Probes random positions over the full range.
void BM_Contains(benchmark::State& state, Layout layout) {
  const Bytes bytes = MakeBitmapBytes(state, layout);
  const MumblingBitmapView bitmap = MakeView(bytes);
  constexpr size_t kProbeCount = 4096;
  std::mt19937_64 rng(kSeed + 1);
  std::uniform_int_distribution<int64_t> dist(0, kMaxPositions - 1);
  std::vector<int64_t> probes(kProbeCount);
  for (auto& probe : probes) {
    probe = dist(rng);
  }

  while (state.KeepRunning()) {
    int64_t hits = 0;
    for (int64_t probe : probes) {
      hits += bitmap.Contains(probe) ? 1 : 0;
    }
    benchmark::DoNotOptimize(hits);
  }
  state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(kProbeCount));
}

MUMBLING_BITMAP_BENCHMARK(BM_Contains);

}  // namespace
}  // namespace iceberg

int main(int argc, char** argv) {
  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
    return 1;
  }
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  return 0;
}
