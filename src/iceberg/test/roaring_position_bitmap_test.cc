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

#include "iceberg/deletes/roaring_position_bitmap.h"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <random>
#include <set>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/test/matchers.h"
#include "iceberg/test/test_config.h"

namespace iceberg {

namespace {

constexpr int64_t kBitmapSize = 0xFFFFFFFFL;
constexpr int64_t kBitmapOffset = kBitmapSize + 1L;
constexpr int64_t kContainerSize = 0xFFFF;  // Character.MAX_VALUE
constexpr int64_t kContainerOffset = kContainerSize + 1L;

// Helper to construct a position from bitmap index, container index, and value
int64_t Position(int32_t bitmap_index, int32_t container_index, int64_t value) {
  return bitmap_index * kBitmapOffset + container_index * kContainerOffset + value;
}

std::string ReadTestResource(const std::string& filename) {
  std::filesystem::path path = std::filesystem::path(ICEBERG_TEST_RESOURCES) / filename;
  std::ifstream file(path, std::ios::binary);
  EXPECT_TRUE(file.good()) << "Cannot open: " << path;
  return {std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()};
}

RoaringPositionBitmap RoundTripSerialize(const RoaringPositionBitmap& bitmap) {
  auto serialized = bitmap.Serialize();
  EXPECT_THAT(serialized, IsOk());
  auto deserialized = RoaringPositionBitmap::Deserialize(serialized.value());
  EXPECT_THAT(deserialized, IsOk());
  return std::move(deserialized.value());
}

void AssertEqualContent(const RoaringPositionBitmap& bitmap,
                        const std::set<int64_t>& positions) {
  ASSERT_EQ(bitmap.Cardinality(), positions.size());
  for (int64_t pos : positions) {
    ASSERT_TRUE(bitmap.Contains(pos)) << "Position not found: " << pos;
  }
  bitmap.ForEach([&](int64_t pos) {
    ASSERT_TRUE(positions.contains(pos)) << "Unexpected position: " << pos;
  });
}

void AssertEqual(RoaringPositionBitmap& bitmap, const std::set<int64_t>& positions) {
  AssertEqualContent(bitmap, positions);

  auto copy1 = RoundTripSerialize(bitmap);
  AssertEqualContent(copy1, positions);

  bitmap.Optimize();
  auto copy2 = RoundTripSerialize(bitmap);
  AssertEqualContent(copy2, positions);
}

RoaringPositionBitmap BuildBitmap(const std::set<int64_t>& positions) {
  RoaringPositionBitmap bitmap;
  for (int64_t pos : positions) {
    bitmap.Add(pos);
  }
  return bitmap;
}

struct AddRangeParams {
  const char* name;
  int64_t start;
  int64_t end;
  std::vector<int64_t> absent_positions;
};

class RoaringPositionBitmapAddRangeTest
    : public ::testing::TestWithParam<AddRangeParams> {};

TEST_P(RoaringPositionBitmapAddRangeTest, AddsExpectedPositions) {
  const auto& param = GetParam();
  RoaringPositionBitmap bitmap;
  bitmap.AddRange(param.start, param.end);

  std::set<int64_t> expected_positions;
  for (int64_t pos = param.start; pos < param.end; ++pos) {
    expected_positions.insert(pos);
  }
  AssertEqualContent(bitmap, expected_positions);
  for (int64_t pos : param.absent_positions) {
    ASSERT_FALSE(bitmap.Contains(pos));
  }
}

INSTANTIATE_TEST_SUITE_P(
    AddRangeScenarios, RoaringPositionBitmapAddRangeTest,
    ::testing::Values(AddRangeParams{.name = "single_key",
                                     .start = 10,
                                     .end = 20,
                                     .absent_positions = {9, 20}},
                      AddRangeParams{
                          .name = "across_keys",
                          .start = (int64_t{1} << 32) - 5,
                          .end = (int64_t{1} << 32) + 5,
                          .absent_positions = {0, (int64_t{1} << 32) + 5},
                      },
                      AddRangeParams{
                          .name = "single_position",
                          .start = 42,
                          .end = 43,
                          .absent_positions = {41, 43},
                      }),
    [](const ::testing::TestParamInfo<AddRangeParams>& info) { return info.param.name; });

TEST(RoaringPositionBitmapTest, TestAddRangeLargeContiguous) {
  RoaringPositionBitmap bitmap;
  bitmap.AddRange(500, 200500);

  ASSERT_EQ(bitmap.Cardinality(), 200000u);
  ASSERT_TRUE(bitmap.Contains(500));
  ASSERT_TRUE(bitmap.Contains(200499));
  ASSERT_FALSE(bitmap.Contains(499));
  ASSERT_FALSE(bitmap.Contains(200500));
}

TEST(RoaringPositionBitmapTest, TestAddRangeSpanningThreeKeys) {
  RoaringPositionBitmap bitmap;

  int64_t start = (int64_t{0} << 32) | int64_t{0xFFFFFFF0};
  int64_t end = (int64_t{2} << 32) | int64_t{0x10};
  bitmap.AddRange(start, end);

  ASSERT_EQ(bitmap.Cardinality(), static_cast<size_t>(end - start));
  ASSERT_TRUE(bitmap.Contains(start));
  ASSERT_TRUE(bitmap.Contains(end - 1));
  ASSERT_FALSE(bitmap.Contains(start - 1));
  ASSERT_FALSE(bitmap.Contains(end));
  ASSERT_TRUE(bitmap.Contains(int64_t{1} << 32));
  // Verify a sample near the end of the middle key is also present
  ASSERT_TRUE(bitmap.Contains((int64_t{1} << 32) | int64_t{0xFFFFFFF0}));
}

TEST(RoaringPositionBitmapTest, TestAddRangeClampNegativeStart) {
  RoaringPositionBitmap bitmap;
  bitmap.AddRange(-1, 10);
  ASSERT_EQ(bitmap.Cardinality(), 10u);
  ASSERT_TRUE(bitmap.Contains(0));
  ASSERT_TRUE(bitmap.Contains(9));
  ASSERT_FALSE(bitmap.Contains(-1));
}

TEST(RoaringPositionBitmapTest, TestAddRangeClampBeyondMaxPosition) {
  RoaringPositionBitmap bitmap;
  // Range entirely beyond kMaxPosition: after clamping both endpoints the range
  // becomes empty, so no allocation or insertion happens.
  bitmap.AddRange(RoaringPositionBitmap::kMaxPosition + 1,
                  RoaringPositionBitmap::kMaxPosition + 10);
  ASSERT_TRUE(bitmap.IsEmpty());
}

struct AddRangeNoOpParams {
  const char* name;
  int64_t start;
  int64_t end;
};

class RoaringPositionBitmapAddRangeNoOpTest
    : public ::testing::TestWithParam<AddRangeNoOpParams> {};

TEST_P(RoaringPositionBitmapAddRangeNoOpTest, IsNoOp) {
  const auto& param = GetParam();
  RoaringPositionBitmap bitmap;
  bitmap.AddRange(param.start, param.end);
  ASSERT_TRUE(bitmap.IsEmpty());
  ASSERT_EQ(bitmap.Cardinality(), 0u);
}

INSTANTIATE_TEST_SUITE_P(
    AddRangeNoOpScenarios, RoaringPositionBitmapAddRangeNoOpTest,
    ::testing::Values(
        AddRangeNoOpParams{.name = "equal", .start = 100, .end = 100},
        AddRangeNoOpParams{.name = "zero_length_at_zero", .start = 0, .end = 0},
        AddRangeNoOpParams{.name = "negative_both", .start = -10, .end = -5}),
    [](const ::testing::TestParamInfo<AddRangeNoOpParams>& info) {
      return info.param.name;
    });

TEST(RoaringPositionBitmapTest, TestAddRangeReversedIsNoOp) {
  RoaringPositionBitmap bitmap;
  bitmap.AddRange(100, 50);
  ASSERT_TRUE(bitmap.IsEmpty());
}

struct OrParams {
  const char* name;
  std::set<int64_t> lhs_input;
  std::set<int64_t> rhs_input;
  std::set<int64_t> lhs_expected;
  std::set<int64_t> rhs_expected;
};

class RoaringPositionBitmapOrTest : public ::testing::TestWithParam<OrParams> {};

TEST_P(RoaringPositionBitmapOrTest, ProducesExpectedUnionAndKeepsRhsUnchanged) {
  const auto& param = GetParam();
  auto lhs = BuildBitmap(param.lhs_input);
  auto rhs = BuildBitmap(param.rhs_input);

  lhs.Or(rhs);

  AssertEqualContent(lhs, param.lhs_expected);
  AssertEqualContent(rhs, param.rhs_expected);
}

INSTANTIATE_TEST_SUITE_P(
    OrScenarios, RoaringPositionBitmapOrTest,
    ::testing::Values(
        OrParams{
            .name = "disjoint",
            .lhs_input = {10L, 20L},
            .rhs_input = {30L, 40L, int64_t{2} << 32},
            .lhs_expected = {10L, 20L, 30L, 40L, int64_t{2} << 32},
            .rhs_expected = {30L, 40L, int64_t{2} << 32},
        },
        OrParams{
            .name = "rhs_empty",
            .lhs_input = {10L, 20L},
            .rhs_input = {},
            .lhs_expected = {10L, 20L},
            .rhs_expected = {},
        },
        OrParams{
            .name = "overlapping",
            .lhs_input = {10L, 20L, 30L},
            .rhs_input = {20L, 40L},
            .lhs_expected = {10L, 20L, 30L, 40L},
            .rhs_expected = {20L, 40L},
        },
        OrParams{
            .name = "sparse_multi_key",
            .lhs_input = {100L, (int64_t{1} << 32) | 200L},
            .rhs_input = {(int64_t{2} << 32) | 300L, (int64_t{3} << 32) | 400L},
            .lhs_expected = {100L, (int64_t{1} << 32) | 200L, (int64_t{2} << 32) | 300L,
                             (int64_t{3} << 32) | 400L},
            .rhs_expected = {(int64_t{2} << 32) | 300L, (int64_t{3} << 32) | 400L},
        }),
    [](const ::testing::TestParamInfo<OrParams>& info) { return info.param.name; });

enum class InteropBitmapShape {
  kEmpty,
  kOnly32BitPositions,
  kSpreadAcrossKeys,
};

struct InteropCase {
  const char* file_name;
  InteropBitmapShape expected_shape;
};

void AssertInteropBitmapShape(const RoaringPositionBitmap& bitmap,
                              InteropBitmapShape expected_shape) {
  bool saw_pos_lt_32_bit = false;
  bool saw_pos_ge_32_bit = false;

  bitmap.ForEach([&](int64_t pos) {
    if (pos < (int64_t{1} << 32)) {
      saw_pos_lt_32_bit = true;
    } else {
      saw_pos_ge_32_bit = true;
    }
  });

  switch (expected_shape) {
    case InteropBitmapShape::kEmpty:
      ASSERT_TRUE(bitmap.IsEmpty());
      ASSERT_EQ(bitmap.Cardinality(), 0u);
      break;
    case InteropBitmapShape::kOnly32BitPositions:
      ASSERT_GT(bitmap.Cardinality(), 0u);
      ASSERT_TRUE(saw_pos_lt_32_bit);
      ASSERT_FALSE(saw_pos_ge_32_bit);
      break;
    case InteropBitmapShape::kSpreadAcrossKeys:
      ASSERT_GT(bitmap.Cardinality(), 0u);
      ASSERT_TRUE(saw_pos_lt_32_bit);
      ASSERT_TRUE(saw_pos_ge_32_bit);
      break;
  }
}

}  // namespace

TEST(RoaringPositionBitmapTest, TestAdd) {
  RoaringPositionBitmap bitmap;

  bitmap.Add(10L);
  ASSERT_TRUE(bitmap.Contains(10L));

  bitmap.Add(0L);
  ASSERT_TRUE(bitmap.Contains(0L));

  bitmap.Add(10L);  // duplicate
  ASSERT_TRUE(bitmap.Contains(10L));
}

TEST(RoaringPositionBitmapTest, TestAddPositionsRequiringMultipleBitmaps) {
  RoaringPositionBitmap bitmap;

  int64_t pos1 = 10L;
  int64_t pos2 = (int64_t{1} << 32) | 20L;
  int64_t pos3 = (int64_t{2} << 32) | 30L;
  int64_t pos4 = (int64_t{100} << 32) | 40L;

  bitmap.Add(pos1);
  bitmap.Add(pos2);
  bitmap.Add(pos3);
  bitmap.Add(pos4);

  AssertEqualContent(bitmap, {pos1, pos2, pos3, pos4});
  ASSERT_EQ(bitmap.SerializedSizeInBytes(), 1260);
}

TEST(RoaringPositionBitmapTest, TestAddEmptyRange) {
  RoaringPositionBitmap bitmap;
  bitmap.AddRange(10, 10);
  ASSERT_TRUE(bitmap.IsEmpty());
}

TEST(RoaringPositionBitmapTest, TestCardinality) {
  RoaringPositionBitmap bitmap;

  ASSERT_EQ(bitmap.Cardinality(), 0);

  bitmap.Add(10L);
  bitmap.Add(20L);
  bitmap.Add(30L);
  ASSERT_EQ(bitmap.Cardinality(), 3);

  bitmap.Add(10L);  // already exists
  ASSERT_EQ(bitmap.Cardinality(), 3);
}

TEST(RoaringPositionBitmapTest, TestCardinalitySparseBitmaps) {
  RoaringPositionBitmap bitmap;

  bitmap.Add(100L);
  bitmap.Add(101L);
  bitmap.Add(105L);
  bitmap.Add((int64_t{1} << 32) | 200L);
  bitmap.Add((int64_t{100} << 32) | 300L);

  ASSERT_EQ(bitmap.Cardinality(), 5);
}

TEST(RoaringPositionBitmapTest, TestSerializeDeserializeRoundTrip) {
  RoaringPositionBitmap bitmap;
  bitmap.Add(10L);
  bitmap.Add(20L);
  bitmap.Add((int64_t{1} << 32) | 30L);

  auto copy = RoundTripSerialize(bitmap);
  AssertEqualContent(copy, {10L, 20L, (int64_t{1} << 32) | 30L});
}

TEST(RoaringPositionBitmapTest, TestCopyConstructor) {
  RoaringPositionBitmap bitmap;
  bitmap.Add(10L);
  bitmap.Add((int64_t{2} << 32) | 30L);

  RoaringPositionBitmap copy(bitmap);

  AssertEqualContent(copy, {10L, (int64_t{2} << 32) | 30L});

  // Copy is independent from source.
  copy.Add(99L);
  ASSERT_FALSE(bitmap.Contains(99L));
}

TEST(RoaringPositionBitmapTest, TestCopyAssignment) {
  RoaringPositionBitmap bitmap;
  bitmap.Add(10L);
  bitmap.Add((int64_t{3} << 32) | 40L);

  RoaringPositionBitmap assigned;
  assigned.Add(1L);

  assigned = bitmap;
  AssertEqualContent(assigned, {10L, (int64_t{3} << 32) | 40L});

  // Assignment result is independent from source.
  bitmap.Add(200L);
  ASSERT_FALSE(assigned.Contains(200L));
}

TEST(RoaringPositionBitmapTest, TestSerializeDeserializeEmpty) {
  RoaringPositionBitmap bitmap;
  auto copy = RoundTripSerialize(bitmap);
  ASSERT_TRUE(copy.IsEmpty());
  ASSERT_EQ(copy.Cardinality(), 0);
}

TEST(RoaringPositionBitmapTest, TestSerializeDeserializeAllContainerBitmap) {
  RoaringPositionBitmap bitmap;

  // bitmap 0, container 0 (array - few elements)
  bitmap.Add(Position(0, 0, 5));
  bitmap.Add(Position(0, 0, 7));

  // bitmap 0, container 1 (array that can be compressed)
  bitmap.AddRange(Position(0, 1, 1), Position(0, 1, 1000));

  // bitmap 0, container 2 (bitset - nearly full container)
  bitmap.AddRange(Position(0, 2, 1), Position(0, 2, kContainerOffset - 1));

  // bitmap 1, container 0 (array)
  bitmap.Add(Position(1, 0, 10));
  bitmap.Add(Position(1, 0, 20));

  // bitmap 1, container 1 (array that can be compressed)
  bitmap.AddRange(Position(1, 1, 10), Position(1, 1, 500));

  // bitmap 1, container 2 (bitset)
  bitmap.AddRange(Position(1, 2, 1), Position(1, 2, kContainerOffset - 1));

  ASSERT_TRUE(bitmap.Optimize());

  auto copy = RoundTripSerialize(bitmap);
  std::set<int64_t> expected_positions;
  bitmap.ForEach([&](int64_t pos) { expected_positions.insert(pos); });

  AssertEqualContent(copy, expected_positions);
}

TEST(RoaringPositionBitmapTest, TestForEach) {
  RoaringPositionBitmap bitmap;
  bitmap.Add(30L);
  bitmap.Add(10L);
  bitmap.Add(20L);
  bitmap.Add((int64_t{1} << 32) | 5L);

  std::vector<int64_t> positions;
  bitmap.ForEach([&](int64_t pos) { positions.push_back(pos); });

  ASSERT_EQ(positions.size(), 4u);
  ASSERT_EQ(positions[0], 10L);
  ASSERT_EQ(positions[1], 20L);
  ASSERT_EQ(positions[2], 30L);
  ASSERT_EQ(positions[3], (int64_t{1} << 32) | 5L);
}

TEST(RoaringPositionBitmapTest, TestIsEmpty) {
  RoaringPositionBitmap bitmap;
  ASSERT_TRUE(bitmap.IsEmpty());

  bitmap.Add(10L);
  ASSERT_FALSE(bitmap.IsEmpty());
}

TEST(RoaringPositionBitmapTest, TestOptimize) {
  RoaringPositionBitmap bitmap;
  // Use Add() instead of AddRange() because addRange() creates run-length
  // encoded containers directly, leaving nothing for Optimize() to compress.
  for (int64_t i = 0; i < 10000; ++i) {
    bitmap.Add(i);
  }
  size_t size_before = bitmap.SerializedSizeInBytes();

  bool changed = bitmap.Optimize();
  ASSERT_TRUE(changed);

  // RLE optimization should reduce size for this dense range
  size_t size_after = bitmap.SerializedSizeInBytes();
  ASSERT_GT(size_before, size_after);

  // Content should be unchanged after RLE optimization
  std::set<int64_t> expected_positions;
  for (int64_t i = 0; i < 10000; ++i) {
    expected_positions.insert(i);
  }
  AssertEqualContent(bitmap, expected_positions);

  // Round-trip should preserve content after RLE
  auto copy = RoundTripSerialize(bitmap);
  AssertEqualContent(copy, expected_positions);
}

TEST(RoaringPositionBitmapTest, TestUnsupportedPositions) {
  RoaringPositionBitmap bitmap;

  // Negative position
  bitmap.Add(-1L);
  ASSERT_FALSE(bitmap.Contains(-1L));

  // Contains with negative position

  // Position exceeding MAX_POSITION - should be silently ignored
  bitmap.Add(RoaringPositionBitmap::kMaxPosition + 1L);
  ASSERT_FALSE(bitmap.Contains(RoaringPositionBitmap::kMaxPosition + 1L));

  // Contains with position exceeding MAX_POSITION - should return false
  ASSERT_FALSE(bitmap.Contains(RoaringPositionBitmap::kMaxPosition + 1L));
}

TEST(RoaringPositionBitmapTest, TestRandomSparseBitmap) {
  std::mt19937_64 rng(42);
  RoaringPositionBitmap bitmap;
  std::set<int64_t> positions;

  std::uniform_int_distribution<int64_t> dist(0, int64_t{5} << 32);

  for (int i = 0; i < 100000; ++i) {
    int64_t pos = dist(rng);
    positions.insert(pos);
    bitmap.Add(pos);
  }

  AssertEqual(bitmap, positions);

  // Random lookups
  std::mt19937_64 rng2(123);
  std::uniform_int_distribution<int64_t> lookup_dist(0,
                                                     RoaringPositionBitmap::kMaxPosition);
  for (int i = 0; i < 20000; ++i) {
    int64_t pos = lookup_dist(rng2);
    ASSERT_EQ(bitmap.Contains(pos), positions.contains(pos));
  }
}

TEST(RoaringPositionBitmapTest, TestRandomDenseBitmap) {
  RoaringPositionBitmap bitmap;
  std::set<int64_t> positions;

  // Create dense ranges across multiple bitmap keys
  for (int64_t offset : {int64_t{0}, int64_t{2} << 32, int64_t{5} << 32}) {
    for (int64_t i = 0; i < 10000; ++i) {
      bitmap.Add(offset + i);
      positions.insert(offset + i);
    }
  }

  AssertEqual(bitmap, positions);
}

TEST(RoaringPositionBitmapTest, TestRandomMixedBitmap) {
  std::mt19937_64 rng(42);
  RoaringPositionBitmap bitmap;
  std::set<int64_t> positions;

  // Sparse positions in [3<<32, 5<<32)
  std::uniform_int_distribution<int64_t> dist1(int64_t{3} << 32, int64_t{5} << 32);
  for (int i = 0; i < 50000; ++i) {
    int64_t pos = dist1(rng);
    positions.insert(pos);
    bitmap.Add(pos);
  }

  // Dense range in [0, 10000)
  for (int64_t i = 0; i < 10000; ++i) {
    bitmap.Add(i);
    positions.insert(i);
  }

  // More sparse positions in [0, 1<<32)
  std::uniform_int_distribution<int64_t> dist2(0, int64_t{1} << 32);
  for (int i = 0; i < 5000; ++i) {
    int64_t pos = dist2(rng);
    positions.insert(pos);
    bitmap.Add(pos);
  }

  AssertEqual(bitmap, positions);
}

TEST(RoaringPositionBitmapTest, TestDeserializeInvalidData) {
  // Buffer too small
  auto result = RoaringPositionBitmap::Deserialize("");
  ASSERT_THAT(result, IsError(ErrorKind::kInvalidArgument));

  // Invalid bitmap count (very large)
  std::string buf(8, '\xFF');
  result = RoaringPositionBitmap::Deserialize(buf);
  ASSERT_THAT(result, IsError(ErrorKind::kInvalidArgument));
}

TEST(RoaringPositionBitmapInteropTest, TestDeserializeSupportedRoaringExamples) {
  // These .bin fixtures are copied from the Apache Iceberg Java repository's
  // roaring position bitmap interoperability test resources.
  static const std::vector<InteropCase> kCases = {
      {.file_name = "64map32bitvals.bin",
       .expected_shape = InteropBitmapShape::kOnly32BitPositions},
      {.file_name = "64mapempty.bin", .expected_shape = InteropBitmapShape::kEmpty},
      {.file_name = "64mapspreadvals.bin",
       .expected_shape = InteropBitmapShape::kSpreadAcrossKeys},
  };

  for (const auto& test_case : kCases) {
    SCOPED_TRACE(test_case.file_name);
    std::string data = ReadTestResource(test_case.file_name);
    auto result = RoaringPositionBitmap::Deserialize(data);
    ASSERT_THAT(result, IsOk());

    const auto& bitmap = result.value();
    AssertInteropBitmapShape(bitmap, test_case.expected_shape);

    std::set<int64_t> positions;
    bitmap.ForEach([&](int64_t pos) { positions.insert(pos); });
    AssertEqualContent(bitmap, positions);

    auto copy = RoundTripSerialize(bitmap);
    AssertEqualContent(copy, positions);
  }
}

TEST(RoaringPositionBitmapInteropTest, TestDeserializeUnsupportedRoaringExample) {
  // This file is copied from the Apache Iceberg Java repository and it contains
  // a value with key larger than max supported
  std::string data = ReadTestResource("64maphighvals.bin");
  auto result = RoaringPositionBitmap::Deserialize(data);
  ASSERT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  ASSERT_THAT(result, HasErrorMessage("Invalid unsigned key"));
}

}  // namespace iceberg
