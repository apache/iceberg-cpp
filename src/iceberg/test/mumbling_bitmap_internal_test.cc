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

#include "iceberg/util/mumbling_bitmap_internal.h"

#include <algorithm>
#include <array>
#include <bit>
#include <cstdint>
#include <fstream>
#include <limits>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/test/matchers.h"
#include "iceberg/test/mumbling_bitmap_test_util.h"
#include "iceberg/test/test_resource.h"

namespace iceberg {

namespace {

using mumbling_test::BuildBitmap;
using mumbling_test::Bytes;
using mumbling_test::Dense;
using mumbling_test::DenseRange;
using mumbling_test::EncodePfor;
using mumbling_test::Header;
using mumbling_test::kBestWidth;

std::vector<int64_t> Range(int64_t start, int64_t end, int64_t step = 1) {
  std::vector<int64_t> result;
  for (int64_t i = start; i < end; i += step) {
    result.push_back(i);
  }
  return result;
}

// Checks Contains() for every position up to one container past the last
// expected one, and for the ends of the position range.
void ExpectPositions(const MumblingBitmapView& bitmap,
                     const std::vector<int64_t>& expected) {
  EXPECT_EQ(bitmap.Cardinality(), static_cast<int64_t>(expected.size()));
  std::set<int64_t> expected_set(expected.begin(), expected.end());
  const int64_t limit = expected.empty() ? 512 : (expected.back() / 256 + 2) * 256;
  for (int64_t pos = -1; pos < limit; ++pos) {
    ASSERT_EQ(bitmap.Contains(pos), expected_set.contains(pos)) << "pos=" << pos;
  }
  for (int64_t pos : {int64_t{MumblingBitmapView::kMaxCardinality} - 1,
                      int64_t{MumblingBitmapView::kMaxCardinality},
                      std::numeric_limits<int64_t>::max()}) {
    ASSERT_EQ(bitmap.Contains(pos), expected_set.contains(pos)) << "pos=" << pos;
  }
}

Result<std::vector<uint8_t>> DecodePfor(const Bytes& data, size_t count,
                                        size_t expected_bytes_read) {
  std::vector<uint8_t> out(count);
  ICEBERG_ASSIGN_OR_RAISE(auto bytes_read, DecodePforBytes(data, out));
  EXPECT_EQ(bytes_read, expected_bytes_read);
  return out;
}

// One line of a fixture file: a hex string and a comma-separated list of
// integers, separated by a tab.
struct Fixture {
  int line;
  Bytes bytes;
  std::vector<int64_t> values;
};

Bytes ParseHex(std::string_view hex) {
  Bytes out;
  for (size_t i = 0; i + 1 < hex.size(); i += 2) {
    out.push_back(
        static_cast<uint8_t>(std::stoi(std::string(hex.substr(i, 2)), nullptr, 16)));
  }
  return out;
}

std::vector<int64_t> ParseValues(std::string_view text) {
  std::vector<int64_t> values;
  size_t start = 0;
  while (start < text.size()) {
    size_t end = text.find(',', start);
    if (end == std::string_view::npos) {
      end = text.size();
    }
    values.push_back(std::stoll(std::string(text.substr(start, end - start))));
    start = end + 1;
  }
  return values;
}

// Reads `file_name` from the test resources. `hex_column` is the column (0 or 1)
// that holds the encoded bytes; the other column holds the values.
std::vector<Fixture> ReadFixtures(const std::string& file_name, int hex_column) {
  std::ifstream file(GetResourcePath(file_name));
  EXPECT_TRUE(file.is_open()) << "Cannot open " << file_name;
  std::vector<Fixture> fixtures;
  std::string line;
  for (int line_number = 1; std::getline(file, line); ++line_number) {
    if (line.empty() || line.starts_with('#')) {
      continue;
    }
    const size_t tab = line.find('\t');
    EXPECT_NE(tab, std::string::npos) << file_name << ":" << line_number;
    if (tab == std::string::npos) {
      continue;
    }
    std::array<std::string_view, 2> columns = {std::string_view(line).substr(0, tab),
                                               std::string_view(line).substr(tab + 1)};
    fixtures.push_back({.line = line_number,
                        .bytes = ParseHex(columns[hex_column]),
                        .values = ParseValues(columns[1 - hex_column])});
  }
  return fixtures;
}

}  // namespace

// Examples from "Appendix A: PFOR encoding for unsigned bytes" of the Mumbling
// spec: https://github.com/apache/iceberg/blob/main/format/mumbling-spec.md#examples
TEST(PforDecodeTest, SpecExamples) {
  ICEBERG_UNWRAP_OR_FAIL(auto all_zero, DecodePfor({0x00, 0x00, 0x00}, 256, 3));
  EXPECT_EQ(all_zero, std::vector<uint8_t>(256, 0));

  ICEBERG_UNWRAP_OR_FAIL(auto all_five, DecodePfor({0x00, 0x00, 0x05}, 51, 3));
  EXPECT_EQ(all_five, std::vector<uint8_t>(51, 5));

  ICEBERG_UNWRAP_OR_FAIL(auto exceptions_only,
                         DecodePfor({0x80, 0x02, 0x00, 0x04, 0x07, 0xFF, 0xFE}, 8, 7));
  EXPECT_EQ(exceptions_only, (std::vector<uint8_t>{0, 0, 0, 0, 0xFF, 0, 0, 0xFE}));

  ICEBERG_UNWRAP_OR_FAIL(auto two_bits, DecodePfor({0x02, 0x00, 0x06, 0x18}, 3, 4));
  EXPECT_EQ(two_bits, (std::vector<uint8_t>{6, 7, 8}));

  ICEBERG_UNWRAP_OR_FAIL(auto with_exception,
                         DecodePfor({0x32, 0x01, 0x06, 0x09, 0x01, 0xE0}, 4, 6));
  EXPECT_EQ(with_exception, (std::vector<uint8_t>{6, 34, 8, 7}));
}

// The Java encoder's choice for spec example 5 (b1 = 5, no exceptions), from
// TestPFOREncoding#testExample5 in apache/iceberg.
TEST(PforDecodeTest, JavaEncodingOfExample5) {
  ICEBERG_UNWRAP_OR_FAIL(auto values,
                         DecodePfor({0x05, 0x00, 0x06, 0x07, 0x04, 0x10}, 4, 6));
  EXPECT_EQ(values, (std::vector<uint8_t>{6, 34, 8, 7}));
}

TEST(PforDecodeTest, HandPackedWidths) {
  ICEBERG_UNWRAP_OR_FAIL(auto one_bit, DecodePfor({0x01, 0x00, 0x00, 0xB0, 0x80}, 9, 5));
  EXPECT_EQ(one_bit, (std::vector<uint8_t>{1, 0, 1, 1, 0, 0, 0, 0, 1}));

  ICEBERG_UNWRAP_OR_FAIL(auto three_bits,
                         DecodePfor({0x03, 0x00, 0x00, 0xE2, 0xA0}, 4, 5));
  EXPECT_EQ(three_bits, (std::vector<uint8_t>{7, 0, 5, 2}));

  ICEBERG_UNWRAP_OR_FAIL(auto four_bits,
                         DecodePfor({0x04, 0x00, 0x01, 0xA5, 0xF0}, 3, 5));
  EXPECT_EQ(four_bits, (std::vector<uint8_t>{0xB, 0x6, 0x10}));

  ICEBERG_UNWRAP_OR_FAIL(auto six_bits,
                         DecodePfor({0x06, 0x00, 0x00, 0xFC, 0x18, 0x00}, 3, 6));
  EXPECT_EQ(six_bits, (std::vector<uint8_t>{63, 1, 32}));

  ICEBERG_UNWRAP_OR_FAIL(auto seven_bits,
                         DecodePfor({0x07, 0x00, 0x00, 0xFE, 0x04}, 2, 5));
  EXPECT_EQ(seven_bits, (std::vector<uint8_t>{0x7F, 0x01}));

  ICEBERG_UNWRAP_OR_FAIL(auto eight_bits,
                         DecodePfor({0x08, 0x00, 0x00, 0x20, 0x00, 0x1F}, 3, 6));
  EXPECT_EQ(eight_bits, (std::vector<uint8_t>{0x20, 0x00, 0x1F}));
}

TEST(PforDecodeTest, EmptyInput) {
  ICEBERG_UNWRAP_OR_FAIL(auto values, DecodePfor({}, 0, 0));
  EXPECT_TRUE(values.empty());
}

TEST(PforDecodeTest, MultipleChunks) {
  // 256 values of 7, then 44 raw values.
  Bytes data = {0x00, 0x00, 0x07, 0x08, 0x00, 0x00};
  std::vector<uint8_t> expected(256, 7);
  for (int i = 0; i < 44; ++i) {
    data.push_back(static_cast<uint8_t>(i));
    expected.push_back(static_cast<uint8_t>(i));
  }
  // Trailing bytes after the encoded values are not consumed.
  data.push_back(0xAB);
  ICEBERG_UNWRAP_OR_FAIL(auto values, DecodePfor(data, 300, data.size() - 1));
  EXPECT_EQ(values, expected);
}

TEST(PforDecodeTest, AllWidthsRoundTrip) {
  std::mt19937 rng(42);
  for (int b1 = 0; b1 <= 8; ++b1) {
    for (size_t count : {1, 7, 255, 256, 257, 600}) {
      std::vector<uint8_t> values(count);
      for (auto& value : values) {
        // Mostly small values with a few larger ones to produce exceptions.
        value = static_cast<uint8_t>(rng() % 16 == 0 ? 3 + rng() % 253 : 3 + rng() % 4);
      }
      auto encoded = EncodePfor(values, b1);
      ICEBERG_UNWRAP_OR_FAIL(auto decoded, DecodePfor(encoded, count, encoded.size()));
      EXPECT_EQ(decoded, values) << "b1=" << b1 << " count=" << count;
    }
  }
}

TEST(PforDecodeTest, Malformed) {
  std::vector<uint8_t> out(4);
  auto decode = [&](const Bytes& data) { return DecodePforBytes(data, out); };

  EXPECT_THAT(decode({0x00, 0x00}), HasErrorMessage("Truncated PFOR chunk header"));
  EXPECT_THAT(decode({0x09, 0x00, 0x00, 0, 0, 0, 0, 0}),
              HasErrorMessage("Invalid PFOR primary bit width: 9"));
  EXPECT_THAT(decode({0x72, 0x00, 0x00, 0, 0, 0, 0}),
              HasErrorMessage("Invalid PFOR exception bit width: 7"));
  EXPECT_THAT(decode({0x18, 0x00, 0x00, 0, 0, 0, 0, 0}),
              HasErrorMessage("Invalid PFOR exception bit width: 1"));
  EXPECT_THAT(decode({0x08, 0x01, 0x00, 0, 0, 0, 0, 0}),
              HasErrorMessage("exceptions with 8-bit primary values"));
  EXPECT_THAT(decode({0x80, 0x05, 0x00, 0, 1, 2, 3, 4, 1, 1, 1, 1, 1}),
              HasErrorMessage("5 exceptions for 4 values"));
  EXPECT_THAT(decode({0x02, 0x00, 0x00}), HasErrorMessage("Truncated PFOR chunk:"));
  EXPECT_THAT(decode({0x80, 0x01, 0x00, 0x00}), HasErrorMessage("Truncated PFOR chunk:"));
  EXPECT_THAT(decode({0x80, 0x01, 0x00, 0x04, 0x01}),
              HasErrorMessage("Invalid PFOR exception offset: 4"));
  EXPECT_THAT(decode({0x01, 0x00, 0xFF, 0x80}),
              HasErrorMessage("Invalid PFOR value at index 0: 256"));
  EXPECT_THAT(decode({0x08, 0x00, 0x01, 0x00, 0x00, 0xFF, 0x00}),
              HasErrorMessage("Invalid PFOR value at index 2: 256"));

  // The second chunk is missing.
  std::vector<uint8_t> two_chunks(257);
  EXPECT_THAT(DecodePforBytes(Bytes{0x00, 0x00, 0x00}, two_chunks),
              HasErrorMessage("Truncated PFOR chunk header"));
}

TEST(MumblingBitmapTest, EmptyBitmap) {
  const Bytes bitmap_bytes = Bytes{0x01, 0, 0, 0, 0, 0};
  ICEBERG_UNWRAP_OR_FAIL(auto bitmap, MumblingBitmapView::Make(bitmap_bytes));
  ExpectPositions(bitmap, {});
}

TEST(MumblingBitmapTest, EmptySparseContainers) {
  const Bytes bitmap_bytes = BuildBitmap({{}, {}, {}});
  ICEBERG_UNWRAP_OR_FAIL(auto bitmap, MumblingBitmapView::Make(bitmap_bytes));
  ExpectPositions(bitmap, {});
}

// Sparse container examples from the spec.
TEST(MumblingBitmapTest, SparseSpecExamples) {
  // Hand-encoded: header, PFOR chunk {b1=0, e=0, m=3}, container `00 22 FF`.
  Bytes bytes = {0x01, 0x03, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x03, 0x00, 0x22, 0xFF};
  ICEBERG_UNWRAP_OR_FAIL(auto bitmap, MumblingBitmapView::Make(bytes));
  ExpectPositions(bitmap, {0, 34, 255});

  Bytes full;
  for (uint8_t i = 0; i < 31; ++i) {
    full.push_back(i);
  }
  const Bytes full_bitmap_bytes = BuildBitmap({full});
  ICEBERG_UNWRAP_OR_FAIL(auto full_bitmap, MumblingBitmapView::Make(full_bitmap_bytes));
  ExpectPositions(full_bitmap, Range(0, 31));
}

// Dense container examples from the spec.
TEST(MumblingBitmapTest, DenseSpecExamples) {
  Bytes first_32(32, 0);
  std::fill_n(first_32.begin(), 4, 0xFF);
  const Bytes bitmap1_bytes = BuildBitmap({first_32});
  ICEBERG_UNWRAP_OR_FAIL(auto bitmap1, MumblingBitmapView::Make(bitmap1_bytes));
  ExpectPositions(bitmap1, Range(0, 32));

  Bytes first_33 = first_32;
  first_33[4] = 0x80;
  const Bytes bitmap2_bytes = BuildBitmap({first_33});
  ICEBERG_UNWRAP_OR_FAIL(auto bitmap2, MumblingBitmapView::Make(bitmap2_bytes));
  ExpectPositions(bitmap2, Range(0, 33));

  Bytes ends(32, 0);
  ends[0] = ends[1] = ends[30] = ends[31] = 0xFF;
  const Bytes bitmap3_bytes = BuildBitmap({ends});
  ICEBERG_UNWRAP_OR_FAIL(auto bitmap3, MumblingBitmapView::Make(bitmap3_bytes));
  auto expected = Range(0, 16);
  auto tail = Range(240, 256);
  expected.insert(expected.end(), tail.begin(), tail.end());
  ExpectPositions(bitmap3, expected);

  Bytes even(32, 0xAA);
  const Bytes bitmap4_bytes = BuildBitmap({even});
  ICEBERG_UNWRAP_OR_FAIL(auto bitmap4, MumblingBitmapView::Make(bitmap4_bytes));
  ExpectPositions(bitmap4, Range(0, 256, 2));

  Bytes all(32, 0xFF);
  const Bytes bitmap5_bytes = BuildBitmap({all});
  ICEBERG_UNWRAP_OR_FAIL(auto bitmap5, MumblingBitmapView::Make(bitmap5_bytes));
  ExpectPositions(bitmap5, Range(0, 256));
}

TEST(MumblingBitmapTest, MixedContainers) {
  // Same layout as SPARSE_DENSE_SPARSE in Java's TestMumblingBitmap: every
  // other byte of the first 8 bytes of the dense container is 0xFF.
  Bytes dense(32, 0);
  dense[1] = dense[3] = dense[5] = dense[7] = 0xFF;
  const Bytes bitmap_bytes = BuildBitmap({{0}, dense, {}, {0x01, 0xFF}, {}});
  ICEBERG_UNWRAP_OR_FAIL(auto bitmap, MumblingBitmapView::Make(bitmap_bytes));

  std::vector<int64_t> expected = {0};
  for (int64_t byte : {1, 3, 5, 7}) {
    auto range = Range(256 + byte * 8, 256 + byte * 8 + 8);
    expected.insert(expected.end(), range.begin(), range.end());
  }
  expected.push_back(3 * 256 + 1);
  expected.push_back(3 * 256 + 255);
  ExpectPositions(bitmap, expected);
}

TEST(MumblingBitmapTest, PforEncodedDescriptors) {
  // Descriptors {0, 0, 5, 0, 32, 0, 1}, encoded with every primary bit width.
  std::vector<int> dense_positions;
  for (int i = 0; i < 40; ++i) {
    dense_positions.push_back(i * 6);
  }
  std::vector<Bytes> containers = {{}, {}, {1, 2, 3, 4, 200}, {}, Dense(dense_positions),
                                   {}, {9}};
  std::vector<int64_t> expected = {512 + 1, 512 + 2, 512 + 3, 512 + 4, 512 + 200};
  for (int64_t i = 0; i < 40; ++i) {
    expected.push_back(1024 + i * 6);
  }
  expected.push_back(6 * 256 + 9);
  for (int b1 = 0; b1 <= 8; ++b1) {
    const Bytes bitmap_bytes = BuildBitmap(containers, b1);
    ICEBERG_UNWRAP_OR_FAIL(auto bitmap, MumblingBitmapView::Make(bitmap_bytes));
    ExpectPositions(bitmap, expected);
  }
}

// The spec says readers must ignore the low bits of a dense descriptor. Note:
// the Java reader currently rejects such descriptors.
TEST(MumblingBitmapTest, DenseDescriptorIgnoresLowBits) {
  Bytes bytes = Header(256, 1);
  bytes.insert(bytes.end(), {0x08, 0x00, 0x00, 0x21});
  bytes.insert(bytes.end(), 32, 0xFF);
  ICEBERG_UNWRAP_OR_FAIL(auto bitmap, MumblingBitmapView::Make(bytes));
  ExpectPositions(bitmap, Range(0, 256));
}

TEST(MumblingBitmapTest, MaximumSize) {
  // 8192 dense containers with every bit set. Descriptors are 32 PFOR chunks of
  // 256 values, each encoded as `00 00 20`.
  Bytes bytes =
      Header(MumblingBitmapView::kMaxCardinality, MumblingBitmapView::kMaxContainerCount);
  for (int chunk = 0; chunk < 32; ++chunk) {
    bytes.insert(bytes.end(), {0x00, 0x00, 0x20});
  }
  bytes.insert(bytes.end(), size_t{MumblingBitmapView::kMaxContainerCount} * 32, 0xFF);
  ICEBERG_UNWRAP_OR_FAIL(auto bitmap, MumblingBitmapView::Make(bytes));
  EXPECT_EQ(bitmap.Cardinality(), 2'097'152);
  EXPECT_TRUE(bitmap.Contains(0));
  EXPECT_TRUE(bitmap.Contains(2'097'151));
  EXPECT_FALSE(bitmap.Contains(2'097'152));
  int64_t set = 0;
  for (int64_t pos = 0; pos < MumblingBitmapView::kMaxCardinality; ++pos) {
    set += bitmap.Contains(pos) ? 1 : 0;
  }
  EXPECT_EQ(set, MumblingBitmapView::kMaxCardinality);
}

TEST(MumblingBitmapTest, OutOfRangePositions) {
  const Bytes bitmap_bytes = BuildBitmap({{0, 255}});
  ICEBERG_UNWRAP_OR_FAIL(auto bitmap, MumblingBitmapView::Make(bitmap_bytes));
  EXPECT_FALSE(bitmap.Contains(-1));
  EXPECT_FALSE(bitmap.Contains(std::numeric_limits<int64_t>::min()));
  EXPECT_FALSE(bitmap.Contains(256));
  EXPECT_FALSE(bitmap.Contains(std::numeric_limits<int64_t>::max()));
  EXPECT_TRUE(bitmap.Contains(255));
}

TEST(MumblingBitmapTest, RandomRoundTrip) {
  std::mt19937 rng(7);
  for (int iteration = 0; iteration < 20; ++iteration) {
    const int container_count = 1 + static_cast<int>(rng() % 40);
    std::set<int64_t> positions;
    std::vector<Bytes> containers;
    for (int c = 0; c < container_count; ++c) {
      // Pick a density: empty, sparse, borderline or dense.
      const int target = std::vector<int>{0, 3, 31, 32, 100, 256}[rng() % 6];
      std::set<int> in_container;
      while (std::cmp_less(in_container.size(), target)) {
        in_container.insert(static_cast<int>(rng() % 256));
      }
      for (int pos : in_container) {
        positions.insert(int64_t{c} * 256 + pos);
      }
      std::vector<int> sorted(in_container.begin(), in_container.end());
      if (sorted.size() < 32) {
        containers.emplace_back(sorted.begin(), sorted.end());
      } else {
        containers.push_back(Dense(sorted));
      }
    }
    const int b1 = static_cast<int>(rng() % 9);
    const Bytes bitmap_bytes = BuildBitmap(containers, b1);
    ICEBERG_UNWRAP_OR_FAIL(auto bitmap, MumblingBitmapView::Make(bitmap_bytes));
    ExpectPositions(bitmap, std::vector<int64_t>(positions.begin(), positions.end()));
  }
}

TEST(MumblingBitmapTest, InvalidHeader) {
  EXPECT_THAT(MumblingBitmapView::Make(Bytes{}),
              HasErrorMessage("Truncated Mumbling bitmap header"));
  EXPECT_THAT(MumblingBitmapView::Make(Bytes{0x01, 0, 0, 0, 0}),
              HasErrorMessage("Truncated Mumbling bitmap header"));

  // Versions 0 and 2, as in Java's TestMumblingBitmap#testInvalidVersion.
  EXPECT_THAT(MumblingBitmapView::Make(Bytes{0x00, 0, 0, 0, 0, 0}),
              IsError(ErrorKind::kNotSupported));
  EXPECT_THAT(MumblingBitmapView::Make(Bytes{0x02, 0, 0, 0, 0, 0}),
              HasErrorMessage("Unsupported Mumbling bitmap version: 2"));

  // From Java's TestMumblingBitmap#testInvalidCardinality.
  EXPECT_THAT(MumblingBitmapView::Make(Bytes{0x01, 0x01, 0x00, 0x20, 0x00, 0x00}),
              HasErrorMessage("cardinality: 2097153"));

  EXPECT_THAT(MumblingBitmapView::Make(Header(0, 8193)),
              HasErrorMessage("container count: 8193"));
  EXPECT_THAT(MumblingBitmapView::Make(Bytes{0x01, 0, 0, 0, 0xFF, 0xFF}),
              HasErrorMessage("container count: 65535"));
}

TEST(MumblingBitmapTest, InvalidDescriptors) {
  // Reserved descriptor bits, as in Java's TestMumblingBitmap#testInvalidDescriptor.
  for (uint8_t descriptor : {0x40, 0x80, 0xC0, 0x5F}) {
    Bytes bytes = Header(1, 1);
    bytes.insert(bytes.end(), {0x08, 0x00, 0x00, descriptor});
    EXPECT_THAT(MumblingBitmapView::Make(bytes), HasErrorMessage("not sparse or dense"));
  }

  // The descriptor array is truncated.
  Bytes truncated = Header(0, 2);
  truncated.insert(truncated.end(), {0x08, 0x00, 0x00, 0x00});
  EXPECT_THAT(MumblingBitmapView::Make(truncated),
              HasErrorMessage("Truncated PFOR chunk"));

  // From Java's TestMumblingBitmap#testMaximumCardinalityAndContainers: the
  // header is valid but no descriptors follow.
  EXPECT_THAT(MumblingBitmapView::Make(Bytes{0x01, 0xFF, 0xFF, 0x1F, 0xFF, 0x1F}),
              HasErrorMessage("Truncated PFOR chunk header"));
}

TEST(MumblingBitmapTest, InvalidContainers) {
  auto truncated = BuildBitmap({{1, 2, 3}, DenseRange(0, 64)});
  truncated.pop_back();
  EXPECT_THAT(MumblingBitmapView::Make(truncated),
              HasErrorMessage("Truncated Mumbling bitmap containers: 34 bytes available, "
                              "35 required"));

  auto trailing = BuildBitmap({{1, 2, 3}});
  trailing.push_back(0x00);
  EXPECT_THAT(MumblingBitmapView::Make(trailing),
              HasErrorMessage("1 unexpected trailing bytes"));
}

// Make() checks only what memory safety needs, like the Java reader. Input
// that breaks the spec's content rules is accepted and still gives defined
// answers.
TEST(MumblingBitmapTest, ContentRulesAreNotChecked) {
  // The header cardinality is reported as stored.
  const Bytes wrong_cardinality = BuildBitmap({{1, 2, 3}}, 8, 4);
  ICEBERG_UNWRAP_OR_FAIL(auto bitmap1, MumblingBitmapView::Make(wrong_cardinality));
  EXPECT_EQ(bitmap1.Cardinality(), 4);
  EXPECT_TRUE(bitmap1.Contains(1));
  EXPECT_TRUE(bitmap1.Contains(3));
  EXPECT_FALSE(bitmap1.Contains(4));

  // Unsorted and duplicate sparse positions: Contains() finds every stored
  // position, including in a non-final container, which uses the SSE2 path
  // on x86-64.
  Bytes unsorted;
  for (uint8_t i = 0; i < 31; ++i) {
    unsorted.push_back(static_cast<uint8_t>(250 - i * 8));
  }
  const Bytes unsorted_bytes = BuildBitmap({unsorted, {5, 3, 3}}, 8, 34);
  ICEBERG_UNWRAP_OR_FAIL(auto bitmap2, MumblingBitmapView::Make(unsorted_bytes));
  for (uint8_t value : unsorted) {
    EXPECT_TRUE(bitmap2.Contains(value)) << int{value};
  }
  EXPECT_FALSE(bitmap2.Contains(251));
  EXPECT_TRUE(bitmap2.Contains(256 + 3));
  EXPECT_TRUE(bitmap2.Contains(256 + 5));
  EXPECT_FALSE(bitmap2.Contains(256 + 4));

  // A dense container with fewer than 32 positions.
  const Bytes sparse_dense = BuildBitmap({DenseRange(0, 3)});
  ICEBERG_UNWRAP_OR_FAIL(auto bitmap3, MumblingBitmapView::Make(sparse_dense));
  ExpectPositions(bitmap3, {0, 1, 2});
}

// The view reads the caller's buffer instead of copying it.
TEST(MumblingBitmapTest, ViewDoesNotCopy) {
  Bytes bytes = BuildBitmap({{1, 2, 3}});
  ICEBERG_UNWRAP_OR_FAIL(auto bitmap, MumblingBitmapView::Make(bytes));
  EXPECT_FALSE(bitmap.Contains(4));
  bytes.back() = 4;  // the container {1, 2, 3} becomes {1, 2, 4}
  EXPECT_TRUE(bitmap.Contains(4));
  EXPECT_FALSE(bitmap.Contains(3));
}

// The fixtures in resources/mumbling were produced by the Java implementation
// (org.apache.iceberg.mumbling) and are copied from apache/iceberg-go#1750
// (mumbling/testdata at e305815).
TEST(PforDecodeTest, JavaFixtures) {
  auto fixtures = ReadFixtures("mumbling/pfor-java-fixtures.tsv", /*hex_column=*/1);
  ASSERT_EQ(fixtures.size(), 130);
  for (const auto& fixture : fixtures) {
    SCOPED_TRACE("line " + std::to_string(fixture.line));
    std::vector<uint8_t> expected(fixture.values.begin(), fixture.values.end());
    ICEBERG_UNWRAP_OR_FAIL(
        auto decoded, DecodePfor(fixture.bytes, expected.size(), fixture.bytes.size()));
    EXPECT_EQ(decoded, expected);
    // The test encoder chooses bit widths like the Java encoder, so the
    // benchmark inputs match what Java writes.
    EXPECT_EQ(EncodePfor(expected, kBestWidth), fixture.bytes);
  }
}

TEST(MumblingBitmapTest, JavaFixtures) {
  auto fixtures = ReadFixtures("mumbling/bitmap-java-fixtures.tsv", /*hex_column=*/0);
  ASSERT_EQ(fixtures.size(), 6);
  for (const auto& fixture : fixtures) {
    SCOPED_TRACE("line " + std::to_string(fixture.line));
    ICEBERG_UNWRAP_OR_FAIL(auto bitmap, MumblingBitmapView::Make(fixture.bytes));
    ExpectPositions(bitmap, fixture.values);
  }
}

}  // namespace iceberg
