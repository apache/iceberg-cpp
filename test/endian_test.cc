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

#include "iceberg/util/endian.h"

#include <array>
#include <cmath>
#include <limits>

#include <gtest/gtest.h>

namespace iceberg {

// test round trip preserves value
TEST(EndianTest, RoundTripPreservesValue) {
  EXPECT_EQ(FromLittleEndian(ToLittleEndian<uint16_t>(0x1234)), 0x1234);
  EXPECT_EQ(FromBigEndian(ToBigEndian<uint32_t>(0xDEADBEEF)), 0xDEADBEEF);
  EXPECT_EQ(FromLittleEndian(ToLittleEndian(std::numeric_limits<uint64_t>::max())),
            std::numeric_limits<uint64_t>::max());
  EXPECT_EQ(FromBigEndian(ToBigEndian<uint32_t>(0)), 0);

  EXPECT_EQ(FromBigEndian(ToBigEndian<int16_t>(-1)), -1);
  EXPECT_EQ(FromLittleEndian(ToLittleEndian<int32_t>(-0x12345678)), -0x12345678);
  EXPECT_EQ(FromBigEndian(ToBigEndian(std::numeric_limits<int64_t>::min())),
            std::numeric_limits<int64_t>::min());
  EXPECT_EQ(FromLittleEndian(ToLittleEndian(std::numeric_limits<int16_t>::max())),
            std::numeric_limits<int16_t>::max());

  EXPECT_EQ(FromLittleEndian(ToLittleEndian(3.14f)), 3.14f);
  EXPECT_EQ(FromBigEndian(ToBigEndian(2.718281828459045)), 2.718281828459045);

  EXPECT_EQ(FromLittleEndian(ToLittleEndian(std::numeric_limits<float>::infinity())),
            std::numeric_limits<float>::infinity());
  EXPECT_EQ(FromBigEndian(ToBigEndian(-std::numeric_limits<float>::infinity())),
            -std::numeric_limits<float>::infinity());
  EXPECT_TRUE(std::isnan(
      FromLittleEndian(ToLittleEndian(std::numeric_limits<float>::quiet_NaN()))));
  EXPECT_EQ(FromBigEndian(ToBigEndian(0.0f)), 0.0f);
  EXPECT_EQ(FromLittleEndian(ToLittleEndian(-0.0f)), -0.0f);

  EXPECT_EQ(FromBigEndian(ToBigEndian(std::numeric_limits<double>::infinity())),
            std::numeric_limits<double>::infinity());
  EXPECT_EQ(FromLittleEndian(ToLittleEndian(-std::numeric_limits<double>::infinity())),
            -std::numeric_limits<double>::infinity());
  EXPECT_TRUE(
      std::isnan(FromBigEndian(ToBigEndian(std::numeric_limits<double>::quiet_NaN()))));
  EXPECT_EQ(FromLittleEndian(ToLittleEndian(0.0)), 0.0);
  EXPECT_EQ(FromBigEndian(ToBigEndian(-0.0)), -0.0);
}

// test constexpr evaluation
TEST(EndianTest, ConstexprEvaluation) {
  static_assert(FromBigEndian(ToBigEndian<uint16_t>(0x1234)) == 0x1234);
  static_assert(FromLittleEndian(ToLittleEndian<uint32_t>(0x12345678)) == 0x12345678);
  static_assert(FromBigEndian(ToBigEndian<int64_t>(-1)) == -1);

  static_assert(ToBigEndian<uint8_t>(0xFF) == 0xFF);
  static_assert(FromLittleEndian<int8_t>(-1) == -1);

  static_assert(FromLittleEndian(ToLittleEndian(3.14f)) == 3.14f);
  static_assert(FromBigEndian(ToBigEndian(2.718)) == 2.718);
}

// test platform dependent behavior
TEST(EndianTest, PlatformDependentBehavior) {
  uint32_t test_value = 0x12345678;

  if constexpr (std::endian::native == std::endian::little) {
    EXPECT_EQ(ToLittleEndian(test_value), test_value);
    EXPECT_EQ(FromLittleEndian(test_value), test_value);
    EXPECT_NE(ToBigEndian(test_value), test_value);
  } else if constexpr (std::endian::native == std::endian::big) {
    EXPECT_EQ(ToBigEndian(test_value), test_value);
    EXPECT_EQ(FromBigEndian(test_value), test_value);
    EXPECT_NE(ToLittleEndian(test_value), test_value);
  }

  EXPECT_EQ(ToLittleEndian<uint8_t>(0xAB), 0xAB);
  EXPECT_EQ(ToBigEndian<uint8_t>(0xAB), 0xAB);
}

// test specific byte pattern validation
TEST(EndianTest, SpecificBytePatternValidation) {
  uint32_t original_int = 0x12345678;
  uint32_t little_endian_int = ToLittleEndian(original_int);
  uint32_t big_endian_int = ToBigEndian(original_int);

  auto little_int_bytes = std::bit_cast<std::array<uint8_t, 4>>(little_endian_int);
  auto big_int_bytes = std::bit_cast<std::array<uint8_t, 4>>(big_endian_int);

  EXPECT_EQ(little_int_bytes, (std::array<uint8_t, 4>{0x78, 0x56, 0x34, 0x12}));
  EXPECT_EQ(big_int_bytes, (std::array<uint8_t, 4>{0x12, 0x34, 0x56, 0x78}));

  float original_float = 3.14f;
  float little_endian_float = ToLittleEndian(original_float);
  float big_endian_float = ToBigEndian(original_float);

  auto little_float_bytes = std::bit_cast<std::array<uint8_t, 4>>(little_endian_float);
  auto big_float_bytes = std::bit_cast<std::array<uint8_t, 4>>(big_endian_float);

  EXPECT_EQ(little_float_bytes, (std::array<uint8_t, 4>{0xC3, 0xF5, 0x48, 0x40}));
  EXPECT_EQ(big_float_bytes, (std::array<uint8_t, 4>{0x40, 0x48, 0xF5, 0xC3}));
}

}  // namespace iceberg
