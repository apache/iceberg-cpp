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

#define EXPECT_ROUNDTRIP(value)                                \
  do {                                                         \
    EXPECT_EQ(FromLittleEndian(ToLittleEndian(value)), value); \
    EXPECT_EQ(FromBigEndian(ToBigEndian(value)), value);       \
  } while (false)

TEST(EndianTest, RoundTripPreservesValue) {
  EXPECT_ROUNDTRIP(static_cast<uint16_t>(0x1234));
  EXPECT_ROUNDTRIP(static_cast<uint32_t>(0xDEADBEEF));
  EXPECT_ROUNDTRIP(std::numeric_limits<uint64_t>::max());
  EXPECT_ROUNDTRIP(static_cast<uint32_t>(0));

  EXPECT_ROUNDTRIP(static_cast<int16_t>(-1));
  EXPECT_ROUNDTRIP(static_cast<int32_t>(-0x12345678));
  EXPECT_ROUNDTRIP(std::numeric_limits<int64_t>::min());
  EXPECT_ROUNDTRIP(std::numeric_limits<int16_t>::max());

  EXPECT_ROUNDTRIP(3.14f);
  EXPECT_ROUNDTRIP(2.718281828459045);
  EXPECT_ROUNDTRIP(0.0f);
  EXPECT_ROUNDTRIP(-0.0f);
  EXPECT_ROUNDTRIP(0.0);
  EXPECT_ROUNDTRIP(-0.0);

  EXPECT_ROUNDTRIP(std::numeric_limits<float>::infinity());
  EXPECT_ROUNDTRIP(-std::numeric_limits<float>::infinity());
  EXPECT_ROUNDTRIP(std::numeric_limits<double>::infinity());
  EXPECT_ROUNDTRIP(-std::numeric_limits<double>::infinity());

  EXPECT_TRUE(std::isnan(
      FromLittleEndian(ToLittleEndian(std::numeric_limits<float>::quiet_NaN()))));
  EXPECT_TRUE(
      std::isnan(FromBigEndian(ToBigEndian(std::numeric_limits<double>::quiet_NaN()))));
}

TEST(EndianTest, ByteWiseValidation) {
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
