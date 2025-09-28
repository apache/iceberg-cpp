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

#include "iceberg/util/uuid.h"

#include <chrono>
#include <cstdint>
#include <cstring>
#include <random>
#include <string>

#include "iceberg/exception.h"
#include "iceberg/result.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep
#include "iceberg/util/int128.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

constexpr std::array<uint8_t, 256> BuildHexTable() {
  std::array<uint8_t, 256> buf{};
  for (int32_t i = 0; i < 256; i++) {
    if (i >= '0' && i <= '9') {
      buf[i] = static_cast<uint8_t>(i - '0');
    } else if (i >= 'a' && i <= 'f') {
      buf[i] = static_cast<uint8_t>(i - 'a' + 10);
    } else if (i >= 'A' && i <= 'F') {
      buf[i] = static_cast<uint8_t>(i - 'A' + 10);
    } else {
      buf[i] = 0xFF;
    }
  }
  return buf;
}

constexpr std::array<uint8_t, 256> BuildShl4Table() {
  std::array<uint8_t, 256> buf{};
  for (int32_t i = 0; i < 256; i++) {
    buf[i] = static_cast<uint8_t>(i << 4);
  }
  return buf;
}

constexpr auto kHexTable = BuildHexTable();
constexpr auto kShl4Table = BuildShl4Table();

// Parse a UUID string without dashes, e.g. "67e5504410b1426f9247bb680e5fe0c8"
inline Result<Uuid> ParseSimple(std::string_view s) {
  ICEBERG_DCHECK(s.size() == 32, "s must be 32 characters long");

  std::array<uint8_t, 16> uuid{};
  for (size_t i = 0; i < 16; i++) {
    uint8_t h1 = kHexTable[static_cast<uint8_t>(s[i * 2])];
    uint8_t h2 = kHexTable[static_cast<uint8_t>(s[i * 2 + 1])];

    if ((h1 | h2) == 0xFF) [[unlikely]] {
      return InvalidArgument("Invalid UUID string: {}", s);
    }

    uuid[i] = static_cast<uint8_t>(kShl4Table[h1] | h2);
  }
  return Uuid(std::move(uuid));
}

// Parse a UUID string with dashes, e.g. "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
inline Result<Uuid> ParseHyphenated(std::string_view s) {
  ICEBERG_DCHECK(s.size() == 36, "s must be 36 characters long");

  // Check that dashes are in the right places
  if (!(s[8] == '-' && s[13] == '-' && s[18] == '-' && s[23] == '-')) [[unlikely]] {
    return InvalidArgument("Invalid UUID string: {}", s);
  }

  constexpr std::array<size_t, 8> positions = {0, 4, 9, 14, 19, 24, 28, 32};
  std::array<uint8_t, 16> uuid{};

  for (size_t j = 0; j < 8; j++) {
    size_t i = positions[j];
    uint8_t h1 = kHexTable[static_cast<uint8_t>(s[i])];
    uint8_t h2 = kHexTable[static_cast<uint8_t>(s[i + 1])];
    uint8_t h3 = kHexTable[static_cast<uint8_t>(s[i + 2])];
    uint8_t h4 = kHexTable[static_cast<uint8_t>(s[i + 3])];

    if ((h1 | h2 | h3 | h4) == 0xFF) [[unlikely]] {
      return InvalidArgument("Invalid UUID string: {}", s);
    }

    uuid[j * 2] = static_cast<uint8_t>(kShl4Table[h1] | h2);
    uuid[j * 2 + 1] = static_cast<uint8_t>(kShl4Table[h3] | h4);
  }

  return Uuid(std::move(uuid));
}

}  // namespace

Uuid::Uuid(std::array<uint8_t, kLength> data) : data_(std::move(data)) {}

Uuid Uuid::GenerateV4() {
  static std::random_device rd;
  static std::mt19937 gen(rd());
  static std::uniform_int_distribution<uint64_t> distrib(
      std::numeric_limits<uint64_t>::min(), std::numeric_limits<uint64_t>::max());
  std::array<uint8_t, 16> uuid;

  // Generate two random 64-bit integers
  uint64_t high_bits = distrib(gen);
  uint64_t low_bits = distrib(gen);

  // Combine them into a uint128_t
  uint128_t random_128_bit_number = (static_cast<uint128_t>(high_bits) << 64) | low_bits;

  // Copy the bytes into the uuid array
  std::memcpy(uuid.data(), &random_128_bit_number, 16);

  // Set magic numbers for a "version 4" (pseudorandom) UUID and variant,
  // see https://datatracker.ietf.org/doc/html/rfc9562#name-uuid-version-4
  uuid[6] = (uuid[6] & 0x0F) | 0x40;
  // Set variant field, top two bits are 1, 0
  uuid[8] = (uuid[8] & 0x3F) | 0x80;

  return Uuid(std::move(uuid));
}

Uuid Uuid::GenerateV7() {
  // Get the current time in milliseconds since the Unix epoch
  auto now = std::chrono::system_clock::now();
  auto duration_since_epoch = now.time_since_epoch();
  auto unix_ts_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(duration_since_epoch).count();

  return GenerateV7(static_cast<uint64_t>(unix_ts_ms));
}

Uuid Uuid::GenerateV7(uint64_t unix_ts_ms) {
  std::array<uint8_t, 16> uuid = {};

  // Set the timestamp (in milliseconds since Unix epoch)
  uuid[0] = (unix_ts_ms >> 40) & 0xFF;
  uuid[1] = (unix_ts_ms >> 32) & 0xFF;
  uuid[2] = (unix_ts_ms >> 24) & 0xFF;
  uuid[3] = (unix_ts_ms >> 16) & 0xFF;
  uuid[4] = (unix_ts_ms >> 8) & 0xFF;
  uuid[5] = unix_ts_ms & 0xFF;

  // Generate random bytes for the remaining fields
  static std::random_device rd;
  static std::mt19937 gen(rd());
  static std::uniform_int_distribution<uint16_t> distrib(
      std::numeric_limits<uint16_t>::min(), std::numeric_limits<uint16_t>::max());

  // Note: uint8_t is invalid for uniform_int_distribution on Windows
  for (size_t i = 6; i < 16; i += 2) {
    auto rand = static_cast<uint16_t>(distrib(gen));
    uuid[i] = (rand >> 8) & 0xFF;
    uuid[i + 1] = rand & 0xFF;
  }

  // Set magic numbers for a "version 7" (pseudorandom) UUID and variant,
  // see https://www.rfc-editor.org/rfc/rfc9562#name-version-field
  uuid[6] = (uuid[6] & 0x0F) | 0x70;
  // set variant field, top two bits are 1, 0
  uuid[8] = (uuid[8] & 0x3F) | 0x80;

  return Uuid(std::move(uuid));
}

Result<Uuid> Uuid::FromString(std::string_view str) {
  if (str.size() == 32) {
    return ParseSimple(str);
  } else if (str.size() == 36) {
    return ParseHyphenated(str);
  } else {
    return InvalidArgument("Invalid UUID string: {}", str);
  }
}

Result<Uuid> Uuid::FromBytes(std::span<const uint8_t> bytes) {
  if (bytes.size() != kLength) [[unlikely]] {
    return InvalidArgument("UUID byte array must be exactly {} bytes, was {}", kLength,
                           bytes.size());
  }
  std::array<uint8_t, kLength> data;
  std::memcpy(data.data(), bytes.data(), kLength);
  return Uuid(std::move(data));
}

uint8_t Uuid::operator[](size_t index) const {
  ICEBERG_CHECK(index < kLength, "UUID index out of range: {}", index);
  return data_[index];
}

std::string Uuid::ToString() const {
  return std::format(
      "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}"
      "{:02x}{:02x}{:02x}",
      data_[0], data_[1], data_[2], data_[3], data_[4], data_[5], data_[6], data_[7],
      data_[8], data_[9], data_[10], data_[11], data_[12], data_[13], data_[14],
      data_[15]);
}

}  // namespace iceberg
