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

/// \file iceberg/puffin/puffin_format.h
/// Puffin file format constants and utilities.

#include <array>
#include <cstdint>
#include <span>

#include "iceberg/iceberg_export.h"
#include "iceberg/puffin/file_metadata.h"
#include "iceberg/result.h"

namespace iceberg::puffin {

/// \brief Puffin file format constants.
struct ICEBERG_EXPORT PuffinFormat {
  /// Magic bytes: "PFA1" (Puffin Fratercula arctica, version 1)
  static constexpr std::array<uint8_t, 4> kMagicV1 = {0x50, 0x46, 0x41, 0x31};

  static constexpr int32_t kMagicLength = 4;
  static constexpr int32_t kFooterStartMagicOffset = 0;
  static constexpr int32_t kFooterStartMagicLength = kMagicLength;
  static constexpr int32_t kFooterStructPayloadSizeOffset = 0;
  static constexpr int32_t kFooterStructFlagsOffset = kFooterStructPayloadSizeOffset + 4;
  static constexpr int32_t kFooterStructFlagsLength = 4;
  static constexpr int32_t kFooterStructMagicOffset =
      kFooterStructFlagsOffset + kFooterStructFlagsLength;

  /// Total length of the footer struct: payload_size(4) + flags(4) + magic(4)
  static constexpr int32_t kFooterStructLength = kFooterStructMagicOffset + kMagicLength;

  /// Default compression codec for footer payload.
  static constexpr PuffinCompressionCodec kDefaultFooterCompressionCodec =
      PuffinCompressionCodec::kLz4;
};

/// \brief Footer flags for Puffin files.
enum class PuffinFlag : uint8_t {
  /// Whether the footer payload is compressed.
  kFooterPayloadCompressed = 0,
};

/// \brief Check if a flag is set in the flags bytes.
ICEBERG_EXPORT bool IsFlagSet(std::span<const uint8_t, 4> flags, PuffinFlag flag);

/// \brief Set a flag in the flags bytes.
ICEBERG_EXPORT void SetFlag(std::span<uint8_t, 4> flags, PuffinFlag flag);

/// \brief Read a 32-bit integer from a buffer at the given offset in little-endian
/// format.
ICEBERG_EXPORT Result<int32_t> ReadInt32LittleEndian(std::span<const uint8_t> data,
                                                     int32_t offset);

}  // namespace iceberg::puffin
