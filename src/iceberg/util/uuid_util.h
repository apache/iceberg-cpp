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

#include <array>
#include <cstdint>
#include <span>
#include <string_view>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"

/// \file iceberg/util/uuid_util.h
/// \brief UUID (Universally Unique Identifier) utilities.

namespace iceberg {

class ICEBERG_EXPORT UUIDUtils {
 public:
  /// \brief Generate a random UUID (version 4).
  static std::array<uint8_t, 16> GenerateUuidV4();

  /// \brief Generate UUID version 7 per RFC 9562, with the current timestamp.
  static std::array<uint8_t, 16> GenerateUuidV7();

  /// \brief Generate UUID version 7 per RFC 9562, with the given timestamp.
  ///
  /// UUID version 7 consists of a Unix timestamp in milliseconds (48 bits) and
  /// 74 random bits, excluding the required version and variant bits.
  ///
  /// \param unix_ts_ms number of milliseconds since start of the UNIX epoch
  ///
  /// \note unix_ts_ms cannot be negative per RFC.
  static std::array<uint8_t, 16> GenerateUuidV7(uint64_t unix_ts_ms);

  /// \brief Create a UUID from a string in standard format.
  static Result<std::array<uint8_t, 16>> FromString(std::string_view str);

  /// \brief Convert a UUID to a string in standard format.
  static std::string ToString(std::span<uint8_t> uuid);
};

}  // namespace iceberg
