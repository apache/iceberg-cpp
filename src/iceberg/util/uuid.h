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
#include "iceberg/util/formattable.h"

/// \file iceberg/util/uuid.h
/// \brief UUID (Universally Unique Identifier) representation.

namespace iceberg {

class ICEBERG_EXPORT Uuid : public util::Formattable {
 public:
  Uuid() = delete;
  constexpr static size_t kLength = 16;

  explicit Uuid(std::array<uint8_t, kLength> data);

  /// \brief Generate a random UUID (version 4).
  static Uuid GenerateV4();

  /// \brief Generate UUID version 7 per RFC 9562, with the current timestamp.
  static Uuid GenerateV7();

  /// \brief Generate UUID version 7 per RFC 9562, with the given timestamp.
  ///
  /// UUID version 7 consists of a Unix timestamp in milliseconds (48 bits) and
  /// 74 random bits, excluding the required version and variant bits.
  ///
  /// \param unix_ts_ms number of milliseconds since start of the UNIX epoch
  ///
  /// \note unix_ts_ms cannot be negative per RFC.
  static Uuid GenerateV7(uint64_t unix_ts_ms);

  /// \brief Create a UUID from a string in standard format.
  static Result<Uuid> FromString(std::string_view str);

  /// \brief Create a UUID from a 16-byte array.
  static Result<Uuid> FromBytes(std::span<const uint8_t> bytes);

  /// \brief Get the raw bytes of the UUID.
  std::span<const uint8_t> bytes() const { return data_; }

  /// \brief Access individual bytes of the UUID.
  /// \param index The index of the byte to access (0-15).
  /// \return The byte at the specified index.
  /// \throw IcebergError if index is out of bounds.
  uint8_t operator[](size_t index) const;

  /// \brief Convert the UUID to a string in standard format.
  std::string ToString() const override;

  friend bool operator==(const Uuid& lhs, const Uuid& rhs) {
    return lhs.data_ == rhs.data_;
  }

  int64_t high_bits() const;
  int64_t low_bits() const;

 private:
  std::array<uint8_t, kLength> data_;
};

}  // namespace iceberg
