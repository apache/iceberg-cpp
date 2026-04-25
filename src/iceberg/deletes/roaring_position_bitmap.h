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

/// \file iceberg/deletes/roaring_position_bitmap.h
/// A 64-bit position bitmap using an array of 32-bit Roaring bitmaps.

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

#include "iceberg/iceberg_data_export.h"
#include "iceberg/result.h"

namespace iceberg {

/// \brief A bitmap that supports positive 64-bit positions, optimized
/// for cases where most positions fit in 32 bits.
///
/// Incoming 64-bit positions are divided into a 32-bit "key" using the
/// most significant 4 bytes and a 32-bit position using the least
/// significant 4 bytes. For each key, a 32-bit Roaring bitmap is
/// maintained to store positions for that key.
///

/// \note This class is used to represent deletion vectors.  The Puffin puffin
/// reader/write handle adding the additional required framing (length prefix, magic,
/// magic bytes, CRC-32) for  `deletion-vector-v1` persistence.
class ICEBERG_DATA_EXPORT RoaringPositionBitmap {
 public:
  /// \brief Maximum supported position (aligned with the Java implementation).
  static constexpr int64_t kMaxPosition = 0x7FFFFFFE80000000LL;

  RoaringPositionBitmap();
  ~RoaringPositionBitmap();

  RoaringPositionBitmap(RoaringPositionBitmap&& other) noexcept;
  RoaringPositionBitmap& operator=(RoaringPositionBitmap&& other) noexcept;

  RoaringPositionBitmap(const RoaringPositionBitmap& other);
  RoaringPositionBitmap& operator=(const RoaringPositionBitmap& other);

  /// \brief Sets a position in the bitmap.
  /// \param pos the position (must be >= 0 and <= kMaxPosition)
  /// \note Invalid positions are silently ignored
  void Add(int64_t pos);

  /// \brief Sets a range of positions [pos_start, pos_end).
  /// \param pos_start the start of the range (inclusive), clamped to 0
  /// \param pos_end the end of the range (exclusive), clamped to kMaxPosition + 1
  /// \note If pos_start > pos_end, the call is silently ignored.
  ///       If pos_start == pos_end, this method does nothing.
  ///       Positions outside [0, kMaxPosition] are silently ignored.
  void AddRange(int64_t pos_start, int64_t pos_end);

  /// \brief Checks if a position is set in the bitmap.
  /// \param pos the position to check
  /// \return true if the position is set, false otherwise (including invalid positions)
  bool Contains(int64_t pos) const;

  /// \brief Returns true if the bitmap has no positions set.
  bool IsEmpty() const;

  /// \brief Returns the number of set positions in the bitmap.
  size_t Cardinality() const;

  /// \brief Merges all positions from the other bitmap into this one
  /// (in-place union).
  void Or(const RoaringPositionBitmap& other);

  /// \brief Optimizes the bitmap by applying run-length encoding to
  /// containers where it is more space efficient than array or bitset
  /// representations.
  /// \return true if any container was changed
  bool Optimize();

  /// \brief Iterates over all set positions in ascending order.
  void ForEach(const std::function<void(int64_t)>& fn) const;

  /// \brief Returns the serialized size in bytes.
  size_t SerializedSizeInBytes() const;

  /// \brief Serializes using the portable format (little-endian).
  Result<std::string> Serialize() const;

  /// \brief Deserializes a bitmap from bytes.
  static Result<RoaringPositionBitmap> Deserialize(std::string_view bytes);

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;

  explicit RoaringPositionBitmap(std::unique_ptr<Impl> impl);
};

}  // namespace iceberg
