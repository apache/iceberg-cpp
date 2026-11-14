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

/// \file iceberg/util/mumbling_bitmap_internal.h
/// A read-only Mumbling bitmap, the compressed bitmap format used by v4
/// manifest deletion vectors.
///
/// Spec: https://github.com/apache/iceberg/blob/main/format/mumbling-spec.md
/// The spec is still a draft, so this header is internal and not installed.

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"

namespace iceberg {

/// \brief Decodes a PFOR-encoded array of unsigned bytes.
///
/// Implements "Appendix A: PFOR encoding for unsigned bytes" of the Mumbling
/// bitmap spec
/// (https://github.com/apache/iceberg/blob/main/format/mumbling-spec.md#appendix-a-pfor-encoding-for-unsigned-bytes),
/// which encodes the descriptor array. Values are encoded in
/// chunks of 256 values (the last chunk holds the remainder), each with a
/// 3-byte header, a bit-packed primary array, exception offsets and
/// bit-packed exception values.
///
/// \param data the encoded bytes; may extend past the encoded values.
/// \param out receives the decoded values; its size is the number of values.
/// \return the number of bytes read from `data`, or an error if the input is
/// malformed or truncated.
ICEBERG_EXPORT Result<size_t> DecodePforBytes(std::span<const uint8_t> data,
                                              std::span<uint8_t> out);

/// \brief A read-only view of a serialized Mumbling bitmap (format version 1).
///
/// Positions are split into a container index (the most significant bits) and
/// an 8-bit position within a 256-bit container. Each container is either
/// sparse (0-31 sorted one-byte positions) or dense (a 32-byte bit set).
///
/// The view does not copy the bitmap: the serialized bytes must outlive it,
/// like the characters of a `std::string_view`. In v4 scan planning these are
/// the bytes of `manifest_info.dv` owned by the root manifest entry.
///
/// Make() checks everything that memory safety depends on (header limits,
/// the PFOR descriptor array, descriptor types and the container sizes against
/// the buffer), so the accessors cannot fail and never read out of bounds.
/// Like the Java reader, it does not check container contents: for input that
/// breaks the spec's content rules (unsorted sparse positions, dense
/// containers with fewer than 32 positions, a wrong header cardinality),
/// Contains() still reports whether the position is stored, and Cardinality()
/// returns the header value.
class ICEBERG_EXPORT MumblingBitmapView {
 public:
  /// \brief The only supported format version.
  static constexpr uint8_t kVersion = 1;
  /// \brief Maximum number of containers in a bitmap.
  static constexpr int32_t kMaxContainerCount = 8192;
  /// \brief Number of positions covered by one container.
  static constexpr int32_t kContainerSize = 256;
  /// \brief Maximum number of set positions (8192 containers of 256 bits).
  static constexpr int32_t kMaxCardinality = kMaxContainerCount * kContainerSize;

  /// \brief Creates a view of a serialized bitmap. `bytes` must outlive the
  /// view and must hold exactly one bitmap.
  static Result<MumblingBitmapView> Make(std::span<const uint8_t> bytes);

  /// \brief Returns true if `pos` is set. Positions that are negative or past
  /// the last container are never set.
  ///
  /// Inline so the common cases (out of range, empty or dense container) cost
  /// a few instructions in the caller's loop; only sparse containers are
  /// searched out of line.
  bool Contains(int64_t pos) const {
    if (pos < 0 || pos >= static_cast<int64_t>(containers_.size()) * kContainerSize) {
      return false;
    }
    const uint32_t ref = containers_[static_cast<size_t>(pos / kContainerSize)];
    const auto length = static_cast<uint8_t>(ref & kLengthMask);
    if (length == 0) {
      return false;
    }
    const auto pos_in_container = static_cast<uint8_t>(pos % kContainerSize);
    if (length == kDenseLength) {
      const uint8_t byte = containers_data_[(ref >> kOffsetShift) + pos_in_container / 8];
      return ((byte >> (7 - pos_in_container % 8)) & 1) != 0;
    }
    return SparseContains(ref, pos_in_container);
  }

  /// \brief Returns the number of set positions, as stored in the header.
  int64_t Cardinality() const { return cardinality_; }

 private:
  // Each entry of `containers_` packs a container's byte offset in
  // `containers_data_` (upper bits) with its length in bytes (low 8 bits):
  // 0-31 for a sparse container, or kDenseLength for a dense one.
  static constexpr uint32_t kOffsetShift = 8;
  static constexpr uint32_t kLengthMask = 0xFF;
  static constexpr uint8_t kDenseLength = 32;

  MumblingBitmapView() = default;

  // Searches the sparse container described by `ref`.
  bool SparseContains(uint32_t ref, uint8_t pos_in_container) const;

  std::vector<uint32_t> containers_;
  // The container section of the caller's buffer (not owned).
  std::span<const uint8_t> containers_data_;
  int64_t cardinality_ = 0;
};

}  // namespace iceberg
