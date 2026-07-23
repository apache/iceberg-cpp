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

/// \file iceberg/deletes/position_delete_index.h
/// Index of deleted row positions for a data file.

#include <cstdint>
#include <memory>
#include <span>
#include <vector>

#include "iceberg/deletes/roaring_position_bitmap.h"
#include "iceberg/iceberg_data_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Tracks deleted row positions using a bitmap.
///
/// This class provides a domain-specific API for position deletes
/// in Iceberg MOR (merge-on-read) tables. Positions are 0-based
/// row indices within a data file.
class ICEBERG_DATA_EXPORT PositionDeleteIndex {
 public:
  PositionDeleteIndex() = default;
  explicit PositionDeleteIndex(std::shared_ptr<DataFile> delete_file);
  explicit PositionDeleteIndex(std::vector<std::shared_ptr<DataFile>> delete_files);
  ~PositionDeleteIndex() = default;

  /// \brief Mark a position as deleted.
  /// \param pos The 0-based row position to delete
  void Delete(int64_t pos);

  /// \brief Mark a range of positions as deleted [pos_start, pos_end).
  /// \param pos_start Start position (inclusive)
  /// \param pos_end End position (exclusive)
  void Delete(int64_t pos_start, int64_t pos_end);

  /// \brief Check if a position is deleted.
  /// \param pos The 0-based row position to check
  /// \return true if the position is deleted, false otherwise
  bool IsDeleted(int64_t pos) const;

  /// \brief Check if the index is empty (no positions deleted).
  bool IsEmpty() const;

  /// \brief Get the number of deleted positions.
  int64_t Cardinality() const;

  /// \brief Merge another index into this one.
  /// \param other The index to merge (union operation)
  void Merge(const PositionDeleteIndex& other);

  /// \brief The delete files whose positions were merged into this index.
  ///
  /// Populated by constructors and Deserialize, and preserved across Merge.
  /// Callers use these to report the delete files that were rewritten when
  /// replacing them with a new deletion vector.
  const std::vector<std::shared_ptr<DataFile>>& delete_files() const {
    return delete_files_;
  }

  /// \brief Serialize the index into a `deletion-vector-v1` blob.
  ///
  /// The positions are run-length encoded, then framed per the Puffin spec:
  /// https://iceberg.apache.org/puffin-spec/#deletion-vector-v1-blob-type
  Result<std::vector<uint8_t>> Serialize();

  /// \brief Deserialize a `deletion-vector-v1` blob into an index.
  ///
  /// Validates the blob framing (length prefix, magic sequence, CRC-32) and,
  /// against the source delete file, that the blob length matches
  /// `content_size_in_bytes` and the bitmap cardinality matches `record_count`.
  /// The source delete file is retained and exposed via delete_files().
  static Result<PositionDeleteIndex> Deserialize(std::span<const uint8_t> blob,
                                                 std::shared_ptr<DataFile> delete_file);

 private:
  explicit PositionDeleteIndex(RoaringPositionBitmap bitmap);

  // Bulk-add positions sharing high-32-bit `key`. Private hook for
  // `ForEachPositionDelete`'s bulk path; keeps `Delete` the sole public
  // mutation surface.
  void BulkAddForKey(int32_t key, std::span<const uint32_t> positions);

  friend void ICEBERG_DATA_EXPORT
  ForEachPositionDelete(std::span<const int64_t> positions, PositionDeleteIndex& target,
                        std::vector<uint32_t>& scratch);

  RoaringPositionBitmap bitmap_;
  std::vector<std::shared_ptr<DataFile>> delete_files_;
};

}  // namespace iceberg
