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

#include "iceberg/deletes/roaring_position_bitmap.h"
#include "iceberg/iceberg_data_export.h"

namespace iceberg {

/// \brief Tracks deleted row positions using a bitmap.
///
/// This class provides a domain-specific API for position deletes
/// in Iceberg MOR (merge-on-read) tables. Positions are 0-based
/// row indices within a data file.
class ICEBERG_DATA_EXPORT PositionDeleteIndex {
 public:
  PositionDeleteIndex() = default;
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

 private:
  RoaringPositionBitmap bitmap_;
};

}  // namespace iceberg
