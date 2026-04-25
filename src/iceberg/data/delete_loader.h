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

/// \file iceberg/data/delete_loader.h
/// Loads position and equality delete files into in-memory indexes.

#include <memory>
#include <span>
#include <string_view>

#include "iceberg/iceberg_data_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Loads delete files and constructs in-memory delete indexes.
class ICEBERG_DATA_EXPORT DeleteLoader {
 public:
  /// \brief Create a DeleteLoader.
  /// \param io FileIO instance for reading delete files
  explicit DeleteLoader(std::shared_ptr<FileIO> io);

  ~DeleteLoader();

  /// \brief Load position deletes for a specific data file.
  ///
  /// Reads the given position delete files and returns a PositionDeleteIndex
  /// containing only the positions that apply to the specified data file path.
  /// Supports both regular position delete files and deletion vectors.
  ///
  /// \param delete_files Position delete files to load (must have
  ///   content == Content::kPositionDeletes)
  /// \param data_file_path Path of the data file to filter positions for
  /// \return A PositionDeleteIndex with deleted positions for the data file
  Result<PositionDeleteIndex> LoadPositionDeletes(
      std::span<const std::shared_ptr<DataFile>> delete_files,
      std::string_view data_file_path) const;

  /// \brief Load equality deletes into an in-memory set.
  ///
  /// \param delete_files Equality delete files to load (must have
  ///   content == Content::kEqualityDeletes)
  /// \param equality_type The struct type describing the equality columns
  /// \return A StructLikeSet containing the deleted rows
  Result<std::unique_ptr<UncheckedStructLikeSet>> LoadEqualityDeletes(
      std::span<const std::shared_ptr<DataFile>> delete_files,
      const StructType& equality_type) const;

 private:
  /// \brief Load a single position delete file into the index.
  Status LoadPositionDelete(const DataFile& file, PositionDeleteIndex& index,
                            std::string_view data_file_path) const;

  /// \brief Load a single deletion vector file into the index.
  Status LoadDV(const DataFile& file, PositionDeleteIndex& index) const;

  std::shared_ptr<FileIO> io_;
};

}  // namespace iceberg
