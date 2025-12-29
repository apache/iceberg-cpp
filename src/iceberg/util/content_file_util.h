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

/// \file iceberg/util/content_file_util.h
/// Utility functions for content files (data files and delete files).

#include <memory>
#include <optional>
#include <span>
#include <string>
#include <unordered_set>

#include "iceberg/iceberg_export.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Utility functions for content files.
struct ICEBERG_EXPORT ContentFileUtil {
  /// \brief Check if a delete file is a deletion vector (DV).
  static bool IsDV(const DataFile& file);

  /// \brief Get the referenced data file path from a position delete file.
  static Result<std::optional<std::string>> ReferencedDataFile(const DataFile& file);

  /// \brief Check if a delete file is file-scoped.
  static Result<bool> IsFileScoped(const DataFile& file);

  /// \brief Check if a collection of delete files contains exactly one DV.
  static bool ContainsSingleDV(std::span<const std::shared_ptr<DataFile>> files);

  /// \brief Generate a description string for a deletion vector.
  static std::string DVDesc(const DataFile& file);

  /// \brief In-place drop stats.
  static void DropAllStats(DataFile& data_file);

  /// \brief Preserve stats based on selected columns.
  static void DropUnselectedStats(DataFile& data_file,
                                  const std::unordered_set<int32_t>& selected_columns);
};

}  // namespace iceberg
