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

#include <optional>
#include <string>
#include <string_view>

#include "iceberg/error.h"
#include "iceberg/expected.h"
#include "iceberg/iceberg_export.h"

namespace iceberg {

/// \brief Pluggable module for reading, writing, and deleting metadata files.
///
/// This module only handle metadata files, not data files. The metadata files
/// are typically small and are used to store schema, partition information,
/// and other metadata about the table.
class ICEBERG_EXPORT FileIO {
 public:
  FileIO() = default;
  virtual ~FileIO() = default;

  /// \brief Read the content of the file at the given location.
  ///
  /// \param file_location The location of the file to read.
  /// \param length The number of bytes to read. Some object storage need to specify
  /// the length to read, e.g. S3 `GetObject` has a Range parameter.
  /// \return The content of the file if the read succeeded, an error code if the read
  /// failed.
  virtual expected<std::string, Error> ReadFile(const std::string& file_location,
                                                std::optional<size_t> length) = 0;

  /// \brief Write the given content to the file at the given location.
  ///
  /// \param file_location The location of the file to write.
  /// \param content The content to write to the file.
  /// \param overwrite If true, overwrite the file if it exists. If false, fail if the
  /// file exists.
  /// \return void if the write succeeded, an error code if the write failed.
  virtual expected<void, Error> WriteFile(const std::string& file_location,
                                          std::string_view content, bool overwrite) = 0;

  /// \brief Delete a file at the given location.
  ///
  /// \param file_location The location of the file to delete.
  /// \return void if the delete succeeded, an error code if the delete failed.
  virtual expected<void, Error> DeleteFile(const std::string& file_location) = 0;
};

}  // namespace iceberg
