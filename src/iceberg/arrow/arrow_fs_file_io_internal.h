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

#include <memory>

#include <arrow/filesystem/filesystem.h>

#include "iceberg/file_io.h"
#include "iceberg/iceberg_bundle_export.h"

namespace iceberg::arrow {

/// \brief A concrete implementation of FileIO for Arrow file system.
class ICEBERG_BUNDLE_EXPORT ArrowFileSystemFileIO : public FileIO {
 public:
  explicit ArrowFileSystemFileIO(std::shared_ptr<::arrow::fs::FileSystem> arrow_fs)
      : arrow_fs_(std::move(arrow_fs)) {}

  /// \brief Make an in-memory FileIO backed by arrow::fs::internal::MockFileSystem.
  static std::unique_ptr<::arrow::fs::FileSystem> MakeMockFileIO();

  /// \brief Make a local FileIO backed by arrow::fs::LocalFileSystem.
  static std::unique_ptr<::arrow::fs::FileSystem> MakeLocalFileIO();

  ~ArrowFileSystemFileIO() override = default;

  /// \brief Read the content of the file at the given location.
  Result<std::string> ReadFile(const std::string& file_location,
                               std::optional<size_t> length) override;

  /// \brief Write the given content to the file at the given location.
  Status WriteFile(const std::string& file_location, std::string_view content) override;

  /// \brief Delete a file at the given location.
  Status DeleteFile(const std::string& file_location) override;

  /// \brief Get the Arrow file system.
  const std::shared_ptr<::arrow::fs::FileSystem>& fs() const { return arrow_fs_; }

 private:
  std::shared_ptr<::arrow::fs::FileSystem> arrow_fs_;
};

}  // namespace iceberg::arrow
