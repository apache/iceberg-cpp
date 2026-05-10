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

#include <cstddef>
#include <memory>
#include <optional>
#include <string>

#include <arrow/filesystem/type_fwd.h>
#include <arrow/io/type_fwd.h>

#include "iceberg/file_io.h"
#include "iceberg/iceberg_bundle_export.h"

namespace iceberg::arrow {

/// \brief Open a FileIO input as an Arrow input stream.
///
/// Uses ArrowFileSystemFileIO's native Arrow stream directly when possible and falls
/// back to a FileIO stream adapter otherwise. The fallback requires FileIO to
/// implement NewInputFile.
ICEBERG_BUNDLE_EXPORT Result<std::shared_ptr<::arrow::io::RandomAccessFile>>
OpenArrowInputStream(const std::shared_ptr<FileIO>& io, const std::string& path,
                     std::optional<size_t> length = std::nullopt);

/// \brief Open a FileIO output as an Arrow output stream.
///
/// Uses ArrowFileSystemFileIO's native Arrow stream directly when possible and falls
/// back to a FileIO stream adapter otherwise. The fallback requires FileIO to
/// implement NewOutputFile.
ICEBERG_BUNDLE_EXPORT Result<std::shared_ptr<::arrow::io::OutputStream>>
OpenArrowOutputStream(const std::shared_ptr<FileIO>& io, const std::string& path,
                      bool overwrite = true);

/// \brief A concrete implementation of FileIO for Arrow file system.
class ICEBERG_BUNDLE_EXPORT ArrowFileSystemFileIO : public FileIO {
 public:
  explicit ArrowFileSystemFileIO(std::shared_ptr<::arrow::fs::FileSystem> arrow_fs)
      : arrow_fs_(std::move(arrow_fs)) {}

  /// \brief Make an in-memory FileIO backed by arrow::fs::internal::MockFileSystem.
  static std::unique_ptr<FileIO> MakeMockFileIO();

  /// \brief Make a local FileIO backed by arrow::fs::LocalFileSystem.
  static std::unique_ptr<FileIO> MakeLocalFileIO();

  ~ArrowFileSystemFileIO() override = default;

  /// \brief Create an input file handle for the given location.
  Result<std::unique_ptr<InputFile>> NewInputFile(std::string file_location) override;

  /// \brief Create an input file handle for the given location with a known length.
  Result<std::unique_ptr<InputFile>> NewInputFile(std::string file_location,
                                                  size_t length) override;

  /// \brief Create an output file handle for the given location.
  Result<std::unique_ptr<OutputFile>> NewOutputFile(std::string file_location) override;

  /// \brief Delete a file at the given location.
  Status DeleteFile(const std::string& file_location) override;

  /// \brief Get the Arrow file system.
  const std::shared_ptr<::arrow::fs::FileSystem>& fs() const { return arrow_fs_; }

 private:
  friend Result<std::shared_ptr<::arrow::io::RandomAccessFile>> OpenArrowInputStream(
      const std::shared_ptr<FileIO>& io, const std::string& path,
      std::optional<size_t> length);

  friend Result<std::shared_ptr<::arrow::io::OutputStream>> OpenArrowOutputStream(
      const std::shared_ptr<FileIO>& io, const std::string& path, bool overwrite);

  /// \brief Resolve a file location to a filesystem path.
  Result<std::string> ResolvePath(const std::string& file_location);

  std::shared_ptr<::arrow::fs::FileSystem> arrow_fs_;
};

}  // namespace iceberg::arrow
