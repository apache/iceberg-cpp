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
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"

namespace iceberg {

/// \brief Seekable byte stream for reading file contents.
class ICEBERG_EXPORT SeekableInputStream {
 public:
  virtual ~SeekableInputStream() = default;

  /// \brief Return the current read position.
  virtual Result<int64_t> Position() const = 0;

  /// \brief Seek to an absolute byte position.
  virtual Status Seek(int64_t position) = 0;

  /// \brief Read up to out.size() bytes from the current position.
  virtual Result<int64_t> Read(std::span<std::byte> out) = 0;

  /// \brief Read exactly out.size() bytes from an absolute position.
  ///
  /// Fails if fewer than out.size() bytes are available. The current stream position
  /// after this call is unspecified; callers should Seek before subsequent
  /// position-dependent reads.
  virtual Status ReadFully(int64_t position, std::span<std::byte> out) = 0;

  /// \brief Close the stream. Implementations should allow repeated Close calls.
  virtual Status Close() = 0;
};

/// \brief Positioned byte stream for writing file contents.
class ICEBERG_EXPORT PositionOutputStream {
 public:
  virtual ~PositionOutputStream() = default;

  /// \brief Return the current write position.
  virtual Result<int64_t> Position() const = 0;

  /// \brief Write all bytes in data at the current position.
  virtual Status Write(std::span<const std::byte> data) = 0;

  /// \brief Flush buffered data to the underlying store.
  virtual Status Flush() = 0;

  /// \brief Close the stream. Implementations should allow repeated Close calls.
  virtual Status Close() = 0;
};

/// \brief Handle for opening a readable file.
class ICEBERG_EXPORT InputFile {
 public:
  virtual ~InputFile() = default;

  /// \brief File location represented by this handle.
  virtual std::string_view location() const = 0;

  /// \brief Return the total file size in bytes.
  virtual Result<int64_t> Size() const = 0;

  /// \brief Open a new independent input stream.
  virtual Result<std::unique_ptr<SeekableInputStream>> Open() = 0;
};

/// \brief Handle for creating a writable file.
class ICEBERG_EXPORT OutputFile {
 public:
  virtual ~OutputFile() = default;

  /// \brief File location represented by this handle.
  virtual std::string_view location() const = 0;

  /// \brief Create a new output stream and fail if the file already exists.
  virtual Result<std::unique_ptr<PositionOutputStream>> Create() = 0;

  /// \brief Create a new output stream, replacing any existing file.
  virtual Result<std::unique_ptr<PositionOutputStream>> CreateOrOverwrite() = 0;
};

/// \brief Pluggable module for reading, writing, and deleting files.
///
/// This module handles metadata and data file bytes for table IO.
///
/// Note that these functions are not atomic. For example, if a write fails,
/// the file may be partially written. Implementations should be careful to
/// avoid corrupting metadata files.
class ICEBERG_EXPORT FileIO {
 public:
  FileIO() = default;
  virtual ~FileIO() = default;

  /// \brief Create an input file handle for the given location.
  virtual Result<std::unique_ptr<InputFile>> NewInputFile(std::string file_location);

  /// \brief Create an input file handle for the given location with a known length.
  ///
  /// The length is a caller-provided content length hint. Implementations may use it to
  /// avoid an extra metadata lookup.
  virtual Result<std::unique_ptr<InputFile>> NewInputFile(std::string file_location,
                                                          size_t length);

  /// \brief Create an output file handle for the given location.
  virtual Result<std::unique_ptr<OutputFile>> NewOutputFile(std::string file_location);

  /// \brief Read the content of the file at the given location.
  ///
  /// \param file_location The location of the file to read.
  /// \param length The number of bytes to read. Some object storage need to specify
  /// the length to read, e.g. S3 `GetObject` has a Range parameter.
  /// \return The content of the file if the read succeeded, an error code if the read
  /// failed.
  virtual Result<std::string> ReadFile(const std::string& file_location,
                                       std::optional<size_t> length);

  /// \brief Write the given content to the file at the given location.
  ///
  /// \param file_location The location of the file to write.
  /// \param content The content to write to the file.
  /// \return void if the write succeeded, an error code if the write failed.
  virtual Status WriteFile(const std::string& file_location, std::string_view content);

  /// \brief Delete a file at the given location.
  ///
  /// \param file_location The location of the file to delete.
  /// \return void if the delete succeeded, an error code if the delete failed.
  virtual Status DeleteFile(const std::string& file_location) {
    return NotImplemented("DeleteFile not implemented");
  }
};

}  // namespace iceberg
