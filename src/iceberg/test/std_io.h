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
#include <filesystem>
#include <fstream>
#include <ios>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include "iceberg/file_io.h"
#include "iceberg/result.h"
#include "iceberg/util/macros.h"

namespace iceberg::test {

namespace detail {

inline Result<std::streamsize> ToStreamSize(size_t size) {
  if (size > static_cast<size_t>(std::numeric_limits<std::streamsize>::max())) {
    return InvalidArgument("Buffer size {} exceeds streamsize max", size);
  }
  return static_cast<std::streamsize>(size);
}

inline Result<int64_t> ToInt64FileSize(uintmax_t size, std::string_view location) {
  if (size > static_cast<uintmax_t>(std::numeric_limits<int64_t>::max())) {
    return Invalid("File size for {} exceeds int64_t max", location);
  }
  return static_cast<int64_t>(size);
}

}  // namespace detail

class StdSeekableInputStream : public SeekableInputStream {
 public:
  explicit StdSeekableInputStream(std::string location)
      : location_(std::move(location)), file_(location_, std::ios::binary) {}

  bool is_open() const { return file_.is_open(); }

  Result<int64_t> Position() const override {
    auto position = file_.tellg();
    if (position < 0) {
      return IOError("Failed to get read position for: {}", location_);
    }
    return static_cast<int64_t>(position);
  }

  Status Seek(int64_t position) override {
    if (position < 0) {
      return InvalidArgument("Cannot seek to negative position {}", position);
    }
    file_.clear();
    file_.seekg(position);
    if (!file_) {
      return IOError("Failed to seek to {} in file: {}", position, location_);
    }
    return {};
  }

  Result<int64_t> Read(std::span<std::byte> out) override {
    if (out.empty()) {
      return 0;
    }
    ICEBERG_ASSIGN_OR_RAISE(auto read_size, detail::ToStreamSize(out.size()));
    file_.read(reinterpret_cast<char*>(out.data()), read_size);
    auto bytes_read = file_.gcount();
    if (!file_) {
      if (file_.bad() || !file_.eof()) {
        return IOError("Failed to read from file: {}", location_);
      }
      file_.clear();
    }
    if (bytes_read < 0) {
      return IOError("Failed to read from file: {}", location_);
    }
    return static_cast<int64_t>(bytes_read);
  }

  Status ReadFully(int64_t position, std::span<std::byte> out) override {
    if (position < 0) {
      return InvalidArgument("Cannot read from negative position {}", position);
    }
    if (out.empty()) {
      return {};
    }
    ICEBERG_ASSIGN_OR_RAISE(auto original_position, Position());
    auto seek_status = Seek(position);
    if (!seek_status.has_value()) {
      static_cast<void>(Seek(original_position));
      return seek_status;
    }

    Status read_status = {};
    size_t total_read = 0;
    while (total_read < out.size()) {
      auto read_result = Read(out.subspan(total_read));
      if (!read_result.has_value()) {
        read_status = std::unexpected<Error>(read_result.error());
        break;
      }
      if (read_result.value() == 0) {
        read_status =
            IOError("Failed to read {} bytes from file: {}", out.size(), location_);
        break;
      }
      total_read += static_cast<size_t>(read_result.value());
    }

    auto restore_status = Seek(original_position);
    ICEBERG_RETURN_UNEXPECTED(read_status);
    return restore_status;
  }

  Status Close() override {
    if (!file_.is_open()) {
      return {};
    }
    file_.close();
    if (!file_) {
      return IOError("Failed to close file: {}", location_);
    }
    return {};
  }

 private:
  std::string location_;
  mutable std::ifstream file_;
};

class StdPositionOutputStream : public PositionOutputStream {
 public:
  explicit StdPositionOutputStream(std::string location)
      : location_(std::move(location)),
        file_(location_, std::ios::binary | std::ios::out | std::ios::trunc) {}

  bool is_open() const { return file_.is_open(); }

  Result<int64_t> Position() const override {
    auto position = file_.tellp();
    if (position < 0) {
      return IOError("Failed to get write position for: {}", location_);
    }
    return static_cast<int64_t>(position);
  }

  Status Write(std::span<const std::byte> data) override {
    if (data.empty()) {
      return {};
    }
    ICEBERG_ASSIGN_OR_RAISE(auto write_size, detail::ToStreamSize(data.size()));
    file_.write(reinterpret_cast<const char*>(data.data()), write_size);
    if (!file_) {
      return IOError("Failed to write to file: {}", location_);
    }
    return {};
  }

  Status Flush() override {
    file_.flush();
    if (!file_) {
      return IOError("Failed to flush file: {}", location_);
    }
    return {};
  }

  Status Close() override {
    if (!file_.is_open()) {
      return {};
    }
    file_.close();
    if (!file_) {
      return IOError("Failed to close file: {}", location_);
    }
    return {};
  }

 private:
  std::string location_;
  mutable std::ofstream file_;
};

class StdInputFile : public InputFile {
 public:
  explicit StdInputFile(std::string location,
                        std::optional<int64_t> file_size = std::nullopt)
      : location_(std::move(location)), file_size_(file_size) {}

  std::string_view location() const override { return location_; }

  Result<int64_t> Size() const override {
    if (file_size_.has_value()) {
      return *file_size_;
    }
    std::error_code ec;
    auto size = std::filesystem::file_size(location_, ec);
    if (ec) {
      return IOError("Failed to get file size for {}: {}", location_, ec.message());
    }
    return detail::ToInt64FileSize(size, location_);
  }

  Result<std::unique_ptr<SeekableInputStream>> Open() override {
    auto stream = std::make_unique<StdSeekableInputStream>(location_);
    if (!stream->is_open()) {
      return IOError("Failed to open file for reading: {}", location_);
    }
    return stream;
  }

 private:
  std::string location_;
  std::optional<int64_t> file_size_;
};

class StdOutputFile : public OutputFile {
 public:
  explicit StdOutputFile(std::string location) : location_(std::move(location)) {}

  std::string_view location() const override { return location_; }

  Result<std::unique_ptr<PositionOutputStream>> Create() override {
    return Create(/*overwrite=*/false);
  }

  Result<std::unique_ptr<PositionOutputStream>> CreateOrOverwrite() override {
    return Create(/*overwrite=*/true);
  }

 private:
  Result<std::unique_ptr<PositionOutputStream>> Create(bool overwrite) {
    std::filesystem::path path(location_);
    std::error_code ec;
    auto exists = std::filesystem::exists(path, ec);
    if (ec) {
      return IOError("Failed to check file existence for {}: {}", location_,
                     ec.message());
    }
    if (!overwrite && exists) {
      return AlreadyExists("File already exists: {}", location_);
    }
    if (path.has_parent_path()) {
      std::filesystem::create_directories(path.parent_path(), ec);
      if (ec) {
        return IOError("Failed to create parent directories for {}: {}", location_,
                       ec.message());
      }
    }
    auto stream = std::make_unique<StdPositionOutputStream>(location_);
    if (!stream->is_open()) {
      return IOError("Failed to open file for writing: {}", location_);
    }
    return stream;
  }

  std::string location_;
};

/// \brief Simple local filesystem FileIO implementation for testing
///
/// This class provides a basic FileIO implementation that reads and writes
/// files to the local filesystem using standard C++ file streams.
class StdFileIO : public FileIO {
 public:
  Result<std::unique_ptr<InputFile>> NewInputFile(std::string file_location) override {
    return std::make_unique<StdInputFile>(std::move(file_location));
  }

  Result<std::unique_ptr<InputFile>> NewInputFile(std::string file_location,
                                                  size_t length) override {
    if (length > static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
      return InvalidArgument("File length {} exceeds int64_t max", length);
    }
    return std::make_unique<StdInputFile>(std::move(file_location),
                                          static_cast<int64_t>(length));
  }

  Result<std::unique_ptr<OutputFile>> NewOutputFile(std::string file_location) override {
    return std::make_unique<StdOutputFile>(std::move(file_location));
  }

  Status DeleteFile(const std::string& file_location) override {
    std::error_code ec;
    if (!std::filesystem::remove(file_location, ec)) {
      if (ec) {
        return IOError("Failed to delete file {}: {}", file_location, ec.message());
      }
      return IOError("File does not exist: {}", file_location);
    }
    return {};
  }
};

}  // namespace iceberg::test
