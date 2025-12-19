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

#include <filesystem>
#include <fstream>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>

#include "iceberg/file_io.h"
#include "iceberg/result.h"

namespace iceberg::test {

/// \brief Simple local filesystem FileIO implementation for testing
///
/// This class provides a basic FileIO implementation that reads and writes
/// files to the local filesystem using standard C++ file streams.
class StdFileIO : public FileIO {
 public:
  Result<std::string> ReadFile(const std::string& file_location,
                               std::optional<size_t> length) override {
    std::ifstream file(file_location, std::ios::binary);
    if (!file.is_open()) {
      return IOError("Failed to open file for reading: {}", file_location);
    }

    if (length.has_value()) {
      std::string content(length.value(), '\0');
      file.read(content.data(), length.value());
      if (!file) {
        return IOError("Failed to read {} bytes from file: {}", length.value(),
                       file_location);
      }
      return content;
    } else {
      std::stringstream buffer;
      buffer << file.rdbuf();
      if (!file && !file.eof()) {
        return IOError("Failed to read file: {}", file_location);
      }
      return buffer.str();
    }
  }

  Status WriteFile(const std::string& file_location, std::string_view content) override {
    // Create parent directories if they don't exist
    std::filesystem::path path(file_location);
    if (path.has_parent_path()) {
      std::error_code ec;
      std::filesystem::create_directories(path.parent_path(), ec);
      if (ec) {
        return IOError("Failed to create parent directories for: {}", file_location);
      }
    }

    std::ofstream file(file_location, std::ios::binary);
    if (!file.is_open()) {
      return IOError("Failed to open file for writing: {}", file_location);
    }

    file.write(content.data(), content.size());
    if (!file) {
      return IOError("Failed to write to file: {}", file_location);
    }

    return {};
  }

  Status DeleteFile(const std::string& file_location) override {
    std::error_code ec;
    if (!std::filesystem::remove(file_location, ec)) {
      if (ec) {
        return IOError("Failed to delete file: {}", file_location);
      }
      return IOError("File does not exist: {}", file_location);
    }
    return {};
  }
};

}  // namespace iceberg::test
