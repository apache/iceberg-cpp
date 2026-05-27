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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/file_io.h"
#include "iceberg/util/macros.h"

namespace iceberg {

class MockFileIO : public FileIO {
 public:
  MockFileIO() = default;
  ~MockFileIO() override = default;

  Result<std::unique_ptr<InputFile>> NewInputFile(std::string file_location) override {
    auto file = FindFile(file_location);
    if (!file) {
      return NotFound("File does not exist: {}", file_location);
    }
    return std::make_unique<InMemoryInputFile>(std::move(file_location), std::move(file),
                                               std::nullopt);
  }

  Result<std::unique_ptr<InputFile>> NewInputFile(std::string file_location,
                                                  size_t length) override {
    if (length > static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
      return InvalidArgument("File length {} exceeds int64_t max", length);
    }
    auto file = FindFile(file_location);
    if (!file) {
      return NotFound("File does not exist: {}", file_location);
    }
    return std::make_unique<InMemoryInputFile>(std::move(file_location), std::move(file),
                                               static_cast<int64_t>(length));
  }

  Result<std::unique_ptr<OutputFile>> NewOutputFile(std::string file_location) override {
    return std::make_unique<InMemoryOutputFile>(files_, std::move(file_location));
  }

  void AddFile(std::string file_location, std::span<const std::byte> data) {
    files_->insert_or_assign(
        std::move(file_location),
        std::make_shared<std::vector<std::byte>>(data.begin(), data.end()));
  }

  void AddFile(std::string file_location, std::string_view data) {
    AddFile(std::move(file_location),
            std::as_bytes(std::span<const char>(data.data(), data.size())));
  }

  std::vector<std::byte>& FileData(const std::string& file_location) {
    return *GetOrCreateFile(file_location);
  }

  const std::vector<std::byte>& FileData(const std::string& file_location) const {
    return *files_->at(file_location);
  }

  MOCK_METHOD((Result<std::string>), ReadFile,
              (const std::string&, std::optional<size_t>), (override));

  MOCK_METHOD(Status, WriteFile, (const std::string&, std::string_view), (override));

  MOCK_METHOD(Status, DeleteFile, (const std::string&), (override));

 private:
  using FileMap =
      std::unordered_map<std::string, std::shared_ptr<std::vector<std::byte>>>;

  class InMemoryInputStream : public SeekableInputStream {
   public:
    explicit InMemoryInputStream(std::shared_ptr<std::vector<std::byte>> data)
        : data_(std::move(data)) {}

    Result<int64_t> Position() const override { return position_; }

    Status Seek(int64_t position) override {
      ICEBERG_PRECHECK(!closed_, "Input stream is closed");
      ICEBERG_PRECHECK(position >= 0, "Position must not be negative: {}", position);
      position_ = position;
      return {};
    }

    Result<int64_t> Read(std::span<std::byte> out) override {
      ICEBERG_PRECHECK(!closed_, "Input stream is closed");
      auto file_size = static_cast<int64_t>(data_->size());
      ICEBERG_PRECHECK(position_ <= file_size, "Position {} exceeds file size {}",
                       position_, file_size);
      auto bytes_to_read =
          std::min(static_cast<int64_t>(out.size()), file_size - position_);
      if (bytes_to_read > 0) {
        std::memcpy(out.data(), data_->data() + static_cast<size_t>(position_),
                    static_cast<size_t>(bytes_to_read));
        position_ += bytes_to_read;
      }
      return bytes_to_read;
    }

    Status ReadFully(int64_t position, std::span<std::byte> out) override {
      ICEBERG_PRECHECK(!closed_, "Input stream is closed");
      ICEBERG_PRECHECK(position >= 0, "Position must not be negative: {}", position);
      auto file_size = static_cast<int64_t>(data_->size());
      ICEBERG_PRECHECK(static_cast<int64_t>(out.size()) <= file_size - position,
                       "Read out of bounds: offset {} + length {} exceeds file size {}",
                       position, out.size(), file_size);
      if (!out.empty()) {
        std::memcpy(out.data(), data_->data() + static_cast<size_t>(position),
                    out.size());
      }
      return {};
    }

    Status Close() override {
      closed_ = true;
      return {};
    }

   private:
    std::shared_ptr<std::vector<std::byte>> data_;
    int64_t position_ = 0;
    bool closed_ = false;
  };

  class InMemoryOutputStream : public PositionOutputStream {
   public:
    explicit InMemoryOutputStream(std::shared_ptr<std::vector<std::byte>> data)
        : data_(std::move(data)) {}

    Result<int64_t> Position() const override {
      return static_cast<int64_t>(data_->size());
    }

    Status Write(std::span<const std::byte> data) override {
      ICEBERG_PRECHECK(!closed_, "Output stream is closed");
      data_->insert(data_->end(), data.begin(), data.end());
      return {};
    }

    Status Flush() override {
      ICEBERG_PRECHECK(!closed_, "Output stream is closed");
      return {};
    }

    Status Close() override {
      closed_ = true;
      return {};
    }

   private:
    std::shared_ptr<std::vector<std::byte>> data_;
    bool closed_ = false;
  };

  class InMemoryInputFile : public InputFile {
   public:
    InMemoryInputFile(std::string location, std::shared_ptr<std::vector<std::byte>> data,
                      std::optional<int64_t> length)
        : location_(std::move(location)), data_(std::move(data)), length_(length) {}

    std::string_view location() const override { return location_; }

    Result<int64_t> Size() const override {
      return length_.value_or(static_cast<int64_t>(data_->size()));
    }

    Result<std::unique_ptr<SeekableInputStream>> Open() override {
      return std::make_unique<InMemoryInputStream>(data_);
    }

   private:
    std::string location_;
    std::shared_ptr<std::vector<std::byte>> data_;
    std::optional<int64_t> length_;
  };

  class InMemoryOutputFile : public OutputFile {
   public:
    InMemoryOutputFile(std::shared_ptr<FileMap> files, std::string location)
        : files_(std::move(files)), location_(std::move(location)) {}

    std::string_view location() const override { return location_; }

    Result<std::unique_ptr<PositionOutputStream>> Create() override {
      if (files_->contains(location_)) {
        return AlreadyExists("File already exists: {}", location_);
      }
      auto file = std::make_shared<std::vector<std::byte>>();
      files_->emplace(location_, file);
      return std::make_unique<InMemoryOutputStream>(std::move(file));
    }

    Result<std::unique_ptr<PositionOutputStream>> CreateOrOverwrite() override {
      auto file = std::make_shared<std::vector<std::byte>>();
      files_->insert_or_assign(location_, file);
      return std::make_unique<InMemoryOutputStream>(std::move(file));
    }

   private:
    std::shared_ptr<FileMap> files_;
    std::string location_;
  };

  std::shared_ptr<std::vector<std::byte>> FindFile(
      const std::string& file_location) const {
    auto file = files_->find(file_location);
    if (file == files_->end()) {
      return nullptr;
    }
    return file->second;
  }

  std::shared_ptr<std::vector<std::byte>> GetOrCreateFile(
      const std::string& file_location) {
    auto& file = (*files_)[file_location];
    if (!file) {
      file = std::make_shared<std::vector<std::byte>>();
    }
    return file;
  }

  std::shared_ptr<FileMap> files_ = std::make_shared<FileMap>();
};

}  // namespace iceberg
