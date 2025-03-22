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

#include "iceberg/io/fs_file_io.h"

#include <fcntl.h>

#include <cassert>
#include <filesystem>
#include <format>

#include <sys/stat.h>

#include "iceberg/exception.h"
#include "iceberg/io/fs_file_reader.h"
#include "iceberg/io/fs_file_writer.h"

namespace iceberg::io {

bool FsInputFile::exists() const { return std::filesystem::exists(location()); }

int64_t FsInputFile::getLength() const {
  struct stat stat_buffer;
  if (stat(location().c_str(), &stat_buffer) != 0) {
    throw IcebergError(std::format(
        "Failed to get file length. File does not exist or is inaccessible: {}",
        location_));
  }
  return stat_buffer.st_size;
}

std::unique_ptr<Reader> FsInputFile::newReader() {
  return std::make_unique<FsFileReader>(location_);
}

void FsOutputFile::create() {
  // Check if the file already exists
  std::ifstream existing_file(location_);
  bool file_exists = existing_file.good();
  existing_file.close();

  if (file_exists) {
    throw IcebergError(std::format("File already exists: {}", location_));
  }

  // Create or overwrite the file by opening it in truncating mode
  std::ofstream new_file(location_, std::ios::binary | std::ios::out | std::ios::trunc);
  if (!new_file.is_open()) {
    throw IcebergError(std::format("Failed to create or overwrite file: {}", location_));
  }
  new_file.close();
}

std::unique_ptr<Writer> FsOutputFile::newWriter() {
  return std::make_unique<FsFileWriter>(location_);
}

std::shared_ptr<InputFile> FsFileIO::newInputFile(const std::string& location) {
  // Check if the file exists
  if (!fileExists(location)) {
    throw IcebergError(std::format("InputFile does not exist: {}", location));
  }

  // Create and return an FsInputFile instance
  return std::make_shared<FsInputFile>(location);
}

std::shared_ptr<OutputFile> FsFileIO::newOutputFile(const std::string& location) {
  return std::make_shared<FsOutputFile>(location);
}

void FsFileIO::DeleteFile(const std::string& location) {
  // Check if the file exists
  if (!fileExists(location)) {
    throw IcebergError(std::format("InputFile does not exist: {}", location));
  }
  std::error_code ec;
  if (std::filesystem::remove(location, ec) == false) {
    throw IcebergError(
        std::format("Failed to delete file: {}, error code: {}", location, ec.message()));
  }
}

bool FsFileIO::fileExists(const std::string& location) {
  std::ifstream file(location);
  return file.good();
}

}  // namespace iceberg::io
