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

#include "iceberg/io/fs_file_reader.h"

#include <format>

#include "iceberg/exception.h"

namespace iceberg::io {

FsFileReader::FsFileReader(std::string file_path) : file_path_(std::move(file_path)) {
  // Open the file in binary mode
  input_file_.open(file_path_, std::ios::binary | std::ios::in);
  if (!input_file_.is_open()) {
    throw IcebergError(std::format("Failed to open file: {}", file_path_));
  }

  // Calculate the file size
  input_file_.seekg(0, std::ios::end);
  file_size_ = input_file_.tellg();
  input_file_.seekg(0, std::ios::beg);

  if (file_size_ < 0) {
    throw IcebergError(std::format("Failed to determine file size: {}", file_path_));
  }
}

FsFileReader::~FsFileReader() {
  if (input_file_.is_open()) {
    input_file_.close();
  }
}

int64_t FsFileReader::read(int64_t offset, int64_t length, void* buffer) {
  if (!input_file_.is_open()) {
    throw IcebergError("File is not open for reading");
  }

  if (offset < 0 || offset + length > file_size_) {
    throw IcebergError(
        std::format("Invalid read range: [{}, {})", offset, offset + length));
  }

  // Seek to the starting position
  input_file_.seekg(offset, std::ios::beg);
  if (input_file_.fail()) {
    throw IcebergError(std::format("Failed to seek to offset: {}", offset));
  }

  // Read the data into the buffer
  input_file_.read(static_cast<char*>(buffer), length);
  auto bytes_read = static_cast<int64_t>(input_file_.gcount());

  return bytes_read;  // Return actual bytes read
}

std::future<int64_t> FsFileReader::readAsync(int64_t offset, int64_t length,
                                             void* buffer) {
  return std::async(std::launch::async, [this, offset, length, buffer]() -> int64_t {
    return this->read(offset, length, buffer);
  });
}

}  // namespace iceberg::io
