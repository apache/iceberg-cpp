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

#include "iceberg/io/fs_file_writer.h"

#include "iceberg/exception.h"

namespace iceberg::io {

FsFileWriter::FsFileWriter(std::string file_path) : file_path_(std::move(file_path)) {
  // Open the file in binary write mode, truncating any existing file
  output_file_.open(file_path_, std::ios::binary | std::ios::out | std::ios::trunc);
  if (!output_file_.is_open()) {
    throw IcebergError(std::format("Failed to open file for writing: {}", file_path_));
  }
  // Calculate the file size after opening the file
  output_file_.seekp(0, std::ios::end);
  file_size_ = output_file_.tellp();
}

FsFileWriter::~FsFileWriter() {
  if (output_file_.is_open()) {
    output_file_.close();
  }
}

int64_t FsFileWriter::write(int64_t offset, const void* buffer, int64_t length) {
  if (!output_file_.is_open()) {
    throw IcebergError(std::format("File is not open for writing: {}", file_path_));
  }

  if (offset < 0) {
    throw IcebergError(
        std::format("Invalid write range. Offset must be non-negative: {}", offset));
  }

  // Seek the position to write
  output_file_.seekp(offset, std::ios::beg);
  if (output_file_.fail()) {
    throw IcebergError(std::format("Failed to seek to offset: {}", offset));
  }

  // Write data to the file
  output_file_.write(static_cast<const char*>(buffer), length);
  if (output_file_.fail()) {
    throw IcebergError("Failed to write data to file.");
  }

  // Update the file size based on the last written position
  file_size_ = std::max(file_size_, offset + length);

  return length;  // Return number of bytes successfully written
}

std::future<int64_t> FsFileWriter::writeAsync(int64_t offset, const void* buffer,
                                              int64_t length) {
  return std::async(std::launch::async, [this, offset, buffer, length]() {
    return this->write(offset, buffer, length);
  });
}

void FsFileWriter::flush() {
  if (!output_file_.is_open()) {
    throw IcebergError(std::format("File is not open for flushing: {}", file_path_));
  }

  output_file_.flush();
  if (output_file_.fail()) {
    throw IcebergError("Failed to flush data to file.");
  }
}

}  // namespace iceberg::io
