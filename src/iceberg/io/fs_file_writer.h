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

#include <format>
#include <fstream>
#include <future>
#include <string>

#include "iceberg/io/iceberg_io_export.h"
#include "iceberg/writer.h"

namespace iceberg::io {

/// \brief A concrete implementation of Writer for file system files.
class ICEBERG_IO_EXPORT FsFileWriter : public Writer {
 public:
  explicit FsFileWriter(std::string file_path);

  ~FsFileWriter() override;

  // Disable copy constructor and assignment operator
  FsFileWriter(const FsFileWriter&) = delete;
  FsFileWriter& operator=(const FsFileWriter&) = delete;

  /// \brief Get the size of the file currently allocated.
  ///
  /// This implementation assumes size equals the last written offset + length of the
  /// written bytes.
  int64_t getSize() const override { return file_size_; }

  /// \brief Write data to the file synchronously.
  ///
  /// \param offset The starting offset of the write operation.
  /// \param buffer The buffer address containing the bytes to write.
  /// \param length The number of bytes to write.
  /// \return The actual number of bytes written.
  int64_t write(int64_t offset, const void* buffer, int64_t length) override;

  /// \brief Write data to the file asynchronously.
  ///
  /// \param offset The starting offset of the write operation.
  /// \param buffer The buffer address containing the bytes to write.
  /// \return A future resolving to the actual number of bytes written.
  std::future<int64_t> writeAsync(int64_t offset, const void* buffer,
                                  int64_t length) override;

  /// \brief Flush the written data to the file synchronously.
  ///
  /// This ensures all buffered data is committed to disk.
  void flush() override;

  /// \brief Flush the written data to the file asynchronously.
  ///
  /// This ensures all buffered data is committed to disk.
  std::future<void> flushAsync() override {
    return std::async(std::launch::async, [this]() { this->flush(); });
  }

 private:
  std::string file_path_;              // Path of the file being written to
  mutable std::ofstream output_file_;  // File stream for writing
  int64_t file_size_;                  // Current file size after last write
};

}  // namespace iceberg::io
