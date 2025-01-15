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

#include <fstream>
#include <future>
#include <string>

#include "iceberg/io/iceberg_io_export.h"
#include "iceberg/reader.h"

namespace iceberg::io {

/// \brief A concrete implementation of Reader for file system-based reading.
class ICEBERG_IO_EXPORT FsFileReader : public Reader {
 public:
  explicit FsFileReader(std::string file_path);

  ~FsFileReader() override;

  // Disable copy constructor and assignment operator
  FsFileReader(const FsFileReader&) = delete;
  FsFileReader& operator=(const FsFileReader&) = delete;

  /// \brief Get the size of the file.
  int64_t getSize() const override { return file_size_; }

  /// \brief Read a range of bytes from the file synchronously.
  ///
  /// \param offset The starting offset of the read operation.
  /// \param length The number of bytes to read.
  /// \param buffer The buffer address to write the bytes to.
  /// \return The actual number of bytes read.
  int64_t read(int64_t offset, int64_t length, void* buffer) override;

  /// \brief Read a range of bytes from the file asynchronously.
  ///
  /// \param offset The starting offset of the read operation.
  /// \param length The number of bytes to read.
  /// \param buffer The buffer address to write the bytes to.
  /// \return A future resolving to the actual number of bytes read.
  std::future<int64_t> readAsync(int64_t offset, int64_t length, void* buffer) override;

 private:
  std::string file_path_;             // Path to the file being read.
  mutable std::ifstream input_file_;  // File stream for reading.
  int64_t file_size_{0};              // Size of the file in bytes.
};

}  // namespace iceberg::io
