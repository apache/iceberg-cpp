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

#include <future>

#include "iceberg/exception.h"
#include "iceberg/iceberg_export.h"

namespace iceberg {

/// \brief Interface for reading bytes from a file.
class ICEBERG_EXPORT Reader {
 public:
  virtual ~Reader() = default;

  /// \brief Get the size of the file.
  virtual int64_t getSize() const { throw IcebergError("getSize() not implemented"); }

  /// \brief Get the size of the file (asynchronous).
  ///
  /// \return A future resolving to the file size in bytes.
  virtual std::future<int64_t> getSizeAsync() const {
    return std::async(std::launch::deferred, [this] { return getSize(); });
  }

  /// \brief Read a range of bytes from the file.
  ///
  /// \param offset The starting offset of the read operation.
  /// \param length The number of bytes to read.
  /// \param buffer The buffer address to write the bytes to.
  /// \return The actual number of bytes read.
  virtual int64_t read(int64_t offset, int64_t length, void* buffer) {
    throw IcebergError("read() not implemented");
  }

  /// \brief Read a range of bytes from the file (asynchronous).
  ///
  /// \param offset The starting offset of the read operation.
  /// \param length The number of bytes to read.
  /// \param buffer The buffer address to write the bytes to.
  /// \return A future resolving to the actual number of bytes read.
  virtual std::future<int64_t> readAsync(int64_t offset, int64_t length, void* buffer) {
    return std::async(std::launch::deferred, [this, offset, length, buffer] {
      return read(offset, length, buffer);
    });
  }
};

}  // namespace iceberg
