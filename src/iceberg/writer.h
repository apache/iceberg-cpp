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

/// \brief Interface for writing bytes to a file.
class ICEBERG_EXPORT Writer {
 public:
  virtual ~Writer() = default;

  /// \brief Get the size of the file currently allocated.
  ///
  /// Default implementation throws a runtime exception if not overridden.
  virtual int64_t getSize() const { throw IcebergError("getSize() not implemented"); }

  /// \brief Get the size of the file (asynchronous).
  ///
  /// Default implementation wraps `getSize()` in a future.
  virtual std::future<int64_t> getSizeAsync() const {
    return std::async(std::launch::deferred, [this] { return getSize(); });
  }

  /// \brief Write data to the file.
  ///
  /// \param offset The starting offset of the write operation.
  /// \param buffer The buffer address containing the bytes to write.
  /// \param length The number of bytes to write.
  /// \return The actual number of bytes written.
  virtual int64_t write(int64_t offset, const void* buffer, int64_t length) {
    throw IcebergError("write() not implemented");
  }

  /// \brief Write data to the file (asynchronous).
  ///
  /// \param offset The starting offset of the write operation.
  /// \param buffer The buffer address containing the bytes to write.
  /// \param length The number of bytes to write.
  /// \return A future resolving to the actual number of bytes written.
  virtual std::future<int64_t> writeAsync(int64_t offset, const void* buffer,
                                          int64_t length) {
    return std::async(std::launch::deferred, [this, offset, buffer, length] {
      return write(offset, buffer, length);
    });
  }

  /// \brief Flush buffered data to the file.
  ///
  /// Default implementation does nothing if not overridden.
  virtual void flush() {}

  /// \brief Flush buffered data to the file (asynchronous).
  ///
  /// Default implementation wraps `flush()` in a future.
  virtual std::future<void> flushAsync() {
    return std::async(std::launch::deferred, [this] { flush(); });
  }
};

}  // namespace iceberg
