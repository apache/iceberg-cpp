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
#include <stdexcept>

#include "iceberg/iceberg_export.h"

namespace iceberg {

/// \brief A range of bytes to read from a file.
struct ICEBERG_EXPORT ReadRange {
  int64_t offset;
  int64_t length;

  friend bool operator==(const ReadRange& left, const ReadRange& right) {
    return (left.offset == right.offset && left.length == right.length);
  }
  friend bool operator!=(const ReadRange& left, const ReadRange& right) {
    return !(left == right);
  }

  bool Contains(const ReadRange& other) const {
    return (offset <= other.offset && offset + length >= other.offset + other.length);
  }
};

/// \brief Interface for reading bytes from a file.
class ICEBERG_EXPORT Reader {
 public:
  Reader() = default;
  virtual ~Reader() = default;

  /// \brief Get the size of the file.
  virtual int64_t getSize() const {
    throw std::runtime_error("getSize() not implemented");
  }

  /// \brief Get the size of the file (asynchronous).
  ///
  /// \return A future resolving to the file size in bytes.
  virtual std::future<int64_t> getSizeAsync() const {
    return std::async(std::launch::deferred, [this] { return getSize(); });
  }

  /// \brief Read a range of bytes from the file.
  ///
  /// \param range The range of bytes to read.
  /// \param buffer The buffer address to write the bytes to.
  /// \return The actual number of bytes read.
  virtual int64_t read(ReadRange range, void* buffer) {
    throw std::runtime_error("read() not implemented");
  }

  /// \brief Read a range of bytes from the file (asynchronous).
  ///
  /// \param range The range of bytes to read.
  /// \param buffer The buffer address to write the bytes to.
  /// \return A future resolving to the actual number of bytes read.
  virtual std::future<int64_t> readAsync(ReadRange range, void* buffer) {
    return std::async(std::launch::deferred,
                      [this, range, buffer] { return read(range, buffer); });
  }
};

}  // namespace iceberg
