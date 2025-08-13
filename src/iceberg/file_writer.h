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

/// \file iceberg/file_writer.h
/// Writer interface for file formats like Parquet, Avro and ORC.

#include <functional>
#include <memory>
#include <optional>

#include "iceberg/arrow_c_data.h"
#include "iceberg/file_format.h"
#include "iceberg/metrics.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Options for creating a writer.
struct ICEBERG_EXPORT WriterOptions {
  /// \brief The path to the file to write.
  std::string path;
  /// \brief The schema of the data to write.
  std::shared_ptr<Schema> schema;
  /// \brief FileIO instance to open the file. Writer implementations should down cast it
  /// to the specific FileIO implementation. By default, the `iceberg-bundle` library uses
  /// `ArrowFileSystemFileIO` as the default implementation.
  std::shared_ptr<class FileIO> io;
  /// \brief Format-specific or implementation-specific properties.
  std::unordered_map<std::string, std::string> properties;
};

/// \brief Base writer class to write data from different file formats.
class ICEBERG_EXPORT Writer {
 public:
  virtual ~Writer() = default;
  Writer() = default;
  Writer(const Writer&) = delete;
  Writer& operator=(const Writer&) = delete;

  /// \brief Open the writer.
  virtual Status Open(const struct WriterOptions& options) = 0;

  /// \brief Close the writer.
  virtual Status Close() = 0;

  /// \brief Write arrow data to the file.
  ///
  /// \return Status of write results.
  virtual Status Write(ArrowArray data) = 0;

  /// \brief Get the file statistics.
  /// Only valid after the file is closed.
  virtual std::optional<Metrics> metrics() = 0;

  /// \brief Get the file length.
  /// Only valid after the file is closed.
  virtual std::optional<int64_t> length() = 0;

  /// \brief Returns a list of recommended split locations, if applicable, empty
  /// otherwise. When available, this information is used for planning scan tasks whose
  /// boundaries are determined by these offsets. The returned list must be sorted in
  /// ascending order. Only valid after the file is closed.
  virtual std::vector<int64_t> split_offsets() = 0;
};

/// \brief Factory function to create a writer of a specific file format.
using WriterFactory = std::function<Result<std::unique_ptr<Writer>>()>;

/// \brief Registry of writer factories for different file formats.
struct ICEBERG_EXPORT WriterFactoryRegistry {
  /// \brief Register a factory function for a specific file format.
  WriterFactoryRegistry(FileFormatType format_type, WriterFactory factory);

  /// \brief Get the factory function for a specific file format.
  static WriterFactory& GetFactory(FileFormatType format_type);

  /// \brief Open a writer for a specific file format.
  static Result<std::unique_ptr<Writer>> Open(FileFormatType format_type,
                                              const WriterOptions& options);
};

}  // namespace iceberg
