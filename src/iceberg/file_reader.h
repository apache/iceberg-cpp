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

/// \file iceberg/file_reader.h
/// Reader interface for file formats like Parquet, Avro and ORC.

#include <functional>
#include <memory>
#include <optional>

#include "iceberg/arrow_c_data.h"
#include "iceberg/file_format.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Base reader class to read data from different file formats.
class ICEBERG_EXPORT Reader {
 public:
  virtual ~Reader() = default;
  Reader() = default;
  Reader(const Reader&) = delete;
  Reader& operator=(const Reader&) = delete;

  /// \brief Open the reader.
  virtual Status Open(const struct ReaderOptions& options) = 0;

  /// \brief Close the reader.
  virtual Status Close() = 0;

  /// \brief Read next data from file.
  ///
  /// \return std::nullopt if the reader has no more data, otherwise `ArrowArray`.
  virtual Result<std::optional<ArrowArray>> Next() = 0;

  /// \brief Get the schema of the data.
  virtual Result<ArrowSchema> Schema() = 0;
};

/// \brief A split of the file to read.
struct ICEBERG_EXPORT Split {
  /// \brief The offset of the split.
  size_t offset;
  /// \brief The length of the split.
  size_t length;
};

/// \brief Options for creating a reader.
struct ICEBERG_EXPORT ReaderOptions {
  static constexpr int64_t kDefaultBatchSize = 4096;

  /// \brief The path to the file to read.
  std::string path;
  /// \brief The total length of the file.
  std::optional<size_t> length;
  /// \brief The split to read.
  std::optional<Split> split;
  /// \brief The batch size to read. Only applies to implementations that support
  /// batching.
  int64_t batch_size = kDefaultBatchSize;
  /// \brief FileIO instance to open the file. Reader implementations should down cast it
  /// to the specific FileIO implementation. By default, the `iceberg-bundle` library uses
  /// `ArrowFileSystemFileIO` as the default implementation.
  std::shared_ptr<class FileIO> io;
  /// \brief The projection schema to read from the file. This field is required.
  std::shared_ptr<class Schema> projection;
  /// \brief The filter to apply to the data. Reader implementations may ignore this if
  /// the file format does not support filtering.
  std::shared_ptr<class Expression> filter;
  /// \brief Format-specific or implementation-specific properties.
  std::unordered_map<std::string, std::string> properties;
};

/// \brief Factory function to create a reader of a specific file format.
using ReaderFactory = std::function<Result<std::unique_ptr<Reader>>()>;

/// \brief Registry of reader factories for different file formats.
struct ICEBERG_EXPORT ReaderFactoryRegistry {
  /// \brief Register a factory function for a specific file format.
  ReaderFactoryRegistry(FileFormatType format_type, ReaderFactory factory);

  /// \brief Get the factory function for a specific file format.
  static ReaderFactory& GetFactory(FileFormatType format_type);

  /// \brief Open a reader for a specific file format.
  static Result<std::unique_ptr<Reader>> Open(FileFormatType format_type,
                                              const ReaderOptions& options);
};

}  // namespace iceberg
