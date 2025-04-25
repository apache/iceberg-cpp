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
#include <variant>

#include "iceberg/arrow_c_data.h"
#include "iceberg/file_format.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Base reader class to read data from different file formats.
class ICEBERG_EXPORT Reader {
 public:
  virtual ~Reader() = default;

  /// \brief Read next data from file.
  ///
  /// \return std::monostate if the reader has no more data, otherwise `ArrowArray` or
  /// `StructLike` depending on the data layout by the reader implementation.
  using Data =
      std::variant<std::monostate, ArrowArray, std::reference_wrapper<const StructLike>>;
  virtual Result<Data> Next() = 0;

  enum class DataLayout { kArrowArray, kStructLike };

  /// \brief Get the data layout returned by `Next()` of the reader.
  virtual DataLayout data_layout() const = 0;
};

/// \brief Wrapper of `Reader` to always return `StructLike`.
///
/// If the data layout of the wrapped reader is `ArrowArray`, the data will be converted
/// to `StructLike`; otherwise, the data will be returned as is without any cost.
class ICEBERG_EXPORT StructLikeReader : public Reader {
 public:
  explicit StructLikeReader(std::unique_ptr<Reader> reader);

  /// \brief Always read data into `StructLike` or monostate if no more data.
  Result<Data> Next() final;

  DataLayout data_layout() const final { return DataLayout::kStructLike; }

 private:
  std::unique_ptr<Reader> reader_;
};

/// \brief Wrapper of `Reader` to always return `ArrowArray`.
///
/// If the data layout of the wrapped reader is `StructLike`, the data will be converted
/// to `ArrowArray`; otherwise, the data will be returned as is without any cost.
class ICEBERG_EXPORT BatchReader : public Reader {
 public:
  explicit BatchReader(std::unique_ptr<Reader> reader);

  /// \brief Always read data into `ArrowArray` or monostate if no more data.
  Result<Data> Next() final;

  DataLayout data_layout() const final { return DataLayout::kArrowArray; }

 private:
  std::unique_ptr<Reader> reader_;
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
  /// \brief The path to the file to read.
  std::string path;
  /// \brief The total length of the file.
  std::optional<size_t> length;
  /// \brief The split to read.
  std::optional<Split> split;
  /// \brief The batch size to read. Only applies to implementations that support
  /// batching.
  int64_t batch_size;
  /// \brief FileIO instance to open the file. Reader implementations should down cast it
  /// to the specific FileIO implementation. By default, the `iceberg-bundle` library uses
  /// `ArrowFileSystemFileIO` as the default implementation.
  std::shared_ptr<class FileIO> io;
  /// \brief The projection schema to read from the file.
  std::shared_ptr<class Schema> projection;
  /// \brief The filter to apply to the data. Reader implementations may ignore this if
  /// the file format does not support filtering.
  std::shared_ptr<class Expression> filter;
};

/// \brief Factory function to create a reader of a specific file format.
using ReaderFactory =
    std::function<Result<std::unique_ptr<Reader>>(const ReaderOptions&)>;

/// \brief Registry of reader factories for different file formats.
struct ICEBERG_EXPORT ReaderFactoryRegistry {
  /// \brief Register a factory function for a specific file format.
  ReaderFactoryRegistry(FileFormatType format_type, ReaderFactory factory);

  /// \brief Get the factory function for a specific file format.
  static ReaderFactory& GetFactory(FileFormatType format_type);

  /// \brief Create a reader for a specific file format.
  static Result<std::unique_ptr<Reader>> Create(FileFormatType format_type,
                                                const ReaderOptions& options);
};

/// \brief Macro to register a reader factory for a specific file format.
#define ICEBERG_REGISTER_READER_FACTORY(format_type, reader_factory)             \
  static ::iceberg::ReaderFactoryRegistry register_reader_factory_##format_type( \
      ::iceberg::FileFormatType::k##format_type, reader_factory);

}  // namespace iceberg
