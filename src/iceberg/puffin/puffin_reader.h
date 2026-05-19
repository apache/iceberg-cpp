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

/// \file iceberg/puffin/puffin_reader.h
/// Puffin file reader.

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <utility>
#include <vector>

#include "iceberg/iceberg_data_export.h"
#include "iceberg/puffin/file_metadata.h"
#include "iceberg/result.h"

namespace iceberg {
class InputFile;
class SeekableInputStream;
}  // namespace iceberg

namespace iceberg::puffin {

/// \brief Reader for Puffin files.
///
/// Supports two modes:
/// - Memory mode: parses from an in-memory buffer.
/// - File mode: reads from an InputFile with seek support (for DV use case).
class ICEBERG_DATA_EXPORT PuffinReader {
 public:
  /// \brief Construct a memory-mode reader from file data.
  explicit PuffinReader(std::span<const std::byte> data);

  /// \brief Construct a file-mode reader from an InputFile.
  explicit PuffinReader(std::unique_ptr<InputFile> input_file);

  ~PuffinReader();

  /// \brief Read and return the file metadata from the footer.
  Result<FileMetadata> ReadFileMetadata();

  /// \brief Read a specific blob's data by its metadata.
  /// \param blob_metadata The metadata describing the blob to read.
  /// \return A pair of (BlobMetadata, decompressed data), or an error.
  Result<std::pair<BlobMetadata, std::vector<std::byte>>> ReadBlob(
      const BlobMetadata& blob_metadata);

  /// \brief Read all blobs described in the file metadata.
  /// \return A vector of (BlobMetadata, decompressed data) pairs, or an error.
  Result<std::vector<std::pair<BlobMetadata, std::vector<std::byte>>>> ReadAll(
      const std::vector<BlobMetadata>& blobs);

 private:
  /// In-memory data for memory mode.
  std::span<const std::byte> data_;
  /// Input file for file mode.
  std::unique_ptr<InputFile> input_file_;
  /// Opened stream (lazily opened in file mode).
  std::unique_ptr<SeekableInputStream> stream_;
  /// Cached file size.
  int64_t file_size_ = 0;

  bool IsFileMode() const { return input_file_ != nullptr; }
  Result<std::vector<std::byte>> ReadBytes(int64_t offset, int64_t length);
};

}  // namespace iceberg::puffin
