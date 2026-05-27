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
#include <optional>
#include <utility>
#include <vector>

#include "iceberg/iceberg_data_export.h"
#include "iceberg/puffin/file_metadata.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg::puffin {

/// \brief Reader for Puffin files.
///
/// Reads from an InputFile with seek support for efficient blob access.
class ICEBERG_DATA_EXPORT PuffinReader {
 public:
  /// \brief Create a PuffinReader for the given input file.
  /// \param input_file The input file to read from.
  /// \param footer_size Optional known footer size hint to avoid an extra seek.
  /// \param file_size Optional known file size hint to avoid fetching size.
  static Result<std::unique_ptr<PuffinReader>> Make(
      std::unique_ptr<InputFile> input_file,
      std::optional<int64_t> footer_size = std::nullopt,
      std::optional<int64_t> file_size = std::nullopt);

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

  /// \brief Close the underlying input stream.
  Status Close();

 private:
  PuffinReader(std::unique_ptr<SeekableInputStream> stream, int64_t file_size,
               std::optional<int64_t> known_footer_size);

  Result<std::vector<std::byte>> ReadBytes(int64_t offset, int64_t length);
  Result<int64_t> FooterSize();
  Result<std::vector<std::byte>> ReadFooter(int64_t footer_size);

  /// Opened input stream.
  std::unique_ptr<SeekableInputStream> stream_;
  /// Total file size.
  int64_t file_size_;
  /// Known footer size hint (avoids one seek if provided).
  std::optional<int64_t> known_footer_size_;
  /// Whether the reader has been closed.
  bool closed_ = false;
};

}  // namespace iceberg::puffin
