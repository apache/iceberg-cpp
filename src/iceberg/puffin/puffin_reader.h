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
#include <span>
#include <utility>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/puffin/file_metadata.h"
#include "iceberg/result.h"

namespace iceberg::puffin {

/// \brief Reader for Puffin files.
///
/// Parses a Puffin file from an in-memory buffer. Usage:
///   PuffinReader reader(file_data);
///   auto metadata = reader.ReadFileMetadata();
///   auto blob = reader.ReadBlob(metadata.value().blobs[0]);
class ICEBERG_EXPORT PuffinReader {
 public:
  /// \brief Construct a reader from file data.
  explicit PuffinReader(std::span<const std::byte> data);

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
  std::span<const std::byte> data_;
};

}  // namespace iceberg::puffin
