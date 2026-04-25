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

/// \file iceberg/puffin/puffin_writer.h
/// Puffin file writer.

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/puffin/file_metadata.h"
#include "iceberg/result.h"

namespace iceberg::puffin {

/// \brief Writer for Puffin files.
///
/// Builds a complete Puffin file in memory. Usage:
///   PuffinWriter writer;
///   writer.Add(blob1);
///   writer.Add(blob2);
///   auto result = writer.Finish({{"created-by", "iceberg-cpp"}});
///   // result.value() contains the serialized file bytes
class ICEBERG_EXPORT PuffinWriter {
 public:
  /// \brief Construct a writer with the given default compression codec.
  explicit PuffinWriter(
      PuffinCompressionCodec default_codec = PuffinCompressionCodec::kNone);

  /// \brief Add a blob to be written.
  /// \return The BlobMetadata for the written blob, or an error.
  Result<BlobMetadata> Add(const Blob& blob);

  /// \brief Finalize the file and return the serialized bytes.
  /// \param properties File-level properties to include in the footer.
  /// \return The complete Puffin file as a byte vector, or an error.
  Result<std::vector<std::byte>> Finish(
      std::unordered_map<std::string, std::string> properties = {});

  /// \brief Get metadata for all blobs written so far.
  const std::vector<BlobMetadata>& written_blobs_metadata() const;

  /// \brief Get the footer size after Finish() has been called.
  /// \return The footer size, or std::nullopt if Finish() has not been called.
  std::optional<int64_t> footer_size() const;

 private:
  PuffinCompressionCodec default_codec_;
  std::vector<std::byte> buffer_;
  std::vector<BlobMetadata> written_blobs_metadata_;
  bool header_written_ = false;
  bool finished_ = false;
  std::optional<int64_t> footer_size_;

  void WriteHeader();
};

}  // namespace iceberg::puffin
