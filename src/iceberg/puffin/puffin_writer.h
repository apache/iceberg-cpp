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
#include <memory>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

#include "iceberg/iceberg_data_export.h"
#include "iceberg/puffin/file_metadata.h"
#include "iceberg/result.h"

namespace iceberg {
class OutputFile;
class PositionOutputStream;
}  // namespace iceberg

namespace iceberg::puffin {

/// \brief Writer for Puffin files.
///
/// Writes blobs and footer to an OutputFile stream.
class ICEBERG_DATA_EXPORT PuffinWriter {
 public:
  /// \brief Create a PuffinWriter for the given output file.
  /// \param output_file The output file to write to.
  /// \param properties File-level properties to include in the footer.
  /// \param default_codec Default compression codec for blobs.
  /// \param compress_footer Whether to compress the footer payload.
  static Result<std::unique_ptr<PuffinWriter>> Make(
      std::unique_ptr<OutputFile> output_file,
      std::unordered_map<std::string, std::string> properties = {},
      PuffinCompressionCodec default_codec = PuffinCompressionCodec::kNone,
      bool compress_footer = false);

  ~PuffinWriter();

  /// \brief Write a blob and return its metadata.
  Result<BlobMetadata> Write(const Blob& blob);

  /// \brief Finalize the file by writing the footer and closing the stream.
  Status Finish();

  /// \brief Get metadata for all blobs written so far.
  const std::vector<BlobMetadata>& written_blobs_metadata() const;

  /// \brief Get the footer size. Returns error if Finish() has not been called.
  Result<int64_t> footer_size() const;

  /// \brief Get the total file size. Returns error if Finish() has not been called.
  Result<int64_t> file_size() const;

 private:
  PuffinWriter(std::unique_ptr<PositionOutputStream> stream,
               std::unordered_map<std::string, std::string> properties,
               PuffinCompressionCodec default_codec, bool compress_footer);

  /// Output stream.
  std::unique_ptr<PositionOutputStream> stream_;
  /// File-level properties to include in the footer.
  std::unordered_map<std::string, std::string> properties_;
  /// Default compression codec for blobs without explicit compression.
  PuffinCompressionCodec default_codec_;
  /// Whether to compress the footer payload.
  bool compress_footer_;
  /// Metadata for all blobs written so far.
  std::vector<BlobMetadata> written_blobs_metadata_;
  /// Whether the header magic has been written.
  bool header_written_ = false;
  /// Whether Finish() has been called.
  bool finished_ = false;
  /// Footer size, set after Finish().
  int64_t footer_size_ = -1;
  /// Total file size, set after Finish().
  int64_t file_size_ = -1;

  Status WriteBytes(std::span<const std::byte> data);
  Status WriteHeader();
  Status WriteMagic();
};

}  // namespace iceberg::puffin
