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
#include <optional>
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
/// Supports two modes:
/// - Stream mode: writes directly to an OutputFile/PositionOutputStream.
/// - Memory mode: builds the file in an internal buffer and returns it via Finish().
class ICEBERG_DATA_EXPORT PuffinWriter {
 public:
  /// \brief Construct a memory-mode writer.
  /// \param properties File-level properties to include in the footer.
  /// \param default_codec Default compression codec for blobs.
  explicit PuffinWriter(
      std::unordered_map<std::string, std::string> properties = {},
      PuffinCompressionCodec default_codec = PuffinCompressionCodec::kNone);

  /// \brief Construct a stream-mode writer from an OutputFile.
  /// \param output_file The output file to write to.
  /// \param properties File-level properties to include in the footer.
  /// \param default_codec Default compression codec for blobs.
  PuffinWriter(std::unique_ptr<OutputFile> output_file,
               std::unordered_map<std::string, std::string> properties = {},
               PuffinCompressionCodec default_codec = PuffinCompressionCodec::kNone);

  ~PuffinWriter();

  /// \brief Add a blob to be written.
  Status Add(const Blob& blob);

  /// \brief Finalize the file.
  ///
  /// In memory mode, returns the complete serialized file bytes.
  /// In stream mode, flushes and closes the stream, returns empty vector.
  Result<std::vector<std::byte>> Finish();

  /// \brief Get metadata for all blobs written so far.
  const std::vector<BlobMetadata>& written_blobs_metadata() const;

  /// \brief Get the footer size after Finish() has been called.
  std::optional<int64_t> footer_size() const;

  /// \brief Get the total file size after Finish() has been called.
  std::optional<int64_t> file_size() const;

 private:
  /// Default compression codec for blobs without explicit compression.
  PuffinCompressionCodec default_codec_;
  /// File-level properties to include in the footer.
  std::unordered_map<std::string, std::string> properties_;
  /// Buffer for memory mode.
  std::vector<std::byte> buffer_;
  /// Output stream for stream mode.
  std::unique_ptr<PositionOutputStream> stream_;
  /// Metadata for all blobs written so far.
  std::vector<BlobMetadata> written_blobs_metadata_;
  /// Whether the header magic has been written.
  bool header_written_ = false;
  /// Whether Finish() has been called.
  bool finished_ = false;
  /// Footer size, set after Finish().
  std::optional<int64_t> footer_size_;
  /// Total file size, set after Finish().
  std::optional<int64_t> file_size_;

  bool IsStreamMode() const { return stream_ != nullptr; }
  Status WriteBytes(std::span<const std::byte> data);
  Status WriteHeader();
  Status WriteMagic();
  Result<int64_t> CurrentPosition() const;
};

}  // namespace iceberg::puffin
