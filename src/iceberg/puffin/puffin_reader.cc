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

#include "iceberg/puffin/puffin_reader.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <string_view>

#include "iceberg/puffin/json_serde_internal.h"
#include "iceberg/puffin/puffin_format.h"
#include "iceberg/util/endian.h"
#include "iceberg/util/macros.h"

namespace iceberg::puffin {

namespace {

// Validate magic bytes at the given offset.
Status CheckMagic(std::span<const std::byte> data, int64_t offset) {
  if (offset < 0 ||
      offset + PuffinFormat::kMagicLength > static_cast<int64_t>(data.size())) {
    return Invalid("Invalid file: cannot read magic at offset {}", offset);
  }
  auto* begin = reinterpret_cast<const uint8_t*>(data.data() + offset);
  if (!std::equal(PuffinFormat::kMagicV1.begin(), PuffinFormat::kMagicV1.end(), begin)) {
    return Invalid("Invalid file: expected magic at offset {}", offset);
  }
  return {};
}

}  // namespace

PuffinReader::PuffinReader(std::span<const std::byte> data) : data_(data) {}

Result<FileMetadata> PuffinReader::ReadFileMetadata() {
  auto file_size = static_cast<int64_t>(data_.size());

  if (file_size < PuffinFormat::kFooterStructLength) {
    return Invalid("Invalid file: file length {} is less than minimal footer size {}",
                   file_size, PuffinFormat::kFooterStructLength);
  }

  // Read footer struct from end of file
  auto footer_struct_offset = file_size - PuffinFormat::kFooterStructLength;

  // Validate footer end magic
  ICEBERG_RETURN_UNEXPECTED(
      CheckMagic(data_, footer_struct_offset + PuffinFormat::kFooterStructMagicOffset));

  // Read payload size from footer struct
  auto payload_size = ReadLittleEndian<int32_t>(
      data_.data() + footer_struct_offset + PuffinFormat::kFooterStructPayloadSizeOffset);

  if (payload_size < 0) {
    return Invalid("Invalid file: negative payload size {}", payload_size);
  }

  // Calculate total footer size and validate
  int64_t footer_size = PuffinFormat::kFooterStartMagicLength +
                        static_cast<int64_t>(payload_size) +
                        PuffinFormat::kFooterStructLength;
  auto footer_offset = file_size - footer_size;
  if (footer_offset < 0) {
    return Invalid("Invalid file: footer size {} exceeds file size {}", footer_size,
                   file_size);
  }

  // Validate footer start magic
  ICEBERG_RETURN_UNEXPECTED(CheckMagic(data_, footer_offset));

  // Check flags for footer compression
  std::array<uint8_t, 4> flags{};
  std::memcpy(
      flags.data(),
      data_.data() + footer_struct_offset + PuffinFormat::kFooterStructFlagsOffset, 4);

  PuffinCompressionCodec footer_compression = PuffinCompressionCodec::kNone;
  if (IsFlagSet(flags, PuffinFlag::kFooterPayloadCompressed)) {
    footer_compression = PuffinFormat::kDefaultFooterCompressionCodec;
  }

  // Extract footer payload
  auto payload_offset = footer_offset + PuffinFormat::kFooterStartMagicLength;
  std::span<const std::byte> payload_span(data_.data() + payload_offset, payload_size);
  ICEBERG_ASSIGN_OR_RAISE(auto payload_bytes,
                          Decompress(footer_compression, payload_span));

  // Parse JSON
  std::string_view json_str(reinterpret_cast<const char*>(payload_bytes.data()),
                            payload_bytes.size());
  ICEBERG_ASSIGN_OR_RAISE(auto file_metadata, FileMetadataFromJsonString(json_str));

  // Validate header magic
  ICEBERG_RETURN_UNEXPECTED(CheckMagic(data_, 0));

  return file_metadata;
}

Result<std::pair<BlobMetadata, std::vector<std::byte>>> PuffinReader::ReadBlob(
    const BlobMetadata& blob_metadata) {
  auto file_size = static_cast<int64_t>(data_.size());

  if (blob_metadata.offset < 0 || blob_metadata.length < 0 ||
      blob_metadata.offset > file_size ||
      blob_metadata.length > file_size - blob_metadata.offset) {
    return Invalid("Invalid blob: offset {} + length {} exceeds file size {}",
                   blob_metadata.offset, blob_metadata.length, file_size);
  }

  std::span<const std::byte> raw_data(data_.data() + blob_metadata.offset,
                                      blob_metadata.length);

  // Determine compression codec
  ICEBERG_ASSIGN_OR_RAISE(
      auto codec, PuffinCompressionCodecFromName(blob_metadata.compression_codec));
  ICEBERG_ASSIGN_OR_RAISE(auto decompressed, Decompress(codec, raw_data));

  return std::pair{blob_metadata, std::move(decompressed)};
}

Result<std::vector<std::pair<BlobMetadata, std::vector<std::byte>>>>
PuffinReader::ReadAll(const std::vector<BlobMetadata>& blobs) {
  std::vector<std::pair<BlobMetadata, std::vector<std::byte>>> results;
  results.reserve(blobs.size());
  for (const auto& blob : blobs) {
    ICEBERG_ASSIGN_OR_RAISE(auto blob_pair, ReadBlob(blob));
    results.push_back(std::move(blob_pair));
  }
  return results;
}

}  // namespace iceberg::puffin
