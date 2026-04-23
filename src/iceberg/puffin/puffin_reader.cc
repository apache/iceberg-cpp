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
#include <cstdint>
#include <cstring>
#include <string_view>

#include "iceberg/file_io.h"
#include "iceberg/puffin/json_serde_internal.h"
#include "iceberg/puffin/puffin_format.h"
#include "iceberg/util/endian.h"
#include "iceberg/util/macros.h"

namespace iceberg::puffin {

namespace {

// Validate magic bytes in a buffer at the given offset.
Status CheckMagic(std::span<const std::byte> data, int64_t offset) {
  if (offset < 0 ||
      offset + PuffinFormat::kMagicLength > static_cast<int64_t>(data.size())) {
    return Invalid("Invalid file: cannot read magic at offset {}", offset);
  }
  auto* begin = reinterpret_cast<const uint8_t*>(data.data() + offset);
  if (!std::equal(PuffinFormat::kMagicV1.begin(), PuffinFormat::kMagicV1.end(), begin)) {
    return Invalid(
        "Invalid file: expected magic at offset {}, got [{:#04x}, {:#04x}, "
        "{:#04x}, {:#04x}]",
        offset, begin[0], begin[1], begin[2], begin[3]);
  }
  return {};
}

// Validate that no unknown flag bits are set.
Status CheckUnknownFlags(std::span<const uint8_t, 4> flags) {
  constexpr uint8_t kKnownBitsMask = 0x01;
  if ((flags[0] & ~kKnownBitsMask) != 0 || flags[1] != 0 || flags[2] != 0 ||
      flags[3] != 0) {
    return Invalid(
        "Invalid file: unknown footer flags set [{:#04x}, {:#04x}, {:#04x}, {:#04x}]",
        flags[0], flags[1], flags[2], flags[3]);
  }
  return {};
}

}  // namespace

PuffinReader::PuffinReader(std::span<const std::byte> data)
    : data_(data), file_size_(static_cast<int64_t>(data.size())) {}

PuffinReader::PuffinReader(std::unique_ptr<InputFile> input_file)
    : input_file_(std::move(input_file)) {}

PuffinReader::~PuffinReader() = default;

Result<std::vector<std::byte>> PuffinReader::ReadBytes(int64_t offset, int64_t length) {
  if (IsFileMode()) {
    if (!stream_) {
      ICEBERG_ASSIGN_OR_RAISE(stream_, input_file_->Open());
    }
    std::vector<std::byte> buf(length);
    ICEBERG_RETURN_UNEXPECTED(stream_->ReadFully(offset, buf));
    return buf;
  }
  // Memory mode
  if (offset < 0 || length < 0 || offset > file_size_ || length > file_size_ - offset) {
    return Invalid("Read out of bounds: offset {} + length {} exceeds file size {}",
                   offset, length, file_size_);
  }
  return std::vector<std::byte>(data_.data() + offset, data_.data() + offset + length);
}

Result<FileMetadata> PuffinReader::ReadFileMetadata() {
  // Get file size
  if (IsFileMode()) {
    ICEBERG_ASSIGN_OR_RAISE(file_size_, input_file_->Size());
  }

  if (file_size_ < PuffinFormat::kFooterStructLength) {
    return Invalid("Invalid file: file length {} is less than minimal footer size {}",
                   file_size_, PuffinFormat::kFooterStructLength);
  }

  // Validate header magic
  ICEBERG_ASSIGN_OR_RAISE(auto header_bytes, ReadBytes(0, PuffinFormat::kMagicLength));
  ICEBERG_RETURN_UNEXPECTED(CheckMagic(header_bytes, 0));

  // Read footer struct from end of file
  auto footer_struct_offset = file_size_ - PuffinFormat::kFooterStructLength;
  ICEBERG_ASSIGN_OR_RAISE(
      auto footer_struct,
      ReadBytes(footer_struct_offset, PuffinFormat::kFooterStructLength));

  // Validate footer end magic
  ICEBERG_RETURN_UNEXPECTED(
      CheckMagic(footer_struct, PuffinFormat::kFooterStructMagicOffset));

  // Read payload size
  auto payload_size = ReadLittleEndian<int32_t>(
      footer_struct.data() + PuffinFormat::kFooterStructPayloadSizeOffset);

  if (payload_size < 0) {
    return Invalid("Invalid file: negative payload size {}", payload_size);
  }

  // Calculate total footer size and validate
  int64_t footer_size = PuffinFormat::kFooterStartMagicLength +
                        static_cast<int64_t>(payload_size) +
                        PuffinFormat::kFooterStructLength;
  auto footer_offset = file_size_ - footer_size;
  if (footer_offset < 0) {
    return Invalid("Invalid file: footer size {} exceeds file size {}", footer_size,
                   file_size_);
  }

  // Validate footer start magic
  ICEBERG_ASSIGN_OR_RAISE(auto footer_start_magic,
                          ReadBytes(footer_offset, PuffinFormat::kMagicLength));
  ICEBERG_RETURN_UNEXPECTED(CheckMagic(footer_start_magic, 0));

  // Check flags
  std::array<uint8_t, 4> flags{};
  std::memcpy(flags.data(), footer_struct.data() + PuffinFormat::kFooterStructFlagsOffset,
              4);
  ICEBERG_RETURN_UNEXPECTED(CheckUnknownFlags(flags));

  PuffinCompressionCodec footer_compression = PuffinCompressionCodec::kNone;
  if (IsFlagSet(flags, PuffinFlag::kFooterPayloadCompressed)) {
    footer_compression = PuffinFormat::kDefaultFooterCompressionCodec;
  }

  // Read and decompress footer payload
  auto payload_offset = footer_offset + PuffinFormat::kFooterStartMagicLength;
  ICEBERG_ASSIGN_OR_RAISE(auto payload_bytes, ReadBytes(payload_offset, payload_size));
  ICEBERG_ASSIGN_OR_RAISE(auto decompressed,
                          Decompress(footer_compression, payload_bytes));

  // Parse JSON
  std::string_view json_str(reinterpret_cast<const char*>(decompressed.data()),
                            decompressed.size());
  return FileMetadataFromJsonString(json_str);
}

Result<std::pair<BlobMetadata, std::vector<std::byte>>> PuffinReader::ReadBlob(
    const BlobMetadata& blob_metadata) {
  if (blob_metadata.offset < 0 || blob_metadata.length < 0 ||
      blob_metadata.offset > file_size_ ||
      blob_metadata.length > file_size_ - blob_metadata.offset) {
    return Invalid("Invalid blob: offset {} + length {} exceeds file size {}",
                   blob_metadata.offset, blob_metadata.length, file_size_);
  }

  ICEBERG_ASSIGN_OR_RAISE(auto raw_data,
                          ReadBytes(blob_metadata.offset, blob_metadata.length));

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
