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

// Validate magic bytes in a buffer at offset 0.
Status CheckMagic(std::span<const std::byte> data) {
  if (static_cast<int64_t>(data.size()) < PuffinFormat::kMagicLength) {
    return Invalid("Invalid file: buffer too small for magic");
  }
  auto* begin = reinterpret_cast<const uint8_t*>(data.data());
  if (!std::equal(PuffinFormat::kMagicV1.begin(), PuffinFormat::kMagicV1.end(), begin)) {
    return Invalid(
        "Invalid file: expected magic, got [{:#04x}, {:#04x}, {:#04x}, {:#04x}]",
        begin[0], begin[1], begin[2], begin[3]);
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

PuffinReader::PuffinReader(std::unique_ptr<SeekableInputStream> stream, int64_t file_size,
                           std::optional<int64_t> known_footer_size)
    : stream_(std::move(stream)),
      file_size_(file_size),
      known_footer_size_(known_footer_size) {}

PuffinReader::~PuffinReader() = default;

Result<std::unique_ptr<PuffinReader>> PuffinReader::Make(
    std::unique_ptr<InputFile> input_file, std::optional<int64_t> footer_size) {
  if (!input_file) {
    return InvalidArgument("input_file must not be null");
  }
  ICEBERG_ASSIGN_OR_RAISE(auto file_size, input_file->Size());
  ICEBERG_ASSIGN_OR_RAISE(auto stream, input_file->Open());
  return std::unique_ptr<PuffinReader>(
      new PuffinReader(std::move(stream), file_size, footer_size));
}

Result<std::vector<std::byte>> PuffinReader::ReadBytes(int64_t offset, int64_t length) {
  if (offset < 0 || length < 0 || offset > file_size_ || length > file_size_ - offset) {
    return Invalid("Read out of bounds: offset {} + length {} exceeds file size {}",
                   offset, length, file_size_);
  }
  std::vector<std::byte> buf(length);
  ICEBERG_RETURN_UNEXPECTED(stream_->ReadFully(offset, buf));
  return buf;
}

Result<FileMetadata> PuffinReader::ReadFileMetadata() {
  if (file_size_ < PuffinFormat::kFooterStructLength) {
    return Invalid("Invalid file: file length {} is less than minimal footer size {}",
                   file_size_, PuffinFormat::kFooterStructLength);
  }

  // Validate header magic
  ICEBERG_ASSIGN_OR_RAISE(auto header_bytes, ReadBytes(0, PuffinFormat::kMagicLength));
  ICEBERG_RETURN_UNEXPECTED(CheckMagic(header_bytes));

  // Read footer struct from end of file
  auto footer_struct_offset = file_size_ - PuffinFormat::kFooterStructLength;
  ICEBERG_ASSIGN_OR_RAISE(
      auto footer_struct,
      ReadBytes(footer_struct_offset, PuffinFormat::kFooterStructLength));

  // Validate footer end magic
  std::span<const std::byte> footer_end_magic(
      footer_struct.data() + PuffinFormat::kFooterStructMagicOffset,
      PuffinFormat::kMagicLength);
  ICEBERG_RETURN_UNEXPECTED(CheckMagic(footer_end_magic));

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
  ICEBERG_RETURN_UNEXPECTED(CheckMagic(footer_start_magic));

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

  ICEBERG_ASSIGN_OR_RAISE(
      auto codec, PuffinCompressionCodecFromName(blob_metadata.compression_codec));
  ICEBERG_ASSIGN_OR_RAISE(auto decompressed, Decompress(codec, raw_data));

  return std::pair{blob_metadata, std::move(decompressed)};
}

Result<std::vector<std::pair<BlobMetadata, std::vector<std::byte>>>>
PuffinReader::ReadAll(const std::vector<BlobMetadata>& blobs) {
  // Sort by offset for sequential I/O access pattern
  std::vector<const BlobMetadata*> sorted;
  sorted.reserve(blobs.size());
  for (const auto& blob : blobs) {
    sorted.push_back(&blob);
  }
  std::ranges::sort(sorted,
                    [](const auto* a, const auto* b) { return a->offset < b->offset; });

  std::vector<std::pair<BlobMetadata, std::vector<std::byte>>> results;
  results.reserve(blobs.size());
  for (const auto* blob : sorted) {
    ICEBERG_ASSIGN_OR_RAISE(auto blob_pair, ReadBlob(*blob));
    results.push_back(std::move(blob_pair));
  }
  return results;
}

}  // namespace iceberg::puffin
