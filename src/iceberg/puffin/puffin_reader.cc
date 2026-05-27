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
#include <limits>
#include <span>
#include <string_view>

#include "iceberg/file_io.h"
#include "iceberg/puffin/json_serde_internal.h"
#include "iceberg/puffin/puffin_format.h"
#include "iceberg/util/endian.h"
#include "iceberg/util/macros.h"

namespace iceberg::puffin {

namespace {

struct FooterInfo {
  int32_t payload_size;
  PuffinCompressionCodec compression;
};

Status CheckMagic(std::span<const std::byte> data, int64_t offset = 0) {
  ICEBERG_PRECHECK(offset >= 0, "Invalid file: magic offset {} is negative", offset);
  auto offset_size = static_cast<size_t>(offset);
  ICEBERG_PRECHECK(offset_size <= data.size() &&
                       data.size() - offset_size >= PuffinFormat::kMagicLength,
                   "Invalid file: buffer too small for magic at offset {}", offset);
  auto* begin = reinterpret_cast<const uint8_t*>(data.data() + offset_size);
  ICEBERG_PRECHECK(
      std::equal(PuffinFormat::kMagicV1.cbegin(), PuffinFormat::kMagicV1.cend(), begin),
      "Invalid file: expected magic at offset {}, got [{:#04x}, {:#04x}, {:#04x}, "
      "{:#04x}]",
      offset, begin[0], begin[1], begin[2], begin[3]);
  return {};
}

Status CheckUnknownFlags(std::span<const uint8_t, 4> flags) {
  constexpr uint8_t kKnownBitsMask = 0x01;
  ICEBERG_PRECHECK(
      (flags[0] & ~kKnownBitsMask) == 0 && flags[1] == 0 && flags[2] == 0 &&
          flags[3] == 0,
      "Invalid file: unknown footer flags set [{:#04x}, {:#04x}, {:#04x}, {:#04x}]",
      flags[0], flags[1], flags[2], flags[3]);
  return {};
}

Result<int32_t> FooterPayloadSize(std::span<const std::byte> footer_struct) {
  ICEBERG_PRECHECK(footer_struct.size() >= PuffinFormat::kFooterStructLength,
                   "Invalid file: footer struct is too small");
  auto payload_size = ReadLittleEndian<int32_t>(
      footer_struct.data() + PuffinFormat::kFooterStructPayloadSizeOffset);
  ICEBERG_PRECHECK(payload_size >= 0, "Invalid file: negative payload size {}",
                   payload_size);
  return payload_size;
}

Result<std::array<uint8_t, 4>> DecodeFlags(std::span<const std::byte> footer_struct) {
  ICEBERG_PRECHECK(footer_struct.size() >= PuffinFormat::kFooterStructLength,
                   "Invalid file: footer struct is too small");
  std::array<uint8_t, 4> flags{};
  std::memcpy(flags.data(), footer_struct.data() + PuffinFormat::kFooterStructFlagsOffset,
              flags.size());
  ICEBERG_RETURN_UNEXPECTED(CheckUnknownFlags(flags));
  return flags;
}

PuffinCompressionCodec FooterCompressionCodec(std::span<const uint8_t, 4> flags) {
  if (IsFlagSet(flags, PuffinFlag::kFooterPayloadCompressed)) {
    return PuffinFormat::kDefaultFooterCompressionCodec;
  }
  return PuffinCompressionCodec::kNone;
}

Status CheckFooterSize(int64_t footer_size, int32_t payload_size) {
  auto expected_footer_size = PuffinFormat::kFooterStartMagicLength +
                              static_cast<int64_t>(payload_size) +
                              PuffinFormat::kFooterStructLength;
  ICEBERG_PRECHECK(footer_size == expected_footer_size,
                   "Invalid file: footer size {} does not match payload size {}",
                   footer_size, payload_size);
  return {};
}

Result<FooterInfo> DecodeFooterInfo(std::span<const std::byte> footer,
                                    int64_t footer_size) {
  ICEBERG_PRECHECK(footer_size >= PuffinFormat::kFooterStartMagicLength +
                                      PuffinFormat::kFooterStructLength,
                   "Invalid file: footer size {} is too small", footer_size);
  ICEBERG_PRECHECK(static_cast<uint64_t>(footer_size) <= footer.size(),
                   "Invalid file: footer size {} exceeds buffer size {}", footer_size,
                   footer.size());

  ICEBERG_RETURN_UNEXPECTED(CheckMagic(footer, PuffinFormat::kFooterStartMagicOffset));

  auto footer_struct_offset = footer_size - PuffinFormat::kFooterStructLength;
  std::span<const std::byte> footer_struct(footer.data() + footer_struct_offset,
                                           PuffinFormat::kFooterStructLength);
  ICEBERG_RETURN_UNEXPECTED(
      CheckMagic(footer_struct, PuffinFormat::kFooterStructMagicOffset));

  ICEBERG_ASSIGN_OR_RAISE(auto payload_size, FooterPayloadSize(footer_struct));
  ICEBERG_RETURN_UNEXPECTED(CheckFooterSize(footer_size, payload_size));

  ICEBERG_ASSIGN_OR_RAISE(auto flags, DecodeFlags(footer_struct));
  return FooterInfo{.payload_size = payload_size,
                    .compression = FooterCompressionCodec(flags)};
}

Result<FileMetadata> ParseFileMetadata(std::span<const std::byte> payload,
                                       PuffinCompressionCodec compression) {
  std::vector<std::byte> decompressed;
  if (compression != PuffinCompressionCodec::kNone) {
    ICEBERG_ASSIGN_OR_RAISE(decompressed, Decompress(compression, payload));
    payload = decompressed;
  }

  return FileMetadataFromJsonString(
      std::string_view(reinterpret_cast<const char*>(payload.data()), payload.size()));
}

}  // namespace

PuffinReader::PuffinReader(std::unique_ptr<SeekableInputStream> stream, int64_t file_size,
                           std::optional<int64_t> known_footer_size)
    : stream_(std::move(stream)),
      file_size_(file_size),
      known_footer_size_(known_footer_size) {}

PuffinReader::~PuffinReader() = default;

Result<std::unique_ptr<PuffinReader>> PuffinReader::Make(
    std::unique_ptr<InputFile> input_file, std::optional<int64_t> footer_size,
    std::optional<int64_t> file_size) {
  ICEBERG_PRECHECK(input_file, "Input file must not be null");
  int64_t resolved_file_size = 0;
  if (file_size.has_value()) {
    ICEBERG_PRECHECK(*file_size >= 0, "File size must not be negative: {}", *file_size);
    resolved_file_size = *file_size;
  } else {
    ICEBERG_ASSIGN_OR_RAISE(resolved_file_size, input_file->Size());
  }
  if (footer_size.has_value()) {
    ICEBERG_PRECHECK(*footer_size > 0, "Footer size must be positive: {}", *footer_size);
    ICEBERG_PRECHECK(*footer_size <= resolved_file_size - PuffinFormat::kMagicLength,
                     "Footer size {} exceeds file size {}", *footer_size,
                     resolved_file_size);
    ICEBERG_PRECHECK(*footer_size <= std::numeric_limits<int32_t>::max(),
                     "Footer size {} is too large", *footer_size);
  }
  ICEBERG_ASSIGN_OR_RAISE(auto stream, input_file->Open());
  return std::unique_ptr<PuffinReader>(
      new PuffinReader(std::move(stream), resolved_file_size, footer_size));
}

Result<std::vector<std::byte>> PuffinReader::ReadBytes(int64_t offset, int64_t length) {
  ICEBERG_PRECHECK(!closed_, "Reader already closed");
  ICEBERG_PRECHECK(offset >= 0, "Offset must not be negative: {}", offset);
  ICEBERG_PRECHECK(length >= 0, "Length must not be negative: {}", length);
  ICEBERG_PRECHECK(offset <= file_size_, "Offset {} exceeds file size {}", offset,
                   file_size_);
  ICEBERG_PRECHECK(length <= file_size_ - offset,
                   "Length {} exceeds file size {} at offset {}", length, file_size_,
                   offset);
  std::vector<std::byte> buf(length);
  ICEBERG_RETURN_UNEXPECTED(stream_->ReadFully(offset, buf));
  return buf;
}

Result<int64_t> PuffinReader::FooterSize() {
  if (known_footer_size_.has_value()) {
    return *known_footer_size_;
  }

  ICEBERG_ASSIGN_OR_RAISE(auto footer_struct,
                          ReadBytes(file_size_ - PuffinFormat::kFooterStructLength,
                                    PuffinFormat::kFooterStructLength));
  ICEBERG_RETURN_UNEXPECTED(
      CheckMagic(footer_struct, PuffinFormat::kFooterStructMagicOffset));

  ICEBERG_ASSIGN_OR_RAISE(auto payload_size, FooterPayloadSize(footer_struct));
  known_footer_size_ = PuffinFormat::kFooterStartMagicLength +
                       static_cast<int64_t>(payload_size) +
                       PuffinFormat::kFooterStructLength;
  return *known_footer_size_;
}

Result<std::vector<std::byte>> PuffinReader::ReadFooter(int64_t footer_size) {
  return ReadBytes(file_size_ - footer_size, footer_size);
}

Result<FileMetadata> PuffinReader::ReadFileMetadata() {
  ICEBERG_ASSIGN_OR_RAISE(auto header_bytes, ReadBytes(0, PuffinFormat::kMagicLength));
  ICEBERG_RETURN_UNEXPECTED(CheckMagic(header_bytes));

  ICEBERG_ASSIGN_OR_RAISE(auto footer_size, FooterSize());
  ICEBERG_ASSIGN_OR_RAISE(auto footer, ReadFooter(footer_size));
  ICEBERG_ASSIGN_OR_RAISE(auto footer_info, DecodeFooterInfo(footer, footer_size));
  std::span<const std::byte> payload_bytes(
      footer.data() + PuffinFormat::kFooterStartMagicLength, footer_info.payload_size);
  return ParseFileMetadata(payload_bytes, footer_info.compression);
}

Result<std::pair<BlobMetadata, std::vector<std::byte>>> PuffinReader::ReadBlob(
    const BlobMetadata& blob_metadata) {
  ICEBERG_ASSIGN_OR_RAISE(auto raw_data,
                          ReadBytes(blob_metadata.offset, blob_metadata.length));

  ICEBERG_ASSIGN_OR_RAISE(
      auto codec, PuffinCompressionCodecFromName(blob_metadata.compression_codec));
  if (codec == PuffinCompressionCodec::kNone) {
    return std::pair{blob_metadata, std::move(raw_data)};
  }

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

Status PuffinReader::Close() {
  if (closed_) {
    return {};
  }
  ICEBERG_RETURN_UNEXPECTED(stream_->Close());
  closed_ = true;
  return {};
}

}  // namespace iceberg::puffin
