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

#include "iceberg/puffin/puffin_writer.h"

#include <array>

#include "iceberg/puffin/json_serde_internal.h"
#include "iceberg/puffin/puffin_format.h"
#include "iceberg/util/endian.h"
#include "iceberg/util/macros.h"

namespace iceberg::puffin {

PuffinWriter::PuffinWriter(PuffinCompressionCodec default_codec)
    : default_codec_(default_codec) {}

void PuffinWriter::WriteHeader() {
  if (header_written_) return;
  const auto& magic = PuffinFormat::kMagicV1;
  buffer_.insert(buffer_.end(), reinterpret_cast<const std::byte*>(magic.data()),
                 reinterpret_cast<const std::byte*>(magic.data() + magic.size()));
  header_written_ = true;
}

Result<BlobMetadata> PuffinWriter::Add(const Blob& blob) {
  if (finished_) {
    return Invalid("Writer already finished");
  }

  WriteHeader();

  auto codec = blob.requested_compression.value_or(default_codec_);
  std::span<const std::byte> input_span(
      reinterpret_cast<const std::byte*>(blob.data.data()), blob.data.size());
  ICEBERG_ASSIGN_OR_RAISE(auto compressed, Compress(codec, input_span));

  auto offset = static_cast<int64_t>(buffer_.size());
  auto length = static_cast<int64_t>(compressed.size());
  buffer_.insert(buffer_.end(), compressed.begin(), compressed.end());

  auto codec_name = CodecName(codec);
  BlobMetadata metadata{
      .type = blob.type,
      .input_fields = blob.input_fields,
      .snapshot_id = blob.snapshot_id,
      .sequence_number = blob.sequence_number,
      .offset = offset,
      .length = length,
      .compression_codec = std::string(codec_name),
      .properties = blob.properties,
  };
  written_blobs_metadata_.push_back(metadata);
  return metadata;
}

Result<std::vector<std::byte>> PuffinWriter::Finish(
    std::unordered_map<std::string, std::string> properties) {
  if (finished_) {
    return Invalid("Writer already finished");
  }

  WriteHeader();

  FileMetadata file_metadata{
      .blobs = written_blobs_metadata_,
      .properties = std::move(properties),
  };

  auto footer_json = ToJsonString(file_metadata);
  auto footer_payload = std::span<const std::byte>(
      reinterpret_cast<const std::byte*>(footer_json.data()), footer_json.size());

  // Footer start magic
  auto footer_start = static_cast<int64_t>(buffer_.size());
  const auto& magic = PuffinFormat::kMagicV1;
  buffer_.insert(buffer_.end(), reinterpret_cast<const std::byte*>(magic.data()),
                 reinterpret_cast<const std::byte*>(magic.data() + magic.size()));

  // Footer payload
  buffer_.insert(buffer_.end(), footer_payload.begin(), footer_payload.end());

  // Footer struct: payload_size (4) + flags (4) + magic (4)
  auto payload_size = static_cast<int32_t>(footer_payload.size());
  std::array<std::byte, 4> size_buf{};
  WriteLittleEndian(payload_size, size_buf.data());
  buffer_.insert(buffer_.end(), size_buf.begin(), size_buf.end());

  // Flags (no compression for now)
  std::array<std::byte, 4> flags{};
  buffer_.insert(buffer_.end(), flags.begin(), flags.end());

  // Footer end magic
  buffer_.insert(buffer_.end(), reinterpret_cast<const std::byte*>(magic.data()),
                 reinterpret_cast<const std::byte*>(magic.data() + magic.size()));

  footer_size_ = static_cast<int64_t>(buffer_.size()) - footer_start;
  finished_ = true;
  return std::move(buffer_);
}

const std::vector<BlobMetadata>& PuffinWriter::written_blobs_metadata() const {
  return written_blobs_metadata_;
}

std::optional<int64_t> PuffinWriter::footer_size() const { return footer_size_; }

}  // namespace iceberg::puffin
