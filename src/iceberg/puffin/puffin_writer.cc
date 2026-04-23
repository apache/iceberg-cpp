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

#include "iceberg/file_io.h"
#include "iceberg/puffin/json_serde_internal.h"
#include "iceberg/puffin/puffin_format.h"
#include "iceberg/util/endian.h"
#include "iceberg/util/macros.h"

namespace iceberg::puffin {

PuffinWriter::PuffinWriter(std::unique_ptr<PositionOutputStream> stream,
                           std::unordered_map<std::string, std::string> properties,
                           PuffinCompressionCodec default_codec, bool compress_footer)
    : stream_(std::move(stream)),
      properties_(std::move(properties)),
      default_codec_(default_codec),
      compress_footer_(compress_footer) {}

PuffinWriter::~PuffinWriter() = default;

Result<std::unique_ptr<PuffinWriter>> PuffinWriter::Make(
    std::unique_ptr<OutputFile> output_file,
    std::unordered_map<std::string, std::string> properties,
    PuffinCompressionCodec default_codec, bool compress_footer) {
  if (!output_file) {
    return InvalidArgument("output_file must not be null");
  }
  ICEBERG_ASSIGN_OR_RAISE(auto stream, output_file->CreateOrOverwrite());
  return std::unique_ptr<PuffinWriter>(new PuffinWriter(
      std::move(stream), std::move(properties), default_codec, compress_footer));
}

Status PuffinWriter::WriteBytes(std::span<const std::byte> data) {
  return stream_->Write(data);
}

Status PuffinWriter::WriteMagic() {
  const auto& magic = PuffinFormat::kMagicV1;
  return WriteBytes(std::span<const std::byte>(
      reinterpret_cast<const std::byte*>(magic.data()), magic.size()));
}

Status PuffinWriter::WriteHeader() {
  if (header_written_) return {};
  ICEBERG_RETURN_UNEXPECTED(WriteMagic());
  header_written_ = true;
  return {};
}

Result<BlobMetadata> PuffinWriter::Write(const Blob& blob) {
  if (finished_) {
    return Invalid("Writer already finished");
  }

  ICEBERG_RETURN_UNEXPECTED(WriteHeader());

  auto codec = blob.requested_compression.value_or(default_codec_);
  std::span<const std::byte> input_span(
      reinterpret_cast<const std::byte*>(blob.data.data()), blob.data.size());
  ICEBERG_ASSIGN_OR_RAISE(auto compressed, Compress(codec, input_span));

  ICEBERG_ASSIGN_OR_RAISE(auto offset, stream_->Position());
  ICEBERG_RETURN_UNEXPECTED(
      WriteBytes(std::span<const std::byte>(compressed.data(), compressed.size())));
  auto length = static_cast<int64_t>(compressed.size());

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

Status PuffinWriter::Finish() {
  if (finished_) {
    return Invalid("Writer already finished");
  }

  ICEBERG_RETURN_UNEXPECTED(WriteHeader());

  FileMetadata file_metadata{
      .blobs = written_blobs_metadata_,
      .properties = properties_,
  };

  auto footer_json = ToJsonString(file_metadata);
  std::vector<std::byte> footer_payload(
      reinterpret_cast<const std::byte*>(footer_json.data()),
      reinterpret_cast<const std::byte*>(footer_json.data() + footer_json.size()));

  // Compress footer if requested
  std::array<uint8_t, 4> flags{};
  if (compress_footer_) {
    ICEBERG_ASSIGN_OR_RAISE(
        footer_payload,
        Compress(PuffinFormat::kDefaultFooterCompressionCodec, footer_payload));
    SetFlag(flags, PuffinFlag::kFooterPayloadCompressed);
  }

  // Footer start magic
  ICEBERG_ASSIGN_OR_RAISE(auto footer_start, stream_->Position());
  ICEBERG_RETURN_UNEXPECTED(WriteMagic());

  // Footer payload
  ICEBERG_RETURN_UNEXPECTED(WriteBytes(footer_payload));

  // Footer struct: payload_size (4) + flags (4) + magic (4)
  auto payload_size = static_cast<int32_t>(footer_payload.size());
  std::array<std::byte, 4> size_buf{};
  WriteLittleEndian(payload_size, size_buf.data());
  ICEBERG_RETURN_UNEXPECTED(WriteBytes(size_buf));

  // Flags
  ICEBERG_RETURN_UNEXPECTED(WriteBytes(std::span<const std::byte>(
      reinterpret_cast<const std::byte*>(flags.data()), flags.size())));

  // Footer end magic
  ICEBERG_RETURN_UNEXPECTED(WriteMagic());

  ICEBERG_ASSIGN_OR_RAISE(auto end_pos, stream_->Position());
  footer_size_ = end_pos - footer_start;
  file_size_ = end_pos;
  finished_ = true;

  ICEBERG_RETURN_UNEXPECTED(stream_->Flush());
  return stream_->Close();
}

const std::vector<BlobMetadata>& PuffinWriter::written_blobs_metadata() const {
  return written_blobs_metadata_;
}

Result<int64_t> PuffinWriter::footer_size() const {
  if (!finished_) {
    return Invalid("Writer not finished yet");
  }
  return footer_size_;
}

Result<int64_t> PuffinWriter::file_size() const {
  if (!finished_) {
    return Invalid("Writer not finished yet");
  }
  return file_size_;
}

}  // namespace iceberg::puffin
