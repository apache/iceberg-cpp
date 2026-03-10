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

#include "iceberg/puffin/file_metadata.h"

#include <format>
#include <utility>

#include "iceberg/util/formatter_internal.h"

namespace iceberg::puffin {

namespace {
constexpr std::string_view kLz4CodecName = "lz4";
constexpr std::string_view kZstdCodecName = "zstd";
}  // namespace

std::string_view CodecName(PuffinCompressionCodec codec) {
  switch (codec) {
    case PuffinCompressionCodec::kNone:
      return "";
    case PuffinCompressionCodec::kLz4:
      return kLz4CodecName;
    case PuffinCompressionCodec::kZstd:
      return kZstdCodecName;
  }
  std::unreachable();
}

Result<PuffinCompressionCodec> PuffinCompressionCodecFromName(
    std::string_view codec_name) {
  if (codec_name.empty()) {
    return PuffinCompressionCodec::kNone;
  }
  if (codec_name == kLz4CodecName) {
    return PuffinCompressionCodec::kLz4;
  }
  if (codec_name == kZstdCodecName) {
    return PuffinCompressionCodec::kZstd;
  }
  return InvalidArgument("Unknown codec name: {}", codec_name);
}

std::string ToString(PuffinCompressionCodec codec) {
  return std::string(CodecName(codec));
}

std::string ToString(const Blob& blob) {
  std::string repr = "Blob[";
  std::format_to(std::back_inserter(repr), "type='{}',inputFields={},", blob.type,
                 blob.input_fields);
  std::format_to(std::back_inserter(repr), "snapshotId={},sequenceNumber={},",
                 blob.snapshot_id, blob.sequence_number);
  std::format_to(std::back_inserter(repr), "dataSize={}", blob.data.size());
  if (blob.requested_compression.has_value()) {
    std::format_to(std::back_inserter(repr), ",requestedCompression={}",
                   ToString(*blob.requested_compression));
  }
  if (!blob.properties.empty()) {
    std::format_to(std::back_inserter(repr), ",properties={}", blob.properties);
  }
  std::format_to(std::back_inserter(repr), "]");
  return repr;
}

std::string ToString(const BlobMetadata& blob_metadata) {
  std::string repr = "BlobMetadata[";
  std::format_to(std::back_inserter(repr), "type='{}',inputFields={},",
                 blob_metadata.type, blob_metadata.input_fields);
  std::format_to(std::back_inserter(repr), "snapshotId={},sequenceNumber={},",
                 blob_metadata.snapshot_id, blob_metadata.sequence_number);
  std::format_to(std::back_inserter(repr), "offset={},length={}", blob_metadata.offset,
                 blob_metadata.length);
  if (!blob_metadata.compression_codec.empty()) {
    std::format_to(std::back_inserter(repr), ",compressionCodec='{}'",
                   blob_metadata.compression_codec);
  }
  if (!blob_metadata.properties.empty()) {
    std::format_to(std::back_inserter(repr), ",properties={}", blob_metadata.properties);
  }
  std::format_to(std::back_inserter(repr), "]");
  return repr;
}

std::string ToString(const FileMetadata& file_metadata) {
  std::string repr = "FileMetadata[";
  std::format_to(std::back_inserter(repr), "blobs=[");
  for (size_t i = 0; i < file_metadata.blobs.size(); ++i) {
    if (i > 0) {
      std::format_to(std::back_inserter(repr), ",");
    }
    std::format_to(std::back_inserter(repr), "{}", ToString(file_metadata.blobs[i]));
  }
  std::format_to(std::back_inserter(repr), "]");
  if (!file_metadata.properties.empty()) {
    std::format_to(std::back_inserter(repr), ",properties={}", file_metadata.properties);
  }
  std::format_to(std::back_inserter(repr), "]");
  return repr;
}

}  // namespace iceberg::puffin
