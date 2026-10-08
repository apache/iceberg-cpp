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

#include "iceberg/encryption/standard_key_metadata.h"

#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

void WriteLong(int64_t value, std::vector<uint8_t>& out) {
  // Avro zig-zag varint encoding
  auto n = (static_cast<uint64_t>(value) << 1) ^ static_cast<uint64_t>(value >> 63);
  while (n & ~0x7FULL) {
    out.push_back(static_cast<uint8_t>((n & 0x7F) | 0x80));
    n >>= 7;
  }
  out.push_back(static_cast<uint8_t>(n));
}

void WriteBytes(std::span<const uint8_t> bytes, std::vector<uint8_t>& out) {
  WriteLong(static_cast<int64_t>(bytes.size()), out);
  out.insert(out.end(), bytes.begin(), bytes.end());
}

class Decoder {
 public:
  explicit Decoder(std::span<const uint8_t> buffer) : buffer_(buffer) {}

  Result<int64_t> ReadLong() {
    uint64_t n = 0;
    int shift = 0;
    while (true) {
      if (pos_ >= buffer_.size()) {
        return Invalid("Truncated key metadata");
      }
      if (shift > 63) {
        return Invalid("Invalid varint in key metadata");
      }
      uint8_t b = buffer_[pos_++];
      n |= static_cast<uint64_t>(b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        break;
      }
      shift += 7;
    }
    return static_cast<int64_t>((n >> 1) ^ -(n & 1));
  }

  Result<std::vector<uint8_t>> ReadBytes() {
    ICEBERG_ASSIGN_OR_RAISE(auto length, ReadLong());
    if (length < 0 || static_cast<uint64_t>(length) > buffer_.size() - pos_) {
      return Invalid("Invalid bytes length in key metadata: {}", length);
    }
    std::vector<uint8_t> bytes(buffer_.begin() + pos_, buffer_.begin() + pos_ + length);
    pos_ += length;
    return bytes;
  }

  /// \brief Read the branch index of a ["null", T] union; true if T is present.
  Result<bool> ReadOptionalBranch() {
    ICEBERG_ASSIGN_OR_RAISE(auto branch, ReadLong());
    if (branch != 0 && branch != 1) {
      return Invalid("Invalid union branch in key metadata: {}", branch);
    }
    return branch == 1;
  }

 private:
  std::span<const uint8_t> buffer_;
  size_t pos_ = 0;
};

}  // namespace

Result<StandardKeyMetadata> StandardKeyMetadata::Parse(std::span<const uint8_t> buffer) {
  if (buffer.empty()) {
    return Invalid("Empty key metadata");
  }
  if (buffer[0] != kV1) {
    return NotSupported("Unsupported key metadata schema version: {}",
                        static_cast<int>(buffer[0]));
  }

  Decoder decoder(buffer.subspan(1));
  StandardKeyMetadata metadata;
  ICEBERG_ASSIGN_OR_RAISE(metadata.encryption_key, decoder.ReadBytes());
  ICEBERG_ASSIGN_OR_RAISE(auto has_aad_prefix, decoder.ReadOptionalBranch());
  if (has_aad_prefix) {
    ICEBERG_ASSIGN_OR_RAISE(metadata.aad_prefix, decoder.ReadBytes());
  }
  ICEBERG_ASSIGN_OR_RAISE(auto has_file_length, decoder.ReadOptionalBranch());
  if (has_file_length) {
    ICEBERG_ASSIGN_OR_RAISE(metadata.file_length, decoder.ReadLong());
  }
  // Trailing bytes may come from a newer schema version with extra fields; ignore them,
  // as Java does.
  return metadata;
}

std::vector<uint8_t> StandardKeyMetadata::Serialize() const {
  std::vector<uint8_t> out;
  out.push_back(kV1);
  WriteBytes(encryption_key, out);
  if (aad_prefix.has_value()) {
    WriteLong(1, out);
    WriteBytes(*aad_prefix, out);
  } else {
    WriteLong(0, out);
  }
  if (file_length.has_value()) {
    WriteLong(1, out);
    WriteLong(*file_length, out);
  } else {
    WriteLong(0, out);
  }
  return out;
}

StandardKeyMetadata StandardKeyMetadata::WithFileLength(int64_t length) const {
  StandardKeyMetadata copy = *this;
  copy.file_length = length;
  return copy;
}

}  // namespace iceberg
