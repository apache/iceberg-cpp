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

/// \file iceberg/encryption/aes_gcm_stream.h
/// AES GCM Stream (AGS1) files, see format/gcm-stream-spec.md.

#include <array>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

#include "iceberg/file_io.h"
#include "iceberg/iceberg_bundle_export.h"
#include "iceberg/result.h"

namespace iceberg {

/// \brief AES GCM Stream format constants.
///
/// AGS1 stores the block length in its header. This implementation only reads and
/// writes 1 MiB blocks, matching Java, rather than supporting other block lengths.
///
/// For Java compatibility, an empty file contains one zero-length plaintext block
/// with a nonce and tag. The AGS1 spec describes a non-empty last block and does not
/// explicitly define this empty-file representation.
struct AesGcmStream {
  static constexpr int32_t kPlainBlockSize = 1024 * 1024;
  static constexpr int32_t kNonceLength = 12;
  static constexpr int32_t kGcmTagLength = 16;
  static constexpr int32_t kCipherBlockSize =
      kPlainBlockSize + kNonceLength + kGcmTagLength;
  static constexpr std::array<char, 4> kMagic = {'A', 'G', 'S', '1'};
  static constexpr int32_t kHeaderLength = 8;  // magic + plain block size (LE int32)
  static constexpr int64_t kMinStreamLength =
      kHeaderLength + kNonceLength + kGcmTagLength;

  /// \brief Plaintext length of a stream with the given encrypted length.
  ///
  /// Returns an error if the length is shorter than an empty stream, implies more
  /// than int32 max blocks, or ends in a block too short to hold a nonce and tag.
  ICEBERG_BUNDLE_EXPORT static Result<int64_t> PlaintextLength(int64_t encrypted_length);
};

/// \brief An InputFile that decrypts an AGS1 file.
///
/// The encrypted length must come from a trusted source (the key metadata), so that
/// truncation of the file is detected.
class ICEBERG_BUNDLE_EXPORT AesGcmInputFile : public InputFile {
 public:
  AesGcmInputFile(std::unique_ptr<InputFile> source, std::vector<uint8_t> key,
                  std::vector<uint8_t> aad_prefix, int64_t encrypted_length);

  std::string_view location() const override { return source_->location(); }
  Result<int64_t> Size() const override;
  Result<std::unique_ptr<SeekableInputStream>> Open() override;

 private:
  std::unique_ptr<InputFile> source_;
  std::vector<uint8_t> key_;
  std::vector<uint8_t> aad_prefix_;
  int64_t encrypted_length_;
};

/// \brief An OutputFile that writes an AGS1 file.
class ICEBERG_BUNDLE_EXPORT AesGcmOutputFile : public OutputFile {
 public:
  AesGcmOutputFile(std::unique_ptr<OutputFile> target, std::vector<uint8_t> key,
                   std::vector<uint8_t> aad_prefix);

  std::string_view location() const override { return target_->location(); }
  Result<std::unique_ptr<PositionOutputStream>> Create() override;
  Result<std::unique_ptr<PositionOutputStream>> CreateOrOverwrite() override;

 private:
  std::unique_ptr<OutputFile> target_;
  std::vector<uint8_t> key_;
  std::vector<uint8_t> aad_prefix_;
};

}  // namespace iceberg
