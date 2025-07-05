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

#include "iceberg/util/gzip_internal.h"

#include <zlib.h>

#include <cstring>
#include <vector>

#include "iceberg/util/macros.h"

namespace iceberg {

class ZlibImpl {
 public:
  ZlibImpl() { memset(&stream_, 0, sizeof(stream_)); }

  ~ZlibImpl() {
    if (initialized_) {
      inflateEnd(&stream_);
    }
  }

  Status Init() {
    // Maximum window size
    constexpr int kWindowBits = 15;
    // Determine if this is libz or gzip from header.
    constexpr int kDetectCodec = 32;
    int ret = inflateInit2(&stream_, kWindowBits | kDetectCodec);
    if (ret != Z_OK) {
      return DecompressError("inflateInit2 failed, result:{}", ret);
    }
    initialized_ = true;
    return {};
  }

  Result<std::string> Decompress(const std::string& compressed_data) {
    if (compressed_data.empty()) {
      return {};
    }
    if (!initialized_) {
      ICEBERG_RETURN_UNEXPECTED(Init());
    }
    stream_.avail_in = static_cast<uInt>(compressed_data.size());
    stream_.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(compressed_data.data()));

    // TODO(xiao.dong) magic buffer, can we get a estimated size from compressed data?
    std::vector<char> out_buffer(32 * 1024);
    std::string result;
    int ret = 0;
    do {
      stream_.avail_out = static_cast<uInt>(out_buffer.size());
      stream_.next_out = reinterpret_cast<Bytef*>(out_buffer.data());
      ret = inflate(&stream_, Z_NO_FLUSH);
      if (ret != Z_OK && ret != Z_STREAM_END) {
        return DecompressError("inflate failed, result:{}", ret);
      }
      result.append(out_buffer.data(), out_buffer.size() - stream_.avail_out);
    } while (ret != Z_STREAM_END);
    return result;
  }

 private:
  bool initialized_ = false;
  z_stream stream_;
};

GZipDecompressor::GZipDecompressor() : zlib_impl_(std::make_unique<ZlibImpl>()) {}

GZipDecompressor::~GZipDecompressor() = default;

Status GZipDecompressor::Init() { return zlib_impl_->Init(); }

Result<std::string> GZipDecompressor::Decompress(const std::string& compressed_data) {
  return zlib_impl_->Decompress(compressed_data);
}

}  // namespace iceberg
