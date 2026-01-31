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

#include <cstdlib>
#include <mutex>
#include <string_view>

#include <arrow/filesystem/filesystem.h>
#if __has_include(<arrow/filesystem/s3fs.h>)
#include <arrow/filesystem/s3fs.h>
#define ICEBERG_ARROW_HAS_S3 1
#else
#define ICEBERG_ARROW_HAS_S3 0
#endif

#include "iceberg/arrow/arrow_file_io.h"
#include "iceberg/arrow/arrow_status_internal.h"

namespace iceberg::arrow {

namespace {

bool IsS3Uri(std::string_view uri) { return uri.rfind("s3://", 0) == 0; }

Status EnsureS3Initialized() {
#if ICEBERG_ARROW_HAS_S3
  static std::once_flag init_flag;
  static ::arrow::Status init_status = ::arrow::Status::OK();
  std::call_once(init_flag, []() {
    ::arrow::fs::S3GlobalOptions options;
    init_status = ::arrow::fs::InitializeS3(options);
    if (init_status.ok()) {
      std::atexit([]() { (void)::arrow::fs::FinalizeS3(); });
    }
  });
  if (!init_status.ok()) {
    return std::unexpected<Error>{
        {.kind = ::iceberg::arrow::ToErrorKind(init_status),
         .message = init_status.ToString()}};
  }
  return {};
#else
  return NotImplemented("Arrow S3 support is not enabled");
#endif
}

Result<std::shared_ptr<::arrow::fs::FileSystem>> ResolveFileSystemFromUri(
    const std::string& uri, std::string* out_path) {
  if (IsS3Uri(uri)) {
    auto init_status = EnsureS3Initialized();
    if (!init_status.has_value()) {
      return std::unexpected<Error>(init_status.error());
    }
  }
  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto fs, ::arrow::fs::FileSystemFromUri(uri, out_path));
  return fs;
}

class ArrowUriFileIO : public FileIO {
 public:
  Result<std::string> ReadFile(const std::string& file_location,
                               std::optional<size_t> length) override {
    std::string path;
    auto fs_result = ResolveFileSystemFromUri(file_location, &path);
    if (!fs_result.has_value()) {
      return std::unexpected<Error>(fs_result.error());
    }
    auto fs = std::move(fs_result).value();
    ::arrow::fs::FileInfo file_info(path);
    if (length.has_value()) {
      file_info.set_size(length.value());
    }
    std::string content;
    ICEBERG_ARROW_ASSIGN_OR_RETURN(auto file, fs->OpenInputFile(file_info));
    ICEBERG_ARROW_ASSIGN_OR_RETURN(auto file_size, file->GetSize());

    content.resize(file_size);
    size_t remain = file_size;
    size_t offset = 0;
    while (remain > 0) {
      size_t read_length = std::min(remain, static_cast<size_t>(1024 * 1024));
      ICEBERG_ARROW_ASSIGN_OR_RETURN(
          auto read_bytes,
          file->Read(read_length, reinterpret_cast<uint8_t*>(&content[offset])));
      remain -= read_bytes;
      offset += read_bytes;
    }

    return content;
  }

  Status WriteFile(const std::string& file_location,
                   std::string_view content) override {
    std::string path;
    auto fs_result = ResolveFileSystemFromUri(file_location, &path);
    if (!fs_result.has_value()) {
      return std::unexpected<Error>(fs_result.error());
    }
    auto fs = std::move(fs_result).value();
    ICEBERG_ARROW_ASSIGN_OR_RETURN(auto file, fs->OpenOutputStream(path));
    ICEBERG_ARROW_RETURN_NOT_OK(file->Write(content.data(), content.size()));
    ICEBERG_ARROW_RETURN_NOT_OK(file->Flush());
    ICEBERG_ARROW_RETURN_NOT_OK(file->Close());
    return {};
  }

  Status DeleteFile(const std::string& file_location) override {
    std::string path;
    auto fs_result = ResolveFileSystemFromUri(file_location, &path);
    if (!fs_result.has_value()) {
      return std::unexpected<Error>(fs_result.error());
    }
    auto fs = std::move(fs_result).value();
    ICEBERG_ARROW_RETURN_NOT_OK(fs->DeleteFile(path));
    return {};
  }
};

}  // namespace

Result<std::unique_ptr<FileIO>> MakeS3FileIO(const std::string& uri) {
  if (!IsS3Uri(uri)) {
    return InvalidArgument("S3 URI must start with s3://");
  }
#if !ICEBERG_ARROW_HAS_S3
  return NotImplemented("Arrow S3 support is not enabled");
#else
  std::string path;
  auto fs_result = ResolveFileSystemFromUri(uri, &path);
  if (!fs_result.has_value()) {
    return std::unexpected<Error>(fs_result.error());
  }
  (void)path;
  return std::make_unique<ArrowUriFileIO>();
#endif
}

}  // namespace iceberg::arrow
