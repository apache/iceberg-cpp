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
#include <arrow/filesystem/localfs.h>
#if __has_include(<arrow/filesystem/s3fs.h>)
#include <arrow/filesystem/s3fs.h>
#define ICEBERG_ARROW_HAS_S3 1
#else
#define ICEBERG_ARROW_HAS_S3 0
#endif

#include "iceberg/arrow/arrow_file_io.h"
#include "iceberg/arrow/arrow_fs_file_io_internal.h"
#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/arrow/s3_properties.h"
#include "iceberg/util/macros.h"

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

#if ICEBERG_ARROW_HAS_S3
/// \brief Configure S3Options from a properties map.
///
/// \param properties The configuration properties map.
/// \return Configured S3Options.
::arrow::fs::S3Options ConfigureS3Options(
    const std::unordered_map<std::string, std::string>& properties) {
  ::arrow::fs::S3Options options;

  // Configure credentials
  auto access_key_it = properties.find(S3Properties::kAccessKeyId);
  auto secret_key_it = properties.find(S3Properties::kSecretAccessKey);
  auto session_token_it = properties.find(S3Properties::kSessionToken);

  if (access_key_it != properties.end() && secret_key_it != properties.end()) {
    if (session_token_it != properties.end()) {
      options.ConfigureAccessKey(access_key_it->second, secret_key_it->second,
                                 session_token_it->second);
    } else {
      options.ConfigureAccessKey(access_key_it->second, secret_key_it->second);
    }
  } else {
    // Use default credential chain (environment, instance profile, etc.)
    options.ConfigureDefaultCredentials();
  }

  // Configure region
  auto region_it = properties.find(S3Properties::kRegion);
  if (region_it != properties.end()) {
    options.region = region_it->second;
  }

  // Configure endpoint (for MinIO, LocalStack, etc.)
  auto endpoint_it = properties.find(S3Properties::kEndpoint);
  if (endpoint_it != properties.end()) {
    options.endpoint_override = endpoint_it->second;
  }

  // Configure path-style access (needed for MinIO)
  auto path_style_it = properties.find(S3Properties::kPathStyleAccess);
  if (path_style_it != properties.end()) {
    // Arrow's S3 path-style is controlled via endpoint scheme
    // For path-style access, we need to ensure the endpoint is properly configured
  }

  // Configure SSL
  auto ssl_it = properties.find(S3Properties::kSslEnabled);
  if (ssl_it != properties.end() && ssl_it->second == "false") {
    options.scheme = "http";
  }

  // Configure timeouts
  auto connect_timeout_it = properties.find(S3Properties::kConnectTimeoutMs);
  if (connect_timeout_it != properties.end()) {
    options.connect_timeout = std::stod(connect_timeout_it->second) / 1000.0;
  }

  auto socket_timeout_it = properties.find(S3Properties::kSocketTimeoutMs);
  if (socket_timeout_it != properties.end()) {
    options.request_timeout = std::stod(socket_timeout_it->second) / 1000.0;
  }

  return options;
}

/// \brief Create an S3 FileSystem with the given options.
///
/// \param options The S3Options to use.
/// \return A shared_ptr to the S3FileSystem, or an error.
Result<std::shared_ptr<::arrow::fs::FileSystem>> MakeS3FileSystem(
    const ::arrow::fs::S3Options& options) {
  ICEBERG_RETURN_UNEXPECTED(EnsureS3Initialized());
  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto fs, ::arrow::fs::S3FileSystem::Make(options));
  return fs;
}
#endif

Result<std::shared_ptr<::arrow::fs::FileSystem>> ResolveFileSystemFromUri(
    const std::string& uri, std::string* out_path) {
  if (IsS3Uri(uri)) {
    ICEBERG_RETURN_UNEXPECTED(EnsureS3Initialized());
  }
  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto fs, ::arrow::fs::FileSystemFromUri(uri, out_path));
  return fs;
}

/// \brief ArrowUriFileIO resolves FileSystem from URI for each operation.
///
/// This implementation is thread-safe as it creates a new FileSystem instance
/// for each operation. However, it may be less efficient than caching the
/// FileSystem. S3 initialization is done once per process.
class ArrowUriFileIO : public FileIO {
 public:
  Result<std::string> ReadFile(const std::string& file_location,
                               std::optional<size_t> length) override {
    std::string path;
    ICEBERG_ASSIGN_OR_RAISE(auto fs, ResolveFileSystemFromUri(file_location, &path));
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
    ICEBERG_ASSIGN_OR_RAISE(auto fs, ResolveFileSystemFromUri(file_location, &path));
    ICEBERG_ARROW_ASSIGN_OR_RETURN(auto file, fs->OpenOutputStream(path));
    ICEBERG_ARROW_RETURN_NOT_OK(file->Write(content.data(), content.size()));
    ICEBERG_ARROW_RETURN_NOT_OK(file->Flush());
    ICEBERG_ARROW_RETURN_NOT_OK(file->Close());
    return {};
  }

  Status DeleteFile(const std::string& file_location) override {
    std::string path;
    ICEBERG_ASSIGN_OR_RAISE(auto fs, ResolveFileSystemFromUri(file_location, &path));
    ICEBERG_ARROW_RETURN_NOT_OK(fs->DeleteFile(path));
    return {};
  }
};

}  // namespace

Result<std::unique_ptr<FileIO>> MakeS3FileIO(
    const std::string& uri,
    const std::unordered_map<std::string, std::string>& properties) {
  if (!IsS3Uri(uri)) {
    return InvalidArgument("S3 URI must start with s3://");
  }
#if !ICEBERG_ARROW_HAS_S3
  return NotImplemented("Arrow S3 support is not enabled");
#else
  // If properties are empty, use the simple URI-based resolution
  if (properties.empty()) {
    // Validate that S3 can be initialized and the URI is valid
    std::string path;
    ICEBERG_ASSIGN_OR_RAISE(auto fs, ResolveFileSystemFromUri(uri, &path));
    (void)path;
    (void)fs;
    return std::make_unique<ArrowUriFileIO>();
  }

  // Create S3FileSystem with explicit configuration
  auto options = ConfigureS3Options(properties);
  ICEBERG_ASSIGN_OR_RAISE(auto fs, MakeS3FileSystem(options));

  // Return ArrowFileSystemFileIO with the configured S3 filesystem
  return std::make_unique<ArrowFileSystemFileIO>(std::move(fs));
#endif
}

}  // namespace iceberg::arrow
