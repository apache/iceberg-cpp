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
#include <stdexcept>

#include <arrow/filesystem/filesystem.h>
#ifdef ICEBERG_S3_ENABLED
#  include <arrow/filesystem/s3fs.h>
#  define ICEBERG_ARROW_HAS_S3 1
#else
#  define ICEBERG_ARROW_HAS_S3 0
#endif

#include "iceberg/arrow/arrow_file_io.h"
#include "iceberg/arrow/arrow_fs_file_io_internal.h"
#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/arrow/s3_properties.h"
#include "iceberg/util/macros.h"

namespace iceberg::arrow {

namespace {

Status EnsureS3Initialized() {
#if ICEBERG_ARROW_HAS_S3
  static std::once_flag init_flag;
  static ::arrow::Status init_status = ::arrow::Status::OK();
  std::call_once(init_flag, []() {
    ::arrow::fs::S3GlobalOptions options;
    init_status = ::arrow::fs::InitializeS3(options);
  });
  if (!init_status.ok()) {
    return std::unexpected(Error{.kind = ::iceberg::arrow::ToErrorKind(init_status),
                                 .message = init_status.ToString()});
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
Result<::arrow::fs::S3Options> ConfigureS3Options(
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
  } else {
    // Fall back to AWS standard environment variables for endpoint override
    const char* s3_endpoint_env = std::getenv("AWS_ENDPOINT_URL_S3");
    if (s3_endpoint_env != nullptr) {
      options.endpoint_override = s3_endpoint_env;
    } else {
      const char* endpoint_env = std::getenv("AWS_ENDPOINT_URL");
      if (endpoint_env != nullptr) {
        options.endpoint_override = endpoint_env;
      }
    }
  }

  auto path_style_it = properties.find(S3Properties::kPathStyleAccess);
  if (path_style_it != properties.end() && path_style_it->second == "true") {
    options.force_virtual_addressing = false;
  }

  // Configure SSL
  auto ssl_it = properties.find(S3Properties::kSslEnabled);
  if (ssl_it != properties.end() && ssl_it->second == "false") {
    options.scheme = "http";
  }

  // Configure timeouts
  auto connect_timeout_it = properties.find(S3Properties::kConnectTimeoutMs);
  if (connect_timeout_it != properties.end()) {
    try {
      options.connect_timeout = std::stod(connect_timeout_it->second) / 1000.0;
    } catch (const std::exception& e) {
      return InvalidArgument("Invalid {}: '{}' ({})", S3Properties::kConnectTimeoutMs,
                             connect_timeout_it->second, e.what());
    }
  }

  auto socket_timeout_it = properties.find(S3Properties::kSocketTimeoutMs);
  if (socket_timeout_it != properties.end()) {
    try {
      options.request_timeout = std::stod(socket_timeout_it->second) / 1000.0;
    } catch (const std::exception& e) {
      return InvalidArgument("Invalid {}: '{}' ({})", S3Properties::kSocketTimeoutMs,
                             socket_timeout_it->second, e.what());
    }
  }

  return options;
}
#endif

}  // namespace

Result<std::unique_ptr<FileIO>> MakeS3FileIO(
    const std::string& uri,
    const std::unordered_map<std::string, std::string>& properties) {
  if (!uri.starts_with("s3://")) {
    return InvalidArgument("S3 URI must start with s3://");
  }
#if !ICEBERG_ARROW_HAS_S3
  return NotImplemented("Arrow S3 support is not enabled");
#else
  ICEBERG_RETURN_UNEXPECTED(EnsureS3Initialized());

  // Configure S3 options from properties (uses default credentials if empty)
  ICEBERG_ASSIGN_OR_RAISE(auto options, ConfigureS3Options(properties));
  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto fs, ::arrow::fs::S3FileSystem::Make(options));

  return std::make_unique<ArrowFileSystemFileIO>(std::move(fs));
#endif
}

}  // namespace iceberg::arrow
