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
#include <optional>
#include <string>
#include <string_view>

#include <arrow/filesystem/filesystem.h>
#if ICEBERG_S3_ENABLED
#  include <arrow/filesystem/s3fs.h>
#endif

#include "iceberg/arrow/arrow_file_io.h"
#include "iceberg/arrow/arrow_fs_file_io_internal.h"
#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/arrow/s3/s3_properties.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/string_util.h"

namespace iceberg::arrow {

namespace {

#if ICEBERG_S3_ENABLED
const std::string* FindProperty(
    const std::unordered_map<std::string, std::string>& properties,
    std::string_view key) {
  auto it = properties.find(std::string(key));
  return it == properties.end() ? nullptr : &it->second;
}

Result<std::optional<bool>> ParseOptionalBool(
    const std::unordered_map<std::string, std::string>& properties,
    std::string_view key) {
  const auto* value = FindProperty(properties, key);
  if (value == nullptr) {
    return std::nullopt;
  }
  if (*value == "true") {
    return true;
  }
  if (*value == "false") {
    return false;
  }
  return InvalidArgument(R"("{}" must be "true" or "false")", key);
}

Status EnsureS3Initialized() {
  static const ::arrow::Status init_status = []() {
    auto options = ::arrow::fs::S3GlobalOptions::Defaults();
    return ::arrow::fs::InitializeS3(options);
  }();
  if (!init_status.ok()) {
    return std::unexpected(Error{.kind = ::iceberg::arrow::ToErrorKind(init_status),
                                 .message = init_status.ToString()});
  }
  return {};
}

/// \brief Configure S3Options from a properties map.
///
/// \param properties The configuration properties map.
/// \return Configured S3Options.
Result<::arrow::fs::S3Options> ConfigureS3Options(
    const std::unordered_map<std::string, std::string>& properties) {
  auto options = ::arrow::fs::S3Options::Defaults();

  // Configure credentials
  const auto* access_key = FindProperty(properties, S3Properties::kAccessKeyId);
  const auto* secret_key = FindProperty(properties, S3Properties::kSecretAccessKey);
  const auto* session_token = FindProperty(properties, S3Properties::kSessionToken);

  if ((access_key == nullptr) != (secret_key == nullptr)) {
    return InvalidArgument(
        "S3 client access key ID and secret access key must be set at the same time");
  }
  if (access_key != nullptr) {
    if (session_token != nullptr) {
      options.ConfigureAccessKey(*access_key, *secret_key, *session_token);
    } else {
      options.ConfigureAccessKey(*access_key, *secret_key);
    }
  }

  // Configure region
  if (const auto* region = FindProperty(properties, S3Properties::kRegion);
      region != nullptr) {
    options.region = *region;
  }

  // Configure endpoint (for MinIO, LocalStack, etc.)
  if (const auto* endpoint = FindProperty(properties, S3Properties::kEndpoint);
      endpoint != nullptr) {
    options.endpoint_override = *endpoint;
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

  ICEBERG_ASSIGN_OR_RAISE(const auto path_style_access,
                          ParseOptionalBool(properties, S3Properties::kPathStyleAccess));
  if (path_style_access.has_value()) {
    options.force_virtual_addressing = !*path_style_access;
  }

  // Configure SSL
  ICEBERG_ASSIGN_OR_RAISE(const auto ssl_enabled,
                          ParseOptionalBool(properties, S3Properties::kSslEnabled));
  if (ssl_enabled.has_value() && !*ssl_enabled) {
    options.scheme = "http";
  }

  // Configure timeouts
  auto connect_timeout_it = properties.find(std::string(S3Properties::kConnectTimeoutMs));
  if (connect_timeout_it != properties.end()) {
    ICEBERG_ASSIGN_OR_RAISE(auto timeout_ms,
                            StringUtils::ParseNumber<double>(connect_timeout_it->second));
    options.connect_timeout = timeout_ms / 1000.0;
  }

  auto socket_timeout_it = properties.find(std::string(S3Properties::kSocketTimeoutMs));
  if (socket_timeout_it != properties.end()) {
    ICEBERG_ASSIGN_OR_RAISE(auto timeout_ms,
                            StringUtils::ParseNumber<double>(socket_timeout_it->second));
    options.request_timeout = timeout_ms / 1000.0;
  }

  return options;
}
#endif

}  // namespace

Result<std::unique_ptr<FileIO>> MakeS3FileIO(
    const std::unordered_map<std::string, std::string>& properties) {
#if ICEBERG_S3_ENABLED
  ICEBERG_RETURN_UNEXPECTED(EnsureS3Initialized());

  // Configure S3 options from properties (uses default credentials if empty)
  ICEBERG_ASSIGN_OR_RAISE(auto options, ConfigureS3Options(properties));
  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto fs, ::arrow::fs::S3FileSystem::Make(options));

  return std::make_unique<ArrowFileSystemFileIO>(std::move(fs));
#else
  return NotSupported("Arrow S3 support is not enabled");
#endif
}

Status FinalizeS3() {
#if ICEBERG_S3_ENABLED
  auto status = ::arrow::fs::FinalizeS3();
  ICEBERG_ARROW_RETURN_NOT_OK(status);
  return {};
#else
  return NotSupported("Arrow S3 support is not enabled");
#endif
}

}  // namespace iceberg::arrow
