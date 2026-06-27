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
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <arrow/filesystem/filesystem.h>
#if ICEBERG_S3_ENABLED
#  include <arrow/filesystem/s3fs.h>
#endif

#include "iceberg/arrow/arrow_io_internal.h"
#include "iceberg/arrow/arrow_io_util.h"
#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/arrow/s3/s3_properties.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/string_util.h"

namespace iceberg::arrow {

#if ICEBERG_S3_ENABLED

namespace {

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

// Splits any URI scheme off `endpoint` into `options.scheme`, returning the bare
// host[:port] that Arrow's `endpoint_override` expects.
std::string SplitEndpointScheme(std::string_view endpoint,
                                ::arrow::fs::S3Options& options) {
  if (const auto pos = endpoint.find("://"); pos != std::string_view::npos) {
    options.scheme = std::string(endpoint.substr(0, pos));
    endpoint = endpoint.substr(pos + 3);
  }
  return std::string(endpoint);
}

bool IsS3FileIOCredentialPrefix(std::string_view prefix) {
  return prefix == "s3" || prefix.starts_with("s3://") || prefix.starts_with("s3a://") ||
         prefix.starts_with("s3n://");
}

}  // namespace

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
  if (const auto* region = FindProperty(properties, S3Properties::kClientRegion);
      region != nullptr) {
    options.region = *region;
  }

  // Configure endpoint (for MinIO, LocalStack, etc.)
  if (const auto* endpoint = FindProperty(properties, S3Properties::kEndpoint);
      endpoint != nullptr) {
    options.endpoint_override = SplitEndpointScheme(*endpoint, options);
  } else if (const char* s3_endpoint_env = std::getenv("AWS_ENDPOINT_URL_S3");
             s3_endpoint_env != nullptr) {
    options.endpoint_override = SplitEndpointScheme(s3_endpoint_env, options);
  } else if (const char* endpoint_env = std::getenv("AWS_ENDPOINT_URL");
             endpoint_env != nullptr) {
    options.endpoint_override = SplitEndpointScheme(endpoint_env, options);
  }

  ICEBERG_ASSIGN_OR_RAISE(const auto path_style_access,
                          ParseOptionalBool(properties, S3Properties::kPathStyleAccess));
  if (path_style_access.has_value()) {
    options.force_virtual_addressing = !*path_style_access;
  }

  // Explicit `s3.ssl.enabled` overrides any endpoint-derived scheme.
  ICEBERG_ASSIGN_OR_RAISE(const auto ssl_enabled,
                          ParseOptionalBool(properties, S3Properties::kSslEnabled));
  if (ssl_enabled.has_value()) {
    options.scheme = *ssl_enabled ? "https" : "http";
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

namespace {

Result<std::shared_ptr<::arrow::fs::FileSystem>> BuildArrowS3FileSystem(
    const std::unordered_map<std::string, std::string>& properties) {
  ICEBERG_RETURN_UNEXPECTED(EnsureS3Initialized());
  ICEBERG_ASSIGN_OR_RAISE(auto options, ConfigureS3Options(properties));
  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto fs, ::arrow::fs::S3FileSystem::Make(options));
  return std::shared_ptr<::arrow::fs::FileSystem>(std::move(fs));
}

std::string CanonicalizeS3Scheme(std::string_view location) {
  for (std::string_view scheme : {"s3a://", "s3n://", "oss://"}) {
    if (location.starts_with(scheme)) {
      return std::string("s3://").append(location.substr(scheme.size()));
    }
  }
  return std::string(location);
}

class ArrowS3FileIO final : public FileIO, public SupportsStorageCredentials {
 public:
  ArrowS3FileIO(std::shared_ptr<::arrow::fs::FileSystem> arrow_fs,
                std::unordered_map<std::string, std::string> default_properties)
      : default_file_io_(std::move(arrow_fs)),
        default_properties_(std::move(default_properties)) {}

  Result<std::unique_ptr<InputFile>> NewInputFile(std::string file_location) override;

  Result<std::unique_ptr<InputFile>> NewInputFile(std::string file_location,
                                                  size_t length) override;

  Result<std::unique_ptr<OutputFile>> NewOutputFile(std::string file_location) override;

  Status DeleteFile(const std::string& file_location) override;

  Status DeleteFiles(const std::vector<std::string>& file_locations) override;

  Status SetStorageCredentials(
      const std::vector<StorageCredential>& storage_credentials) override;

  const std::vector<StorageCredential>& credentials() const override {
    return storage_credentials_;
  }

  SupportsStorageCredentials* AsSupportsStorageCredentials() override { return this; }

 private:
  ArrowFileSystemFileIO& FileIOForPath(std::string_view location);

  ArrowFileSystemFileIO default_file_io_;
  std::unordered_map<std::string, std::string> default_properties_;
  std::vector<StorageCredential> storage_credentials_;
  std::vector<std::pair<std::string, std::unique_ptr<ArrowFileSystemFileIO>>>
      file_io_by_prefix_;
};

Status ArrowS3FileIO::SetStorageCredentials(
    const std::vector<StorageCredential>& storage_credentials) {
  std::vector<std::pair<std::string, std::unique_ptr<ArrowFileSystemFileIO>>>
      file_io_by_prefix;
  file_io_by_prefix.reserve(storage_credentials.size());
  // TODO(gangwu): Refresh vended credentials via credentials.uri before tokens expire.
  for (const auto& credential : storage_credentials) {
    ICEBERG_RETURN_UNEXPECTED(credential.Validate());
    if (!IsS3FileIOCredentialPrefix(credential.prefix)) {
      return NotSupported(
          "Storage credential prefix '{}' is unsupported by Arrow S3 FileIO",
          credential.prefix);
    }
    auto properties = default_properties_;
    for (const auto& [key, value] : credential.config) {
      properties[key] = value;
    }
    ICEBERG_ASSIGN_OR_RAISE(auto fs, BuildArrowS3FileSystem(properties));
    file_io_by_prefix.emplace_back(
        CanonicalizeS3Scheme(credential.prefix),
        std::make_unique<ArrowFileSystemFileIO>(std::move(fs)));
  }
  file_io_by_prefix_ = std::move(file_io_by_prefix);
  storage_credentials_ = storage_credentials;
  return {};
}

ArrowFileSystemFileIO& ArrowS3FileIO::FileIOForPath(std::string_view location) {
  if (file_io_by_prefix_.empty()) {
    return default_file_io_;
  }
  const std::string canonical = CanonicalizeS3Scheme(location);
  ArrowFileSystemFileIO* best = &default_file_io_;
  size_t best_len = 0;
  for (const auto& [prefix, file_io] : file_io_by_prefix_) {
    if (prefix.size() > best_len && canonical.starts_with(prefix)) {
      best = file_io.get();
      best_len = prefix.size();
    }
  }
  return *best;
}

Result<std::unique_ptr<InputFile>> ArrowS3FileIO::NewInputFile(
    std::string file_location) {
  return FileIOForPath(file_location).NewInputFile(std::move(file_location));
}

Result<std::unique_ptr<InputFile>> ArrowS3FileIO::NewInputFile(std::string file_location,
                                                               size_t length) {
  return FileIOForPath(file_location).NewInputFile(std::move(file_location), length);
}

Result<std::unique_ptr<OutputFile>> ArrowS3FileIO::NewOutputFile(
    std::string file_location) {
  return FileIOForPath(file_location).NewOutputFile(std::move(file_location));
}

Status ArrowS3FileIO::DeleteFile(const std::string& file_location) {
  return FileIOForPath(file_location).DeleteFile(file_location);
}

Status ArrowS3FileIO::DeleteFiles(const std::vector<std::string>& file_locations) {
  std::unordered_map<ArrowFileSystemFileIO*, std::vector<std::string>> locations_by_io;
  for (const auto& file_location : file_locations) {
    locations_by_io[&FileIOForPath(file_location)].push_back(file_location);
  }
  for (auto& [file_io, locations] : locations_by_io) {
    ICEBERG_RETURN_UNEXPECTED(file_io->DeleteFiles(locations));
  }
  return {};
}

}  // namespace

Result<std::unique_ptr<FileIO>> MakeS3FileIO(
    const std::unordered_map<std::string, std::string>& properties) {
  // Uses default credentials if properties are empty.
  ICEBERG_ASSIGN_OR_RAISE(auto fs, BuildArrowS3FileSystem(properties));
  return std::make_unique<ArrowS3FileIO>(std::move(fs), properties);
}

Status FinalizeS3() {
  auto status = ::arrow::fs::FinalizeS3();
  ICEBERG_ARROW_RETURN_NOT_OK(status);
  return {};
}

#else

Result<std::unique_ptr<FileIO>> MakeS3FileIO(
    [[maybe_unused]] const std::unordered_map<std::string, std::string>& properties) {
  return NotSupported("Arrow S3 support is not enabled");
}

Status FinalizeS3() { return NotSupported("Arrow S3 support is not enabled"); }

#endif

}  // namespace iceberg::arrow
