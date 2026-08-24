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

#include <algorithm>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
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
#include "iceberg/logging/log_macros.h"
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
  if (StringUtils::EqualsIgnoreCase(*value, "true")) {
    return true;
  }
  if (StringUtils::EqualsIgnoreCase(*value, "false")) {
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

// Rewrites any alias of `s3://` (any case — routing is case-insensitive) to
// exactly that, so locations and credential prefixes compare equal. An alias
// missing from kS3Schemes would silently stop matching its credential.
std::string CanonicalizeS3Scheme(std::string_view location) {
  const auto separator = location.find("://");
  if (separator != std::string_view::npos && IsS3Scheme(location.substr(0, separator))) {
    return std::string("s3://").append(location.substr(separator + 3));
  }
  return std::string(location);
}

class ArrowS3FileIO final : public FileIO, public SupportsStorageCredentials {
 public:
  ArrowS3FileIO(std::shared_ptr<::arrow::fs::FileSystem> arrow_fs,
                std::unordered_map<std::string, std::string> default_properties)
      : default_file_io_(std::make_shared<ArrowFileSystemFileIO>(std::move(arrow_fs))),
        default_properties_(std::move(default_properties)) {}

  Result<std::unique_ptr<InputFile>> NewInputFile(std::string file_location) override;

  Result<std::unique_ptr<InputFile>> NewInputFile(std::string file_location,
                                                  size_t length) override;

  Result<std::unique_ptr<OutputFile>> NewOutputFile(std::string file_location) override;

  Status DeleteFile(const std::string& file_location) override;

  Status DeleteFiles(const std::vector<std::string>& file_locations) override;

  Status SetStorageCredentials(
      const std::vector<StorageCredential>& storage_credentials) override;

  std::vector<StorageCredential> credentials() const override {
    std::shared_lock lock(mutex_);
    return storage_credentials_;
  }

  SupportsStorageCredentials* AsSupportsStorageCredentials() override { return this; }

 private:
  /// \brief Delegate serving `location`, pinned by the caller against a
  /// concurrent credential install.
  std::shared_ptr<ArrowFileSystemFileIO> FileIOForPath(std::string_view location);

  using DelegatesByPrefix =
      std::vector<std::pair<std::string, std::shared_ptr<ArrowFileSystemFileIO>>>;

  /// \brief Longest-prefix match against one consistent view of the delegates.
  static std::shared_ptr<ArrowFileSystemFileIO> MatchDelegate(
      const std::shared_ptr<ArrowFileSystemFileIO>& fallback,
      const DelegatesByPrefix& by_prefix, std::string_view location);

  /// \brief Build a delegate for each credential this FileIO can serve.
  ///
  /// Lock-free on purpose: building an S3 client can reach out to discover a
  /// bucket region, which would stall every concurrent operation. Reads no
  /// mutable member state.
  Result<DelegatesByPrefix> BuildDelegates(
      const std::vector<StorageCredential>& storage_credentials) const;

  /// \brief Swap in credentials and delegates, handing back the retired ones.
  ///
  /// Callers must hold `mutex_` exclusively and let the returned generation
  /// destruct only after releasing it: tearing down an S3 client can block on
  /// in-flight requests, which would stall every operation.
  void InstallCredentials(std::vector<StorageCredential>& storage_credentials,
                          DelegatesByPrefix& delegates);

  std::shared_ptr<ArrowFileSystemFileIO> default_file_io_;
  std::unordered_map<std::string, std::string> default_properties_;
  // Guards everything below; shared because reads happen per file operation.
  mutable std::shared_mutex mutex_;
  std::vector<StorageCredential> storage_credentials_;
  DelegatesByPrefix file_io_by_prefix_;
};

Status ArrowS3FileIO::SetStorageCredentials(
    const std::vector<StorageCredential>& storage_credentials) {
  ICEBERG_ASSIGN_OR_RAISE(auto delegates, BuildDelegates(storage_credentials));
  auto credentials = storage_credentials;
  {
    std::unique_lock lock(mutex_);
    InstallCredentials(credentials, delegates);
  }
  // `credentials` and `delegates` now hold the retired generation and destruct
  // here, outside the lock.
  return {};
}

Result<ArrowS3FileIO::DelegatesByPrefix> ArrowS3FileIO::BuildDelegates(
    const std::vector<StorageCredential>& storage_credentials) const {
  DelegatesByPrefix delegates;
  delegates.reserve(storage_credentials.size());
  // TODO(gangwu): Refresh vended credentials via credentials.uri before tokens expire.
  for (const auto& credential : storage_credentials) {
    ICEBERG_RETURN_UNEXPECTED(credential.Validate());
    // A server may vend credentials for several storage systems at once;
    // non-S3 prefixes are skipped, not rejected (Java S3FileIO filters
    // credentials by the "s3" prefix).
    if (!IsS3CredentialPrefix(credential.prefix)) {
      continue;
    }
    auto properties = default_properties_;
    for (const auto& [key, value] : credential.config) {
      properties[key] = value;
    }
    ICEBERG_ASSIGN_OR_RAISE(auto fs, BuildArrowS3FileSystem(properties));
    delegates.emplace_back(CanonicalizeS3Scheme(credential.prefix),
                           std::make_shared<ArrowFileSystemFileIO>(std::move(fs)));
  }
  if (delegates.empty() && !storage_credentials.empty()) {
    // Silent skipping of every vended credential is hard to diagnose: S3 access
    // would proceed with the default credentials and fail only at IO time.
    ICEBERG_LOG_WARN(
        "None of the {} vended storage credential(s) has an S3-compatible prefix; "
        "S3 access will use the default credentials",
        storage_credentials.size());
  }
  return delegates;
}

void ArrowS3FileIO::InstallCredentials(
    std::vector<StorageCredential>& storage_credentials, DelegatesByPrefix& delegates) {
  file_io_by_prefix_.swap(delegates);
  storage_credentials_.swap(storage_credentials);
}

std::shared_ptr<ArrowFileSystemFileIO> ArrowS3FileIO::MatchDelegate(
    const std::shared_ptr<ArrowFileSystemFileIO>& fallback,
    const DelegatesByPrefix& by_prefix, std::string_view location) {
  if (by_prefix.empty()) {
    return fallback;
  }
  const std::string canonical = CanonicalizeS3Scheme(location);
  auto best = fallback;
  size_t best_len = 0;
  for (const auto& [prefix, file_io] : by_prefix) {
    if (prefix.size() > best_len && canonical.starts_with(prefix)) {
      best = file_io;
      best_len = prefix.size();
    }
  }
  return best;
}

std::shared_ptr<ArrowFileSystemFileIO> ArrowS3FileIO::FileIOForPath(
    std::string_view location) {
  std::shared_lock lock(mutex_);
  return MatchDelegate(default_file_io_, file_io_by_prefix_, location);
}

Result<std::unique_ptr<InputFile>> ArrowS3FileIO::NewInputFile(
    std::string file_location) {
  return FileIOForPath(file_location)->NewInputFile(std::move(file_location));
}

Result<std::unique_ptr<InputFile>> ArrowS3FileIO::NewInputFile(std::string file_location,
                                                               size_t length) {
  return FileIOForPath(file_location)->NewInputFile(std::move(file_location), length);
}

Result<std::unique_ptr<OutputFile>> ArrowS3FileIO::NewOutputFile(
    std::string file_location) {
  return FileIOForPath(file_location)->NewOutputFile(std::move(file_location));
}

Status ArrowS3FileIO::DeleteFile(const std::string& file_location) {
  return FileIOForPath(file_location)->DeleteFile(file_location);
}

Status ArrowS3FileIO::DeleteFiles(const std::vector<std::string>& file_locations) {
  // One snapshot so the whole batch matches the same delegate generation; only
  // ever a handful of delegates, so a linear scan beats hashing.
  std::shared_ptr<ArrowFileSystemFileIO> fallback;
  DelegatesByPrefix by_prefix;
  {
    std::shared_lock lock(mutex_);
    fallback = default_file_io_;
    by_prefix = file_io_by_prefix_;
  }
  std::vector<std::pair<std::shared_ptr<ArrowFileSystemFileIO>, std::vector<std::string>>>
      locations_by_io;
  for (const auto& file_location : file_locations) {
    auto file_io = MatchDelegate(fallback, by_prefix, file_location);
    auto it = std::ranges::find_if(
        locations_by_io, [&](const auto& entry) { return entry.first == file_io; });
    if (it == locations_by_io.end()) {
      locations_by_io.emplace_back(std::move(file_io),
                                   std::vector<std::string>{file_location});
    } else {
      it->second.push_back(file_location);
    }
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
