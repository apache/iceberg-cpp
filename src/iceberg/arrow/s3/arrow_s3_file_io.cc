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
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <format>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <arrow/filesystem/filesystem.h>
#if ICEBERG_S3_ENABLED
#  include <arrow/filesystem/s3fs.h>
#  include <aws/core/auth/AWSCredentials.h>
#  include <aws/core/auth/AWSCredentialsProvider.h>
#endif

#include "iceberg/arrow/arrow_io_internal.h"
#include "iceberg/arrow/arrow_io_util.h"
#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/arrow/s3/s3_properties.h"
#include "iceberg/logging/log_macros.h"
#include "iceberg/logging/logger.h"
#include "iceberg/util/executor.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/property_util.h"
#include "iceberg/util/string_util.h"

namespace iceberg::arrow {

namespace {

struct DeleteTasks {
  std::vector<std::future<void>> futures;

  ~DeleteTasks() {
    for (auto& future : futures) {
      if (future.valid()) {
        future.wait();
      }
    }
  }
};

// Submit may throw instead of returning an error; both reject the worker.
Status SubmitWorker(Executor& executor, ExecutorTask task) {
  try {
    return executor.Submit(std::move(task));
  } catch (const std::exception& e) {
    return IOError("Submit threw an exception: {}", e.what());
  } catch (...) {
    return IOError("Submit threw an unknown exception");
  }
}

DeleteExecutorState delete_executor;

}  // namespace

Status DeleteExecutorState::Initialize(Executor* executor) {
  std::lock_guard lock(mutex_);
  if (initialized_) {
    if (executor_ != executor) {
      return InvalidArgument("Delete executor is already initialized");
    }
    return {};
  }
  executor_ = executor;
  initialized_ = true;
  return {};
}

Executor* DeleteExecutorState::Get() {
  std::lock_guard lock(mutex_);
  initialized_ = true;
  return executor_;
}

Status BulkDeleteFiles(std::span<const std::string> file_locations, Executor* executor,
                       size_t parallelism,
                       const std::function<Status(const std::string&)>& delete_file) {
  ICEBERG_PRECHECK(parallelism > 0, "Delete parallelism must be positive");
  auto logger = GetCurrentLogger();
  std::atomic<size_t> next = 0;
  std::atomic<size_t> failed = 0;
  auto work = [&] {
    ScopedLogger bind(logger);
    while (true) {
      const auto index = next.fetch_add(1, std::memory_order_relaxed);
      if (index >= file_locations.size()) {
        return;
      }
      const auto& location = file_locations[index];
      Status status;
      try {
        status = delete_file(location);
      } catch (const std::exception& e) {
        status = IOError("Delete threw an exception: {}", e.what());
      } catch (...) {
        status = IOError("Delete threw an unknown exception");
      }
      if (!status.has_value()) {
        failed.fetch_add(1, std::memory_order_relaxed);
        ICEBERG_LOG_WARN("Failed to delete {}: {}", location, status.error().message);
      }
    }
  };

  std::optional<Error> submission_error;
  try {
    if (executor == nullptr) {
      work();
    } else {
      const auto workers = std::min(parallelism, file_locations.size());
      DeleteTasks tasks;
      tasks.futures.reserve(workers);
      size_t accepted = 0;
      for (; accepted < workers; ++accepted) {
        std::packaged_task<void()> task(work);
        // Submit may accept the task before failing.
        tasks.futures.push_back(task.get_future());
        ExecutorTask executor_task([task = std::move(task)]() mutable { task(); });
        if (auto status = SubmitWorker(*executor, std::move(executor_task));
            !status.has_value()) {
          submission_error = std::move(status.error());
          break;
        }
      }
      // A rejected worker's future is only waited for, by `tasks`.
      for (size_t i = 0; i < accepted; ++i) {
        tasks.futures[i].get();
      }
    }
  } catch (const std::exception& e) {
    return IOError("Bulk delete execution failed: {}", e.what());
  } catch (...) {
    return IOError("Bulk delete execution failed with an unknown exception");
  }
  if (submission_error.has_value()) {
    if (failed > 0) {
      submission_error->message += std::format("; failed to delete {} of {} files",
                                               failed.load(), file_locations.size());
    }
    return Status::unexpected_type(std::move(*submission_error));
  }
  if (failed > 0) {
    return IOError("Failed to delete {} of {} files", failed.load(),
                   file_locations.size());
  }
  return {};
}

Status InitializeS3FileIODeleteExecutor(Executor* executor) {
  return delete_executor.Initialize(executor);
}

#if ICEBERG_S3_ENABLED

namespace {

const std::string* FindProperty(
    const std::unordered_map<std::string, std::string>& properties,
    std::string_view key) {
  auto it = properties.find(std::string(key));
  return it == properties.end() ? nullptr : &it->second;
}

Status EnsureS3Initialized() {
  static const ::arrow::Status init_status = []() {
    auto options = ::arrow::fs::S3GlobalOptions::Defaults();
    return ::arrow::fs::InitializeS3(options);
  }();
  if (!init_status.ok()) {
    return ::iceberg::unexpected(Error{.kind = ::iceberg::arrow::ToErrorKind(init_status),
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

  // Configure endpoint (for S3-compatible object stores)
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

  // Both boolean properties below read through PropertyUtil, so a value that does not
  // spell a boolean reads as false instead of failing the build, as in Java.
  const auto path_style_access =
      PropertyUtil::PropertyAsOptionalBoolean(properties, S3Properties::kPathStyleAccess);
  if (path_style_access.has_value()) {
    options.force_virtual_addressing = !*path_style_access;
  }

  // Explicit `s3.ssl.enabled` overrides any endpoint-derived scheme.
  const auto ssl_enabled =
      PropertyUtil::PropertyAsOptionalBoolean(properties, S3Properties::kSslEnabled);
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

// `credentials_provider`, if set, signs requests instead of the keys in
// `properties`.
Result<std::shared_ptr<::arrow::fs::FileSystem>> BuildArrowS3FileSystem(
    const std::unordered_map<std::string, std::string>& properties,
    std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_provider = nullptr) {
  ICEBERG_RETURN_UNEXPECTED(EnsureS3Initialized());
  ICEBERG_ASSIGN_OR_RAISE(auto options, ConfigureS3Options(properties));
  if (credentials_provider != nullptr) {
    options.credentials_provider = std::move(credentials_provider);
    options.credentials_kind = ::arrow::fs::S3CredentialsKind::Explicit;
  }
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

// Lead time before expiry, matching Java's VendedCredentialsProvider.
constexpr auto kRefreshLeadTime = std::chrono::minutes(5);

// How long to wait after a failed refresh before asking the catalog again.
constexpr auto kRefreshRetryBackoff = std::chrono::seconds(30);

// Floor on a backoff shortened to land on the expiry.
constexpr auto kMinRefreshRetryBackoff = std::chrono::seconds(1);

// How long a caller with expired credentials waits for a refresh under way.
constexpr auto kExpiredCredentialWait = std::chrono::seconds(10);

// Whether `config` carries an access key pair.
bool HasKeys(const std::unordered_map<std::string, std::string>& config) {
  return FindProperty(config, S3Properties::kAccessKeyId) != nullptr &&
         FindProperty(config, S3Properties::kSecretAccessKey) != nullptr;
}

// The properties an S3 client for `credential` is built from.
std::unordered_map<std::string, std::string> PropertiesFor(
    const std::unordered_map<std::string, std::string>& default_properties,
    const StorageCredential& credential) {
  auto properties = default_properties;
  for (const auto& [key, value] : credential.config) {
    properties[key] = value;
  }
  return properties;
}

// The client settings in `properties`: everything but the keys.
std::unordered_map<std::string, std::string> SettingsOf(
    std::unordered_map<std::string, std::string> properties) {
  for (const auto key :
       {S3Properties::kAccessKeyId, S3Properties::kSecretAccessKey,
        S3Properties::kSessionToken, S3Properties::kSessionTokenExpiresAtMs}) {
    properties.erase(std::string(key));
  }
  return properties;
}

// When the session token that `properties` sign with expires: nullopt if they
// sign without one, or an error if the expiry is missing or invalid.
Result<std::optional<std::chrono::system_clock::time_point>> ExpiryOf(
    const std::unordered_map<std::string, std::string>& properties,
    std::string_view prefix) {
  // Only a nonempty token alongside an access key pair signs requests.
  const auto* token = FindProperty(properties, S3Properties::kSessionToken);
  if (!HasKeys(properties) || token == nullptr || token->empty()) {
    return std::nullopt;
  }
  const auto* value = FindProperty(properties, S3Properties::kSessionTokenExpiresAtMs);
  if (value == nullptr) {
    return InvalidArgument("Credential \"{}\" has a session token but no \"{}\"", prefix,
                           S3Properties::kSessionTokenExpiresAtMs);
  }
  auto millis = StringUtils::ParseNumber<int64_t>(*value);
  if (!millis.has_value()) {
    return InvalidArgument(
        "Credential \"{}\" has a session token but an unparseable \"{}\" value \"{}\"",
        prefix, S3Properties::kSessionTokenExpiresAtMs, *value);
  }
  // Beyond what the clock can hold, converting would overflow it.
  constexpr auto kMaxMillis = std::chrono::duration_cast<std::chrono::milliseconds>(
                                  std::chrono::system_clock::duration::max())
                                  .count();
  constexpr auto kMinMillis = std::chrono::duration_cast<std::chrono::milliseconds>(
                                  std::chrono::system_clock::duration::min())
                                  .count();
  if (*millis > kMaxMillis || *millis < kMinMillis) {
    return InvalidArgument("Credential \"{}\" has an out-of-range \"{}\" value \"{}\"",
                           prefix, S3Properties::kSessionTokenExpiresAtMs, *value);
  }
  return std::chrono::system_clock::time_point(std::chrono::milliseconds(*millis));
}

// The S3 credentials in effect: the first for each canonical prefix, which is
// the one its locations match.
std::vector<const StorageCredential*> S3CredentialsOf(
    const std::vector<StorageCredential>& credentials) {
  std::vector<const StorageCredential*> s3_credentials;
  std::unordered_set<std::string> prefixes;
  for (const auto& credential : credentials) {
    if (IsS3CredentialPrefix(credential.prefix) &&
        prefixes.insert(CanonicalizeS3Scheme(credential.prefix)).second) {
      s3_credentials.push_back(&credential);
    }
  }
  return s3_credentials;
}

// When installed credentials need replacing.
struct CredentialsExpiry {
  // The earliest expiry known, if any.
  std::optional<std::chrono::system_clock::time_point> expires_at;
  // A session token lacks a usable expiry: refresh at once, as Java does, but
  // do not treat it as expired.
  bool unknown = false;
};

CredentialsExpiry ExpiryOf(
    const std::unordered_map<std::string, std::string>& default_properties,
    const std::vector<StorageCredential>& credentials) {
  CredentialsExpiry expiry;
  for (const auto* credential : S3CredentialsOf(credentials)) {
    auto expires_at =
        ExpiryOf(PropertiesFor(default_properties, *credential), credential->prefix);
    if (!expires_at.has_value()) {
      ICEBERG_LOG_WARN("{}", expires_at.error().message);
      expiry.unknown = true;
    } else if (expires_at->has_value() &&
               (!expiry.expires_at.has_value() || **expires_at < *expiry.expires_at)) {
      expiry.expires_at = **expires_at;
    }
  }
  return expiry;
}

// Known expiries by canonical prefix.
std::unordered_map<std::string, std::chrono::system_clock::time_point> ExpiriesOf(
    const std::unordered_map<std::string, std::string>& default_properties,
    const std::vector<StorageCredential>& credentials) {
  std::unordered_map<std::string, std::chrono::system_clock::time_point> expiries;
  for (const auto* credential : S3CredentialsOf(credentials)) {
    // Invalid expiries are logged on install.
    if (auto expires_at =
            ExpiryOf(PropertiesFor(default_properties, *credential), credential->prefix);
        expires_at.has_value() && expires_at->has_value()) {
      expiries.emplace(CanonicalizeS3Scheme(credential->prefix), **expires_at);
    }
  }
  return expiries;
}

// The S3 prefixes `credentials` serve, each with its client settings and
// whether it signs with keys.
using Layout =
    std::unordered_map<std::string,
                       std::pair<std::unordered_map<std::string, std::string>, bool>>;

Layout LayoutOf(const std::unordered_map<std::string, std::string>& default_properties,
                const std::vector<StorageCredential>& credentials) {
  Layout layout;
  for (const auto* credential : S3CredentialsOf(credentials)) {
    auto properties = PropertiesFor(default_properties, *credential);
    const bool keys = HasKeys(properties);
    layout.try_emplace(CanonicalizeS3Scheme(credential->prefix),
                       SettingsOf(std::move(properties)), keys);
  }
  return layout;
}

// A refresh may only rotate keys, so the current clients serve it: it must keep
// the layout of `current`, and its tokens must be usable.
Status ValidateRefreshed(
    const std::unordered_map<std::string, std::string>& default_properties,
    const std::vector<StorageCredential>& current,
    const std::vector<StorageCredential>& credentials) {
  for (const auto& credential : credentials) {
    ICEBERG_RETURN_UNEXPECTED(credential.Validate());
  }
  if (LayoutOf(default_properties, credentials) !=
      LayoutOf(default_properties, current)) {
    return InvalidArgument(
        "Refreshed credentials change the S3 prefixes, their settings or keys");
  }
  const auto now = std::chrono::system_clock::now();
  for (const auto* credential : S3CredentialsOf(credentials)) {
    ICEBERG_ASSIGN_OR_RAISE(
        auto expires_at,
        ExpiryOf(PropertiesFor(default_properties, *credential), credential->prefix));
    if (expires_at.has_value() && *expires_at <= now) {
      return InvalidArgument("Credential \"{}\" has already expired", credential->prefix);
    }
  }
  return {};
}

// Requires HasKeys(config).
Aws::Auth::AWSCredentials AwsCredentialsOf(
    const std::unordered_map<std::string, std::string>& config) {
  Aws::Auth::AWSCredentials credentials;
  credentials.SetAWSAccessKeyId(
      FindProperty(config, S3Properties::kAccessKeyId)->c_str());
  credentials.SetAWSSecretKey(
      FindProperty(config, S3Properties::kSecretAccessKey)->c_str());
  if (const auto* session_token = FindProperty(config, S3Properties::kSessionToken);
      session_token != nullptr) {
    credentials.SetSessionToken(session_token->c_str());
  }
  return credentials;
}

class ArrowS3FileIO;

// Gives S3 clients their FileIO's current keys. Clients may outlive the
// FileIO, which detaches on destruction.
class KeySource {
 public:
  explicit KeySource(ArrowS3FileIO* io) : io_(io) {}

  void Detach() {
    std::unique_lock lock(mutex_);
    io_ = nullptr;
  }

  /// \brief Refreshes if due, then returns the FileIO's keys for `prefix` and
  /// `settings`; nullopt if it has none or is gone.
  std::optional<Aws::Auth::AWSCredentials> KeysFor(
      std::string_view prefix,
      const std::unordered_map<std::string, std::string>& settings);

 private:
  std::shared_mutex mutex_;
  ArrowS3FileIO* io_;
};

// Supplies an S3 client's keys. The AWS SDK asks before each request, so open
// handles pick up refreshed keys, as in Java. Keys vended with other settings
// (e.g. another endpoint) are never used; the last keys stay instead.
class VendedCredentialsProvider final : public Aws::Auth::AWSCredentialsProvider {
 public:
  VendedCredentialsProvider(std::shared_ptr<KeySource> source, std::string prefix,
                            std::unordered_map<std::string, std::string> settings,
                            Aws::Auth::AWSCredentials credentials)
      : source_(std::move(source)),
        prefix_(std::move(prefix)),
        settings_(std::move(settings)),
        credentials_(std::move(credentials)) {}

  Aws::Auth::AWSCredentials GetAWSCredentials() override {
    auto current = source_->KeysFor(prefix_, settings_);
    std::lock_guard lock(mutex_);
    if (current.has_value()) {
      credentials_ = std::move(current).value();
    }
    return credentials_;
  }

 private:
  const std::shared_ptr<KeySource> source_;
  const std::string prefix_;
  const std::unordered_map<std::string, std::string> settings_;
  std::mutex mutex_;
  Aws::Auth::AWSCredentials credentials_;
};

class ArrowS3FileIO final : public FileIO, public SupportsStorageCredentials {
 public:
  ArrowS3FileIO(std::shared_ptr<::arrow::fs::FileSystem> arrow_fs,
                std::unordered_map<std::string, std::string> default_properties,
                size_t delete_parallelism)
      : default_file_io_(std::make_shared<ArrowFileSystemFileIO>(std::move(arrow_fs))),
        default_properties_(std::move(default_properties)),
        delete_parallelism_(delete_parallelism) {}

  ~ArrowS3FileIO() override { key_source_->Detach(); }

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

  Status InitializeStorageCredentials(
      const std::vector<StorageCredential>& storage_credentials,
      std::shared_ptr<StorageCredentialProvider> provider) override;

  SupportsStorageCredentials* AsSupportsStorageCredentials() override { return this; }

 private:
  friend class KeySource;

  using DelegatesByPrefix =
      std::vector<std::pair<std::string, std::shared_ptr<ArrowFileSystemFileIO>>>;
  using ExpiriesByPrefix =
      std::unordered_map<std::string, std::chrono::system_clock::time_point>;

  /// \brief Delegate serving `location`, pinned by the caller against a
  /// concurrent credential install.
  Result<std::shared_ptr<ArrowFileSystemFileIO>> FileIOForPath(std::string_view location);

  /// \brief Longest-prefix match, or `fallback` if none matches. Fails with
  /// TokenExpired once the matched prefix's keys have expired.
  static Result<std::shared_ptr<ArrowFileSystemFileIO>> MatchDelegate(
      const std::shared_ptr<ArrowFileSystemFileIO>& fallback,
      const DelegatesByPrefix& by_prefix, const ExpiriesByPrefix& expiries,
      std::string_view location);

  /// \brief Build a delegate for each credential this FileIO can serve.
  ///
  /// Runs without holding `mutex_`: building an S3 client can block (without
  /// static keys the AWS SDK may wait on the EC2 metadata service), which would
  /// stall every concurrent operation. Reads no mutable member state.
  Result<DelegatesByPrefix> BuildDelegates(
      const std::vector<StorageCredential>& storage_credentials) const;

  /// \brief Swap in credentials and, unless null, delegates, handing back the
  /// retired ones. A refresh keeps the layout, so the delegates stay.
  ///
  /// Callers must hold `mutex_` exclusively and let the returned generation
  /// destruct only after releasing it: tearing down an S3 client can block on
  /// in-flight requests, which would stall every operation.
  void InstallCredentials(std::vector<StorageCredential>& storage_credentials,
                          DelegatesByPrefix* delegates);

  /// \brief Whether a refresh is due and not backed off. Requires `mutex_`.
  bool RefreshDue() const;

  /// \brief Whether the installed credentials have expired. Requires `mutex_`.
  bool Expired() const;

  /// \brief When to retry after a failed refresh; never past the expiry.
  /// Requires `mutex_`.
  std::chrono::steady_clock::time_point BackoffUntil() const;

  /// \brief Refresh the credentials if they are close to expiring.
  ///
  /// Called before file operations and by S3 clients before each request. A
  /// refresh only rotates keys and builds no clients, so a request can run it
  /// or, with expired credentials, wait for one under way. A failure keeps the
  /// current credentials.
  void MaybeRefreshCredentials();

  /// \brief The installed keys for `prefix`, if its credential has `settings`.
  std::optional<Aws::Auth::AWSCredentials> KeysFor(
      std::string_view prefix,
      const std::unordered_map<std::string, std::string>& settings) const;

  std::shared_ptr<ArrowFileSystemFileIO> default_file_io_;
  std::unordered_map<std::string, std::string> default_properties_;
  const size_t delete_parallelism_;
  const std::shared_ptr<KeySource> key_source_ = std::make_shared<KeySource>(this);
  // Guards everything below; shared because reads happen per file operation.
  mutable std::shared_mutex mutex_;
  std::vector<StorageCredential> storage_credentials_;
  DelegatesByPrefix file_io_by_prefix_;
  ExpiriesByPrefix expiries_by_prefix_;
  std::shared_ptr<StorageCredentialProvider> provider_;
  CredentialsExpiry expiry_;
  std::chrono::steady_clock::time_point retry_refresh_at_;
  // Bumped on each install so a refresh cannot overwrite newer credentials.
  uint64_t credential_generation_ = 0;
  // Serializes refreshes; timed so waits are bounded.
  std::timed_mutex refresh_mutex_;
};

Status ArrowS3FileIO::InitializeStorageCredentials(
    const std::vector<StorageCredential>& storage_credentials,
    std::shared_ptr<StorageCredentialProvider> provider) {
  ICEBERG_ASSIGN_OR_RAISE(auto delegates, BuildDelegates(storage_credentials));
  auto credentials = storage_credentials;
  {
    std::unique_lock lock(mutex_);
    provider_.swap(provider);
    InstallCredentials(credentials, &delegates);
  }
  return {};
}

Status ArrowS3FileIO::SetStorageCredentials(
    const std::vector<StorageCredential>& storage_credentials) {
  ICEBERG_ASSIGN_OR_RAISE(auto delegates, BuildDelegates(storage_credentials));
  auto credentials = storage_credentials;
  {
    std::unique_lock lock(mutex_);
    InstallCredentials(credentials, &delegates);
  }
  // `credentials` and `delegates` now hold the retired generation and destruct
  // here, outside the lock.
  return {};
}

Result<ArrowS3FileIO::DelegatesByPrefix> ArrowS3FileIO::BuildDelegates(
    const std::vector<StorageCredential>& storage_credentials) const {
  DelegatesByPrefix delegates;
  delegates.reserve(storage_credentials.size());
  for (const auto& credential : storage_credentials) {
    ICEBERG_RETURN_UNEXPECTED(credential.Validate());
    // A server may vend credentials for several storage systems at once;
    // non-S3 prefixes are skipped, not rejected (Java S3FileIO filters
    // credentials by the "s3" prefix).
    if (!IsS3CredentialPrefix(credential.prefix)) {
      continue;
    }
    auto prefix = CanonicalizeS3Scheme(credential.prefix);
    const auto properties = PropertiesFor(default_properties_, credential);
    std::shared_ptr<VendedCredentialsProvider> keys;
    // Signs with what ConfigureS3Options would, defaults included.
    if (HasKeys(properties)) {
      keys = std::make_shared<VendedCredentialsProvider>(
          key_source_, prefix, SettingsOf(properties), AwsCredentialsOf(properties));
    }
    ICEBERG_ASSIGN_OR_RAISE(auto fs, BuildArrowS3FileSystem(properties, std::move(keys)));
    delegates.emplace_back(std::move(prefix),
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
    std::vector<StorageCredential>& storage_credentials, DelegatesByPrefix* delegates) {
  if (delegates != nullptr) {
    file_io_by_prefix_.swap(*delegates);
  }
  expiry_ = ExpiryOf(default_properties_, storage_credentials);
  expiries_by_prefix_ = ExpiriesOf(default_properties_, storage_credentials);
  retry_refresh_at_ = {};
  ++credential_generation_;
  storage_credentials_.swap(storage_credentials);
}

bool ArrowS3FileIO::RefreshDue() const {
  return (expiry_.unknown ||
          (expiry_.expires_at.has_value() &&
           std::chrono::system_clock::now() + kRefreshLeadTime >= *expiry_.expires_at)) &&
         std::chrono::steady_clock::now() >= retry_refresh_at_;
}

bool ArrowS3FileIO::Expired() const {
  return expiry_.expires_at.has_value() &&
         std::chrono::system_clock::now() >= *expiry_.expires_at;
}

std::chrono::steady_clock::time_point ArrowS3FileIO::BackoffUntil() const {
  auto delay =
      std::chrono::duration_cast<std::chrono::milliseconds>(kRefreshRetryBackoff);
  if (expiry_.expires_at.has_value()) {
    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
        *expiry_.expires_at - std::chrono::system_clock::now());
    // Retry sooner while still valid; once expired, use the full backoff.
    if (remaining > std::chrono::milliseconds::zero()) {
      delay = std::clamp(
          remaining,
          std::chrono::duration_cast<std::chrono::milliseconds>(kMinRefreshRetryBackoff),
          delay);
    }
  }
  return std::chrono::steady_clock::now() + delay;
}

void ArrowS3FileIO::MaybeRefreshCredentials() {
  {
    // Fast path.
    std::shared_lock lock(mutex_);
    if (!provider_ || !RefreshDue()) {
      return;
    }
  }

  std::unique_lock refresh_lock(refresh_mutex_, std::defer_lock);
  if (!refresh_lock.try_lock()) {
    // Another caller is refreshing; keep the current keys unless expired.
    {
      std::shared_lock lock(mutex_);
      if (!Expired()) {
        return;
      }
    }
    if (!refresh_lock.try_lock_for(kExpiredCredentialWait)) {
      return;
    }
  }
  std::shared_ptr<StorageCredentialProvider> provider;
  uint64_t generation = 0;
  std::vector<StorageCredential> current;
  {
    // The previous holder may have just refreshed.
    std::shared_lock lock(mutex_);
    if (!provider_ || !RefreshDue()) {
      return;
    }
    provider = provider_;
    generation = credential_generation_;
    current = storage_credentials_;
  }

  // Slow, so done without holding `mutex_`.
  auto refreshed = provider->Load();
  Status status;
  if (refreshed.has_value()) {
    status = ValidateRefreshed(default_properties_, current, *refreshed);
  } else {
    status = ::iceberg::unexpected<Error>(refreshed.error());
  }

  std::unique_lock lock(mutex_);
  // Credentials installed meanwhile are newer than what this fetched.
  const bool superseded = credential_generation_ != generation;
  if (!status.has_value()) {
    // Logged even if superseded, so catalog failures stay visible.
    if (superseded) {
      ICEBERG_LOG_WARN(
          "Failed to refresh vended storage credentials ({}); they have since been "
          "replaced",
          status.error().message);
      return;
    }
    retry_refresh_at_ = BackoffUntil();
    ICEBERG_LOG_WARN(
        "Failed to refresh vended storage credentials ({}); keeping the current "
        "ones and retrying in {}ms",
        status.error().message,
        std::chrono::duration_cast<std::chrono::milliseconds>(
            retry_refresh_at_ - std::chrono::steady_clock::now())
            .count());
    return;
  }
  if (superseded) {
    return;
  }

  InstallCredentials(*refreshed, /*delegates=*/nullptr);
  if (RefreshDue()) {
    // Tokens shorter-lived than the lead time would be due again at once.
    retry_refresh_at_ = BackoffUntil();
  }
}

std::optional<Aws::Auth::AWSCredentials> ArrowS3FileIO::KeysFor(
    std::string_view prefix,
    const std::unordered_map<std::string, std::string>& settings) const {
  std::shared_lock lock(mutex_);
  for (const auto* credential : S3CredentialsOf(storage_credentials_)) {
    if (CanonicalizeS3Scheme(credential->prefix) != prefix) {
      continue;
    }
    const auto properties = PropertiesFor(default_properties_, *credential);
    if (!HasKeys(properties) || SettingsOf(properties) != settings) {
      return std::nullopt;
    }
    return AwsCredentialsOf(properties);
  }
  return std::nullopt;
}

Result<std::shared_ptr<ArrowFileSystemFileIO>> ArrowS3FileIO::MatchDelegate(
    const std::shared_ptr<ArrowFileSystemFileIO>& fallback,
    const DelegatesByPrefix& by_prefix, const ExpiriesByPrefix& expiries,
    std::string_view location) {
  if (by_prefix.empty()) {
    return fallback;
  }
  const std::string canonical = CanonicalizeS3Scheme(location);
  auto best = fallback;
  size_t best_len = 0;
  const std::string* best_prefix = nullptr;
  for (const auto& [prefix, file_io] : by_prefix) {
    if (prefix.size() > best_len && canonical.starts_with(prefix)) {
      best = file_io;
      best_len = prefix.size();
      best_prefix = &prefix;
    }
  }
  if (best_prefix == nullptr) {
    return fallback;
  }
  if (const auto expiry = expiries.find(*best_prefix);
      expiry != expiries.end() && std::chrono::system_clock::now() >= expiry->second) {
    return TokenExpired(
        "Vended credentials for \"{}\" have expired and could not be refreshed",
        *best_prefix);
  }
  return best;
}

Result<std::shared_ptr<ArrowFileSystemFileIO>> ArrowS3FileIO::FileIOForPath(
    std::string_view location) {
  MaybeRefreshCredentials();
  std::shared_lock lock(mutex_);
  return MatchDelegate(default_file_io_, file_io_by_prefix_, expiries_by_prefix_,
                       location);
}

Result<std::unique_ptr<InputFile>> ArrowS3FileIO::NewInputFile(
    std::string file_location) {
  ICEBERG_ASSIGN_OR_RAISE(auto file_io, FileIOForPath(file_location));
  return file_io->NewInputFile(std::move(file_location));
}

Result<std::unique_ptr<InputFile>> ArrowS3FileIO::NewInputFile(std::string file_location,
                                                               size_t length) {
  ICEBERG_ASSIGN_OR_RAISE(auto file_io, FileIOForPath(file_location));
  return file_io->NewInputFile(std::move(file_location), length);
}

Result<std::unique_ptr<OutputFile>> ArrowS3FileIO::NewOutputFile(
    std::string file_location) {
  ICEBERG_ASSIGN_OR_RAISE(auto file_io, FileIOForPath(file_location));
  return file_io->NewOutputFile(std::move(file_location));
}

Status ArrowS3FileIO::DeleteFile(const std::string& file_location) {
  ICEBERG_ASSIGN_OR_RAISE(auto file_io, FileIOForPath(file_location));
  return file_io->DeleteFile(file_location);
}

Status ArrowS3FileIO::DeleteFiles(const std::vector<std::string>& file_locations) {
  // Each file is resolved afresh, as a long batch may outlive its credentials.
  return BulkDeleteFiles(file_locations, delete_executor.Get(), delete_parallelism_,
                         [this](const std::string& location) -> Status {
                           ICEBERG_ASSIGN_OR_RAISE(auto file_io, FileIOForPath(location));
                           return file_io->DeleteFile(location);
                         });
}

std::optional<Aws::Auth::AWSCredentials> KeySource::KeysFor(
    std::string_view prefix,
    const std::unordered_map<std::string, std::string>& settings) {
  std::shared_lock lock(mutex_);
  if (io_ == nullptr) {
    return std::nullopt;
  }
  io_->MaybeRefreshCredentials();
  return io_->KeysFor(prefix, settings);
}

}  // namespace

Result<std::unique_ptr<FileIO>> MakeS3FileIO(
    const std::unordered_map<std::string, std::string>& properties) {
  size_t delete_parallelism = std::max(1u, std::thread::hardware_concurrency());
  if (const auto* value = FindProperty(properties, S3Properties::kDeleteNumThreads);
      value != nullptr) {
    ICEBERG_ASSIGN_OR_RAISE(auto count, StringUtils::ParseNumber<int32_t>(*value));
    ICEBERG_PRECHECK(count > 0, "{} must be positive", S3Properties::kDeleteNumThreads);
    delete_parallelism = static_cast<size_t>(count);
  }
  // Uses default credentials if properties are empty.
  ICEBERG_ASSIGN_OR_RAISE(auto fs, BuildArrowS3FileSystem(properties));
  return std::make_unique<ArrowS3FileIO>(std::move(fs), properties, delete_parallelism);
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
