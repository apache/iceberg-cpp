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

// After a failed refresh, how long to keep the current credentials before
// asking again, so an unreachable catalog is not queried per file operation.
constexpr auto kRefreshRetryBackoff = std::chrono::seconds(30);

// Floor on a backoff shortened to land on the expiry.
constexpr auto kMinRefreshRetryBackoff = std::chrono::seconds(1);

// How long an operation with expired credentials waits for a refresh already
// under way. Bounded: a provider may not limit how long its request takes.
constexpr auto kExpiredCredentialWait = std::chrono::seconds(10);

// When `credential`'s session token stops being valid: nullopt for static
// keys, which never expire, or an error if its expiry is missing or unusable.
Result<std::optional<std::chrono::system_clock::time_point>> ExpiryOf(
    const StorageCredential& credential) {
  if (FindProperty(credential.config, S3Properties::kSessionToken) == nullptr) {
    return std::nullopt;
  }
  const auto* value =
      FindProperty(credential.config, S3Properties::kSessionTokenExpiresAtMs);
  if (value == nullptr) {
    return InvalidArgument("Credential \"{}\" has a session token but no \"{}\"",
                           credential.prefix, S3Properties::kSessionTokenExpiresAtMs);
  }
  auto millis = StringUtils::ParseNumber<int64_t>(*value);
  if (!millis.has_value()) {
    return InvalidArgument(
        "Credential \"{}\" has a session token but an unparseable \"{}\" value \"{}\"",
        credential.prefix, S3Properties::kSessionTokenExpiresAtMs, *value);
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
                           credential.prefix, S3Properties::kSessionTokenExpiresAtMs,
                           *value);
  }
  return std::chrono::system_clock::time_point(std::chrono::milliseconds(*millis));
}

// When installed credentials need replacing.
struct CredentialsExpiry {
  // The earliest expiry known, if any.
  std::optional<std::chrono::system_clock::time_point> expires_at;
  // A session token has no usable expiry. It is replaced at once rather than
  // used until it fails, as Java does, but not reported as expired.
  bool unknown = false;
};

CredentialsExpiry ExpiryOf(const std::vector<StorageCredential>& credentials) {
  CredentialsExpiry expiry;
  for (const auto& credential : credentials) {
    if (!IsS3CredentialPrefix(credential.prefix)) {
      continue;
    }
    auto expires_at = ExpiryOf(credential);
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

std::vector<const StorageCredential*> S3CredentialsOf(
    const std::vector<StorageCredential>& credentials) {
  std::vector<const StorageCredential*> s3_credentials;
  for (const auto& credential : credentials) {
    if (IsS3CredentialPrefix(credential.prefix)) {
      s3_credentials.push_back(&credential);
    }
  }
  return s3_credentials;
}

// A refresh must not replace working credentials with ones that cannot be used
// or whose expiry is unknown, which would also defer the next refresh.
Status ValidateRefreshed(const std::vector<StorageCredential>& credentials) {
  const auto now = std::chrono::system_clock::now();
  for (const auto& credential : credentials) {
    ICEBERG_RETURN_UNEXPECTED(credential.Validate());
  }
  const auto s3_credentials = S3CredentialsOf(credentials);
  if (s3_credentials.empty()) {
    // Installing this would drop working credentials for whatever ambient
    // identity the AWS chain finds. Java refuses an empty list too.
    return NotFound("Refreshed credentials contain no S3-compatible prefix");
  }
  for (const auto* credential : s3_credentials) {
    ICEBERG_ASSIGN_OR_RAISE(auto expires_at, ExpiryOf(*credential));
    if (expires_at.has_value() && *expires_at <= now) {
      return InvalidArgument("Credential \"{}\" has already expired", credential->prefix);
    }
  }
  return {};
}

// Whether `config` carries vended keys, which reach the S3 client through a
// VendedCredentialsProvider.
bool HasKeys(const std::unordered_map<std::string, std::string>& config) {
  return FindProperty(config, S3Properties::kAccessKeyId) != nullptr &&
         FindProperty(config, S3Properties::kSecretAccessKey) != nullptr;
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

// Whether `refreshed` serves the same prefix with the same settings as
// `current`, so that only its keys need to reach the existing S3 client.
bool DiffersOnlyInKeys(const StorageCredential& current,
                       const StorageCredential& refreshed) {
  if (current.prefix != refreshed.prefix || !HasKeys(refreshed.config)) {
    return false;
  }
  const auto settings = [](std::unordered_map<std::string, std::string> config) {
    for (const auto key :
         {S3Properties::kAccessKeyId, S3Properties::kSecretAccessKey,
          S3Properties::kSessionToken, S3Properties::kSessionTokenExpiresAtMs}) {
      config.erase(std::string(key));
    }
    return config;
  };
  return settings(current.config) == settings(refreshed.config);
}

class ArrowS3FileIO;

// Where the S3 clients of an ArrowS3FileIO get their keys. Open handles keep
// clients alive past the FileIO, which therefore detaches on destruction.
class KeySource {
 public:
  explicit KeySource(ArrowS3FileIO* io) : io_(io) {}

  void Detach() {
    std::unique_lock lock(mutex_);
    io_ = nullptr;
  }

  /// \brief The keys the FileIO vends for the canonical `prefix`, after the
  /// refresh a request may run; nullopt once it is gone or vends none for
  /// `prefix`.
  std::optional<Aws::Auth::AWSCredentials> KeysFor(std::string_view prefix);

 private:
  std::shared_mutex mutex_;
  ArrowS3FileIO* io_;
};

// Signs an S3 client's requests with the keys its FileIO currently vends for
// the client's prefix. The AWS SDK asks before each request, so a refresh
// reaches handles and streams opened before it, even those on a client it
// replaced, as each client's VendedCredentialsProvider does in Java. Without
// current keys for the prefix, the last ones stay.
class VendedCredentialsProvider final : public Aws::Auth::AWSCredentialsProvider {
 public:
  VendedCredentialsProvider(std::shared_ptr<KeySource> source, std::string prefix,
                            Aws::Auth::AWSCredentials credentials)
      : source_(std::move(source)),
        prefix_(std::move(prefix)),
        credentials_(std::move(credentials)) {}

  Aws::Auth::AWSCredentials GetAWSCredentials() override {
    auto current = source_->KeysFor(prefix_);
    std::lock_guard lock(mutex_);
    if (current.has_value()) {
      credentials_ = std::move(current).value();
    }
    return credentials_;
  }

 private:
  const std::shared_ptr<KeySource> source_;
  const std::string prefix_;
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

  /// \brief The S3 client serving the locations under `prefix`.
  struct Delegate {
    std::string prefix;
    std::shared_ptr<ArrowFileSystemFileIO> file_io;
    // Whether the client signs with the keys vended for `prefix`, which it
    // reads through a VendedCredentialsProvider.
    bool vended_keys = false;
    // When those keys stop being valid, if known.
    std::optional<std::chrono::system_clock::time_point> expires_at;
  };

  using DelegatesByPrefix = std::vector<Delegate>;

  /// \brief Delegate serving `location`, pinned by the caller against a
  /// concurrent credential install.
  Result<std::shared_ptr<ArrowFileSystemFileIO>> FileIOForPath(std::string_view location);

  /// \brief Longest-prefix match against one consistent view of the delegates,
  /// or `fallback` if none matches.
  ///
  /// Fails once the matching delegate's keys have expired: a handle built on
  /// them would only fail at S3.
  static Result<std::shared_ptr<ArrowFileSystemFileIO>> MatchDelegate(
      const std::shared_ptr<ArrowFileSystemFileIO>& fallback,
      const DelegatesByPrefix& by_prefix, std::string_view location);

  /// \brief Build a delegate for each credential this FileIO can serve.
  ///
  /// Runs without holding `mutex_`: building an S3 client can block (without
  /// static keys the AWS SDK may wait on the EC2 metadata service), which would
  /// stall every concurrent operation. Reads no mutable member state.
  Result<DelegatesByPrefix> BuildDelegates(
      const std::vector<StorageCredential>& storage_credentials) const;

  /// \brief Swap in credentials and delegates, handing back the retired ones.
  ///
  /// Callers must hold `mutex_` exclusively and let the returned generation
  /// destruct only after releasing it: tearing down an S3 client can block on
  /// in-flight requests, which would stall every operation.
  void InstallCredentials(std::vector<StorageCredential>& storage_credentials,
                          DelegatesByPrefix& delegates);

  /// \brief Whether the installed credentials are close enough to expiring to
  /// be replaced, and no backoff is in effect.
  ///
  /// Callers must hold `mutex_`, at least shared.
  bool RefreshDue() const;

  /// \brief Whether the installed credentials have already stopped being valid.
  ///
  /// Callers must hold `mutex_`, at least shared.
  bool Expired() const;

  /// \brief When the next refresh attempt becomes allowed after a failure.
  ///
  /// Never past the point the credentials stop being valid.
  ///
  /// Callers must hold `mutex_`, at least shared.
  std::chrono::steady_clock::time_point BackoffUntil() const;

  /// \brief Replace the credentials once they are close to expiring.
  ///
  /// Called before each handle is created, and by the S3 clients before each
  /// request, so open handles see the new keys too. A failure is logged and
  /// keeps the current credentials, which MatchDelegate refuses once expired.
  ///
  /// \param from_request Whether an S3 client calls while signing a request.
  ///        Arrow then holds its client registry, which building a client
  ///        locks exclusively, so a refresh needing new clients is left to the
  ///        next file operation. Nor does it wait for another refresh, which
  ///        may be building clients.
  void MaybeRefreshCredentials(bool from_request = false);

  /// \brief The current delegates, with `refreshed`'s expiries, if keys are all
  /// it changes: their S3 clients stay and read the new keys through their
  /// providers. Nullopt if it needs new clients.
  static std::optional<DelegatesByPrefix> WithRefreshedKeys(
      const std::vector<StorageCredential>& current, const DelegatesByPrefix& delegates,
      const std::vector<StorageCredential>& refreshed);

  /// \brief The keys installed for the canonical `prefix`, if any.
  std::optional<Aws::Auth::AWSCredentials> KeysFor(std::string_view prefix) const;

  std::shared_ptr<ArrowFileSystemFileIO> default_file_io_;
  std::unordered_map<std::string, std::string> default_properties_;
  const size_t delete_parallelism_;
  const std::shared_ptr<KeySource> key_source_ = std::make_shared<KeySource>(this);
  // Guards everything below; shared because reads happen per file operation.
  mutable std::shared_mutex mutex_;
  std::vector<StorageCredential> storage_credentials_;
  DelegatesByPrefix file_io_by_prefix_;
  std::shared_ptr<StorageCredentialProvider> provider_;
  CredentialsExpiry expiry_;
  std::chrono::steady_clock::time_point retry_refresh_at_;
  // Until when requests leave the refresh to file operations, after finding
  // that it needs new clients.
  std::chrono::steady_clock::time_point request_refresh_at_;
  // Bumped on each install so a refresh cannot overwrite newer credentials.
  uint64_t credential_generation_ = 0;
  // Held across a refresh so concurrent operations skip it. Timed, so waiting
  // on it is bounded.
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
    InstallCredentials(credentials, delegates);
  }
  return {};
}

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
    auto prefix = CanonicalizeS3Scheme(credential.prefix);
    std::shared_ptr<VendedCredentialsProvider> keys;
    if (HasKeys(credential.config)) {
      keys = std::make_shared<VendedCredentialsProvider>(
          key_source_, prefix, AwsCredentialsOf(credential.config));
    }
    const bool vended_keys = keys != nullptr;
    ICEBERG_ASSIGN_OR_RAISE(auto fs, BuildArrowS3FileSystem(properties, std::move(keys)));
    delegates.push_back(
        {.prefix = std::move(prefix),
         .file_io = std::make_shared<ArrowFileSystemFileIO>(std::move(fs)),
         .vended_keys = vended_keys,
         // Unusable expiries are reported by ExpiryOf when installed.
         .expires_at = ExpiryOf(credential).value_or(std::nullopt)});
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
  expiry_ = ExpiryOf(storage_credentials);
  retry_refresh_at_ = {};
  request_refresh_at_ = {};
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
    // Worth retrying before they run out; once they have, faster retries only
    // hammer a catalog that is already failing.
    if (remaining > std::chrono::milliseconds::zero()) {
      delay = std::clamp(
          remaining,
          std::chrono::duration_cast<std::chrono::milliseconds>(kMinRefreshRetryBackoff),
          delay);
    }
  }
  return std::chrono::steady_clock::now() + delay;
}

void ArrowS3FileIO::MaybeRefreshCredentials(bool from_request) {
  const auto due = [&] {
    return provider_ && RefreshDue() &&
           (!from_request || std::chrono::steady_clock::now() >= request_refresh_at_);
  };
  {
    // Cheap pre-check, so the common case costs one shared lock and no more.
    std::shared_lock lock(mutex_);
    if (!due()) {
      return;
    }
  }

  std::unique_lock refresh_lock(refresh_mutex_, std::defer_lock);
  if (!refresh_lock.try_lock()) {
    // Another operation is already fetching; normally just use what we have.
    if (from_request) {
      return;
    }
    {
      std::shared_lock lock(mutex_);
      if (!Expired()) {
        return;
      }
    }
    // Expired credentials leave nothing to proceed with, so wait instead.
    if (!refresh_lock.try_lock_for(kExpiredCredentialWait)) {
      return;
    }
  }
  // Snapshot the provider, credentials and their generation together.
  std::shared_ptr<StorageCredentialProvider> provider;
  uint64_t generation = 0;
  std::vector<StorageCredential> current;
  DelegatesByPrefix current_delegates;
  {
    // Whoever held the lock may also have just finished, leaving nothing to do.
    std::shared_lock lock(mutex_);
    if (!due()) {
      return;
    }
    provider = provider_;
    generation = credential_generation_;
    current = storage_credentials_;
    current_delegates = file_io_by_prefix_;
  }

  // Outside `mutex_`: both are slow and must not block readers.
  Status status;
  DelegatesByPrefix delegates;
  bool needs_clients = false;
  auto refreshed = provider->Load();
  if (refreshed.has_value()) {
    status = ValidateRefreshed(*refreshed);
  } else {
    status = ::iceberg::unexpected<Error>(refreshed.error());
  }
  if (status.has_value()) {
    if (auto same_clients = WithRefreshedKeys(current, current_delegates, *refreshed);
        same_clients.has_value()) {
      delegates = std::move(same_clients).value();
    } else if (from_request) {
      needs_clients = true;
    } else if (auto built = BuildDelegates(*refreshed); built.has_value()) {
      delegates = std::move(built).value();
    } else {
      status = ::iceberg::unexpected<Error>(built.error());
    }
  }

  std::unique_lock lock(mutex_);
  // Credentials installed meanwhile supersede this refresh: what it fetched is
  // by now the older set.
  const bool superseded = credential_generation_ != generation;
  if (!status.has_value()) {
    // Reported either way, so a failing catalog stays visible.
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
  if (needs_clients) {
    // Fetched again by the next file operation, so it is not stale by then.
    request_refresh_at_ = std::chrono::steady_clock::now() + kRefreshRetryBackoff;
    ICEBERG_LOG_DEBUG(
        "Refreshed vended storage credentials need new S3 clients; leaving them to "
        "the next file operation");
    return;
  }

  // `delegates`/`*refreshed` take the retired generation; both are declared
  // before the lock, so it destructs only after the lock releases.
  InstallCredentials(*refreshed, delegates);
  if (RefreshDue()) {
    // Tokens shorter-lived than the lead time come back due again at once.
    retry_refresh_at_ = BackoffUntil();
  }
}

std::optional<ArrowS3FileIO::DelegatesByPrefix> ArrowS3FileIO::WithRefreshedKeys(
    const std::vector<StorageCredential>& current, const DelegatesByPrefix& delegates,
    const std::vector<StorageCredential>& refreshed) {
  // BuildDelegates made one delegate per S3 credential, in order.
  const auto current_s3 = S3CredentialsOf(current);
  const auto refreshed_s3 = S3CredentialsOf(refreshed);
  if (current_s3.size() != delegates.size() || refreshed_s3.size() != delegates.size()) {
    return std::nullopt;
  }
  auto result = delegates;
  for (size_t i = 0; i < result.size(); ++i) {
    if (!result[i].vended_keys || !DiffersOnlyInKeys(*current_s3[i], *refreshed_s3[i])) {
      return std::nullopt;
    }
    // ValidateRefreshed has checked the expiry.
    result[i].expires_at = ExpiryOf(*refreshed_s3[i]).value_or(std::nullopt);
  }
  return result;
}

std::optional<Aws::Auth::AWSCredentials> ArrowS3FileIO::KeysFor(
    std::string_view prefix) const {
  std::shared_lock lock(mutex_);
  for (const auto* credential : S3CredentialsOf(storage_credentials_)) {
    if (HasKeys(credential->config) &&
        CanonicalizeS3Scheme(credential->prefix) == prefix) {
      return AwsCredentialsOf(credential->config);
    }
  }
  return std::nullopt;
}

Result<std::shared_ptr<ArrowFileSystemFileIO>> ArrowS3FileIO::MatchDelegate(
    const std::shared_ptr<ArrowFileSystemFileIO>& fallback,
    const DelegatesByPrefix& by_prefix, std::string_view location) {
  if (by_prefix.empty()) {
    return fallback;
  }
  const std::string canonical = CanonicalizeS3Scheme(location);
  const Delegate* best = nullptr;
  size_t best_len = 0;
  for (const auto& delegate : by_prefix) {
    if (delegate.prefix.size() > best_len && canonical.starts_with(delegate.prefix)) {
      best = &delegate;
      best_len = delegate.prefix.size();
    }
  }
  if (best == nullptr) {
    return fallback;
  }
  if (best->expires_at.has_value() &&
      std::chrono::system_clock::now() >= *best->expires_at) {
    return TokenExpired(
        "Vended credentials for \"{}\" have expired and could not be refreshed",
        best->prefix);
  }
  return best->file_io;
}

Result<std::shared_ptr<ArrowFileSystemFileIO>> ArrowS3FileIO::FileIOForPath(
    std::string_view location) {
  MaybeRefreshCredentials();

  std::shared_lock lock(mutex_);
  return MatchDelegate(default_file_io_, file_io_by_prefix_, location);
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
  MaybeRefreshCredentials();

  // Keep one delegate snapshot for the whole batch.
  std::shared_ptr<ArrowFileSystemFileIO> fallback;
  DelegatesByPrefix by_prefix;
  {
    std::shared_lock lock(mutex_);
    fallback = default_file_io_;
    by_prefix = file_io_by_prefix_;
  }
  return BulkDeleteFiles(file_locations, delete_executor.Get(), delete_parallelism_,
                         [&](const std::string& location) -> Status {
                           ICEBERG_ASSIGN_OR_RAISE(
                               auto file_io,
                               MatchDelegate(fallback, by_prefix, location));
                           return file_io->DeleteFile(location);
                         });
}

std::optional<Aws::Auth::AWSCredentials> KeySource::KeysFor(std::string_view prefix) {
  std::shared_lock lock(mutex_);
  if (io_ == nullptr) {
    return std::nullopt;
  }
  io_->MaybeRefreshCredentials(/*from_request=*/true);
  return io_->KeysFor(prefix);
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
