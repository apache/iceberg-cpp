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
#include "iceberg/util/timepoint.h"

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

// Build an Arrow S3 filesystem. If a provider is supplied, the AWS signer gets
// access keys from it instead of using the keys in `properties`.
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

// Make the URI scheme lower-case for matching. Keep different schemes, such as
// `s3` and `oss`, separate because they can use different endpoints and keys.
std::string NormalizeS3Location(std::string_view location) {
  const auto separator = location.find("://");
  if (separator != std::string_view::npos && IsS3Scheme(location.substr(0, separator))) {
    // The scheme matched an ASCII alias, so only ASCII letters change.
    return StringUtils::ToLower(location.substr(0, separator))
        .append(location.substr(separator));
  }
  return std::string(location);
}

// Start refreshing five minutes before the earliest token expires.
constexpr auto kRefreshLeadTime = std::chrono::minutes(5);

// Wait this long before retrying a failed refresh.
constexpr std::chrono::milliseconds kRefreshRetryBackoff = std::chrono::seconds(30);

// Never use a retry delay shorter than this value.
constexpr std::chrono::milliseconds kMinRefreshRetryBackoff = std::chrono::seconds(1);

// An expired request waits this long for another request's refresh to finish.
constexpr auto kExpiredCredentialWait = std::chrono::seconds(10);

// Return true when the config has both parts of a static key pair.
bool HasKeys(const std::unordered_map<std::string, std::string>& config) {
  return FindProperty(config, S3Properties::kAccessKeyId) != nullptr &&
         FindProperty(config, S3Properties::kSecretAccessKey) != nullptr;
}

// Convert a complete static key pair to the AWS SDK credential object.
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

struct ResolvedCredential {
  std::string prefix;
  std::unordered_map<std::string, std::string> properties;
  // These settings identify the client configuration independently of its
  // short-lived credentials. They protect a client from receiving keys meant
  // for a different endpoint, region, or addressing mode.
  std::unordered_map<std::string, std::string> client_settings;
  std::optional<Aws::Auth::AWSCredentials> credentials;
};

// Merge FileIO defaults with one vended credential and derive all values used
// by the client and refresh store in one pass.
ResolvedCredential ResolveCredential(
    const std::unordered_map<std::string, std::string>& default_properties,
    const StorageCredential& credential) {
  ResolvedCredential resolved;
  resolved.prefix = NormalizeS3Location(credential.prefix);
  resolved.properties = default_properties;
  for (const auto& [key, value] : credential.config) {
    resolved.properties[key] = value;
  }
  resolved.client_settings = resolved.properties;
  for (const auto key :
       {S3Properties::kAccessKeyId, S3Properties::kSecretAccessKey,
        S3Properties::kSessionToken, S3Properties::kSessionTokenExpiresAtMs}) {
    resolved.client_settings.erase(std::string(key));
  }
  if (HasKeys(resolved.properties)) {
    resolved.credentials = AwsCredentialsOf(resolved.properties);
  }
  return resolved;
}

// Read the expiry of the token used by these properties. No session token
// means there is no expiry to track; a bad token expiry is an error.
Result<std::optional<TimePointMs>> ParseCredentialExpiry(
    const std::unordered_map<std::string, std::string>& properties,
    std::string_view prefix) {
  // Only a non-empty session token paired with access keys needs an expiry.
  const auto* token = FindProperty(properties, S3Properties::kSessionToken);
  if (!HasKeys(properties) || token == nullptr || token->empty()) {
    return std::nullopt;
  }
  const auto* value = FindProperty(properties, S3Properties::kSessionTokenExpiresAtMs);
  if (value == nullptr) {
    return InvalidArgument(R"(Credential "{}" has a session token but no "{}")", prefix,
                           S3Properties::kSessionTokenExpiresAtMs);
  }
  auto millis = StringUtils::ParseNumber<int64_t>(*value);
  if (!millis.has_value()) {
    return InvalidArgument(
        R"(Credential "{}" has a session token but an unparseable "{}" value "{}")",
        prefix, S3Properties::kSessionTokenExpiresAtMs, *value);
  }
  return TimePointMsFromUnixMs(*millis);
}

// Keep the first credential for each prefix. This is also the order used when
// a duplicate prefix is returned by the catalog.
std::vector<const StorageCredential*> S3CredentialsOf(
    const std::vector<StorageCredential>& credentials) {
  std::vector<const StorageCredential*> s3_credentials;
  std::unordered_set<std::string> prefixes;
  for (const auto& credential : credentials) {
    if (IsS3CredentialPrefix(credential.prefix) &&
        prefixes.insert(NormalizeS3Location(credential.prefix)).second) {
      s3_credentials.push_back(&credential);
    }
  }
  return s3_credentials;
}

struct CredentialSlot {
  std::string prefix;
  std::unordered_map<std::string, std::string> settings;
  std::optional<Aws::Auth::AWSCredentials> credentials;
  std::optional<TimePointMs> expires_at;
  bool expiry_unknown = false;

  bool ExpiresBy(TimePointMs time) const {
    return expires_at.has_value() && time >= *expires_at;
  }
};

class CredentialStore {
 public:
  explicit CredentialStore(
      std::unordered_map<std::string, std::string> default_properties)
      : default_properties_(std::move(default_properties)) {}

  ResolvedCredential Resolve(const StorageCredential& credential) const {
    return ResolveCredential(default_properties_, credential);
  }

  /// \brief Make `credentials` current, and `provider` too when one is given.
  void Install(std::vector<StorageCredential> credentials,
               std::optional<std::shared_ptr<StorageCredentialProvider>> provider) {
    std::unique_lock lock(mutex_);
    if (provider.has_value()) {
      provider_ = std::move(*provider);
    }
    InstallLocked(std::move(credentials));
  }

  std::shared_ptr<CredentialSlot> SlotFor(
      std::string prefix, std::unordered_map<std::string, std::string> settings,
      std::optional<Aws::Auth::AWSCredentials> initial_credentials) {
    std::unique_lock lock(mutex_);
    return SlotForLocked(std::move(prefix), std::move(settings),
                         std::move(initial_credentials));
  }

  std::vector<StorageCredential> credentials() const {
    std::shared_lock lock(mutex_);
    return storage_credentials_;
  }

  bool IsExpired(const std::string& prefix) const {
    std::shared_lock lock(mutex_);
    const auto current = current_slots_.find(prefix);
    return current != current_slots_.end() &&
           current->second->ExpiresBy(CurrentTimePointMs());
  }

  void MaybeRefreshCredentials() {
    // Only one caller loads from the catalog. Other callers keep using the
    // current keys while they are still valid.
    {
      std::shared_lock lock(mutex_);
      if (!provider_ || !RefreshDue()) {
        return;
      }
    }

    std::unique_lock refresh_lock(refresh_mutex_, std::defer_lock);
    if (!refresh_lock.try_lock()) {
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
    {
      std::shared_lock lock(mutex_);
      if (!provider_ || !RefreshDue()) {
        return;
      }
      provider = provider_;
      generation = credential_generation_;
    }

    auto refreshed = provider->Load();

    std::unique_lock lock(mutex_);
    if (credential_generation_ != generation) {
      return;  // An install replaced the credentials this refresh started from.
    }
    const Status status = refreshed.has_value()
                              ? ApplyRefreshLocked(*refreshed)
                              : Status(::iceberg::unexpected<Error>(refreshed.error()));
    if (status.has_value() && !RefreshDue()) {
      return;
    }
    const auto delay = BackoffDelay();
    retry_refresh_at_ = std::chrono::steady_clock::now() + delay;
    if (!status.has_value()) {
      ICEBERG_LOG_WARN(
          "Failed to refresh vended storage credentials ({}); keeping the current "
          "ones and retrying in {}ms",
          status.error().message, delay.count());
    }
  }

  std::optional<Aws::Auth::AWSCredentials> CredentialsFor(const CredentialSlot& slot) {
    MaybeRefreshCredentials();
    std::shared_lock lock(mutex_);
    return slot.credentials;
  }

 private:
  void InstallLocked(std::vector<StorageCredential> credentials) {
    std::unordered_map<std::string, std::shared_ptr<CredentialSlot>> current_slots;
    for (const auto* credential : S3CredentialsOf(credentials)) {
      auto resolved = Resolve(*credential);
      auto parsed_expiry = ParseCredentialExpiry(resolved.properties, credential->prefix);
      if (!parsed_expiry.has_value()) {
        ICEBERG_LOG_WARN("{}", parsed_expiry.error().message);
      }
      auto slot = SlotForLocked(std::move(resolved.prefix),
                                std::move(resolved.client_settings), std::nullopt);
      // Clients from an earlier install may share this slot; they move to these
      // keys too.
      slot->credentials = std::move(resolved.credentials);
      slot->expires_at = parsed_expiry.value_or(std::nullopt);
      slot->expiry_unknown = !parsed_expiry.has_value();
      current_slots.emplace(slot->prefix, std::move(slot));
    }
    retry_refresh_at_ = {};
    ++credential_generation_;
    current_slots_ = std::move(current_slots);
    storage_credentials_ = std::move(credentials);
  }

  Status ApplyRefreshLocked(const std::vector<StorageCredential>& credentials) {
    // Check every credential, skipped ones included, before changing any slot.
    for (const auto& credential : credentials) {
      ICEBERG_RETURN_UNEXPECTED(credential.Validate());
    }
    bool updated = false;
    for (const auto* credential : S3CredentialsOf(credentials)) {
      auto resolved = Resolve(*credential);
      const auto& prefix = resolved.prefix;
      const auto current = current_slots_.find(prefix);
      if (current == current_slots_.end()) {
        continue;
      }
      const auto& settings = resolved.client_settings;
      const bool has_keys = resolved.credentials.has_value();
      auto& slot = *current->second;
      if (slot.settings != settings || slot.credentials.has_value() != has_keys) {
        ICEBERG_LOG_WARN(
            "Ignoring refreshed credentials for '{}' because their client settings "
            "changed",
            prefix);
        continue;
      }
      auto parsed_expiry = ParseCredentialExpiry(resolved.properties, credential->prefix);
      if (!parsed_expiry.has_value()) {
        ICEBERG_LOG_WARN("Ignoring refreshed credentials for '{}': {}", prefix,
                         parsed_expiry.error().message);
        continue;
      }
      if (parsed_expiry->has_value() && **parsed_expiry <= CurrentTimePointMs()) {
        ICEBERG_LOG_WARN("Ignoring already expired refreshed credentials for '{}'",
                         prefix);
        continue;
      }
      slot.credentials = std::move(resolved.credentials);
      slot.expires_at = *parsed_expiry;
      slot.expiry_unknown = false;
      for (auto& installed : storage_credentials_) {
        if (NormalizeS3Location(installed.prefix) == prefix) {
          installed = *credential;
          break;
        }
      }
      updated = true;
    }
    if (!updated && !current_slots_.empty()) {
      return InvalidArgument("Refreshed credentials did not update any active prefix");
    }
    ++credential_generation_;
    return {};
  }

  std::shared_ptr<CredentialSlot> SlotForLocked(
      std::string prefix, std::unordered_map<std::string, std::string> settings,
      std::optional<Aws::Auth::AWSCredentials> initial_credentials) {
    for (const auto& slot : known_slots_) {
      if (slot->prefix == prefix && slot->settings == settings) {
        return slot;
      }
    }
    auto slot = std::make_shared<CredentialSlot>(
        CredentialSlot{.prefix = std::move(prefix),
                       .settings = std::move(settings),
                       .credentials = std::move(initial_credentials)});
    known_slots_.push_back(slot);
    return slot;
  }

  bool RefreshDue() const {
    if (std::chrono::steady_clock::now() < retry_refresh_at_) {
      return false;
    }
    const auto refresh_by = CurrentTimePointMs() + kRefreshLeadTime;
    return std::ranges::any_of(current_slots_, [refresh_by](const auto& entry) {
      return entry.second->expiry_unknown || entry.second->ExpiresBy(refresh_by);
    });
  }

  bool Expired() const {
    const auto now = CurrentTimePointMs();
    return std::ranges::any_of(current_slots_, [now](const auto& entry) {
      return entry.second->ExpiresBy(now);
    });
  }

  // Retry after kRefreshRetryBackoff, or sooner if a token expires first, but
  // never sooner than kMinRefreshRetryBackoff.
  std::chrono::milliseconds BackoffDelay() const {
    std::optional<TimePointMs> earliest;
    for (const auto& [_, slot] : current_slots_) {
      if (slot->expires_at.has_value() &&
          (!earliest.has_value() || *slot->expires_at < *earliest)) {
        earliest = slot->expires_at;
      }
    }
    const auto now = CurrentTimePointMs();
    if (earliest.has_value() && *earliest > now) {
      return std::clamp<std::chrono::milliseconds>(
          *earliest - now, kMinRefreshRetryBackoff, kRefreshRetryBackoff);
    }
    return kRefreshRetryBackoff;
  }

  const std::unordered_map<std::string, std::string> default_properties_;
  mutable std::shared_mutex mutex_;
  std::vector<StorageCredential> storage_credentials_;
  std::vector<std::shared_ptr<CredentialSlot>> known_slots_;
  std::unordered_map<std::string, std::shared_ptr<CredentialSlot>> current_slots_;
  std::shared_ptr<StorageCredentialProvider> provider_;
  std::chrono::steady_clock::time_point retry_refresh_at_;
  uint64_t credential_generation_ = 0;
  std::timed_mutex refresh_mutex_;
};

// Supplies one S3 client's keys from its slot in the shared store, which lets
// clients and handles outlive the FileIO. The last keys seen stay in use while
// the slot has none, as after the prefix is reinstalled without keys.
class VendedCredentialsProvider final : public Aws::Auth::AWSCredentialsProvider {
 public:
  VendedCredentialsProvider(std::shared_ptr<CredentialStore> store,
                            std::shared_ptr<const std::atomic<bool>> installed,
                            std::shared_ptr<CredentialSlot> slot,
                            Aws::Auth::AWSCredentials credentials)
      : store_(std::move(store)),
        installed_(std::move(installed)),
        slot_(std::move(slot)),
        credentials_(std::move(credentials)) {}

  Aws::Auth::AWSCredentials GetAWSCredentials() override {
    // The SDK also asks while building the client. Until its install, the slot
    // may hold the keys being replaced, and asking could start their refresh.
    if (!installed_->load()) {
      std::lock_guard lock(mutex_);
      return credentials_;
    }
    auto current = store_->CredentialsFor(*slot_);
    std::lock_guard lock(mutex_);
    if (current.has_value()) {
      credentials_ = std::move(current).value();
    }
    return credentials_;
  }

 private:
  const std::shared_ptr<CredentialStore> store_;
  // Set once the install that built this client has made its keys current.
  const std::shared_ptr<const std::atomic<bool>> installed_;
  const std::shared_ptr<CredentialSlot> slot_;
  std::mutex mutex_;
  Aws::Auth::AWSCredentials credentials_;
};

class ArrowS3FileIO final : public FileIO, public SupportsStorageCredentials {
 public:
  ArrowS3FileIO(std::shared_ptr<::arrow::fs::FileSystem> arrow_fs,
                std::unordered_map<std::string, std::string> default_properties,
                size_t delete_parallelism)
      : default_file_io_(std::make_shared<ArrowFileSystemFileIO>(std::move(arrow_fs))),
        delete_parallelism_(delete_parallelism),
        credential_store_(
            std::make_shared<CredentialStore>(std::move(default_properties))) {}

  Result<std::unique_ptr<InputFile>> NewInputFile(std::string file_location) override;

  Result<std::unique_ptr<InputFile>> NewInputFile(std::string file_location,
                                                  size_t length) override;

  Result<std::unique_ptr<OutputFile>> NewOutputFile(std::string file_location) override;

  Status DeleteFile(const std::string& file_location) override;

  Status DeleteFiles(const std::vector<std::string>& file_locations) override;

  Status SetStorageCredentials(
      const std::vector<StorageCredential>& storage_credentials) override;

  std::vector<StorageCredential> credentials() const override {
    return credential_store_->credentials();
  }

  Status InitializeStorageCredentials(
      const std::vector<StorageCredential>& storage_credentials,
      std::shared_ptr<StorageCredentialProvider> provider) override;

  SupportsStorageCredentials* AsSupportsStorageCredentials() override { return this; }

 private:
  using DelegatesByPrefix =
      std::vector<std::pair<std::string, std::shared_ptr<ArrowFileSystemFileIO>>>;

  /// \brief Delegate with the longest prefix of `location`, pinned by the caller
  /// against a concurrent credential install. Fails with TokenExpired once that
  /// prefix's keys have expired.
  Result<std::shared_ptr<ArrowFileSystemFileIO>> FileIOForPath(std::string_view location);

  /// \brief Build delegates for `storage_credentials` and make them current,
  /// replacing the refresh provider too when one is given.
  Status InstallStorageCredentials(
      const std::vector<StorageCredential>& storage_credentials,
      std::optional<std::shared_ptr<StorageCredentialProvider>> provider);

  /// \brief Build a delegate for each credential this FileIO can serve. Their
  /// clients take keys from the store once `installed` is set.
  ///
  /// Runs without holding `mutex_`: building an S3 client can block (without
  /// static keys the AWS SDK may wait on the EC2 metadata service), which would
  /// stall every concurrent operation. Reads no mutable member state.
  Result<DelegatesByPrefix> BuildDelegates(
      const std::vector<StorageCredential>& storage_credentials,
      const std::shared_ptr<const std::atomic<bool>>& installed) const;

  std::shared_ptr<ArrowFileSystemFileIO> default_file_io_;
  const size_t delete_parallelism_;
  const std::shared_ptr<CredentialStore> credential_store_;
  // Guards everything below; shared because reads happen per file operation.
  mutable std::shared_mutex mutex_;
  DelegatesByPrefix file_io_by_prefix_;
};

Status ArrowS3FileIO::InitializeStorageCredentials(
    const std::vector<StorageCredential>& storage_credentials,
    std::shared_ptr<StorageCredentialProvider> provider) {
  return InstallStorageCredentials(storage_credentials, std::move(provider));
}

Status ArrowS3FileIO::SetStorageCredentials(
    const std::vector<StorageCredential>& storage_credentials) {
  return InstallStorageCredentials(storage_credentials, std::nullopt);
}

Status ArrowS3FileIO::InstallStorageCredentials(
    const std::vector<StorageCredential>& storage_credentials,
    std::optional<std::shared_ptr<StorageCredentialProvider>> provider) {
  auto installed = std::make_shared<std::atomic<bool>>(false);
  ICEBERG_ASSIGN_OR_RAISE(auto delegates, BuildDelegates(storage_credentials, installed));
  auto credentials = storage_credentials;
  {
    std::unique_lock lock(mutex_);
    file_io_by_prefix_.swap(delegates);
    credential_store_->Install(std::move(credentials), std::move(provider));
    installed->store(true);
  }
  // `delegates` now holds the retired clients and destructs here, outside the
  // lock: tearing down an S3 client can block on its in-flight requests.
  return {};
}

Result<ArrowS3FileIO::DelegatesByPrefix> ArrowS3FileIO::BuildDelegates(
    const std::vector<StorageCredential>& storage_credentials,
    const std::shared_ptr<const std::atomic<bool>>& installed) const {
  DelegatesByPrefix delegates;
  delegates.reserve(storage_credentials.size());
  for (const auto& credential : storage_credentials) {
    ICEBERG_RETURN_UNEXPECTED(credential.Validate());
  }
  for (const auto* credential : S3CredentialsOf(storage_credentials)) {
    auto resolved = credential_store_->Resolve(*credential);
    std::shared_ptr<VendedCredentialsProvider> keys;
    if (resolved.credentials.has_value()) {
      auto slot = credential_store_->SlotFor(
          resolved.prefix, std::move(resolved.client_settings), resolved.credentials);
      keys = std::make_shared<VendedCredentialsProvider>(
          credential_store_, installed, std::move(slot),
          *std::move(resolved.credentials));
    }
    ICEBERG_ASSIGN_OR_RAISE(auto fs,
                            BuildArrowS3FileSystem(resolved.properties, std::move(keys)));
    delegates.emplace_back(std::move(resolved.prefix),
                           std::make_shared<ArrowFileSystemFileIO>(std::move(fs)));
  }
  if (delegates.empty() && !storage_credentials.empty()) {
    ICEBERG_LOG_WARN(
        "None of the {} vended storage credential(s) has an S3-compatible prefix; "
        "S3 access will use the default credentials",
        storage_credentials.size());
  }
  return delegates;
}

Result<std::shared_ptr<ArrowFileSystemFileIO>> ArrowS3FileIO::FileIOForPath(
    std::string_view location) {
  credential_store_->MaybeRefreshCredentials();
  std::shared_lock lock(mutex_);
  if (file_io_by_prefix_.empty()) {
    return default_file_io_;
  }
  const std::string normalized = NormalizeS3Location(location);
  const DelegatesByPrefix::value_type* best = nullptr;
  for (const auto& delegate : file_io_by_prefix_) {
    const auto& prefix = delegate.first;
    if (prefix.size() > (best != nullptr ? best->first.size() : 0) &&
        normalized.starts_with(prefix)) {
      best = &delegate;
    }
  }
  if (best == nullptr) {
    return default_file_io_;
  }
  if (credential_store_->IsExpired(best->first)) {
    return TokenExpired(
        "Vended credentials for \"{}\" have expired and could not be refreshed",
        best->first);
  }
  return best->second;
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
