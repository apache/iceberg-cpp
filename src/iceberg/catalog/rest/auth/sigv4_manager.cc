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

#include "iceberg/catalog/rest/auth/auth_manager_internal.h"
#include "iceberg/catalog/rest/auth/sigv4_auth_manager_internal.h"
#include "iceberg/catalog/session_context.h"
#include "iceberg/result.h"

#if ICEBERG_SIGV4_ENABLED

#  include <atomic>
#  include <map>
#  include <mutex>
#  include <sstream>
#  include <string_view>
#  include <utility>

#  include <aws/core/Aws.h>
#  include <aws/core/auth/AWSAuthSigner.h>
#  include <aws/core/auth/AWSCredentialsProvider.h>
#  include <aws/core/auth/AWSCredentialsProviderChain.h>
#  include <aws/core/client/ClientConfiguration.h>
#  include <aws/core/config/ConfigAndCredentialsCacheManager.h>
#  include <aws/core/http/standard/StandardHttpRequest.h>
#  include <aws/core/platform/Environment.h>
#  include <aws/core/utils/HashingUtils.h>

#  include "iceberg/catalog/rest/auth/auth_managers.h"
#  include "iceberg/catalog/rest/auth/auth_properties.h"
#  include "iceberg/catalog/rest/auth/oauth2_util.h"
#  include "iceberg/util/macros.h"
#  include "iceberg/util/string_util.h"

namespace iceberg::rest::auth {

namespace {

constexpr std::string_view kAmzContentSha256Header = "x-amz-content-sha256";

class AwsSdkLifecycle {
 public:
  static AwsSdkLifecycle& Instance() {
    static AwsSdkLifecycle instance;
    return instance;
  }

  Status Initialize() {
    std::lock_guard<std::mutex> lock(mutex_);
    auto s = state_.load();
    if (s == State::kInitialized) return {};
    if (s == State::kFinalized) {
      return InvalidArgument("AWS SDK has already been finalized; cannot reinitialize");
    }
    Aws::InitAPI(options_);
    state_.store(State::kInitialized);
    return {};
  }

  Status Finalize() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_.load() != State::kInitialized) return {};
    if (active_session_count_ != 0) {
      return Invalid(
          "Cannot finalize AWS SDK while {} SigV4 auth session(s) are still alive",
          active_session_count_);
    }
    Aws::ShutdownAPI(options_);
    state_.store(State::kFinalized);
    return {};
  }

  Status EnsureInitialized() {
    if (state_.load() == State::kInitialized) return {};
    return Initialize();
  }

  bool IsInitialized() const { return state_.load() == State::kInitialized; }
  bool IsFinalized() const { return state_.load() == State::kFinalized; }

  // Holds the mutex while incrementing, so Finalize() can never observe a
  // stale 0 between its count check and Aws::ShutdownAPI.
  Status RegisterSession() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_.load() != State::kInitialized) {
      return InvalidArgument(
          "AWS SDK is not initialized; cannot create a SigV4AuthSession");
    }
    ++active_session_count_;
    return {};
  }

  void UnregisterSession() {
    std::lock_guard<std::mutex> lock(mutex_);
    --active_session_count_;
  }

 private:
  enum class State : uint8_t { kUninitialized, kInitialized, kFinalized };

  AwsSdkLifecycle() = default;

  std::atomic<State> state_{State::kUninitialized};
  std::mutex mutex_;
  Aws::SDKOptions options_;
  size_t active_session_count_{0};  // guarded by mutex_
};

Aws::Http::HttpMethod ToAwsMethod(HttpMethod method) {
  switch (method) {
    case HttpMethod::kGet:
      return Aws::Http::HttpMethod::HTTP_GET;
    case HttpMethod::kPost:
      return Aws::Http::HttpMethod::HTTP_POST;
    case HttpMethod::kPut:
      return Aws::Http::HttpMethod::HTTP_PUT;
    case HttpMethod::kDelete:
      return Aws::Http::HttpMethod::HTTP_DELETE;
    case HttpMethod::kHead:
      return Aws::Http::HttpMethod::HTTP_HEAD;
  }
  return Aws::Http::HttpMethod::HTTP_GET;
}

std::unordered_map<std::string, std::string> MergeProperties(
    const std::unordered_map<std::string, std::string>& base,
    const std::unordered_map<std::string, std::string>& overrides) {
  auto merged = base;
  for (const auto& [key, value] : overrides) {
    merged.insert_or_assign(key, value);
  }
  return merged;
}

Result<std::unordered_map<std::string, std::string>> ContextProperties(
    const SessionContext& context) {
  auto merged = context.properties;
  for (const auto& [key, value] : context.credentials) {
    auto [it, inserted] = merged.emplace(key, value);
    if (!inserted && it->second != value) {
      return InvalidArgument("Session context has conflicting values for property '{}'",
                             key);
    }
  }
  return merged;
}

/// Matches Java RESTSigV4AuthSession: canonical headers carry
/// Base64(SHA256(body)), canonical request trailer uses hex.
class RestSigV4Signer : public Aws::Client::AWSAuthV4Signer {
 public:
  RestSigV4Signer(const std::shared_ptr<Aws::Auth::AWSCredentialsProvider>& creds,
                  const char* service_name, const Aws::String& region)
      : Aws::Client::AWSAuthV4Signer(creds, service_name, region,
                                     PayloadSigningPolicy::Always,
                                     /*urlEscapePath=*/false) {
    // Skip the signer's hex overwrite of x-amz-content-sha256 so canonical
    // headers see the caller's Base64; ComputePayloadHash still feeds hex
    // into the canonical request trailer.
    m_includeSha256HashHeader = false;
  }
};

// TODO(sigv4): support loading a custom AWSCredentialsProvider via a class
// name property, matching Java's AwsProperties.restCredentialsProvider().
Result<std::shared_ptr<Aws::Auth::AWSCredentialsProvider>> MakeCredentialsProvider(
    const std::unordered_map<std::string, std::string>& properties) {
  auto access_key_it = properties.find(AuthProperties::kSigV4AccessKeyId);
  auto secret_key_it = properties.find(AuthProperties::kSigV4SecretAccessKey);
  auto session_token_it = properties.find(AuthProperties::kSigV4SessionToken);
  bool has_ak = access_key_it != properties.end() && !access_key_it->second.empty();
  bool has_sk = secret_key_it != properties.end() && !secret_key_it->second.empty();
  bool has_token =
      session_token_it != properties.end() && !session_token_it->second.empty();

  ICEBERG_PRECHECK(
      has_ak == has_sk, "Both '{}' and '{}' must be set together, or neither",
      AuthProperties::kSigV4AccessKeyId, AuthProperties::kSigV4SecretAccessKey);
  ICEBERG_PRECHECK(!has_token || (has_ak && has_sk),
                   "'{}' requires both '{}' and '{}' to be set",
                   AuthProperties::kSigV4SessionToken, AuthProperties::kSigV4AccessKeyId,
                   AuthProperties::kSigV4SecretAccessKey);

  if (has_ak) {
    Aws::Auth::AWSCredentials credentials(access_key_it->second.c_str(),
                                          secret_key_it->second.c_str());
    if (has_token) {
      credentials.SetSessionToken(session_token_it->second.c_str());
    }
    return std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(credentials);
  }

  return std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>();
}

Result<std::string> ResolveSigningRegion(
    const std::unordered_map<std::string, std::string>& properties) {
  if (auto it = properties.find(AuthProperties::kSigV4SigningRegion);
      it != properties.end() && !it->second.empty()) {
    return it->second;
  }
  // Resolve from env then the shared config profile, otherwise fail.
  // If this becomes expensive, cache it at the catalog/AuthManager scope or
  // introduce an AwsProperties-like object as Java does.
  Aws::String region = Aws::Environment::GetEnv("AWS_REGION");
  if (region.empty()) {
    region = Aws::Environment::GetEnv("AWS_DEFAULT_REGION");
  }
  if (region.empty()) {
    const auto& profiles = Aws::Config::GetCachedConfigProfiles();
    if (auto it = profiles.find(Aws::Auth::GetConfigProfileName());
        it != profiles.end()) {
      region = it->second.GetRegion();
    }
  }
  if (region.empty()) {
    return InvalidArgument(
        "SigV4: could not resolve a signing region; set the '{}' property or the "
        "AWS_REGION environment variable",
        AuthProperties::kSigV4SigningRegion);
  }
  return std::string(region.c_str());
}

std::string ResolveSigningName(
    const std::unordered_map<std::string, std::string>& properties) {
  if (auto it = properties.find(AuthProperties::kSigV4SigningName);
      it != properties.end() && !it->second.empty()) {
    return it->second;
  }
  return AuthProperties::kSigV4SigningNameDefault;
}

bool HasSigV4CredentialOverride(
    const std::unordered_map<std::string, std::string>& properties) {
  return properties.contains(AuthProperties::kSigV4AccessKeyId) ||
         properties.contains(AuthProperties::kSigV4SecretAccessKey) ||
         properties.contains(AuthProperties::kSigV4SessionToken);
}

Result<std::shared_ptr<Aws::Auth::AWSCredentialsProvider>> ResolveCredentialsProvider(
    const std::unordered_map<std::string, std::string>& properties,
    std::shared_ptr<Aws::Auth::AWSCredentialsProvider> reuse_credentials = nullptr) {
  if (reuse_credentials && !HasSigV4CredentialOverride(properties)) {
    return reuse_credentials;
  }
  return MakeCredentialsProvider(properties);
}

template <typename Fn>
class ScopeExit {
 public:
  explicit ScopeExit(Fn fn) : fn_(std::move(fn)) {}
  ScopeExit(ScopeExit&& other) noexcept
      : fn_(std::move(other.fn_)), active_(other.active_) {
    other.active_ = false;
  }
  ScopeExit(const ScopeExit&) = delete;
  ScopeExit& operator=(const ScopeExit&) = delete;
  ScopeExit& operator=(ScopeExit&&) = delete;
  ~ScopeExit() {
    if (active_) fn_();
  }
  void Cancel() noexcept { active_ = false; }

 private:
  Fn fn_;
  bool active_ = true;
};

}  // namespace

// ---- SigV4AuthSession ----

SigV4AuthSession::SigV4AuthSession(
    std::shared_ptr<AuthSession> delegate, std::string signing_region,
    std::string signing_name,
    std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_provider)
    : delegate_(std::move(delegate)),
      signing_region_(std::move(signing_region)),
      signing_name_(std::move(signing_name)),
      credentials_provider_(std::move(credentials_provider)),
      signer_(std::make_unique<RestSigV4Signer>(
          credentials_provider_, signing_name_.c_str(), signing_region_.c_str())) {}

SigV4AuthSession::~SigV4AuthSession() { AwsSdkLifecycle::Instance().UnregisterSession(); }

Result<HttpRequest> SigV4AuthSession::Authenticate(HttpRequest request) {
  ICEBERG_ASSIGN_OR_RAISE(auto delegate_request,
                          delegate_->Authenticate(std::move(request)));
  const auto& original_headers = delegate_request.headers;

  std::unordered_map<std::string, std::string> signing_headers;
  for (const auto& [name, value] : original_headers) {
    if (StringUtils::EqualsIgnoreCase(name, kAuthorizationHeader)) {
      signing_headers[std::string(kRelocatedHeaderPrefix) + name] = value;
    } else {
      signing_headers[name] = value;
    }
  }

  Aws::Http::URI aws_uri(delegate_request.url.c_str());
  auto aws_request = std::make_shared<Aws::Http::Standard::StandardHttpRequest>(
      aws_uri, ToAwsMethod(delegate_request.method));
  for (const auto& [name, value] : signing_headers) {
    aws_request->SetHeaderValue(Aws::String(name.c_str()), Aws::String(value.c_str()));
  }

  // Empty bodies use the hex SHA256 constant; non-empty bodies use
  // Base64(SHA256(body)). This matches Java RESTSigV4AuthSession behavior.
  if (delegate_request.body.empty()) {
    aws_request->SetHeaderValue(Aws::String(kAmzContentSha256Header),
                                Aws::String(kEmptyBodySha256));
  } else {
    auto body_stream =
        Aws::MakeShared<std::stringstream>("SigV4Body", delegate_request.body);
    aws_request->AddContentBody(body_stream);
    auto sha256 = Aws::Utils::HashingUtils::CalculateSHA256(
        Aws::String(delegate_request.body.data(), delegate_request.body.size()));
    aws_request->SetHeaderValue(Aws::String(kAmzContentSha256Header),
                                Aws::Utils::HashingUtils::Base64Encode(sha256));
  }

  if (!signer_->SignRequest(*aws_request)) {
    return AuthenticationFailed("AWS SigV4 request signing failed");
  }

  // Build a case-insensitive view of original headers so signer-added headers
  // can be compared without lowercasing or copying the originals.
  std::map<std::string_view, std::string_view, CaseInsensitiveHeaderLess>
      originals_by_name;
  for (const auto& [orig_name, orig_value] : original_headers) {
    originals_by_name.emplace(orig_name, orig_value);
  }

  HttpRequest signed_request{.method = delegate_request.method,
                             .url = std::move(delegate_request.url),
                             .headers = {},
                             .body = std::move(delegate_request.body)};
  for (const auto& [aws_name, aws_value] : aws_request->GetHeaders()) {
    std::string name(aws_name.c_str(), aws_name.size());
    std::string value(aws_value.c_str(), aws_value.size());
    if (auto it = originals_by_name.find(std::string_view(name));
        it != originals_by_name.end()) {
      // Preserve the original value when the signer overwrites a header.
      if (it->second != std::string_view(value)) {
        signed_request.headers.try_emplace(std::string(kRelocatedHeaderPrefix) + name,
                                           std::string(it->second));
      }
    }
    signed_request.headers.insert_or_assign(std::move(name), std::move(value));
  }

  return signed_request;
}

Status SigV4AuthSession::Close() { return delegate_->Close(); }

// ---- SigV4AuthManager ----

SigV4AuthManager::SigV4AuthManager(std::unique_ptr<AuthManager> delegate)
    : delegate_(std::move(delegate)) {}

SigV4AuthManager::~SigV4AuthManager() = default;

Result<std::shared_ptr<AuthSession>> SigV4AuthManager::InitSession(
    HttpClient& init_client,
    const std::unordered_map<std::string, std::string>& properties) {
  ICEBERG_RETURN_UNEXPECTED(AwsSdkLifecycle::Instance().EnsureInitialized());
  ICEBERG_ASSIGN_OR_RAISE(auto delegate_session,
                          delegate_->InitSession(init_client, properties));
  ICEBERG_ASSIGN_OR_RAISE(auto credentials, ResolveCredentialsProvider(properties));
  return WrapSession(std::move(delegate_session), properties, std::move(credentials));
}

Result<std::shared_ptr<AuthSession>> SigV4AuthManager::CatalogSession(
    HttpClient& shared_client,
    const std::unordered_map<std::string, std::string>& properties) {
  ICEBERG_RETURN_UNEXPECTED(AwsSdkLifecycle::Instance().EnsureInitialized());
  catalog_properties_ = properties;
  ICEBERG_ASSIGN_OR_RAISE(auto delegate_session,
                          delegate_->CatalogSession(shared_client, properties));
  ICEBERG_ASSIGN_OR_RAISE(auto credentials, ResolveCredentialsProvider(properties));
  return WrapSession(std::move(delegate_session), properties, std::move(credentials));
}

Result<std::shared_ptr<AuthSession>> SigV4AuthManager::ContextualSession(
    const SessionContext& context, std::shared_ptr<AuthSession> parent) {
  auto sigv4_parent = std::dynamic_pointer_cast<SigV4AuthSession>(std::move(parent));
  ICEBERG_PRECHECK(sigv4_parent != nullptr,
                   "SigV4AuthManager parent must be a SigV4AuthSession");

  ICEBERG_ASSIGN_OR_RAISE(auto delegate_session, delegate_->ContextualSession(
                                                     context, sigv4_parent->delegate()));

  ICEBERG_ASSIGN_OR_RAISE(auto context_properties, ContextProperties(context));
  auto merged = MergeProperties(catalog_properties_, context_properties);
  ICEBERG_ASSIGN_OR_RAISE(
      auto credentials, ResolveCredentialsProvider(context_properties,
                                                   sigv4_parent->credentials_provider()));
  return WrapSession(std::move(delegate_session), merged, std::move(credentials));
}

Result<std::shared_ptr<AuthSession>> SigV4AuthManager::TableSession(
    const TableIdentifier& table,
    const std::unordered_map<std::string, std::string>& properties,
    std::shared_ptr<AuthSession> parent) {
  auto sigv4_parent = std::dynamic_pointer_cast<SigV4AuthSession>(std::move(parent));
  ICEBERG_PRECHECK(sigv4_parent != nullptr,
                   "SigV4AuthManager parent must be a SigV4AuthSession");

  ICEBERG_ASSIGN_OR_RAISE(
      auto delegate_session,
      delegate_->TableSession(table, properties, sigv4_parent->delegate()));

  auto merged = MergeProperties(catalog_properties_, properties);
  ICEBERG_ASSIGN_OR_RAISE(
      auto credentials,
      ResolveCredentialsProvider(properties, sigv4_parent->credentials_provider()));
  return WrapSession(std::move(delegate_session), merged, std::move(credentials));
}

Status SigV4AuthManager::Close() { return delegate_->Close(); }

Result<std::shared_ptr<SigV4AuthSession>> SigV4AuthSession::Make(
    std::shared_ptr<AuthSession> delegate, std::string signing_region,
    std::string signing_name,
    std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_provider) {
  ICEBERG_RETURN_UNEXPECTED(AwsSdkLifecycle::Instance().RegisterSession());
  ScopeExit unregister_on_failure(
      [] { AwsSdkLifecycle::Instance().UnregisterSession(); });
  auto session = std::shared_ptr<SigV4AuthSession>(
      new SigV4AuthSession(std::move(delegate), std::move(signing_region),
                           std::move(signing_name), std::move(credentials_provider)));
  // The session's destructor now owns the unregister.
  unregister_on_failure.Cancel();
  return session;
}

Result<std::shared_ptr<AuthSession>> SigV4AuthManager::WrapSession(
    std::shared_ptr<AuthSession> delegate_session,
    const std::unordered_map<std::string, std::string>& properties,
    std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials) {
  ICEBERG_ASSIGN_OR_RAISE(auto region, ResolveSigningRegion(properties));
  auto service = ResolveSigningName(properties);

  // Fail fast when the provider cannot resolve credentials (e.g. an empty
  // default chain) instead of sending an effectively unsigned request later.
  if (credentials->GetAWSCredentials().IsEmpty()) {
    return AuthenticationFailed(
        "SigV4: AWS credentials provider returned empty credentials; set '{}' and '{}' "
        "or configure the AWS credentials chain",
        AuthProperties::kSigV4AccessKeyId, AuthProperties::kSigV4SecretAccessKey);
  }
  ICEBERG_ASSIGN_OR_RAISE(
      auto session, SigV4AuthSession::Make(std::move(delegate_session), std::move(region),
                                           std::move(service), std::move(credentials)));
  return session;
}

Result<std::unique_ptr<AuthManager>> MakeSigV4AuthManager(
    std::string_view name,
    const std::unordered_map<std::string, std::string>& properties) {
  // Default to OAuth2 when delegate type is not specified.
  std::string delegate_type = AuthProperties::kAuthTypeOAuth2;
  if (auto it = properties.find(AuthProperties::kSigV4DelegateAuthType);
      it != properties.end() && !it->second.empty()) {
    delegate_type = StringUtils::ToLower(it->second);
  }

  // Prevent circular delegation (sigv4 -> sigv4 -> ...).
  ICEBERG_PRECHECK(delegate_type != AuthProperties::kAuthTypeSigV4,
                   "Cannot delegate a SigV4 auth manager to another SigV4 auth "
                   "manager (delegate_type='{}')",
                   delegate_type);

  auto delegate_props = properties;
  delegate_props[AuthProperties::kAuthType] = delegate_type;
  // Strip the legacy flag so the recursive Load doesn't bounce back to SigV4.
  delegate_props.erase(AuthProperties::kSigV4Enabled);
  ICEBERG_ASSIGN_OR_RAISE(auto delegate, AuthManagers::Load(name, delegate_props));
  return std::make_unique<SigV4AuthManager>(std::move(delegate));
}

Status InitializeAwsSdk() { return AwsSdkLifecycle::Instance().Initialize(); }

Status FinalizeAwsSdk() { return AwsSdkLifecycle::Instance().Finalize(); }

bool IsAwsSdkInitialized() { return AwsSdkLifecycle::Instance().IsInitialized(); }

bool IsAwsSdkFinalized() { return AwsSdkLifecycle::Instance().IsFinalized(); }

}  // namespace iceberg::rest::auth

#else  // !ICEBERG_SIGV4_ENABLED

namespace iceberg::rest::auth {

Result<std::unique_ptr<AuthManager>> MakeSigV4AuthManager(
    std::string_view /*name*/,
    const std::unordered_map<std::string, std::string>& /*properties*/) {
  return NotSupported(
      "SigV4 authentication is not built; configure with -DICEBERG_SIGV4=ON");
}

Status InitializeAwsSdk() {
  return NotSupported(
      "SigV4 authentication is not built; configure with -DICEBERG_SIGV4=ON");
}

Status FinalizeAwsSdk() { return {}; }

bool IsAwsSdkInitialized() { return false; }

bool IsAwsSdkFinalized() { return false; }

}  // namespace iceberg::rest::auth

#endif  // ICEBERG_SIGV4_ENABLED
