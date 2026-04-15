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

#include "iceberg/catalog/rest/auth/sigv4_auth_manager.h"

#include <cstdlib>
#include <sstream>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSAuthSigner.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/http/standard/StandardHttpRequest.h>
#include <aws/core/utils/HashingUtils.h>

#include "iceberg/catalog/rest/auth/auth_manager_internal.h"
#include "iceberg/catalog/rest/auth/auth_managers.h"
#include "iceberg/catalog/rest/auth/auth_properties.h"
#include "iceberg/catalog/rest/endpoint.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/string_util.h"

namespace iceberg::rest::auth {

namespace {

/// \brief Ensures AWS SDK is initialized exactly once per process.
/// ShutdownAPI is intentionally never called (leak-by-design) to avoid
/// static destruction order issues with objects that may outlive shutdown.
class AwsSdkGuard {
 public:
  static void EnsureInitialized() {
    static AwsSdkGuard instance;
    (void)instance;
  }

 private:
  AwsSdkGuard() {
    Aws::SDKOptions options;
    Aws::InitAPI(options);
  }
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

}  // namespace

// ---- SigV4AuthSession ----

SigV4AuthSession::SigV4AuthSession(
    std::shared_ptr<AuthSession> delegate, std::string signing_region,
    std::string signing_name,
    std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_provider,
    std::unordered_map<std::string, std::string> effective_properties)
    : delegate_(std::move(delegate)),
      signing_region_(std::move(signing_region)),
      signing_name_(std::move(signing_name)),
      credentials_provider_(std::move(credentials_provider)),
      signer_(std::make_unique<RestSigV4Signer>(
          credentials_provider_, signing_name_.c_str(), signing_region_.c_str())),
      effective_properties_(std::move(effective_properties)) {}

SigV4AuthSession::~SigV4AuthSession() = default;

Result<HTTPRequest> SigV4AuthSession::Authenticate(const HTTPRequest& request) {
  ICEBERG_ASSIGN_OR_RAISE(auto delegate_request, delegate_->Authenticate(request));
  const auto& original_headers = delegate_request.headers;

  std::unordered_map<std::string, std::string> signing_headers;
  for (const auto& [name, value] : original_headers) {
    if (StringUtils::EqualsIgnoreCase(name, "Authorization")) {
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

  // Empty body: hex EMPTY_BODY_SHA256 (Java parity workaround for the signer
  // computing an invalid checksum on empty bodies). Non-empty: Base64.
  if (delegate_request.body.empty()) {
    aws_request->SetHeaderValue("x-amz-content-sha256", Aws::String(kEmptyBodySha256));
  } else {
    auto body_stream =
        Aws::MakeShared<std::stringstream>("SigV4Body", delegate_request.body);
    aws_request->AddContentBody(body_stream);
    auto sha256 = Aws::Utils::HashingUtils::CalculateSHA256(
        Aws::String(delegate_request.body.data(), delegate_request.body.size()));
    aws_request->SetHeaderValue("x-amz-content-sha256",
                                Aws::Utils::HashingUtils::Base64Encode(sha256));
  }

  if (!signer_->SignRequest(*aws_request)) {
    return std::unexpected<Error>(Error{.kind = ErrorKind::kAuthenticationFailed,
                                        .message = "SigV4 signing failed"});
  }

  HTTPRequest signed_request{.method = delegate_request.method,
                             .url = std::move(delegate_request.url),
                             .headers = {},
                             .body = std::move(delegate_request.body)};
  for (const auto& [aws_name, aws_value] : aws_request->GetHeaders()) {
    std::string name(aws_name.c_str(), aws_name.size());
    std::string value(aws_value.c_str(), aws_value.size());
    for (const auto& [orig_name, orig_value] : original_headers) {
      if (StringUtils::EqualsIgnoreCase(orig_name, name) && orig_value != value) {
        signed_request.headers[std::string(kRelocatedHeaderPrefix) + orig_name] =
            orig_value;
        break;
      }
    }
    signed_request.headers[std::move(name)] = std::move(value);
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
  AwsSdkGuard::EnsureInitialized();
  ICEBERG_ASSIGN_OR_RAISE(auto delegate_session,
                          delegate_->InitSession(init_client, properties));
  return WrapSession(std::move(delegate_session), properties);
}

Result<std::shared_ptr<AuthSession>> SigV4AuthManager::CatalogSession(
    HttpClient& shared_client,
    const std::unordered_map<std::string, std::string>& properties) {
  AwsSdkGuard::EnsureInitialized();
  ICEBERG_ASSIGN_OR_RAISE(auto delegate_session,
                          delegate_->CatalogSession(shared_client, properties));
  return WrapSession(std::move(delegate_session), properties);
}

Result<std::shared_ptr<AuthSession>> SigV4AuthManager::ContextualSession(
    const std::unordered_map<std::string, std::string>& context,
    std::shared_ptr<AuthSession> parent) {
  auto sigv4_parent = internal::checked_pointer_cast<SigV4AuthSession>(std::move(parent));

  ICEBERG_ASSIGN_OR_RAISE(auto delegate_session, delegate_->ContextualSession(
                                                     context, sigv4_parent->delegate()));

  auto merged = MergeProperties(sigv4_parent->effective_properties(), context);
  return WrapSession(std::move(delegate_session), std::move(merged));
}

Result<std::shared_ptr<AuthSession>> SigV4AuthManager::TableSession(
    const TableIdentifier& table,
    const std::unordered_map<std::string, std::string>& properties,
    std::shared_ptr<AuthSession> parent) {
  auto sigv4_parent = internal::checked_pointer_cast<SigV4AuthSession>(std::move(parent));

  ICEBERG_ASSIGN_OR_RAISE(
      auto delegate_session,
      delegate_->TableSession(table, properties, sigv4_parent->delegate()));

  auto merged = MergeProperties(sigv4_parent->effective_properties(), properties);
  return WrapSession(std::move(delegate_session), std::move(merged));
}

Status SigV4AuthManager::Close() { return delegate_->Close(); }

Result<std::shared_ptr<Aws::Auth::AWSCredentialsProvider>>
SigV4AuthManager::MakeCredentialsProvider(
    const std::unordered_map<std::string, std::string>& properties) {
  auto access_key_it = properties.find(AuthProperties::kSigV4AccessKeyId);
  auto secret_key_it = properties.find(AuthProperties::kSigV4SecretAccessKey);
  bool has_ak = access_key_it != properties.end() && !access_key_it->second.empty();
  bool has_sk = secret_key_it != properties.end() && !secret_key_it->second.empty();

  ICEBERG_PRECHECK(
      has_ak == has_sk, "Both '{}' and '{}' must be set together, or neither",
      AuthProperties::kSigV4AccessKeyId, AuthProperties::kSigV4SecretAccessKey);

  if (has_ak) {
    Aws::Auth::AWSCredentials credentials(access_key_it->second.c_str(),
                                          secret_key_it->second.c_str());
    auto session_token_it = properties.find(AuthProperties::kSigV4SessionToken);
    if (session_token_it != properties.end() && !session_token_it->second.empty()) {
      credentials.SetSessionToken(session_token_it->second.c_str());
    }
    return std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(credentials);
  }

  return std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>();
}

std::string SigV4AuthManager::ResolveSigningRegion(
    const std::unordered_map<std::string, std::string>& properties) {
  if (auto it = properties.find(AuthProperties::kSigV4SigningRegion);
      it != properties.end() && !it->second.empty()) {
    return it->second;
  }
  if (const char* env = std::getenv("AWS_REGION")) {
    return {env};
  }
  if (const char* env = std::getenv("AWS_DEFAULT_REGION")) {
    return {env};
  }
  return "us-east-1";
}

std::string SigV4AuthManager::ResolveSigningName(
    const std::unordered_map<std::string, std::string>& properties) {
  if (auto it = properties.find(AuthProperties::kSigV4SigningName);
      it != properties.end() && !it->second.empty()) {
    return it->second;
  }
  return AuthProperties::kSigV4SigningNameDefault;
}

Result<std::shared_ptr<AuthSession>> SigV4AuthManager::WrapSession(
    std::shared_ptr<AuthSession> delegate_session,
    std::unordered_map<std::string, std::string> properties) {
  auto region = ResolveSigningRegion(properties);
  auto service = ResolveSigningName(properties);
  ICEBERG_ASSIGN_OR_RAISE(auto credentials, MakeCredentialsProvider(properties));
  return std::make_shared<SigV4AuthSession>(
      std::move(delegate_session), std::move(region), std::move(service),
      std::move(credentials), std::move(properties));
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
  ICEBERG_ASSIGN_OR_RAISE(auto delegate, AuthManagers::Load(name, delegate_props));
  return std::make_unique<SigV4AuthManager>(std::move(delegate));
}

}  // namespace iceberg::rest::auth
